package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
)

type commandReader struct {
	sc *bufio.Scanner
}

type retrievalCommand struct {
	opcode string
	keys   []string
}

type storageCommand struct {
	opcode    string
	key       string
	flags     int
	expTime   int
	dataSize  int
	casUnique int
	noReply   bool
	data      []byte
}

type deleteCommand struct {
	opcode  string
	key     string
	noReply bool
}

type incDecCommand struct {
	opcode  string
	key     string
	value   int
	noReply bool
}

type touchCommand struct {
	opcode  string
	key     string
	expTime int
	noReply bool
}

type Command interface {
	Execute()
}

func (c *retrievalCommand) Execute() {
	log.Println("[retrievalCommand.Execute] retrieving from", c.keys)
}

func (c *storageCommand) Execute() {
	log.Println("[storageCommand.Execute] storing in", c.key)
}

func (c *deleteCommand) Execute() {
	log.Println("[deleteCommand.Execute] deleting", c.key)
}

func (c *incDecCommand) Execute() {
	log.Println("[incDecCommand.Execute] inc/dec(ing)", c.key)
}

func (c *touchCommand) Execute() {
	log.Println("[touchCommand.Execute] touching", c.key)
}

func (c *retrievalCommand) String() string {
	return fmt.Sprintf("retrievalCommand{opcode=%s, keys=%s}", c.opcode, c.keys)
}

func (c *storageCommand) String() string {
	var d string
	if len(c.data) > 5 {
		d = string(c.data[:5]) + "..."
	} else {
		d = string(c.data)
	}
	return fmt.Sprintf("storageCommand{opcode=%s, key=%s, flags=%d, expTime=%d, dataSize=%d, casUnique=%d, noReply=%s, data=%s}", c.opcode, c.key, c.flags, c.expTime, c.dataSize, c.casUnique, c.noReply, d)
}

func (c *deleteCommand) String() string {
	return fmt.Sprintf("deleteCommand{key=%s, noReply=%s}", c.key, c.noReply)
}

func (c *incDecCommand) String() string {
	return fmt.Sprintf("incDecCommand{key=%s, value=%d, noReply=%s}", c.key, c.value, c.noReply)
}

func (c *touchCommand) String() string {
	return fmt.Sprintf("touchCommand{key=%s, expTime=%d, noReply=%s}", c.key, c.expTime, c.noReply)
}

func splitFixed(n int) bufio.SplitFunc {
	return func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if len(data) < n {
			if atEOF {
				return len(data), data, errors.New("Insufficient bytes to read")
			} else {
				return 0, nil, nil
			}
		} else {
			return n, data[:n], nil
		}
	}

}

func splitAtSpaceOrLine(data []byte, atEOF bool) (advance int, token []byte, err error) {

	log.Printf("[splitAtSpaceOrLine] data=%q, atEOF=%s\n", data, atEOF)
	if len(data) > 1 {
		if data[0] == '\r' {
			if data[1] == '\n' {
				return 2, data[:2], nil
			} else {
				return 0, nil, errors.New("Expected \\n after \\r")
			}
		}
	}

	for i, v := range data {
		if v == ' ' {
			advance = i + 1
			token = data[:i]
			return
		} else if v == '\r' {
			advance = i
			token = data[:i]
			return
		}
	}

	if !atEOF {
		return 0, nil, nil
	} else {
		return len(data), data, nil
	}
}

func (cr *commandReader) buildRetrievalCommand(opcode string) (*retrievalCommand, error) {
	c := &retrievalCommand{opcode: opcode}
	sc := cr.sc
	sc.Split(splitAtSpaceOrLine)
	log.Println("[buildRetrievalCommand]", opcode)
	for sc.Scan() {
		if t := sc.Text(); t == "\r\n" {
			log.Println("[buildRetrievalCommand] found \\r\\n")
			return c, nil
		} else {
			log.Println("[buildRetrievalCommand] t", t)
			c.keys = append(c.keys, t)
		}
	}

	return nil, sc.Err()
}

func (cr *commandReader) buildStorageCommand(opcode string) (*storageCommand, error) {
	c := &storageCommand{opcode: opcode}
	sc := cr.sc
	sc.Split(splitAtSpaceOrLine)
	log.Println("[buildStorageCommand]", opcode)
	var e error

	if sc.Scan() {
		c.key = sc.Text()
	} else {
		return nil, errors.New("Key not found")
	}

	if sc.Scan() {
		if c.flags, e = strconv.Atoi(sc.Text()); e != nil {
			return nil, errors.New("Flags is not a valid integer")
		}
	} else {
		return nil, errors.New("Flags not found")
	}

	if sc.Scan() {
		if c.expTime, e = strconv.Atoi(sc.Text()); e != nil {
			return nil, errors.New("ExpTime is not a valid integer")
		}
	} else {
		return nil, errors.New("ExpTime not found")
	}

	if sc.Scan() {
		if c.dataSize, e = strconv.Atoi(sc.Text()); e != nil {
			return nil, errors.New("DataSize is not a valid integer")
		}
	} else {
		return nil, errors.New("DataSize not found")
	}

	if c.opcode == "cas" {
		if sc.Scan() {
			if c.casUnique, e = strconv.Atoi(sc.Text()); e != nil {
				return nil, errors.New("CasUnique is not a valid integer")
			}
		} else {
			return nil, errors.New("CasUnique not found")
		}
	}

	if sc.Scan() {
		if t := sc.Text(); t == "noreply" {
			c.noReply = true
			if sc.Scan() {
				if sc.Text() != "\r\n" {
					return nil, errors.New("\\r\\n not found")
				}
			} else {
				return nil, errors.New("\\r\\n not found")
			}
			sc.Split(splitFixed(c.dataSize))
		} else if t == "\r\n" {
			sc.Split(splitFixed(c.dataSize))
		} else {
			return nil, errors.New("NoReply/\\r\\n not found")
		}
	}

	if sc.Scan() {
		c.data = make([]byte, c.dataSize)
		copy(c.data, sc.Bytes())
		sc.Split(splitAtSpaceOrLine)
	} else {
		return nil, errors.New("Data not found")
	}

	if sc.Scan() {
		if sc.Text() == "\r\n" {
			return c, nil
		} else {
			return nil, errors.New("\\r\\n expected after data")
		}
	} else {
		return nil, errors.New("\\r\\n not found")
	}
}

func (cr *commandReader) buildDeleteCommand(opcode string) (*deleteCommand, error) {
	c := &deleteCommand{opcode: opcode}
	sc := cr.sc

	if sc.Scan() {
		c.key = sc.Text()
	} else {
		return nil, errors.New("Key not found")
	}

	if sc.Scan() {
		if t := sc.Text(); t == "noreply" {
			c.noReply = true
			if sc.Scan() {
				if sc.Text() != "\r\n" {
					return nil, errors.New("\\r\\n not found")
				}
			} else {
				return nil, errors.New("\\r\\n not found")
			}
			return c, nil
		} else if t == "\r\n" {
			return c, nil
		} else {
			return nil, errors.New("NoReply/\\r\\n not found")
		}
	} else {
		return nil, errors.New("NoReply/\\r\\n not found")
	}
}

func (cr *commandReader) buildIncDecCommand(opcode string) (*incDecCommand, error) {
	c := &incDecCommand{opcode: opcode}
	sc := cr.sc
	var e error

	if sc.Scan() {
		c.key = sc.Text()
	} else {
		return nil, errors.New("Key not found")
	}

	if sc.Scan() {
		if c.value, e = strconv.Atoi(sc.Text()); e != nil {
			return nil, errors.New("Value is not valid")
		}
	} else {
		return nil, errors.New("Value not found")
	}

	if sc.Scan() {
		if t := sc.Text(); t == "noreply" {
			c.noReply = true
			if sc.Scan() {
				if sc.Text() != "\r\n" {
					return nil, errors.New("\\r\\n not found")
				}
			} else {
				return nil, errors.New("\\r\\n not found")
			}
			return c, nil
		} else if t == "\r\n" {
			return c, nil
		} else {
			return nil, errors.New("NoReply/\\r\\n not found")
		}
	} else {
		return nil, errors.New("NoReply/\\r\\n not found")
	}
}

func (cr *commandReader) buildTouchCommand(opcode string) (*touchCommand, error) {
	c := &touchCommand{opcode: opcode}
	sc := cr.sc
	var e error

	if sc.Scan() {
		c.key = sc.Text()
	} else {
		return nil, errors.New("Key not found")
	}

	if sc.Scan() {
		if c.expTime, e = strconv.Atoi(sc.Text()); e != nil {
			return nil, errors.New("ExpTime is not a valid integer")
		}
	} else {
		return nil, errors.New("ExpTime not found")
	}

	if sc.Scan() {
		if t := sc.Text(); t == "noreply" {
			c.noReply = true
			if sc.Scan() {
				if sc.Text() != "\r\n" {
					return nil, errors.New("\\r\\n not found")
				}
			} else {
				return nil, errors.New("\\r\\n not found")
			}
			return c, nil
		} else if t == "\r\n" {
			return c, nil
		} else {
			return nil, errors.New("NoReply/\\r\\n not found")
		}
	} else {
		return nil, errors.New("NoReply/\\r\\n not found")
	}
}

func (cr *commandReader) Read() (Command, error) {
	sc := cr.sc
	sc.Split(splitAtSpaceOrLine)

	for sc.Scan() {
		opcode := sc.Text()

		if opcode == "get" || opcode == "gets" {
			return cr.buildRetrievalCommand(opcode)
		} else if opcode == "set" || opcode == "add" || opcode == "replace" || opcode == "append" || opcode == "prepend" || opcode == "cas" {
			return cr.buildStorageCommand(opcode)
		} else if opcode == "delete" {
			return cr.buildDeleteCommand(opcode)
		} else if opcode == "incr" || opcode == "decr" {
			return cr.buildIncDecCommand(opcode)
		} else if opcode == "touch" {
			return cr.buildTouchCommand(opcode)
		} else {
			log.Println("[cr.Read] invalid opcode", opcode)
		}
		sc.Split(splitAtSpaceOrLine)
	}

	return nil, sc.Err()
}

func main() {

	if l, err := net.Listen("tcp", ":1234"); err == nil {

		if conn, err := l.Accept(); err == nil {
			cr := &commandReader{sc: bufio.NewScanner(conn)}
			for {
				if command, err := cr.Read(); command != nil {
					log.Println("[main] command", command)
					command.Execute()
				} else {
					log.Println("[main] err", err)
					break
				}
			}

		} else {
			log.Println("[main] err", err)
		}

	} else {
		log.Println("[main]", err)
	}
}
