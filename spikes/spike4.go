package main

import "io"

//import "strings"
import "log"
import "time"
import "errors"
import "fmt"
import "strconv"
import "net"

const bufferMinLength = 16

type commandReader struct {
	rd       io.Reader
	c        chan byte
	buffer   []byte
	finished bool
}

func newCommandReader(rd io.Reader) *commandReader {
	cr := &commandReader{rd: rd, c: make(chan byte, bufferMinLength*2), buffer: make([]byte, 0, bufferMinLength)}
	go cr.readFrom()
	return cr
}

func (cr *commandReader) readFrom() {
	var b [bufferMinLength]byte
	for {
		if n, e := cr.rd.Read(b[:]); n > 0 {
			for _, v := range b[:n] {
				cr.c <- v
			}
		} else if e != nil {
			log.Println("[readFrom]", e)
			close(cr.c)
			return
		}
	}
}

func (cr *commandReader) fill() bool {
	for !cr.finished {
		select {
		case b, ok := <-cr.c:
			if ok {
				cr.buffer = append(cr.buffer, b)

				if len(cr.buffer) > bufferMinLength {
					return true
				}
			} else {
				cr.finished = true
				log.Println("[fill] channel closed")
			}
		case <-time.After(30 * time.Second):
			cr.finished = true
			log.Println("[fill] timed out waiting data")
		}
	}
	return len(cr.buffer) > 0
}

func (cr *commandReader) resizeBuffer(i int) {
	copy(cr.buffer, cr.buffer[i:])
	cr.buffer = cr.buffer[:len(cr.buffer)-i]
}

func (cr *commandReader) readToken() (string, bool) {
	currToken := make([]byte, 0, bufferMinLength)
	for cr.fill() {
		log.Printf("[readToken] buffer %q\n", cr.buffer)
		for i, v := range cr.buffer {
			if v == ' ' {
				cr.resizeBuffer(i + 1)
				log.Printf("[readToken] currToken %q\n", currToken)
				return string(currToken), len(currToken) > 0
			} else {
				currToken = append(currToken, v)
			}
		}
		cr.buffer = cr.buffer[:0]
	}

	return string(currToken), len(currToken) > 0
}

func (cr *commandReader) readData(n int) (string, bool) {
	currToken := make([]byte, 0, n)
	for cr.fill() {
		log.Printf("[readData] buffer %q\n", cr.buffer)
		for i, v := range cr.buffer {
			if len(currToken) == n {
				cr.resizeBuffer(i)
				log.Printf("[readData] currToken %q\n", currToken)
				return string(currToken), len(currToken) > 0
			} else {
				currToken = append(currToken, v)
			}
		}
		cr.buffer = cr.buffer[:0]
	}

	return string(currToken), len(currToken) > 0
}

type retrievalCommand struct {
	opcode string
	keys   []string
}

type storageCommand struct {
	opcode    string
	key       string
	flags     int
	exptime   int
	bytes     int
	casunique string
	noreply   bool
	data      string
}

func (c *retrievalCommand) String() string {
	return fmt.Sprintf("retrievalCommand[opcode=%s, keys=%s]", c.opcode, c.keys)
}

func (c *storageCommand) String() string {
	return fmt.Sprintf("storageCommand[opcode=%s, key=%s, flags=%d, exptime=%d, bytes=%d, noreply=%s]", c.opcode, c.key, c.flags, c.exptime, c.bytes, c.noreply)
}

func (cr *commandReader) buildRetrievalCommand(c *retrievalCommand) bool {

	type buildState int
	const (
		key buildState = iota
		probeNl
		nl
	)
	var state buildState = key
	var e error
	var p string

	for e == nil {

		switch state {
		case key:
			if t, ok := cr.readToken(); ok {
				c.keys = append(c.keys, p+t)
				p = ""
				state = probeNl
			} else {
				e = errors.New("Key not specified")
			}
		case probeNl:
			if t, ok := cr.readData(1); ok {
				if t == "\r" {
					state = nl
				} else {
					p = t
					state = key
				}
			} else {
				e = errors.New("Unfinished command")
			}
		case nl:
			if t, ok := cr.readData(1); ok {
				if t == "\n" {
					return true
				} else {
					e = errors.New("Invalid endof line sequence")
				}
			} else {
				e = errors.New("Unfinished command")
			}
		}

	}

	log.Println("[buildRetrievalCommand]", e)
	return false
}

func (cr *commandReader) buildStorageCommand(c *storageCommand) bool {

	type buildState int
	const (
		key buildState = iota
		flags
		exptime
		bytes
		casunique
		probeNoreply
		noreply
		probeNl
		nl
		data
	)
	var state buildState = key
	var e error

	for e == nil {

		switch state {
		case key:
			if t, ok := cr.readToken(); ok {
				c.key = t
				state = flags
			} else {
				e = errors.New("Key not specified")
			}
		case flags:
			if t, ok := cr.readToken(); ok {
				if c.flags, e = strconv.Atoi(t); e == nil {
					state = exptime
				}
			} else {
				e = errors.New("Flags not specified")
			}
		case exptime:
			if t, ok := cr.readToken(); ok {
				if c.exptime, e = strconv.Atoi(t); e == nil {
					state = bytes
				}
			} else {
				e = errors.New("Exptime not specified")
			}
		case bytes:
			if t, ok := cr.readToken(); ok {
				if c.bytes, e = strconv.Atoi(t); e == nil {
					if c.opcode == "cas" {
						state = casunique
					} else {
						state = probeNoreply
					}
				}
			} else {
				e = errors.New("Exptime not specified")
			}
		case casunique:
			if t, ok := cr.readToken(); ok {
				c.casunique = t
				state = probeNoreply
			} else {
				e = errors.New("Casunique not specified")
			}
		case probeNoreply:
			if t, ok := cr.readData(1); ok {
				if t == "\r" {
					state = nl
				} else if t == "n" {
					state = noreply
				} else {
					e = errors.New("Invalid end of command")
				}
			} else {
				e = errors.New("Unfinished command")
			}
		case noreply:
			if t, ok := cr.readData(6); ok {
				if t == "oreply" {
					c.noreply = true
					state = probeNl
				} else {
					e = errors.New("Invalid end of command")
				}
			} else {
				e = errors.New("Unfinished command")
			}
		case probeNl:
			if t, ok := cr.readData(1); ok {
				if t == "\r" {
					state = nl
				} else {
					e = errors.New("Invalid end of command")
				}
			} else {
				e = errors.New("Unfinished command")
			}
		case nl:
			if t, ok := cr.readData(1); ok {
				if t == "\n" {
					state = data
				} else {
					e = errors.New("Invalid end of command")
				}
			} else {
				e = errors.New("Unfinished command")
			}
		case data:
			if t, ok := cr.readData(c.bytes + 2); ok {
				if t[len(t)-2:] == "\r\n" {
					c.data = t[:len(t)-2]
					return true
				} else {
					e = errors.New("Invalid end of command")
				}
			} else {
				e = errors.New("Unfinished command")
			}
		}

	}

	log.Println("[buildStorageCommand]", e)
	return false
}
func (cr *commandReader) parse() interface{} {
	for {
		if t, ok := cr.readToken(); ok {
			if t == "get" || t == "gets" {
				if c := (&retrievalCommand{opcode: t}); cr.buildRetrievalCommand(c) {
					return c
				}
			} else if t == "set" || t == "add" || t == "replace" || t == "append" || t == "prepend" || t == "cas" {
				if c := (&storageCommand{opcode: t}); cr.buildStorageCommand(c) {
					return c
				}
			} else {
				log.Printf("[parse] Unrecognized token: %q\n", t)
			}
		} else {
			log.Printf("[parse] No more tokens to read")
			return nil
		}
	}

}

type connReader struct {
	conn net.Conn
}

func (cr connReader) Read(b []byte) (n int, e error) {
	cr.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

	n, e = cr.conn.Read(b)

	if oe, ok := e.(*net.OpError); ok {
		if oe.Timeout() {
			e = nil
		}
	}

	return
}

func main() {

	//cr := newCommandReader(strings.NewReader("get a7% 9+m \r\nset MACARR0NE 28374 111726 10 \r\n1m2n3b4v5c\r\ngets 788a dd4__sdf \r\nadd ***&\" 1 2 4 noreply\r\nabcd\r\ncas aaa 1 2 3 UU78uI \r\nasd\r\ncas bbb 3 2 1 jjdjdjd noreply\r\nX\r\n"))

	if l, err := net.Listen("tcp", ":1234"); err == nil {

		if conn, err := l.Accept(); err == nil {

			cr := newCommandReader(connReader{conn: conn})

			for c := cr.parse(); c != nil; c = cr.parse() {
				log.Println("[main]", c)
			}
		} else {
			log.Println("[main]", err)
		}

	} else {
		log.Println("[main]", err)
	}
}
