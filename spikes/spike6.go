package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"time"
)

const (
	maxOffset = 60 * 60 * 24 * 30
)

type cacheEntry struct {
	creation  time.Time
	flags     int
	expTime   int
	dataSize  int
	casUnique int
	key       string
	data      []byte
}

func (entry *cacheEntry) String() string {
	return fmt.Sprintf("cacheEntry{creation=%s, flags=%d, expTime=%d, dataSize=%d, casUnique=%d, key=%s, data=%q}", entry.creation, entry.flags, entry.expTime, entry.dataSize, entry.casUnique, entry.key, entry.data)
}

type Cache interface {
	Set(entry *cacheEntry) error
	Get(key string) (*cacheEntry, error)
	Remove(key string) (*cacheEntry, error)
}

type simpleCache struct {
	entries map[string]*cacheEntry
}

var cache Cache = &simpleCache{entries: make(map[string]*cacheEntry)}

func (c *simpleCache) Set(entry *cacheEntry) error {
	c.entries[entry.key] = entry
	return nil
}

func (c *simpleCache) Get(key string) (entry *cacheEntry, e error) {
	now := time.Now()
	entry = c.entries[key]
	if entry != nil {
		var deadline time.Time
		if entry.expTime <= maxOffset {
			deadline = entry.creation.Add(time.Duration(entry.expTime) * time.Second)
		} else {
			deadline = time.Unix(int64(entry.expTime), 0)
		}

		log.Println("[simpleCache.Get] now", now, "deadline", deadline)

		if now.After(deadline) {
			log.Println("[simpleCache.Get] key", key, "expired")
			entry = nil
		}
	}
	return
}

func (c *simpleCache) Remove(key string) (entry *cacheEntry, e error) {
	entry = c.entries[key]
	if entry != nil {
		delete(c.entries, key)
	}
	return
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
	Execute(w io.Writer)
}

func (c *retrievalCommand) Execute(w io.Writer) {
	log.Println("[retrievalCommand.Execute] retrieving from", c.keys)

	for _, v := range c.keys {
		if entry, e := cache.Get(v); e != nil {
			log.Println("[retrievalCommand.Execute] error:", e)
			w.Write([]byte("SERVER_ERROR\r\n"))
			return
		} else {
			log.Println("[retrievalCommand.Execute] entry:", entry)

			if entry != nil {
				if c.opcode == "get" {
					fmt.Fprintf(w, "VALUE %s %d %d\r\n", entry.key, entry.flags, entry.dataSize)
				} else {
					fmt.Fprintf(w, "VALUE %s %d %d %d\r\n", entry.key, entry.flags, entry.dataSize, entry.casUnique)
				}
				w.Write(entry.data)
				w.Write([]byte("\r\n"))
			}
		}
	}
	w.Write([]byte("END\r\n"))
}

func (c *storageCommand) Execute(w io.Writer) {
	log.Println("[storageCommand.Execute] storing in", c.key)

	entry := &cacheEntry{creation: time.Now(), key: c.key, flags: c.flags, expTime: c.expTime, dataSize: c.dataSize, data: c.data}
	if c.opcode == "set" {
		entry.casUnique = 1 // TODO use something like hash/crc64
		if e := cache.Set(entry); e != nil {
			log.Println("[storageCommand.Execute] error:", e)
			if !c.noReply {
				w.Write([]byte("SERVER_ERROR\r\n"))
			}
		} else {
			if !c.noReply {
				w.Write([]byte("STORED\r\n"))
			}
		}
	} else {
		if oldEntry, e := cache.Get(c.key); e != nil {
			log.Println("[storageCommand.Execute] error:", e)
			if !c.noReply {
				w.Write([]byte("SERVER_ERROR\r\n"))
			}
		} else {
			if c.opcode == "add" {
				if oldEntry != nil {
					if !c.noReply {
						w.Write([]byte("NOT_STORED\r\n"))
					}
				} else {
					entry.casUnique = 1 // TODO use something like hash/crc64
					if e := cache.Set(entry); e != nil {
						log.Println("[storageCommand.Execute] error:", e)
						if !c.noReply {
							w.Write([]byte("SERVER_ERROR\r\n"))
						}
					} else {
						if !c.noReply {
							w.Write([]byte("STORED\r\n"))
						}
					}
				}
			} else if c.opcode == "replace" {
				if oldEntry == nil {
					if !c.noReply {
						w.Write([]byte("NOT_STORED\r\n"))
					}
				} else {
					entry.casUnique = oldEntry.casUnique + 1 // TODO use somenthing like hash/crc65
					if e := cache.Set(entry); e != nil {
						log.Println("[storageCommand.Execute] error:", e)
						if !c.noReply {
							w.Write([]byte("SERVER_ERROR\r\n"))
						}
					} else {
						if !c.noReply {
							w.Write([]byte("STORED\r\n"))
						}
					}
				}
			} else if c.opcode == "cas" {
				if oldEntry == nil {
					if !c.noReply {
						w.Write([]byte("NOT_FOUND\r\n"))
					}
				} else {
					if oldEntry.casUnique != c.casUnique {
						if !c.noReply {
							w.Write([]byte("EXISTS\r\n"))
						}
					} else {
						entry.casUnique = oldEntry.casUnique + 1

						if e := cache.Set(entry); e != nil {
							if !c.noReply {
								w.Write([]byte("SERVER_ERROR\r\n"))
							}
						} else {
							if !c.noReply {
								w.Write([]byte("STORED\r\n"))
							}
						}
					}
				}
			} else {
				if oldEntry != nil {
					ndata := make([]byte, oldEntry.dataSize+entry.dataSize)
					log.Println("[storageCommand.Execute] len(ndata)", len(ndata), "cap(ndata)", cap(ndata))
					if c.opcode == "prepend" {
						copy(ndata, entry.data)
						copy(ndata[entry.dataSize:], oldEntry.data)
					} else {
						copy(ndata, oldEntry.data)
						copy(ndata[oldEntry.dataSize:], entry.data)
					}
					entry.casUnique = oldEntry.casUnique + 1
					entry.dataSize = len(ndata)
					entry.data = ndata
				} else {
					entry.casUnique = 1
				}
				if e := cache.Set(entry); e != nil {
					log.Println("[storageCommand.Execute] error:", e)
					if !c.noReply {
						w.Write([]byte("SERVER_ERROR\r\n"))
					}
				} else {
					if !c.noReply {
						w.Write([]byte("STORED\r\n"))
					}
				}
			}
		}
	}
}

func (c *deleteCommand) Execute(w io.Writer) {
	log.Println("[deleteCommand.Execute] deleting", c.key)

	if entry, e := cache.Remove(c.key); e != nil {
		if !c.noReply {
			w.Write([]byte("SERVER_ERROR\r\n"))
		}
	} else {
		if entry == nil {
			if !c.noReply {
				w.Write([]byte("NOT_FOUND\r\n"))
			}
		} else {
			if !c.noReply {
				w.Write([]byte("DELETED\r\n"))
			}
		}
	}
}

func (c *incDecCommand) Execute(w io.Writer) {
	log.Println("[incDecCommand.Execute] inc/dec(ing)", c.key)

	if entry, e := cache.Get(c.key); e != nil {
		if !c.noReply {
			w.Write([]byte("SERVER_ERROR\r\n"))
		}
	} else {
		if entry == nil {
			if !c.noReply {
				w.Write([]byte("NOT_FOUND\r\n"))
			}
		} else {
			if v, e := strconv.Atoi(string(entry.data)); e != nil {
				log.Println("[incDecCommand.Execute] error", e)
				if !c.noReply {
					w.Write([]byte("ERROR\r\n"))
				}
			} else {
				if c.opcode == "incr" {
					v = v + c.value
				} else if v-c.value >= 0 {
					v = v - c.value
				} else {
					v = 0
				}
				entry.data = []byte(fmt.Sprintf("%d", v))
				if e := cache.Set(entry); e != nil {
					if !c.noReply {
						w.Write([]byte("SERVER_ERROR\r\n"))
					}
				} else {
					if !c.noReply {
						fmt.Fprintf(w, "%d\r\n", v)
					}
				}
			}
		}
	}
}

func (c *touchCommand) Execute(w io.Writer) {
	log.Println("[touchCommand.Execute] touching", c.key)

	if entry, e := cache.Get(c.key); e != nil {
		if !c.noReply {
			w.Write([]byte("SERVER_ERROR\r\n"))
		}
	} else {
		if entry == nil {
			if !c.noReply {
				w.Write([]byte("NOT_FOUND\r\n"))
			}
		} else {
			entry.expTime = c.expTime
			if e := cache.Set(entry); e != nil {
				if !c.noReply {
					w.Write([]byte("SERVER_ERROR\r\n"))
				}
			} else {
				if !c.noReply {
					w.Write([]byte("TOUCHED\r\n"))
				}
			}
		}
	}
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

type builder func([][]byte) (Command, error)

func storageBuilder(tokens [][]byte) (Command, error) {
	if len(tokens) < 5 {
		return nil, errors.New("Insufficient tokens to build storageCommand")
	}

	var e error
	c := &storageCommand{opcode: string(tokens[0])}
	c.key = string(tokens[1])

	if c.flags, e = strconv.Atoi(string(tokens[2])); e != nil {
		return nil, errors.New("Invalid flags")
	}

	if c.expTime, e = strconv.Atoi(string(tokens[3])); e != nil {
		return nil, errors.New("Invalid expTime")
	}

	if c.dataSize, e = strconv.Atoi(string(tokens[4])); e != nil {
		return nil, errors.New("Invalid dataSize")
	}

	if c.opcode == "cas" {
		if len(tokens) < 6 {
			return nil, errors.New("Invalid casUnique")
		} else if c.casUnique, e = strconv.Atoi(string(tokens[5])); e != nil {
			return nil, errors.New("Invalid casUnique")
		}
		if len(tokens) == 7 {
			if string(tokens[6]) == "noreply" {
				c.noReply = true
			} else {
				return nil, errors.New("Invalid noReply")
			}
		} else if len(tokens) > 7 {
			return nil, errors.New("Surplus tokens in line")
		}
	} else if len(tokens) == 6 {
		if string(tokens[5]) == "noreply" {
			c.noReply = true
		} else {
			return nil, errors.New("Invalid noReply")
		}
	} else if len(tokens) > 6 {
		return nil, errors.New("Surplus tokens in line")
	}

	return c, nil
}

func retrievalBuilder(tokens [][]byte) (Command, error) {
	c := &retrievalCommand{opcode: string(tokens[0])}
	for _, v := range tokens[1:] {
		c.keys = append(c.keys, string(v))
	}

	return c, nil
}

func incDecBuilder(tokens [][]byte) (Command, error) {

	if len(tokens) < 3 {
		return nil, errors.New("Insufficient tokens to build incDecCommand")
	}

	var e error
	c := &incDecCommand{opcode: string(tokens[0])}
	c.key = string(tokens[1])

	if c.value, e = strconv.Atoi(string(tokens[2])); e != nil {
		return nil, errors.New("Invalid value")
	}

	if len(tokens) == 4 {
		if string(tokens[3]) == "noreply" {
			c.noReply = true
		} else {
			return nil, errors.New("Invalid noReply")
		}
	} else if len(tokens) > 4 {
		return nil, errors.New("Surplus tokens in line")
	}

	return c, nil
}

func deleteBuilder(tokens [][]byte) (Command, error) {

	if len(tokens) < 2 {
		return nil, errors.New("Insufficient tokens to build deleteCommand")
	}

	c := &deleteCommand{opcode: string(tokens[0])}
	c.key = string(tokens[1])

	if len(tokens) == 3 {
		if string(tokens[2]) == "noreply" {
			c.noReply = true
		} else {
			return nil, errors.New("Invalid noReply")
		}
	} else if len(tokens) > 3 {
		return nil, errors.New("Surplus tokens in line")
	}

	return c, nil
}

func touchBuilder(tokens [][]byte) (Command, error) {

	if len(tokens) < 3 {
		return nil, errors.New("Insufficient tokens to build touchCommand")
	}

	var e error
	c := &touchCommand{opcode: string(tokens[0])}
	c.key = string(tokens[1])

	if c.expTime, e = strconv.Atoi(string(tokens[2])); e != nil {
		return nil, errors.New("Invalid exptime")
	}

	if len(tokens) == 4 {
		if string(tokens[3]) == "noreply" {
			c.noReply = true
		} else {
			return nil, errors.New("Invalid noReply")
		}
	} else if len(tokens) > 4 {
		return nil, errors.New("Surplus tokens in line")
	}

	return c, nil
}

var builders = map[string]builder{
	"set":     storageBuilder,
	"add":     storageBuilder,
	"replace": storageBuilder,
	"append":  storageBuilder,
	"prepend": storageBuilder,
	"cas":     storageBuilder,
	"get":     retrievalBuilder,
	"gets":    retrievalBuilder,
	"incr":    incDecBuilder,
	"decr":    incDecBuilder,
	"delete":  deleteBuilder,
	"touch":   touchBuilder,
}

func handleConn(conn net.Conn) {
	rd := bufio.NewReader(conn)
	for {
		if line, _, e := rd.ReadLine(); e == nil {
			split := bytes.Split(line, []byte(" "))

			if len(split) < 2 {
				log.Println("[handleConn] insufficient number of tokens in line")
				conn.Write([]byte("ERROR\r\n"))
				continue
			} else {
				if builder := builders[string(split[0])]; builder != nil {
					if command, e := builder(split); e == nil {
						if sc, ok := command.(*storageCommand); ok {
							data := make([]byte, sc.dataSize+2)
							if n, e := rd.Read(data); e != nil {
								log.Println("[handleConn] cannot read data", e)
								conn.Write([]byte("ERROR\r\n"))
								continue
							} else if n < sc.dataSize+2 {
								log.Println("[handleConn] could not read entire data", n, " < ", sc.dataSize+2)
								conn.Write([]byte("ERROR\r\n"))
								continue
							} else if string(data[len(data)-2:]) != "\r\n" {
								log.Println("[handleConn] end of line not found after data", string(data[len(data)-2:]))
								conn.Write([]byte("ERROR\r\n"))
								continue
							}
							sc.data = data[:len(data)-2]
						}
						log.Println("[handleConn] executing command", command)
						command.Execute(conn)
					} else {
						log.Println("[handleConn] cannot build command", e)
						conn.Write([]byte("ERROR\r\n"))
						continue
					}
				} else {
					log.Println("[handleConn] builder not found")
					conn.Write([]byte("ERROR\r\n"))
					continue
				}
			}
		} else {
			log.Println("[handleConn] e", e)
			break
		}
	}
}

func main() {

	if l, err := net.Listen("tcp", ":1234"); err == nil {

		for {
			if conn, err := l.Accept(); err == nil {
				go handleConn(conn)
			} else {
				log.Println("[main] err", err)
			}
		}
	} else {
		log.Println("[main]", err)
	}
}
