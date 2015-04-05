package main

import (
	//"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	//"os"
	"net"
	"strconv"
	"strings"
	"time"
)

type command struct {
	opcode    string
	key       []string
	flags     string
	exptime   string
	nbytes    string
	casunique string
	noreply   string
	data      string
}

func (c *command) String() string {
	return fmt.Sprintf("command[opcode=%s, key=%s, flags=%s, exptime=%s, nbytes=%s, casunique=%s, noreply=%s, data=%s]", c.opcode, c.key, c.flags, c.exptime, c.nbytes, c.casunique, c.noreply, c.data)
}

type consumer func(command)

type parser struct {
	rd      io.Reader
	r       int
	w       int
	b       [4096]byte
	e       error
	c       int
	cmdChan chan *command
}

func (p *parser) String() string {
	return fmt.Sprintf("parser[r=%d, w=%d, e=%s, c=%d]", p.r, p.w, p.e, p.c)
}

func (p *parser) fill() {

	//log.Printf("fill: p=%s\n", p)

	if p.e != nil {
		//	log.Printf("fill: we will no read anymore because we got an error %s from reader\n", p.e)
		return
	}

	if p.available() > 0 && p.c < 1 {
		//	log.Printf("fill: we will not try to read now, there are %d available yet\n", p.available())
		p.c++
		return
	}

	if p.r > 0 {
		copy(p.b[0:], p.b[p.r:p.w])
		p.w -= p.r
		p.r = 0
	}

	//log.Println("fill: reading from reader")

	var n int
	n, p.e = p.rd.Read(p.b[p.w:])

	if n <= 0 {
		p.c++
	} else {
		p.c = 0
		p.w += n
	}

	//log.Printf("fill: n=%d, p=%s\n", n, p)
}

func (p *parser) available() int {
	return p.w - p.r
}

func pos(bytes []byte, value byte) int {
	for p, v := range bytes {
		if v == value {
			return p
		}
	}
	return -1
}

func (p *parser) readToken(delims ...byte) (string, error) {

	//log.Printf("rt: p=%s, delims=%s\n", p, delims)

	p.fill()
	if p.available() < 3 {
		//log.Println("rt: we will try to fill again because there are less than 3 bytes available")
		time.Sleep(100 * time.Millisecond)
		p.fill()
	}

	if p.available() >= 3 {
		var i int
		for i = p.r; i < p.w && pos(delims, p.b[i]) == -1; i++ {
			//log.Printf("rt: p.b[%d] = %d\n", i, p.b[i])
		}

		if i < p.w {
			defer func() {
				if p.b[i] == ' ' {
					p.r = i + 1
				} else {
					p.r = i
				}
			}()
			s := string(p.b[p.r:i])
			//log.Printf("rt: s=%s\n", s)
			return s, nil
		}
	}

	return "", errors.New("Token not found")
}

func (p *parser) readLineSeparator() (bool, error) {

	p.fill()
	if p.available() < 2 {
		log.Println("rls: we will try to fill again because there are less than 2 bytes available")
		time.Sleep(100 * time.Millisecond)
		p.fill()
	}

	if p.available() >= 2 {
		if string(p.b[p.r:p.r+2]) == "\r\n" {
			defer func() {
				p.r += 2
			}()
			return true, nil
		} else {
			return false, nil
		}
	}

	return false, errors.New("Line separator not found")
}

// We will have problem if size is too large
func (p *parser) readData(size int) (string, error) {

	p.fill()
	if p.available() < size {
		log.Printf("rd: we will try to fill again because there are less than %d bytes available\n", size)
		time.Sleep(100 * time.Millisecond)
		p.fill()
	}

	if p.available() >= size {
		defer func() {
			p.r += size
		}()
		return string(p.b[p.r : p.r+size]), nil
	}

	return "", fmt.Errorf("Data with size %d not found", size)
}

func (p *parser) buildStorageCommand(opcode string) bool {
	c := &command{opcode: opcode}

	if key, e := p.readToken(' '); e != nil {
		log.Println("bsc key: ", e)
		return p.finish()
	} else {
		c.key = []string{key}
		log.Println("bsc key: ", c)

		if flags, e := p.readToken(' '); e != nil {
			log.Println("bsc flags: ", e)
			return p.finish()
		} else {
			c.flags = flags
			log.Println("bsc flags: ", c)

			if exptime, e := p.readToken(' '); e != nil {
				log.Println("bsc exptime: ", e)
				return p.finish()
			} else {
				c.exptime = exptime

				if nbytes, e := p.readToken(' '); e != nil {
					log.Println("bsc exptime: ", e)
					return p.finish()
				} else {
					c.nbytes = nbytes
					log.Println("bsc nbytes: ", c)

					if c.opcode == "cas" {
						if casunique, e := p.readToken(' '); e != nil {
							log.Println("bsc cas: ", e)
							return p.finish()
						} else {
							c.casunique = casunique
							log.Println("bsc cas: ", c)
						}
					}

					if nl, e := p.readLineSeparator(); e != nil {
						log.Println("bsc nl1: ", e)
						return p.finish()
					} else {

						if !nl {
							if noreply, e := p.readToken('\r'); e != nil {
								log.Println("bsc noreply: ", e)
								return p.finish()
							} else {
								c.noreply = noreply
								log.Println("bsc noreply: ", c)
							}

							if nl, e := p.readLineSeparator(); e != nil {
								log.Println("bsc nlnoreply: ", e)
								return p.finish()
							} else if !nl {
								log.Println("bsc nlnoreply: expected lineseparator here")
								return p.finish()
							}
						}

						if inbytes, e := strconv.Atoi(c.nbytes); e != nil {
							log.Println("bsc inbytes: ", e)
							return p.finish()
						} else if data, e := p.readData(inbytes); e != nil {
							log.Println("bsc data: ", e)
							return p.finish()
						} else {
							c.data = data
							log.Println("bsc data: ", c)

							p.cmdChan <- c

							if nl, e := p.readLineSeparator(); e != nil {
								log.Println("bsc nl2: ", e)
								return p.finish()
							} else if !nl {
								return p.finish()
							} else {
								return true
							}
						}
					}
				}
			}
		}
	}
}

func (p *parser) buildRetrievalCommand(opcode string) bool {
	c := &command{opcode: opcode}

	var key []string

	for {
		if k, e := p.readToken(' ', '\r'); e != nil {
			log.Println("brc: ", e)
			return p.finish()
		} else {
			key = append(key, k)

			if nl, e := p.readLineSeparator(); e != nil {
				log.Println("brc: ", e)
				return p.finish()
			} else {
				if nl {
					c.key = key
					log.Println("brc: ", c)

					p.cmdChan <- c
					return true
				}
			}
		}
	}
}

func (p *parser) buildDeleteCommand(opcode string) bool {
	c := &command{opcode: opcode}

	if key, e := p.readToken(' '); e != nil {
		log.Println("bdc: ", e)
		return p.finish()
	} else {
		c.key = []string{key}
		log.Println("bdc: ", c)

		if nl, e := p.readLineSeparator(); e != nil {
			log.Println("bdc: ", e)
			return p.finish()
		} else if !nl {
			if noreply, e := p.readToken('\r'); e != nil {
				log.Println("bdc noreply: ", e)
				return p.finish()
			} else {
				c.noreply = noreply
				log.Println("bdc noreply: ", c)
			}

			if nl, e := p.readLineSeparator(); e != nil {
				log.Println("bdc nlnoreply: ", e)
				return p.finish()
			} else if !nl {
				log.Println("bdc nlnoreply: expected lineseparator here")
				return p.finish()
			} else {
				p.cmdChan <- c
				return true
			}
		} else {
			p.cmdChan <- c
			return true
		}
	}
}

func (p *parser) buildIncDecCommand(opcode string) bool {
	c := &command{opcode: opcode}

	if key, e := p.readToken(' '); e != nil {
		log.Println("bic: ", e)
		return p.finish()
	} else {
		c.key = []string{key}
		log.Println("bic: ", c)

		if value, e := p.readToken(' '); e != nil {
			log.Println("bic: ", e)
			return p.finish()
		} else {
			c.data = value
			log.Println("bic: ", c)

			if nl, e := p.readLineSeparator(); e != nil {
				log.Println("bic: ", e)
				return p.finish()
			} else if !nl {
				if noreply, e := p.readToken('\r'); e != nil {
					log.Println("bic noreply: ", e)
					return p.finish()
				} else {
					c.noreply = noreply
					log.Println("bic noreply: ", c)
				}

				if nl, e := p.readLineSeparator(); e != nil {
					log.Println("bic nlnoreply: ", e)
					return p.finish()
				} else if !nl {
					log.Println("bic nlnoreply: expected lineseparator here")
					return p.finish()
				} else {
					p.cmdChan <- c
					return true
				}
			} else {
				p.cmdChan <- c
				return true
			}
		}
	}
}

func (p *parser) buildTouchCommand(opcode string) bool {

	c := &command{opcode: opcode}

	if key, e := p.readToken(' '); e != nil {
		log.Println("btc: ", e)
		return p.finish()
	} else {
		c.key = []string{key}
		log.Println("btc: ", c)

		if exptime, e := p.readToken(' '); e != nil {
			log.Println("btc: ", e)
			return p.finish()
		} else {
			c.exptime = exptime
			log.Println("btc: ", c)

			if nl, e := p.readLineSeparator(); e != nil {
				log.Println("btc: ", e)
				return p.finish()
			} else if !nl {
				if noreply, e := p.readToken('\r'); e != nil {
					log.Println("btc noreply: ", e)
					return p.finish()
				} else {
					c.noreply = noreply
					log.Println("btc noreply: ", c)
				}

				if nl, e := p.readLineSeparator(); e != nil {
					log.Println("btc nlnoreply: ", e)
					return p.finish()
				} else if !nl {
					log.Println("btc nlnoreply: expected lineseparator here")
					return p.finish()
				} else {
					p.cmdChan <- c
					return true
				}
			} else {
				p.cmdChan <- c
				return true
			}
		}
	}
}

func (p *parser) finish() bool {
	close(p.cmdChan)
	return false
}

func (p *parser) parse() {

	for {
		t, e := p.readToken(' ')
		log.Printf("parse: t=%s, e=%s\n", t, e)
		if e != nil {
			log.Println("parse: ", e)
			p.finish()
			break
		}

		if t == "set" || t == "add" || t == "replace" || t == "append" || t == "prepend" || t == "cas" {
			if !p.buildStorageCommand(t) {
				break
			}
		} else if t == "get" || t == "gets" {
			if !p.buildRetrievalCommand(t) {
				break
			}
		} else if t == "delete" {
			if !p.buildDeleteCommand(t) {
				break
			}
		} else if t == "incr" || t == "decr" {
			if !p.buildIncDecCommand(t) {
				break
			}
		} else if t == "touch" {
			if !p.buildTouchCommand(t) {
				break
			}
		} else {
			log.Println("parse: Token not recognized")
			p.finish()
			break
		}
	}
}

func (p *parser) next() *command {
	return <-p.cmdChan
}

func NewParser(rd io.Reader) *parser {
	ch := make(chan *command)
	p := &parser{rd: rd, cmdChan: ch}

	go p.parse()

	return p
}

func main() {
	log.SetFlags(log.Lmicroseconds)
	log.Println("main: Starting...")
	//reader := bufio.NewReader(os.Stdin)

	reader2 := strings.NewReader("set abcde 1 2 3 \r\nasd\r\nget abcbbc77&#*#&*8\r\nadd 7188sh 332 1000 10 \r\nabc123ert6\r\ngets aaa bbb 234f abababab\r\ncas abc 1 2 3 zxc \r\nvbn\r\nappend a 1 2 3 noreply\r\npoi\r\ndelete kovalsky noreply\r\ndecr oque 23 \r\nincr oque 99 noreply\r\ntouch maque 1789828 \r\n")

	//p := NewParser(reader)
	p := NewParser(reader2)
	log.Printf("main: p=%s\n", p)

	for c := p.next(); c != nil; c = p.next() {
		log.Printf("main: c=%s\n", c)
	}

}

func handleConnection(conn net.Conn) {
	p := NewParser(conn)

	log.Printf("Serving %q\n", conn)

	for c := p.next(); c != nil; c = p.next() {
		log.Printf("hc: c=%s\n", c)
		fmt.Fprintf(conn, "%s", c)
	}

	log.Printf("Stop serving %q\n", conn)
}

func oldmain() {
	log.SetFlags(log.Lmicroseconds)
	log.Println("main: Starting...")
	ln, err := net.Listen("tcp", ":1234")
	if err != nil {
		panic(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("Err: ", err)
		} else {
			go handleConnection(conn)
		}
	}
}
