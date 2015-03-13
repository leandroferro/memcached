package tokenizer

import (
	"log"
)

type Tokenizer interface {
	Next() string
	FixedNext(n int) string
}

type byteArrayTokenizer struct {
	pos  int
	base []byte
}

func (t *byteArrayTokenizer) Next() string {
	for k := t.pos; k < len(t.base) && t.base[k] == ' '; k++ {
		t.pos = k + 1
	}

	if t.pos >= len(t.base) {
		return ""
	}

	i := t.findNextSpace()

	defer func() {
		t.pos = i + 1
	}()
	return string(t.base[t.pos:i])
}

func (t *byteArrayTokenizer) FixedNext(n int) string {
	log.Println("t.pos", t.pos, ", n", n)

	if t.pos+n >= len(t.base) {
		n -= t.pos + n - len(t.base)
	}

	defer func() {
		t.pos = t.pos + n
	}()

	return string(t.base[t.pos : t.pos+n])
}

func (t *byteArrayTokenizer) findNextSpace() int {

	for i, v := range t.base[t.pos:] {
		if v == ' ' {
			return t.pos + i
		}
	}

	return len(t.base)
}

func New(base []byte) Tokenizer {
	return &byteArrayTokenizer{0, base}
}
