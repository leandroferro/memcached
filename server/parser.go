package main

// http://golang.org/ref/spec#Iota

type CommandOpcode int
type CommandKey []byte

const (
	INVALID CommandOpcode = iota
	SET
	GET
)

type Command struct {
	Opcode CommandOpcode
	Key    CommandKey
}

var mappings = []struct {
	key    string
	opcode CommandOpcode
}{
	{"set", SET},
	{"get", GET},
}

func Parse(line string) Command {
	for _, mapping := range mappings {
		if line == mapping.key {
			return Command{mapping.opcode, nil}
		}
	}
	return Command{INVALID, nil}
}
