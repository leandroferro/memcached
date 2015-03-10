package main

import (
	"reflect"
	"testing"
)

func TestSimpleParse(t *testing.T) {
	cases := []struct {
		in  string
		out Command
	}{
		{"set", Command{SET, nil}},
		{"get", Command{GET, nil}},
		{"Set", Command{INVALID, nil}},
		{"invalid", Command{INVALID, nil}},
	}
	for _, c := range cases {
		got := Parse(c.in)
		if !reflect.DeepEqual(got, c.out) {
			t.Errorf("Parse(%q) == %q, want %q", c.in, got, c.out)
		}
	}
}
