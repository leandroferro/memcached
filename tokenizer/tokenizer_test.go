package tokenizer

import (
	"testing"
)

func TestSimpleNext(t *testing.T) {

	expected := []string{
		"abc",
		"def",
		"ghi",
	}

	tok := New([]byte("abc def ghi"))

	for _, e := range expected {
		got := tok.Next()
		if e != got {
			t.Errorf("got %q, want %q", got, e)
		}
	}
}

func TestSimpleFixedNext(t *testing.T) {
	expected := []struct {
		n    int
		want string
	}{
		{5, "abc d"},
		{2, "ef"},
		{6, " ghi"},
		{10, ""},
		{5, ""},
	}

	tok := New([]byte("abc def ghi"))

	for _, e := range expected {
		got := tok.FixedNext(e.n)
		if e.want != got {
			t.Errorf("got %q, want %q", got, e.want)
		}
	}
}

func TestLittleMoreComplesNext(t *testing.T) {

	expected := []string{
		"abc",
		"def",
		"ghi",
		"",
		"",
	}

	tok := New([]byte(" abc  def   ghi    "))

	for _, e := range expected {
		got := tok.Next()
		if e != got {
			t.Errorf("got %q, want %q", got, e)
		}
	}
}
