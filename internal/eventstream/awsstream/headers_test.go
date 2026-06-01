package awsstream

import (
	"encoding/binary"
	"testing"
)

// buildStringHeader produces the wire bytes for one string-typed header.
//
//	name_len(1) | name | type(1=7) | val_len(2 BE) | val
func buildStringHeader(name, value string) []byte {
	out := make([]byte, 0, 1+len(name)+1+2+len(value))
	out = append(out, byte(len(name)))
	out = append(out, name...)
	out = append(out, byte(HeaderValueTypeString))
	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], uint16(len(value)))
	out = append(out, lenBuf[:]...)
	out = append(out, value...)
	return out
}

func TestParseHeadersTwoStrings(t *testing.T) {
	buf := append(buildStringHeader(":event-type", "contentBlock"),
		buildStringHeader(":content-type", "application/json")...)
	got, err := parseHeaders(buf)
	if err != nil {
		t.Fatalf("parseHeaders: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 headers, got %d", len(got))
	}
	if got[0].Name != ":event-type" || string(got[0].Value) != "contentBlock" {
		t.Errorf("header 0 = %+v", got[0])
	}
	if got[1].Name != ":content-type" || string(got[1].Value) != "application/json" {
		t.Errorf("header 1 = %+v", got[1])
	}
}

func TestParseHeadersEmpty(t *testing.T) {
	got, err := parseHeaders(nil)
	if err != nil {
		t.Fatalf("parseHeaders([]) should be empty, got err: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 headers, got %d", len(got))
	}
}

func TestParseHeadersTruncated(t *testing.T) {
	// Truncated mid-name.
	buf := []byte{5, 'a', 'b'} // claims name_len=5 but only provides 2 bytes
	_, err := parseHeaders(buf)
	if err == nil {
		t.Fatal("expected truncation error, got nil")
	}
}

func TestParseHeadersUnsupportedTypeMarked(t *testing.T) {
	// Header with type 99 (unknown). The parser MUST set Unsupported=true and
	// skip remaining bytes safely (without panic). Since we don't know the
	// length, we treat any unsupported type as fatal — return an error.
	buf := []byte{1, 'x', 99}
	_, err := parseHeaders(buf)
	if err == nil {
		t.Fatal("expected unsupported-type error, got nil")
	}
}

func TestParseHeadersBoolTypes(t *testing.T) {
	// type 0 = BoolTrue (no value bytes), type 1 = BoolFalse (no value bytes).
	buf := []byte{1, 'a', 0, 1, 'b', 1}
	got, err := parseHeaders(buf)
	if err != nil {
		t.Fatalf("parseHeaders bool types: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 headers, got %d", len(got))
	}
	if got[0].Type != HeaderValueTypeBoolTrue || got[1].Type != HeaderValueTypeBoolFalse {
		t.Errorf("got types: %v %v", got[0].Type, got[1].Type)
	}
}
