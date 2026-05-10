package awsstream

import (
	"encoding/binary"
	"testing"
)

func makePrelude(totalLen, headersLen uint32) [PreludeSize]byte {
	var buf [PreludeSize]byte
	binary.BigEndian.PutUint32(buf[0:4], totalLen)
	binary.BigEndian.PutUint32(buf[4:8], headersLen)
	binary.BigEndian.PutUint32(buf[8:12], crcOf(buf[0:8]))
	return buf
}

func makeBadPrelude(totalLen, headersLen, badCRC uint32) [PreludeSize]byte {
	var buf [PreludeSize]byte
	binary.BigEndian.PutUint32(buf[0:4], totalLen)
	binary.BigEndian.PutUint32(buf[4:8], headersLen)
	binary.BigEndian.PutUint32(buf[8:12], badCRC)
	return buf
}

func TestParsePreludeOK(t *testing.T) {
	buf := makePrelude(60, 30)
	p, err := parsePrelude(buf)
	if err != nil {
		t.Fatal(err)
	}
	if p.TotalLen != 60 || p.HeadersLen != 30 {
		t.Errorf("got %+v; want TotalLen=60 HeadersLen=30", p)
	}
}

func TestParsePreludeCRCMismatch(t *testing.T) {
	buf := makeBadPrelude(60, 30, 0xDEADBEEF)
	_, err := parsePrelude(buf)
	if err == nil {
		t.Fatal("expected CRC mismatch error, got nil")
	}
}

func TestParsePreludeTotalTooSmall(t *testing.T) {
	// Minimum frame size = 12 prelude + 0 headers + 0 payload + 4 message CRC = 16
	// total < 16 must fail.
	buf := makePrelude(8, 0)
	_, err := parsePrelude(buf)
	if err == nil {
		t.Fatal("expected too-small error, got nil")
	}
}

func TestParsePreludeHeadersOverflow(t *testing.T) {
	// headers_len > total_len - 16 (prelude + message CRC) must fail.
	buf := makePrelude(20, 100)
	_, err := parsePrelude(buf)
	if err == nil {
		t.Fatal("expected headers overflow error, got nil")
	}
}

func TestParsePreludeMaxFrameSize(t *testing.T) {
	buf := makePrelude(uint32(MaxFrameSize)+1, 0)
	_, err := parsePrelude(buf)
	if err == nil {
		t.Fatal("expected max-frame-size error, got nil")
	}
}
