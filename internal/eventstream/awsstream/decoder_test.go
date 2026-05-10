package awsstream

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// makeFrame builds a complete event-stream frame from headers payload.
// Headers section is the raw bytes from buildStringHeader concatenated.
func makeFrame(headersBytes, payload []byte) []byte {
	totalLen := uint32(PreludeSize + len(headersBytes) + len(payload) + MessageCRCSize)
	headersLen := uint32(len(headersBytes))

	frame := make([]byte, 0, totalLen)
	// prelude
	var preludeBuf [PreludeSize]byte
	binary.BigEndian.PutUint32(preludeBuf[0:4], totalLen)
	binary.BigEndian.PutUint32(preludeBuf[4:8], headersLen)
	binary.BigEndian.PutUint32(preludeBuf[8:12], crcOf(preludeBuf[0:8]))
	frame = append(frame, preludeBuf[:]...)
	// headers
	frame = append(frame, headersBytes...)
	// payload
	frame = append(frame, payload...)
	// message CRC over [0, totalLen-4)
	msgCRC := crcOf(frame[:totalLen-MessageCRCSize])
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], msgCRC)
	frame = append(frame, crcBuf[:]...)
	return frame
}

func TestDecoderSingleFrame(t *testing.T) {
	headers := buildStringHeader(":event-type", "contentBlock")
	payload := []byte(`{"text":"hello"}`)
	wire := makeFrame(headers, payload)

	d := NewDecoder(bytes.NewReader(wire))
	f, err := d.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if et, _ := f.StringHeader(":event-type"); et != "contentBlock" {
		t.Errorf("event-type = %q; want contentBlock", et)
	}
	if string(f.Payload) != string(payload) {
		t.Errorf("payload mismatch: got %q want %q", f.Payload, payload)
	}

	// Second read should return io.EOF.
	_, err = d.ReadFrame()
	if !errors.Is(err, io.EOF) {
		t.Errorf("second ReadFrame should be io.EOF, got %v", err)
	}
}

func TestDecoderTwoFrames(t *testing.T) {
	a := makeFrame(buildStringHeader(":event-type", "first"), []byte(`{"n":1}`))
	b := makeFrame(buildStringHeader(":event-type", "second"), []byte(`{"n":2}`))
	wire := append(a, b...)

	d := NewDecoder(bytes.NewReader(wire))
	got := []string{}
	for {
		f, err := d.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		et, _ := f.StringHeader(":event-type")
		got = append(got, et)
	}
	if len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Errorf("got events %v; want [first second]", got)
	}
}

func TestDecoderMessageCRCMismatch(t *testing.T) {
	wire := makeFrame(buildStringHeader(":a", "b"), []byte(`{}`))
	// flip a payload byte.
	wire[len(wire)-MessageCRCSize-1] ^= 0xff

	d := NewDecoder(bytes.NewReader(wire))
	_, err := d.ReadFrame()
	if err == nil {
		t.Fatal("expected message CRC mismatch error, got nil")
	}
}

func TestDecoderTruncatedFrame(t *testing.T) {
	wire := makeFrame(buildStringHeader(":a", "b"), []byte(`{}`))
	// Truncate.
	wire = wire[:len(wire)-3]

	d := NewDecoder(bytes.NewReader(wire))
	_, err := d.ReadFrame()
	if err == nil {
		t.Fatal("expected truncation error, got nil")
	}
}
