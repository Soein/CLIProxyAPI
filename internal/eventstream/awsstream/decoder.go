package awsstream

import (
	"encoding/binary"
	"errors"
	"io"
)

// Decoder reads AWS event-stream frames from an underlying io.Reader.
// Each call to ReadFrame returns one decoded frame or io.EOF when the
// stream is cleanly exhausted.
type Decoder struct {
	r io.Reader
}

// NewDecoder constructs a Decoder around r.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

// ReadFrame returns the next decoded frame. Returns io.EOF when the reader
// is empty at a frame boundary, or io.ErrUnexpectedEOF when the stream ends
// mid-frame.
func (d *Decoder) ReadFrame() (*Frame, error) {
	// Read prelude.
	var preludeBuf [PreludeSize]byte
	n, err := io.ReadFull(d.r, preludeBuf[:])
	if errors.Is(err, io.EOF) && n == 0 {
		return nil, io.EOF
	}
	if err != nil {
		return nil, newError("read prelude", err)
	}

	p, err := parsePrelude(preludeBuf)
	if err != nil {
		return nil, err
	}

	// Read remainder = headers + payload + message CRC.
	remaining := int(p.TotalLen) - PreludeSize
	tail := make([]byte, remaining)
	if _, err := io.ReadFull(d.r, tail); err != nil {
		return nil, newError("read frame body", err)
	}

	// Validate message CRC.
	wantMsgCRC := binary.BigEndian.Uint32(tail[remaining-MessageCRCSize:])
	// The CRC is computed over the full frame minus the trailing 4 CRC bytes.
	full := make([]byte, 0, int(p.TotalLen)-MessageCRCSize)
	full = append(full, preludeBuf[:]...)
	full = append(full, tail[:remaining-MessageCRCSize]...)
	gotMsgCRC := crcOf(full)
	if wantMsgCRC != gotMsgCRC {
		return nil, newError("message crc mismatch", nil)
	}

	// Slice out headers and payload.
	headersBytes := tail[:p.HeadersLen]
	payload := tail[p.HeadersLen : p.HeadersLen+p.PayloadLen]

	hdrs, err := parseHeaders(headersBytes)
	if err != nil {
		return nil, err
	}

	return &Frame{
		Headers: hdrs,
		Payload: append([]byte(nil), payload...), // copy to avoid aliasing
	}, nil
}
