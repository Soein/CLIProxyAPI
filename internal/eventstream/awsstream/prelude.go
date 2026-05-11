package awsstream

import "encoding/binary"

// prelude is the parsed 12-byte frame prelude.
type prelude struct {
	TotalLen   uint32 // entire frame including itself
	HeadersLen uint32 // length of headers section
	PayloadLen uint32 // total - prelude - headers - message CRC
}

// parsePrelude validates the 12-byte prelude (CRC + size sanity) and returns
// the structured view.
func parsePrelude(buf [PreludeSize]byte) (prelude, error) {
	totalLen := binary.BigEndian.Uint32(buf[0:4])
	headersLen := binary.BigEndian.Uint32(buf[4:8])
	gotCRC := binary.BigEndian.Uint32(buf[8:12])

	wantCRC := crcOf(buf[0:8])
	if gotCRC != wantCRC {
		return prelude{}, newError("prelude crc mismatch", nil)
	}

	// Minimum valid frame: prelude(12) + 0 headers + 0 payload + msg crc(4) = 16
	if totalLen < PreludeSize+MessageCRCSize {
		return prelude{}, newError("frame too small", nil)
	}
	if totalLen > MaxFrameSize {
		return prelude{}, newError("frame exceeds max size", nil)
	}
	// headers must fit within total - prelude - message CRC
	if int64(headersLen) > int64(totalLen)-PreludeSize-MessageCRCSize {
		return prelude{}, newError("headers length overflows frame", nil)
	}

	payloadLen := totalLen - PreludeSize - headersLen - MessageCRCSize
	return prelude{
		TotalLen:   totalLen,
		HeadersLen: headersLen,
		PayloadLen: payloadLen,
	}, nil
}
