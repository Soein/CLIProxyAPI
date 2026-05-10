// Package awsstream parses the AWS event-stream binary frame format used by
// Kiro / Amazon Q backends (and other AWS streaming services). The package
// has no external dependencies beyond the Go standard library.
package awsstream

import "fmt"

// HeaderValueType corresponds to the 1-byte type tag in an event-stream header.
type HeaderValueType uint8

const (
	HeaderValueTypeBoolTrue  HeaderValueType = 0
	HeaderValueTypeBoolFalse HeaderValueType = 1
	HeaderValueTypeByte      HeaderValueType = 2
	HeaderValueTypeInt16     HeaderValueType = 3
	HeaderValueTypeInt32     HeaderValueType = 4
	HeaderValueTypeInt64     HeaderValueType = 5
	HeaderValueTypeByteArray HeaderValueType = 6
	HeaderValueTypeString    HeaderValueType = 7
	HeaderValueTypeTimestamp HeaderValueType = 8
	HeaderValueTypeUUID      HeaderValueType = 9
)

// String renders the type for diagnostics.
func (t HeaderValueType) String() string {
	switch t {
	case HeaderValueTypeBoolTrue:
		return "bool_true"
	case HeaderValueTypeBoolFalse:
		return "bool_false"
	case HeaderValueTypeByte:
		return "byte"
	case HeaderValueTypeInt16:
		return "int16"
	case HeaderValueTypeInt32:
		return "int32"
	case HeaderValueTypeInt64:
		return "int64"
	case HeaderValueTypeByteArray:
		return "byte_array"
	case HeaderValueTypeString:
		return "string"
	case HeaderValueTypeTimestamp:
		return "timestamp"
	case HeaderValueTypeUUID:
		return "uuid"
	default:
		return "unknown"
	}
}

// Header is one entry in a frame's headers section. Value is the raw bytes;
// callers should interpret based on Type. For unknown types Unsupported is set.
type Header struct {
	Name        string
	Type        HeaderValueType
	Value       []byte
	Unsupported bool
}

// Frame represents one decoded event-stream message: headers + payload.
type Frame struct {
	Headers []Header
	Payload []byte
}

// StringHeader returns the value of a string-typed header by name.
// Returns (value, true) on success; ("", false) if the header is missing
// or not a string type.
func (f *Frame) StringHeader(name string) (string, bool) {
	for _, h := range f.Headers {
		if h.Name == name && h.Type == HeaderValueTypeString {
			return string(h.Value), true
		}
	}
	return "", false
}

// Wire-level constants. Exported so callers (and tests) can reference them.
const (
	PreludeSize    = 12 // total_len(4) + headers_len(4) + prelude_crc(4)
	MessageCRCSize = 4
	MaxFrameSize   = 16 << 20 // 16 MiB hard cap; AWS spec is 16 MiB.
)

// frameError wraps a decoder failure with structured info for tests / logs.
type frameError struct {
	Reason string
	Inner  error
}

func (e *frameError) Error() string {
	if e.Inner != nil {
		return fmt.Sprintf("eventstream: %s: %v", e.Reason, e.Inner)
	}
	return "eventstream: " + e.Reason
}

func (e *frameError) Unwrap() error { return e.Inner }

func newError(reason string, inner error) error {
	return &frameError{Reason: reason, Inner: inner}
}
