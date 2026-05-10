package awsstream

import (
	"encoding/binary"
	"fmt"
)

// parseHeaders decodes the headers section of a frame. Supports a subset of
// AWS event-stream header types (BoolTrue, BoolFalse, Byte, Int16, Int32,
// Int64, ByteArray, String, Timestamp, UUID). Unknown types return an error
// rather than silently advancing the cursor by an unknown amount.
func parseHeaders(buf []byte) ([]Header, error) {
	var out []Header
	for i := 0; i < len(buf); {
		// name_len (1B)
		if i+1 > len(buf) {
			return nil, newError("header truncated at name length", nil)
		}
		nameLen := int(buf[i])
		i++

		// name
		if i+nameLen > len(buf) {
			return nil, newError("header truncated at name", nil)
		}
		name := string(buf[i : i+nameLen])
		i += nameLen

		// type (1B)
		if i+1 > len(buf) {
			return nil, newError("header truncated at type", nil)
		}
		valueType := HeaderValueType(buf[i])
		i++

		// value
		var value []byte
		switch valueType {
		case HeaderValueTypeBoolTrue, HeaderValueTypeBoolFalse:
			// no value bytes
		case HeaderValueTypeByte:
			if i+1 > len(buf) {
				return nil, newError("header truncated at byte value", nil)
			}
			value = buf[i : i+1]
			i++
		case HeaderValueTypeInt16:
			if i+2 > len(buf) {
				return nil, newError("header truncated at int16 value", nil)
			}
			value = buf[i : i+2]
			i += 2
		case HeaderValueTypeInt32:
			if i+4 > len(buf) {
				return nil, newError("header truncated at int32 value", nil)
			}
			value = buf[i : i+4]
			i += 4
		case HeaderValueTypeInt64, HeaderValueTypeTimestamp:
			if i+8 > len(buf) {
				return nil, newError("header truncated at int64/timestamp value", nil)
			}
			value = buf[i : i+8]
			i += 8
		case HeaderValueTypeUUID:
			if i+16 > len(buf) {
				return nil, newError("header truncated at uuid value", nil)
			}
			value = buf[i : i+16]
			i += 16
		case HeaderValueTypeByteArray, HeaderValueTypeString:
			if i+2 > len(buf) {
				return nil, newError("header truncated at value length", nil)
			}
			vLen := int(binary.BigEndian.Uint16(buf[i : i+2]))
			i += 2
			if i+vLen > len(buf) {
				return nil, newError("header truncated at value bytes", nil)
			}
			value = buf[i : i+vLen]
			i += vLen
		default:
			return nil, newError(fmt.Sprintf("unsupported header type %d", byte(valueType)), nil)
		}

		out = append(out, Header{
			Name:  name,
			Type:  valueType,
			Value: append([]byte(nil), value...), // copy to avoid backing array aliasing
		})
	}
	return out, nil
}
