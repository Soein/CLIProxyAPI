package awsstream

import "hash/crc32"

// crcTable is the IEEE polynomial table used by the AWS event-stream format.
var crcTable = crc32.IEEETable

// crcOf returns the CRC32 IEEE checksum of b.
func crcOf(b []byte) uint32 {
	return crc32.Checksum(b, crcTable)
}
