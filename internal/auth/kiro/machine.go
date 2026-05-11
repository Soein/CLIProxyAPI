package kiro

import (
	"crypto/sha256"
	"encoding/hex"
)

// MachineID derives a stable per-credential device fingerprint, matching
// AIClient2API's behavior. SHA-256(hex) of the first non-empty among:
//
//	uuid > profileArn > clientId > MachineIDFallback
//
// The value is recomputed at startup from the loaded credential and never
// persisted to a separate device_id file.
func MachineID(c *Credentials) string {
	src := MachineIDFallback
	switch {
	case c == nil:
		// fall through with fallback
	case c.UUID != "":
		src = c.UUID
	case c.ProfileArn != "":
		src = c.ProfileArn
	case c.ClientID != "":
		src = c.ClientID
	}
	sum := sha256.Sum256([]byte(src))
	return hex.EncodeToString(sum[:])
}
