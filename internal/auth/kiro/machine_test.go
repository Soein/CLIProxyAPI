package kiro

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func TestMachineIDPriority(t *testing.T) {
	cases := []struct {
		name string
		c    *Credentials
		want string
	}{
		{"uuid wins", &Credentials{UUID: "u1", ProfileArn: "p", ClientID: "c"}, sha256Hex("u1")},
		{"profileArn when no uuid", &Credentials{ProfileArn: "p1", ClientID: "c"}, sha256Hex("p1")},
		{"clientId when no uuid/profileArn", &Credentials{ClientID: "c1"}, sha256Hex("c1")},
		{"fallback when nothing set", &Credentials{}, sha256Hex(MachineIDFallback)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := MachineID(tc.c)
			if got != tc.want {
				t.Errorf("MachineID = %s; want %s", got, tc.want)
			}
		})
	}
}

func TestMachineIDStable(t *testing.T) {
	c := &Credentials{UUID: "stable"}
	a := MachineID(c)
	b := MachineID(c)
	if a != b {
		t.Errorf("MachineID is not deterministic: %s vs %s", a, b)
	}
	if len(a) != 64 {
		t.Errorf("MachineID length = %d; want 64 (hex sha256)", len(a))
	}
}
