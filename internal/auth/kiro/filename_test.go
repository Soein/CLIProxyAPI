package kiro

import "testing"

func TestCredentialFileName(t *testing.T) {
	cases := []struct {
		name string
		id   string
		want string
	}{
		{"empty id falls back to default", "", "kiro.json"},
		{"plain alphanumeric", "abc123", "kiro-abc123.json"},
		{"strips whitespace", "  abc  ", "kiro-abc.json"},
		{"sanitizes path separators", "a/b\\c", "kiro-a_b_c.json"},
		{"sanitizes upper case is preserved", "AbC", "kiro-AbC.json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CredentialFileName(tc.id); got != tc.want {
				t.Errorf("CredentialFileName(%q) = %q; want %q", tc.id, got, tc.want)
			}
		})
	}
}
