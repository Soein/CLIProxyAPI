package kiro

import (
	"fmt"
	"strings"
)

// CredentialFileName returns the filename used to persist Kiro credentials.
// The id argument is typically the account email or a derived identifier.
// Path separators are replaced with underscores to keep the file flat.
func CredentialFileName(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return "kiro.json"
	}
	id = strings.NewReplacer("/", "_", "\\", "_").Replace(id)
	return fmt.Sprintf("kiro-%s.json", id)
}
