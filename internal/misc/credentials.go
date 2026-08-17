package misc

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
)

// Separator used to visually group related log lines.
var credentialSeparator = strings.Repeat("-", 67)

// LogSavingCredentials emits a consistent log message when persisting auth material.
func LogSavingCredentials(path string) {
	if path == "" {
		return
	}
	// Use filepath.Clean so logs remain stable even if callers pass redundant separators.
	fmt.Printf("Saving credentials to %s\n", filepath.Clean(path))
}

// LogCredentialSeparator adds a visual separator to group auth/key processing logs.
func LogCredentialSeparator() {
	log.Debug(credentialSeparator)
}

// MergeMetadata serializes source into a map and adds metadata fields that are
// not owned by the source payload. Source fields remain authoritative so stale
// metadata cannot replace newly acquired credential or identity values.
func MergeMetadata(source any, metadata map[string]any) (map[string]any, error) {
	data := make(map[string]any, len(metadata))
	for k, v := range metadata {
		data[k] = v
	}

	var sourceData map[string]any
	if srcMap, ok := source.(map[string]any); ok {
		sourceData = make(map[string]any, len(srcMap))
		for k, v := range srcMap {
			sourceData[k] = v
		}
	} else if source != nil {
		temp, errMarshal := json.Marshal(source)
		if errMarshal != nil {
			return nil, fmt.Errorf("failed to marshal source: %w", errMarshal)
		}
		if errUnmarshal := json.Unmarshal(temp, &sourceData); errUnmarshal != nil {
			return nil, fmt.Errorf("failed to unmarshal to map: %w", errUnmarshal)
		}
	}

	for k, v := range sourceData {
		data[k] = v
	}

	return data, nil
}
