package kiro

import (
	"encoding/json"
	"fmt"
)

// mustJSON marshals to JSON; panics only if the input contains a type that
// json cannot encode. All call sites in this package use map[string]any
// or map[string]string with primitive values, so the panic path is unreachable.
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("kiro: unexpected json marshal failure: %v", err))
	}
	return b
}
