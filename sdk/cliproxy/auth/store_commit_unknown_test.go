package auth

import (
	"errors"
	"fmt"
	"testing"
)

func TestAuthStoreCommitCandidateGenerationJoinedErrorsMatchExactID(t *testing.T) {
	errCommit := errors.Join(
		NewAuthStoreCommitUnknown(map[string]uint64{"first.json": 3}, errors.New("first commit unknown")),
		NewAuthStoreCommitUnknown(map[string]uint64{"second.json": 9}, errors.New("second commit unknown")),
	)

	tests := []struct {
		name           string
		id             string
		wantGeneration uint64
		wantOK         bool
	}{
		{name: "first ID", id: "first.json", wantGeneration: 3, wantOK: true},
		{name: "second ID", id: "second.json", wantGeneration: 9, wantOK: true},
		{name: "normalized ID", id: "  second.json\t", wantGeneration: 9, wantOK: true},
		{name: "unrelated ID", id: "other.json"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			generation, ok := AuthStoreCommitCandidateGeneration(errCommit, tc.id)
			if generation != tc.wantGeneration || ok != tc.wantOK {
				t.Fatalf("AuthStoreCommitCandidateGeneration() = (%d, %v), want (%d, %v)", generation, ok, tc.wantGeneration, tc.wantOK)
			}
		})
	}
}

func TestAuthStoreCommitCandidateGenerationTraversesNestedErrorTree(t *testing.T) {
	errCommit := fmt.Errorf("outer operation: %w", errors.Join(
		errors.New("unrelated failure"),
		fmt.Errorf("nested operation: %w", NewAuthStoreCommitUnknown(
			map[string]uint64{"nested.json": 17},
			errors.New("commit verification unavailable"),
		)),
	))

	generation, ok := AuthStoreCommitCandidateGeneration(errCommit, "nested.json")
	if !ok || generation != 17 {
		t.Fatalf("AuthStoreCommitCandidateGeneration() = (%d, %v), want (17, true)", generation, ok)
	}
}
