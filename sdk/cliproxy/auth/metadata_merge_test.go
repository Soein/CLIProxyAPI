package auth

import "testing"

func TestMergeExistingAuthMetadataSkipsProviderOwnedIdentity(t *testing.T) {
	target := &Auth{
		Metadata: map[string]any{
			"email": "new@example.com",
		},
	}
	existing := map[string]any{
		"account_id":   "old-account",
		"prefix":       "custom-prefix",
		"access_token": "old-token",
	}

	MergeExistingAuthMetadata(target, existing)

	if _, ok := target.Metadata["account_id"]; ok {
		t.Fatalf("account_id was merged from existing metadata: %#v", target.Metadata)
	}
	if got := target.Metadata["prefix"]; got != "custom-prefix" {
		t.Fatalf("prefix = %v, want custom-prefix", got)
	}
	if _, ok := target.Metadata["access_token"]; ok {
		t.Fatalf("access_token was merged from existing metadata: %#v", target.Metadata)
	}
}
