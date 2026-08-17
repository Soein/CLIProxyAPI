package auth

import (
	"encoding/json"
	"testing"
)

type metadataMergeStorage struct {
	ProviderIdentity string `json:"provider_identity"`
	OptionalIdentity string `json:"optional_identity,omitempty"`
	metadata         map[string]any
}

func (*metadataMergeStorage) SaveTokenToFile(string) error { return nil }

func (s *metadataMergeStorage) SetMetadata(metadata map[string]any) {
	s.metadata = metadata
}

type metadataMergeRawStorage struct {
	payload  map[string]any
	metadata map[string]any
}

func (*metadataMergeRawStorage) SaveTokenToFile(string) error { return nil }

func (s *metadataMergeRawStorage) RawJSON() []byte {
	raw, _ := json.Marshal(s.payload)
	return raw
}

func (s *metadataMergeRawStorage) SetMetadata(metadata map[string]any) {
	s.metadata = metadata
}

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

func TestMergeExistingAuthMetadataSkipsStorageOwnedFields(t *testing.T) {
	storage := &metadataMergeStorage{ProviderIdentity: "new-identity"}
	target := &Auth{
		Storage:  storage,
		Metadata: map[string]any{"email": "new@example.com"},
	}
	existing := map[string]any{
		"provider_identity": "old-identity",
		"optional_identity": "old-optional-identity",
		"prefix":            "custom-prefix",
	}

	MergeExistingAuthMetadata(target, existing)

	if _, ok := target.Metadata["provider_identity"]; ok {
		t.Fatalf("storage-owned identity was merged from existing metadata: %#v", target.Metadata)
	}
	if _, ok := target.Metadata["optional_identity"]; ok {
		t.Fatalf("omitted storage-owned identity was merged from existing metadata: %#v", target.Metadata)
	}
	if got := target.Metadata["prefix"]; got != "custom-prefix" {
		t.Fatalf("prefix = %v, want custom-prefix", got)
	}
	if got := storage.metadata["prefix"]; got != "custom-prefix" {
		t.Fatalf("storage metadata prefix = %v, want custom-prefix", got)
	}
}

func TestMergeExistingAuthMetadataSkipsRawStorageOwnedFields(t *testing.T) {
	storage := &metadataMergeRawStorage{payload: map[string]any{
		"provider_identity": "new-identity",
		"access_token":      "new-token",
	}}
	target := &Auth{
		Storage:  storage,
		Metadata: map[string]any{"type": "plugin"},
	}
	existing := map[string]any{
		"provider_identity": "old-identity",
		"access_token":      "old-token",
		"custom_extension":  map[string]any{"enabled": true},
	}

	MergeExistingAuthMetadata(target, existing)

	if _, ok := target.Metadata["provider_identity"]; ok {
		t.Fatalf("raw storage-owned identity was merged from existing metadata: %#v", target.Metadata)
	}
	if _, ok := target.Metadata["access_token"]; ok {
		t.Fatalf("stale access token was merged from existing metadata: %#v", target.Metadata)
	}
	if got, ok := target.Metadata["custom_extension"].(map[string]any); !ok || got["enabled"] != true {
		t.Fatalf("custom_extension = %#v, want preserved extension", target.Metadata["custom_extension"])
	}
	if got := storage.metadata["custom_extension"]; got == nil {
		t.Fatal("preserved custom extension was not forwarded to storage metadata")
	}
}
