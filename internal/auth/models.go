// Package auth provides authentication functionality for various AI service providers.
// It includes interfaces and implementations for token storage and authentication methods.
package auth

// TokenStorage defines the legacy path-based interface for storing authentication
// tokens. FileTokenStore treats implementations that do not also implement
// TokenJSONMarshaler as trusted: their path callback is responsible for its own
// filesystem containment and cannot be guarded by os.Root without changing this
// interface's contract.
type TokenStorage interface {
	// SaveTokenToFile persists authentication tokens to the specified file path.
	//
	// Parameters:
	//   - authFilePath: The file path where the authentication tokens should be saved
	//
	// Returns:
	//   - error: An error if the save operation fails, nil otherwise
	SaveTokenToFile(authFilePath string) error
}

// TokenJSONMarshaler is an optional extension for TokenStorage implementations
// that can serialize their complete auth-file payload without opening the target
// path. Implementations must not perform filesystem access. FileTokenStore writes
// the returned payload through an os.Root anchored at AuthDir.
type TokenJSONMarshaler interface {
	MarshalTokenJSON() ([]byte, error)
}
