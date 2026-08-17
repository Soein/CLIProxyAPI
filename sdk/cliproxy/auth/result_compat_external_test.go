package auth_test

import (
	"testing"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestResultUnkeyedLiteralRemainsSourceCompatible(t *testing.T) {
	result := cliproxyauth.Result{
		"auth-id",
		"provider",
		"model",
		true,
		nil,
		false,
		nil,
		cliproxyexecutor.Options{},
	}
	if result.AuthID != "auth-id" || !result.Success {
		t.Fatalf("Result = %+v", result)
	}
}
