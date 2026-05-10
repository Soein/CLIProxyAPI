package management

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	internalkiro "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/kiro"
)

// PostKiroPKCEStart begins a Kiro social PKCE login.
//
// Request body: {"provider":"google"|"github","region":"us-east-1"}
// Response: {"session_id","auth_url","state"}
//
// The frontend should open auth_url in a browser; the OAuth callback will
// land on the local Kiro callback server (started by the SDK during login),
// not on this management endpoint.
func (h *Handler) PostKiroPKCEStart(c *gin.Context) {
	if h == nil || h.kiroSessions == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "kiro session store not initialized"})
		return
	}
	var req struct {
		Provider string `json:"provider"`
		Region   string `json:"region,omitempty"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	idp := normalizeKiroProvider(req.Provider)
	if idp == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "provider must be google or github"})
		return
	}

	verifier, err := internalkiro.GenerateCodeVerifier()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	state, err := internalkiro.GenerateState()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	challenge := internalkiro.CodeChallenge(verifier)

	const callbackPort = 19876
	redirectURI := buildKiroCallbackURI(callbackPort)
	authURL := internalkiro.BuildPKCEAuthURL(idp, redirectURI, challenge, state, req.Region)

	sid := h.kiroSessions.NewPKCESession(verifier, state, redirectURI)
	c.JSON(http.StatusOK, gin.H{
		"session_id": sid,
		"auth_url":   authURL,
		"state":      state,
	})
}

func normalizeKiroProvider(p string) string {
	switch p {
	case "google", "Google":
		return "Google"
	case "github", "Github", "GitHub":
		return "Github"
	default:
		return ""
	}
}

func buildKiroCallbackURI(port int) string {
	return "http://127.0.0.1:" + strconv.Itoa(port) + "/oauth/callback"
}
