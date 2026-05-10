package management

import (
	"context"
	"net/http"
	"path/filepath"
	"strconv"
	"time"

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

// PostKiroDeviceStart begins a Builder ID device-code login.
//
// Request body: {"region":"us-east-1"} (optional)
// Response: {"session_id","user_code","verification_uri","expires_in"}
//
// The handler:
//  1. Calls BuilderIDClient.RegisterClient to get clientId/clientSecret
//  2. Calls StartDeviceAuthorization to get device_code + user_code
//  3. Spawns a goroutine that polls /token until success or timeout
//  4. Returns the user_code immediately so the frontend can display it
func (h *Handler) PostKiroDeviceStart(c *gin.Context) {
	if h == nil || h.kiroSessions == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "kiro session store not initialized"})
		return
	}
	var req struct {
		Region string `json:"region,omitempty"`
	}
	_ = c.ShouldBindJSON(&req)

	bd := internalkiro.NewBuilderIDClient(nil)
	if req.Region != "" {
		bd.Region = req.Region
	}

	reg, err := bd.RegisterClient(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "register client: " + err.Error()})
		return
	}

	auth, err := bd.StartDeviceAuthorization(c.Request.Context(), reg.ClientID, reg.ClientSecret)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "device authorization: " + err.Error()})
		return
	}

	sid := h.kiroSessions.NewDeviceSession(reg.ClientID, reg.ClientSecret,
		auth.DeviceCode, auth.UserCode, auth.VerificationURIComplete)

	// Spawn background poller. Captures cfg/AuthDir at this moment.
	authDir := ""
	if h.cfg != nil {
		authDir = h.cfg.AuthDir
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		creds, perr := bd.PollToken(ctx, reg.ClientID, reg.ClientSecret, auth.DeviceCode, 10*time.Minute)
		h.kiroSessions.CompleteDevice(sid, creds, perr)
		if perr == nil && creds != nil && authDir != "" {
			fname := internalkiro.CredentialFileName(reg.ClientID)
			full := filepath.Join(authDir, fname)
			_ = internalkiro.SaveCredentials(full, creds)
		}
	}()

	c.JSON(http.StatusOK, gin.H{
		"session_id":       sid,
		"user_code":        auth.UserCode,
		"verification_uri": auth.VerificationURIComplete,
		"expires_in":       auth.ExpiresIn,
	})
}

// GetKiroDeviceStatus returns the current status of a device login session.
// Status is "pending" / "success" / "error".
func (h *Handler) GetKiroDeviceStatus(c *gin.Context) {
	if h == nil || h.kiroSessions == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "kiro session store not initialized"})
		return
	}
	sid := c.Param("sid")
	sess, err := h.kiroSessions.GetDevice(sid)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "session not found"})
		return
	}
	resp := gin.H{
		"status":           sess.Status,
		"user_code":        sess.UserCode,
		"verification_uri": sess.VerificationURI,
	}
	if sess.Err != "" {
		resp["error"] = sess.Err
	}
	if sess.Credentials != nil {
		resp["access_token_preview"] = previewKiroToken(sess.Credentials.AccessToken)
	}
	c.JSON(http.StatusOK, resp)
}

func previewKiroToken(t string) string {
	if len(t) < 12 {
		return "***"
	}
	return t[:8] + "..." + t[len(t)-4:]
}
