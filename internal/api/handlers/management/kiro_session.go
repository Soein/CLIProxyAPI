package management

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"time"

	internalkiro "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/kiro"
)

var errKiroSessionNotFound = errors.New("kiro session not found")

const defaultKiroSessionTTL = 10 * time.Minute

// kiroPKCESession captures one in-flight PKCE login.
type kiroPKCESession struct {
	CodeVerifier string
	State        string
	RedirectURI  string
	CreatedAt    time.Time
	ExpiresAt    time.Time
}

// kiroDeviceSession captures one in-flight Builder ID device flow.
// Status is one of "pending", "success", "error".
type kiroDeviceSession struct {
	ClientID        string
	ClientSecret    string
	DeviceCode      string
	UserCode        string
	VerificationURI string
	Status          string
	Credentials     *internalkiro.Credentials // set when Status == "success"
	Err             string                    // set when Status == "error"
	CreatedAt       time.Time
	ExpiresAt       time.Time
}

// kiroSessionStore tracks PKCE + Device login sessions in memory with TTL.
type kiroSessionStore struct {
	mu     sync.RWMutex
	ttl    time.Duration
	pkce   map[string]*kiroPKCESession
	device map[string]*kiroDeviceSession
}

func newKiroSessionStore(ttl time.Duration) *kiroSessionStore {
	if ttl <= 0 {
		ttl = defaultKiroSessionTTL
	}
	return &kiroSessionStore{
		ttl:    ttl,
		pkce:   map[string]*kiroPKCESession{},
		device: map[string]*kiroDeviceSession{},
	}
}

func newSessionID() string {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	return strings.TrimRight(base64.URLEncoding.EncodeToString(buf), "=")
}

func (s *kiroSessionStore) NewPKCESession(verifier, state, redirectURI string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	sid := newSessionID()
	now := time.Now()
	s.pkce[sid] = &kiroPKCESession{
		CodeVerifier: verifier,
		State:        state,
		RedirectURI:  redirectURI,
		CreatedAt:    now,
		ExpiresAt:    now.Add(s.ttl),
	}
	s.purgeExpiredLocked(now)
	return sid
}

func (s *kiroSessionStore) GetPKCE(sid string) (*kiroPKCESession, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.pkce[sid]
	if !ok {
		return nil, errKiroSessionNotFound
	}
	if !sess.ExpiresAt.IsZero() && time.Now().After(sess.ExpiresAt) {
		return nil, errKiroSessionNotFound
	}
	return sess, nil
}

func (s *kiroSessionStore) DeletePKCE(sid string) {
	s.mu.Lock()
	delete(s.pkce, sid)
	s.mu.Unlock()
}

func (s *kiroSessionStore) NewDeviceSession(clientID, clientSecret, deviceCode, userCode, verificationURI string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	sid := newSessionID()
	now := time.Now()
	s.device[sid] = &kiroDeviceSession{
		ClientID:        clientID,
		ClientSecret:    clientSecret,
		DeviceCode:      deviceCode,
		UserCode:        userCode,
		VerificationURI: verificationURI,
		Status:          "pending",
		CreatedAt:       now,
		ExpiresAt:       now.Add(s.ttl),
	}
	s.purgeExpiredLocked(now)
	return sid
}

func (s *kiroSessionStore) GetDevice(sid string) (*kiroDeviceSession, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.device[sid]
	if !ok {
		return nil, errKiroSessionNotFound
	}
	if !sess.ExpiresAt.IsZero() && time.Now().After(sess.ExpiresAt) && sess.Status == "pending" {
		return nil, errKiroSessionNotFound
	}
	return sess, nil
}

func (s *kiroSessionStore) CompleteDevice(sid string, creds *internalkiro.Credentials, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.device[sid]
	if !ok {
		return
	}
	if err != nil {
		sess.Status = "error"
		sess.Err = err.Error()
		return
	}
	sess.Status = "success"
	sess.Credentials = creds
}

func (s *kiroSessionStore) purgeExpiredLocked(now time.Time) {
	for k, v := range s.pkce {
		if !v.ExpiresAt.IsZero() && now.After(v.ExpiresAt) {
			delete(s.pkce, k)
		}
	}
	for k, v := range s.device {
		// Don't purge completed device sessions immediately — give frontend time to fetch result.
		if !v.ExpiresAt.IsZero() && now.After(v.ExpiresAt.Add(2*time.Minute)) {
			delete(s.device, k)
		}
	}
}
