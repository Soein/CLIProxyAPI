package cliproxy

import (
	"context"
	"testing"
)

func TestRejectedServiceRunPreservesProbeContext(t *testing.T) {
	for _, stopping := range []bool{false, true} {
		name := "already-running"
		if stopping {
			name = "stopping"
		}
		t.Run(name, func(t *testing.T) {
			probeCtx, cancel := context.WithCancel(t.Context())
			defer cancel()
			service := &Service{
				stopping:           stopping,
				runCancel:          cancel,
				antigravityContext: probeCtx,
			}
			if err := service.Run(t.Context()); err == nil {
				t.Fatal("Run accepted a service that is already running or stopping")
			}
			if service.antigravityContext != probeCtx {
				t.Fatal("rejected Run replaced the active background model probe context")
			}
			if probeCtx.Err() != nil {
				t.Fatal("rejected Run canceled the active background model probe context")
			}
		})
	}
}
