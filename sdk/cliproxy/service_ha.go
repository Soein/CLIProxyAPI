package cliproxy

import (
	"context"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/api"
	log "github.com/sirupsen/logrus"
)

// startCoreAutoRefresh fences worker startup against fatal cluster failures
// and shutdown. A concurrent stop waits for an in-progress start, so a stop
// that observes terminal state is always the last lifecycle operation.
func (s *Service) startCoreAutoRefresh(ctx context.Context, interval time.Duration) bool {
	if s == nil || s.coreManager == nil {
		return false
	}
	manager := s.coreManager
	return s.startCoreAutoRefreshWith(func() {
		manager.StartAutoRefresh(ctx, interval)
	})
}

func (s *Service) startCoreAutoRefreshWith(start func()) bool {
	if s == nil || start == nil {
		return false
	}
	s.lifecycleMu.Lock()
	terminal := s.clusterFatal || s.stopping || s.shutdownStarted
	if terminal {
		s.lifecycleMu.Unlock()
		return false
	}
	startDone := make(chan struct{})
	s.coreAutoRefreshStartDone = startDone
	s.lifecycleMu.Unlock()

	start()

	s.lifecycleMu.Lock()
	if s.coreAutoRefreshStartDone == startDone {
		s.coreAutoRefreshStartDone = nil
	}
	close(startDone)
	terminal = s.clusterFatal || s.stopping || s.shutdownStarted
	s.lifecycleMu.Unlock()
	return !terminal
}

func (s *Service) stopCoreAutoRefresh() {
	if s == nil || s.coreManager == nil {
		return
	}
	manager := s.coreManager
	s.stopCoreAutoRefreshWith(manager.StopAutoRefresh)
}

func (s *Service) stopCoreAutoRefreshWith(stop func()) {
	if s == nil || stop == nil {
		return
	}
	s.lifecycleMu.Lock()
	startDone := s.coreAutoRefreshStartDone
	s.lifecycleMu.Unlock()
	if startDone != nil {
		<-startDone
	}
	stop()
}

func (s *Service) currentClusterFatalError() error {
	if s == nil {
		return nil
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	return s.clusterFatalErr
}

func (s *Service) releaseClusterNodeLease(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.lifecycleMu.Lock()
	lease := s.clusterNodeLease
	s.clusterNodeLease = nil
	s.lifecycleMu.Unlock()
	if lease == nil {
		return nil
	}
	releaseCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	errRelease := lease.release(releaseCtx)
	cancel()
	return errRelease
}

func (s *Service) releaseClusterNodeLeaseAfterStop(server *api.Server) {
	if s == nil || server == nil {
		return
	}
	s.leaseReleaseOnce.Do(func() {
		go func() {
			if errWait := server.WaitUntilStopped(context.Background()); errWait != nil {
				log.WithError(errWait).Error("cluster: failed waiting for API handlers before lease release")
				return
			}
			if errFinalize := s.finalizeClusterStop(context.Background()); errFinalize != nil {
				log.WithError(errFinalize).Error("cluster: failed to finalize dispatch authority after API handlers stopped")
			}
			if errRelease := s.releaseClusterNodeLease(context.Background()); errRelease != nil {
				log.WithError(errRelease).Error("cluster: failed to release node-id lease after API handlers stopped")
			}
		}()
	})
}

func (s *Service) finalizeClusterStop(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.clusterStopOnce.Do(func() {
		if s.clusterRegistrar != nil {
			drainCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			if errDrain := s.clusterRegistrar.Drain(drainCtx); errDrain != nil {
				log.WithError(errDrain).Warn("cluster: final drain notification failed; front-door will wait for staleness threshold")
			}
			cancel()
		}
		s.lifecycleMu.Lock()
		authority := s.clusterDispatchAuthority
		s.clusterDispatchAuthority = nil
		s.lifecycleMu.Unlock()
		if authority != nil {
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			s.clusterStopErr = authority.Close(closeCtx)
			cancel()
		}
		if s.coreManager != nil {
			s.coreManager.SetDispatchAuthority(nil)
		}
		s.lifecycleMu.Lock()
		cancelCluster := s.fatalClusterCancel
		if cancelCluster == nil {
			cancelCluster = s.clusterCancel
		}
		s.clusterCancel = nil
		s.fatalClusterCancel = nil
		s.lifecycleMu.Unlock()
		if cancelCluster != nil {
			cancelCluster()
		}
	})
	return s.clusterStopErr
}
