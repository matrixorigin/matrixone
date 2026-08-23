// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

const proxyViewMetadataAdmissionGenerationKey = "view-metadata-admission-generation"

func (s *Server) initViewMetadataAdmission(ctx context.Context) error {
	timeout := s.config.HAKeeper.HeartbeatTimeout.Duration
	if timeout <= 0 {
		timeout = defaultHeartbeatTimeout
	}
	allocateCtx, cancel := context.WithTimeoutCause(ctx, timeout, moerr.CauseAllocateID)
	defer cancel()
	generation, err := s.haKeeperClient.AllocateIDByKey(allocateCtx, proxyViewMetadataAdmissionGenerationKey)
	if err != nil {
		return moerr.AttachCause(allocateCtx, err)
	}
	if generation == 0 {
		return moerr.NewInternalErrorNoCtx("HAKeeper returned zero proxy view metadata admission generation")
	}
	s.viewMetadataAdmissionGeneration = generation
	s.viewMetadataAdmissionUpdated = make(chan struct{}, 1)
	s.viewMetadataHeartbeatWakeup = make(chan struct{}, 1)
	return nil
}

func (s *Server) notifyViewMetadataAdmissionUpdated() {
	select {
	case s.viewMetadataAdmissionUpdated <- struct{}{}:
	default:
	}
}

func (s *Server) notifyViewMetadataHeartbeat() {
	select {
	case s.viewMetadataHeartbeatWakeup <- struct{}{}:
	default:
	}
}

func (s *Server) applyViewMetadataAdmission(
	ctx context.Context,
	snapshot *logservicepb.ViewMetadataAdmission,
) error {
	if s.viewMetadataAdmissionGeneration == 0 {
		return nil
	}
	if snapshot == nil {
		s.viewMetadataAdmission.Store(&logservicepb.ViewMetadataAdmission{Ready: true, Admitted: true})
		s.notifyViewMetadataAdmissionUpdated()
		return nil
	}
	if snapshot.Generation < s.viewMetadataAdmissionGeneration {
		if s.runtime != nil {
			s.runtime.Logger().Warn("ignored stale view metadata admission response for proxy",
				zap.Uint64("local-generation", s.viewMetadataAdmissionGeneration),
				zap.Uint64("response-generation", snapshot.Generation))
		}
		return nil
	}
	if snapshot.Generation > s.viewMetadataAdmissionGeneration {
		copy := *snapshot
		s.viewMetadataAdmission.Store(&copy)
		s.notifyViewMetadataAdmissionUpdated()
		s.revokeViewMetadataGeneration(snapshot.Generation)
		return nil
	}
	if snapshot.Epoch > s.viewMetadataObservedEpoch.Load() {
		if s.handler == nil || s.handler.moCluster == nil {
			return moerr.NewInternalErrorNoCtx("proxy cluster service is unavailable during admission")
		}
		refresher, ok := s.handler.moCluster.(clusterservice.AuthoritativeRefresher)
		if !ok {
			return moerr.NewInternalErrorNoCtx("proxy cluster service does not support authoritative refresh")
		}
		if err := refresher.Refresh(ctx); err != nil {
			return err
		}
		reader, ok := s.handler.moCluster.(clusterservice.ViewMetadataAdmissionReader)
		if !ok {
			return moerr.NewInternalErrorNoCtx("proxy cluster service does not expose admission snapshot")
		}
		refreshed := reader.GetViewMetadataAdmission()
		if refreshed.Epoch < snapshot.Epoch {
			return moerr.NewInternalErrorf(
				ctx,
				"proxy cluster snapshot did not reach admission epoch %d",
				snapshot.Epoch)
		}
		s.viewMetadataObservedEpoch.Store(snapshot.Epoch)
		s.notifyViewMetadataHeartbeat()
	}
	copy := *snapshot
	s.viewMetadataAdmission.Store(&copy)
	s.notifyViewMetadataAdmissionUpdated()
	return nil
}

func (s *Server) revokeViewMetadataGeneration(authoritative uint64) {
	s.viewMetadataRevocationOnce.Do(func() {
		if s.viewMetadataAdmissionCancel != nil {
			s.viewMetadataAdmissionCancel()
		}
		// Stop closes the listener before waiting for sessions, so no new client
		// can enter after this method returns and existing tunnels are disconnected.
		if s.app != nil {
			if err := s.app.Stop(); err != nil && s.runtime != nil {
				s.runtime.Logger().Error("failed to stop superseded proxy ingress",
					zap.Uint64("local-generation", s.viewMetadataAdmissionGeneration),
					zap.Uint64("authoritative-generation", authoritative),
					zap.Error(err))
			}
		}
		if s.stopper != nil || s.viewMetadataCloseFn != nil {
			closeFn := s.Close
			if s.viewMetadataCloseFn != nil {
				closeFn = s.viewMetadataCloseFn
			}
			go func() {
				if err := closeFn(); err != nil && s.runtime != nil {
					s.runtime.Logger().Error("failed to close superseded proxy",
						zap.Uint64("local-generation", s.viewMetadataAdmissionGeneration),
						zap.Uint64("authoritative-generation", authoritative),
						zap.Error(err))
				}
			}()
		}
	})
}

func (s *Server) waitForViewMetadataAdmission(parent context.Context) error {
	if s.viewMetadataAdmissionGeneration == 0 {
		return nil
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, s.viewMetadataAdmissionTimeout())
	defer cancel()
	for {
		snapshot := s.viewMetadataAdmission.Load()
		if snapshot != nil && !snapshot.Preparing && !snapshot.Enabled {
			return nil
		}
		if snapshot != nil && snapshot.Generation != s.viewMetadataAdmissionGeneration {
			return moerr.NewInternalErrorf(
				ctx,
				"Proxy %s admission generation was superseded: local=%d authoritative=%d",
				s.config.UUID,
				s.viewMetadataAdmissionGeneration,
				snapshot.Generation)
		}
		if snapshot != nil && snapshot.Ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-s.viewMetadataAdmissionUpdated:
		}
	}
}

// viewMetadataAdmissionTimeout covers the two RPCs needed by an active epoch:
// one to discover the authoritative epoch and one to report that observation.
// The interval is retained as scheduling tolerance and as a fallback window.
func (s *Server) viewMetadataAdmissionTimeout() time.Duration {
	interval := s.config.HAKeeper.HeartbeatInterval.Duration
	if interval <= 0 {
		interval = defaultHeartbeatInterval
	}
	rpcTimeout := s.config.HAKeeper.HeartbeatTimeout.Duration
	if rpcTimeout <= 0 {
		rpcTimeout = defaultHeartbeatTimeout
	}
	const maxDuration = time.Duration(1<<63 - 1)
	if rpcTimeout > (maxDuration-interval)/2 {
		return time.Duration(1<<63 - 1)
	}
	return interval + 2*rpcTimeout
}
