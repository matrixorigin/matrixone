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

package cnservice

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
)

const viewMetadataAdmissionGenerationKey = "view-metadata-admission-generation"

func (s *service) initViewMetadataAdmission(ctx context.Context) error {
	s.viewMetadataEpochFence = compile.NewViewMetadataEpochFence()
	s.viewMetadataAdmissionUpdated = make(chan struct{}, 1)

	timeout := s.cfg.HAKeeper.DiscoveryTimeout.Duration
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	allocateCtx, cancel := context.WithTimeoutCause(ctx, timeout, moerr.CauseAllocateID)
	defer cancel()
	generation, err := s._hakeeperClient.AllocateIDByKey(allocateCtx, viewMetadataAdmissionGenerationKey)
	if err != nil {
		return moerr.AttachCause(allocateCtx, err)
	}
	if generation == 0 {
		return moerr.NewInternalErrorNoCtx("HAKeeper returned zero view metadata admission generation")
	}
	s.viewMetadataAdmissionGeneration = generation
	runtime.ServiceRuntime(s.cfg.UUID).SetGlobalVariables(
		compile.ViewMetadataEpochFenceRuntimeKey,
		s.viewMetadataEpochFence,
	)
	return nil
}

func (s *service) closeViewMetadataAdmission() {
	if s.viewMetadataEpochFence == nil {
		return
	}
	s.viewMetadataEpochFence.Close()
	runtime.ServiceRuntime(s.cfg.UUID).CompareAndDeleteGlobalVariables(
		compile.ViewMetadataEpochFenceRuntimeKey,
		s.viewMetadataEpochFence,
	)
}

func (s *service) notifyViewMetadataAdmissionUpdated() {
	if s.viewMetadataAdmissionUpdated == nil {
		return
	}
	select {
	case s.viewMetadataAdmissionUpdated <- struct{}{}:
	default:
	}
}

func (s *service) applyViewMetadataAdmission(
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
		if s.logger != nil {
			s.logger.Warn("ignored stale view metadata admission response",
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
	if s.viewMetadataEpochFence == nil {
		return moerr.NewInternalErrorNoCtx("view metadata epoch fence is not initialized")
	}
	if err := s.viewMetadataEpochFence.Advance(ctx, snapshot.Epoch); err != nil {
		return err
	}
	copy := *snapshot
	s.viewMetadataAdmission.Store(&copy)
	s.notifyViewMetadataAdmissionUpdated()
	return s.fenceViewMetadataCatalog(ctx, &copy)
}

// revokeViewMetadataGeneration fences a process that no longer owns its UUID.
// The gates are closed synchronously so scheduling an asynchronous full Close
// cannot leave a window for new SQL, QueryService, or pipeline work. Full Close
// runs outside the heartbeat stopper task to avoid waiting for itself.
func (s *service) revokeViewMetadataGeneration(authoritative uint64) {
	s.viewMetadataRevocationOnce.Do(func() {
		s.viewMetadataGenerationRevoked.Store(true)
		s.viewMetadataIngressReady.Store(false)
		_ = s.closePipelineAdmission()
		s.queryWork.beginClose()
		// Serialize physical frontend shutdown with MOServer.Start without waiting
		// for the broader lifecycleMu held by the complete Start sequence.
		if s.mo != nil {
			if err := s.stopFrontendSerialized(); err != nil && s.logger != nil {
				s.logger.Error("failed to stop superseded CN frontend",
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
				if err := closeFn(); err != nil && s.logger != nil {
					s.logger.Error("failed to close superseded CN",
						zap.Uint64("local-generation", s.viewMetadataAdmissionGeneration),
						zap.Uint64("authoritative-generation", authoritative),
						zap.Error(err))
				}
			}()
		}
	})
}

func (s *service) fenceViewMetadataCatalog(
	ctx context.Context,
	snapshot *logservicepb.ViewMetadataAdmission,
) error {
	if snapshot == nil || !snapshot.RevalidationRequired || snapshot.Epoch == 0 ||
		s.viewMetadataCatalogFencedEpoch.Load() >= snapshot.Epoch {
		return nil
	}
	s.viewMetadataCatalogFenceMu.Lock()
	defer s.viewMetadataCatalogFenceMu.Unlock()
	if s.viewMetadataCatalogFencedEpoch.Load() >= snapshot.Epoch {
		return nil
	}
	if snapshot.CatalogFencedEpoch >= snapshot.Epoch {
		s.viewMetadataCatalogFencedEpoch.Store(snapshot.Epoch)
		return nil
	}
	if !s.viewMetadataCatalogFenceReady.Load() || s.sqlExecutor == nil {
		return nil
	}
	if err := compile.RequireViewMetadataRevalidation(ctx, s.sqlExecutor); err != nil {
		return err
	}
	s.viewMetadataCatalogFencedEpoch.Store(snapshot.Epoch)
	return nil
}

func (s *service) waitForViewMetadataAdmission() error {
	if s.viewMetadataAdmissionGeneration == 0 {
		// Focused unit tests can construct a partial service. Production
		// NewService always allocates a non-zero generation.
		return nil
	}
	timeout := s.cfg.HAKeeper.DiscoveryTimeout.Duration
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		snapshot := s.viewMetadataAdmission.Load()
		if snapshot != nil && !snapshot.Preparing && !snapshot.Enabled {
			return nil
		}
		if snapshot != nil && snapshot.Generation != s.viewMetadataAdmissionGeneration {
			return moerr.NewInternalErrorf(
				ctx,
				"CN %s admission generation was superseded: local=%d authoritative=%d",
				s.cfg.UUID,
				s.viewMetadataAdmissionGeneration,
				snapshot.Generation)
		}
		if snapshot != nil && snapshot.Epoch > 0 && s.viewMetadataEpochFence.Epoch() < snapshot.Epoch {
			if err := s.viewMetadataEpochFence.Advance(ctx, snapshot.Epoch); err != nil {
				return moerr.AttachCause(ctx, err)
			}
		}
		if err := s.fenceViewMetadataCatalog(ctx, snapshot); err != nil {
			return moerr.AttachCause(ctx, err)
		}
		if snapshot != nil && snapshot.Admitted {
			return nil
		}

		select {
		case <-ctx.Done():
			return moerr.NewInternalErrorf(
				context.Background(),
				"CN %s was not admitted before startup deadline: %v",
				s.cfg.UUID,
				ctx.Err())
		case <-s.viewMetadataAdmissionUpdated:
		}
	}
}
