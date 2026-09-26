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
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
)

const (
	viewMetadataAdmissionGenerationKey        = "view-metadata-admission-generation"
	viewMetadataCatalogFenceInitialRetryDelay = 250 * time.Millisecond
	viewMetadataCatalogFenceMaxRetryDelay     = 5 * time.Second
)

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
	rt := runtime.ServiceRuntime(s.cfg.UUID)
	if rt == nil {
		return moerr.NewInternalErrorNoCtx("CN runtime is not initialized")
	}
	if _, ok := rt.GetGlobalVariables(runtime.PersistedExpressionProtocolFloor); !ok {
		rt.SetGlobalVariables(runtime.PersistedExpressionProtocolFloor, int64(0))
	}
	// The write gate is local to this CN incarnation. Never inherit it across
	// a restart: the new process must first consume an enabled/admitted,
	// catalog-fenced snapshot again.
	rt.SetGlobalVariables(runtime.PersistedExpressionProtocolAuthoringFloor, int64(0))
	rt.SetGlobalVariables(
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

func (s *service) lockViewMetadataAdmission() {
	s.viewMetadataAdmissionMuWaiters.Add(1)
	s.viewMetadataAdmissionMu.Lock()
	s.viewMetadataAdmissionMuWaiters.Add(-1)
}

func (s *service) applyViewMetadataAdmission(
	ctx context.Context,
	snapshot *logservicepb.ViewMetadataAdmission,
) error {
	if s.viewMetadataAdmissionGeneration == 0 {
		return nil
	}
	if snapshot == nil {
		s.lockViewMetadataAdmission()
		s.viewMetadataAdmission.Store(&logservicepb.ViewMetadataAdmission{Ready: true, Admitted: true})
		s.notifyViewMetadataAdmissionUpdated()
		s.viewMetadataAdmissionMu.Unlock()
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
		s.lockViewMetadataAdmission()
		s.viewMetadataAdmission.Store(&copy)
		s.notifyViewMetadataAdmissionUpdated()
		s.viewMetadataAdmissionMu.Unlock()
		s.revokeViewMetadataGeneration(snapshot.Generation)
		return nil
	}
	if snapshot.PersistedExpressionRequiredProtocolVersion >
		uint64(defines.MORPCLatestVersion) {
		// Reject before advancing the epoch, publishing the snapshot, or
		// running catalog regeneration.  A downgraded CN must remain closed and
		// leave the previous admission state untouched until it is upgraded.
		err := moerr.NewNotSupportedf(
			ctx,
			"CN %s requires persisted expression protocol version %d (local version %d)",
			s.cfg.UUID,
			snapshot.PersistedExpressionRequiredProtocolVersion,
			defines.MORPCLatestVersion)
		// Do not leave an older admitted snapshot published after rejecting a
		// future floor.  Startup/readiness consumers must observe a closed,
		// poisoned snapshot until this CN is upgraded.
		copy := *snapshot
		copy.Preparing = true
		copy.Enabled = false
		copy.Ready = false
		copy.Admitted = false
		s.clearPersistedExpressionAuthoringFloor()
		s.lockViewMetadataAdmission()
		s.viewMetadataAdmission.Store(&copy)
		s.notifyViewMetadataAdmissionUpdated()
		s.viewMetadataAdmissionMu.Unlock()
		return err
	}
	if s.viewMetadataEpochFence == nil {
		return moerr.NewInternalErrorNoCtx("view metadata epoch fence is not initialized")
	}
	if !s.persistedExpressionAuthoringSnapshotReady(snapshot) {
		s.clearPersistedExpressionAuthoringFloor()
	}
	if snapshot.PersistedExpressionRequiredProtocolVersion > 0 {
		if rt := runtime.ServiceRuntime(s.cfg.UUID); rt != nil {
			value, ok := rt.GetGlobalVariables(runtime.PersistedExpressionProtocolFloor)
			current, valid := value.(int64)
			required := int64(snapshot.PersistedExpressionRequiredProtocolVersion)
			if !ok || !valid || current < required {
				// Publish the durable floor before the epoch fence or catalog SQL;
				// no planner/restore path can write or bind a marked view in the
				// window between those operations.
				rt.SetGlobalVariables(runtime.PersistedExpressionProtocolFloor, required)
			}
		}
	}
	if err := s.viewMetadataEpochFence.Advance(ctx, snapshot.Epoch); err != nil {
		return err
	}
	copy := *snapshot
	s.lockViewMetadataAdmission()
	s.viewMetadataAdmission.Store(&copy)
	s.notifyViewMetadataAdmissionUpdated()
	startupWaiting := s.viewMetadataCatalogFenceStartupWaiting.Load()
	s.viewMetadataAdmissionMu.Unlock()
	if startupWaiting {
		return nil
	}
	if err := s.fenceViewMetadataCatalog(ctx, &copy); err != nil &&
		!viewMetadataCatalogFenceRetryable(err, false) {
		return err
	}
	s.publishPersistedExpressionAuthoringFloor(&copy)
	return nil
}

// publishPersistedExpressionAuthoringFloor advances the local write gate only
// after HAKeeper has completed phase two for this CN and the local catalog
// fence is known. The durable read floor is intentionally published earlier,
// while the snapshot is still Preparing, so existing marked metadata can be
// safely read/revalidated without allowing new metadata to be authored during
// the routing handoff.
func (s *service) publishPersistedExpressionAuthoringFloor(
	snapshot *logservicepb.ViewMetadataAdmission,
) {
	if !s.persistedExpressionAuthoringSnapshotReady(snapshot) {
		return
	}
	rt := runtime.ServiceRuntime(s.cfg.UUID)
	if rt == nil {
		return
	}
	required := int64(snapshot.PersistedExpressionRequiredProtocolVersion)
	value, ok := rt.GetGlobalVariables(runtime.PersistedExpressionProtocolAuthoringFloor)
	current, valid := value.(int64)
	if !ok || !valid || current < required {
		rt.SetGlobalVariables(runtime.PersistedExpressionProtocolAuthoringFloor, required)
	}
}

// Ready describes public routing and depends on ingress already listening.
// Admitted authorizes local initialization after the protocol/catalog barrier;
// requiring routing readiness here would deadlock derived-schema bootstrap.
func (s *service) persistedExpressionAuthoringSnapshotReady(
	snapshot *logservicepb.ViewMetadataAdmission,
) bool {
	if snapshot == nil || snapshot.PersistedExpressionRequiredProtocolVersion == 0 ||
		snapshot.Preparing || !snapshot.Enabled || !snapshot.Admitted ||
		snapshot.PersistedExpressionProtocolActivationPending {
		return false
	}
	if !snapshot.RevalidationRequired {
		return true
	}
	return snapshot.Epoch > 0 && snapshot.CatalogFencedEpoch >= snapshot.Epoch &&
		s.viewMetadataCatalogFencedEpoch.Load() >= snapshot.Epoch
}

func (s *service) clearPersistedExpressionAuthoringFloor() {
	if rt := runtime.ServiceRuntime(s.cfg.UUID); rt != nil {
		rt.SetGlobalVariables(runtime.PersistedExpressionProtocolAuthoringFloor, int64(0))
	}
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
		runner := s.detachRevokedTaskRunner()
		// Serialize physical frontend shutdown with MOServer.Start before waiting
		// for task executors, whose Stop may block indefinitely. Do not wait for
		// the broader lifecycleMu held by the complete Start sequence.
		if s.mo != nil {
			if err := s.stopFrontendSerialized(); err != nil && s.logger != nil {
				s.logger.Error("failed to stop superseded CN frontend",
					zap.Uint64("local-generation", s.viewMetadataAdmissionGeneration),
					zap.Uint64("authoritative-generation", authoritative),
					zap.Error(err))
			}
		}
		s.stopRevokedTaskRunner(runner)
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

func viewMetadataCatalogFenceRetryable(err error, upgradeOwnerActive bool) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		retryable := false
		for _, child := range joined.Unwrap() {
			if child == nil {
				continue
			}
			if !viewMetadataCatalogFenceRetryable(child, upgradeOwnerActive) {
				return false
			}
			retryable = true
		}
		return retryable
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		if child := wrapped.Unwrap(); child != nil {
			return viewMetadataCatalogFenceRetryable(child, upgradeOwnerActive)
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return upgradeOwnerActive
	}
	// Catalog fencing is a startup read/write transaction. A rolling restart can
	// temporarily leave its cached lock-table owner unavailable while discovery
	// and the allocator converge. Retry the complete transaction within the
	// existing startup deadline; mixed errors still fail closed because joined
	// leaves are classified independently above.
	if morpc.IsConnectionError(err) {
		return true
	}
	var moErr *moerr.Error
	if errors.As(err, &moErr) {
		switch moErr.ErrorCode() {
		case moerr.ErrNoSuchTable, moerr.ErrBadDB:
			return true
		case moerr.ErrTxnNeedRetry, moerr.ErrTxnNeedRetryWithDefChanged:
			return upgradeOwnerActive
		}
	}
	return false
}

func (s *service) acceptViewMetadataAdmissionSnapshot(
	fenced *logservicepb.ViewMetadataAdmission,
	disabled bool,
	publishIngress bool,
	upgradeResult <-chan error,
	minimumAuthoringProtocol uint64,
) (bool, <-chan error, error) {
	s.lockViewMetadataAdmission()
	defer s.viewMetadataAdmissionMu.Unlock()

	var upgradeErr error
	upgradeResult, upgradeErr = pollBootstrapUpgradeResult(upgradeResult)
	if upgradeErr != nil {
		return false, upgradeResult, upgradeErr
	}
	current := s.viewMetadataAdmission.Load()
	if fenced == nil || current == nil ||
		current.Generation != fenced.Generation || current.Epoch != fenced.Epoch {
		return false, upgradeResult, nil
	}
	if disabled {
		if current.Preparing || current.Enabled {
			return false, upgradeResult, nil
		}
	} else {
		if fenced.PersistedExpressionRequiredProtocolVersion >
			uint64(defines.MORPCLatestVersion) {
			return false, upgradeResult, moerr.NewNotSupportedf(
				context.Background(),
				"CN %s requires persisted expression protocol version %d (local version %d)",
				s.cfg.UUID,
				fenced.PersistedExpressionRequiredProtocolVersion,
				defines.MORPCLatestVersion)
		}
		if !current.Admitted || current.Epoch > 0 &&
			(s.viewMetadataEpochFence == nil || s.viewMetadataEpochFence.Epoch() < current.Epoch) {
			return false, upgradeResult, nil
		}
		if current.RevalidationRequired && current.Epoch > 0 &&
			current.CatalogFencedEpoch < current.Epoch &&
			s.viewMetadataCatalogFencedEpoch.Load() < current.Epoch {
			return false, upgradeResult, nil
		}
	}
	if minimumAuthoringProtocol > 0 &&
		(!s.persistedExpressionAuthoringSnapshotReady(current) ||
			current.PersistedExpressionRequiredProtocolVersion < minimumAuthoringProtocol) {
		return false, upgradeResult, nil
	}
	if minimumAuthoringProtocol > 0 {
		s.publishPersistedExpressionAuthoringFloor(current)
	}
	if publishIngress {
		if s.beforeViewMetadataAdmissionHandoff != nil {
			s.beforeViewMetadataAdmissionHandoff()
		}
		// This store and snapshot publication share one lock. An update is
		// therefore either validated above or observes startupWaiting=false and
		// owns fencing its newer epoch after this handoff.
		s.viewMetadataCatalogFenceStartupWaiting.Store(false)
		s.viewMetadataIngressReady.Store(true)
	}
	return true, upgradeResult, nil
}

func viewMetadataCatalogFenceRetryDelay(serviceID string, attempt uint32) time.Duration {
	delay := viewMetadataCatalogFenceInitialRetryDelay
	for remaining := attempt; remaining > 0 && delay < viewMetadataCatalogFenceMaxRetryDelay/2; remaining-- {
		delay *= 2
	}

	// Stable per-CN jitter spreads transactions across a bounded ±20% window
	// without introducing a process-global random source into startup tests.
	hash := uint64(14695981039346656037)
	for i := range len(serviceID) {
		hash ^= uint64(serviceID[i])
		hash *= 1099511628211
	}
	hash ^= uint64(attempt)
	hash *= 1099511628211
	jitterWindow := delay / 5
	jitter := time.Duration(hash%uint64(2*jitterWindow+1)) - jitterWindow
	return delay + jitter
}

func (s *service) waitForViewMetadataAdmission() error {
	return s.waitForViewMetadataAdmissionHandoff(false, 0)
}

func (s *service) waitForViewMetadataIngressAdmission() error {
	return s.waitForViewMetadataAdmissionHandoff(true, 0)
}

func pollBootstrapUpgradeResult(result <-chan error) (<-chan error, error) {
	select {
	case err := <-result:
		return nil, err
	default:
		return result, nil
	}
}

func (s *service) waitForViewMetadataAdmissionHandoff(publishIngress bool, minimumAuthoringProtocol uint64) error {
	if s.viewMetadataAdmissionGeneration == 0 {
		// Focused unit tests can construct a partial service. Production
		// NewService always allocates a non-zero generation.
		if publishIngress {
			s.viewMetadataIngressReady.Store(true)
		}
		return nil
	}
	timeout := s.cfg.HAKeeper.DiscoveryTimeout.Duration
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	discoveryCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	operationCtx := discoveryCtx
	var upgradeResult <-chan error
	if s.bootstrapUpgradeContext != nil {
		operationCtx = s.bootstrapUpgradeContext
		upgradeResult = s.bootstrapUpgradeResult
	}
	s.viewMetadataCatalogFenceStartupWaiting.Store(true)
	defer s.viewMetadataCatalogFenceStartupWaiting.Store(false)

	catalogRetryTimer := time.NewTimer(time.Hour)
	if !catalogRetryTimer.Stop() {
		<-catalogRetryTimer.C
	}
	defer catalogRetryTimer.Stop()
	var catalogRetryAttempt uint32
	var catalogPendingEpoch uint64
	catalogRetryReady := true

	for {
		snapshot := s.viewMetadataAdmission.Load()
		if minimumAuthoringProtocol > 0 {
			if err := s.checkViewMetadataGenerationRevoked(); err != nil {
				return err
			}
			if snapshot != nil && snapshot.Generation != 0 && snapshot.Generation != s.viewMetadataAdmissionGeneration {
				return moerr.NewInvalidStateNoCtx("CN system view admission generation was superseded")
			}
			if snapshot != nil && snapshot.PersistedExpressionRequiredProtocolVersion > uint64(defines.MORPCLatestVersion) {
				return moerr.NewNotSupportedNoCtx("CN system view admission requires a newer protocol")
			}
		}
		if snapshot != nil && !snapshot.Preparing && !snapshot.Enabled {
			if minimumAuthoringProtocol > 0 {
				// A new cluster can advertise a disabled snapshot before activation.
				// Prerequisites are committed; wait for capability/fence heartbeats,
				// without opening ingress or spinning on this unchanged snapshot.
				select {
				case <-discoveryCtx.Done():
					return moerr.AttachCause(discoveryCtx, discoveryCtx.Err())
				case <-operationCtx.Done():
					return moerr.AttachCause(operationCtx, operationCtx.Err())
				case upgradeErr := <-upgradeResult:
					upgradeResult = nil
					if upgradeErr != nil {
						return upgradeErr
					}
				case <-s.viewMetadataAdmissionUpdated:
				}
				continue
			}
			accepted, result, upgradeErr := s.acceptViewMetadataAdmissionSnapshot(
				snapshot, true, publishIngress, upgradeResult, 0)
			upgradeResult = result
			if upgradeErr != nil {
				return upgradeErr
			}
			if accepted {
				s.publishPersistedExpressionAuthoringFloor(snapshot)
				return nil
			}
			continue
		}
		if snapshot != nil && snapshot.Generation != s.viewMetadataAdmissionGeneration {
			return moerr.NewInternalErrorf(
				operationCtx,
				"CN %s admission generation was superseded: local=%d authoritative=%d",
				s.cfg.UUID,
				s.viewMetadataAdmissionGeneration,
				snapshot.Generation)
		}
		if snapshot != nil && snapshot.PersistedExpressionRequiredProtocolVersion >
			uint64(defines.MORPCLatestVersion) {
			return moerr.NewNotSupportedf(
				operationCtx,
				"CN %s requires persisted expression protocol version %d (local version %d)",
				s.cfg.UUID,
				snapshot.PersistedExpressionRequiredProtocolVersion,
				defines.MORPCLatestVersion)
		}
		if snapshot != nil && snapshot.Epoch > 0 && s.viewMetadataEpochFence.Epoch() < snapshot.Epoch {
			if err := s.viewMetadataEpochFence.Advance(operationCtx, snapshot.Epoch); err != nil {
				return moerr.AttachCause(operationCtx, err)
			}
		}

		catalogPending := false
		var catalogRetry <-chan time.Time
		var catalogEpoch uint64
		if snapshot != nil {
			catalogEpoch = snapshot.Epoch
		}
		catalogFenceStillPending := snapshot != nil && snapshot.RevalidationRequired && catalogEpoch > 0 &&
			snapshot.CatalogFencedEpoch < catalogEpoch &&
			s.viewMetadataCatalogFencedEpoch.Load() < catalogEpoch
		if !catalogRetryReady && catalogFenceStillPending && catalogEpoch == catalogPendingEpoch {
			// A heartbeat may refresh the same admission snapshot while the
			// catalog is unavailable. Process authority changes immediately, but
			// do not let that notification bypass the catalog backoff.
			catalogPending = true
			catalogRetry = catalogRetryTimer.C
		} else if err := s.fenceViewMetadataCatalog(operationCtx, snapshot); err != nil {
			upgradeOwnerActive := upgradeResult != nil && operationCtx.Err() == nil
			if !viewMetadataCatalogFenceRetryable(err, upgradeOwnerActive) {
				select {
				case upgradeErr := <-upgradeResult:
					if upgradeErr != nil {
						return upgradeErr
					}
				default:
				}
				return moerr.AttachCause(operationCtx, err)
			}
			catalogPending = true
			if catalogEpoch != catalogPendingEpoch {
				catalogRetryAttempt = 0
			}
			catalogPendingEpoch = catalogEpoch
			catalogRetryReady = false
			// BootstrapUpgrade owns creating the lifecycle catalog asynchronously.
			// Keep ingress closed and retry without requiring a new admission
			// heartbeat after that transaction becomes visible.
			if !catalogRetryTimer.Stop() {
				select {
				case <-catalogRetryTimer.C:
				default:
				}
			}
			catalogRetryTimer.Reset(viewMetadataCatalogFenceRetryDelay(
				s.cfg.UUID, catalogRetryAttempt))
			catalogRetryAttempt++
			catalogRetry = catalogRetryTimer.C
		} else {
			catalogPendingEpoch = 0
			catalogRetryAttempt = 0
			catalogRetryReady = true
			if !catalogRetryTimer.Stop() {
				select {
				case <-catalogRetryTimer.C:
				default:
				}
			}
			accepted, result, upgradeErr := s.acceptViewMetadataAdmissionSnapshot(
				snapshot, false, publishIngress, upgradeResult, minimumAuthoringProtocol)
			upgradeResult = result
			if upgradeErr != nil {
				return upgradeErr
			}
			if accepted {
				if minimumAuthoringProtocol == 0 {
					s.publishPersistedExpressionAuthoringFloor(snapshot)
				}
				return nil
			}
		}

		discoveryDone := discoveryCtx.Done()
		var upgradeDone <-chan struct{}
		if catalogPending && upgradeResult != nil {
			// Once the asynchronous catalog owner is known to be progressing,
			// HAKeeper discovery's shorter deadline no longer owns this wait.
			discoveryDone = nil
			upgradeDone = operationCtx.Done()
		}
		select {
		case <-discoveryDone:
			return moerr.NewInternalErrorf(
				context.Background(),
				"CN %s was not admitted before startup deadline: %v",
				s.cfg.UUID,
				discoveryCtx.Err())
		case upgradeErr := <-upgradeResult:
			if upgradeErr != nil {
				return upgradeErr
			}
			upgradeResult = nil
		case <-upgradeDone:
			select {
			case upgradeErr := <-upgradeResult:
				if upgradeErr != nil {
					return upgradeErr
				}
			default:
			}
			return moerr.AttachCause(operationCtx, operationCtx.Err())
		case <-s.viewMetadataAdmissionUpdated:
		case <-catalogRetry:
			catalogRetryReady = true
		}
	}
}
