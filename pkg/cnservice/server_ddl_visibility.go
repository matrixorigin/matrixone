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

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

func ddlVisibilityBarrierSupported(serviceID string) bool {
	value, ok := moruntime.ServiceRuntime(serviceID).GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := value.(int64)
	return ok && valid && version >= defines.MORPCVersion35
}

// prepareDDLVisibilityBarrier publishes this CN only after QueryService is
// listening. With protocol v35 active, it then catches up to the largest
// frontier held by the already-published barrier participants before public
// SQL ingress can be admitted.
func (s *service) prepareDDLVisibilityBarrier() error {
	s.ddlVisibilityBarrierMu.Lock()
	defer s.ddlVisibilityBarrierMu.Unlock()
	if err := s.prepareDDLVisibilityBarrierLocked(); err != nil {
		return err
	}
	s.ddlVisibilityBarrierPrepared.Store(true)
	return nil
}

func (s *service) prepareDDLVisibilityBarrierLocked() error {
	supported := ddlVisibilityBarrierSupported(s.cfg.UUID)
	if supported && (s.moCluster == nil || s.queryClient == nil || s._txnClient == nil) {
		// Focused service tests may construct only the dependencies relevant to
		// their lifecycle assertion. Production NewService initializes a non-zero
		// admission generation together with all three barrier dependencies.
		if s.viewMetadataAdmissionGeneration != 0 {
			return moerr.NewInternalErrorNoCtx("DDL visibility barrier dependencies are unavailable")
		}
	}

	s.ddlVisibilityBarrierReady.Store(true)
	s.notifyHeartbeat()
	if !supported || s.moCluster == nil || s.queryClient == nil || s._txnClient == nil {
		return nil
	}

	timeout := s.cfg.HAKeeper.DiscoveryTimeout.Duration
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	retryInterval := s.cfg.HAKeeper.HeatbeatInterval.Duration
	if retryInterval < minClusterReadinessRetryInterval {
		retryInterval = minClusterReadinessRetryInterval
	}
	if err := s.waitForDDLVisibilityBarrierPublication(ctx, retryInterval); err != nil {
		return err
	}
	return s.syncStartupDDLVisibilityFrontier(ctx)
}

// withdrawDDLVisibilityBarrier closes both externally published gates before
// QueryService is stopped. closeService invokes it only after the periodic
// heartbeat task has terminated, so no previously captured ready heartbeat can
// overwrite this final withdrawal.
func (s *service) withdrawDDLVisibilityBarrier() error {
	s.ddlVisibilityBarrierMu.Lock()
	defer s.ddlVisibilityBarrierMu.Unlock()
	ctx, cancel := s.newDDLVisibilityBarrierContext(context.Background())
	defer cancel()
	return s.withdrawDDLVisibilityBarrierLocked(ctx)
}

func (s *service) withdrawDDLVisibilityBarrierLocked(ctx context.Context) error {
	s.viewMetadataIngressReady.Store(false)
	s.ddlVisibilityBarrierReady.Store(false)
	if s.viewMetadataAdmissionGeneration == 0 {
		return nil
	}
	if s.cfg == nil || s._hakeeperClient == nil || s.moCluster == nil || s.config == nil {
		return moerr.NewInternalErrorNoCtx("DDL visibility barrier withdrawal dependencies are unavailable")
	}

	if _, err := s._hakeeperClient.SendCNHeartbeat(ctx, s.newCNStoreHeartbeat()); err != nil {
		return moerr.AttachCause(ctx, err)
	}
	return s.waitForDDLVisibilityBarrierWithdrawal(ctx, s.ddlVisibilityBarrierRetryInterval())
}

func (s *service) newDDLVisibilityBarrierContext(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := 30 * time.Second
	if s.cfg != nil && s.cfg.HAKeeper.DiscoveryTimeout.Duration > 0 {
		timeout = s.cfg.HAKeeper.DiscoveryTimeout.Duration
	}
	return context.WithTimeout(parent, timeout)
}

func (s *service) ddlVisibilityBarrierRetryInterval() time.Duration {
	retryInterval := time.Duration(0)
	if s.cfg != nil {
		retryInterval = s.cfg.HAKeeper.HeatbeatInterval.Duration
	}
	if retryInterval < minClusterReadinessRetryInterval {
		retryInterval = minClusterReadinessRetryInterval
	}
	return retryInterval
}

// setProtocolVersion serializes the live protocol transition with startup and
// shutdown. Before startup preparation, changing the runtime value is enough:
// the normal startup path will observe it and fence before ingress. Once this
// generation has been published, the first transition into v35 must withdraw,
// catch up, and republish before the new capability becomes visible locally.
func (s *service) setProtocolVersion(ctx context.Context, version int64) error {
	s.ddlVisibilityBarrierMu.Lock()
	defer s.ddlVisibilityBarrierMu.Unlock()

	if s.cfg == nil {
		return moerr.NewInternalError(ctx, "CN configuration is unavailable")
	}
	rt := moruntime.ServiceRuntime(s.cfg.UUID)
	value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return moerr.NewInternalError(ctx, "protocol version not found")
	}
	current, ok := value.(int64)
	if !ok {
		return moerr.NewInternalError(ctx, "invalid protocol version")
	}
	if current >= defines.MORPCVersion35 || version < defines.MORPCVersion35 ||
		!s.ddlVisibilityBarrierPrepared.Load() {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		return nil
	}
	if s.ddlVisibilityBarrierClosing.Load() || s.viewMetadataGenerationRevoked.Load() {
		return moerr.NewServiceUnavailableNoCtx("CN is closing")
	}
	if s.viewMetadataAdmissionGeneration == 0 || s.cfg == nil || s._hakeeperClient == nil ||
		s.moCluster == nil || s.queryClient == nil || s._txnClient == nil || s.config == nil {
		return moerr.NewInternalError(ctx, "DDL visibility activation dependencies are unavailable")
	}

	barrierCtx, cancel := s.newDDLVisibilityBarrierContext(ctx)
	defer cancel()
	if err := s.withdrawDDLVisibilityBarrierLocked(barrierCtx); err != nil {
		return err
	}
	if err := s.syncStartupDDLVisibilityFrontier(barrierCtx); err != nil {
		return err
	}
	if s.ddlVisibilityBarrierClosing.Load() || s.viewMetadataGenerationRevoked.Load() {
		return moerr.NewServiceUnavailableNoCtx("CN is closing")
	}

	// The local gates are opened before their heartbeat is sent, but ingress is
	// not externally routable until HAKeeper publishes that heartbeat. The
	// runtime version remains old until publication is authoritative.
	s.ddlVisibilityBarrierReady.Store(true)
	s.viewMetadataIngressReady.Store(true)
	_, publishErr := s._hakeeperClient.SendCNHeartbeat(barrierCtx, s.newCNStoreHeartbeat())
	if publishErr == nil {
		publishErr = s.waitForDDLVisibilityBarrierPublication(
			barrierCtx, s.ddlVisibilityBarrierRetryInterval())
	}
	if publishErr != nil {
		// A successful heartbeat followed by an uncertain refresh may already
		// have published true. Always issue a fresh bounded withdrawal before
		// returning the activation failure.
		cleanupCtx, cleanupCancel := s.newDDLVisibilityBarrierContext(context.Background())
		cleanupErr := s.withdrawDDLVisibilityBarrierLocked(cleanupCtx)
		cleanupCancel()
		return errors.Join(moerr.AttachCause(barrierCtx, publishErr), cleanupErr)
	}

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
	return nil
}

func (s *service) waitForDDLVisibilityBarrierWithdrawal(
	ctx context.Context,
	retryInterval time.Duration,
) error {
	refresher, ok := s.moCluster.(clusterservice.AuthoritativeRefresher)
	if !ok {
		return moerr.NewInternalErrorNoCtx(
			"CN cluster service does not support authoritative DDL visibility refresh")
	}

	for {
		if err := refresher.Refresh(ctx); err == nil {
			withdrawn := true
			err = clusterservice.GetCNServiceRawWithContext(
				ctx,
				s.moCluster,
				clusterservice.NewServiceIDSelector(s.cfg.UUID),
				func(cn metadata.CNService) bool {
					if cn.ViewMetadataAdmissionGeneration < s.viewMetadataAdmissionGeneration ||
						(cn.ViewMetadataAdmissionGeneration == s.viewMetadataAdmissionGeneration &&
							cn.DDLVisibilityBarrierReady) {
						withdrawn = false
					}
					return false
				})
			if err != nil {
				return err
			}
			if withdrawn {
				return nil
			}
		}

		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return moerr.NewInternalErrorf(
				context.Background(),
				"CN %s DDL visibility barrier was not withdrawn before shutdown deadline: %v",
				s.cfg.UUID,
				ctx.Err())
		case <-timer.C:
		}
	}
}

func (s *service) waitForDDLVisibilityBarrierPublication(
	ctx context.Context,
	retryInterval time.Duration,
) error {
	refresher, ok := s.moCluster.(clusterservice.AuthoritativeRefresher)
	if !ok {
		return moerr.NewInternalErrorNoCtx(
			"CN cluster service does not support authoritative DDL visibility refresh")
	}

	for {
		if err := refresher.Refresh(ctx); err == nil {
			published := false
			err = clusterservice.GetCNServiceRawWithContext(
				ctx,
				s.moCluster,
				clusterservice.NewServiceIDSelector(s.cfg.UUID),
				func(cn metadata.CNService) bool {
					published = cn.ViewMetadataAdmissionGeneration == s.viewMetadataAdmissionGeneration &&
						cn.DDLVisibilityBarrierReady
					return false
				})
			if err != nil {
				return err
			}
			if published {
				return nil
			}
		}

		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return moerr.NewInternalErrorf(
				context.Background(),
				"CN %s DDL visibility barrier was not published before startup deadline: %v",
				s.cfg.UUID,
				ctx.Err())
		case <-timer.C:
		}
	}
}

func (s *service) syncStartupDDLVisibilityFrontier(ctx context.Context) error {
	addresses := make([]string, 0, 4)
	err := clusterservice.GetCNServiceRawWithContext(
		ctx,
		s.moCluster,
		clusterservice.NewSelector(),
		func(cn metadata.CNService) bool {
			if cn.DDLVisibilityBarrierReady {
				addresses = append(addresses, cn.QueryAddress)
			}
			return true
		})
	if err != nil {
		return err
	}

	maxTS := timestamp.Timestamp{}
	for _, address := range addresses {
		if address == "" {
			return moerr.NewInternalErrorNoCtx(
				"barrier-ready CN has no query address during DDL visibility startup fence")
		}
		req := s.queryClient.NewRequest(query.CmdMethod_GetCommit)
		resp, err := s.queryClient.SendMessage(ctx, address, req)
		if err != nil {
			return err
		}
		if resp == nil {
			return moerr.NewInternalErrorf(ctx, "empty DDL frontier response from CN %s", address)
		}
		if resp.GetCommit == nil {
			s.queryClient.Release(resp)
			return moerr.NewInternalErrorf(ctx, "missing DDL frontier response from CN %s", address)
		}
		if maxTS.Less(resp.GetCommit.CurrentCommitTS) {
			maxTS = resp.GetCommit.CurrentCommitTS
		}
		s.queryClient.Release(resp)
	}
	if maxTS.IsEmpty() {
		return nil
	}
	if _, err := s._txnClient.WaitLogTailAppliedAt(ctx, maxTS); err != nil {
		return err
	}
	s._txnClient.SyncLatestCommitTS(maxTS)
	return nil
}
