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
	"github.com/matrixorigin/matrixone/pkg/util/file"
	"github.com/matrixorigin/matrixone/pkg/util/protoc"
)

func ddlVisibilityBarrierSupported(serviceID string) bool {
	value, ok := moruntime.ServiceRuntime(serviceID).GetGlobalVariables(moruntime.MOProtocolVersion)
	version, valid := value.(int64)
	return ok && valid && version >= defines.MORPCVersion38
}

// prepareDDLVisibilityBarrier publishes this CN only after QueryService is
// listening. With protocol v38 active, it then catches up to the largest
// frontier held by the already-published barrier participants before public
// SQL ingress can be admitted.
func (s *service) prepareDDLVisibilityBarrier() error {
	// MORPCLatestVersion describes compiled capability, not deployment-wide
	// activation. Restore the durable per-CN deployed protocol before deciding
	// whether this restart can produce v38 DDL. A fresh upgraded process has no
	// marker and starts on v37 until the complete-target cut persists v38.
	deployedVersion := s.loadDDLVisibilityDeployedProtocol()
	rt := moruntime.ServiceRuntime(s.cfg.UUID)
	if deployedVersion >= defines.MORPCVersion38 {
		s.ddlVisibilityActivationComplete.Store(true)
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, deployedVersion)
	} else if value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion); ok {
		if version, valid := value.(int64); valid && version >= defines.MORPCVersion38 {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion37)
		}
	}

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
	if supported && (s.moCluster == nil || s.queryClient == nil || s._txnClient == nil ||
		s._hakeeperClient == nil || s.config == nil) {
		// Focused service tests may construct only the dependencies relevant to
		// their lifecycle assertion. Production NewService initializes a non-zero
		// admission generation together with all three barrier dependencies.
		if s.viewMetadataAdmissionGeneration != 0 {
			return moerr.NewInternalErrorNoCtx("DDL visibility barrier dependencies are unavailable")
		}
	}

	s.ddlVisibilityBarrierReady.Store(true)
	if !supported || s.moCluster == nil || s.queryClient == nil || s._txnClient == nil {
		s.notifyHeartbeat()
		return nil
	}

	timeout := s.cfg.HAKeeper.DiscoveryTimeout.Duration
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Startup owns this mutex, so relying on the periodic heartbeat (which uses
	// the same mutex to order readiness snapshots) would deadlock publication.
	// Publish the startup barrier directly before waiting for its authoritative
	// observation; periodic heartbeats remain serialized behind startup.
	if _, err := s._hakeeperClient.SendCNHeartbeat(ctx, s.newCNStoreHeartbeat()); err != nil {
		return moerr.AttachCause(ctx, err)
	}

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
func (s *service) publishDDLVisibilityIngressAfterStart() error {
	// Serialize listener-ready publication with startup/activation/shutdown.
	// Compiled v38 capability is not evidence that the deployment-wide producer
	// cut completed: a default-v38 CN stays fail-closed until its complete-target
	// activation succeeds. If activation raced ahead of listener startup, this
	// method remains the sole owner that opens ingress after listeners are live.
	s.ddlVisibilityBarrierMu.Lock()
	defer s.ddlVisibilityBarrierMu.Unlock()
	if err := s.checkViewMetadataGenerationRevoked(); err != nil {
		return err
	}
	s.ddlVisibilityListenersReady.Store(true)
	if ddlVisibilityBarrierSupported(s.cfg.UUID) &&
		!s.ddlVisibilityActivationComplete.Load() {
		s.viewMetadataIngressReady.Store(false)
		s.notifyHeartbeat()
		return nil
	}
	if s.ddlCommitGate != nil {
		s.ddlCommitGate.EnablePublicDDL()
	}
	s.viewMetadataIngressReady.Store(true)
	if err := s.checkViewMetadataGenerationRevoked(); err != nil {
		return err
	}
	s.notifyHeartbeat()
	return nil
}

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

// setProtocolVersion serializes live activation with startup and shutdown.
// Every target CN blocks and drains local DDL before publishing Prepared. Once
// all targets are prepared, no v34 DDL producer exists; each CN applies the
// converged frontier and publishes Fenced. A CN releases its DDL gate only after
// every target is fenced, at which point all later DDL uses the v38 fan-out and
// every still-fencing receiver remains barrier-reachable.
func (s *service) setProtocolVersion(ctx context.Context, version int64, targets []string) error {
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
	pending := s.ddlVisibilityActivationPending.Load()
	if version < defines.MORPCVersion38 {
		if pending {
			return moerr.NewInvalidStateNoCtx("cannot downgrade during DDL visibility activation")
		}
		if err := s.persistDDLVisibilityDeployedProtocol(version); err != nil {
			return err
		}
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		s.ddlVisibilityActivationPrepared.Store(false)
		s.ddlVisibilityActivationFenced.Store(false)
		s.ddlVisibilityActivationComplete.Store(false)
		return nil
	}
	if !s.ddlVisibilityBarrierPrepared.Load() {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		return nil
	}
	if current >= defines.MORPCVersion38 && !pending &&
		s.ddlVisibilityActivationComplete.Load() {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		return nil
	}
	if s.ddlVisibilityBarrierClosing.Load() || s.viewMetadataGenerationRevoked.Load() {
		return moerr.NewServiceUnavailableNoCtx("CN is closing")
	}
	if s.viewMetadataAdmissionGeneration == 0 || s._hakeeperClient == nil ||
		s.moCluster == nil || s.queryClient == nil || s._txnClient == nil || s.config == nil ||
		s.ddlCommitGate == nil {
		return moerr.NewInternalError(ctx, "DDL visibility activation dependencies are unavailable")
	}
	activationTargets, err := validateDDLVisibilityActivationTargets(s.cfg.UUID, targets)
	if err != nil {
		return err
	}

	barrierCtx, cancel := s.newDDLVisibilityBarrierContext(ctx)
	defer cancel()
	if !pending {
		// A default-v38 process deliberately keeps ingress closed until the
		// complete-target cut. Restore ingress when listeners are already live;
		// if activation races before listener startup, Start opens it afterward.
		s.ddlVisibilityRestoreIngress.Store(
			s.viewMetadataIngressReady.Load() || s.ddlVisibilityListenersReady.Load())
	}
	s.ddlVisibilityActivationPending.Store(true)
	s.ddlVisibilityActivationPrepared.Store(false)
	s.ddlVisibilityActivationFenced.Store(false)
	// Stop new old-protocol DDL admission before the fallible authoritative
	// withdrawal. Both withdrawal and subsequent drain failures deliberately
	// leave the gate blocked for a fail-closed retry.
	if err := s.ddlCommitGate.BlockNew(); err != nil {
		return err
	}
	if err := s.setDDLVisibilityIngressLocked(barrierCtx, false); err != nil {
		return err
	}
	if err := s.ddlCommitGate.WaitDrained(barrierCtx); err != nil {
		return err
	}

	// Version 38 is visible only after the local pre-v38 producers are drained. It
	// acts as the distributed Prepared signal queried by every other target.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
	s.ddlVisibilityActivationPrepared.Store(true)
	if err := s.waitForDDLVisibilityActivationPhase(
		barrierCtx, activationTargets, false); err != nil {
		return err
	}
	if err := s.syncStartupDDLVisibilityFrontier(barrierCtx); err != nil {
		return err
	}
	s.ddlVisibilityActivationFenced.Store(true)
	if err := s.waitForDDLVisibilityActivationPhase(
		barrierCtx, activationTargets, true); err != nil {
		return err
	}
	if s.ddlVisibilityBarrierClosing.Load() || s.viewMetadataGenerationRevoked.Load() {
		return moerr.NewServiceUnavailableNoCtx("CN is closing")
	}
	// Persist the deployed cut before reopening ingress. A restart after this
	// point restores v38 and fences its startup frontier before admitting DDL.
	if err := s.persistDDLVisibilityDeployedProtocol(version); err != nil {
		return err
	}
	if err := s.setDDLVisibilityIngressLocked(
		barrierCtx, s.ddlVisibilityRestoreIngress.Load()); err != nil {
		cleanupCtx, cleanupCancel := s.newDDLVisibilityBarrierContext(context.Background())
		cleanupErr := s.setDDLVisibilityIngressLocked(cleanupCtx, false)
		cleanupCancel()
		return errors.Join(err, cleanupErr)
	}

	s.ddlVisibilityActivationComplete.Store(true)
	s.ddlVisibilityActivationPending.Store(false)
	if s.ddlVisibilityRestoreIngress.Load() {
		s.ddlCommitGate.EnablePublicDDL()
	}
	s.ddlCommitGate.Unblock()
	return nil
}

func (s *service) loadDDLVisibilityDeployedProtocol() int64 {
	return s.metadata.DDLVisibilityDeployedProtocol
}

func (s *service) persistDDLVisibilityDeployedProtocol(version int64) error {
	if s.metadataFS == nil {
		// Focused service tests may omit the local metadata file service.
		return nil
	}
	previous := s.metadata.DDLVisibilityDeployedProtocol
	s.metadata.DDLVisibilityDeployedProtocol = version
	if err := file.WriteFile(s.metadataFS, getMetadataFile(s.cfg.UUID), protoc.MustMarshal(&s.metadata)); err != nil {
		s.metadata.DDLVisibilityDeployedProtocol = previous
		return err
	}
	return nil
}

func validateDDLVisibilityActivationTargets(serviceID string, targets []string) (map[string]struct{}, error) {
	result := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		if target == "" {
			return nil, moerr.NewInternalErrorNoCtx("DDL visibility activation target is empty")
		}
		if _, exists := result[target]; exists {
			return nil, moerr.NewInternalErrorNoCtxf(
				"DDL visibility activation target %s is duplicated", target)
		}
		result[target] = struct{}{}
	}
	if _, ok := result[serviceID]; !ok {
		return nil, moerr.NewInternalErrorNoCtxf(
			"DDL visibility activation targets do not include local CN %s", serviceID)
	}
	return result, nil
}

func (s *service) setDDLVisibilityIngressLocked(ctx context.Context, ready bool) error {
	s.ddlVisibilityBarrierReady.Store(true)
	s.viewMetadataIngressReady.Store(ready)
	if _, err := s._hakeeperClient.SendCNHeartbeat(ctx, s.newCNStoreHeartbeat()); err != nil {
		return moerr.AttachCause(ctx, err)
	}
	return s.waitForDDLVisibilityIngress(
		ctx, s.ddlVisibilityBarrierRetryInterval(), ready)
}

func (s *service) waitForDDLVisibilityIngress(
	ctx context.Context,
	retryInterval time.Duration,
	ready bool,
) error {
	refresher, ok := s.moCluster.(clusterservice.AuthoritativeRefresher)
	if !ok {
		return moerr.NewInternalErrorNoCtx(
			"CN cluster service does not support authoritative DDL visibility refresh")
	}
	for {
		if err := refresher.Refresh(ctx); err == nil {
			matched := false
			err = clusterservice.GetCNServiceRawWithContext(
				ctx,
				s.moCluster,
				clusterservice.NewServiceIDSelector(s.cfg.UUID),
				func(cn metadata.CNService) bool {
					matched = cn.ViewMetadataAdmissionGeneration == s.viewMetadataAdmissionGeneration &&
						cn.DDLVisibilityBarrierReady && cn.ViewMetadataIngressReady == ready
					return false
				})
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}
		if err := waitDDLVisibilityRetry(ctx, retryInterval); err != nil {
			return moerr.NewInternalErrorf(
				context.Background(),
				"CN %s DDL visibility ingress=%t was not published before deadline: %v",
				s.cfg.UUID,
				ready,
				err)
		}
	}
}

func (s *service) waitForDDLVisibilityActivationPhase(
	ctx context.Context,
	targets map[string]struct{},
	requireFenced bool,
) error {
	refresher, ok := s.moCluster.(clusterservice.AuthoritativeRefresher)
	if !ok {
		return moerr.NewInternalErrorNoCtx(
			"CN cluster service does not support authoritative DDL visibility refresh")
	}
	for {
		allReady := true
		addresses := make(map[string]string, len(targets))
		if err := refresher.Refresh(ctx); err != nil {
			allReady = false
		} else if err := clusterservice.GetCNServiceRawWithContext(
			ctx,
			s.moCluster,
			clusterservice.NewSelector(),
			func(cn metadata.CNService) bool {
				if !cn.DDLVisibilityBarrierReady {
					return true
				}
				if _, expected := targets[cn.ServiceID]; !expected {
					allReady = false
					return true
				}
				addresses[cn.ServiceID] = cn.QueryAddress
				return true
			}); err != nil {
			return err
		}
		if len(addresses) != len(targets) {
			allReady = false
		}
		for serviceID := range targets {
			if !allReady {
				break
			}
			if serviceID == s.cfg.UUID {
				if !s.ddlVisibilityActivationPrepared.Load() ||
					(requireFenced && !s.ddlVisibilityActivationFenced.Load()) {
					allReady = false
				}
				continue
			}
			address := addresses[serviceID]
			if address == "" {
				return moerr.NewInternalErrorNoCtxf(
					"DDL visibility activation target %s has no query address", serviceID)
			}
			req := s.queryClient.NewRequest(query.CmdMethod_GetProtocolVersion)
			resp, err := s.queryClient.SendMessage(ctx, address, req)
			if err != nil {
				allReady = false
				break
			}
			if resp == nil || resp.GetProtocolVersion == nil {
				if resp != nil {
					s.queryClient.Release(resp)
				}
				return moerr.NewInternalErrorNoCtxf(
					"missing protocol activation response from CN %s", serviceID)
			}
			phase := resp.GetProtocolVersion
			allReady = phase.Version >= defines.MORPCVersion38 &&
				phase.DDLVisibilityActivationPrepared &&
				(!requireFenced || phase.DDLVisibilityActivationFenced)
			s.queryClient.Release(resp)
		}
		if allReady {
			return nil
		}
		if err := waitDDLVisibilityRetry(ctx, s.ddlVisibilityBarrierRetryInterval()); err != nil {
			return moerr.NewInternalErrorf(
				context.Background(),
				"DDL visibility activation fenced=%t did not converge before deadline: %v",
				requireFenced,
				err)
		}
	}
}

func waitDDLVisibilityRetry(ctx context.Context, retryInterval time.Duration) error {
	timer := time.NewTimer(retryInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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
