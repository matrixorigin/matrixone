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
	ingressWasReady := s.viewMetadataIngressReady.Swap(false)
	barrierWasReady := s.ddlVisibilityBarrierReady.Swap(false)
	if !ingressWasReady && !barrierWasReady {
		return nil
	}
	if s.viewMetadataAdmissionGeneration == 0 {
		return nil
	}
	if s.cfg == nil || s._hakeeperClient == nil || s.moCluster == nil || s.config == nil {
		return moerr.NewInternalErrorNoCtx("DDL visibility barrier withdrawal dependencies are unavailable")
	}

	timeout := s.cfg.HAKeeper.DiscoveryTimeout.Duration
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if _, err := s._hakeeperClient.SendCNHeartbeat(ctx, s.newCNStoreHeartbeat()); err != nil {
		return moerr.AttachCause(ctx, err)
	}
	retryInterval := s.cfg.HAKeeper.HeatbeatInterval.Duration
	if retryInterval < minClusterReadinessRetryInterval {
		retryInterval = minClusterReadinessRetryInterval
	}
	return s.waitForDDLVisibilityBarrierWithdrawal(ctx, retryInterval)
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
