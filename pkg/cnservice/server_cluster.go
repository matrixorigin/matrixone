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

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/version"
)

const minClusterReadinessRetryInterval = 100 * time.Millisecond

// waitForClusterSelfReady prevents ingress from opening before this CN's local
// authoritative cluster snapshot contains the heartbeat generation it just
// published. Proxy and CN maintain independent snapshots, so a replacement CN
// can otherwise receive traffic while its own snapshot still predates itself.
func (s *service) waitForClusterSelfReady() error {
	// Some focused lifecycle tests build only the service dependencies relevant
	// to their assertion. NewService always initializes moCluster.
	if s.moCluster == nil {
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
	return s.waitForClusterSelfReadyWithContext(ctx, retryInterval)
}

func (s *service) waitForClusterSelfReadyWithContext(
	ctx context.Context,
	retryInterval time.Duration,
) error {
	select {
	case <-ctx.Done():
		return s.clusterSelfReadinessError(ctx, nil)
	case <-s.hakeeperConnected:
	}

	refresher, ok := s.moCluster.(clusterservice.AuthoritativeRefresher)
	if !ok {
		return moerr.NewInternalErrorNoCtx(
			"CN cluster service does not support authoritative refresh")
	}

	var lastRefreshErr error
	for attempts := 1; ; attempts++ {
		lastRefreshErr = refresher.Refresh(ctx)
		if lastRefreshErr == nil {
			ready, err := s.clusterSnapshotContainsSelf(ctx)
			if err != nil {
				lastRefreshErr = err
			} else if ready {
				s.logger.Info("CN is visible in local cluster inventory",
					zap.String("uuid", s.cfg.UUID),
					zap.Int("refresh-attempts", attempts))
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
			return s.clusterSelfReadinessError(ctx, lastRefreshErr)
		case <-timer.C:
		}
	}
}

func (s *service) clusterSnapshotContainsSelf(ctx context.Context) (bool, error) {
	found := false
	requireGeneration := false
	if reader, ok := s.moCluster.(clusterservice.ViewMetadataAdmissionReader); ok {
		admission := reader.GetViewMetadataAdmission()
		requireGeneration = admission.Preparing || admission.Enabled
	}
	err := clusterservice.GetCNServiceRawWithContext(
		ctx,
		s.moCluster,
		clusterservice.NewServiceIDSelector(s.cfg.UUID),
		func(cn metadata.CNService) bool {
			found = cn.PipelineServiceAddress == s.pipelineServiceServiceAddr() &&
				cn.CommitID == version.CommitID &&
				(!requireGeneration || s.viewMetadataAdmissionGeneration == 0 ||
					cn.ViewMetadataAdmissionGeneration == s.viewMetadataAdmissionGeneration)
			return false
		})
	return found, err
}

func (s *service) clusterSelfReadinessError(ctx context.Context, refreshErr error) error {
	if refreshErr != nil {
		return moerr.NewInternalErrorf(
			context.Background(),
			"CN %s was not published in its local cluster inventory before startup deadline: %v: %v",
			s.cfg.UUID,
			ctx.Err(),
			refreshErr)
	}
	return moerr.NewInternalErrorf(
		context.Background(),
		"CN %s was not published in its local cluster inventory before startup deadline: %v",
		s.cfg.UUID,
		ctx.Err())
}
