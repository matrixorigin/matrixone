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

// waitForClusterSelfReady checks raw self registration before bootstrap, then
// admission-aware query membership before public SQL acceptance. The latter
// must follow ingress publication so HAKeeper can make this incarnation routable.
func (s *service) waitForClusterSelfReady(requireQueryReady bool) error {
	// Some focused lifecycle tests build only the service dependencies relevant
	// to their assertion. NewService always initializes moCluster.
	if s.moCluster == nil {
		return nil
	}

	timeout := s.cfg.HAKeeper.DiscoveryTimeout.Duration
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	parent := context.Background()
	if requireQueryReady && s.bootstrapUpgradeContext != nil {
		parent = s.bootstrapUpgradeContext
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	retryInterval := s.cfg.HAKeeper.HeatbeatInterval.Duration
	if retryInterval < minClusterReadinessRetryInterval {
		retryInterval = minClusterReadinessRetryInterval
	}
	return s.waitForClusterSelfReadyWithContext(ctx, retryInterval, requireQueryReady)
}

func (s *service) waitForClusterSelfReadyWithContext(
	ctx context.Context,
	retryInterval time.Duration,
	requireQueryReady bool,
) error {
	select {
	case <-ctx.Done():
		return s.clusterSelfReadinessError(ctx, nil, requireQueryReady)
	case <-s.hakeeperConnected:
	}

	refresher, ok := s.moCluster.(clusterservice.AuthoritativeRefresher)
	if !ok {
		return moerr.NewInternalErrorNoCtx(
			"CN cluster service does not support authoritative refresh")
	}

	var upgradeResult <-chan error
	if requireQueryReady {
		upgradeResult = s.bootstrapUpgradeResult
	}
	checkStartup := func() error {
		if err := s.checkViewMetadataGenerationRevoked(); err != nil {
			return err
		}
		if requireQueryReady {
			var err error
			upgradeResult, err = pollBootstrapUpgradeResult(s.bootstrapUpgradeContext, upgradeResult)
			return err
		}
		return nil
	}
	var lastRefreshErr error
	for attempts := 1; ; attempts++ {
		if err := checkStartup(); err != nil {
			return err
		}
		lastRefreshErr = refresher.Refresh(ctx)
		if err := checkStartup(); err != nil {
			return err
		}
		if lastRefreshErr == nil {
			ready, err := s.clusterSnapshotContainsSelf(ctx, requireQueryReady)
			if err != nil {
				lastRefreshErr = err
			} else if ready {
				s.logger.Info("CN is visible in local cluster inventory",
					zap.String("uuid", s.cfg.UUID),
					zap.Int("refresh-attempts", attempts),
					zap.Bool("query-ready", requireQueryReady))
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
			return s.clusterSelfReadinessError(ctx, lastRefreshErr, requireQueryReady)
		case err := <-upgradeResult:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			upgradeResult = nil
			if err != nil {
				return err
			}
		case <-timer.C:
		}
	}
}

func (s *service) clusterSnapshotContainsSelf(ctx context.Context, requireQueryReady bool) (bool, error) {
	found := false
	requireGeneration := requireQueryReady
	if reader, ok := s.moCluster.(clusterservice.ViewMetadataAdmissionReader); ok && !requireQueryReady {
		admission := reader.GetViewMetadataAdmission()
		requireGeneration = admission.Preparing || admission.Enabled
	}
	read := clusterservice.GetCNServiceRawWithContext
	if requireQueryReady {
		read = clusterservice.GetCNServiceWithoutWorkingStateWithContext
	}
	err := read(
		ctx,
		s.moCluster,
		clusterservice.NewServiceIDSelector(s.cfg.UUID),
		func(cn metadata.CNService) bool {
			found = cn.ServiceID == s.cfg.UUID && cn.PipelineServiceAddress == s.pipelineServiceServiceAddr() &&
				cn.CommitID == version.CommitID &&
				(!requireGeneration || s.viewMetadataAdmissionGeneration == 0 ||
					cn.ViewMetadataAdmissionGeneration == s.viewMetadataAdmissionGeneration)
			return false
		})
	return found, err
}

func (s *service) clusterSelfReadinessError(ctx context.Context, refreshErr error, requireQueryReady bool) error {
	if refreshErr != nil {
		return moerr.NewInternalErrorf(
			context.Background(),
			"CN %s was not published in its local cluster inventory before startup deadline (query-ready=%t): %v: %v",
			s.cfg.UUID,
			requireQueryReady,
			ctx.Err(),
			refreshErr)
	}
	return moerr.NewInternalErrorf(
		context.Background(),
		"CN %s was not published in its local cluster inventory before startup deadline (query-ready=%t): %v",
		s.cfg.UUID,
		requireQueryReady,
		ctx.Err())
}
