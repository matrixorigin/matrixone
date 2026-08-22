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
	generation, err := s.haKeeperClient.AllocateIDByKey(ctx, proxyViewMetadataAdmissionGenerationKey)
	if err != nil {
		return err
	}
	if generation == 0 {
		return moerr.NewInternalErrorNoCtx("HAKeeper returned zero proxy view metadata admission generation")
	}
	s.viewMetadataAdmissionGeneration = generation
	s.viewMetadataAdmissionUpdated = make(chan struct{}, 1)
	return nil
}

func (s *Server) notifyViewMetadataAdmissionUpdated() {
	select {
	case s.viewMetadataAdmissionUpdated <- struct{}{}:
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
		s.viewMetadataAdmission.Store(&logservicepb.ViewMetadataAdmission{Ready: true})
		s.notifyViewMetadataAdmissionUpdated()
		return nil
	}
	if snapshot.Generation != s.viewMetadataAdmissionGeneration {
		s.runtime.Logger().Warn("ignored view metadata admission response for another proxy generation",
			zap.Uint64("local-generation", s.viewMetadataAdmissionGeneration),
			zap.Uint64("response-generation", snapshot.Generation))
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
	}
	copy := *snapshot
	s.viewMetadataAdmission.Store(&copy)
	s.notifyViewMetadataAdmissionUpdated()
	return nil
}

func (s *Server) waitForViewMetadataAdmission() error {
	if s.viewMetadataAdmissionGeneration == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
			return moerr.NewInternalErrorf(
				context.Background(),
				"Proxy %s was not admitted before startup deadline: %v",
				s.config.UUID,
				ctx.Err())
		case <-s.viewMetadataAdmissionUpdated:
		}
	}
}
