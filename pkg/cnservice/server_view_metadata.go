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

	"github.com/matrixorigin/matrixone/pkg/sql/compile"
)

const viewMetadataRecoveryInterval = 10 * time.Second

func (s *service) startViewMetadataRecovery() error {
	return s.stopper.RunNamedTask("view-metadata-recovery", func(ctx context.Context) {
		ticker := time.NewTicker(viewMetadataRecoveryInterval)
		defer ticker.Stop()
		runViewMetadataRecoveryLoop(ctx, ticker.C, s.runViewMetadataRecoveryTick, func(err error) {
			s.logger.Warn("View metadata recovery tick failed", zap.Error(err))
		})
	})
}

type viewMetadataRecoveryTickState struct {
	locallyReady         bool
	epoch                uint64
	catalogFenced        bool
	refreshReady         bool
	refreshEnabled       bool
	revalidationRequired bool
}

func (s *service) runViewMetadataRecoveryTick(ctx context.Context) error {
	if s.sqlExecutor == nil || s.viewMetadataEpochFence == nil {
		return nil
	}
	snapshot := s.viewMetadataAdmission.Load()
	if snapshot == nil {
		return nil
	}
	state := viewMetadataRecoveryTickState{
		locallyReady:         s.viewMetadataRefreshReady(),
		epoch:                snapshot.Epoch,
		catalogFenced:        s.viewMetadataCatalogFencedEpoch.Load() >= snapshot.Epoch,
		refreshReady:         snapshot.RefreshReady,
		refreshEnabled:       snapshot.RefreshEnabled,
		revalidationRequired: snapshot.RevalidationRequired,
	}
	return runViewMetadataRecoveryTick(
		ctx,
		state,
		s.viewMetadataEpochFence.Acquire,
		func(ctx context.Context) error {
			return compile.StartViewMetadataRevalidation(ctx, s.sqlExecutor, s.cfg.UUID)
		},
		func(ctx context.Context) error {
			return compile.RunViewMetadataRecovery(ctx, s.sqlExecutor, s.cfg.UUID)
		},
		func(ctx context.Context) (bool, error) {
			return compile.ViewMetadataRevalidationComplete(ctx, s.sqlExecutor)
		},
		func(epoch uint64) bool {
			latest := s.viewMetadataAdmission.Load()
			if latest == nil || latest.Generation != s.viewMetadataAdmissionGeneration ||
				latest.Epoch != epoch || !latest.RefreshReady ||
				s.viewMetadataEpochFence.Epoch() != epoch {
				return false
			}
			s.viewMetadataRevalidatedEpoch.Store(epoch)
			s.notifyHeartbeat()
			return true
		},
	)
}

func runViewMetadataRecoveryTick(
	ctx context.Context,
	state viewMetadataRecoveryTickState,
	acquire func(context.Context) (*compile.ViewMetadataEpochLease, error),
	startRevalidation func(context.Context) error,
	recoverPage func(context.Context) error,
	revalidationComplete func(context.Context) (bool, error),
	publishCompletion func(uint64) bool,
) error {
	if !state.locallyReady || state.epoch == 0 || !state.catalogFenced ||
		(!state.refreshReady && !state.refreshEnabled) {
		return nil
	}
	lease, err := acquire(ctx)
	if err != nil {
		return err
	}
	defer lease.Release()
	if lease.Epoch() != state.epoch {
		return nil
	}
	if state.revalidationRequired {
		if !state.refreshReady {
			return nil
		}
		if err := startRevalidation(ctx); err != nil {
			return err
		}
	}
	if err := recoverPage(ctx); err != nil {
		return err
	}
	if !state.revalidationRequired {
		return nil
	}
	complete, err := revalidationComplete(ctx)
	if err != nil || !complete {
		return err
	}
	publishCompletion(state.epoch)
	return nil
}

func runViewMetadataRecoveryLoop(
	ctx context.Context,
	ticks <-chan time.Time,
	recoverOne func(context.Context) error,
	onError func(error),
) {
	if err := recoverOne(ctx); err != nil && ctx.Err() == nil {
		onError(err)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			if err := recoverOne(ctx); err != nil && ctx.Err() == nil {
				onError(err)
			}
		}
	}
}
