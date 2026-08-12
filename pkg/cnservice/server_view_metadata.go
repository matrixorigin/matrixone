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
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
)

const viewMetadataRecoveryInterval = 10 * time.Second

func (s *service) startViewMetadataRecovery() error {
	return s.stopper.RunNamedTask("view-metadata-recovery", func(ctx context.Context) {
		ticker := time.NewTicker(viewMetadataRecoveryInterval)
		defer ticker.Stop()
		runViewMetadataRecoveryLoop(ctx, ticker.C, func(ctx context.Context) error {
			return runViewMetadataRecoveryTick(
				ctx,
				s.viewMetadataRefreshReady(),
				clusterservice.AllKnownCNsSupportViewMetadataRefresh(s.cfg.UUID),
				func(ctx context.Context) error {
					return compile.RequireViewMetadataRevalidation(ctx, s.sqlExecutor)
				},
				func(ctx context.Context) error {
					return compile.StartViewMetadataRevalidation(ctx, s.sqlExecutor, s.cfg.UUID)
				},
				func(ctx context.Context) error {
					return compile.RunViewMetadataRecovery(ctx, s.sqlExecutor, s.cfg.UUID)
				},
			)
		}, func(err error) {
			s.logger.Warn("View metadata recovery tick failed", zap.Error(err))
		})
	})
}

func runViewMetadataRecoveryTick(
	ctx context.Context,
	locallyReady bool,
	globallyEnabled bool,
	requireRevalidation func(context.Context) error,
	startRevalidation func(context.Context) error,
	recoverPage func(context.Context) error,
) error {
	if !locallyReady {
		return nil
	}
	if !globallyEnabled {
		return requireRevalidation(ctx)
	}
	if err := startRevalidation(ctx); err != nil {
		return err
	}
	return recoverPage(ctx)
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
