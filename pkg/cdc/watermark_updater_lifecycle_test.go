// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cdc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// The blocked executor deliberately ignores cancellation after observing it.
// A close timeout must retain dependencies until that real call returns.
type lifetimeExecutor struct {
	ie.InternalExecutor
	entered  chan struct{}
	canceled chan struct{}
	release  chan struct{}
	calls    atomic.Int32
}

func (e *lifetimeExecutor) Exec(ctx context.Context, _ string, _ ie.SessionOverrideOptions) error {
	e.calls.Add(1)
	close(e.entered)
	<-ctx.Done()
	close(e.canceled)
	<-e.release
	return ctx.Err()
}
func TestWatermarkUpdaterCloseJoinsAdmittedIO(t *testing.T) {
	for _, path := range []string{"direct", "cron", "queued", "owner-check"} {
		t.Run(path, func(t *testing.T) {
			e := &lifetimeExecutor{entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
			interval := time.Hour
			if path == "cron" {
				interval = time.Millisecond
			}
			var u *CDCWatermarkUpdater
			u = NewCDCWatermarkUpdater(t.Name(), e, WithCronJobInterval(interval), WithCustomizedCronJob(func(ctx context.Context) { _ = u.execSQL(ctx, "blocked", ie.SessionOverrideOptions{}) }))
			t.Cleanup(func() {
				select {
				case <-e.release:
				default:
					close(e.release)
				}
				u.Stop()
			})
			done := make(chan error, 1)
			switch path {
			case "cron":
				require.NoError(t, u.Start(context.Background()))
			case "queued":
				u.cacheUncommitted[WatermarkKey{AccountId: 1, TaskId: "task", DBName: "db", TableName: "table"}] = types.BuildTS(1, 1)
				require.NoError(t, u.Start(context.Background()))
				go func() { done <- u.ForceFlush(context.Background()) }()
			case "direct":
				go func() { done <- u.execSQL(context.Background(), "blocked", ie.SessionOverrideOptions{}) }()
			case "owner-check":
				go func() {
					done <- u.checkOwnerFence(context.Background(), NewOwnerFence(func(ctx context.Context) error { return e.Exec(ctx, "", ie.SessionOverrideOptions{}) }))
				}()
			}
			select {
			case <-e.entered:
			case <-time.After(time.Second):
				t.Fatal("I/O not admitted")
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			require.ErrorIs(t, u.CloseAndWait(ctx), context.Canceled)
			select {
			case <-e.canceled:
			case <-time.After(time.Second):
				t.Fatal("owner cancellation not propagated")
			}
			require.ErrorIs(t, u.Start(context.Background()), sm.ErrClose)
			require.ErrorIs(t, u.execSQL(context.Background(), "late", ie.SessionOverrideOptions{}), sm.ErrClose)
			require.EqualValues(t, 1, e.calls.Load())
			close(e.release)
			require.NoError(t, u.CloseAndWait(context.Background()))
			if path != "cron" {
				require.ErrorIs(t, <-done, context.Canceled)
			}
		})
	}
}
func TestWatermarkUpdaterRuntimeGenerationOwnership(t *testing.T) {
	oldRT := runtime.NewRuntime(0, "same-cn", zap.NewNop())
	newRT := runtime.NewRuntime(0, "same-cn", zap.NewNop())
	old := InitCDCWatermarkUpdater(oldRT, nil)
	fresh := InitCDCWatermarkUpdater(newRT, &watermarkReadExecutor{})
	t.Cleanup(fresh.Stop)
	old.Stop()
	require.Same(t, old, CDCWatermarkUpdaterFromRuntime(oldRT))
	require.Same(t, fresh, CDCWatermarkUpdaterFromRuntime(newRT))
	require.ErrorIs(t, old.Start(context.Background()), sm.ErrClose)
	require.NoError(t, fresh.Start(context.Background()))
}
