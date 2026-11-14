// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package firstreplay

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/testutil/clusteradmission"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

func TestStartReplayControlPlaneStopsOnFailure(t *testing.T) {
	failure := errors.New("startup failure")
	for failAt := -1; failAt < 3; failAt++ {
		t.Run(fmtStage(failAt), func(t *testing.T) {
			var calls []int
			steps := make([]func() error, 3)
			for i := range steps {
				index := i
				steps[i] = func() error {
					calls = append(calls, index)
					if index == failAt {
						return failure
					}
					return nil
				}
			}
			err := startReplayControlPlane(steps[0], steps[1], steps[2])
			if failAt < 0 {
				require.NoError(t, err)
				require.Equal(t, []int{0, 1, 2}, calls)
			} else {
				require.ErrorIs(t, err, failure)
				require.Equal(t, []int{0, 1, 2}[:failAt+1], calls)
			}
		})
	}
}

func fmtStage(i int) string {
	return []string{"success", "log", "tn", "shard"}[i+1]
}

type replayDetailsFunc func(context.Context) (logservicepb.ClusterDetails, error)

func (fn replayDetailsFunc) GetClusterDetails(ctx context.Context) (logservicepb.ClusterDetails, error) {
	return fn(ctx)
}

func TestWaitForReplayTN(t *testing.T) {
	t.Run("waits for a shard", func(t *testing.T) {
		calls := 0
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := waitForReplayTN(ctx, replayDetailsFunc(func(context.Context) (logservicepb.ClusterDetails, error) {
			calls++
			if calls == 1 {
				return logservicepb.ClusterDetails{}, nil
			}
			return logservicepb.ClusterDetails{TNStores: []logservicepb.TNStore{{Shards: []logservicepb.TNShardInfo{{}}}}}, nil
		}))
		require.NoError(t, err)
		require.Equal(t, 2, calls)
	})
	t.Run("preserves timeout and last error", func(t *testing.T) {
		lastErr := errors.New("HAKeeper unavailable")
		ctx, cancel := context.WithCancel(context.Background())
		err := waitForReplayTN(ctx, replayDetailsFunc(func(context.Context) (logservicepb.ClusterDetails, error) {
			cancel()
			return logservicepb.ClusterDetails{}, lastErr
		}))
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorIs(t, err, lastErr)
	})
	t.Run("already canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		called := false
		err := waitForReplayTN(ctx, replayDetailsFunc(func(context.Context) (logservicepb.ClusterDetails, error) {
			called = true
			return logservicepb.ClusterDetails{}, nil
		}))
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, called)
	})
}

func TestCleanupReplayResources(t *testing.T) {
	t.Run("cleans data when admission was never acquired", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "owned")
		require.NoError(t, os.Mkdir(path, 0o700))
		var admission *clusteradmission.Lease
		err := cleanupReplayResources(func() {}, func() error { return nil },
			func() error { return os.RemoveAll(path) }, admission.Release)
		require.NoError(t, err)
		_, err = os.Stat(path)
		require.True(t, os.IsNotExist(err))
	})
	t.Run("joins startup before closing and releasing", func(t *testing.T) {
		startupDone := make(chan struct{})
		defer close(startupDone)
		joining := make(chan struct{})
		events := make(chan string, 3)
		result := make(chan error, 1)
		go func() {
			result <- cleanupReplayResources(func() {
				close(joining)
				<-startupDone
			}, func() error {
				events <- "close"
				return nil
			}, func() error {
				events <- "remove"
				return nil
			}, func() error {
				events <- "release"
				return nil
			})
		}()
		<-joining
		select {
		case event := <-events:
			t.Fatalf("%s ran while startup was still active", event)
		default:
		}
		startupDone <- struct{}{}
		require.NoError(t, <-result)
		require.Equal(t, "close", <-events)
		require.Equal(t, "remove", <-events)
		require.Equal(t, "release", <-events)
	})
	t.Run("failed close retains storage and admission", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "owned")
		require.NoError(t, os.Mkdir(path, 0o700))
		closeErr := errors.New("partially closed cluster")
		released := false
		err := cleanupReplayResources(func() {}, func() error {
			return closeErr
		}, func() error {
			return os.RemoveAll(path)
		}, func() error {
			released = true
			return nil
		})
		require.ErrorIs(t, err, closeErr)
		_, err = os.Stat(path)
		require.NoError(t, err)
		require.False(t, released)
	})
	t.Run("reports removal and release errors", func(t *testing.T) {
		removeErr := errors.New("remove failed")
		releaseErr := errors.New("release failed")
		err := cleanupReplayResources(func() {}, func() error { return nil },
			func() error { return removeErr }, func() error { return releaseErr })
		require.ErrorIs(t, err, removeErr)
		require.ErrorIs(t, err, releaseErr)
	})
}

func TestReplayCleanupAfterFaultRemoval(t *testing.T) {
	ctx := t.Context()
	const point = "first-replay-cleanup-wait"
	const waiters = "first-replay-cleanup-waiters"
	require.True(t, fault.Enable())
	t.Cleanup(func() { fault.Disable() })
	require.NoError(t, fault.AddFaultPoint(ctx, point, ":::", "wait", 0, "", false))
	var startup *replayStartup
	t.Cleanup(func() {
		_, _ = fault.RemoveFaultPoint(context.Background(), point)
		if startup != nil {
			<-startup.done
		}
	})
	require.NoError(t, fault.AddFaultPoint(ctx, waiters, ":::", "getwaiters", 0, point, false))
	startupErr := errors.New("CN startup failed")
	startup = startReplayCN(func() error {
		fault.TriggerFault(point)
		return startupErr
	})
	require.Eventually(t, func() bool {
		n, _, ok := fault.TriggerFault(waiters)
		return ok && n == 1
	}, 5*time.Second, 10*time.Millisecond)
	_, err := fault.RemoveFaultPoint(ctx, point)
	require.NoError(t, err)
	closed := false
	require.NoError(t, cleanupReplayResources(func() { <-startup.done }, func() error {
		closed = true
		return nil
	}, func() error { return nil }, func() error { return nil }))
	require.True(t, closed)
	require.ErrorIs(t, startup.err, startupErr)
}
