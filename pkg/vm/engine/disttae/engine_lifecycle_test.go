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

package disttae

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

func TestEngineGCSchedulerIsOwnedAndJoined(t *testing.T) {
	e := &Engine{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, e.StartGCScheduler(ctx))
	require.NoError(t, e.StartGCScheduler(ctx))
	require.NoError(t, e.Close())
	require.NoError(t, e.Close())
	require.Error(t, e.StartGCScheduler(context.Background()))
}

func TestEngineGCSchedulerRejectsCanceledContext(t *testing.T) {
	e := &Engine{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, e.StartGCScheduler(ctx), context.Canceled)
	require.NoError(t, e.Close())
}

func TestEngineCloseStopsGCSchedulerBeforePushClient(t *testing.T) {
	e := &Engine{}
	done := make(chan struct{})
	var pushClientClosed atomic.Bool
	e.gcSchedulerCancel = func() {
		pushClientClosed.Store(e.pClient.closed.Load())
		close(done)
	}
	e.gcSchedulerDone = done

	require.NoError(t, e.Close())
	require.False(t, pushClientClosed.Load())
	require.True(t, e.pClient.closed.Load())
}

// Observe the real constructor before any owner has been published. The option
// sentinel prevents unrelated initialization after test cleanup opens readiness.
func TestEngineConstructorReadinessCancellation(t *testing.T) {
	for _, pendingRefresh := range []bool{false, true} {
		name := "missing runtime cluster"
		if pendingRefresh {
			name = "builtin initial refresh pending"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sid := t.Name()
				rt := runtime.NewRuntime(metadata.ServiceType_CN, sid, logutil.GetGlobalLogger())
				runtime.SetupServiceBasedRuntime(sid, rt)
				rt.SetGlobalVariables(runtime.LockService, &prefetchTestLockService{id: sid})
				client := &pendingLogtailClusterClient{
					testHAKeeperClient: &testHAKeeperClient{}, started: make(chan struct{}),
				}
				var cluster clusterservice.MOCluster
				if pendingRefresh {
					cluster = clusterservice.NewMOCluster(sid, client, time.Hour)
					rt.SetGlobalVariables(runtime.ClusterService, cluster)
					<-client.started
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				finished := make(chan any, 1)
				published := make(chan struct{}, 1)
				const sentinel = "stop after readiness"
				go func() {
					defer func() { finished <- recover() }()
					New(ctx, sid, nil, nil, nil, nil, nil, 1,
						func(*Engine) { published <- struct{}{} },
						func(*Engine) { panic(sentinel) })
				}()
				synctest.Wait()
				cancel()
				synctest.Wait()
				blocked := false
				var result any
				select {
				case result = <-finished:
				default:
					blocked = true
				}
				select {
				case <-published:
					t.Error("constructor owner was published before readiness")
				default:
				}
				// Always retire the real dependency and the constructor before
				// reporting the failed cancellation contract.
				if pendingRefresh {
					cluster.Close()
				} else {
					cluster = clusterservice.NewMOCluster(sid, nil, time.Hour, clusterservice.WithDisableRefresh())
					rt.SetGlobalVariables(runtime.ClusterService, cluster)
				}
				if blocked {
					result = <-finished
				}
				cluster.Close()
				if !blocked {
					err, ok := result.(error)
					if !ok || !errors.Is(err, context.Canceled) {
						t.Errorf("constructor returned unexpected outcome after cancellation: %v", result)
					}
				}
				if blocked {
					if result != sentinel {
						t.Errorf("test cleanup stopped at unexpected constructor phase: %v", result)
					}
					t.Errorf("canceled Engine.New remains blocked before owner publication; only retiring/publishing the cluster releases it")
				}
			})
		})
	}
}

func TestEngineConstructorRejectsEndedContext(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline=%t", deadline), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			expected := context.Canceled
			if deadline {
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				expected = context.DeadlineExceeded
			} else {
				cancel()
			}
			defer cancel()
			published := false
			var failure any
			func() {
				defer func() { failure = recover() }()
				New(ctx, t.Name(), nil, nil, nil, nil, nil, 1, func(*Engine) { published = true })
			}()
			require.ErrorIs(t, failure.(error), expected)
			require.False(t, published)
		})
	}
}

func TestEngineConstructorReadyFailureRetiresOwner(t *testing.T) {
	sid := t.Name()
	rt := runtime.NewRuntime(metadata.ServiceType_CN, sid, logutil.GetGlobalLogger())
	runtime.SetupServiceBasedRuntime(sid, rt)
	rt.SetGlobalVariables(runtime.LockService, &prefetchTestLockService{id: sid})
	cluster := clusterservice.NewMOCluster(sid, nil, time.Hour, clusterservice.WithDisableRefresh())
	defer cluster.Close()
	rt.SetGlobalVariables(runtime.ClusterService, cluster)
	var owner *Engine
	const failure = "failure after owner publication"
	require.PanicsWithValue(t, failure, func() {
		New(context.Background(), sid, nil, nil, nil, nil, nil, 1, func(e *Engine) { owner = e }, func(*Engine) { panic(failure) })
	})
	require.NotNil(t, owner)
	require.True(t, owner.gcPool.IsClosed(), "real acquired pool must retire before constructor panic escapes")
	require.NoError(t, owner.Close())
	require.Error(t, owner.StartGCScheduler(context.Background()))
}

// The gate observes real deletion contexts and delegates successful deletion
// to the existing memory file service, so joining is checked against objects.
type workspaceGCFileService struct {
	mu sync.Mutex
	fileservice.FileService
	entered         chan context.Context
	release         chan struct{}
	returned        chan struct{}
	batches         [][]string
	waitForDeadline bool
}

func (f *workspaceGCFileService) Delete(ctx context.Context, names ...string) error {
	f.mu.Lock()
	f.batches = append(f.batches, append([]string(nil), names...))
	f.mu.Unlock()
	f.entered <- ctx
	if f.waitForDeadline {
		<-ctx.Done()
		<-f.release
		f.returned <- struct{}{}
		return ctx.Err()
	}
	<-f.release
	err := f.FileService.Delete(ctx, names...)
	f.returned <- struct{}{}
	return err
}

func TestEngineCloseJoinsWorkspaceGC(t *testing.T) {
	for _, count := range []int{0, 1, 999, 1000, 1001, 2001} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				op, stop := client.NewTestTxnOperator(ctx)
				defer stop()
				pool, err := ants.NewPool(3)
				require.NoError(t, err)
				fs := &workspaceGCFileService{FileService: newCleanFS(t), entered: make(chan context.Context, 3), release: make(chan struct{}), returned: make(chan struct{}, 3)}
				owner := &Engine{gcPool: pool, fs: fs}
				released := false
				defer func() {
					if !released {
						close(fs.release)
					}
					_ = owner.Close()
				}()
				stats := mockStatsList(t, count)
				names := make([]string, count)
				for i := range stats {
					names[i] = stats[i].ObjectName().String()
					require.NoError(t, writeObjectToFS(context.Background(), fs, names[i]))
				}
				txn := &Transaction{engine: owner, op: op}
				cancel() // Query cancellation must not abandon rollback object cleanup.
				submitted := make(chan error, 1)
				go func() { submitted <- txn.GCObjsByStats(stats...) }()
				if count == 0 {
					require.NoError(t, <-submitted)
					require.NoError(t, owner.Close())
					require.Empty(t, fs.batches)
					return
				}
				admitted := <-fs.entered
				_, bounded := admitted.Deadline()
				require.True(t, bounded)
				require.NoError(t, admitted.Err())
				closed := make(chan error, 2)
				go func() { closed <- owner.Close() }()
				time.Sleep(4 * time.Second) // Virtual time crosses the removed three-second close deadline.
				synctest.Wait()
				select {
				case err := <-closed:
					t.Fatalf("close returned before accepted deletion: %v", err)
				default:
				}
				require.Error(t, txn.GCObjsByStats(stats[0]))
				require.Error(t, owner.ResetGCWorkerPool(nil))
				close(fs.release)
				released = true
				require.NoError(t, <-submitted)
				require.NoError(t, <-closed)
				require.NoError(t, owner.Close())
				require.Zero(t, pool.Running())
				var deleted []string
				for _, batch := range fs.batches {
					require.LessOrEqual(t, len(batch), GCBatchOfFileCount)
					deleted = append(deleted, batch...)
				}
				require.ElementsMatch(t, names, deleted)
				for _, name := range names {
					_, err := fs.StatFile(context.Background(), name)
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), err)
				}
			})
		})
	}
}

func TestEngineCloseWaitsForExpiredGCToReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool, err := ants.NewPool(1)
		require.NoError(t, err)
		op, stop := client.NewTestTxnOperator(context.Background())
		defer stop()
		fs := &workspaceGCFileService{FileService: newCleanFS(t), entered: make(chan context.Context, 1), release: make(chan struct{}), returned: make(chan struct{}, 1), waitForDeadline: true}
		owner := &Engine{gcPool: pool, fs: fs}
		released := false
		defer func() {
			if !released {
				close(fs.release)
			}
			_ = owner.Close()
		}()
		txn := &Transaction{engine: owner, op: op}
		require.NoError(t, txn.GCObjsByStats(mockStatsList(t, 1)...))
		deletion := <-fs.entered
		closed := make(chan error, 1)
		go func() { closed <- owner.Close() }()
		deadline, ok := deletion.Deadline()
		require.True(t, ok)
		time.Sleep(time.Until(deadline))
		synctest.Wait()
		require.ErrorIs(t, deletion.Err(), context.DeadlineExceeded)
		select {
		case err := <-closed:
			t.Fatalf("close returned while storage still owns request: %v", err)
		default:
		}
		close(fs.release)
		released = true
		<-fs.returned
		require.NoError(t, <-closed)
		require.Zero(t, pool.Running())
		require.NoError(t, owner.Close())
	})
}

func TestEngineCloseDrainsAdmittedGCBatches(t *testing.T) {
	pool, err := ants.NewPool(1)
	require.NoError(t, err)
	op, stop := client.NewTestTxnOperator(context.Background())
	defer stop()
	fs := &workspaceGCFileService{FileService: newCleanFS(t), entered: make(chan context.Context, 3), release: make(chan struct{}), returned: make(chan struct{}, 3)}
	owner := &Engine{gcPool: pool, fs: fs}
	released := false
	defer func() {
		if !released {
			close(fs.release)
		}
		_ = owner.Close()
	}()
	txn := &Transaction{engine: owner, op: op}
	stats := mockStatsList(t, 2001)
	for _, stat := range stats {
		require.NoError(t, writeObjectToFS(context.Background(), fs, stat.ObjectName().String()))
	}
	submitted := make(chan error, 1)
	go func() { submitted <- txn.GCObjsByStats(stats...) }()
	<-fs.entered // The next submission is blocked by this occupied worker.
	closed := make(chan error, 2)
	go func() { closed <- owner.Close() }()
	go func() { closed <- owner.Close() }()
	require.Eventually(t, owner.dynamicCtx.closed.Load, 5*time.Second, time.Millisecond)
	require.Error(t, txn.GCObjsByStats(stats[0]))
	select {
	case err := <-closed:
		t.Fatalf("close abandoned admitted request: %v", err)
	default:
	}
	close(fs.release)
	released = true
	require.NoError(t, <-submitted)
	require.NoError(t, <-closed)
	require.NoError(t, <-closed)
	require.Len(t, fs.batches, 3)
	require.Zero(t, pool.Running())
	for _, stat := range stats {
		require.False(t, objectExistsInFS(context.Background(), fs, stat.ObjectName().String()))
	}
}
