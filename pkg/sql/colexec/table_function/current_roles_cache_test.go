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

package table_function

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mockfrontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	executortest "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/stretchr/testify/require"
)

// observedDoneContext signals when get evaluates the waiter cancellation case.
// With the loader still blocked, this proves the consumer joined the loading
// generation before it is retired.
type observedDoneContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (c *observedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestCurrentRolesCacheRetirementWakesWaitingConsumer(t *testing.T) {
	cache := NewCurrentRolesExecutionCache(context.Background())
	defer cache.Close()
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	loaderDone := make(chan error, 1)
	go func() {
		_, err := cache.get(context.Background(), currentRolesCacheKey{}, func(ctx context.Context) ([]int64, error) {
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-release
			return []int64{42}, nil
		})
		loaderDone <- err
	}()
	<-entered
	waiterCtx := &observedDoneContext{Context: context.Background(), waiting: make(chan struct{})}
	waiterDone := make(chan error, 1)
	go func() {
		_, err := cache.get(waiterCtx, currentRolesCacheKey{}, nil)
		waiterDone <- err
	}()
	<-waiterCtx.waiting
	failure := errors.New("merge failed")
	CloseCurrentRolesExecutionCache(WithCurrentRolesExecutionCache(context.Background(), cache), failure)
	<-canceled
	require.ErrorIs(t, <-waiterDone, failure)
	// Retiring a cache does not wait for an uncooperative old loader, and its
	// late result cannot publish into either the retired or the next generation.
	next := NewCurrentRolesExecutionCache(context.Background())
	defer next.Close()
	roles, err := next.get(context.Background(), currentRolesCacheKey{}, func(context.Context) ([]int64, error) { return []int64{7}, nil })
	require.NoError(t, err)
	require.Equal(t, []int64{7}, roles)
	unblock()
	require.ErrorIs(t, <-loaderDone, failure)
	roles, err = next.get(context.Background(), currentRolesCacheKey{}, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{7}, roles)
}

func TestCurrentRolesCacheConcurrentAndGenerations(t *testing.T) {
	c := NewCurrentRolesExecutionCache(context.Background())
	defer c.Close()
	key := currentRolesCacheKey{txnID: "first"}
	entered, unblock := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	loader := func(context.Context) ([]int64, error) {
		calls.Add(1)
		close(entered)
		<-unblock
		return []int64{1, 2}, nil
	}
	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			roles, err := c.get(context.Background(), key, loader)
			if err == nil && (len(roles) != 2 || roles[1] != 2) {
				err = errors.New("wrong roles")
			}
			errs <- err
		}()
	}
	<-entered
	_, err := c.get(context.Background(), currentRolesCacheKey{txnID: "second"}, loader)
	require.ErrorContains(t, err, "identity or read view changed")
	close(unblock)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, calls.Load())
	c.Close()
	c.Close()
	_, err = c.get(context.Background(), key, loader)
	require.ErrorIs(t, err, context.Canceled)
	next := NewCurrentRolesExecutionCache(context.Background())
	defer next.Close()
	roles, err := next.get(context.Background(), key, func(context.Context) ([]int64, error) { return []int64{3}, nil })
	require.NoError(t, err)
	require.Equal(t, []int64{3}, roles)
}

func TestCurrentRolesCacheCancellationAndFailure(t *testing.T) {
	t.Run("waiter and loader have independent contexts", func(t *testing.T) {
		c := NewCurrentRolesExecutionCache(context.Background())
		defer c.Close()
		entered, finish := make(chan struct{}), make(chan struct{})
		firstCtx, cancelFirst := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := c.get(firstCtx, currentRolesCacheKey{}, func(ctx context.Context) ([]int64, error) { close(entered); <-finish; return []int64{7}, ctx.Err() })
			result <- err
		}()
		<-entered
		cancelFirst()
		close(finish)
		require.ErrorIs(t, <-result, context.Canceled)
		roles, err := c.get(context.Background(), currentRolesCacheKey{}, nil)
		require.NoError(t, err)
		require.Equal(t, []int64{7}, roles)
	})
	t.Run("close cancels loader and wakes waiter", func(t *testing.T) {
		c := NewCurrentRolesExecutionCache(context.Background())
		entered := make(chan struct{})
		result := make(chan error, 2)
		go func() {
			_, err := c.get(context.Background(), currentRolesCacheKey{}, func(ctx context.Context) ([]int64, error) { close(entered); <-ctx.Done(); return nil, ctx.Err() })
			result <- err
		}()
		<-entered
		go func() { _, err := c.get(context.Background(), currentRolesCacheKey{}, nil); result <- err }()
		c.Close()
		require.ErrorIs(t, <-result, context.Canceled)
		require.ErrorIs(t, <-result, context.Canceled)
	})
	for _, panicLoader := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "panic"}[panicLoader], func(t *testing.T) {
			c := NewCurrentRolesExecutionCache(context.Background())
			defer c.Close()
			var calls int
			load := func(context.Context) ([]int64, error) {
				calls++
				if panicLoader {
					panic("loader panic")
				}
				return []int64{1}, errors.New("failure")
			}
			roles, err := c.get(context.Background(), currentRolesCacheKey{}, load)
			require.Error(t, err)
			require.Nil(t, roles)
			roles, err = c.get(context.Background(), currentRolesCacheKey{}, load)
			require.Error(t, err)
			require.Nil(t, roles)
			require.Equal(t, 1, calls)
		})
	}
}

func TestCurrentRolesCacheRunsCatalogOnceAndRejectsChangedView(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcess(t)
	id := process.CurrentRoleContext{Mode: 1, AccountID: 7, AccountName: "tenant", UserID: 8, UserName: "alice", PrimaryRoleID: 1}
	ctx := defines.AttachAccountId(context.Background(), id.AccountID)
	cache := NewCurrentRolesExecutionCache(ctx)
	defer cache.Close()
	proc.Ctx = WithCurrentRolesExecutionCache(ctx, cache)
	proc.Base.SessionInfo.CurrentRoleContext = id
	tx := mockfrontend.NewMockTxnOperator(ctrl)
	ws := mockfrontend.NewMockWorkspace(ctrl)
	proc.Base.TxnOperator = tx
	tx.EXPECT().GetWorkspace().Return(ws).AnyTimes()
	tx.EXPECT().Txn().Return(txn.TxnMeta{ID: []byte("txn")}).AnyTimes()
	ws.EXPECT().GetSnapshotWriteOffset().Return(2).AnyTimes()
	offset := uint64(3)
	ws.EXPECT().WriteOffset().DoAndReturn(func() uint64 { return offset }).AnyTimes()
	rt := moruntime.ServiceRuntime(proc.GetService())
	old, existed := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	sqlExec := executortest.NewMockSQLExecutor(ctrl)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, sqlExec)
	t.Cleanup(func() {
		if existed {
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, old)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, sqlExec)
		}
	})
	calls := 0
	sqlExec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string, opts executor.Options) (executor.Result, error) {
		calls++
		require.Same(t, tx, opts.Txn())
		require.True(t, opts.DisableIncrStatement())
		var rows []int64
		switch calls {
		case 1:
			require.Contains(t, sql, "mo_account")
			rows = []int64{7}
		case 2:
			require.Contains(t, sql, "mo_user WHERE")
			rows = []int64{8}
		case 3:
			require.Contains(t, sql, "mo_role_grant")
			rows = []int64{5}
		case 4:
			require.Contains(t, sql, "mo_role_grant")
		default:
			t.Fatal("duplicate catalog load")
		}
		result := executor.NewMemResult([]types.Type{types.T_int64.ToType()}, proc.Mp())
		result.NewBatchWithRowCount(len(rows))
		require.NoError(t, executor.AppendFixedRows(result, 0, rows))
		return result.GetResult(), nil
	}).Times(4)
	for i := 0; i < 3; i++ {
		roles, err := getCurrentRoles(proc)
		require.NoError(t, err)
		require.Equal(t, []int64{1, 5}, roles)
	}
	require.Equal(t, 4, calls)
	// A cached result must not bypass the execution-time protocol gate.
	previousVersion, versionExisted := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if versionExisted {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previousVersion)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion12)
		}
	})
	tf := &TableFunction{Attrs: []string{"role_id"}}
	tf.ctr.retSchema = []types.Type{types.T_int64.ToType()}
	s := &currentRolesState{}
	defer s.free(tf, proc, false, nil)
	for _, version := range []int64{defines.MORPCVersion12, defines.MORPCVersion11, defines.MORPCVersion12} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		err := s.start(tf, proc, 0, nil)
		if version == defines.MORPCVersion11 {
			require.ErrorContains(t, err, "requires protocol version 12")
			require.Equal(t, 0, s.batch.RowCount())
		} else {
			require.NoError(t, err)
			require.Equal(t, []int64{1, 5}, vector.MustFixedColNoTypeCheck[int64](s.batch.Vecs[0]))
		}
		require.Equal(t, 4, calls)
	}
	offset++
	_, err := getCurrentRoles(proc)
	require.ErrorContains(t, err, "identity or read view changed")
}

func TestCurrentRolesCacheCloseDoesNotWaitForLateLoader(t *testing.T) {
	old := NewCurrentRolesExecutionCache(context.Background())
	entered, release := make(chan struct{}), make(chan struct{})
	oldResult := make(chan error, 1)
	go func() {
		_, err := old.get(context.Background(), currentRolesCacheKey{txnID: "same"}, func(context.Context) ([]int64, error) {
			close(entered)
			<-release // Deliberately model I/O that has not yet observed cancellation.
			return []int64{1}, nil
		})
		oldResult <- err
	}()
	<-entered
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	old.Close()

	next := NewCurrentRolesExecutionCache(context.Background())
	defer next.Close()
	key := currentRolesCacheKey{txnID: "same"}
	roles, err := next.get(context.Background(), key, func(context.Context) ([]int64, error) { return []int64{2}, nil })
	require.NoError(t, err)
	require.Equal(t, []int64{2}, roles)
	releaseOnce.Do(func() { close(release) })
	require.ErrorIs(t, <-oldResult, context.Canceled)
	roles, err = next.get(context.Background(), key, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{2}, roles)
}
