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

package incrservice

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	mock_executor "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/stretchr/testify/require"
)

func TestAutoIDCacheSQLStoreRequiresBarrier(t *testing.T) {
	exec := mock_executor.NewMockSQLExecutor(gomock.NewController(t))
	_, err := NewSQLStore(exec, nil, nil)
	require.ErrorContains(t, err, "logtail read barrier")
	store, err := NewSQLStore(exec, nil, func(context.Context) (timestamp.Timestamp, error) {
		return timestamp.Timestamp{}, nil
	})
	require.NoError(t, err)
	require.NotNil(t, store)
}

func TestAutoIDCachePointObservation(t *testing.T) {
	client.RunTxnTests(func(tc client.TxnClient, _ rpc.TxnSender) {
		txn, err := tc.New(t.Context(), timestamp.Timestamp{})
		require.NoError(t, err)
		defer txn.Rollback(t.Context())
		mp := mpool.MustNewZero()
		t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
		exec := mock_executor.NewMockSQLExecutor(gomock.NewController(t))
		frontier := timestamp.Timestamp{PhysicalTime: 99}
		barrierCalls := 0
		store := &sqlStore{exec: exec, acquireLogtailReadFence: func(context.Context) (timestamp.Timestamp, error) {
			barrierCalls++
			return frontier, nil
		}}
		allocator := &countingAllocator{}
		cfg := Config{EnableAutoIDCache: true}
		cfg.adjust()
		cache, err := newTableCache(t.Context(), "", 42, 0, []AutoColumn{{TableID: 42, ColName: "i'd", Step: 1, CacheSize: 1}}, cfg, allocator, txn, false)
		require.NoError(t, err)
		defer cache.close()
		calls := 0
		exec.EXPECT().Exec(gomock.Any(), "select offset, step from mo_increment_columns where table_id = 42 and col_name = 'i''d'", gomock.Any()).Times(4).DoAndReturn(func(ctx context.Context, query string, opts executor.Options) (executor.Result, error) {
			calls++
			if calls <= 2 {
				require.Same(t, txn, opts.Txn())
				require.True(t, opts.DisableIncrStatement())
			} else {
				require.Nil(t, opts.Txn())
				require.False(t, opts.DisableIncrStatement())
				require.Equal(t, frontier, opts.MinCommittedTS())
			}
			result := executor.NewMemResult([]types.Type{types.T_uint64.ToType(), types.T_uint64.ToType()}, mp)
			result.NewBatchWithRowCount(1)
			require.NoError(t, executor.AppendFixedRows(result, 0, []uint64{uint64(calls * 10)}))
			require.NoError(t, executor.AppendFixedRows(result, 1, []uint64{2}))
			return result.GetResult(), nil
		})
		for i := 1; i <= 4; i++ {
			if i == 3 {
				cache.commit()
			}
			value, err := cache.currentValue(t.Context(), 42, "i'd", store)
			require.NoError(t, err)
			require.Equal(t, uint64(i*10+2), value, "每次观测必须看到最新allocator值，而不是缓存上次观测")
			require.Zero(t, mp.CurrNB())
		}
		require.Equal(t, 4, calls)
		require.Equal(t, 2, barrierCalls, "private transaction observations must not acquire an external fence")
		require.Zero(t, allocator.asyncCalls.Load())
	})
}

func TestAutoIDCachePointObservationWaitsForBarrierBeforeRead(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	entered := make(chan struct{})
	release := make(chan struct{})
	executed := make(chan struct{})
	frontier := timestamp.Timestamp{PhysicalTime: 99}
	exec := mock_executor.NewMockSQLExecutor(gomock.NewController(t))
	exec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ string, opts executor.Options) (executor.Result, error) {
		require.Equal(t, frontier, opts.MinCommittedTS())
		close(executed)
		result := executor.NewMemResult([]types.Type{types.T_uint64.ToType(), types.T_uint64.ToType()}, mp)
		result.NewBatchWithRowCount(1)
		require.NoError(t, executor.AppendFixedRows(result, 0, []uint64{10}))
		require.NoError(t, executor.AppendFixedRows(result, 1, []uint64{1}))
		return result.GetResult(), nil
	})
	store := &sqlStore{exec: exec, acquireLogtailReadFence: func(ctx context.Context) (timestamp.Timestamp, error) {
		close(entered)
		select {
		case <-release:
			return frontier, nil
		case <-ctx.Done():
			return timestamp.Timestamp{}, context.Cause(ctx)
		}
	}}
	type observation struct {
		offset uint64
		step   uint64
		err    error
	}
	result := make(chan observation, 1)
	go func() {
		offset, step, err := store.GetColumnValue(t.Context(), 42, "id", nil)
		result <- observation{offset: offset, step: step, err: err}
	}()
	<-entered
	select {
	case <-executed:
		t.Fatal("allocator read started before the logtail barrier completed")
	default:
	}
	close(release)
	got := <-result
	require.NoError(t, got.err)
	require.Equal(t, uint64(10), got.offset)
	require.Equal(t, uint64(1), got.step)
	<-executed
}

func TestAutoIDCachePointObservationBarrierErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		barrier logtailReadBarrier
		want    error
	}{
		{name: "missing", want: moerr.NewInternalErrorNoCtx("AUTO_INCREMENT observation requires a logtail read barrier")},
		{name: "canceled", barrier: func(context.Context) (timestamp.Timestamp, error) {
			return timestamp.Timestamp{}, context.Canceled
		}, want: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := mock_executor.NewMockSQLExecutor(gomock.NewController(t))
			_, _, err := (&sqlStore{exec: exec, acquireLogtailReadFence: tc.barrier}).GetColumnValue(t.Context(), 42, "id", nil)
			require.Error(t, err)
			if tc.name == "canceled" {
				require.ErrorIs(t, err, tc.want)
			} else {
				require.ErrorContains(t, err, "logtail read barrier")
			}
		})
	}
}

func TestAutoIDCachePointObservationErrors(t *testing.T) {
	for _, tc := range []struct {
		name         string
		rows         int
		offset, step uint64
		err          error
	}{
		{name: "missing", rows: 0}, {name: "duplicate", rows: 2},
		{name: "query", err: errors.New("read failed")}, {name: "cancel", err: context.Canceled},
		{name: "zero-step", rows: 1, offset: 1}, {name: "overflow", rows: 1, offset: math.MaxUint64, step: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			exec := mock_executor.NewMockSQLExecutor(gomock.NewController(t))
			exec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, string, executor.Options) (executor.Result, error) {
				if tc.err != nil {
					return executor.Result{}, tc.err
				}
				result := executor.NewMemResult([]types.Type{types.T_uint64.ToType(), types.T_uint64.ToType()}, mp)
				for range tc.rows {
					result.NewBatchWithRowCount(1)
					require.NoError(t, executor.AppendFixedRows(result, 0, []uint64{tc.offset}))
					require.NoError(t, executor.AppendFixedRows(result, 1, []uint64{tc.step}))
				}
				return result.GetResult(), nil
			})
			cfg := Config{EnableAutoIDCache: true}
			cfg.adjust()
			cache, err := newTableCache(t.Context(), "", 42, 0, []AutoColumn{{TableID: 42, ColName: "id", Step: 1, CacheSize: 1}}, cfg, &countingAllocator{}, nil, true)
			require.NoError(t, err)
			defer cache.close()
			store := &sqlStore{exec: exec, acquireLogtailReadFence: func(context.Context) (timestamp.Timestamp, error) {
				return timestamp.Timestamp{PhysicalTime: 99}, nil
			}}
			_, err = cache.currentValue(t.Context(), 42, "id", store)
			require.Error(t, err)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
			}
			require.Zero(t, mp.CurrNB())
		})
	}
}
