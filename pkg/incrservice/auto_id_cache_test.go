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
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	mock_executor "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/stretchr/testify/require"
)

type autoIDCacheStore struct {
	IncrValueStore
	mu     sync.Mutex
	counts []int
}

func (s *autoIDCacheStore) Allocate(ctx context.Context, tableID uint64, key string, count int, txn client.TxnOperator) (uint64, uint64, timestamp.Timestamp, error) {
	s.mu.Lock()
	s.counts = append(s.counts, count)
	s.mu.Unlock()
	from, to, _, err := s.IncrValueStore.Allocate(ctx, tableID, key, count, txn)
	return from, to, timestamp.Timestamp{PhysicalTime: 100}, err
}

func (s *autoIDCacheStore) requests() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.counts...)
}

func TestAutoIDCacheConfig(t *testing.T) {
	base := Config{EnableAutoIDCache: true, CountPerAllocate: 80, LowCapacity: 17}
	for _, size := range []uint64{0, 1, 2, 100, MaxAutoIDCache} {
		cfg, err := base.forTable(t.Context(), size)
		require.NoError(t, err)
		if size == 0 {
			require.Equal(t, base, cfg)
		} else {
			require.Equal(t, int(size), cfg.CountPerAllocate)
			require.Equal(t, int(size/2), cfg.LowCapacity)
			require.Equal(t, size == 1, cfg.demandOnly)
		}
	}
	_, err := base.forTable(t.Context(), MaxAutoIDCache+1)
	require.ErrorContains(t, err, "AUTO_ID_CACHE")
	require.Equal(t, 80, base.CountPerAllocate)
}

func TestAutoIDCacheDemandOnly(t *testing.T) {
	for _, tc := range []struct {
		name              string
		values            []int64
		nulls             []bool
		increment, offset uint64
		want              []int64
		requests          []int
	}{
		{"automatic", []int64{0, 0, 0}, []bool{true, true, true}, 1, 1, []int64{1, 2, 3}, []int{3}},
		{"explicit_only", []int64{100, -1, 0}, []bool{false, false, false}, 1, 1, []int64{100, -1, 0}, nil},
		{"mixed", []int64{0, 100, 0}, []bool{true, false, true}, 1, 1, []int64{1, 100, 101}, []int{2, 1}},
		{"explicit_first", []int64{100, 0, 0}, []bool{false, true, true}, 1, 1, []int64{100, 101, 102}, []int{2, 2}},
		{"series", []int64{0, 0, 0}, []bool{true, true, true}, 3, 2, []int64{2, 5, 8}, []int{9}},
		{"negative_series", []int64{-1, 0, 100, 0}, []bool{false, true, false, true}, 3, 2, []int64{-1, 2, 100, 101}, []int{6, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				ctx := defines.AttachAccountId(t.Context(), catalog.System_Account)
				ctx = WithAutoIncrementOptions(ctx, tc.increment, tc.offset)
				store := &autoIDCacheStore{IncrValueStore: NewMemStore()}
				col := AutoColumn{ColName: "id", Step: 1, CacheSize: 1}
				require.NoError(t, store.Create(ctx, 0, []AutoColumn{col}, nil))
				a := newValueAllocator("", store)
				defer a.close()
				c, err := newColumnCache(ctx, "", 0, col, Config{EnableAutoIDCache: true, CountPerAllocate: 10000, LowCapacity: 5000}, true, a, nil)
				require.NoError(t, err)
				require.Empty(t, store.requests(), "construction must not reserve IDs")
				c.preAllocate(ctx, 0, 1000000, nil)
				require.NoError(t, c.maybeAllocate(ctx, 0, nil))
				require.Empty(t, store.requests(), "estimate and low-water paths must not reserve IDs")
				mp := mpool.MustNewZero()
				v := vector.NewVec(types.T_int64.ToType())
				for i, value := range tc.values {
					require.NoError(t, vector.AppendFixed(v, value, tc.nulls[i], mp))
				}
				_, err = c.insertAutoValues(ctx, 0, v, len(tc.values), nil)
				require.NoError(t, err)
				require.Equal(t, tc.want, vector.MustFixedColWithTypeCheck[int64](v))
				require.Equal(t, tc.requests, store.requests())
				v.Free(mp)
				require.Zero(t, mp.CurrNB())
			})
		})
	}
}

func TestAutoIDCacheExplicitSpanAndConcurrentDemand(t *testing.T) {
	for _, size := range []uint64{1, 8} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				ctx := WithAutoIncrementOptions(defines.AttachAccountId(t.Context(), catalog.System_Account), 3, 2)
				store := &autoIDCacheStore{IncrValueStore: NewMemStore()}
				require.NoError(t, store.Create(ctx, 42, []AutoColumn{{TableID: 42, ColName: "id", Step: 1, CacheSize: size}}, nil))
				s := NewIncrService("", store, Config{EnableAutoIDCache: true, CountPerAllocate: 10000}).(*service)
				defer s.Close()
				cache, err := s.acquireCommittedTableCache(ctx, 42)
				require.NoError(t, err)
				cache.release()
				require.Empty(t, store.requests(), "non-default series must not prefetch")
				type result struct {
					id    uint64
					err   error
					bytes int64
				}
				start := make(chan struct{})
				results := make(chan result, 2)
				var wg sync.WaitGroup
				for range 2 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						mp := mpool.MustNewZero()
						v := vector.NewVec(types.T_uint64.ToType())
						err := vector.AppendFixed(v, uint64(0), true, mp)
						<-start
						var id uint64
						if err == nil {
							id, err = s.InsertValues(ctx, 42, 0, nil, []*vector.Vector{v}, 1, 999999)
						}
						v.Free(mp)
						results <- result{id, err, mp.CurrNB()}
					}()
				}
				close(start)
				wg.Wait()
				close(results)
				var ids []uint64
				for result := range results {
					require.NoError(t, result.err)
					require.Zero(t, result.bytes)
					ids = append(ids, result.id)
				}
				require.ElementsMatch(t, []uint64{2, 5}, ids)
				requests := store.requests()
				if size == 1 {
					require.Equal(t, []int{3, 3}, requests, "waiting callers must not amplify demand-only spans")
				} else {
					require.Len(t, requests, 1)
					require.GreaterOrEqual(t, requests[0], 8)
					require.LessOrEqual(t, requests[0], 16, "table policy must replace the CN default")
				}
			})
		})
	}
}

func TestAutoIDCacheColdServicesAndProbe(t *testing.T) {
	client.RunTxnTests(func(tc client.TxnClient, _ rpc.TxnSender) {
		ctx := defines.AttachAccountId(t.Context(), catalog.System_Account)
		store := &autoIDCacheStore{IncrValueStore: NewMemStore()}
		require.NoError(t, store.Create(ctx, 42, []AutoColumn{{TableID: 42, ColName: "id", Step: 1, CacheSize: 1}}, nil))
		first := NewIncrService("", store, Config{EnableAutoIDCache: true}).(*service)
		second := NewIncrService("", store, Config{EnableAutoIDCache: true}).(*service)
		defer first.Close()
		defer second.Close()
		op, err := tc.New(ctx, timestamp.Timestamp{PhysicalTime: 50})
		require.NoError(t, err)
		defer op.Rollback(ctx)
		op.SetSnapshotTS(timestamp.Timestamp{PhysicalTime: 50})
		for _, s := range []*service{first, second} {
			value, err := s.CurrentValue(ctx, 42, "id")
			require.NoError(t, err)
			require.Equal(t, uint64(1), value)
			probe, err := s.GetLastAllocateTS(ctx, 42, 0, op, "id")
			require.NoError(t, err)
			require.Equal(t, op.SnapshotTS(), probe)
			_, err = s.GetLastAllocateTS(ctx, 42, 0, nil, "id")
			require.ErrorContains(t, err, "transaction snapshot")
		}
		require.Empty(t, store.requests())
		mp := mpool.MustNewZero()
		for i, s := range []*service{first, second, first} {
			v := vector.NewVec(types.T_uint64.ToType())
			require.NoError(t, vector.AppendFixed(v, uint64(0), true, mp))
			id, err := s.InsertValues(ctx, 42, 0, op, []*vector.Vector{v}, 1, 999999)
			v.Free(mp)
			require.NoError(t, err)
			require.Equal(t, uint64(i+1), id)
			value, err := second.CurrentValue(ctx, 42, "id")
			require.NoError(t, err)
			require.Equal(t, uint64(i+2), value)
		}
		require.Equal(t, []int{1, 1, 1}, store.requests())
		require.NoError(t, second.Reload(ctx, 42))
		value, err := second.CurrentValue(ctx, 42, "id")
		require.NoError(t, err)
		require.Equal(t, uint64(4), value)
		require.Equal(t, []int{1, 1, 1}, store.requests())

		// A type-overflow failure can leave an owned raw tail. Its real
		// allocation TS must win over a newer transaction snapshot.
		require.NoError(t, store.UpdateMinValue(ctx, 42, "id", 255, nil))
		v := vector.NewVec(types.T_uint8.ToType())
		require.NoError(t, vector.AppendFixed(v, uint8(0), true, mp))
		require.NoError(t, vector.AppendFixed(v, uint8(0), true, mp))
		_, err = second.InsertValues(ctx, 42, 0, op, []*vector.Vector{v}, 2, 0)
		v.Free(mp)
		require.Error(t, err)
		op.SetSnapshotTS(timestamp.Timestamp{PhysicalTime: 150})
		probe, err := second.GetLastAllocateTS(ctx, 42, 0, op, "id")
		require.NoError(t, err)
		require.Equal(t, timestamp.Timestamp{PhysicalTime: 100}, probe)
		require.Zero(t, mp.CurrNB())
	})
}

func TestAutoIDCacheCurrentValueOverflow(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		ctx := defines.AttachAccountId(t.Context(), catalog.System_Account)
		store := NewMemStore()
		require.NoError(t, store.Create(ctx, 42, []AutoColumn{{ColName: "id", Offset: math.MaxUint64, Step: 1, CacheSize: 1}}, nil))
		s := NewIncrService("", store, Config{EnableAutoIDCache: true})
		defer s.Close()
		_, err := s.CurrentValue(ctx, 42, "id")
		require.Error(t, err)
	})
}

func TestAutoIDCacheSQLMetadata(t *testing.T) {
	for _, size := range []uint64{0, 1, 64, MaxAutoIDCache + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			mp := mpool.MustNewZero()
			mem := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_int32.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_varchar.ToType()}, mp)
			mem.NewBatchWithRowCount(1)
			require.NoError(t, executor.AppendStringRows(mem, 0, []string{"id"}))
			require.NoError(t, executor.AppendFixedRows(mem, 1, []int32{0}))
			require.NoError(t, executor.AppendFixedRows(mem, 2, []uint64{123}))
			require.NoError(t, executor.AppendFixedRows(mem, 3, []uint64{1}))
			require.NoError(t, executor.AppendStringRows(mem, 4, []string{string(api.MustMarshalTblExtra(&api.SchemaExtra{AutoIdCache: size}))}))
			exec := mock_executor.NewMockSQLExecutor(gomock.NewController(t))
			exec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
				require.Contains(t, sql, "select extra_info from mo_tables where rel_id = 42")
				require.Contains(t, sql, "from mo_increment_columns where table_id = 7")
				return mem.GetResult(), nil
			})
			store := &sqlStore{exec: exec}
			// Reset reads old allocator rows but the replacement's live metadata.
			ctx := context.WithValue(t.Context(), autoColumnPolicyTableKey{}, uint64(42))
			cols, err := store.GetColumns(ctx, 7, nil)
			if size > MaxAutoIDCache {
				require.ErrorContains(t, err, "AUTO_ID_CACHE")
			} else {
				require.NoError(t, err)
				require.Equal(t, []AutoColumn{{TableID: 7, ColName: "id", Offset: 123, Step: 1, CacheSize: size}}, cols)
			}
			require.Zero(t, mp.CurrNB(), "GetColumns must close its SQL result on success and failure")
		})
	}
}
