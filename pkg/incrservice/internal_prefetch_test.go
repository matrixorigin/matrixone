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

package incrservice

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	mock_executor "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/stretchr/testify/require"
)

func internalFakePKColumn() AutoColumn {
	return GetAutoColumnFromDef(&plan.TableDef{TblId: 42, Cols: []*plan.ColDef{{
		Name: catalog.FakePrimaryKeyColName, Hidden: true,
		Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
	}}})[0]
}

func internalPrefetchTestMP(t *testing.T) *mpool.MPool {
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB(), "test-owned vectors and SQL results must be freed before pool deletion")
	})
	return mp
}

// Use actual CREATE/commit ownership and a counting store, rather than a
// timing assertion. MemStore does not simulate SQL workspace scanning.
func TestInternalAutoIDPrefetchBulkDemand(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("committed=%t", committed), func(t *testing.T) {
			client.RunTxnTests(func(tc client.TxnClient, _ rpc.TxnSender) {
				ctx, cancel := context.WithTimeout(defines.AttachAccountId(t.Context(), catalog.System_Account), 30*time.Second)
				defer cancel()
				store := &autoIDCacheStore{IncrValueStore: NewMemStore()}
				s := NewIncrService("", store, Config{}).(*service)
				defer s.Close()
				op, err := tc.New(ctx, timestamp.Timestamp{})
				require.NoError(t, err)
				defer op.Rollback(ctx)
				require.NoError(t, s.Create(ctx, 42, []AutoColumn{internalFakePKColumn()}, op))
				insertTxn := op
				if committed {
					require.NoError(t, op.Commit(ctx))
					insertTxn = nil
				}
				mp := internalPrefetchTestMP(t)
				next := uint64(1)
				const actualRows = 2_000_000
				for consumed := 0; consumed < actualRows; {
					rows := min(8192, actualRows-consumed)
					func() {
						v := vector.NewVec(types.T_uint64.ToType())
						defer v.Free(mp)
						vecErr := vector.AppendFixedList(v, make([]uint64, rows), nil, mp)
						require.NoError(t, vecErr)
						v.GetNulls().AddRange(0, uint64(rows))
						_, insertErr := s.InsertValues(ctx, 42, 0, insertTxn, []*vector.Vector{v}, rows, math.MaxInt64-1)
						require.NoError(t, insertErr)
						for _, id := range vector.MustFixedColWithTypeCheck[uint64](v) {
							if id != next {
								t.Fatalf("generated ID %d, expected %d", id, next)
							}
							next++
						}
					}()
					consumed += rows
				}
				cache, err := s.getTableCache(ctx, 42)
				require.NoError(t, err)
				cc := cache.(*tableCache).getColumnCache(catalog.FakePrimaryKeyColName)
				cc.Lock()
				err = cc.waitPrevAllocatingLocked(ctx)
				consumed := cc.internalConsumedRows
				cc.Unlock()
				require.NoError(t, err)
				require.Equal(t, maxInternalAutoIDPrefetch, consumed)
				requests := store.requests()
				require.Less(t, len(requests), 20, "actual bulk demand should avoid 10k-sized allocation RPCs")
				require.Equal(t, defaultCountPerAllocate, requests[0], "a planner estimate cannot enlarge the cold reservation")
				for _, request := range requests {
					require.LessOrEqual(t, request, maxInternalAutoIDPrefetch)
				}
				require.Equal(t, uint64(actualRows+1), next)
				require.Zero(t, mp.CurrNB())
			})
		})
	}
}

type internalPrefetchObserver struct {
	valueAllocator
	counts []int
}

func (a *internalPrefetchObserver) asyncAllocate(_ context.Context, _ uint64, _ string, count int, _ client.TxnOperator, _ func(uint64, uint64, timestamp.Timestamp, error)) error {
	a.counts = append(a.counts, count)
	return errors.New("reservation deliberately rejected")
}

func TestInternalAutoIDPrefetchQualification(t *testing.T) {
	for _, tc := range []struct {
		name     string
		column   AutoColumn
		cfg      Config
		series   bool
		adaptive bool
	}{
		{name: "hidden_default", column: internalFakePKColumn(), adaptive: true},
		{name: "visible_same_name", column: AutoColumn{ColName: catalog.FakePrimaryKeyColName, Step: 1}},
		{name: "visible_user_id", column: AutoColumn{ColName: "id", Step: 1}},
		{name: "other_hidden_name", column: AutoColumn{ColName: "other", Step: 1, isInternal: true}},
		{name: "explicit_default_size", column: internalFakePKColumn(), cfg: Config{CountPerAllocate: defaultCountPerAllocate}},
		{name: "explicit_custom_size", column: internalFakePKColumn(), cfg: Config{CountPerAllocate: 20000}},
		{name: "table_cache_policy", column: AutoColumn{ColName: catalog.FakePrimaryKeyColName, Step: 1, isInternal: true, CacheSize: 10000}, cfg: Config{EnableAutoIDCache: true}},
		{name: "demand_only", column: AutoColumn{ColName: catalog.FakePrimaryKeyColName, Step: 1, isInternal: true, CacheSize: 1}, cfg: Config{EnableAutoIDCache: true}},
		{name: "non_default_step", column: AutoColumn{ColName: catalog.FakePrimaryKeyColName, Step: 2, isInternal: true}},
		{name: "non_default_series", column: internalFakePKColumn(), series: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			if tc.series {
				ctx = WithAutoIncrementOptions(ctx, 3, 2)
			}
			tc.cfg.adjust()
			cfg, err := tc.cfg.forTable(ctx, tc.column.CacheSize)
			require.NoError(t, err)
			a := &internalPrefetchObserver{}
			c := &columnCache{col: tc.column, cfg: cfg, ranges: &ranges{step: tc.column.Step}, allocator: a, internalConsumedRows: 500000}
			c.preAllocate(ctx, 42, 8192, nil)
			if tc.series || cfg.demandOnly {
				require.Empty(t, a.counts)
			} else if tc.adaptive {
				require.Equal(t, []int{500000}, a.counts)
			} else {
				require.Equal(t, []int{max(8192, cfg.CountPerAllocate)}, a.counts)
			}
			require.False(t, c.allocating, "failed allocation must finish its existing ownership transition")
			before := len(a.counts)
			c.retire()
			c.preAllocate(ctx, 42, 8192, nil)
			require.Len(t, a.counts, before, "retirement cannot schedule an adaptive reservation")
		})
	}
}

func TestInternalAutoIDOwnershipFromDefinitionAndSQL(t *testing.T) {
	for _, hidden := range []bool{false, true} {
		def := &plan.TableDef{Cols: []*plan.ColDef{{Name: catalog.FakePrimaryKeyColName, Hidden: hidden, Typ: plan.Type{AutoIncr: true}}}}
		require.Equal(t, hidden, GetAutoColumnFromDef(def)[0].isInternal)
	}
	def := &plan.TableDef{Cols: []*plan.ColDef{{Name: "id", Hidden: true, Typ: plan.Type{AutoIncr: true}}}}
	// Other internal allocators are not automatically eligible for fake-PK policy.
	// The ownership marker itself is restricted to the exact system column.
	require.False(t, GetAutoColumnFromDef(def)[0].isInternal)
	for _, tc := range []struct {
		name         string
		lookup       bool
		null         bool
		reset        bool
		visibleName  string
		wantInternal bool
	}{
		{name: "hidden", lookup: true, visibleName: catalog.FakePrimaryKeyColName, wantInternal: true},
		{name: "visible_same_name", visibleName: catalog.FakePrimaryKeyColName},
		{name: "missing_metadata", null: true, visibleName: catalog.FakePrimaryKeyColName},
		{name: "duplicate_metadata_fails_closed", visibleName: catalog.FakePrimaryKeyColName},
		{name: "other_user_column", lookup: true, visibleName: "id"},
		{name: "reset_new_schema", lookup: true, reset: true, visibleName: catalog.FakePrimaryKeyColName, wantInternal: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := internalPrefetchTestMP(t)
			exec := mock_executor.NewMockSQLExecutor(gomock.NewController(t))
			exec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, query string, _ executor.Options) (executor.Result, error) {
				policyTableID, allocatorTableID := 42, 42
				if tc.reset {
					allocatorTableID = 7
				}
				require.Contains(t, query, fmt.Sprintf("att_relname_id = %d", policyTableID))
				require.Contains(t, query, fmt.Sprintf("where table_id = %d", allocatorTableID))
				require.Contains(t, query, "count(*) = 1 and max(att_is_hidden) = 1", "duplicate names cannot establish hidden ownership")
				mem := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_int32.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_bool.ToType()}, mp)
				transferred := false
				defer func() {
					if !transferred {
						mem.GetResult().Close()
					}
				}()
				mem.NewBatchWithRowCount(1)
				require.NoError(t, executor.AppendStringRows(mem, 0, []string{tc.visibleName}))
				require.NoError(t, executor.AppendFixedRows(mem, 1, []int32{0}))
				require.NoError(t, executor.AppendFixedRows(mem, 2, []uint64{123}))
				require.NoError(t, executor.AppendFixedRows(mem, 3, []uint64{1}))
				require.NoError(t, executor.AppendStringRows(mem, 4, []string{""}))
				require.NoError(t, executor.AppendFixedRows(mem, 5, []bool{tc.lookup}))
				result := mem.GetResult()
				if tc.null {
					result.Batches[0].Vecs[5].SetNull(0)
				}
				transferred = true
				return result, nil
			})
			ctx, tableID := WithAutoIDCachePolicy(t.Context(), 42, 0), uint64(42)
			if tc.reset {
				ctx = context.WithValue(ctx, autoColumnPolicyTableKey{}, uint64(42))
				tableID = 7
			}
			cols, err := (&sqlStore{exec: exec}).GetColumns(ctx, tableID, nil)
			require.NoError(t, err)
			require.Len(t, cols, 1)
			require.Equal(t, tc.wantInternal, cols[0].isInternal)
			require.Zero(t, mp.CurrNB(), "ownership observation must close its SQL result")
		})
	}
}

func TestInternalAutoIDPrefetchFailureAndTerminal(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		cfg := Config{}
		cfg.adjust()
		c := &columnCache{col: internalFakePKColumn(), cfg: cfg, ranges: &ranges{step: 1}}
		c.ranges.add(1, 3)
		errRejected := errors.New("reject generated row")
		err := c.applyAutoValues(t.Context(), 42, 2, nil, func(int) bool { return false }, func(int, uint64) error { return errRejected }, nil, NormalizeAutoIncrementOptions(1, 1), 2)
		require.ErrorIs(t, err, errRejected)
		require.Zero(t, c.internalConsumedRows, "partial failed batches cannot grow reservation policy")
		ctx := defines.AttachAccountId(t.Context(), catalog.System_Account)
		store := &autoIDCacheStore{IncrValueStore: NewMemStore()}
		col := internalFakePKColumn()
		col.Offset = math.MaxUint64 - 3
		require.NoError(t, store.Create(ctx, 42, []AutoColumn{col}, nil))
		a := newValueAllocator("", store)
		defer a.close()
		cc, err := newColumnCache(ctx, "", 42, col, cfg, false, a, nil)
		require.NoError(t, err)
		mp := internalPrefetchTestMP(t)
		v := vector.NewVec(types.T_uint64.ToType())
		defer v.Free(mp)
		require.NoError(t, vector.AppendFixedList(v, []uint64{0, 0, 0}, nil, mp))
		v.GetNulls().AddRange(0, 3)
		_, err = cc.insertAutoValues(ctx, 42, v, 3, nil)
		require.NoError(t, err)
		require.Equal(t, []uint64{math.MaxUint64 - 2, math.MaxUint64 - 1, math.MaxUint64}, vector.MustFixedColWithTypeCheck[uint64](v))
		require.Equal(t, 3, cc.internalConsumedRows)
		v.GetNulls().AddRange(0, 3)
		_, err = cc.insertAutoValues(ctx, 42, v, 3, nil)
		require.Error(t, err, "terminal max ID may only be consumed once")
		require.Equal(t, 3, cc.internalConsumedRows)
	})
}

func TestInternalAutoIDPrefetchMultipleCNAndColdReload(t *testing.T) {
	client.RunTxnTests(func(tc client.TxnClient, _ rpc.TxnSender) {
		ctx, cancel := context.WithTimeout(defines.AttachAccountId(t.Context(), catalog.System_Account), 30*time.Second)
		defer cancel()
		store := NewMemStore()
		first := NewIncrService("", store, Config{}).(*service)
		second := NewIncrService("", store, Config{}).(*service)
		defer first.Close()
		defer second.Close()
		op, err := tc.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		defer op.Rollback(ctx)
		require.NoError(t, first.Create(ctx, 42, []AutoColumn{internalFakePKColumn()}, op))
		require.NoError(t, op.Commit(ctx))
		type result struct {
			ids   []uint64
			err   error
			bytes int64
		}
		results := make(chan result, 2)
		var wg sync.WaitGroup
		for _, s := range []*service{first, second} {
			wg.Add(1)
			go func(s *service) {
				defer wg.Done()
				mp := mpool.MustNewZero()
				defer mpool.DeleteMPool(mp)
				r := result{}
				for range 8 {
					v := vector.NewVec(types.T_uint64.ToType())
					r.err = vector.AppendFixedList(v, make([]uint64, 8192), nil, mp)
					v.GetNulls().AddRange(0, 8192)
					if r.err == nil {
						_, r.err = s.InsertValues(ctx, 42, 0, nil, []*vector.Vector{v}, 8192, math.MaxInt64-1)
					}
					if r.err == nil {
						r.ids = append(r.ids, vector.MustFixedColWithTypeCheck[uint64](v)...)
					}
					v.Free(mp)
					if r.err != nil {
						break
					}
				}
				r.bytes = mp.CurrNB()
				results <- r
			}(s)
		}
		wg.Wait()
		close(results)
		seen := make(map[uint64]struct{})
		for r := range results {
			require.NoError(t, r.err)
			require.Zero(t, r.bytes)
			for _, id := range r.ids {
				require.NotZero(t, id)
				if _, duplicate := seen[id]; duplicate {
					t.Fatalf("duplicate generated ID across CN caches: %d", id)
				}
				seen[id] = struct{}{}
			}
		}
		require.Len(t, seen, 2*8*8192)
		require.NoError(t, first.Reload(ctx, 42))
		cache, err := first.acquireCommittedTableCache(ctx, 42)
		require.NoError(t, err)
		defer cache.release()
		cc := cache.(*tableCache).getColumnCache(catalog.FakePrimaryKeyColName)
		require.True(t, cc.col.isInternal, "cold store reload retains authoritative hidden ownership")
		require.Zero(t, cc.internalConsumedRows, "a new owner starts with no inferred demand history")
	})
}
