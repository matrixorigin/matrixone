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

package disttae

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/mock/gomock"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/stretchr/testify/require"
)

type statsDispatchExecutor struct {
	ie.InternalExecutor
	err error
}

func (e statsDispatchExecutor) Query(context.Context, string, ie.SessionOverrideOptions) ie.InternalExecResult {
	return statsDispatchResult{err: e.err}
}

type statsDispatchResult struct {
	ie.InternalExecResult
	err error
}

func (r statsDispatchResult) Error() error { return r.err }

// Exercise the actual SQL builtin and catalog with A's process after B has
// initialized and closed. Distinct errors identify which executor was used.
func TestMoTableStatsUsesCurrentEngine(t *testing.T) {
	oldRows, oldSize := function.GetMoTableRowsFunc.Load(), function.GetMoTableSizeFunc.Load()
	t.Cleanup(func() {
		function.GetMoTableRowsFunc.Store(oldRows)
		function.GetMoTableSizeFunc.Store(oldSize)
	})
	first, second := &Engine{}, &Engine{}
	want := errors.New("current engine statistics")
	for i, owner := range []*Engine{first, second} {
		ctx, cancel := context.WithCancel(t.Context())
		owner.config.ieFactory = func() ie.InternalExecutor {
			if i == 0 {
				return statsDispatchExecutor{err: want}
			}
			return statsDispatchExecutor{err: errors.New("retired foreign engine statistics")}
		}
		require.NoError(t, initMoTableStatsConfig(ctx, owner))
		t.Cleanup(func() { cancel(); require.NoError(t, owner.Close()) })
		if i == 1 {
			cancel()
			require.NoError(t, owner.Close())
		}
	}
	first.catalog.Store(cache.NewCatalog())
	op, txn := newResetTxnForTest(t, first)
	proc := txn.proc
	proc.Ctx = defines.AttachAccountId(t.Context(), 7)
	proc.Base.TxnOperator = op
	first.GetLatestCatalogCache().UpdateDuration(types.TS{}, types.MaxTs())
	packer := types.NewPacker()
	defer packer.Close()
	bat, err := catalog.GenCreateDatabaseTuple("", 7, 1, 1, "app", 10, "", proc.Mp(), packer)
	require.NoError(t, err)
	defer bat.Clean(proc.Mp())
	_, err = fillRandomRowidAndZeroTs(bat, proc.Mp())
	require.NoError(t, err)
	first.GetLatestCatalogCache().InsertDatabase(bat)
	insertCatalogTableForResetTest(t, first, txn, 7, 10, 42, "app", "t")
	dbv, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("app"), 1, proc.Mp())
	require.NoError(t, err)
	defer dbv.Free(proc.Mp())
	tbv, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("t"), 1, proc.Mp())
	require.NoError(t, err)
	defer tbv.Free(proc.Mp())
	for _, wrapped := range []bool{false, true} {
		layout := "/direct"
		if wrapped {
			layout = "/wrapped"
		}
		var owner engine.Engine = first
		if wrapped {
			owner = &engine.EntireEngine{Engine: first}
		}
		proc.Ctx = context.WithValue(proc.Ctx, defines.EngineKey{}, owner)
		for _, name := range []string{"rows", "size"} {
			t.Run(name+layout, func(t *testing.T) {
				result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
				defer result.Free()
				require.NoError(t, result.PreExtendAndReset(1))
				call := function.MoTableRows
				if name == "size" {
					call = function.MoTableSize
				}
				require.ErrorIs(t, call([]*vector.Vector{dbv, tbv}, result, proc, 1, nil), want)
			})
		}
	}
}

func TestMoTableStatsOwnerModeSQLFallback(t *testing.T) {
	for _, name := range []string{"rows", "size"} {
		t.Run(name, func(t *testing.T) {
			oldRows := function.GetMoTableRowsFunc.Load()
			oldSize := function.GetMoTableSizeFunc.Load()
			t.Cleanup(func() {
				function.GetMoTableRowsFunc.Store(oldRows)
				function.GetMoTableSizeFunc.Store(oldSize)
			})
			first, second := &Engine{}, &Engine{}
			t.Cleanup(func() { require.NoError(t, first.Close()); require.NoError(t, second.Close()) })
			function.GetMoTableRowsFunc.Store(moTableRowsFunc())
			function.GetMoTableSizeFunc.Store(moTableSizeFunc())
			wantErr := errors.New("first owner's statistics executor")
			first.dynamicCtx.executorPool.New = func() any { return statsDispatchExecutor{err: wantErr} }
			proc := testutil.NewProcess(t, testutil.WithFileService(nil))
			ctrl := gomock.NewController(t)
			op := mock_frontend.NewMockTxnOperator(ctrl)
			tx := &Transaction{engine: first, proc: proc, tableCache: &sync.Map{}, tableOps: newTableOps()}
			tx.op = op
			op.EXPECT().GetWorkspace().Return(tx).AnyTimes()
			op.EXPECT().Status().Return(txnpb.TxnStatus_Active).AnyTimes()
			op.EXPECT().Txn().Return(txnpb.TxnMeta{}).AnyTimes()
			op.EXPECT().IsSnapOp().Return(false).AnyTimes()
			op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
			db := &txnDatabase{op: op, databaseId: catalog.MO_CATALOG_ID, databaseName: catalog.MO_CATALOG}
			tbl := &txnTable{eng: first, db: db, tableId: catalog.MO_TABLES_ID, relKind: "V", tableDef: &plan.TableDef{Cols: []*plan.ColDef{{Name: "id"}}}}
			first.partitions = map[[2]uint64]*logtailreplay.Partition{{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}: logtailreplay.NewPartition("", nil, 0, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID, nil)}
			tx.tableCache.Store(genTableKey(0, catalog.MO_TABLES, catalog.MO_CATALOG_ID, catalog.MO_CATALOG), &txnTableDelegate{origin: tbl, isLocal: func() (bool, error) { return true, nil }})
			proc.Ctx = defines.AttachAccountId(context.Background(), 0)
			proc.Ctx = context.WithValue(proc.Ctx, defines.EngineKey{}, &engine.EntireEngine{Engine: first})
			proc.Base.TxnOperator = op
			dbv, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(catalog.MO_CATALOG), 1, proc.Mp())
			require.NoError(t, err)
			defer dbv.Free(proc.Mp())
			tbv, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(catalog.MO_TABLES), 1, proc.Mp())
			require.NoError(t, err)
			defer tbv.Free(proc.Mp())
			result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
			defer result.Free()
			require.NoError(t, result.PreExtendAndReset(1))
			call := function.MoTableRows
			if name == "size" {
				call = function.MoTableSize
			}
			// Control: actual SQL function resolves one valid table and reaches
			// the supplied concrete owner through EntireEngine.
			first.dynamicCtx.setUseOldImpl(false)
			require.ErrorIs(t, call([]*vector.Vector{dbv, tbv}, result, proc, 1, nil), wantErr)
			first.dynamicCtx.setUseOldImpl(true)
			// A control operation on a different CN must not change this owner's
			// path. This is the actual per-owner control used by mo_ctl.
			second.dynamicCtx.setUseOldImpl(false)
			require.True(t, first.dynamicCtx.conf.StatsUsingOldImpl)
			values := make([]int64, 0, 2)
			for _, previous := range []int64{111, 222} {
				require.NoError(t, result.PreExtendAndReset(1))
				vector.MustFixedColWithTypeCheck[int64](result.GetResultVector())[0] = previous
				require.NoError(t, call([]*vector.Vector{dbv, tbv}, result, proc, 1, nil))
				values = append(values, vector.MustFixedColWithTypeCheck[int64](result.GetResultVector())[0])
			}
			require.Equal(t, []int64{0, 0}, values, "the real old SQL path must return the empty relation result, not retained vector data")

			for _, expired := range []bool{false, true} {
				mode := "canceled"
				if expired {
					mode = "expired"
				}
				t.Run(mode, func(t *testing.T) {
					original := proc.Ctx
					canceled, cancel := context.WithCancel(original)
					expected := context.Canceled
					if expired {
						cancel()
						canceled, cancel = context.WithDeadline(original, time.Now().Add(-time.Second))
						expected = context.DeadlineExceeded
					} else {
						cancel()
					}
					proc.Ctx = canceled
					require.NoError(t, result.PreExtendAndReset(1))
					vector.MustFixedColWithTypeCheck[int64](result.GetResultVector())[0] = 111
					err := call([]*vector.Vector{dbv, tbv}, result, proc, 1, nil)
					cancel()
					proc.Ctx = original
					require.ErrorIs(t, err, expected, "canceled old-mode admission must not fall back")
					require.Equal(t, int64(111), vector.MustFixedColWithTypeCheck[int64](result.GetResultVector())[0], "cancellation must leave the result untouched")
				})
			}
			require.NoError(t, second.Close())
			require.NoError(t, result.PreExtendAndReset(1))
			require.NoError(t, call([]*vector.Vector{dbv, tbv}, result, proc, 1, nil))
			require.Equal(t, int64(0), vector.MustFixedColWithTypeCheck[int64](result.GetResultVector())[0])
			first.dynamicCtx.setUseOldImpl(false)
			require.ErrorIs(t, call([]*vector.Vector{dbv, tbv}, result, proc, 1, nil), wantErr)
		})
	}
}

type policyStatsExecutor struct {
	statsDispatchExecutor
	observe func(string)
}

func (e policyStatsExecutor) Query(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
	e.observe(sql)
	return e.statsDispatchExecutor.Query(ctx, sql, opts)
}

func TestMoTableStatsPolicyAdmissionAndIsolation(t *testing.T) {
	first, second := &Engine{}, &Engine{}
	defer first.Close()
	defer second.Close()
	sentinel := errors.New("admitted statistics query")
	entered := make(chan string, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseQuery := func() { releaseOnce.Do(func() { close(release) }) }
	first.dynamicCtx.executorPool.New = func() any {
		return policyStatsExecutor{statsDispatchExecutor: statsDispatchExecutor{err: sentinel}, observe: func(sql string) { entered <- sql; <-release }}
	}
	callback := moTableRowsFunc()
	done := make(chan error, 1)
	go func() {
		defer close(done)
		_, err, _ := (*callback)(context.Background(), first, func() ([]uint64, []uint64, []uint64, error) { return []uint64{0}, []uint64{1}, []uint64{2}, nil }, false, false)
		done <- err
	}()
	defer func() { releaseQuery(); <-done }()
	sql := <-entered
	require.Contains(t, sql, "COALESCE")
	first.dynamicCtx.setUseOldImpl(true)
	second.dynamicCtx.setUseOldImpl(false)
	second.dynamicCtx.setForceUpdate(true)
	releaseQuery()
	require.ErrorIs(t, <-done, sentinel, "changing policy cannot reinterpret admitted new-path work")
	_, err, handled := (*callback)(context.Background(), first, func() ([]uint64, []uint64, []uint64, error) { return []uint64{0}, []uint64{1}, []uint64{2}, nil }, false, false)
	require.NoError(t, err)
	require.False(t, handled, "next admission uses the caller owner's updated mode")
	first.dynamicCtx.setUseOldImpl(false)
	first.dynamicCtx.executorPool = sync.Pool{New: func() any {
		return policyStatsExecutor{statsDispatchExecutor: statsDispatchExecutor{err: sentinel}, observe: func(sql string) { entered <- sql }}
	}}
	for _, force := range []bool{false, true} {
		first.dynamicCtx.setForceUpdate(force)
		_, err, handled = (*callback)(context.Background(), &engine.EntireEngine{Engine: first}, func() ([]uint64, []uint64, []uint64, error) { return []uint64{0}, []uint64{1}, []uint64{2}, nil }, false, false)
		require.ErrorIs(t, err, sentinel)
		require.True(t, handled)
		require.Equal(t, !force, strings.Contains(<-entered, "COALESCE"), "owner force-update must determine the query path independently")
	}
	first.dynamicCtx.defaultConf = MoTableStatsConfig{StatsUsingOldImpl: true}
	first.dynamicCtx.restoreDefaultSetting(true)
	_, err, handled = (*callback)(context.Background(), first, nil, false, false)
	require.NoError(t, err)
	require.False(t, handled)
	second.dynamicCtx.restoreDefaultSetting(true)
	_, err, handled = (*callback)(context.Background(), first, nil, false, false)
	require.NoError(t, err)
	require.False(t, handled, "restoring another owner's defaults must not affect this owner")
}

func TestMoTableStatsLazyResolutionUsesAdmissionPolicy(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(fmt.Sprint(force), func(t *testing.T) {
			owner := &Engine{}
			defer owner.Close()
			owner.dynamicCtx.setForceUpdate(force)
			sentinel := errors.New("selected query")
			var query string
			owner.dynamicCtx.executorPool.New = func() any {
				return policyStatsExecutor{statsDispatchExecutor: statsDispatchExecutor{err: sentinel}, observe: func(sql string) { query = sql }}
			}
			resolved := 0
			resolve := func() ([]uint64, []uint64, []uint64, error) {
				resolved++
				owner.dynamicCtx.setUseOldImpl(true)
				owner.dynamicCtx.setForceUpdate(!force)
				return []uint64{0}, []uint64{1}, []uint64{2}, nil
			}
			callback := moTableRowsFunc()
			_, err, handled := (*callback)(context.Background(), owner, resolve, false, false)
			require.ErrorIs(t, err, sentinel)
			require.True(t, handled)
			require.Equal(t, 1, resolved)
			require.Equal(t, !force, strings.Contains(query, "COALESCE"))
			_, err, handled = (*callback)(context.Background(), owner, resolve, false, false)
			require.NoError(t, err)
			require.False(t, handled)
			require.Equal(t, 1, resolved, "old mode must not invoke metadata resolution")
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err, _ = (*callback)(ctx, owner, resolve, false, false)
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, 1, resolved)
		})
	}
}
