//go:build gpu

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

// Cache-key and SqlProcess.SnapshotTS behaviour of the IVF-PQ and CAGRA search TVFs under a
// named snapshot (#27927).

import (
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Current txn read TS. A snapshot below it is historical; at or above it is not.
const gpuSnapshotCurrentPhysicalTS int64 = 1_000_000

func newCagraMockAlgoFn(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) veccache.VectorIndexSearchIf {
	return &MockSearch{Idxcfg: idxcfg, Tblcfg: tblcfg}
}

// gpuSnapshotSearchArgs builds the (IndexTableConfig const, vecf32(4) query) argument pair.
// parttype must name the index base column type or the TVF rejects the query.
func gpuSnapshotSearchArgs(indexTable string) []*plan.Expr {
	tblcfg := fmt.Sprintf(
		`{"db":"db","src":"src","metadata":"__meta","index":"%s","parttype":%d}`,
		indexTable, int32(types.T_array_float32))
	return []*plan.Expr{
		{
			Typ:  plan.Type{Id: int32(types.T_varchar), Width: 512},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: tblcfg}}},
		},
		plan2.MakePlan2Vecf32ConstExprWithType("[1,2,3,4]", 4),
	}
}

func gpuSnapshotSearchBatch(proc *process.Process, indexTable string) *batch.Batch {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))
	bat.Vecs[1] = vector.NewVec(types.New(types.T_array_float32, 4, 0))
	tblcfg := fmt.Sprintf(
		`{"db":"db","src":"src","metadata":"__meta","index":"%s","parttype":%d}`,
		indexTable, int32(types.T_array_float32))
	vector.AppendBytes(bat.Vecs[0], []byte(tblcfg), false, proc.Mp())
	vector.AppendArray(bat.Vecs[1], []float32{1, 2, 3, 4}, false, proc.Mp())
	bat.SetRowCount(1)
	return bat
}

// runGpuSnapshotSearch drives funcName's TVF through prepare and start with tf.ScanSnapshot
// set, against a mocked txn at gpuSnapshotCurrentPhysicalTS. Cache entries are dropped on
// cleanup.
func runGpuSnapshotSearch(
	t *testing.T, funcName, params, indexTable string, snapshot *plan.Snapshot,
) (*TableFunction, *process.Process, tvfState) {
	t.Helper()

	ctrl := gomock.NewController(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	// Only Txn() is reachable: MockSearch.Load runs no SQL, so CloneSnapshotOp is not called.
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{
		SnapshotTS: timestamp.Timestamp{PhysicalTime: gpuSnapshotCurrentPhysicalTS},
	}).AnyTimes()
	proc.Base.TxnOperator = txnOp

	tf := &TableFunction{
		Attrs: []string{"pkid", "score"},
		Rets: []*plan.ColDef{
			{Name: "pkid", Typ: plan.Type{Id: int32(types.T_int64), Width: 8}},
			{Name: "score", Typ: plan.Type{Id: int32(types.T_float64), Width: 8}},
		},
		FuncName:     funcName,
		Params:       []byte(params),
		Args:         gpuSnapshotSearchArgs(indexTable),
		ScanSnapshot: snapshot,
	}
	tf.OperatorBase = vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}}

	require.NoError(t, tf.Prepare(proc))

	inbat := gpuSnapshotSearchBatch(proc, indexTable)
	var err error
	for i := range tf.ctr.executorsForArgs {
		tf.ctr.argVecs[i], err = tf.ctr.executorsForArgs[i].Eval(proc, []*batch.Batch{inbat}, nil)
		require.NoError(t, err)
	}

	require.NoError(t, tf.ctr.state.start(tf, proc, 0, nil))

	state := tf.ctr.state
	t.Cleanup(func() {
		veccache.Cache.RemovePrefix(indexTable)
		state.free(tf, proc, false, nil)
	})
	return tf, proc, state
}

// gpuSnapshotCacheKeys reports which candidate keys exist in the cache.
func gpuSnapshotCacheKeys(indexTable string, ts *timestamp.Timestamp) (bare bool, suffixed bool, key string) {
	_, bare = veccache.Cache.IndexMap.Load(indexTable)
	if ts == nil {
		return bare, false, ""
	}
	key = fmt.Sprintf("%s@%d-%d", indexTable, ts.PhysicalTime, ts.LogicalTime)
	_, suffixed = veccache.Cache.IndexMap.Load(key)
	return bare, suffixed, key
}

func historicalSnapshot() *plan.Snapshot {
	return &plan.Snapshot{TS: &timestamp.Timestamp{
		PhysicalTime: gpuSnapshotCurrentPhysicalTS - 500,
		LogicalTime:  3,
	}}
}

// ---------------------------------------------------------------- IVF-PQ

const ivfpqSnapshotParams = `{"op_type":"vector_l2_ops","lists":"4","m":"2","bits_per_code":"8"}`

// A historical snapshot caches under the TS-suffixed key and not the bare key.
func TestIvfpqSearchSnapshotUsesTSSuffixedCacheKey(t *testing.T) {
	orig := newIvfpqAlgo
	newIvfpqAlgo = newIvfpqMockAlgoFn
	defer func() { newIvfpqAlgo = orig }()

	snapshot := historicalSnapshot()
	const idxTable = "__idx_ivfpq_snapshot_hist"
	tf, proc, st := runGpuSnapshotSearch(t, "ivfpq_search", ivfpqSnapshotParams, idxTable, snapshot)

	bare, suffixed, key := gpuSnapshotCacheKeys(idxTable, snapshot.TS)
	require.True(t, suffixed, "historical search must cache the index under %q", key)
	require.False(t, bare, "historical search must not touch the current-index cache entry %q", idxTable)

	u := st.(*ivfpqSearchState)
	require.NotNil(t, u.scanSnapshot)
	require.Equal(t, snapshot.TS.PhysicalTime, u.scanSnapshot.TS.PhysicalTime)
	u.reset(tf, proc)
	require.Nil(t, u.scanSnapshot, "reset must clear the per-query snapshot")
}

// Without a snapshot the bare key is used.
func TestIvfpqSearchNoSnapshotUsesBareCacheKey(t *testing.T) {
	orig := newIvfpqAlgo
	newIvfpqAlgo = newIvfpqMockAlgoFn
	defer func() { newIvfpqAlgo = orig }()

	const idxTable = "__idx_ivfpq_snapshot_none"
	_, _, st := runGpuSnapshotSearch(t, "ivfpq_search", ivfpqSnapshotParams, idxTable, nil)

	bare, _, _ := gpuSnapshotCacheKeys(idxTable, nil)
	require.True(t, bare, "an unsnapshotted search must cache under the bare index table name")
	require.Nil(t, st.(*ivfpqSearchState).scanSnapshot)
}

// A snapshot TS not earlier than the current txn's is not historical: the key stays bare.
func TestIvfpqSearchNonHistoricalSnapshotUsesBareCacheKey(t *testing.T) {
	orig := newIvfpqAlgo
	newIvfpqAlgo = newIvfpqMockAlgoFn
	defer func() { newIvfpqAlgo = orig }()

	future := &plan.Snapshot{TS: &timestamp.Timestamp{
		PhysicalTime: gpuSnapshotCurrentPhysicalTS + 500,
	}}
	const idxTable = "__idx_ivfpq_snapshot_future"
	runGpuSnapshotSearch(t, "ivfpq_search", ivfpqSnapshotParams, idxTable, future)

	bare, suffixed, key := gpuSnapshotCacheKeys(idxTable, future.TS)
	require.True(t, bare, "a non-historical snapshot must reuse the current-index cache entry")
	require.False(t, suffixed, "a non-historical snapshot must not create %q", key)
}

// Two distinct snapshots produce two entries; the same snapshot produces one.
func TestIvfpqSearchDistinctSnapshotsGetDistinctCacheEntries(t *testing.T) {
	orig := newIvfpqAlgo
	newIvfpqAlgo = newIvfpqMockAlgoFn
	defer func() { newIvfpqAlgo = orig }()

	const idxTable = "__idx_ivfpq_snapshot_multi"
	first := historicalSnapshot()
	second := &plan.Snapshot{TS: &timestamp.Timestamp{
		PhysicalTime: gpuSnapshotCurrentPhysicalTS - 100,
		LogicalTime:  1,
	}}

	runGpuSnapshotSearch(t, "ivfpq_search", ivfpqSnapshotParams, idxTable, first)
	runGpuSnapshotSearch(t, "ivfpq_search", ivfpqSnapshotParams, idxTable, first)
	runGpuSnapshotSearch(t, "ivfpq_search", ivfpqSnapshotParams, idxTable, second)

	_, firstCached, firstKey := gpuSnapshotCacheKeys(idxTable, first.TS)
	_, secondCached, secondKey := gpuSnapshotCacheKeys(idxTable, second.TS)
	require.True(t, firstCached, "missing entry for %q", firstKey)
	require.True(t, secondCached, "missing entry for %q", secondKey)
	require.NotEqual(t, firstKey, secondKey)

	n := 0
	veccache.Cache.IndexMap.Range(func(k, _ any) bool {
		if s, ok := k.(string); ok && strings.HasPrefix(s, idxTable) {
			n++
		}
		return true
	})
	require.Equal(t, 2, n, "two distinct snapshots must produce exactly two entries, got %d", n)
}

// ---------------------------------------------------------------- CAGRA

const cagraSnapshotParams = `{"op_type":"vector_l2_ops"}`

// CAGRA twin of TestIvfpqSearchSnapshotUsesTSSuffixedCacheKey.
func TestCagraSearchSnapshotUsesTSSuffixedCacheKey(t *testing.T) {
	orig := newCagraAlgo
	newCagraAlgo = newCagraMockAlgoFn
	defer func() { newCagraAlgo = orig }()

	snapshot := historicalSnapshot()
	const idxTable = "__idx_cagra_snapshot_hist"
	tf, proc, st := runGpuSnapshotSearch(t, "cagra_search", cagraSnapshotParams, idxTable, snapshot)

	bare, suffixed, key := gpuSnapshotCacheKeys(idxTable, snapshot.TS)
	require.True(t, suffixed, "historical search must cache the index under %q", key)
	require.False(t, bare, "historical search must not touch the current-index cache entry %q", idxTable)

	u := st.(*cagraSearchState)
	require.NotNil(t, u.scanSnapshot)
	require.Equal(t, snapshot.TS.PhysicalTime, u.scanSnapshot.TS.PhysicalTime)
	u.reset(tf, proc)
	require.Nil(t, u.scanSnapshot, "reset must clear the per-query snapshot")
}

// CAGRA twin of TestIvfpqSearchNoSnapshotUsesBareCacheKey.
func TestCagraSearchNoSnapshotUsesBareCacheKey(t *testing.T) {
	orig := newCagraAlgo
	newCagraAlgo = newCagraMockAlgoFn
	defer func() { newCagraAlgo = orig }()

	const idxTable = "__idx_cagra_snapshot_none"
	_, _, st := runGpuSnapshotSearch(t, "cagra_search", cagraSnapshotParams, idxTable, nil)

	bare, _, _ := gpuSnapshotCacheKeys(idxTable, nil)
	require.True(t, bare, "an unsnapshotted search must cache under the bare index table name")
	require.Nil(t, st.(*cagraSearchState).scanSnapshot)
}

// CAGRA twin of TestIvfpqSearchNonHistoricalSnapshotUsesBareCacheKey.
func TestCagraSearchNonHistoricalSnapshotUsesBareCacheKey(t *testing.T) {
	orig := newCagraAlgo
	newCagraAlgo = newCagraMockAlgoFn
	defer func() { newCagraAlgo = orig }()

	future := &plan.Snapshot{TS: &timestamp.Timestamp{
		PhysicalTime: gpuSnapshotCurrentPhysicalTS + 500,
	}}
	const idxTable = "__idx_cagra_snapshot_future"
	runGpuSnapshotSearch(t, "cagra_search", cagraSnapshotParams, idxTable, future)

	bare, suffixed, key := gpuSnapshotCacheKeys(idxTable, future.TS)
	require.True(t, bare, "a non-historical snapshot must reuse the current-index cache entry")
	require.False(t, suffixed, "a non-historical snapshot must not create %q", key)
}

// CAGRA twin of TestIvfpqSearchDistinctSnapshotsGetDistinctCacheEntries.
func TestCagraSearchDistinctSnapshotsGetDistinctCacheEntries(t *testing.T) {
	orig := newCagraAlgo
	newCagraAlgo = newCagraMockAlgoFn
	defer func() { newCagraAlgo = orig }()

	const idxTable = "__idx_cagra_snapshot_multi"
	first := historicalSnapshot()
	second := &plan.Snapshot{TS: &timestamp.Timestamp{
		PhysicalTime: gpuSnapshotCurrentPhysicalTS - 100,
		LogicalTime:  1,
	}}

	runGpuSnapshotSearch(t, "cagra_search", cagraSnapshotParams, idxTable, first)
	runGpuSnapshotSearch(t, "cagra_search", cagraSnapshotParams, idxTable, first)
	runGpuSnapshotSearch(t, "cagra_search", cagraSnapshotParams, idxTable, second)

	_, firstCached, firstKey := gpuSnapshotCacheKeys(idxTable, first.TS)
	_, secondCached, secondKey := gpuSnapshotCacheKeys(idxTable, second.TS)
	require.True(t, firstCached, "missing entry for %q", firstKey)
	require.True(t, secondCached, "missing entry for %q", secondKey)
	require.NotEqual(t, firstKey, secondKey)
}
