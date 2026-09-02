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

// TVF-side half of the named-snapshot vector search fix (#27927) for the two GPU
// algorithms, IVF-PQ and CAGRA.
//
// ivfpqRunSearchQuery / cagraRunSearchQuery must, when the FUNCTION_SCAN node carries a
// snapshot (plan side: apply_indices_gpu_vector_snapshot_test.go):
//
//  1. set SqlProcess.SnapshotTS, so the nested index-load SQL time-travels via a cloned
//     read txn (sqlexec.txnForRun; covered in pkg/vectorindex/sqlexec/snapshot_test.go), and
//  2. suffix the veccache key with the EFFECTIVE snapshot TS, so the historical index gets
//     its own cache entry -- never served from, and never polluting, the current-index
//     entry keyed by the bare index table name.
//
// Point 2 is the load-bearing one and is what these tests pin: the key is asserted on the
// live cache map, and the un-suffixed key is asserted ABSENT so a regression that caches
// historical data under the current name fails here rather than at some later query. The
// suffix must also appear only for a genuinely historical TS -- the same condition
// txnForRun clones on -- so an ordinary query keeps hitting the shared warm entry.

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

// The current txn reads at this TS; a snapshot BELOW it is historical (clone + suffixed
// cache key), a snapshot at or above it is not (current txn, bare key).
const gpuSnapshotCurrentPhysicalTS int64 = 1_000_000

func newCagraMockAlgoFn(idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) veccache.VectorIndexSearchIf {
	return &MockSearch{Idxcfg: idxcfg, Tblcfg: tblcfg}
}

// gpuSnapshotSearchArgs builds the (IndexTableConfig const, vecf32(4) query) argument pair
// both GPU search TVFs take. parttype must name the index base column type or the TVF
// rejects the query outright (see makeConstInputExprsIvfpqSearch).
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

// runGpuSnapshotSearch drives funcName's TVF through prepare → start with tf.ScanSnapshot
// set to snapshot, against a mocked txn reading at gpuSnapshotCurrentPhysicalTS. It returns
// the state so callers can inspect/reset it. Every cache entry it created is dropped on
// cleanup so the process-wide veccache stays clean for the rest of the package.
func runGpuSnapshotSearch(
	t *testing.T, funcName, params, indexTable string, snapshot *plan.Snapshot,
) (*TableFunction, *process.Process, tvfState) {
	t.Helper()

	ctrl := gomock.NewController(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	// Only Txn() is reachable here: the historical decision reads the current txn's TS, and
	// MockSearch.Load runs no SQL, so CloneSnapshotOp is never called. gomock fails the test
	// if the production code reaches for anything else.
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
		// RemovePrefix drops both the bare and every TS-suffixed generation of this key,
		// so these fixtures cannot leak into the rest of the package's cache assertions.
		veccache.Cache.RemovePrefix(indexTable)
		state.free(tf, proc, false, nil)
	})
	return tf, proc, state
}

// gpuSnapshotCacheKeys reports which of the two candidate keys the search actually created.
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

// A historical snapshot must land the index under its OWN, TS-suffixed cache entry, leaving
// the current-index entry untouched. Before the fix the search used the bare key and the
// current txn, so a `{snapshot=...}` query read (and warmed) the CURRENT index.
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

	// The TS also has to reach the state, and reset() must clear it so a reused operator
	// does not carry a stale snapshot into the next query.
	u := st.(*ivfpqSearchState)
	require.NotNil(t, u.scanSnapshot)
	require.Equal(t, snapshot.TS.PhysicalTime, u.scanSnapshot.TS.PhysicalTime)
	u.reset(tf, proc)
	require.Nil(t, u.scanSnapshot, "reset must clear the per-query snapshot")
}

// No snapshot: the ordinary query keeps the bare key, so every current query shares one
// warm entry (the fix must not fragment the cache).
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

// A snapshot TS that is NOT earlier than the current txn's is not a historical read --
// txnForRun would not clone -- so the key must stay bare. Deriving the key from
// EffectiveSnapshotTS (not from "snapshot != nil") is what keeps the two in agreement.
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

// Two queries on the SAME snapshot must share one cache entry (one load), and a second,
// different snapshot must get its own -- neither may collapse onto the other.
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
