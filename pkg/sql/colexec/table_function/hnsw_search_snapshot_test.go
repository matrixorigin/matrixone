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

// TVF-side half of the named-snapshot vector search fix (#27927) for HNSW.
//
// runHnswSearch must, when the FUNCTION_SCAN node carries a snapshot (plan side:
// apply_indices_hnsw.go), set SqlProcess.SnapshotTS so the nested index-load SQL
// time-travels via a cloned read txn, AND suffix the veccache key with the EFFECTIVE
// snapshot TS so the historical index gets its own cache entry -- never served from, and
// never polluting, the current-index entry keyed by the bare index table name.
//
// The key is asserted on the live cache map and the un-suffixed key is asserted ABSENT, so
// a regression that caches historical data under the current name fails here rather than at
// some later query. The suffix must appear only for a genuinely historical TS -- the same
// condition sqlexec.txnForRun clones on -- so an ordinary query keeps hitting the shared
// warm entry instead of fragmenting the cache.
//
// The GPU algorithms' twin of this file is vector_search_snapshot_gpu_test.go.

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
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// The current txn reads at this TS; a snapshot BELOW it is historical (clone + suffixed
// cache key), a snapshot at or above it is not (current txn, bare key).
const hnswSnapshotCurrentPhysicalTS int64 = 2_000_000

func hnswSnapshotTblCfg(indexTable string) string {
	return fmt.Sprintf(`{"db":"db","src":"src","metadata":"__metadata","index":"%s"}`, indexTable)
}

// runHnswSnapshotSearch drives hnsw_search through prepare → start with tf.ScanSnapshot set
// to snapshot, against a mocked txn reading at hnswSnapshotCurrentPhysicalTS. Every cache
// entry it creates is dropped on cleanup so the process-wide veccache stays clean for the
// rest of the package.
func runHnswSnapshotSearch(
	t *testing.T, indexTable string, snapshot *plan.Snapshot,
) (*TableFunction, *process.Process, tvfState) {
	t.Helper()

	ctrl := gomock.NewController(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	// Only Txn() is reachable here: the historical decision reads the current txn's TS, and
	// MockSearch.Load runs no SQL, so CloneSnapshotOp is never called. gomock fails the test
	// if the production code reaches for anything else.
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{
		SnapshotTS: timestamp.Timestamp{PhysicalTime: hnswSnapshotCurrentPhysicalTS},
	}).AnyTimes()
	proc.Base.TxnOperator = txnOp

	tf := &TableFunction{
		Attrs:    hnswsearchdefaultAttrs,
		Rets:     hnswsearchdefaultColdefs,
		FuncName: "hnsw_search",
		Params:   []byte(`{"op_type": "vector_l2_ops"}`),
		Args: []*plan.Expr{
			{
				Typ:  plan.Type{Id: int32(types.T_varchar), Width: 512},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: hnswSnapshotTblCfg(indexTable)}}},
			},
			plan2.MakePlan2Vecf32ConstExprWithType("[0,1,2]", 3),
		},
		ScanSnapshot: snapshot,
	}
	tf.OperatorBase = vm.OperatorBase{OperatorInfo: vm.OperatorInfo{Idx: 0}}

	require.NoError(t, tf.Prepare(proc))

	inbat := batch.NewWithSize(2)
	inbat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))
	inbat.Vecs[1] = vector.NewVec(types.New(types.T_array_float32, 3, 0))
	require.NoError(t, vector.AppendBytes(inbat.Vecs[0], []byte(hnswSnapshotTblCfg(indexTable)), false, proc.Mp()))
	require.NoError(t, vector.AppendArray[float32](inbat.Vecs[1], []float32{0, 1, 2}, false, proc.Mp()))
	inbat.SetRowCount(1)

	var err error
	for i := range tf.ctr.executorsForArgs {
		tf.ctr.argVecs[i], err = tf.ctr.executorsForArgs[i].Eval(proc, []*batch.Batch{inbat}, nil)
		require.NoError(t, err)
	}

	require.NoError(t, tf.ctr.state.start(tf, proc, 0, nil))

	state := tf.ctr.state
	t.Cleanup(func() {
		// RemovePrefix drops both the bare and every TS-suffixed generation of this key.
		veccache.Cache.RemovePrefix(indexTable)
		state.free(tf, proc, false, nil)
	})
	return tf, proc, state
}

// hnswSnapshotCacheKeys reports which of the two candidate keys the search actually created.
func hnswSnapshotCacheKeys(indexTable string, ts *timestamp.Timestamp) (bare bool, suffixed bool, key string) {
	_, bare = veccache.Cache.IndexMap.Load(indexTable)
	if ts == nil {
		return bare, false, ""
	}
	key = fmt.Sprintf("%s@%d-%d", indexTable, ts.PhysicalTime, ts.LogicalTime)
	_, suffixed = veccache.Cache.IndexMap.Load(key)
	return bare, suffixed, key
}

func hnswHistoricalSnapshot() *plan.Snapshot {
	return &plan.Snapshot{TS: &timestamp.Timestamp{
		PhysicalTime: hnswSnapshotCurrentPhysicalTS - 500,
		LogicalTime:  3,
	}}
}

// A historical snapshot must land the index under its OWN, TS-suffixed cache entry, leaving
// the current-index entry untouched. Before the fix the search used the bare key and the
// current txn, so a `{snapshot=...}` query read (and warmed) the CURRENT index.
func TestHnswSearchSnapshotUsesTSSuffixedCacheKey(t *testing.T) {
	orig := newHnswAlgo
	newHnswAlgo = newMockAlgoFn
	defer func() { newHnswAlgo = orig }()

	snapshot := hnswHistoricalSnapshot()
	const idxTable = "__idx_hnsw_snapshot_hist"
	tf, proc, st := runHnswSnapshotSearch(t, idxTable, snapshot)

	bare, suffixed, key := hnswSnapshotCacheKeys(idxTable, snapshot.TS)
	require.True(t, suffixed, "historical search must cache the index under %q", key)
	require.False(t, bare, "historical search must not touch the current-index cache entry %q", idxTable)

	// The TS also has to reach the state, and reset() must clear it so a reused operator
	// does not carry a stale snapshot into the next query.
	u := st.(*hnswSearchState)
	require.NotNil(t, u.scanSnapshot)
	require.Equal(t, snapshot.TS.PhysicalTime, u.scanSnapshot.TS.PhysicalTime)
	u.reset(tf, proc)
	require.Nil(t, u.scanSnapshot, "reset must clear the per-query snapshot")
}

// No snapshot: the ordinary query keeps the bare key, so every current query shares one
// warm entry (the fix must not fragment the cache).
func TestHnswSearchNoSnapshotUsesBareCacheKey(t *testing.T) {
	orig := newHnswAlgo
	newHnswAlgo = newMockAlgoFn
	defer func() { newHnswAlgo = orig }()

	const idxTable = "__idx_hnsw_snapshot_none"
	_, _, st := runHnswSnapshotSearch(t, idxTable, nil)

	bare, _, _ := hnswSnapshotCacheKeys(idxTable, nil)
	require.True(t, bare, "an unsnapshotted search must cache under the bare index table name")
	require.Nil(t, st.(*hnswSearchState).scanSnapshot)
}

// A snapshot TS that is NOT earlier than the current txn's is not a historical read --
// txnForRun would not clone -- so the key must stay bare. Deriving the key from
// EffectiveSnapshotTS (not from "snapshot != nil") is what keeps the two in agreement.
func TestHnswSearchNonHistoricalSnapshotUsesBareCacheKey(t *testing.T) {
	orig := newHnswAlgo
	newHnswAlgo = newMockAlgoFn
	defer func() { newHnswAlgo = orig }()

	future := &plan.Snapshot{TS: &timestamp.Timestamp{
		PhysicalTime: hnswSnapshotCurrentPhysicalTS + 500,
	}}
	const idxTable = "__idx_hnsw_snapshot_future"
	runHnswSnapshotSearch(t, idxTable, future)

	bare, suffixed, key := hnswSnapshotCacheKeys(idxTable, future.TS)
	require.True(t, bare, "a non-historical snapshot must reuse the current-index cache entry")
	require.False(t, suffixed, "a non-historical snapshot must not create %q", key)
}

// Two queries on the SAME snapshot must share one cache entry (one load), and a second,
// different snapshot must get its own -- neither may collapse onto the other.
func TestHnswSearchDistinctSnapshotsGetDistinctCacheEntries(t *testing.T) {
	orig := newHnswAlgo
	newHnswAlgo = newMockAlgoFn
	defer func() { newHnswAlgo = orig }()

	const idxTable = "__idx_hnsw_snapshot_multi"
	first := hnswHistoricalSnapshot()
	second := &plan.Snapshot{TS: &timestamp.Timestamp{
		PhysicalTime: hnswSnapshotCurrentPhysicalTS - 100,
		LogicalTime:  1,
	}}

	runHnswSnapshotSearch(t, idxTable, first)
	runHnswSnapshotSearch(t, idxTable, first)
	runHnswSnapshotSearch(t, idxTable, second)

	_, firstCached, firstKey := hnswSnapshotCacheKeys(idxTable, first.TS)
	_, secondCached, secondKey := hnswSnapshotCacheKeys(idxTable, second.TS)
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
