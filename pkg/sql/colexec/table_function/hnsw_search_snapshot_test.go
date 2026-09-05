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

// Cache-key and SqlProcess.SnapshotTS behaviour of the HNSW search TVF under a named
// snapshot (#27927).

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

// Current txn read TS. A snapshot below it is historical; at or above it is not.
const hnswSnapshotCurrentPhysicalTS int64 = 2_000_000

func hnswSnapshotTblCfg(indexTable string) string {
	return fmt.Sprintf(`{"db":"db","src":"src","metadata":"__metadata","index":"%s"}`, indexTable)
}

// runHnswSnapshotSearch drives hnsw_search through prepare and start with tf.ScanSnapshot
// set, against a mocked txn at hnswSnapshotCurrentPhysicalTS. Cache entries are dropped on
// cleanup.
func runHnswSnapshotSearch(
	t *testing.T, indexTable string, snapshot *plan.Snapshot,
) (*TableFunction, *process.Process, tvfState) {
	t.Helper()

	ctrl := gomock.NewController(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	// Only Txn() is reachable: MockSearch.Load runs no SQL, so CloneSnapshotOp is not called.
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
		veccache.Cache.RemovePrefix(indexTable)
		state.free(tf, proc, false, nil)
	})
	return tf, proc, state
}

// hnswSnapshotCacheKeys reports which candidate keys exist in the cache.
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

// A historical snapshot caches under the TS-suffixed key and not the bare key.
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

	u := st.(*hnswSearchState)
	require.NotNil(t, u.scanSnapshot)
	require.Equal(t, snapshot.TS.PhysicalTime, u.scanSnapshot.TS.PhysicalTime)
	u.reset(tf, proc)
	require.Nil(t, u.scanSnapshot, "reset must clear the per-query snapshot")
}

// Without a snapshot the bare key is used.
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

// A snapshot TS not earlier than the current txn's is not historical: the key stays bare.
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

// Two distinct snapshots produce two entries; the same snapshot produces one.
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
