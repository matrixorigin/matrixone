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

// Cache-key and SqlProcess.SnapshotTS behaviour of the fulltext2 search TVF under a named
// snapshot, on both the materialized and streaming paths (#27941).

import (
	"fmt"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

const ft2SnapshotCurrentPhysicalTS int64 = 3_000_000

// ft2StubIndex records the SqlProcess of each search that reaches it, and whether Load fired.
type ft2StubIndex struct {
	mu      sync.Mutex
	hits    int
	lastTS  *timestamp.Timestamp
	loadHit bool
}

func (s *ft2StubIndex) record(sqlproc *sqlexec.SqlProcess) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hits++
	if sqlproc != nil {
		s.lastTS = sqlproc.SnapshotTS
	}
}

func (s *ft2StubIndex) SearchInto(sqlproc *sqlexec.SqlProcess, _ any, _ vectorindex.RuntimeConfig, _ *vectorindex.SearchOutput) error {
	s.record(sqlproc)
	return nil
}

func (s *ft2StubIndex) Search(sqlproc *sqlexec.SqlProcess, _ any, _ vectorindex.RuntimeConfig) (any, []float64, error) {
	s.record(sqlproc)
	return []int64{}, []float64{}, nil
}

func (s *ft2StubIndex) SearchFloat32(_ *sqlexec.SqlProcess, _ any, _ vectorindex.RuntimeConfig, _ []int64, _ []float32) error {
	return nil
}

func (s *ft2StubIndex) Load(*sqlexec.SqlProcess) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadHit = true
	return nil
}

func (s *ft2StubIndex) Destroy() {}

func (s *ft2StubIndex) state() (hits int, ts *timestamp.Timestamp, loadHit bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits, s.lastTS, s.loadHit
}

// plantFT2Index stores a STATUS_LOADED cache entry under key. Removed on cleanup.
func plantFT2Index(t *testing.T, key string) *ft2StubIndex {
	t.Helper()
	stub := &ft2StubIndex{}
	entry := &veccache.VectorIndexSearch{Algo: stub}
	entry.Cond = sync.NewCond(entry.Mutex.RLocker())
	entry.Status.Store(veccache.STATUS_LOADED)
	veccache.Cache.IndexMap.Store(key, entry)
	t.Cleanup(func() { veccache.Cache.IndexMap.Delete(key) })
	return stub
}

// startFT2SnapshotSearch drives fulltext2SearchState.start with a pushed LIMIT (the
// materialized SearchInto path) against a mocked txn at ft2SnapshotCurrentPhysicalTS.
func startFT2SnapshotSearch(t *testing.T, indexTable string, snapshot *plan.Snapshot) error {
	t.Helper()
	_, err := startFT2SnapshotSearchWithLimit(t, indexTable, snapshot, 1)
	return err
}

// startFT2SnapshotSearchWithLimit takes an explicit pushed LIMIT; 0 selects the streaming
// path. Returns the state so a streaming caller can await the producer goroutine.
func startFT2SnapshotSearchWithLimit(
	t *testing.T, indexTable string, snapshot *plan.Snapshot, limit uint64,
) (*fulltext2SearchState, error) {
	t.Helper()

	ctrl := gomock.NewController(t)
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{
		SnapshotTS: timestamp.Timestamp{PhysicalTime: ft2SnapshotCurrentPhysicalTS},
	}).AnyTimes()
	proc.Base.TxnOperator = txnOp
	// fulltext2ScoreAlgo reads a session variable.
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return nil, nil })

	tf := newFT2TF([]string{"doc_id", "score"}, ft2SearchRets())
	tf.ScanSnapshot = snapshot

	patVec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(patVec, []byte("hello"), false, mp))
	modeVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](modeVec, int64(0), false, mp))
	tf.ctr.argVecs = []*vector.Vector{
		ft2ConstStr(t, mp, fmt.Sprintf(`{"index":"%s"}`, indexTable)), patVec, modeVec,
	}

	// start() resets limit from plannedLimit on every row.
	st := &fulltext2SearchState{plannedLimit: limit}
	err := st.start(tf, proc, 0, nil)
	t.Cleanup(func() { st.free(tf, proc, false, nil) })
	return st, err
}

func ft2HistoricalSnapshot() *plan.Snapshot {
	return &plan.Snapshot{TS: &timestamp.Timestamp{
		PhysicalTime: ft2SnapshotCurrentPhysicalTS - 500,
		LogicalTime:  3,
	}}
}

// A snapshotted MATCH resolves the TS-suffixed key and carries the read TS on the SqlProcess.
// The stub is planted only under the suffixed key.
func TestFulltext2SearchSnapshotUsesTSSuffixedCacheKey(t *testing.T) {
	const idxTable = "__idx_ft2_snapshot_hist"
	snapshot := ft2HistoricalSnapshot()
	key := fmt.Sprintf("%s@%d-%d", idxTable, snapshot.TS.PhysicalTime, snapshot.TS.LogicalTime)
	stub := plantFT2Index(t, key)

	require.NoError(t, startFT2SnapshotSearch(t, idxTable, snapshot))

	hits, ts, loadHit := stub.state()
	require.Equal(t, 1, hits, "the search must resolve to the index planted under %q", key)
	require.False(t, loadHit, "an already-loaded cache entry must not be reloaded")
	require.NotNil(t, ts, "the search must carry the snapshot read TS onto the SqlProcess")
	require.Equal(t, snapshot.TS.PhysicalTime, ts.PhysicalTime)
	require.Equal(t, snapshot.TS.LogicalTime, ts.LogicalTime)

	_, bare := veccache.Cache.IndexMap.Load(idxTable)
	require.False(t, bare, "a historical read must not create or warm the current-index entry")
}

// Without a snapshot the bare key is used and no read TS is set.
func TestFulltext2SearchNoSnapshotUsesBareCacheKey(t *testing.T) {
	const idxTable = "__idx_ft2_snapshot_none"
	stub := plantFT2Index(t, idxTable)

	require.NoError(t, startFT2SnapshotSearch(t, idxTable, nil))

	hits, ts, _ := stub.state()
	require.Equal(t, 1, hits, "an unsnapshotted MATCH must resolve under the bare index table name")
	require.Nil(t, ts, "and must leave the read at the current txn")
}

// A snapshot TS not earlier than the current txn's is not historical: the key stays bare.
func TestFulltext2SearchNonHistoricalSnapshotUsesBareCacheKey(t *testing.T) {
	const idxTable = "__idx_ft2_snapshot_future"
	future := &plan.Snapshot{TS: &timestamp.Timestamp{
		PhysicalTime: ft2SnapshotCurrentPhysicalTS + 500,
	}}
	bareStub := plantFT2Index(t, idxTable)

	require.NoError(t, startFT2SnapshotSearch(t, idxTable, future))

	hits, _, _ := bareStub.state()
	require.Equal(t, 1, hits, "a non-historical snapshot must reuse the current-index entry")

	suffixed := fmt.Sprintf("%s@%d-%d", idxTable, future.TS.PhysicalTime, future.TS.LogicalTime)
	_, found := veccache.Cache.IndexMap.Load(suffixed)
	require.False(t, found, "a non-historical snapshot must not create %q", suffixed)
}

// The streaming path (no pushed LIMIT) runs the search through Cache.Search on a producer
// goroutine. Awaiting errCh joins it before asserting.
func TestFulltext2SearchStreamingSnapshotUsesTSSuffixedCacheKey(t *testing.T) {
	const idxTable = "__idx_ft2_snapshot_stream"
	snapshot := ft2HistoricalSnapshot()
	key := fmt.Sprintf("%s@%d-%d", idxTable, snapshot.TS.PhysicalTime, snapshot.TS.LogicalTime)
	stub := plantFT2Index(t, key)

	st, err := startFT2SnapshotSearchWithLimit(t, idxTable, snapshot, 0)
	require.NoError(t, err)
	require.True(t, st.streaming, "no pushed LIMIT must select the streaming path")
	require.NoError(t, <-st.errCh, "the producer goroutine's search must succeed")

	hits, ts, _ := stub.state()
	require.Equal(t, 1, hits, "the streaming search must resolve to the index planted under %q", key)
	require.NotNil(t, ts, "the streaming search must carry the snapshot read TS too")
	require.Equal(t, snapshot.TS.PhysicalTime, ts.PhysicalTime)

	_, bare := veccache.Cache.IndexMap.Load(idxTable)
	require.False(t, bare, "a historical read must not create or warm the current-index entry")
}
