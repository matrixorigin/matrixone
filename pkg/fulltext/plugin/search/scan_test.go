// Copyright 2022 Matrix Origin
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

package search

import (
	"context"
	"math/rand"
	goruntime "runtime"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestFulltextTopKLimitBounds(t *testing.T) {
	maxLimit := ^uint64(0)
	require.Equal(t, maxLimit, fulltextTopKLimit(maxLimit, true))
	require.Equal(t, uint64(30), fulltextTopKLimit(10, true))
	require.Equal(t, 1<<20, vectorindex.SearchResultPreallocate(maxLimit))

	proc := testutil.NewProc(t)
	state := &Scan{
		batch:  batch.NewWithSize(1),
		resbuf: []*vectorindex.SearchResultAnyKey{{Id: int64(7)}},
	}
	state.batch.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	result, err := state.returnResultFromBuffer(proc, maxLimit)
	require.NoError(t, err)
	require.Equal(t, 1, result.Batch.RowCount())
	require.Empty(t, state.resbuf)
	result.Batch.Clean(proc.Mp())
}

func TestFulltextResolveExecutionTarget(t *testing.T) {
	proc := testutil.NewProc(t)

	t.Run("direct call stays in current tenant", func(t *testing.T) {
		state := &Scan{}
		source, index, err := state.ResolveExecutionTarget(proc.Ctx, nil, nil, "user_source", "user_index")
		require.NoError(t, err)
		require.Equal(t, "user_source", source)
		require.Equal(t, "user_index", index)
		require.Nil(t, state.publisherAccount)
	})

	t.Run("trusted refs select publisher and quote identifiers", func(t *testing.T) {
		state := &Scan{}
		sourceRef := &plan.ObjectRef{
			SchemaName: "pub`db", ObjName: "source`table", SubscriptionName: "sub_alias",
			PubInfo: &plan.PubInfo{TenantId: 42},
		}
		indexRef := &plan.ObjectRef{
			SchemaName: "pub`db", ObjName: "index`table", SubscriptionName: "sub_alias",
			PubInfo: &plan.PubInfo{TenantId: 42},
		}
		source, index, err := state.ResolveExecutionTarget(proc.Ctx, sourceRef, indexRef, "ignored", "ignored")
		require.NoError(t, err)
		require.Equal(t, "`pub``db`.`source``table`", source)
		require.Equal(t, "`pub``db`.`index``table`", index)
		require.Equal(t, uint32(42), *state.publisherAccount)
		require.Equal(t, "pub`db", state.publisherDB)
		require.Equal(t, uint32(42), *state.sqlProcess(proc).AccountIDOverride)
	})

	t.Run("incomplete or inconsistent refs are rejected", func(t *testing.T) {
		valid := &plan.ObjectRef{
			SchemaName: "publisher", ObjName: "source", SubscriptionName: "sub_alias",
			PubInfo: &plan.PubInfo{TenantId: 42},
		}
		cases := []struct {
			name  string
			src   *plan.ObjectRef
			index *plan.ObjectRef
		}{
			{name: "missing index", src: valid},
			{name: "missing publisher", src: valid, index: &plan.ObjectRef{SchemaName: "publisher", ObjName: "index", SubscriptionName: "sub_alias"}},
			{name: "different tenant", src: valid, index: &plan.ObjectRef{SchemaName: "publisher", ObjName: "index", SubscriptionName: "sub_alias", PubInfo: &plan.PubInfo{TenantId: 43}}},
			{name: "different database", src: valid, index: &plan.ObjectRef{SchemaName: "other", ObjName: "index", SubscriptionName: "sub_alias", PubInfo: &plan.PubInfo{TenantId: 42}}},
			{name: "different subscription", src: valid, index: &plan.ObjectRef{SchemaName: "publisher", ObjName: "index", SubscriptionName: "other_alias", PubInfo: &plan.PubInfo{TenantId: 42}}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, _, err := (&Scan{}).ResolveExecutionTarget(proc.Ctx, tc.src, tc.index, "source", "index")
				require.Error(t, err)
			})
		}
	})
}

func TestRunCountStarUsesCountOnlyForTFIDF(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	s := &fulltext.SearchAccum{TblName: "idx_table", ScoreAlgo: fulltext.ALGO_TFIDF}

	prev := RunSql
	defer func() { RunSql = prev }()

	var gotSQL string
	RunSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		gotSQL = sql
		return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeCountOnlyBatchFT(proc)}}, nil
	}

	_, err := runCountStar(&Scan{}, proc, s)
	require.NoError(t, err)
	require.Equal(t, "SELECT COUNT(*) from idx_table where word = '__DocLen'", gotSQL)
	require.Equal(t, int64(100), s.Nrow)
	require.Zero(t, s.AvgDocLen)
}

func TestRunCountStarUsesDedupedDocLenForBM25(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	s := &fulltext.SearchAccum{TblName: "idx_table", ScoreAlgo: fulltext.ALGO_BM25}

	prev := RunSql
	defer func() { RunSql = prev }()

	var gotSQL string
	RunSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		gotSQL = sql
		return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeCountBatchFT(proc)}}, nil
	}

	_, err := runCountStar(&Scan{}, proc, s)
	require.NoError(t, err)
	require.Equal(t, "SELECT COUNT(*), AVG(CAST(pos AS DOUBLE)) from (SELECT doc_id, MAX(pos) AS pos from idx_table where word = '__DocLen' GROUP BY doc_id) doc_len", gotSQL)
	require.Equal(t, int64(100), s.Nrow)
	require.InDelta(t, 10.6666, s.AvgDocLen, 1e-9)
}

func TestSortTopKReleasesAggregates(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	s, err := fulltext.NewSearchAccum("src", "index", "pattern", 0, "", fulltext.ALGO_TFIDF)
	require.NoError(t, err)
	s.Nrow = 100

	st := &Scan{
		agghtab:   make(map[any]uint64, 3),
		aggcnt:    make([]int64, s.Nkeywords),
		docLenMap: make(map[any]int32, 3),
		mpool:     fulltext.NewFixedBytePool(proc, uint64(s.Nkeywords), 0, 0),
	}
	st.aggcnt[0] = 3

	for i := 0; i < 3; i++ {
		addr, docvec, allocErr := st.mpool.NewItem()
		require.NoError(t, allocErr)
		docvec[0] = uint8(i + 1)
		st.agghtab[i] = addr
		st.docLenMap[i] = int32(i + 1)
	}

	err = sort_topk(st, proc, s, 1)
	require.NoError(t, err)
	require.Len(t, st.minheap, 1)
	require.Empty(t, st.agghtab)
	require.Empty(t, st.docLenMap)
}

func TestReturnResultUsesCachedBinaryDocID(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	st := &Scan{
		docIDMap: map[any]any{
			"doc-key": []byte{0x01, 0x02, 0x03},
		},
		batch: batch.NewWithSize(2),
	}
	st.batch.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 256, 0))
	st.batch.Vecs[1] = vector.NewVec(types.New(types.T_float32, 4, 0))

	result, err := st.returnResult(proc, map[any]float32{"doc-key": 1.5})
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.Equal(t, []byte{0x01, 0x02, 0x03}, result.Batch.Vecs[0].GetBytesAt(0))
	require.Equal(t, float32(1.5), vector.GetFixedAtWithTypeCheck[float32](result.Batch.Vecs[1], 0))
	require.Empty(t, st.docIDMap)
}

func TestReturnResultUsesCachedBinaryDocIDWithOneAttr(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	st := &Scan{
		docIDMap: map[any]any{
			"doc-key": []byte{0x04, 0x05, 0x06},
		},
		batch: batch.NewWithSize(1),
	}
	st.batch.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 256, 0))

	result, err := st.returnResult(proc, map[any]float32{"doc-key": 2.5})
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.Equal(t, []byte{0x04, 0x05, 0x06}, result.Batch.Vecs[0].GetBytesAt(0))
	require.Empty(t, st.docIDMap)
}

func TestEvaluateKeepsBinaryDocIDUntilOutput(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	s, err := fulltext.NewSearchAccum("src", "index", "pattern", 0, "", fulltext.ALGO_TFIDF)
	require.NoError(t, err)
	s.Nrow = 100

	st := &Scan{
		agghtab:   make(map[any]uint64, 1),
		aggcnt:    make([]int64, s.Nkeywords),
		docLenMap: make(map[any]int32, 1),
		docIDMap:  make(map[any]any, 1),
		mpool:     fulltext.NewFixedBytePool(proc, uint64(s.Nkeywords), 0, 0),
		batch:     batch.NewWithSize(2),
	}
	st.batch.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 256, 0))
	st.batch.Vecs[1] = vector.NewVec(types.New(types.T_float32, 4, 0))
	st.aggcnt[0] = 1

	addr, docvec, err := st.mpool.NewItem()
	require.NoError(t, err)
	docvec[0] = 1
	docID := st.normalizeDocID([]byte("doc-key"))
	st.agghtab[docID] = addr
	st.docLenMap[docID] = 3

	scoremap, err := evaluate(st, proc, s)
	require.NoError(t, err)
	require.Contains(t, st.docIDMap, docID)

	result, err := st.returnResult(proc, scoremap)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.Equal(t, []byte("doc-key"), result.Batch.Vecs[0].GetBytesAt(0))
	require.Empty(t, st.docIDMap)
}

func TestSortTopKPreservesBinaryDocIDUntilOutput(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	s, err := fulltext.NewSearchAccum("src", "index", "pattern", 0, "", fulltext.ALGO_TFIDF)
	require.NoError(t, err)
	s.Nrow = 100

	st := &Scan{
		agghtab:   make(map[any]uint64, 2),
		aggcnt:    make([]int64, s.Nkeywords),
		docLenMap: make(map[any]int32, 2),
		docIDMap:  make(map[any]any, 2),
		mpool:     fulltext.NewFixedBytePool(proc, uint64(s.Nkeywords), 0, 0),
		batch:     batch.NewWithSize(2),
	}
	st.batch.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 256, 0))
	st.batch.Vecs[1] = vector.NewVec(types.New(types.T_float32, 4, 0))
	st.aggcnt[0] = 2

	for i, doc := range [][]byte{[]byte("doc-a"), []byte("doc-b")} {
		addr, docvec, allocErr := st.mpool.NewItem()
		require.NoError(t, allocErr)
		docvec[0] = uint8(2 - i)
		docID := st.normalizeDocID(doc)
		st.agghtab[docID] = addr
		st.docLenMap[docID] = 4
	}

	err = sort_topk(st, proc, s, 1)
	require.NoError(t, err)
	require.Contains(t, st.docIDMap, "doc-a")

	result, err := st.returnResultFromHeap(proc, 1)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.Equal(t, []byte("doc-a"), result.Batch.Vecs[0].GetBytesAt(0))
	require.Empty(t, st.docIDMap)
}

func TestSortTopKRankingReleasesFilteredDocs(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	s, err := fulltext.NewSearchAccum("src", "index", "+apple -banana", int64(tree.FULLTEXT_BOOLEAN), "", fulltext.ALGO_TFIDF)
	require.NoError(t, err)
	s.Nrow = 100

	st := &Scan{
		agghtab:   make(map[any]uint64, 2),
		aggcnt:    make([]int64, s.Nkeywords),
		docLenMap: make(map[any]int32, 2),
		docIDMap:  make(map[any]any, 2),
		mpool:     fulltext.NewFixedBytePool(proc, uint64(s.Nkeywords), 0, 0),
		batch:     batch.NewWithSize(2),
		ranking:   true,
	}
	st.batch.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 256, 0))
	st.batch.Vecs[1] = vector.NewVec(types.New(types.T_float32, 4, 0))
	st.aggcnt[0] = 2
	st.aggcnt[1] = 1

	keepAddr, keepVec, err := st.mpool.NewItem()
	require.NoError(t, err)
	keepVec[0] = 1
	keepID := st.normalizeDocID([]byte("keep"))
	st.agghtab[keepID] = keepAddr
	st.docLenMap[keepID] = 4

	dropAddr, dropVec, err := st.mpool.NewItem()
	require.NoError(t, err)
	dropVec[0] = 1
	dropVec[1] = 1
	dropID := st.normalizeDocID([]byte("drop"))
	st.agghtab[dropID] = dropAddr
	st.docLenMap[dropID] = 5

	err = sort_topk(st, proc, s, 1)
	require.NoError(t, err)
	require.Empty(t, st.agghtab)
	require.Empty(t, st.docLenMap)
	require.Contains(t, st.docIDMap, keepID)
	require.NotContains(t, st.docIDMap, dropID)

	result, err := st.returnResultFromHeap(proc, 1)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.Equal(t, []byte("keep"), result.Batch.Vecs[0].GetBytesAt(0))
	require.Empty(t, st.docIDMap)
}

// TestSortTopKBoundedUnspills is the #25692 review regression for partition
// thrash: agghtab is a hash map whose iteration order has no relation to pool
// partitions, so scoring in map order made GetItem evict and re-materialize a
// whole partition per DOCUMENT once partitions had spilled (one diagnostic
// showed 120 whole-partition reloads for 120 reads). sort_topk now scores in
// partition order; each spilled partition must be materialized a bounded
// number of times — at most once per pass — regardless of map hash order.
func TestSortTopKBoundedUnspills(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	s, err := fulltext.NewSearchAccum("src", "index", "pattern", 0, "", fulltext.ALGO_TFIDF)
	require.NoError(t, err)
	s.Nrow = 1000

	const ndoc = 64
	// dsize = Nkeywords; 4 items per partition; mem_limit of 2 partitions forces
	// spilling during the build phase and keeps a tiny resident set for scoring.
	dsize := uint64(s.Nkeywords)
	st := &Scan{
		agghtab:   make(map[any]uint64, ndoc),
		aggcnt:    make([]int64, s.Nkeywords),
		docLenMap: make(map[any]int32, ndoc),
		mpool:     fulltext.NewFixedBytePool(proc, dsize, 4*dsize, 2*4*dsize),
	}
	defer st.mpool.Close()
	st.aggcnt[0] = ndoc

	for i := 0; i < ndoc; i++ {
		addr, docvec, allocErr := st.mpool.NewItem()
		require.NoError(t, allocErr)
		docvec[0] = uint8(i%250 + 1)
		st.agghtab[i] = addr
		st.docLenMap[i] = int32(i + 1)
	}

	npart := st.mpool.NumPartitions()
	require.Greater(t, npart, 4, "test must span many partitions")

	before := st.mpool.Unspills()
	require.NoError(t, sort_topk(st, proc, s, 8))
	reloads := st.mpool.Unspills() - before

	// Partition-ordered scoring touches each spilled partition at most once. The
	// old map-order traversal produced up to ~ndoc reloads here.
	require.LessOrEqualf(t, reloads, uint64(npart),
		"top-K scoring must not thrash: %d unspills for %d partitions", reloads, npart)
	require.Len(t, st.minheap, 8)
}

// TestEvaluateMultiBatchBoundedWork is the #25692 review regression for the
// zero-LIMIT scoring path: call() re-enters evaluate for every 8K output
// batch, so the partition-ordered traversal must be built ONCE per scoring
// phase and drained across batches. Rebuilding it per batch costs O(N)
// workspace per batch, O(N^2/8192) traversal work overall, and re-materializes
// spilled partitions on every batch. Asserts (a) every doc is scored exactly
// once across multiple batches, (b) a key added after the first batch is NOT
// discovered (a per-batch rebuild would score it), and (c) unspill I/O stays
// bounded by the partition count across ALL batches.
func TestEvaluateMultiBatchBoundedWork(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	s, err := fulltext.NewSearchAccum("src", "index", "pattern", 0, "", fulltext.ALGO_TFIDF)
	require.NoError(t, err)
	s.Nrow = 100000

	const ndoc = 20000
	// 2048 items per partition (~10 partitions), resident set of 2 partitions so
	// the build phase spills and scoring has to unspill.
	dsize := uint64(s.Nkeywords)
	st := &Scan{
		agghtab:   make(map[any]uint64, ndoc),
		aggcnt:    make([]int64, s.Nkeywords),
		docLenMap: make(map[any]int32, ndoc),
		docIDMap:  make(map[any]any),
		mpool:     fulltext.NewFixedBytePool(proc, dsize, 2048*dsize, 2*2048*dsize),
	}
	defer st.mpool.Close()
	st.aggcnt[0] = ndoc

	for i := 0; i < ndoc; i++ {
		addr, docvec, allocErr := st.mpool.NewItem()
		require.NoError(t, allocErr)
		docvec[0] = uint8(i%250 + 1)
		st.agghtab[i] = addr
		st.docLenMap[i] = int32(i%100 + 1)
	}
	require.Greater(t, st.mpool.NumPartitions(), 4, "test must span several partitions")

	before := st.mpool.Unspills()
	seen := make(map[any]struct{}, ndoc)
	batches := 0
	injected := false
	for {
		scoremap, evalErr := evaluate(st, proc, s)
		require.NoError(t, evalErr)
		if len(scoremap) == 0 {
			break
		}
		batches++
		require.LessOrEqual(t, len(scoremap), 8192)
		for k := range scoremap {
			_, dup := seen[k]
			require.Falsef(t, dup, "doc %v scored twice", k)
			seen[k] = struct{}{}
		}
		if !injected {
			// Inject a doc AFTER the first batch. The traversal was snapshot at
			// the first evaluate call; a per-batch rebuild (the regression) would
			// pick this key up and score it, the build-once contract never sees it.
			addr, allocErr := func() (uint64, error) {
				a, docvec, e := st.mpool.NewItem()
				if e == nil {
					docvec[0] = 1
				}
				return a, e
			}()
			require.NoError(t, allocErr)
			st.agghtab["injected"] = addr
			st.docLenMap["injected"] = 1
			injected = true
		}
	}

	require.Len(t, seen, ndoc, "every original doc scored exactly once across batches")
	require.GreaterOrEqual(t, batches, 3, "test must span multiple evaluate batches")
	_, stillThere := st.agghtab["injected"]
	require.True(t, stillThere,
		"ordering must be built once: a key added after the first batch must not be re-discovered by a rebuild")
	require.Len(t, st.agghtab, 1)

	// Each spilled partition materialized at most once across ALL batches (+1 for
	// the unspill the injected NewItem itself may trigger on the tail partition).
	npart := st.mpool.NumPartitions()
	reloads := st.mpool.Unspills() - before
	require.LessOrEqualf(t, reloads, uint64(npart)+1,
		"multi-batch scoring must not thrash: %d unspills for %d partitions across all batches", reloads, npart)
}

// TestEvaluateOrderingBudgetGated: the partition-ordered traversal retains
// ~16 bytes per remaining document OUTSIDE the pool's accounting, so building
// it must be gated on the pool's heap budget instead of allocated
// unconditionally (#25692 review).
func TestEvaluateOrderingBudgetGated(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	s, err := fulltext.NewSearchAccum("src", "index", "pattern", 0, "", fulltext.ALGO_TFIDF)
	require.NoError(t, err)
	s.Nrow = 1000

	dsize := uint64(s.Nkeywords)
	st := &Scan{
		agghtab:   make(map[any]uint64, 8),
		aggcnt:    make([]int64, s.Nkeywords),
		docLenMap: make(map[any]int32, 8),
		docIDMap:  make(map[any]any),
		mpool:     fulltext.NewFixedBytePool(proc, dsize, 4*dsize, 2*4*dsize),
	}
	defer st.mpool.Close()
	st.aggcnt[0] = 8
	for i := 0; i < 8; i++ {
		addr, docvec, allocErr := st.mpool.NewItem()
		require.NoError(t, allocErr)
		docvec[0] = uint8(i + 1)
		st.agghtab[i] = addr
		st.docLenMap[i] = int32(i + 1)
	}

	old := fulltext.HeapBudgetPct
	fulltext.HeapBudgetPct = 0 // every allocation is over budget
	defer func() { fulltext.HeapBudgetPct = old }()

	_, err = evaluate(st, proc, s)
	require.Error(t, err, "ordering workspace must be budget-gated")
	require.Contains(t, err.Error(), "budget")
}

// measureTotalAlloc returns the bytes allocated while f runs (monotonic
// TotalAlloc delta, immune to intervening GC).
func measureTotalAlloc(f func()) uint64 {
	goruntime.GC()
	var before, after goruntime.MemStats
	goruntime.ReadMemStats(&before)
	f()
	goruntime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// TestScoreTraversalWorkspaceExact is the #25692 review regression for the
// traversal workspace: the heap-budget admission estimate must match the real
// peak allocation. The previous append-grown buckets admitted 16 B/key but
// allocated ~5.6x that (growth reallocation, slack capacity, uncounted
// headers). The flat-buffer constructor allocates exactly what
// scoreTraversalEstimate admits.
func TestScoreTraversalWorkspaceExact(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	s, err := fulltext.NewSearchAccum("src", "index", "pattern", 0, "", fulltext.ALGO_TFIDF)
	require.NoError(t, err)
	s.Nrow = 1000000

	const ndoc = 200000
	dsize := uint64(s.Nkeywords)
	st := &Scan{
		agghtab:   make(map[any]uint64, ndoc),
		aggcnt:    make([]int64, s.Nkeywords),
		docLenMap: make(map[any]int32, ndoc),
		docIDMap:  make(map[any]any),
		// production-shaped pool: default partition capacity, no forced spilling
		mpool: fulltext.NewFixedBytePool(proc, dsize, 0, 0),
	}
	defer st.mpool.Close()
	st.aggcnt[0] = ndoc
	for i := 0; i < ndoc; i++ {
		addr, docvec, allocErr := st.mpool.NewItem()
		require.NoError(t, allocErr)
		docvec[0] = 1
		st.agghtab[i] = addr
		st.docLenMap[i] = 1
	}

	est := scoreTraversalEstimate(len(st.agghtab), st.mpool.NumPartitions())

	// Byte bound: the real allocation must not exceed the admitted estimate
	// (modulo a small fixed slack for allocator rounding and test noise).
	var keys []any
	measured := measureTotalAlloc(func() {
		var buildErr error
		keys, buildErr = partitionOrderedKeys(proc, st.agghtab, st.mpool)
		require.NoError(t, buildErr)
	})
	require.Len(t, keys, ndoc)
	const slack = 256 << 10
	require.LessOrEqualf(t, measured, est+uint64(slack),
		"traversal allocated %d bytes but the budget only admitted %d", measured, est)

	// Structural bound: constant number of allocations — no append growth chains.
	allocs := testing.AllocsPerRun(3, func() {
		k, buildErr := partitionOrderedKeys(proc, st.agghtab, st.mpool)
		require.NoError(t, buildErr)
		_ = k
	})
	require.LessOrEqualf(t, allocs, 8.0,
		"traversal must preallocate exactly, got %.0f allocations", allocs)
}

// TestEvaluateSparseScoreBounded is the #25692 review regression for sparse
// results: a query whose candidates mostly produce NO score (e.g. boolean
// +required words filtering the aggregated union) previously accumulated every
// processed candidate in an ungated O(N) keys slice within a single evaluate
// call. Candidates must be freed and deleted as they are consumed, so the
// all-filtered call allocates only the (budget-admitted) traversal plus O(1).
func TestEvaluateSparseScoreBounded(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	s, err := fulltext.NewSearchAccum("src", "index", "pattern", 0, "", fulltext.ALGO_TFIDF)
	require.NoError(t, err)
	s.Nrow = 1000000

	const ndoc = 200000
	dsize := uint64(s.Nkeywords)
	st := &Scan{
		agghtab:   make(map[any]uint64, ndoc),
		aggcnt:    make([]int64, s.Nkeywords),
		docLenMap: make(map[any]int32, ndoc),
		docIDMap:  make(map[any]any),
		mpool:     fulltext.NewFixedBytePool(proc, dsize, 0, 0),
	}
	defer st.mpool.Close()
	st.aggcnt[0] = ndoc
	for i := 0; i < ndoc; i++ {
		addr, docvec, allocErr := st.mpool.NewItem()
		require.NoError(t, allocErr)
		docvec[0] = 0 // keyword count 0 -> Eval yields no score for ANY candidate
		st.agghtab[i] = addr
		st.docLenMap[i] = 1
	}

	est := scoreTraversalEstimate(len(st.agghtab), st.mpool.NumPartitions())

	var scoremap map[any]float32
	measured := measureTotalAlloc(func() {
		var evalErr error
		scoremap, evalErr = evaluate(st, proc, s)
		require.NoError(t, evalErr)
	})

	// All candidates filtered: no results, and every candidate was consumed and
	// released immediately rather than accumulated.
	require.Empty(t, scoremap)
	require.Empty(t, st.agghtab, "candidates must be deleted as they are consumed")
	require.Empty(t, st.docLenMap)
	require.Empty(t, st.docIDMap)

	// Memory bound: the whole all-filtered pass allocates the traversal plus
	// small constants — NOT a second O(N) interface buffer (the old keys slice
	// added ~20 MB at this size).
	const slack = 2 << 20
	require.LessOrEqualf(t, measured, est+uint64(slack),
		"sparse evaluate allocated %d bytes; traversal estimate is %d", measured, est)

	// Traversal fully drained: the next call returns an empty batch.
	scoremap, err = evaluate(st, proc, s)
	require.NoError(t, err)
	require.Empty(t, scoremap)
}

// A snapshotted MATCH puts the snapshot TS on the SqlProcess.
func TestFulltextSQLProcessCarriesSnapshotTS(t *testing.T) {
	proc := testutil.NewProc(t)

	ts := timestamp.Timestamp{PhysicalTime: 1700000000, LogicalTime: 7}
	u := &Scan{scanSnapshot: &plan.Snapshot{TS: &ts}}

	sp := u.sqlProcess(proc)
	require.NotNil(t, sp.SnapshotTS, "a snapshotted MATCH must carry the read TS onto the SqlProcess")
	require.Equal(t, ts.PhysicalTime, sp.SnapshotTS.PhysicalTime)
	require.Equal(t, ts.LogicalTime, sp.SnapshotTS.LogicalTime)
}

// Without a snapshot the SqlProcess carries no TS.
func TestFulltextSQLProcessNoSnapshotLeavesTSNil(t *testing.T) {
	proc := testutil.NewProc(t)

	require.Nil(t, (&Scan{}).sqlProcess(proc).SnapshotTS,
		"an unsnapshotted MATCH must leave the read at the current txn")
}

// A subscribed table read at a named snapshot resolves under the PUBLISHER, not under the
// snapshot's tenant. The two identities disagree here and both used to be written into
// AccountIDOverride, so whichever setter ran last won -- a silent dependency on the order of
// two adjacent lines. The mocked txn is what makes this reachable: ApplyScanSnapshot binds a
// tenant only when EffectiveSnapshotTS reports a historical read, and testutil.NewProc has no
// TxnOperator, so a test without one exercises the early return and cannot tell the orders
// apart.
func TestFulltextSQLProcessPublisherOutranksSnapshotTenant(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	// The read txn is NEWER than the snapshot, so the snapshot TS is genuinely historical.
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{
		SnapshotTS: timestamp.Timestamp{PhysicalTime: 1800000000},
	}).AnyTimes()
	proc.Base.TxnOperator = txnOp

	const publisher = uint32(42)
	const snapshotTenant = uint32(7)
	pub := publisher
	ts := timestamp.Timestamp{PhysicalTime: 1700000000, LogicalTime: 7}
	u := &Scan{
		scanSnapshot: &plan.Snapshot{
			TS:     &ts,
			Tenant: &plan.SnapshotTenant{TenantID: snapshotTenant},
		},
		publisherAccount: &pub,
		publisherDB:      "pub_db",
	}

	sp := u.sqlProcess(proc)

	require.NotNil(t, sp.SnapshotTS, "the read stays historical")
	require.Equal(t, ts.PhysicalTime, sp.SnapshotTS.PhysicalTime)
	require.NotNil(t, sp.EffectiveSnapshotTS(), "and the tenant branch was actually reached")

	// Both identities are recorded, neither clobbers the other.
	require.NotNil(t, sp.AccountIDOverride)
	require.Equal(t, publisher, *sp.AccountIDOverride)
	require.NotNil(t, sp.SnapshotAccountID)
	require.Equal(t, snapshotTenant, *sp.SnapshotAccountID)

	// And the account the index-table SQL actually runs as is the publisher, which is where
	// those tables live -- matching the base-table scan's own PubInfo-over-snapshot precedence.
	got, err := sp.EffectiveAccountID()
	require.NoError(t, err)
	require.Equal(t, publisher, got, "the publisher owns the index tables, not the snapshot tenant")
	require.Equal(t, "pub_db", sp.DatabaseOverride)
}

// With no publisher, the snapshot's tenant is what the read resolves under -- the cross-account
// case ApplyScanSnapshot exists for.
func TestFulltextSQLProcessSnapshotTenantAppliesWithoutAPublisher(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{
		SnapshotTS: timestamp.Timestamp{PhysicalTime: 1800000000},
	}).AnyTimes()
	proc.Base.TxnOperator = txnOp

	ts := timestamp.Timestamp{PhysicalTime: 1700000000, LogicalTime: 7}
	u := &Scan{scanSnapshot: &plan.Snapshot{
		TS:     &ts,
		Tenant: &plan.SnapshotTenant{TenantID: 7},
	}}

	sp := u.sqlProcess(proc)
	require.Nil(t, sp.AccountIDOverride, "nothing published, so no execution identity")
	got, err := sp.EffectiveAccountID()
	require.NoError(t, err)
	require.EqualValues(t, 7, got, "the snapshot's owning tenant still binds")
}

// resetRowState clears the snapshot.
func TestFulltextResetRowStateClearsSnapshot(t *testing.T) {
	proc := testutil.NewProc(t)

	ts := timestamp.Timestamp{PhysicalTime: 1700000000}
	u := &Scan{scanSnapshot: &plan.Snapshot{TS: &ts}}

	u.ResetRowState(proc)
	require.Nil(t, u.scanSnapshot, "resetRowState must clear the per-row snapshot")
	require.Nil(t, u.sqlProcess(proc).SnapshotTS, "and the next SqlProcess must read current")
}

func newFTTestProcess(t *testing.T, m *mpool.MPool, algo fulltext.FullTextScoreAlgo) *process.Process {
	proc := testutil.NewProcessWithMPool(t, "", m)
	proc.SetResolveVariableFunc(func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		if varName == fulltext.FulltextRelevancyAlgo {
			if algo == fulltext.ALGO_BM25 {
				return fulltext.FulltextRelevancyAlgo_bm25, nil
			} else if algo == fulltext.ALGO_TFIDF {
				return fulltext.FulltextRelevancyAlgo_tfidf, nil
			}
			return fulltext.FulltextRelevancyAlgo_bm25, nil
		}
		return nil, nil
	})
	return proc
}

func fake_runSql(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
	proc := sqlproc.Proc
	// give count
	return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeCountBatchFT(proc)}}, nil
}

func fake_runSql_streaming(
	ctx context.Context,
	sqlproc *sqlexec.SqlProcess,
	sql string,
	ch chan executor.Result,
	err_chan chan error,
) (executor.Result, error) {
	proc := sqlproc.Proc
	res := executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeTextBatchFT(proc)}}
	ch <- res
	return executor.Result{}, nil
}

// create count (int64)
func makeCountBatchFT(proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_int64, 8, 0))
	bat.Vecs[1] = vector.NewVec(types.New(types.T_float64, 8, 4))

	vector.AppendFixed[int64](bat.Vecs[0], int64(100), false, proc.Mp())
	vector.AppendFixed[float64](bat.Vecs[1], float64(10.6666), false, proc.Mp())

	bat.SetRowCount(1)
	return bat
}

func makeCountOnlyBatchFT(proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_int64, 8, 0))

	vector.AppendFixed[int64](bat.Vecs[0], int64(100), false, proc.Mp())

	bat.SetRowCount(1)
	return bat
}

func makeSmallTextBatchFT(proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_int32, 4, 0))
	bat.Vecs[1] = vector.NewVec(types.New(types.T_int32, 4, 0))
	bat.Vecs[2] = vector.NewVec(types.New(types.T_int32, 4, 0))

	vector.AppendFixed[int32](bat.Vecs[0], int32(7), false, proc.Mp())
	vector.AppendFixed[int32](bat.Vecs[1], int32(0), false, proc.Mp())
	vector.AppendFixed[int32](bat.Vecs[2], int32(4), false, proc.Mp())
	vector.AppendFixed[int32](bat.Vecs[0], int32(8), false, proc.Mp())
	vector.AppendFixed[int32](bat.Vecs[1], int32(0), false, proc.Mp())
	vector.AppendFixed[int32](bat.Vecs[2], int32(5), false, proc.Mp())

	bat.SetRowCount(2)
	return bat
}

// create (doc_id, text)
func makeTextBatchFT(proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_int32, 4, 0)) // doc_id
	bat.Vecs[1] = vector.NewVec(types.New(types.T_int32, 4, 0)) // word index
	bat.Vecs[2] = vector.NewVec(types.New(types.T_int32, 4, 0)) // word index

	nitem := 8192*3 + 1
	for i := 0; i < nitem; i++ {
		// doc_id
		vector.AppendFixed[int32](bat.Vecs[0], int32(i), false, proc.Mp())

		// word index
		vector.AppendFixed[int32](bat.Vecs[1], int32(0), false, proc.Mp())

		// doc len
		docLen := rand.Intn(20)
		vector.AppendFixed[int32](bat.Vecs[2], int32(docLen), false, proc.Mp())
	}

	bat.SetRowCount(nitem)
	return bat
}
