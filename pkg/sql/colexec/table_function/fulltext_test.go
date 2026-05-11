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

package table_function

import (
	"context"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type fulltextTestCase struct {
	arg  *TableFunction
	proc *process.Process
}

var (
	ftdefaultAttrs = []string{"DOC_ID", "SCORE"}

	ftdefaultColdefs = []*plan.ColDef{
		// row_id type should be same as index type
		{
			Name: "DOC_ID",
			Typ: plan.Type{
				Id:          int32(types.T_int32),
				NotNullable: false,
			},
		},
		{
			Name: "SCORE",
			Typ: plan.Type{
				Id:          int32(types.T_float32),
				NotNullable: false,
				Width:       4,
			},
		},
	}
)

func newFTTestCase(t *testing.T, m *mpool.MPool, attrs []string, algo fulltext.FullTextScoreAlgo, limit uint64) fulltextTestCase {
	proc := newFTTestProcess(t, m, algo)
	colDefs := make([]*plan.ColDef, len(attrs))
	for i := range attrs {
		for j := range ftdefaultColdefs {
			if attrs[i] == ftdefaultColdefs[j].Name {
				colDefs[i] = ftdefaultColdefs[j]
				break
			}
		}
	}

	ret := fulltextTestCase{
		proc: proc,
		arg: &TableFunction{
			Attrs:    attrs,
			Rets:     colDefs,
			FuncName: "fulltext_index_scan",
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
			Limit: &plan.Expr{
				Typ: plan.Type{
					Id: int32(types.T_uint64),
				},
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Value: &plan.Literal_U64Val{
							U64Val: limit,
						},
					},
				},
			},
		},
	}
	return ret
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
	// give count
	proc := sqlproc.Proc
	return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeCountBatchFT(proc)}}, nil
}

func fake_runSql_streaming(
	ctx context.Context,
	proc *process.Process,
	sql string,
	ch chan executor.Result,
	err_chan chan error,
) (executor.Result, error) {
	res := executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeTextBatchFT(proc)}}
	ch <- res
	return executor.Result{}, nil
}

// argvec [src_tbl, index_tbl, pattern, mode int64]
func TestFullTextCall(t *testing.T) {

	ut := newFTTestCase(t, mpool.MustNewZero(), ftdefaultAttrs, fulltext.ALGO_TFIDF, uint64(0))

	inbat := makeBatchFT(ut.proc)

	ut.arg.Args = makeConstInputExprsFT()
	//fmt.Printf("%v\n", ut.arg.Args)

	// Prepare
	err := ut.arg.Prepare(ut.proc)
	require.Nil(t, err)

	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.Nil(t, err)
	}

	// stub runSql function
	ft_runSql = fake_runSql
	ft_runSql_streaming = fake_runSql_streaming

	// start
	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.Nil(t, err)

	var result vm.CallResult

	// first call receive data
	for i := 0; i < 3; i++ {
		result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
		require.Nil(t, err)
		require.Equal(t, result.Status, vm.ExecNext)
		require.Equal(t, result.Batch.RowCount(), 8192)
	}

	result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.Nil(t, err)
	require.Equal(t, result.Status, vm.ExecNext)
	require.Equal(t, result.Batch.RowCount(), 1)
	//fmt.Printf("ROW COUNT = %d  BATCH = %v\n", result.Batch.RowCount(), result.Batch)

	// second call receive channel close
	result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.Nil(t, err)
	require.Equal(t, result.Status, vm.ExecStop)

	// reset
	ut.arg.ctr.state.reset(ut.arg, ut.proc)

	// free
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

// argvec [src_tbl, index_tbl, pattern, mode int64]
func TestFullTextCallWithLimitByRank(t *testing.T) {

	ut := newFTTestCase(t, mpool.MustNewZero(), ftdefaultAttrs, fulltext.ALGO_TFIDF, uint64(128))

	inbat := makeBatchFT(ut.proc)

	ut.arg.Args = makeConstInputExprsFT()
	//fmt.Printf("%v\n", ut.arg.Args)

	// Prepare
	err := ut.arg.Prepare(ut.proc)
	require.Nil(t, err)

	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.Nil(t, err)
	}

	// stub runSql function
	ft_runSql = fake_runSql
	ft_runSql_streaming = fake_runSql_streaming

	// enable LIMIT BY RANK
	ut.arg.ctr.state.(*fulltextState).ranking = true

	// start
	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.Nil(t, err)

	var result vm.CallResult

	// first call receive data
	for i := 0; i < 192; i++ {
		result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
		require.Nil(t, err)
		require.Equal(t, result.Status, vm.ExecNext)
		require.Equal(t, result.Batch.RowCount(), 128)
	}

	result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.Nil(t, err)
	require.Equal(t, result.Status, vm.ExecNext)
	require.Equal(t, result.Batch.RowCount(), 1)
	//fmt.Printf("ROW COUNT = %d  BATCH = %v\n", result.Batch.RowCount(), result.Batch)

	// second call receive channel close
	result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.Nil(t, err)
	require.Equal(t, result.Status, vm.ExecStop)

	// reset
	ut.arg.ctr.state.reset(ut.arg, ut.proc)

	// free
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

// argvec [src_tbl, index_tbl, pattern, mode int64]
func TestFullTextCallOneAttr(t *testing.T) {

	ut := newFTTestCase(t, mpool.MustNewZero(), ftdefaultAttrs[0:1], fulltext.ALGO_TFIDF, uint64(0))

	inbat := makeBatchFT(ut.proc)

	ut.arg.Args = makeConstInputExprsFT()
	//fmt.Printf("%v\n", ut.arg.Args)

	// Prepare
	err := ut.arg.Prepare(ut.proc)
	require.Nil(t, err)

	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.Nil(t, err)
	}

	// stub runSql function
	ft_runSql = fake_runSql
	ft_runSql_streaming = fake_runSql_streaming

	// start
	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.Nil(t, err)

	var result vm.CallResult

	// first call receive data
	for i := 0; i < 3; i++ {
		result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
		require.Nil(t, err)
		require.Equal(t, result.Status, vm.ExecNext)
		require.Equal(t, result.Batch.RowCount(), 8192)
	}

	result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.Nil(t, err)
	require.Equal(t, result.Status, vm.ExecNext)
	require.Equal(t, result.Batch.RowCount(), 1)
	//fmt.Printf("ROW COUNT = %d  BATCH = %v\n", result.Batch.RowCount(), result.Batch)

	// second call receive channel close
	result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.Nil(t, err)
	require.Equal(t, result.Status, vm.ExecStop)

	// reset
	ut.arg.ctr.state.reset(ut.arg, ut.proc)

	// free
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

// argvec [src_tbl, index_tbl, pattern, mode int64]
func TestFullTextEarlyFree(t *testing.T) {

	ut := newFTTestCase(t, mpool.MustNewZero(), ftdefaultAttrs[0:1], fulltext.ALGO_TFIDF, uint64(0))

	inbat := makeBatchFT(ut.proc)

	ut.arg.Args = makeConstInputExprsFT()
	//fmt.Printf("%v\n", ut.arg.Args)

	// Prepare
	err := ut.arg.Prepare(ut.proc)
	require.Nil(t, err)

	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.Nil(t, err)
	}

	// stub runSql function
	ft_runSql = fake_runSql
	ft_runSql_streaming = fake_runSql_streaming

	// start
	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.Nil(t, err)

	/*
		var result vm.CallResult
		// first call receive data
		for i := 0; i < 2; i++ {
			result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
			require.Nil(t, err)
			require.Equal(t, result.Status, vm.ExecNext)
			require.Equal(t, result.Batch.RowCount(), 8192)
		}
	*/

	// early free
	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

func TestRunCountStarUsesCountOnlyForTFIDF(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	s := &fulltext.SearchAccum{TblName: "idx_table", ScoreAlgo: fulltext.ALGO_TFIDF}

	prev := ft_runSql
	defer func() { ft_runSql = prev }()

	var gotSQL string
	ft_runSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		gotSQL = sql
		return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeCountOnlyBatchFT(proc)}}, nil
	}

	_, err := runCountStar(proc, s)
	require.NoError(t, err)
	require.Equal(t, "SELECT COUNT(*) from idx_table where word = '__DocLen'", gotSQL)
	require.Equal(t, int64(100), s.Nrow)
	require.Zero(t, s.AvgDocLen)
}

func TestRunCountStarUsesDedupedDocLenForBM25(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	s := &fulltext.SearchAccum{TblName: "idx_table", ScoreAlgo: fulltext.ALGO_BM25}

	prev := ft_runSql
	defer func() { ft_runSql = prev }()

	var gotSQL string
	ft_runSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		gotSQL = sql
		return executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeCountBatchFT(proc)}}, nil
	}

	_, err := runCountStar(proc, s)
	require.NoError(t, err)
	require.Equal(t, "SELECT COUNT(*), AVG(pos) from (SELECT doc_id, MAX(pos) AS pos from idx_table where word = '__DocLen' GROUP BY doc_id) doc_len", gotSQL)
	require.Equal(t, int64(100), s.Nrow)
	require.InDelta(t, 10.6666, s.AvgDocLen, 1e-9)
}

func TestSortTopKReleasesAggregates(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	s, err := fulltext.NewSearchAccum("src", "index", "pattern", 0, "", fulltext.ALGO_TFIDF)
	require.NoError(t, err)
	s.Nrow = 100

	st := &fulltextState{
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
	st := &fulltextState{
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
	st := &fulltextState{
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

	st := &fulltextState{
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

	st := &fulltextState{
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

	st := &fulltextState{
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

func TestFullTextCallWithLimitUsesSingleKeywordTopK(t *testing.T) {
	ut := newFTTestCase(t, mpool.MustNewZero(), ftdefaultAttrs, fulltext.ALGO_TFIDF, uint64(2))

	inbat := makeBatchFT(ut.proc)
	ut.arg.Args = makeConstInputExprsFT()

	err := ut.arg.Prepare(ut.proc)
	require.NoError(t, err)
	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.NoError(t, err)
	}

	prevRunSQL := ft_runSql
	prevRunStreaming := ft_runSql_streaming
	defer func() {
		ft_runSql = prevRunSQL
		ft_runSql_streaming = prevRunStreaming
	}()

	ft_runSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		switch {
		case strings.Contains(sql, "COUNT(*) from index_table where word = '__DocLen'"):
			return executor.Result{Mp: sqlproc.Proc.Mp(), Batches: []*batch.Batch{makeCountOnlyBatchFT(sqlproc.Proc)}}, nil
		case strings.Contains(sql, "COUNT(*) OVER() AS nmatch") && strings.Contains(sql, "ORDER BY tf DESC LIMIT 2"):
			return executor.Result{Mp: sqlproc.Proc.Mp(), Batches: []*batch.Batch{makeTopKBatchFT(sqlproc.Proc)}}, nil
		default:
			return executor.Result{}, moerr.NewInternalErrorf(sqlproc.Proc.Ctx, "unexpected SQL: %s", sql)
		}
	}
	ft_runSql_streaming = func(
		ctx context.Context,
		proc *process.Process,
		sql string,
		ch chan executor.Result,
		errChan chan error,
	) (executor.Result, error) {
		return executor.Result{}, moerr.NewInternalError(proc.Ctx, "streaming SQL should not be used in single-keyword top-k path")
	}

	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.NoError(t, err)

	result, err := ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.Equal(t, 2, result.Batch.RowCount())
	require.Equal(t, int32(11), vector.GetFixedAtWithTypeCheck[int32](result.Batch.Vecs[0], 0))
	require.Equal(t, int32(12), vector.GetFixedAtWithTypeCheck[int32](result.Batch.Vecs[0], 1))

	result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)

	requireStateFreeReturns(t, ut.arg.ctr.state, ut.arg, ut.proc)
}

func TestFullTextCallWithLimitZeroMatchShortCircuits(t *testing.T) {
	ut := newFTTestCase(t, mpool.MustNewZero(), ftdefaultAttrs, fulltext.ALGO_TFIDF, uint64(2))

	inbat := makeBatchFT(ut.proc)
	ut.arg.Args = makeConstInputExprsFT()

	err := ut.arg.Prepare(ut.proc)
	require.NoError(t, err)
	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.NoError(t, err)
	}

	prevRunSQL := ft_runSql
	prevRunStreaming := ft_runSql_streaming
	defer func() {
		ft_runSql = prevRunSQL
		ft_runSql_streaming = prevRunStreaming
	}()

	topKCalled := false
	streamingCalled := false
	ft_runSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		switch {
		case strings.Contains(sql, "COUNT(*) from index_table where word = '__DocLen'"):
			return executor.Result{Mp: sqlproc.Proc.Mp(), Batches: []*batch.Batch{makeCountOnlyBatchFT(sqlproc.Proc)}}, nil
		case strings.Contains(sql, "COUNT(*) OVER() AS nmatch") && strings.Contains(sql, "ORDER BY tf DESC LIMIT 2"):
			topKCalled = true
			return executor.Result{Mp: sqlproc.Proc.Mp(), Batches: []*batch.Batch{makeEmptyTopKBatchFT(sqlproc.Proc)}}, nil
		default:
			return executor.Result{}, moerr.NewInternalErrorf(sqlproc.Proc.Ctx, "unexpected SQL: %s", sql)
		}
	}
	ft_runSql_streaming = func(
		ctx context.Context,
		proc *process.Process,
		sql string,
		ch chan executor.Result,
		errChan chan error,
	) (executor.Result, error) {
		streamingCalled = true
		return executor.Result{}, moerr.NewInternalError(proc.Ctx, "streaming SQL should not run when count is zero")
	}

	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.NoError(t, err)
	require.True(t, topKCalled)
	require.False(t, streamingCalled)

	result, err := ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)
	require.Nil(t, result.Batch)

	requireStateFreeReturns(t, ut.arg.ctr.state, ut.arg, ut.proc)
}

func TestFullTextCallWithLimitUsesSingleKeywordTopKBM25(t *testing.T) {
	ut := newFTTestCase(t, mpool.MustNewZero(), ftdefaultAttrs, fulltext.ALGO_BM25, uint64(2))

	inbat := makeBatchFT(ut.proc)
	ut.arg.Args = makeConstInputExprsFT()

	err := ut.arg.Prepare(ut.proc)
	require.NoError(t, err)
	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.NoError(t, err)
	}

	prevRunSQL := ft_runSql
	prevRunStreaming := ft_runSql_streaming
	defer func() {
		ft_runSql = prevRunSQL
		ft_runSql_streaming = prevRunStreaming
	}()

	ft_runSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		switch {
		case strings.Contains(sql, "COUNT(*), AVG(pos)"):
			return executor.Result{Mp: sqlproc.Proc.Mp(), Batches: []*batch.Batch{makeCountBatchFT(sqlproc.Proc)}}, nil
		case strings.Contains(sql, "COUNT(*) OVER() AS nmatch") && strings.Contains(sql, "ORDER BY score DESC LIMIT 2"):
			return executor.Result{Mp: sqlproc.Proc.Mp(), Batches: []*batch.Batch{makeTopKBM25BatchFT(sqlproc.Proc)}}, nil
		default:
			return executor.Result{}, moerr.NewInternalErrorf(sqlproc.Proc.Ctx, "unexpected SQL: %s", sql)
		}
	}
	ft_runSql_streaming = func(
		ctx context.Context,
		proc *process.Process,
		sql string,
		ch chan executor.Result,
		errChan chan error,
	) (executor.Result, error) {
		return executor.Result{}, moerr.NewInternalError(proc.Ctx, "streaming SQL should not be used in single-keyword BM25 top-k path")
	}

	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.NoError(t, err)

	result, err := ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.Equal(t, 2, result.Batch.RowCount())
	require.Equal(t, int32(21), vector.GetFixedAtWithTypeCheck[int32](result.Batch.Vecs[0], 0))
	require.Equal(t, int32(22), vector.GetFixedAtWithTypeCheck[int32](result.Batch.Vecs[0], 1))
	require.InDelta(t, 2.75, vector.GetFixedAtWithTypeCheck[float32](result.Batch.Vecs[1], 0), 1e-6)
	require.InDelta(t, 1.5, vector.GetFixedAtWithTypeCheck[float32](result.Batch.Vecs[1], 1), 1e-6)

	result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)

	requireStateFreeReturns(t, ut.arg.ctr.state, ut.arg, ut.proc)
}

func TestFullTextCallWithLimitBooleanFallsBackToStreaming(t *testing.T) {
	ut := newFTTestCase(t, mpool.MustNewZero(), ftdefaultAttrs, fulltext.ALGO_TFIDF, uint64(2))

	inbat := makeBatchFT(ut.proc)
	ut.arg.Args = makeConstInputExprsFTWithPattern("+Matrix", int64(tree.FULLTEXT_BOOLEAN))

	err := ut.arg.Prepare(ut.proc)
	require.NoError(t, err)
	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.NoError(t, err)
	}

	prevRunSQL := ft_runSql
	prevRunStreaming := ft_runSql_streaming
	defer func() {
		ft_runSql = prevRunSQL
		ft_runSql_streaming = prevRunStreaming
	}()

	streamingCalled := false
	ft_runSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		switch {
		case strings.Contains(sql, "COUNT(*) from index_table where word = '__DocLen'"):
			return executor.Result{Mp: sqlproc.Proc.Mp(), Batches: []*batch.Batch{makeCountOnlyBatchFT(sqlproc.Proc)}}, nil
		default:
			return executor.Result{}, moerr.NewInternalErrorf(sqlproc.Proc.Ctx, "unexpected SQL: %s", sql)
		}
	}
	ft_runSql_streaming = func(
		ctx context.Context,
		proc *process.Process,
		sql string,
		ch chan executor.Result,
		errChan chan error,
	) (executor.Result, error) {
		streamingCalled = true
		ch <- executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeSmallTextBatchFT(proc)}}
		return executor.Result{}, nil
	}

	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.NoError(t, err)
	require.True(t, streamingCalled)

	result, err := ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.Equal(t, 2, result.Batch.RowCount())

	result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)

	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

func TestFullTextCallWithLimitPhraseFallsBackToStreaming(t *testing.T) {
	ut := newFTTestCase(t, mpool.MustNewZero(), ftdefaultAttrs, fulltext.ALGO_TFIDF, uint64(2))

	inbat := makeBatchFT(ut.proc)
	ut.arg.Args = makeConstInputExprsFTWithPattern("Matrix Origin", 0)

	err := ut.arg.Prepare(ut.proc)
	require.NoError(t, err)
	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.NoError(t, err)
	}

	prevRunSQL := ft_runSql
	prevRunStreaming := ft_runSql_streaming
	defer func() {
		ft_runSql = prevRunSQL
		ft_runSql_streaming = prevRunStreaming
	}()

	streamingCalled := false
	ft_runSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		switch {
		case strings.Contains(sql, "COUNT(*) from index_table where word = '__DocLen'"):
			return executor.Result{Mp: sqlproc.Proc.Mp(), Batches: []*batch.Batch{makeCountOnlyBatchFT(sqlproc.Proc)}}, nil
		default:
			return executor.Result{}, moerr.NewInternalErrorf(sqlproc.Proc.Ctx, "unexpected SQL: %s", sql)
		}
	}
	ft_runSql_streaming = func(
		ctx context.Context,
		proc *process.Process,
		sql string,
		ch chan executor.Result,
		errChan chan error,
	) (executor.Result, error) {
		streamingCalled = true
		ch <- executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeSmallTextBatchFT(proc)}}
		return executor.Result{}, nil
	}

	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.NoError(t, err)
	require.True(t, streamingCalled)

	result, err := ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.Equal(t, 2, result.Batch.RowCount())

	result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)

	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

func TestFullTextCallWithQuotedPhraseFallsBackToStreaming(t *testing.T) {
	ut := newFTTestCase(t, mpool.MustNewZero(), ftdefaultAttrs, fulltext.ALGO_TFIDF, uint64(2))

	inbat := makeBatchFT(ut.proc)
	ut.arg.Args = makeConstInputExprsFTWithPattern("\"Matrix Origin\"", int64(tree.FULLTEXT_BOOLEAN))

	err := ut.arg.Prepare(ut.proc)
	require.NoError(t, err)
	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.NoError(t, err)
	}

	prevRunSQL := ft_runSql
	prevRunStreaming := ft_runSql_streaming
	defer func() {
		ft_runSql = prevRunSQL
		ft_runSql_streaming = prevRunStreaming
	}()

	streamingCalled := false
	ft_runSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		switch {
		case strings.Contains(sql, "COUNT(*) from index_table where word = '__DocLen'"):
			return executor.Result{Mp: sqlproc.Proc.Mp(), Batches: []*batch.Batch{makeCountOnlyBatchFT(sqlproc.Proc)}}, nil
		default:
			return executor.Result{}, moerr.NewInternalErrorf(sqlproc.Proc.Ctx, "unexpected SQL: %s", sql)
		}
	}
	ft_runSql_streaming = func(
		ctx context.Context,
		proc *process.Process,
		sql string,
		ch chan executor.Result,
		errChan chan error,
	) (executor.Result, error) {
		streamingCalled = true
		ch <- executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeSmallTextBatchFT(proc)}}
		return executor.Result{}, nil
	}

	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.NoError(t, err)
	require.True(t, streamingCalled)

	result, err := ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.Equal(t, 2, result.Batch.RowCount())

	result, err = ut.arg.ctr.state.call(ut.arg, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)

	ut.arg.ctr.state.free(ut.arg, ut.proc, false, nil)
}

func TestFullTextFastPathFreeReturnsWithoutStreaming(t *testing.T) {
	ut := newFTTestCase(t, mpool.MustNewZero(), ftdefaultAttrs, fulltext.ALGO_TFIDF, uint64(2))

	inbat := makeBatchFT(ut.proc)
	ut.arg.Args = makeConstInputExprsFT()

	err := ut.arg.Prepare(ut.proc)
	require.NoError(t, err)
	for i := range ut.arg.ctr.executorsForArgs {
		ut.arg.ctr.argVecs[i], err = ut.arg.ctr.executorsForArgs[i].Eval(ut.proc, []*batch.Batch{inbat}, nil)
		require.NoError(t, err)
	}

	prevRunSQL := ft_runSql
	prevRunStreaming := ft_runSql_streaming
	defer func() {
		ft_runSql = prevRunSQL
		ft_runSql_streaming = prevRunStreaming
	}()

	ft_runSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		switch {
		case strings.Contains(sql, "COUNT(*) from index_table where word = '__DocLen'"):
			return executor.Result{Mp: sqlproc.Proc.Mp(), Batches: []*batch.Batch{makeCountOnlyBatchFT(sqlproc.Proc)}}, nil
		case strings.Contains(sql, "COUNT(*) OVER() AS nmatch") && strings.Contains(sql, "ORDER BY tf DESC LIMIT 2"):
			return executor.Result{Mp: sqlproc.Proc.Mp(), Batches: []*batch.Batch{makeTopKBatchFT(sqlproc.Proc)}}, nil
		default:
			return executor.Result{}, moerr.NewInternalErrorf(sqlproc.Proc.Ctx, "unexpected SQL: %s", sql)
		}
	}
	ft_runSql_streaming = func(
		ctx context.Context,
		proc *process.Process,
		sql string,
		ch chan executor.Result,
		errChan chan error,
	) (executor.Result, error) {
		return executor.Result{}, moerr.NewInternalError(proc.Ctx, "streaming SQL should not be used in single-keyword top-k path")
	}

	err = ut.arg.ctr.state.start(ut.arg, ut.proc, 0, nil)
	require.NoError(t, err)

	requireStateFreeReturns(t, ut.arg.ctr.state, ut.arg, ut.proc)
}

func TestFullTextStartResetsStateForLaterRowsFastPath(t *testing.T) {
	ut := newFTTestCase(t, mpool.MustNewZero(), ftdefaultAttrs, fulltext.ALGO_TFIDF, uint64(2))
	err := ut.arg.Prepare(ut.proc)
	require.NoError(t, err)

	tf := ut.arg
	tf.ctr.argVecs = makeMultiRowArgVecsFT(ut.proc,
		fulltextInputRow{source: "src0", index: "idx0", pattern: "Matrix", mode: int64(tree.FULLTEXT_NL)},
		fulltextInputRow{source: "src1", index: "idx1", pattern: "Apple", mode: int64(tree.FULLTEXT_NL)},
	)
	st := tf.ctr.state.(*fulltextState)

	prevRunSQL := ft_runSql
	prevRunStreaming := ft_runSql_streaming
	defer func() {
		ft_runSql = prevRunSQL
		ft_runSql_streaming = prevRunStreaming
	}()

	ft_runSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		switch {
		case strings.Contains(sql, "COUNT(*) from idx0 where word = '__DocLen'"),
			strings.Contains(sql, "COUNT(*) from idx1 where word = '__DocLen'"):
			return executor.Result{Mp: sqlproc.Proc.Mp(), Batches: []*batch.Batch{makeCountOnlyBatchFT(sqlproc.Proc)}}, nil
		case strings.Contains(sql, "COUNT(*) OVER() AS nmatch") && strings.Contains(sql, "ORDER BY tf DESC LIMIT 2") && strings.Contains(sql, "idx0"):
			return executor.Result{Mp: sqlproc.Proc.Mp(), Batches: []*batch.Batch{makeTopKBatchFTWithDocs(sqlproc.Proc, 11, 12)}}, nil
		case strings.Contains(sql, "COUNT(*) OVER() AS nmatch") && strings.Contains(sql, "ORDER BY tf DESC LIMIT 2") && strings.Contains(sql, "idx1"):
			return executor.Result{Mp: sqlproc.Proc.Mp(), Batches: []*batch.Batch{makeTopKBatchFTWithDocs(sqlproc.Proc, 31, 32)}}, nil
		default:
			return executor.Result{}, moerr.NewInternalErrorf(sqlproc.Proc.Ctx, "unexpected SQL: %s", sql)
		}
	}
	ft_runSql_streaming = func(
		ctx context.Context,
		proc *process.Process,
		sql string,
		ch chan executor.Result,
		errChan chan error,
	) (executor.Result, error) {
		return executor.Result{}, moerr.NewInternalError(proc.Ctx, "streaming SQL should not be used in fast-path multi-row test")
	}

	err = st.start(tf, ut.proc, 0, nil)
	require.NoError(t, err)
	result, err := st.call(tf, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.Equal(t, int32(11), vector.GetFixedAtWithTypeCheck[int32](result.Batch.Vecs[0], 0))
	require.Equal(t, int32(12), vector.GetFixedAtWithTypeCheck[int32](result.Batch.Vecs[0], 1))
	result, err = st.call(tf, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)

	err = st.start(tf, ut.proc, 1, nil)
	require.NoError(t, err)
	result, err = st.call(tf, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.Equal(t, int32(31), vector.GetFixedAtWithTypeCheck[int32](result.Batch.Vecs[0], 0))
	require.Equal(t, int32(32), vector.GetFixedAtWithTypeCheck[int32](result.Batch.Vecs[0], 1))
	result, err = st.call(tf, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)

	requireStateFreeReturns(t, st, tf, ut.proc)
}

func TestFullTextStartResetsStateForLaterRowsStreaming(t *testing.T) {
	ut := newFTTestCase(t, mpool.MustNewZero(), ftdefaultAttrs, fulltext.ALGO_TFIDF, uint64(0))
	err := ut.arg.Prepare(ut.proc)
	require.NoError(t, err)

	tf := ut.arg
	tf.ctr.argVecs = makeMultiRowArgVecsFT(ut.proc,
		fulltextInputRow{source: "src0", index: "idx0", pattern: "Matrix", mode: int64(tree.FULLTEXT_NL)},
		fulltextInputRow{source: "src1", index: "idx1", pattern: "Apple", mode: int64(tree.FULLTEXT_NL)},
	)
	st := tf.ctr.state.(*fulltextState)

	prevRunSQL := ft_runSql
	prevRunStreaming := ft_runSql_streaming
	defer func() {
		ft_runSql = prevRunSQL
		ft_runSql_streaming = prevRunStreaming
	}()

	var gotSQL []string
	ft_runSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		gotSQL = append(gotSQL, sql)
		switch {
		case strings.Contains(sql, "COUNT(*) from idx0 where word = '__DocLen'"),
			strings.Contains(sql, "COUNT(*) from idx1 where word = '__DocLen'"):
			return executor.Result{Mp: sqlproc.Proc.Mp(), Batches: []*batch.Batch{makeCountOnlyBatchFT(sqlproc.Proc)}}, nil
		default:
			return executor.Result{}, moerr.NewInternalErrorf(sqlproc.Proc.Ctx, "unexpected SQL: %s", sql)
		}
	}
	ft_runSql_streaming = func(
		ctx context.Context,
		proc *process.Process,
		sql string,
		ch chan executor.Result,
		errChan chan error,
	) (executor.Result, error) {
		gotSQL = append(gotSQL, sql)
		ch <- executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeSmallTextBatchFT(proc)}}
		return executor.Result{}, nil
	}

	err = st.start(tf, ut.proc, 0, nil)
	require.NoError(t, err)
	result, err := st.call(tf, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	result, err = st.call(tf, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)

	err = st.start(tf, ut.proc, 1, nil)
	require.NoError(t, err)
	result, err = st.call(tf, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	result, err = st.call(tf, ut.proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)

	require.Len(t, gotSQL, 4)
	require.Contains(t, gotSQL[0], "idx0")
	require.Contains(t, gotSQL[1], "idx0")
	require.Contains(t, gotSQL[2], "idx1")
	require.Contains(t, gotSQL[3], "idx1")

	requireStateFreeReturns(t, st, tf, ut.proc)
}

// create const input exprs
func makeConstInputExprsFT() []*plan.Expr {
	return makeConstInputExprsFTWithPattern("pattern", 0)
}

func makeConstInputExprsFTWithPattern(pattern string, mode int64) []*plan.Expr {

	//ret := make([]*plan.Expr, 4)
	ret := []*plan.Expr{{
		Typ: plan.Type{
			Id:    int32(types.T_varchar),
			Width: 256,
		},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Sval{
					Sval: "src_table",
				},
			},
		},
	}, {
		Typ: plan.Type{
			Id:    int32(types.T_varchar),
			Width: 256,
		},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Sval{
					Sval: "index_table",
				},
			},
		},
	}, {
		Typ: plan.Type{
			Id:    int32(types.T_varchar),
			Width: 256,
		},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Sval{
					Sval: pattern,
				},
			},
		},
	}, {
		Typ: plan.Type{
			Id: int32(types.T_int64),
		},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_I64Val{
					I64Val: mode,
				},
			},
		},
	}}

	return ret
}

type fulltextInputRow struct {
	source  string
	index   string
	pattern string
	mode    int64
}

func makeMultiRowArgVecsFT(proc *process.Process, rows ...fulltextInputRow) []*vector.Vector {
	srcVec := vector.NewVec(types.New(types.T_varchar, 256, 0))
	idxVec := vector.NewVec(types.New(types.T_varchar, 256, 0))
	patternVec := vector.NewVec(types.New(types.T_varchar, 256, 0))
	modeVec := vector.NewVec(types.New(types.T_int64, 8, 0))

	for _, row := range rows {
		vector.AppendBytes(srcVec, []byte(row.source), false, proc.Mp())
		vector.AppendBytes(idxVec, []byte(row.index), false, proc.Mp())
		vector.AppendBytes(patternVec, []byte(row.pattern), false, proc.Mp())
		vector.AppendFixed[int64](modeVec, row.mode, false, proc.Mp())
	}

	return []*vector.Vector{srcVec, idxVec, patternVec, modeVec}
}

// create input vector for arg (src_table, index_table, pattern, mode)
func makeBatchFT(proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(4)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 256, 0))
	bat.Vecs[1] = vector.NewVec(types.New(types.T_varchar, 256, 0))
	bat.Vecs[2] = vector.NewVec(types.New(types.T_varchar, 256, 0))
	bat.Vecs[3] = vector.NewVec(types.New(types.T_int32, 4, 0))

	vector.AppendBytes(bat.Vecs[0], []byte("src_table"), false, proc.Mp())
	vector.AppendBytes(bat.Vecs[1], []byte("idx_table"), false, proc.Mp())
	vector.AppendBytes(bat.Vecs[2], []byte("pattern"), false, proc.Mp())
	vector.AppendFixed[int32](bat.Vecs[3], int32(0), false, proc.Mp())

	bat.SetRowCount(1)
	return bat
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

func makeTopKBatchFT(proc *process.Process) *batch.Batch {
	return makeTopKBatchFTWithDocs(proc, 11, 12)
}

func makeTopKBatchFTWithDocs(proc *process.Process, doc1, doc2 int32) *batch.Batch {
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_int32, 4, 0))
	bat.Vecs[1] = vector.NewVec(types.New(types.T_int64, 8, 0))
	bat.Vecs[2] = vector.NewVec(types.New(types.T_int64, 8, 0))

	vector.AppendFixed[int32](bat.Vecs[0], doc1, false, proc.Mp())
	vector.AppendFixed[int64](bat.Vecs[1], int64(5), false, proc.Mp())
	vector.AppendFixed[int64](bat.Vecs[2], int64(3), false, proc.Mp())
	vector.AppendFixed[int32](bat.Vecs[0], doc2, false, proc.Mp())
	vector.AppendFixed[int64](bat.Vecs[1], int64(3), false, proc.Mp())
	vector.AppendFixed[int64](bat.Vecs[2], int64(3), false, proc.Mp())

	bat.SetRowCount(2)
	return bat
}

func makeTopKBM25BatchFT(proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_int32, 4, 0))
	bat.Vecs[1] = vector.NewVec(types.New(types.T_float64, 8, 0))
	bat.Vecs[2] = vector.NewVec(types.New(types.T_int64, 8, 0))

	vector.AppendFixed[int32](bat.Vecs[0], int32(21), false, proc.Mp())
	vector.AppendFixed[float64](bat.Vecs[1], float64(1.185774326891547), false, proc.Mp())
	vector.AppendFixed[int64](bat.Vecs[2], int64(3), false, proc.Mp())
	vector.AppendFixed[int32](bat.Vecs[0], int32(22), false, proc.Mp())
	vector.AppendFixed[float64](bat.Vecs[1], float64(0.6467859964862983), false, proc.Mp())
	vector.AppendFixed[int64](bat.Vecs[2], int64(3), false, proc.Mp())

	bat.SetRowCount(2)
	return bat
}

func makeEmptyTopKBatchFT(proc *process.Process) *batch.Batch {
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_int32, 4, 0))
	bat.Vecs[1] = vector.NewVec(types.New(types.T_int64, 8, 0))
	bat.Vecs[2] = vector.NewVec(types.New(types.T_int64, 8, 0))
	bat.SetRowCount(0)
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

func requireStateFreeReturns(t *testing.T, st tvfState, tf *TableFunction, proc *process.Process) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		st.free(tf, proc, false, nil)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("state.free did not return")
	}
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
