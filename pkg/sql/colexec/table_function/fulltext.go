// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"container/heap"
	"context"
	"fmt"
	"sync"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	countstar_sql = "SELECT COUNT(*), AVG(pos) from %s where word = '%s'"
)

var ft_runSql = sqlexec.RunSql
var ft_runSql_streaming = sqlexec.RunStreamingSql

type fulltextState struct {
	inited    bool
	errCh     chan error
	streamCh  chan executor.Result
	n_result  uint64
	sacc      *fulltext.SearchAccum
	limit     uint64
	nrows     int
	idx2word  map[int]string
	agghtab   map[any]uint64
	aggcnt    []int64
	mpool     *fulltext.FixedBytePool
	param     fulltext.FullTextParserParam
	docLenMap map[any]int32
	minheap   vectorindex.SearchResultHeap

	// holding output batch
	batch *batch.Batch
}

func (u *fulltextState) end(tf *TableFunction, proc *process.Process) error {
	return nil
}

func (u *fulltextState) reset(tf *TableFunction, proc *process.Process) {
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
}

func (u *fulltextState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if u.batch != nil {
		u.batch.Clean(proc.Mp())
	}

	if u.mpool != nil {
		u.mpool.Close()
	}

	for {
		select {
		case res, ok := <-u.streamCh:
			if !ok {
				return
			}
			res.Close()
		case <-proc.Ctx.Done():
			return
		}
	}
}

// return (doc_id, score) as result
// when scoremap is empty, return result end.
func (u *fulltextState) returnResult(proc *process.Process, scoremap map[any]float32) (vm.CallResult, error) {
	// return result
	if u.batch.VectorCount() == 1 {
		// only doc_id returned

		// write the batch
		for key := range scoremap {
			doc_id := key
			if str, ok := doc_id.(string); ok {
				bytes := []byte(str)
				doc_id = bytes
			}
			// type of id follow primary key column
			vector.AppendAny(u.batch.Vecs[0], doc_id, false, proc.Mp())
		}
	} else {
		// doc_id and score returned
		for key := range scoremap {
			doc_id := key
			if str, ok := doc_id.(string); ok {
				bytes := []byte(str)
				doc_id = bytes
			}
			// type of id follow primary key column
			vector.AppendAny(u.batch.Vecs[0], doc_id, false, proc.Mp())

			// score
			vector.AppendFixed[float32](u.batch.Vecs[1], scoremap[key], false, proc.Mp())
		}
	}

	u.batch.SetRowCount(len(scoremap))
	u.n_result += uint64(len(scoremap))

	if u.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}

	return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil

}

// return (doc_id, score) as result
// when scoremap is empty, return result end.
func (u *fulltextState) returnResultFromHeap(proc *process.Process, hp *vectorindex.SearchResultHeap) (vm.CallResult, error) {

	blocksz := 8192
	n := hp.Len()
	if n > blocksz {
		n = blocksz
	}

	for range n {
		srif := heap.Pop(hp)
		if srif == nil {
			// skip unexpected nil entries
			logutil.Warnf("fulltext: heap.Pop returned nil; remaining=%d", hp.Len())
			continue
		}
		// handle possible interface boxing: srif may be a concrete *SearchResult...,
		// or a vectorindex.SearchResultIf wrapped in any.
		switch sr := srif.(type) {
		case *vectorindex.SearchResultAnyKey:
			// doc_id returned
			doc_id := sr.Id
			if str, ok := sr.Id.(string); ok {
				bytes := []byte(str)
				doc_id = bytes
			}
			vector.AppendAny(u.batch.Vecs[0], doc_id, false, proc.Mp())
			if u.batch.VectorCount() > 1 {
				vector.AppendFixed[float32](u.batch.Vecs[1], float32(sr.GetDistance()), false, proc.Mp())
			}
		case *vectorindex.SearchResult:
			// accept int64 key as well
			vector.AppendAny(u.batch.Vecs[0], any(sr.Id), false, proc.Mp())
			if u.batch.VectorCount() > 1 {
				vector.AppendFixed[float32](u.batch.Vecs[1], float32(sr.GetDistance()), false, proc.Mp())
			}
		default:
			// try unbox once if it's SearchResultIf inside interface{}
			if sri, ok := srif.(vectorindex.SearchResultIf); ok {
				switch sr2 := sri.(type) {
				case *vectorindex.SearchResultAnyKey:
					doc_id := sr2.Id
					if str, ok := sr2.Id.(string); ok {
						bytes := []byte(str)
						doc_id = bytes
					}
					vector.AppendAny(u.batch.Vecs[0], doc_id, false, proc.Mp())
					if u.batch.VectorCount() > 1 {
						vector.AppendFixed[float32](u.batch.Vecs[1], float32(sr2.GetDistance()), false, proc.Mp())
					}
				case *vectorindex.SearchResult:
					vector.AppendAny(u.batch.Vecs[0], any(sr2.Id), false, proc.Mp())
					if u.batch.VectorCount() > 1 {
						vector.AppendFixed[float32](u.batch.Vecs[1], float32(sr2.GetDistance()), false, proc.Mp())
					}
				default:
					return vm.CancelResult, moerr.NewInternalError(proc.Ctx, fmt.Sprintf("heap return key has unexpected type: %T", srif))
				}
			} else {
				return vm.CancelResult, moerr.NewInternalError(proc.Ctx, fmt.Sprintf("heap return key is not SearchResultAnyKey (got %T)", srif))
			}
		}
	}
	u.batch.SetRowCount(n)
	u.n_result += uint64(n)
	if u.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}

	return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil

}

func (u *fulltextState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {

	var err error
	u.batch.CleanOnlyData()

	// number of result more than pushdown limit and exit
	if u.limit > 0 && u.n_result >= u.limit {
		return vm.CancelResult, nil
	}

	if u.limit == 0 {
		// array is empty, try to get batch from SQL executor
		scoremap, err := evaluate(u, proc, u.sacc)
		if err != nil {
			return vm.CancelResult, err
		}

		if scoremap != nil {
			return u.returnResult(proc, scoremap)
		}
		return vm.CancelResult, nil

	} else {
		// build minheap
		hp := u.minheap
		if hp == nil {
			hp, err = sort_topk(u, proc, u.sacc, u.limit)
			if err != nil {
				return vm.CancelResult, err
			}
		}

		if hp != nil {
			return u.returnResultFromHeap(proc, &hp)
		}
		return vm.CancelResult, nil
	}
}

// start calling tvf on nthRow and put the result in u.batch.  Note that current unnest impl will
// always return one batch per nthRow.
func (u *fulltextState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) error {

	if !u.inited {
		if len(tf.Params) > 0 {
			err := sonic.Unmarshal([]byte(tf.Params), &u.param)
			if err != nil {
				return err
			}
		}
		u.batch = tf.createResultBatch()
		u.errCh = make(chan error)
		u.streamCh = make(chan executor.Result, 8)
		u.idx2word = make(map[int]string)
		u.inited = true
		u.docLenMap = make(map[any]int32)
	}

	v := tf.ctr.argVecs[0]
	if v.GetType().Oid != types.T_varchar {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("First argument (source table name) must be string, but got %s", v.GetType().String()))
	}
	source_table := v.GetStringAt(0)

	v = tf.ctr.argVecs[1]
	if v.GetType().Oid != types.T_varchar {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("Second argument (index table name) must be string, but got %s", v.GetType().String()))
	}
	index_table := v.GetStringAt(0)

	v = tf.ctr.argVecs[2]
	if v.GetType().Oid != types.T_varchar {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("Third argument (pattern) must be string, but got %s", v.GetType().String()))
	}
	pattern := v.GetStringAt(0)

	v = tf.ctr.argVecs[3]
	if v.GetType().Oid != types.T_int64 {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("Fourth argument (mode) must be int64, but got %s", v.GetType().String()))
	}
	mode := vector.GetFixedAtNoTypeCheck[int64](v, 0)

	scoreAlgo, err := fulltext.GetScoreAlgo(proc)
	if err != nil {
		return err
	}

	return fulltextIndexMatch(u, proc, tf, source_table, index_table, pattern, mode, scoreAlgo, u.batch)
}

// prepare
func fulltextIndexScanPrepare(proc *process.Process, tableFunction *TableFunction) (tvfState, error) {
	var err error
	st := &fulltextState{}
	tableFunction.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tableFunction.Args)
	tableFunction.ctr.argVecs = make([]*vector.Vector, len(tableFunction.Args))

	if tableFunction.Limit != nil {
		if cExpr, ok := tableFunction.Limit.Expr.(*plan.Expr_Lit); ok {
			if c, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
				st.limit = c.U64Val
			}
		}
	}
	return st, err
}

// run SQL to get the (doc_id, word_index) of all patterns (words) in the search string
func runWordStats(
	ctx context.Context,
	u *fulltextState,
	proc *process.Process,
	s *fulltext.SearchAccum,
) (result executor.Result, err error) {

	var sql string
	if sql, err = fulltext.PatternToSql(
		s.Pattern, s.Mode, s.TblName, u.param.Parser, s.ScoreAlgo,
	); err != nil {
		return
	}

	result, err = ft_runSql_streaming(ctx, proc, sql, u.streamCh, u.errCh)

	return
}

// evaluate the score for all document vectors in Agg hashtable.
// whenever there is 8192 results, return it immediately.
func evaluate(u *fulltextState, proc *process.Process, s *fulltext.SearchAccum) (scoremap map[any]float32, err error) {

	scoremap = make(map[any]float32, 8192)
	keys := make([]any, 0, 8192)

	aggcnt := u.aggcnt

	for doc_id, addr := range u.agghtab {
		docvec, err := u.mpool.GetItem(addr)
		if err != nil {
			return nil, err
		}

		docLen := int64(0)
		if len, ok := u.docLenMap[doc_id]; ok {
			docLen = int64(len)
		}

		score, err := s.Eval(docvec, docLen, aggcnt)
		if err != nil {
			return nil, err
		}

		keys = append(keys, doc_id)

		if len(score) > 0 {
			scoremap[doc_id] = score[0]
		}

		if len(scoremap) >= 8192 {
			break
		}
	}

	for _, k := range keys {
		u.mpool.FreeItem(u.agghtab[k])
		delete(u.agghtab, k)
	}

	return scoremap, nil
}

func sort_topk(u *fulltextState, proc *process.Process, s *fulltext.SearchAccum, limit uint64) (minheap vectorindex.SearchResultHeap, err error) {
	aggcnt := u.aggcnt
	u.minheap = make(vectorindex.SearchResultHeap, 0, limit)
	// initialize container/heap structure before pushes
	heap.Init(&u.minheap)

	for doc_id, addr := range u.agghtab {

		docvec, err := u.mpool.GetItem(addr)
		if err != nil {
			return nil, err
		}

		docLen := int64(0)
		if len, ok := u.docLenMap[doc_id]; ok {
			docLen = int64(len)
		}

		score, err := s.Eval(docvec, docLen, aggcnt)
		if err != nil {
			return nil, err
		}

		if len(score) > 0 {
			scoref64 := float64(score[0])
			if len(u.minheap) >= int(limit) {
				if u.minheap[0].GetDistance() < scoref64 {
					u.minheap[0] = &vectorindex.SearchResultAnyKey{Id: doc_id, Distance: scoref64}
					heap.Fix(&u.minheap, 0)
				}
			} else {
				heap.Push(&u.minheap, &vectorindex.SearchResultAnyKey{Id: doc_id, Distance: scoref64})
			}
		}
	}

	// close the pool
	u.mpool.Close()
	clear(u.agghtab)

	return u.minheap, nil
}

// result from SQL is (doc_id, index constant (refer to Pattern.Index))
// Two group by happens here
// 1. Group by the result into []uint8 which is DocCount[Pattern.Index].
// 2. Aggregate the total number of documents contain the word index (Pattern.Index). AggCnt[Pattern.Index].
func groupby(u *fulltextState, proc *process.Process, s *fulltext.SearchAccum) (stream_closed bool, err error) {

	// first receive the batch and calculate the scoremap
	// We don't need to calculate mini-batch?????
	var res executor.Result
	var ok bool

	select {
	case res, ok = <-u.streamCh:
		if !ok {
			// channel closed and evaluate the rest of result
			return true, nil
		}
	case err = <-u.errCh:
		return false, err
	case <-proc.Ctx.Done():
		return false, moerr.NewInternalError(proc.Ctx, "context cancelled")
	}

	bat := res.Batches[0]
	defer res.Close()

	if len(bat.Vecs) > 3 {
		return false, moerr.NewInternalError(proc.Ctx, "output vector columns not match")
	}
	needSetDocLen := len(bat.Vecs) == 3

	u.nrows += bat.RowCount()

	for i := 0; i < bat.RowCount(); i++ {
		// doc_id any
		doc_id := vector.GetAny(bat.Vecs[0], i, false)

		bytes, ok := doc_id.([]byte)
		if ok {
			// change it to string
			key := string(bytes)
			doc_id = key
		}

		if needSetDocLen {
			docLen := vector.GetFixedAtWithTypeCheck[int32](bat.Vecs[2], i)
			u.docLenMap[doc_id] = docLen
		}

		// word string
		widx := vector.GetFixedAtWithTypeCheck[int32](bat.Vecs[1], i)

		var docvec []uint8
		if s.Mode == int64(tree.FULLTEXT_NL) || s.Pattern[0].Operator == fulltext.PHRASE {
			// phrase search widx is dummy and fill in value 1 for all keywords
			nwords := s.Nkeywords
			addr, ok := u.agghtab[doc_id]
			if ok {
				docvec, err = u.mpool.GetItem(addr)
				if err != nil {
					return false, err
				}
				for i := 0; i < nwords; i++ {
					if docvec[i] < 255 {
						docvec[i]++
					}
				}
			} else {
				//docvec = make([]uint8, s.Nkeywords)
				addr, docvec, err = u.mpool.NewItem()
				if err != nil {
					return false, err
				}

				for i := 0; i < nwords; i++ {
					docvec[i] = 1
				}
				u.agghtab[doc_id] = addr
			}

			// update only once per doc_id
			for i := 0; i < nwords; i++ {
				if docvec[i] == 1 {
					u.aggcnt[i]++
				}
			}
		} else {

			addr, ok := u.agghtab[doc_id]
			if ok {
				docvec, err = u.mpool.GetItem(addr)
				if err != nil {
					return false, err
				}
				if docvec[widx] < 255 {
					// limit doc count to 255 to fit uint8
					docvec[widx]++
				}
			} else {
				//docvec = make([]uint8, s.Nkeywords)
				addr, docvec, err = u.mpool.NewItem()
				if err != nil {
					return false, err
				}
				docvec[widx] = 1
				u.agghtab[doc_id] = addr
			}

			// update only once per doc_id
			if docvec[widx] == 1 {
				u.aggcnt[widx]++
			}

		}
		//logutil.Infof("ROW widx=%d, docid = %v", widx, doc_id)

	}

	return false, nil
}

// Run SQL to get number of records in source table
func runCountStar(proc *process.Process, s *fulltext.SearchAccum) (executor.Result, error) {
	sql := fmt.Sprintf(countstar_sql, s.TblName, fulltext.DOC_LEN_WORD)

	res, err := ft_runSql(proc, sql)
	if err != nil {
		return executor.Result{}, err
	}
	defer res.Close()

	if len(res.Batches) == 0 {
		return res, nil
	}

	bat := res.Batches[0]
	if bat.RowCount() == 1 {
		nrow := vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[0], 0)
		s.Nrow = nrow

		avgDocLen := vector.GetFixedAtWithTypeCheck[float64](bat.Vecs[1], 0)
		s.AvgDocLen = avgDocLen
		//logutil.Infof("NROW = %d", nrow)
	}
	// downgrade BM25 to TF-IDF if AvgDocLen is zro
	if s.ScoreAlgo == fulltext.ALGO_BM25 && s.AvgDocLen == 0 {
		s.ScoreAlgo = fulltext.ALGO_TFIDF
	}

	return res, nil
}

func fulltextIndexMatch(
	u *fulltextState,
	proc *process.Process,
	tableFunction *TableFunction,
	srctbl, tblname, pattern string,
	mode int64,
	scoreAlgo fulltext.FullTextScoreAlgo,
	bat *batch.Batch,
) (err error) {

	opStats := tableFunction.OpAnalyzer.GetOpStats()

	if u.sacc == nil {
		// parse the search string to []Pattern and create SearchAccum
		s, err := fulltext.NewSearchAccum(srctbl, tblname, pattern, mode, "", scoreAlgo)
		if err != nil {
			return err
		}

		u.mpool = fulltext.NewFixedBytePool(proc, uint64(s.Nkeywords), 0, 0)
		u.agghtab = make(map[any]uint64, 1024)
		u.aggcnt = make([]int64, s.Nkeywords)

		// count(*) to get number of records in source table
		res, err := runCountStar(proc, s)
		if err != nil {
			return err
		}

		u.sacc = s

		opStats.BackgroundQueries = append(opStats.BackgroundQueries, res.LogicalPlan)
	}

	//t1 := time.Now()

	// we should wait the goroutine exit completely here,
	// even the SQL stream is done inside the `runWordStats`.
	// or will be resulting in data race on the tableFunction.
	var (
		waiter      sync.WaitGroup
		ctx, cancel = context.WithCancelCause(proc.GetTopContext())
	)
	defer cancel(nil)

	waiter.Add(1)
	go func() {
		defer waiter.Done()

		// get the statistic of search string ([]Pattern) and store in SearchAccum
		res, err2 := runWordStats(ctx, u, proc, u.sacc)
		if err2 != nil {
			u.errCh <- err2
			return
		}
		opStats.BackgroundQueries = append(opStats.BackgroundQueries, res.LogicalPlan)
	}()

	// get batch from SQL executor
	sql_closed := false
	for !sql_closed {
		if sql_closed, err = groupby(u, proc, u.sacc); err != nil {
			// notify the producer to stop the sql streaming
			cancel(err)
			break
		}
	}

	// wait for the sql streaming to be closed. make sure all the remaining
	// results in stream_chan are closed.
	if !sql_closed {
		for res := range u.streamCh {
			res.Close()
		}
	}

	waiter.Wait()

	if err == nil {
		// fetch potential remaining errors from error_chan
		select {
		case err = <-u.errCh:
		default:
		}
	}

	/*
		t2 := time.Now()
		diff := t2.Sub(t1)
		os.Stderr.WriteString(fmt.Sprintf("FULLTEXT: diff %v\n", diff))
		os.Stderr.WriteString(u.mpool.String())
	*/
	return
}
