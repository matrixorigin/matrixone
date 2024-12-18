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
	"encoding/json"
	"fmt"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	countstar_sql = "SELECT COUNT(*) FROM %s"
)

type fulltextState struct {
	inited      bool
	errors      chan error
	stream_chan chan executor.Result
	n_result    uint64
	sacc        *fulltext.SearchAccum
	limit       uint64
	nrows       int
	idx2word    map[int]string
	agghtab     map[any]uint64
	aggcnt      []int64
	mpool       *fulltext.FixedBytePool
	param       fulltext.FullTextParserParam

	// holding output batch
	batch *batch.Batch
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
		case res, ok := <-u.stream_chan:
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
		//os.Stderr.WriteString("FULLTEXT: return result END\n")
		return vm.CancelResult, nil
	}

	//os.Stderr.WriteString(fmt.Sprintf("score MAP size = %d, %d rows returned\n", len(u.agghtab), len(scoremap)))
	return vm.CallResult{Status: vm.ExecNext, Batch: u.batch}, nil

}

func (u *fulltextState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {

	u.batch.CleanOnlyData()

	// number of result more than pushdown limit and exit
	if u.limit > 0 && u.n_result >= u.limit {
		return vm.CancelResult, nil
	}

	// array is empty, try to get batch from SQL executor
	scoremap, err := evaluate(u, proc, u.sacc)
	if err != nil {
		return vm.CancelResult, err
	}

	if scoremap != nil {
		return u.returnResult(proc, scoremap)
	}
	return vm.CancelResult, nil
}

// start calling tvf on nthRow and put the result in u.batch.  Note that current unnest impl will
// always return one batch per nthRow.
func (u *fulltextState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) error {

	if !u.inited {
		if len(tf.Params) > 0 {
			err := json.Unmarshal([]byte(tf.Params), &u.param)
			if err != nil {
				return err
			}
		}
		u.batch = tf.createResultBatch()
		u.errors = make(chan error)
		u.stream_chan = make(chan executor.Result, 8)
		u.idx2word = make(map[int]string)
		u.inited = true
	}

	v := tf.ctr.argVecs[0]
	if v.GetType().Oid != types.T_varchar {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("First argument (source table name) must be string, but got %s", v.GetType().String()))
	}
	source_table := v.UnsafeGetStringAt(0)

	v = tf.ctr.argVecs[1]
	if v.GetType().Oid != types.T_varchar {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("Second argument (index table name) must be string, but got %s", v.GetType().String()))
	}
	index_table := v.UnsafeGetStringAt(0)

	v = tf.ctr.argVecs[2]
	if v.GetType().Oid != types.T_varchar {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("Third argument (pattern) must be string, but got %s", v.GetType().String()))
	}
	pattern := v.UnsafeGetStringAt(0)

	v = tf.ctr.argVecs[3]
	if v.GetType().Oid != types.T_int64 {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("Fourth argument (mode) must be int64, but got %s", v.GetType().String()))
	}
	mode := vector.GetFixedAtNoTypeCheck[int64](v, 0)

	return fulltextIndexMatch(u, proc, tf, source_table, index_table, pattern, mode, u.batch)
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

var ft_runSql = ft_runSql_fn

// run SQL in batch mode. Result batches will stored in memory and return once all result batches received.
func ft_runSql_fn(proc *process.Process, sql string) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}
	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(proc.GetTxnOperator()).
		WithDatabase(proc.GetSessionInfo().Database).
		WithTimeZone(proc.GetSessionInfo().TimeZone).
		WithAccountID(proc.GetSessionInfo().AccountId)
	return exec.Exec(proc.GetTopContext(), sql, opts)
}

var ft_runSql_streaming = ft_runSql_streaming_fn

// run SQL in WithStreaming() and pass the channel to SQL executor
func ft_runSql_streaming_fn(proc *process.Process, sql string, stream_chan chan executor.Result, error_chan chan error) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(proc.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}
	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(proc.GetTxnOperator()).
		WithDatabase(proc.GetSessionInfo().Database).
		WithTimeZone(proc.GetSessionInfo().TimeZone).
		WithAccountID(proc.GetSessionInfo().AccountId).
		WithStreaming(stream_chan, error_chan)
	return exec.Exec(proc.GetTopContext(), sql, opts)
}

// run SQL to get the (doc_id, word_index) of all patterns (words) in the search string
func runWordStats(u *fulltextState, proc *process.Process, s *fulltext.SearchAccum) (executor.Result, error) {

	sql, err := fulltext.PatternToSql(s.Pattern, s.Mode, s.TblName, u.param.Parser)
	if err != nil {
		return executor.Result{}, err
	}

	//logutil.Infof("SQL is %s", sql)
	os.Stderr.WriteString(sql)
	res, err := ft_runSql_streaming(proc, sql, u.stream_chan, u.errors)
	if err != nil {
		return executor.Result{}, err
	}

	return res, nil
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

		score, err := s.Eval(docvec, aggcnt)
		if err != nil {
			return nil, err
		}

		keys = append(keys, doc_id)

		if len(score) > 0 {
			scoremap[doc_id] = score[0]
		}

		if len(scoremap) >= 8192 {
			//os.Stderr.WriteString(u.mpool.String())
			break
		}
	}

	for _, k := range keys {
		u.mpool.FreeItem(u.agghtab[k])
		delete(u.agghtab, k)
	}

	return scoremap, nil
}

func groupby(u *fulltextState, proc *process.Process, s *fulltext.SearchAccum) (stream_closed bool, err error) {

	// first receive the batch and calculate the scoremap
	// We don't need to calculate mini-batch?????
	var res executor.Result
	var ok bool

	select {
	case res, ok = <-u.stream_chan:
		if !ok {
			// channel closed and evaluate the rest of result
			return true, nil
		}
	case err = <-u.errors:
		return false, err
	case <-proc.Ctx.Done():
		return false, moerr.NewInternalError(proc.Ctx, "context cancelled")
	}

	bat := res.Batches[0]
	defer res.Close()

	if len(bat.Vecs) != 2 {
		return false, moerr.NewInternalError(proc.Ctx, "output vector columns not match")
	}

	u.nrows += bat.RowCount()

	for i := 0; i < bat.RowCount(); i++ {
		// doc_id any
		doc_id := vector.GetAny(bat.Vecs[0], i)

		bytes, ok := doc_id.([]byte)
		if ok {
			// change it to string
			key := string(bytes)
			doc_id = key
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
func runCountStar(proc *process.Process, s *fulltext.SearchAccum) (int64, error) {
	var nrow int64
	nrow = 0
	sql := fmt.Sprintf(countstar_sql, s.SrcTblName)

	res, err := ft_runSql(proc, sql)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	if len(res.Batches) == 0 {
		return 0, nil
	}

	bat := res.Batches[0]
	if bat.RowCount() == 1 {
		nrow = vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[0], 0)
		//logutil.Infof("NROW = %d", nrow)
	}

	return nrow, nil
}

func fulltextIndexMatch(u *fulltextState, proc *process.Process, tableFunction *TableFunction, srctbl, tblname, pattern string,
	mode int64, bat *batch.Batch) (err error) {

	if u.sacc == nil {
		// parse the search string to []Pattern and create SearchAccum
		s, err := fulltext.NewSearchAccum(srctbl, tblname, pattern, mode, "")
		if err != nil {
			return err
		}

		u.mpool = fulltext.NewFixedBytePool(proc, uint64(s.Nkeywords), 0, 0)
		u.agghtab = make(map[any]uint64, 1024)
		u.aggcnt = make([]int64, s.Nkeywords)

		// count(*) to get number of records in source table
		nrow, err := runCountStar(proc, s)
		if err != nil {
			return err
		}
		s.Nrow = nrow

		u.sacc = s

	}

	//t1 := time.Now()
	go func() {
		//os.Stderr.WriteString("GO SQL START\n")
		// get the statistic of search string ([]Pattern) and store in SearchAccum
		_, err := runWordStats(u, proc, u.sacc)
		if err != nil {
			u.errors <- err
			return
		}
	}()

	// array is empty, try to get batch from SQL executor
	//i := 0
	sql_closed := false
	for !sql_closed {
		sql_closed, err = groupby(u, proc, u.sacc)
		if err != nil {
			return err
		}

		/*
			if (i % 1000) == 0 {
				os.Stderr.WriteString(fmt.Sprintf("nrow processed %d, chan size =%d\n", u.nrows, len(u.stream_chan)))
			}
			i++
		*/

	}

	/*
		t2 := time.Now()
		diff := t2.Sub(t1)

		os.Stderr.WriteString(fmt.Sprintf("FULLTEXT: diff %v\n", diff))
		os.Stderr.WriteString(u.mpool.String())
	*/
	return nil
}
