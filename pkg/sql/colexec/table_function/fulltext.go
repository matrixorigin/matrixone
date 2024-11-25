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
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
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
	score_array []map[any]float32
	n_doc_id    int
	last_doc_id any
	n_result    uint64
	sql_closed  bool
	sacc        *fulltext.SearchAccum
	limit       uint64
	nrows       int

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

func (u *fulltextState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {

	var scoremap map[any]float32
	u.batch.CleanOnlyData()

	// number of result more than pushdown limit and exit
	if u.limit > 0 && u.n_result >= u.limit {
		return vm.CancelResult, nil
	}

	// return scoremap when array is not empty
	if len(u.score_array) > 0 {
		scoremap, u.score_array = u.score_array[0], u.score_array[1:]
		return u.returnResult(proc, scoremap)
	}

	// array is empty, try to get batch from SQL executor
	for !u.sql_closed {
		sql_closed, err := getResults(u, proc, u.sacc)
		if err != nil {
			return vm.CancelResult, err
		}
		u.sql_closed = sql_closed

		if len(u.score_array) > 0 {
			scoremap, u.score_array = u.score_array[0], u.score_array[1:]
			return u.returnResult(proc, scoremap)
		}
	}
	return vm.CancelResult, nil
}

// start calling tvf on nthRow and put the result in u.batch.  Note that current unnest impl will
// always return one batch per nthRow.
func (u *fulltextState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) error {

	if !u.inited {
		u.batch = tf.createResultBatch()
		u.errors = make(chan error)
		u.stream_chan = make(chan executor.Result, 8)
		u.score_array = make([]map[any]float32, 0, 512)
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
func ft_runSql_streaming_fn(proc *process.Process, sql string, stream_chan chan executor.Result) (executor.Result, error) {
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
		WithStreaming(stream_chan)
	return exec.Exec(proc.GetTopContext(), sql, opts)
}

// run SQL to get the (doc_id, pos) of all patterns (words) in the search string
func runWordStats(u *fulltextState, proc *process.Process, s *fulltext.SearchAccum) (executor.Result, error) {
	var union []string

	var keywords []string

	for _, p := range s.Pattern {
		ssNoOp := p.GetLeafText(fulltext.TEXT)
		for _, w := range ssNoOp {
			keywords = append(keywords, "'"+w+"'")
		}
	}

	if len(keywords) > 0 {
		union = append(union, fmt.Sprintf("SELECT doc_id, pos, word FROM %s WHERE word IN (%s)",
			s.TblName, strings.Join(keywords, ",")))
	}

	sqlfmt := "SELECT doc_id, pos, '%s' FROM %s WHERE prefix_eq(word, '%s')"
	for _, p := range s.Pattern {
		ssStar := p.GetLeafText(fulltext.STAR)
		for _, w := range ssStar {
			// remove the last character which should be '*' for prefix search
			slen := len(w)
			if w[slen-1] != '*' {
				return executor.Result{}, moerr.NewInternalError(proc.Ctx, "wildcard search without character *")
			}
			// prefix search
			prefix := w[0 : slen-1]
			union = append(union, fmt.Sprintf(sqlfmt, w, s.TblName, prefix))
		}
	}

	sql := strings.Join(union, " UNION ")

	if len(union) == 1 {
		sql += " ORDER BY doc_id"
	} else {
		sql = fmt.Sprintf("SELECT * FROM (%s) ORDER BY doc_id", sql)
	}
	//logutil.Infof("SQL is %s", sql)

	res, err := ft_runSql_streaming(proc, sql, u.stream_chan)
	if err != nil {
		return executor.Result{}, err
	}

	return res, nil
}

// get one batch and evaluate the result in mini batches with size 8192
func getResults(u *fulltextState, proc *process.Process, s *fulltext.SearchAccum) (stream_closed bool, err error) {

	// first receive the batch and calculate the scoremap
	// We don't need to calculate mini-batch?????
	var res executor.Result
	var ok bool
	select {
	case res, ok = <-u.stream_chan:
		if !ok {
			// channel closed and evaluate the rest of result
			scoremap, err := s.Eval()
			if err != nil {
				return false, err
			}
			if len(scoremap) > 0 {
				u.score_array = append(u.score_array, scoremap)
			}

			return true, nil
		}
	case err = <-u.errors:
		return false, err
	case <-proc.Ctx.Done():
		return false, moerr.NewInternalError(proc.Ctx, "context cancelled")
	}

	bat := res.Batches[0]
	defer res.Close()

	if len(bat.Vecs) != 3 {
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

		// pos int32
		pos := vector.GetFixedAtWithTypeCheck[int32](bat.Vecs[1], i)

		// word string
		word := bat.Vecs[2].GetStringAt(i)

		if u.last_doc_id == nil {
			u.last_doc_id = doc_id
		} else if u.last_doc_id != doc_id {
			// new doc_id
			u.last_doc_id = doc_id
			u.n_doc_id++
		}

		//logutil.Infof("WORD:%s, DOC_ID:%v, POS:%d, LAST: %v", word, doc_id, pos, last_doc_id)
		if u.n_doc_id >= 8192 {
			scoremap, err := s.Eval()
			if err != nil {
				return false, err
			}

			if len(scoremap) > 0 {
				u.score_array = append(u.score_array, scoremap)
			}

			clear(s.WordAccums)
			u.n_doc_id = 0
		}

		w, ok := s.WordAccums[word]
		if !ok {
			s.WordAccums[word] = fulltext.NewWordAccum()
			w = s.WordAccums[word]
		}
		_, ok = w.Words[doc_id]
		if ok {
			w.Words[doc_id].Position = append(w.Words[doc_id].Position, pos)
			w.Words[doc_id].DocCount += 1
		} else {
			positions := make([]int32, 0, 128)
			positions = append(positions, pos)
			w.Words[doc_id] = &fulltext.Word{DocId: doc_id, Position: positions, DocCount: 1}
		}
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
	mode int64, bat *batch.Batch) error {

	// parse the search string to []Pattern and create SearchAccum
	s, err := fulltext.NewSearchAccum(srctbl, tblname, pattern, mode, "")
	if err != nil {
		return err
	}

	// count(*) to get number of records in source table
	nrow, err := runCountStar(proc, s)
	if err != nil {
		return err
	}
	s.Nrow = nrow

	u.sacc = s

	go func() {
		// get the statistic of search string ([]Pattern) and store in SearchAccum
		_, err = runWordStats(u, proc, u.sacc)
		if err != nil {
			u.errors <- err
			return
		}
	}()

	return nil
}
