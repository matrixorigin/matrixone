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

	"github.com/matrixorigin/matrixone/pkg/common/fulltext"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	countstar_sql = "SELECT COUNT(*) FROM %s"
)

type fulltextState struct {
	simpleOneBatchState
}

// start calling tvf on nthRow and put the result in u.batch.  Note that current unnest impl will
// always return one batch per nthRow.
func (u *fulltextState) start(tf *TableFunction, proc *process.Process, nthRow int) error {
	u.startPreamble(tf, proc, nthRow)

	var err error

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

	err = fulltextIndexMatch(proc, tf, source_table, index_table, pattern, mode, u.batch)

	return err
}

// prepare
func fulltextIndexScanPrepare(proc *process.Process, tableFunction *TableFunction) (tvfState, error) {
	var err error
	st := &fulltextState{}
	tableFunction.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tableFunction.Args)
	tableFunction.ctr.argVecs = make([]*vector.Vector, len(tableFunction.Args))

	for i := range tableFunction.Attrs {
		tableFunction.Attrs[i] = strings.ToUpper(tableFunction.Attrs[i])
	}
	return st, err
}

func ft_runSql(proc *process.Process, sql string) (executor.Result, error) {
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

// run SQL to get the (doc_id, pos) of all patterns (words) in the search string
func runWordStats(proc *process.Process, s *fulltext.SearchAccum) error {
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
				return moerr.NewInternalError(proc.Ctx, "wildcard search without character *")
			}
			// prefix search
			prefix := w[0 : slen-1]
			union = append(union, fmt.Sprintf(sqlfmt, w, s.TblName, prefix))
		}
	}

	sql := strings.Join(union, " UNION ")
	//logutil.Infof("SQL is %s", sql)

	res, err := ft_runSql(proc, sql)
	if err != nil {
		return err
	}
	defer res.Close()

	for _, bat := range res.Batches {

		if len(bat.Vecs) != 3 {
			return moerr.NewInternalError(proc.Ctx, "output vector columns not match")
		}

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
			//logutil.Infof("ID:%d, DOC_ID:%v, POS:%d, DOC_COUNT:%d, FIRST: %v, LAST: %v", id, doc_id, pos, doc_count, first_doc_id, last_doc_id)

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
				w.Words[doc_id] = &fulltext.Word{DocId: doc_id, Position: []int32{pos}, DocCount: 1}
			}
		}

	}

	// we got all results for all words required.  Evaluate the Pattern against the WordAccums to get answer.

	return nil
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

func fulltextIndexMatch(proc *process.Process, tableFunction *TableFunction, srctbl, tblname, pattern string, mode int64, bat *batch.Batch) (err error) {

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

	// get the statistic of search string ([]Pattern) and store in SearchAccum
	err = runWordStats(proc, s)
	if err != nil {
		return err
	}

	// compute the ranking
	scoremap, err := s.Eval()
	if err != nil {
		return err
	}

	// return result
	if bat.VectorCount() == 1 {
		// only doc_id returned

		// write the batch
		for key := range scoremap {
			doc_id := key
			if str, ok := doc_id.(string); ok {
				bytes := []byte(str)
				doc_id = bytes
			}
			// type of id follow primary key column
			vector.AppendAny(bat.Vecs[0], doc_id, false, proc.Mp())
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
			vector.AppendAny(bat.Vecs[0], doc_id, false, proc.Mp())

			// score
			vector.AppendFixed[float32](bat.Vecs[1], scoremap[key], false, proc.Mp())
		}
	}

	bat.SetRowCount(len(scoremap))
	return nil
}
