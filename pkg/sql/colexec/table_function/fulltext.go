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
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	default_mode_sql = "SELECT doc_id, pos, doc_count, first_doc_id, last_doc_id, word FROM %s WHERE"
	//default_mode_sql = "SELECT doc_id, pos, doc_count, first_doc_id, last_doc_id FROM %s WHERE word = '%s'"

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
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("fulltext_index_scan: first argument (index table name) must be string, but got %s", v.GetType().String()))
	}
	index_table := v.UnsafeGetStringAt(0)

	v = tf.ctr.argVecs[1]
	if v.GetType().Oid != types.T_varchar {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("fulltext_index_scan: second argument (pattern) must be string, but got %s", v.GetType().String()))
	}
	pattern := v.UnsafeGetStringAt(0)

	v = tf.ctr.argVecs[2]
	if v.GetType().Oid != types.T_int64 {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("fulltext_index_scan: third argument (mode) must be int64, but got %s", v.GetType().String()))
	}
	mode := vector.GetFixedAtNoTypeCheck[int64](v, 0)

	err = fulltextIndexMatch(proc, tf, index_table, pattern, mode, u.batch)

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

type Word struct {
	DocId      any
	Position   []int64
	DocCount   int32
	FirstDocId any
	LastDocId  any
}

type WordAccum struct {
	Id    int64
	Mode  int64
	Words map[any]*Word
}

type SearchAccum struct {
	TblName    string
	Mode       int64
	Pattern    []*Pattern
	Params     string
	WordAccums map[string]*WordAccum
	Nrow       int64
}

func NewWordAccum(id int64, mode int64) *WordAccum {
	return &WordAccum{Id: id, Mode: mode, Words: make(map[any]*Word)}
}

/*
// run each word
func (w *WordAccum) run(proc *process.Process, tblname string, first_doc_id, last_doc_id any) error {

	ssNoOp := w.Pattern.GetLeafText(NoOp)

	ssStar := w.Pattern.GetLeafText(Star)

	ss := append(ssNoOp, ssStar...)
	if len(ss) != 1 {
		return moerr.NewInternalError(proc.Ctx, "pattern is array. Not supported")
	}
	sqlWord := ss[0]

	sql := fmt.Sprintf(default_mode_sql, tblname, sqlWord)

	res, err := ft_runSql(proc, sql)
	if err != nil {
		return err
	}
	defer res.Close()

	for _, bat := range res.Batches {

		if len(bat.Vecs) != 5 {
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

			// pos int64
			pos := vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[1], i)

			// doc_count int32
			doc_count := vector.GetFixedAtWithTypeCheck[int32](bat.Vecs[2], i)

			// first_doc_id any
			first_doc_id := vector.GetAny(bat.Vecs[3], i)

			// last_doc_id any
			last_doc_id := vector.GetAny(bat.Vecs[4], i)

			//logutil.Infof("ID:%d, DOC_ID:%v, POS:%d, DOC_COUNT:%d, FIRST: %v, LAST: %v", id, doc_id, pos, doc_count, first_doc_id, last_doc_id)

			_, ok = w.Words[doc_id]
			if ok {
				w.Words[doc_id].Position = append(w.Words[doc_id].Position, pos)
			} else {
				w.Words[doc_id] = &Word{DocId: doc_id, Position: []int64{pos}, DocCount: doc_count, FirstDocId: first_doc_id, LastDocId: last_doc_id}
			}
		}

	}
	return nil
}
*/

func NewSearchAccum(tblname string, pattern string, mode int64, params string) (*SearchAccum, error) {

	// TODO: tokenize the pattern based on mode and params
	// use space as separator for now
	if mode != 0 {
		return nil, moerr.NewNotSupported(context.TODO(), "mode not supported")
	}

	ps, err := ParsePatternInBooleanMode(pattern)
	if err != nil {
		return nil, err
	}

	return &SearchAccum{TblName: tblname, Mode: mode, Pattern: ps, Params: params, WordAccums: make(map[string]*WordAccum)}, nil
}

// $(IDF) = LOG10(#word in collection/sum(doc_count))
// $(TF) = number of nword match in record (doc_count)
// $(rank) = $(TF) * $(IDF) * %(IDF)
func (s *SearchAccum) score(proc *process.Process) map[any]float32 {
	score := make(map[any]float32)

	if s.Nrow == 0 {
		return score
	}

	// calculate sum(doc_count)
	sum_count := make([]int32, len(s.WordAccums))
	i := 0
	for key := range s.WordAccums {
		acc := s.WordAccums[key]
		logutil.Infof("%v", acc)
		for doc_id := range acc.Words {
			sum_count[i] += acc.Words[doc_id].DocCount
		}
		i++
	}

	// calculate the score
	i = 0
	for key := range s.WordAccums {
		acc := s.WordAccums[key]
		logutil.Infof("%v", acc)
		for doc_id := range acc.Words {
			tf := float64(acc.Words[doc_id].DocCount)
			idf := math.Log10(float64(s.Nrow) / float64(sum_count[i]))
			tfidf := float32(tf * idf * idf)
			_, ok := score[doc_id]
			if ok {
				score[doc_id] += tfidf
			} else {
				score[doc_id] = tfidf
			}
		}
		i++
	}

	/*
		// sort by value
		keys := make([]any, 0, len(score))
		for key := range score {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool { return score[keys[i]] < score[keys[j]] })
	*/

	return score
}

func (s *SearchAccum) run(proc *process.Process) error {

	// count(*) to get number of words in the collection
	nrow, err := s.runCountStar(proc)
	if err != nil {
		return err
	}

	s.Nrow = nrow

	// filtering WHERE word = w1 OR word = w2 OR word = w3
	var filters []string
	for _, p := range s.Pattern {

		ssNoOp := p.GetLeafText(NoOp)
		for _, w := range ssNoOp {
			filters = append(filters, fmt.Sprintf("word = '%s'", w))
		}

		ssStar := p.GetLeafText(Star)
		for _, w := range ssStar {
			like := strings.ReplaceAll(w, "*", "%")
			filters = append(filters, fmt.Sprintf("word LIKE '%s'", like))
		}
	}

	whereClauses := strings.Join(filters, " OR ")

	sql := fmt.Sprintf(default_mode_sql, s.TblName)
	sql += " "
	sql += whereClauses

	logutil.Infof("SQL is %s", sql)

	res, err := ft_runSql(proc, sql)
	if err != nil {
		return err
	}
	defer res.Close()

	for _, bat := range res.Batches {

		if len(bat.Vecs) != 6 {
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

			// pos int64
			pos := vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[1], i)

			// doc_count int32
			doc_count := vector.GetFixedAtWithTypeCheck[int32](bat.Vecs[2], i)

			// first_doc_id any
			first_doc_id := vector.GetAny(bat.Vecs[3], i)

			// last_doc_id any
			last_doc_id := vector.GetAny(bat.Vecs[4], i)

			// word string
			word := bat.Vecs[5].GetStringAt(i)
			//logutil.Infof("ID:%d, DOC_ID:%v, POS:%d, DOC_COUNT:%d, FIRST: %v, LAST: %v", id, doc_id, pos, doc_count, first_doc_id, last_doc_id)

			w, ok := s.WordAccums[word]
			if !ok {
				s.WordAccums[word] = NewWordAccum(int64(i), s.Mode)
				w = s.WordAccums[word]
			}
			_, ok = w.Words[doc_id]
			if ok {
				w.Words[doc_id].Position = append(w.Words[doc_id].Position, pos)
			} else {
				w.Words[doc_id] = &Word{DocId: doc_id, Position: []int64{pos}, DocCount: doc_count, FirstDocId: first_doc_id, LastDocId: last_doc_id}
			}
		}

	}

	// we got all results for all words required.  Evaluate the Pattern against the WordAccums to get answer.

	return nil
}

func (s *SearchAccum) runCountStar(proc *process.Process) (int64, error) {
	var nrow int64
	nrow = 0
	sql := fmt.Sprintf(countstar_sql, s.TblName)
	logutil.Infof("COUNT STAR: %s", sql)

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

func fulltextIndexMatch(proc *process.Process, tableFunction *TableFunction, tblname, pattern string, mode int64, bat *batch.Batch) (err error) {

	s, err := NewSearchAccum(tblname, pattern, mode, "")
	if err != nil {
		return err
	}

	s.run(proc)

	scoremap := s.score(proc)

	if bat.VectorCount() == 1 {
		// only doc_id returned

		// write the batch
		for key := range scoremap {
			doc_id := key
			if s, ok := doc_id.(string); ok {
				bytes := []byte(s)
				doc_id = bytes
			}
			// type of id follow primary key column
			vector.AppendAny(bat.Vecs[0], doc_id, false, proc.Mp())
		}
	} else {
		// doc_id and score returned
		for key := range scoremap {
			doc_id := key
			if s, ok := doc_id.(string); ok {
				bytes := []byte(s)
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
