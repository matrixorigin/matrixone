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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	project_doc_id              = "t1.doc_id"
	project_tfidf               = "CAST ((nmatch/nword) * log10((SELECT count(*) from %s) / (SELECT COUNT(1) FROM %s WHERE word='%s')) AS float) AS tfidf"
	single_word_exact_match_sql = `SELECT %s FROM 
	(SELECT MIN(first_doc_id) AS first_doc_id, MAX(last_doc_id) AS last_doc_id, MAX(doc_count) AS nmatch, doc_id FROM %s WHERE word ='%s' GROUP BY doc_id ) AS t1  
	LEFT JOIN  
	(SELECT COUNT(1) as nword, doc_id FROM %s WHERE doc_id in (SELECT doc_id FROM %s WHERE word = '%s') GROUP BY doc_id) AS t2 
	ON 
	t1.doc_id = t2.doc_id;`
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

func fulltextIndexMatch(proc *process.Process, tableFunction *TableFunction, tblname, pattern string, mode int64, bat *batch.Batch) (err error) {

	var projects []string

	if len(tableFunction.Attrs) == 2 {
		projects = append(projects, project_doc_id)
		tfidf := fmt.Sprintf(project_tfidf, tblname, tblname, pattern)
		projects = append(projects, tfidf)

	} else if len(tableFunction.Attrs) == 1 {
		if tableFunction.Attrs[0] == "DOC_ID" {
			projects = append(projects, project_doc_id)
			//tfidf := fmt.Sprintf(project_tfidf, tblname, tblname, pattern)
			//projects = append(projects, tfidf)
		} else {
			tfidf := fmt.Sprintf(project_tfidf, tblname, tblname, pattern)
			projects = append(projects, tfidf)
			projects = append(projects, project_doc_id)
		}
	}

	project := strings.Join(projects, ",")

	sql := fmt.Sprintf(single_word_exact_match_sql, project, tblname, pattern, tblname, tblname, pattern)
	logutil.Infof("FULLTEXT SQL = %s", sql)
	res, err := ft_runSql(proc, sql)
	if err != nil {
		return err
	}
	defer res.Close()

	for _, b := range res.Batches {
		bat, err = bat.Append(proc.Ctx, proc.Mp(), b)
		if err != nil {
			return err
		}
	}

	return nil
}
