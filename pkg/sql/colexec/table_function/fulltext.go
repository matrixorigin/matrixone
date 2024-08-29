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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm"
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
	t1.doc_id = t2.doc_id ORDER BY tfidf;`
)

// prepare
func fulltextIndexScanPrepare(proc *process.Process, tableFunction *TableFunction) (err error) {
	tableFunction.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tableFunction.Args)

	logutil.Infof("FULLTEXTINDESCSCAN PREPARE")
	for i := range tableFunction.Attrs {
		tableFunction.Attrs[i] = strings.ToUpper(tableFunction.Attrs[i])
	}
	return err
}

// run SQL here
func fulltextIndexScanCall(_ int, proc *process.Process, tableFunction *TableFunction, result *vm.CallResult) (bool, error) {

	var (
		err  error
		rbat *batch.Batch
	)
	bat := result.Batch
	defer func() {
		if err != nil && rbat != nil {
			rbat.Clean(proc.Mp())
		}
	}()
	if bat == nil {
		return true, nil
	}

	logutil.Infof("FULLTEXTINDEXSCAN CALL")

	for i, arg := range tableFunction.Args {
		logutil.Infof("ARG %d: %s", i, arg.String())
	}

	logutil.Infof("PARAM : %s", string(tableFunction.Params))

	for i, attr := range tableFunction.Attrs {
		logutil.Infof("ATTRS %d: %s", i, attr)
	}

	v, err := tableFunction.ctr.executorsForArgs[0].Eval(proc, []*batch.Batch{bat}, nil)
	if err != nil {
		return false, err
	}
	if v.GetType().Oid != types.T_varchar {
		return false, moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("fulltext_index_scan: first argument must be string, but got %s", v.GetType().String()))
	}

	index_table := v.UnsafeGetStringAt(0)

	v, err = tableFunction.ctr.executorsForArgs[1].Eval(proc, []*batch.Batch{bat}, nil)
	if err != nil {
		return false, err
	}
	if v.GetType().Oid != types.T_varchar {
		return false, moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("fulltext_index_scan: second argument must be string, but got %s", v.GetType().String()))
	}

	pk_json := v.UnsafeGetStringAt(0)

	v, err = tableFunction.ctr.executorsForArgs[2].Eval(proc, []*batch.Batch{bat}, nil)
	if err != nil {
		return false, err
	}
	if v.GetType().Oid != types.T_varchar {
		return false, moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("fulltext_index_scan: third argument must be string, but got %s", v.GetType().String()))
	}

	keys := v.UnsafeGetStringAt(0)

	v, err = tableFunction.ctr.executorsForArgs[3].Eval(proc, []*batch.Batch{bat}, nil)
	if err != nil {
		return false, err
	}
	if v.GetType().Oid != types.T_varchar {
		return false, moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("fulltext_index_scan: fourth argument must be string, but got %s", v.GetType().String()))
	}

	pattern := v.UnsafeGetStringAt(0)

	logutil.Infof("index %s, pk_json %s, key %s, pattern %s", index_table, pk_json, keys, pattern)

	rbat, err = fulltextIndexMatch(proc, tableFunction, index_table, pattern)
	if err != nil {
		return false, err
	}

	//result.Batch = bat.Dup(proc.Mp())
	result.Batch = rbat
	return false, nil
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

func fulltextIndexMatch(proc *process.Process, tableFunction *TableFunction, tblname, pattern string) (batch *batch.Batch, err error) {

	var projects []string

	if len(tableFunction.Attrs) == 2 {
		projects = append(projects, project_doc_id)
		tfidf := fmt.Sprintf(project_tfidf, tblname, tblname, pattern)
		projects = append(projects, tfidf)

	} else if len(tableFunction.Attrs) == 1 {
		if tableFunction.Attrs[0] == "DOC_ID" {
			projects = append(projects, project_doc_id)
			tfidf := fmt.Sprintf(project_tfidf, tblname, tblname, pattern)
			projects = append(projects, tfidf)
		} else {
			tfidf := fmt.Sprintf(project_tfidf, tblname, tblname, pattern)
			projects = append(projects, tfidf)
			projects = append(projects, project_doc_id)
		}
	}

	/*
		for j, attr := range tableFunction.Attrs {
			if attr == "DOC_ID" {
				projects = append(projects, project_doc_id)
			} else if attr == "TFIDF" {
				tfidf := fmt.Sprintf(project_tfidf, tblname, pattern)
				projects = append(projects, tfidf)
			}
		}
	*/
	/*
		projects = append(projects, project_doc_id)
		tfidf := fmt.Sprintf(project_tfidf, tblname, tblname, pattern)
		projects = append(projects, tfidf)
	*/

	project := strings.Join(projects, ",")

	sql := fmt.Sprintf(single_word_exact_match_sql, project, tblname, pattern, tblname, tblname, pattern)
	logutil.Infof("FULLTEXT SQL = %s", sql)
	res, err := ft_runSql(proc, sql)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	if res.Batches != nil && len(res.Batches) > 0 {
		return res.Batches[0].Dup(proc.Mp())
		//return res.Batches[0], nil
	}

	return nil, nil
}
