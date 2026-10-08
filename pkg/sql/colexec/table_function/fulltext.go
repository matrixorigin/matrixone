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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	ftsearch "github.com/matrixorigin/matrixone/pkg/fulltext/plugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// fulltextState runs the fulltext_index_scan table function: it parses the
// arguments of one row and drives a classic fulltext index scan.
type fulltextState struct {
	inited bool
	scan   *ftsearch.Scan

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
	u.scan.Free(proc)
}

func (u *fulltextState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	return u.scan.Call(proc, u.batch)
}

// start calling tvf on nthRow and put the result in u.batch.  Note that current unnest impl will
// always return one batch per nthRow.
func (u *fulltextState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) error {

	if !u.inited {
		if err := u.scan.SetParam(tf.Params); err != nil {
			return err
		}
		u.batch = tf.createResultBatch()
		u.inited = true
	}
	u.scan.ResetRowState(proc)
	if u.batch != nil {
		u.batch.CleanOnlyData()
	}
	u.scan.SetScanSnapshot(tf.ScanSnapshot)

	v := tf.ctr.argVecs[0]
	if v.GetType().Oid != types.T_varchar {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("First argument (source table name) must be string, but got %s", v.GetType().String()))
	}
	source_table := v.GetStringAt(nthRow)

	v = tf.ctr.argVecs[1]
	if v.GetType().Oid != types.T_varchar {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("Second argument (index table name) must be string, but got %s", v.GetType().String()))
	}
	index_table := v.GetStringAt(nthRow)

	source_table, index_table, err := u.scan.ResolveExecutionTarget(proc.Ctx, tf.FulltextSourceRef, tf.FulltextIndexRef, source_table, index_table)
	if err != nil {
		return err
	}

	// Optional 5th argument: the zero-relevance guard for a MATCH score threshold that
	// was only known at EXECUTE (a prepared '?'). See QueryBuilder.fulltextRuntimeScoreGuard.
	//
	// This runs before the pattern is validated, so the guard decides the outcome
	// whatever the search term binds to. The refusal it restates is a plan-time
	// property of the threshold alone; letting a NULL or empty pattern report its own
	// error first would answer `AGAINST(NULL) > ?` differently from the identical
	// literal, which the planner refuses outright.
	if err := checkFulltextZeroRelevanceGuard(proc, tf.ctr.argVecs, 4, nthRow); err != nil {
		return err
	}

	v = tf.ctr.argVecs[2]
	if v.GetType().Oid != types.T_varchar {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("Third argument (pattern) must be string, but got %s", v.GetType().String()))
	}
	if v.IsConstNull() || v.GetNulls().Contains(uint64(nthRow)) {
		return moerr.NewInvalidInput(proc.Ctx, "fulltext search pattern must not be NULL")
	}
	pattern := v.GetStringAt(nthRow)
	if len(pattern) == 0 {
		return moerr.NewInvalidInput(proc.Ctx, "fulltext search pattern must not be empty")
	}

	v = tf.ctr.argVecs[3]
	if v.GetType().Oid != types.T_int64 {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf("Fourth argument (mode) must be int64, but got %s", v.GetType().String()))
	}
	mode := vector.GetFixedAtNoTypeCheck[int64](v, nthRow)

	scoreAlgo, err := fulltext.GetScoreAlgo(proc)
	if err != nil {
		return err
	}

	// Wait for the unique-join-keys runtime filter if configured (pre-filter pushdown)
	if u.scan.MembershipFilter() == nil && len(tf.RuntimeFilterSpecs) > 0 {
		filter, bfErr := waitFulltextMembershipFilter(proc, tf.RuntimeFilterSpecs)
		if bfErr != nil {
			return bfErr
		}
		if filter != nil {
			u.scan.SetMembershipFilter(filter)
		}
	}

	err = u.scan.Match(proc, source_table, index_table, pattern, mode, string(tf.Params), scoreAlgo)
	opStats := tf.OpAnalyzer.GetOpStats()
	opStats.BackgroundQueries = append(opStats.BackgroundQueries, u.scan.TakeBackgroundQueries()...)
	return err
}

// prepare
func fulltextIndexScanPrepare(proc *process.Process, tableFunction *TableFunction) (tvfState, error) {
	var err error
	st := &fulltextState{}
	tableFunction.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tableFunction.Args)
	tableFunction.ctr.argVecs = make([]*vector.Vector, len(tableFunction.Args))
	if err != nil {
		return nil, err
	}

	limit, err := evalLimitExpression(proc, tableFunction.Limit, 0)
	if err != nil {
		return nil, err
	}

	// TODO: LIMIT BY RANK should set ranking to true
	st.scan = ftsearch.NewScan(limit)
	return st, err
}

// waitFulltextMembershipFilter waits for a unique-join-keys runtime filter message
// and builds the doc_id membership filter for reader-level filtering; nil means
// no filter.
func waitFulltextMembershipFilter(proc *process.Process, specs []*plan.RuntimeFilterSpec) ([]byte, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	spec := specs[0]
	if !spec.UseMembershipFilter {
		return nil, nil
	}

	sqlProc := sqlexec.NewSqlProcess(proc)
	sqlProc.RuntimeFilterSpecs = specs

	vecbytes, err := sqlexec.WaitUniqueJoinKeys(sqlProc)
	if err != nil || len(vecbytes) == 0 {
		return nil, err
	}
	return fulltext.BuildMembershipFilter(proc, vecbytes)
}

// checkFulltextZeroRelevanceGuard evaluates the optional zero-relevance guard argument
// a driving fulltext table function carries when a MATCH score threshold is only known
// at execution.
//
// The planner cannot test `MATCH(...) <op> ?` at plan time, so it emits `0 <op> ?` as a
// boolean argument instead. True means a document with relevance 0 -- one this index
// never returns -- would satisfy the predicate, so answering from the index would drop
// exactly those rows. That is the same condition the planner rejects for a literal
// threshold, and it raises the same error, so `> ?` and `>= ?` behave identically to
// the literals they stand for at every bound value.
//
// A missing argument (the threshold was a literal, so the planner already checked it)
// or a NULL bound is not a violation: a NULL threshold makes the comparison NULL and
// the query returns no rows either way.
func checkFulltextZeroRelevanceGuard(proc *process.Process, argVecs []*vector.Vector, pos int, nthRow int) error {
	if pos >= len(argVecs) {
		return nil
	}
	v := argVecs[pos]
	if v == nil || v.Length() == 0 {
		return nil
	}
	if v.GetType().Oid != types.T_bool {
		return moerr.NewInvalidInput(proc.Ctx, fmt.Sprintf(
			"fulltext score-threshold guard must be bool, but got %s", v.GetType().String()))
	}
	row := nthRow
	if v.IsConst() {
		row = 0
	}
	if v.IsConstNull() || v.GetNulls().Contains(uint64(row)) {
		return nil
	}
	if vector.GetFixedAtNoTypeCheck[bool](v, row) {
		return moerr.NewNotSupported(proc.Ctx,
			"MATCH() AGAINST() function cannot be replaced by FULLTEXT INDEX and full table scan with fulltext search is not supported yet.")
	}
	return nil
}
