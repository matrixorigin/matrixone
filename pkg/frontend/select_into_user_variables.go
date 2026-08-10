// Copyright 2026 Matrix Origin
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

package frontend

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// selectIntoUserVariables collects the result of SELECT ... INTO @var.  The
// output callback can receive several batches, so assignments are delayed
// until the complete result is known.  This also keeps existing variables
// unchanged for a zero-row query, as MySQL does.
type selectIntoUserVariables struct {
	mu       sync.Mutex
	vars     []*tree.VarExpr
	row      []any
	rowIsBin []bool
	rowType  []plan2.Type
	rowCount uint64
}

func newSelectIntoUserVariables(vars []*tree.VarExpr) *selectIntoUserVariables {
	return &selectIntoUserVariables{vars: vars}
}

// validateSelectIntoArity checks the result shape before execution.  A query
// that produces no rows may not invoke the output callback, so validating only
// in capture would let a mismatched SELECT ... INTO silently succeed.
func validateSelectIntoArity(ctx context.Context, p *plan.Plan, variableCount int) error {
	if p == nil {
		return nil
	}
	if len(plan2.GetResultColumnsFromPlan(p)) != variableCount {
		return moerr.NewWrongNumberOfColumnsInSelect(ctx)
	}
	return nil
}

func selectIntoUserVariablesOutputCallback(
	execCtx *ExecCtx,
	ses FeSession,
	stmt tree.Statement,
	fill func(*batch.Batch, *perfcounter.CounterSet) error,
) func(*batch.Batch, *perfcounter.CounterSet) error {
	selectStmt, ok := stmt.(*tree.Select)
	if !ok || len(selectStmt.IntoVars) == 0 {
		return fill
	}
	// ExecCtx is reused for every statement in a COM_QUERY request.  A
	// collector belongs to exactly one SELECT ... INTO statement; retaining it
	// across statement generation leaks rowCount and assignments into the next
	// statement.
	collector := newSelectIntoUserVariables(selectStmt.IntoVars)
	execCtx.selectInto = collector
	return func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
		return collector.capture(execCtx.reqCtx, ses, bat)
	}
}

func (collector *selectIntoUserVariables) capture(ctx context.Context, ses FeSession, bat *batch.Batch) error {
	if bat == nil {
		return nil
	}
	if len(bat.Vecs) != len(collector.vars) {
		return moerr.NewWrongNumberOfColumnsInSelect(ctx)
	}
	if bat.RowCount() == 0 {
		return nil
	}

	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.rowCount+uint64(bat.RowCount()) > 1 {
		return moerr.NewTooManyRows(ctx)
	}
	if collector.rowCount == 0 {
		collector.row = make([]any, len(collector.vars))
		collector.rowIsBin = make([]bool, len(collector.vars))
		collector.rowType = make([]plan2.Type, len(collector.vars))
		if err := extractRowFromEveryVector(ctx, ses, bat, 0, collector.row, false); err != nil {
			return err
		}
		for i, vec := range bat.Vecs {
			collector.rowIsBin[i] = vec.GetIsBin()
			collector.rowType[i] = plan2.MakePlan2Type(vec.GetType())
		}
	}
	collector.rowCount += uint64(bat.RowCount())
	return nil
}

func (collector *selectIntoUserVariables) apply(ctx context.Context, ses FeSession, sql string) error {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.rowCount == 0 {
		appendSelectIntoNoDataWarning(ses)
		return nil
	}
	if collector.rowCount > 1 {
		return moerr.NewTooManyRows(ctx)
	}
	for i, variable := range collector.vars {
		var isBin bool
		if i < len(collector.rowIsBin) {
			isBin = collector.rowIsBin[i]
		}
		var typ plan2.Type
		if i < len(collector.rowType) {
			typ = collector.rowType[i]
		}
		if err := setUserDefinedVarWithType(ses, variable.Name, collector.row[i], sql, isBin, typ); err != nil {
			return err
		}
	}
	return nil
}

func setUserDefinedVarWithIsBin(ses FeSession, name string, value interface{}, sql string, isBin bool) error {
	return setUserDefinedVarWithType(ses, name, value, sql, isBin, plan2.Type{})
}

func setUserDefinedVarWithType(ses FeSession, name string, value interface{}, sql string, isBin bool, typ plan2.Type) error {
	switch session := ses.(type) {
	case *Session:
		return session.setUserDefinedVarWithType(name, value, sql, isBin, typ)
	case *backSession:
		if session.upstream == nil {
			return moerr.NewInternalError(context.Background(), "do not support set user defined var in background exec")
		}
		return setUserDefinedVarWithType(session.upstream, name, value, sql, isBin, typ)
	default:
		if isBin {
			return moerr.NewInternalError(context.Background(), "do not support binary user defined var assignment")
		}
		return ses.SetUserDefinedVar(name, value, sql)
	}
}

func appendSelectIntoNoDataWarning(ses FeSession) {
	switch session := ses.(type) {
	case *Session:
		session.appendWarningDiagnostic(moerr.ER_SP_FETCH_NO_DATA, "No data - zero rows fetched, selected, or processed")
	case *backSession:
		if session.upstream != nil {
			appendSelectIntoNoDataWarning(session.upstream)
		}
	}
}

func appendSelectIntoDeprecatedWarning(ses FeSession, deprecated bool) {
	if !deprecated {
		return
	}
	const msg = "The INTO clause is deprecated inside query blocks of query expressions and will be removed in a future release. Please move the INTO clause to the end of statement instead."
	switch session := ses.(type) {
	case *Session:
		session.appendWarningDiagnostic(moerr.ER_WARN_DEPRECATED_INNER_INTO, msg)
	case *backSession:
		if session.upstream != nil {
			appendSelectIntoDeprecatedWarning(session.upstream, deprecated)
		}
	}
}
