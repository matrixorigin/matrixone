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
	if execCtx.selectInto == nil {
		execCtx.selectInto = newSelectIntoUserVariables(selectStmt.IntoVars)
	}
	return func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
		return execCtx.selectInto.capture(execCtx.reqCtx, ses, bat)
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
		if err := extractRowFromEveryVector(ctx, ses, bat, 0, collector.row, false); err != nil {
			return err
		}
		for i, vec := range bat.Vecs {
			collector.rowIsBin[i] = vec.GetIsBin()
		}
	}
	collector.rowCount += uint64(bat.RowCount())
	return nil
}

func (collector *selectIntoUserVariables) apply(ctx context.Context, ses FeSession, sql string) error {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.rowCount == 0 {
		return nil
	}
	if collector.rowCount > 1 {
		return moerr.NewTooManyRows(ctx)
	}
	for i, variable := range collector.vars {
		if err := setUserDefinedVarWithIsBin(ses, variable.Name, collector.row[i], sql, collector.rowIsBin[i]); err != nil {
			return err
		}
	}
	return nil
}

func setUserDefinedVarWithIsBin(ses FeSession, name string, value interface{}, sql string, isBin bool) error {
	switch session := ses.(type) {
	case *Session:
		return session.setUserDefinedVar(name, value, sql, isBin)
	case *backSession:
		if session.upstream == nil {
			return moerr.NewInternalError(context.Background(), "do not support set user defined var in background exec")
		}
		return setUserDefinedVarWithIsBin(session.upstream, name, value, sql, isBin)
	default:
		if isBin {
			return moerr.NewInternalError(context.Background(), "do not support binary user defined var assignment")
		}
		return ses.SetUserDefinedVar(name, value, sql)
	}
}
