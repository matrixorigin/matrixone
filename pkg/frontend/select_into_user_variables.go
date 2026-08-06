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
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// selectIntoUserVariables collects the result of SELECT ... INTO @var.  The
// output callback can receive several batches, so assignments are delayed
// until the complete result is known.  This also keeps existing variables
// unchanged for a zero-row query, as MySQL does.
type selectIntoUserVariables struct {
	mu       sync.Mutex
	vars     []*tree.VarExpr
	row      []any
	rowCount uint64
}

func newSelectIntoUserVariables(vars []*tree.VarExpr) *selectIntoUserVariables {
	return &selectIntoUserVariables{vars: vars}
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
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	if len(bat.Vecs) != len(collector.vars) {
		return moerr.NewInvalidInputf(ctx,
			"SELECT INTO has %d expressions for %d user variables", len(bat.Vecs), len(collector.vars))
	}

	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.rowCount == 0 {
		collector.row = make([]any, len(collector.vars))
		if err := extractRowFromEveryVector(ctx, ses, bat, 0, collector.row, false); err != nil {
			return err
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
		if err := ses.SetUserDefinedVar(variable.Name, collector.row[i], sql); err != nil {
			return err
		}
	}
	return nil
}
