// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// Rebind every producer and consumer together, without changing the prepared
// AST, logical plan or compile. No new runtime cache is published on this path.
func rebindPreparedIntegerSource(execCtx *ExecCtx, ses FeSession, prepared *PrepareStmt, values []any) (*plan2.Plan, error) {
	compiler := ses.GetTxnCompileCtx()
	previousDatabase := compiler.GetDatabase()
	compiler.SetDatabase(prepared.defaultDatabase)
	defer compiler.SetDatabase(previousDatabase)
	var rebuilt *plan2.Plan
	err := execCtx.withRootSQL(prepared.Sql, func() (err error) {
		rebuilt, err = buildPreparedIntegerSource(execCtx.reqCtx, ses, compiler,
			prepared.Sql, prepared.schedulingSQLMode, values, prepared.integerSourceParamPositions)
		return err
	})
	return rebuilt, err
}

func buildPreparedIntegerSource(reqCtx context.Context, ses FeSession, compiler plan2.CompilerContext,
	sql, sqlMode string, values []any, positions []int32) (*plan2.Plan, error) {
	lowerCase, err := compiler.ResolveVariable("lower_case_table_names", true, false)
	if err != nil {
		return nil, err
	}
	stmts, err := mysql.ParseWithSQLMode(reqCtx, sql, lowerCase.(int64), sqlMode)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	}()
	if len(stmts) != 1 {
		return nil, moerr.NewInternalError(reqCtx, "prepared integer source must contain one statement")
	}
	ctx := plan2.WithPreparedIntegerBindings(reqCtx, compiler, values, positions)
	rebuilt, err := buildPlanWithPrepareMode(reqCtx, ses, ctx, &tree.PrepareStmt{Stmt: stmts[0]}, true)
	if err != nil {
		return nil, err
	}
	p := rebuilt.GetDcl().GetPrepare().Plan
	// Other markers still own their existing assignment, comparison and
	// value-driven offset contracts. Specialize them without touching the
	// freshly bound source domains or changing any cached prepared plan.
	bound := make([]bool, len(values))
	for _, pos := range positions {
		if pos >= 0 && int(pos) < len(bound) {
			bound[pos] = true
		}
	}
	remaining := make([]int32, 0, len(values))
	for i := range values {
		if !bound[i] {
			remaining = append(remaining, int32(i))
		}
	}
	if len(remaining) > 0 {
		p, _, err = plan2.FillPreparedDMLParamsAtPositions(reqCtx, p, values, remaining)
	}
	return p, err
}
