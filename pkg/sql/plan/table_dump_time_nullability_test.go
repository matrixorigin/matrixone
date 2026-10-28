// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// TIME() string generated expressions keep the row-local NULL synthesis
// contract for new bindings. The current catalog shape verifies through the
// real dump-binding consumer, and the historical cross-era restore (the
// pre-8dd NOT NULL metadata) plus the protective exact-tree rejection are
// covered on the released literal shape below, which review 5688430687
// deliberately keeps fraction-free to avoid preexisting precision rebind
// differences of the column-referencing form.
func TestTableDumpTimeStringNullabilityEra(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	compiler.tables["gtime"] = &plan.TableDef{Name: "gtime"}
	const ddl = "create table gtime(a varchar(25) not null,g time(0) generated always as (time(a)) stored)"
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, ddl, 1)
	require.NoError(t, err)
	defer stmt.Free()
	build := func(ctx context.Context) *plan.TableDef {
		built, err := BuildPlan(tableDumpBindContext{CompilerContext: compiler, ctx: ctx, increment: 4}, stmt, false)
		require.NoError(t, err)
		return built.GetDdl().GetCreateTable().GetTableDef()
	}
	generated := func(def *plan.TableDef) *plan.Expr {
		return def.Cols[1].GetGeneratedCol().GetExpr()
	}

	current := build(compiler.GetContext())
	// New bindings infer the row-local NULL synthesis contract.
	require.False(t, generated(current).Typ.NotNullable)

	// The production dump flow re-verifies the current catalog def against
	// itself through the current binding era.
	_, err = AnalyzeTableDumpBindings(compiler, current, current)
	require.NoError(t, err)
}

// TestTableDumpTimeLiteralNullabilityEra covers the exact released shape from
// review 5688430687: the fraction-free TIME('12:00:00') generated expression
// that the 9ebefa5bc1 catalog persisted with NOT NULL metadata.
func TestTableDumpTimeLiteralNullabilityEra(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	compiler.tables["ltime"] = &plan.TableDef{Name: "ltime"}
	const ddl = "create table ltime(g time(0) generated always as (time('12:00:00')) stored)"
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, ddl, 1)
	require.NoError(t, err)
	defer stmt.Free()
	bind := func() *plan.TableDef {
		built, err := BuildPlan(tableDumpBindContext{CompilerContext: compiler, ctx: compiler.GetContext(), increment: 4}, stmt, false)
		require.NoError(t, err)
		return built.GetDdl().GetCreateTable().GetTableDef()
	}
	literal := bind()
	gen := literal.Cols[0].GetGeneratedCol().GetExpr()
	require.NotNil(t, gen.GetF(), "catalog generated expressions keep the bound function node")
	require.False(t, gen.Typ.NotNullable)
	_, err = AnalyzeTableDumpBindings(compiler, literal, literal)
	require.NoError(t, err)

	// An old persisted catalog shipped this function node with the pre-8dd
	// NOT NULL metadata. The dump consumer must re-bind it through the
	// historical era instead of rejecting a legitimate generated expression.
	persisted := proto.Clone(literal).(*plan.TableDef)
	persisted.Cols[0].GetGeneratedCol().GetExpr().Typ.NotNullable = true
	_, err = AnalyzeTableDumpBindings(compiler, persisted, persisted)
	require.NoError(t, err)

	// Exact-tree comparison still rejects forged mixed metadata: a stored
	// tree whose argument nullability no binding era ever produced.
	forged := proto.Clone(persisted).(*plan.TableDef)
	arg := forged.Cols[0].GetGeneratedCol().GetExpr().GetF().Args[0]
	arg.Typ.NotNullable = !arg.Typ.NotNullable
	_, err = AnalyzeTableDumpBindings(compiler, forged, forged)
	require.ErrorContains(t, err, "table dump cannot verify bound")
}
