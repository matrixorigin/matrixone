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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// dependentCountCoalesce counts coalesce function nodes in an expression tree.
func dependentCountCoalesce(e *planpb.Expr) int {
	if e == nil {
		return 0
	}
	total := 0
	if f := e.GetF(); f != nil {
		if f.Func != nil && f.Func.ObjName == "coalesce" {
			total++
		}
		for _, arg := range f.Args {
			total += dependentCountCoalesce(arg)
		}
		return total
	}
	if l := e.GetList(); l != nil {
		for _, item := range l.List {
			total += dependentCountCoalesce(item)
		}
	}
	return total
}

// dependentSumCoalesce counts coalesce nodes over every projection a plan
// node references, so assertions do not depend on exact node positions.
func dependentSumCoalesce(nodes []*planpb.Node) int {
	total := 0
	for _, node := range nodes {
		if node == nil {
			continue
		}
		for _, expr := range node.ProjectList {
			total += dependentCountCoalesce(expr)
		}
		for _, pred := range node.OnList {
			total += dependentCountCoalesce(pred)
		}
	}
	return total
}

// dependentFindCoalesce reports whether the subtree holds any coalesce node.
func dependentFindCoalesce(e *planpb.Expr) bool {
	return dependentCountCoalesce(e) > 0
}

// dependentCoalesceUnderLit reports whether some function in the tree takes a
// coalesce-holding argument plus an integer literal companion, pinning that a
// dependent expression (for example h generated as (g+1) or b default
// (a+1)) reads its NOT NULL source through the implicit-default coalesce.
func dependentCoalesceUnderLit(e *planpb.Expr, companion int64) bool {
	if e == nil {
		return false
	}
	if f := e.GetF(); f != nil {
		for i, arg := range f.Args {
			if dependentFindCoalesce(arg) {
				for j, other := range f.Args {
					if j == i {
						continue
					}
					if lit := other.GetLit(); lit != nil && lit.GetI64Val() == companion {
						return true
					}
				}
			}
		}
		for _, arg := range f.Args {
			if dependentCoalesceUnderLit(arg, companion) {
				return true
			}
		}
		return false
	}
	if l := e.GetList(); l != nil {
		for _, item := range l.List {
			if dependentCoalesceUnderLit(item, companion) {
				return true
			}
		}
	}
	return false
}

func dependentAnyCoalesceUnderLit(nodes []*planpb.Node, companion int64) bool {
	for _, node := range nodes {
		if node == nil {
			continue
		}
		for _, expr := range node.ProjectList {
			if dependentCoalesceUnderLit(expr, companion) {
				return true
			}
		}
		for _, pred := range node.OnList {
			if dependentCoalesceUnderLit(pred, companion) {
				return true
			}
		}
	}
	return false
}

// dependentBuildTable plans the DDL and returns its table definition, so the
// insert projection is exercised against real catalog shapes.
func dependentBuildTable(t *testing.T, ddl string) *planpb.TableDef {
	t.Helper()
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, ddl, 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(ctx, stmt, false, nil)
	require.NoError(t, err)
	table := p.GetDdl().GetCreateTable().TableDef
	require.NotNil(t, table)
	return table
}

// dependentDriveInsert runs the insert projection builder over the supplied
// columns and returns the whole query node set for structural assertions.
func dependentDriveInsert(
	t *testing.T,
	tableDef *planpb.TableDef,
	supplied map[string]*planpb.Expr,
	ignore bool,
) []*planpb.Node {
	t.Helper()
	nodes, err := dependentDriveInsertError(t, tableDef, supplied, ignore)
	require.NoError(t, err)
	return nodes
}

func dependentDriveInsertError(
	t *testing.T,
	tableDef *planpb.TableDef,
	supplied map[string]*planpb.Expr,
	ignore bool,
) ([]*planpb.Node, error) {
	t.Helper()
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	builder := NewQueryBuilder(planpb.Query_INSERT, ctx, false, true)
	bindCtx := NewBindContext(builder, nil)
	child := builder.appendNode(&planpb.Node{NodeType: planpb.Node_VALUE_SCAN}, bindCtx)
	objRef := &planpb.ObjectRef{SchemaName: "db", ObjName: tableDef.Name, Obj: 1}
	_, _, _, _, err := builder.appendNodesForInsertStmt(bindCtx, child, tableDef, objRef, supplied, ignore)
	if err != nil {
		return nil, err
	}
	return builder.qry.Nodes, nil
}

func dependentNullInt64Expr() *planpb.Expr {
	return &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int64), Width: 64},
		Expr: &planpb.Expr_Lit{Lit: &Const{Isnull: true}},
	}
}

// TestInsertIgnoreNormalizesDependentRowExpressions pins the review contract:
// under INSERT IGNORE, every dependent row expression that the pre-insert
// adjustment feeds must read the adjusted value, so its reference to a NOT
// NULL column is rebuilt as coalesce(reference, implicit default) inside the
// insert projection. That covers a generated column whose expression reads a
// NOT NULL generated intermediate (h generated as (g+1) after g computes
// NULL from a filled cell) and a column default whose expression reads a
// supplied NOT NULL column. Without IGNORE, no projection is rewritten.
func TestInsertIgnoreNormalizesDependentRowExpressions(t *testing.T) {
	t.Run("generated chain reads the adjusted intermediate", func(t *testing.T) {
		tableDef := dependentBuildTable(t,
			"create table t(a int not null, g int not null generated always as (nullif(a,0)) stored, h int generated always as (g+1) stored)")
		nodes := dependentDriveInsert(t, tableDef, map[string]*planpb.Expr{"a": dependentNullInt64Expr()}, true)
		// The DDL normalizes nullif(a,0) into a case expression holding two
		// references to a, and g+1 holds one reference to g: the IGNORE
		// projection carries exactly those three coalesces (a -> g twice,
		// g -> h once).
		require.Equal(t, 3, dependentSumCoalesce(nodes),
			"the adjustment must be visible to the generated chain: a -> g twice, g -> h once")
		require.True(t, dependentAnyCoalesceUnderLit(nodes, 1),
			"h generated as (g+1) must read the adjusted g through the coalesce fill")

		strict := dependentDriveInsert(t, tableDef, map[string]*planpb.Expr{"a": dependentNullInt64Expr()}, false)
		require.Equal(t, 0, dependentSumCoalesce(strict),
			"a strict insert never rewrites the projection; it rejects the NULL instead")
	})

	t.Run("column default reads the adjusted base column", func(t *testing.T) {
		tableDef := dependentBuildTable(t,
			"create table t(a int not null, b int default (a+1), c int)")
		nodes := dependentDriveInsert(t, tableDef, map[string]*planpb.Expr{"a": dependentNullInt64Expr()}, true)
		require.Equal(t, 1, dependentSumCoalesce(nodes),
			"the dependent default (a+1) must read the adjusted a exactly once")
		require.True(t, dependentAnyCoalesceUnderLit(nodes, 1),
			"the default expression must combine the adjusted a with its own constant")

		strict := dependentDriveInsert(t, tableDef, map[string]*planpb.Expr{"a": dependentNullInt64Expr()}, false)
		require.Equal(t, 0, dependentSumCoalesce(strict))
	})

	// MySQL 8.0.44 accepts a CREATE TABLE whose expression default references
	// a later-declared column; MatrixOne accepts the DDL too. The statement
	// MatrixOne rejects for this table is the one omitting that column when
	// it is NOT NULL without a default (the pre-existing strict divergence
	// family for omitted no-default columns, kept by this fix: MySQL under
	// IGNORE would fill the omitted cell with warning 1364). With the column
	// supplied, the forward-declared default is exactly the dependent
	// expression this round normalizes.
	t.Run("forward-declared dependent default", func(t *testing.T) {
		tableDef := dependentBuildTable(t,
			"create table t(b int default (a+1), a int not null, c int)")
		require.Equal(t, "b", tableDef.Cols[0].Name)
		require.Equal(t, "a", tableDef.Cols[1].Name)

		// Omitting the NOT NULL column keeps the pre-existing rejection.
		_, err := dependentDriveInsertError(t, tableDef,
			map[string]*planpb.Expr{"c": makePlan2Int64ConstExprWithType(1)}, true)
		require.ErrorContains(t, err, "invalid default value for column 'a'")

		// Supplying the column feeds the forward default through the
		// adjustment: the coalesce must appear in the ignored projection.
		nodes := dependentDriveInsert(t, tableDef,
			map[string]*planpb.Expr{"a": dependentNullInt64Expr()}, true)
		require.Equal(t, 1, dependentSumCoalesce(nodes),
			"the forward-declared default must read the adjusted a")
		require.True(t, dependentAnyCoalesceUnderLit(nodes, 1))

		strict := dependentDriveInsert(t, tableDef,
			map[string]*planpb.Expr{"a": dependentNullInt64Expr()}, false)
		require.Equal(t, 0, dependentSumCoalesce(strict))
	})
}

// TestCoalesceIgnoreFillRefsNormalizeDependentSources pins the rewrite
// classification when a dependent expression reads a source the pre-insert
// adjustment can fill: generated columns (their cells are computed and a NOT
// NULL one can still be NULL when the dependent reads it), defaulted columns
// with a dynamic or NULL-literal default, and protected references (nullable
// generated, hidden, constant default) that stay raw. Every classification
// runs without any planner-visited column state (no supplied map, no lazy
// NULL-default map), so the answer for a column is static and order-free: a
// forward reference resolves identically wherever the DDL permits one.
func TestCoalesceIgnoreFillRefsNormalizeDependentSources(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_INSERT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)
	int64Typ := planpb.Type{Id: int32(types.T_int64)}
	exprDefault := &planpb.Expr{
		Typ: int64Typ,
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: "+"},
			Args: []*planpb.Expr{
				makePlan2Int64ConstExprWithType(1),
				makePlan2Int64ConstExprWithType(2),
			},
		}},
	}
	tableDef := &planpb.TableDef{
		Cols: []*planpb.ColDef{
			{Name: "a", Typ: int64Typ, NotNull: true, Default: &planpb.Default{NullAbility: false}},
			{Name: "g", Typ: int64Typ, NotNull: true, GeneratedCol: &planpb.GeneratedCol{}},
			{Name: "gn", Typ: int64Typ, Default: &planpb.Default{NullAbility: true}, GeneratedCol: &planpb.GeneratedCol{}},
			{Name: "gh", Typ: int64Typ, NotNull: true, Hidden: true, GeneratedCol: &planpb.GeneratedCol{}},
			{Name: "fwd", Typ: int64Typ, NotNull: true, Default: &planpb.Default{NullAbility: true}},
			{Name: "dyn", Typ: int64Typ, NotNull: true, Default: &planpb.Default{NullAbility: false, Expr: exprDefault}},
			{Name: "konst", Typ: int64Typ, NotNull: true, Default: &planpb.Default{
				NullAbility: false, Expr: makePlan2Int64ConstExprWithType(7),
			}},
			{Name: "nn", Typ: int64Typ, Default: &planpb.Default{NullAbility: true}},
		},
	}
	emptySupplied := map[string]*planpb.Expr{}

	requireWrapped := func(t *testing.T, pos int32) {
		t.Helper()
		ref := colRefForTest(pos, int64Typ)
		out, err := builder.coalesceGeneratedColIgnoreFillRefs(tableDef, emptySupplied, ref)
		require.NoError(t, err)
		wrapped := out.GetF()
		require.NotNil(t, wrapped, "reference at position %d must be coalesced to the implicit default", pos)
		require.Equal(t, "coalesce", wrapped.Func.ObjName)
		require.Equal(t, pos, wrapped.Args[0].GetCol().ColPos,
			"reference at position %d survives as the first coalesce argument", pos)
		require.EqualValues(t, 0, wrapped.Args[1].GetLit().GetI64Val())
	}
	requireRaw := func(t *testing.T, pos int32) {
		t.Helper()
		ref := colRefForTest(pos, int64Typ)
		out, err := builder.coalesceGeneratedColIgnoreFillRefs(tableDef, emptySupplied, ref)
		require.NoError(t, err)
		require.Same(t, ref, out, "reference at position %d must stay raw", pos)
	}

	t.Run("not null generated reference wraps", func(t *testing.T) { requireWrapped(t, 1) })
	t.Run("nullable generated reference stays raw", func(t *testing.T) { requireRaw(t, 2) })
	t.Run("hidden generated reference stays raw", func(t *testing.T) { requireRaw(t, 3) })
	t.Run("implicit null default wraps without the lazy map", func(t *testing.T) { requireWrapped(t, 4) })
	t.Run("expression default wraps", func(t *testing.T) { requireWrapped(t, 5) })
	t.Run("constant default stays raw", func(t *testing.T) { requireRaw(t, 6) })
	t.Run("nullable column stays raw", func(t *testing.T) { requireRaw(t, 7) })

	t.Run("generated reference inside a dependent expression", func(t *testing.T) {
		expr := &planpb.Expr{
			Typ: int64Typ,
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "+"},
				Args: []*planpb.Expr{colRefForTest(1, int64Typ), makePlan2Int64ConstExprWithType(1)},
			}},
		}
		out, err := builder.coalesceGeneratedColIgnoreFillRefs(tableDef, emptySupplied, expr)
		require.NoError(t, err)
		f := out.GetF()
		require.NotNil(t, f, "the dependent function node is preserved")
		require.Len(t, f.Args, 2)
		wrapped := f.Args[0].GetF()
		require.NotNil(t, wrapped, "the generated-column argument is rewritten in place")
		require.Equal(t, "coalesce", wrapped.Func.ObjName)
		require.Equal(t, int32(1), wrapped.Args[0].GetCol().ColPos)
		require.EqualValues(t, 1, f.Args[1].GetLit().GetI64Val(),
			"the dependent expression's own constant is untouched")
	})
}
