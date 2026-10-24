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

package plan

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func tableDumpDDLCreate(t *testing.T, ctx CompilerContext, sql string) *planpb.TableDef {
	t.Helper()
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	value, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	return value.GetDdl().GetCreateTable().GetTableDef()
}

func tableDumpDDLColumn(t *testing.T, def *planpb.TableDef, name string) *planpb.ColDef {
	t.Helper()
	for _, col := range def.Cols {
		if col.Name == name {
			return col
		}
	}
	t.Fatalf("column %q absent", name)
	return nil
}

func tableDumpDDLCatalog(def *planpb.TableDef) *planpb.TableDef {
	result := proto.Clone(def).(*planpb.TableDef)
	for _, col := range result.Cols {
		col.Typ.Table = result.Name
		if col.Default != nil {
			col.Typ.NotNullable = !col.Default.NullAbility
		}
	}
	return result
}

// Author through ordinary DDL owners using the actual catalog column types.
// No verifier binding mode, compatibility witness or NN-bit edit authors these
// expected complete trees. The parsed declaration is independent of BuildPlan's AST.
func tableDumpDDLCatalogAuthored(t *testing.T, ctx CompilerContext, catalog *planpb.TableDef, sql string) *planpb.TableDef {
	t.Helper()
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	result := proto.Clone(catalog).(*planpb.TableDef)
	result.Checks = nil
	var columns []*planpb.ColDef
	for _, col := range result.Cols {
		if !col.Hidden {
			columns = append(columns, col)
		}
	}
	bindCtx := ddlExpressionContext(ctx, ctx.GetProcess().Ctx)
	for _, declaration := range stmt.(*tree.CreateTable).Defs {
		switch declaration := declaration.(type) {
		case *tree.ColumnTableDef:
			col := tableDumpDDLColumn(t, result, declaration.Name.ColName())
			if col.GeneratedCol != nil {
				col.GeneratedCol, err = buildGeneratedExpr(bindCtx, declaration, col.Typ, columns, ctx.GetProcess())
				require.NoError(t, err)
			} else if col.Default != nil && col.Default.Expr != nil {
				col.Default, err = buildDefaultExprWithColumns(bindCtx, declaration, col.Typ, ctx.GetProcess(), columns)
				require.NoError(t, err)
			}
		case *tree.CheckIndex:
			name := catalog.Checks[len(result.Checks)].Name
			require.NoError(t, appendCheckDef(ctx, result, name, declaration.Expr, -1))
		}
	}
	return result
}

func tableDumpDDLReferences(t *testing.T, def *planpb.TableDef, expr *planpb.Expr, column string) []*planpb.Expr {
	t.Helper()
	columnPos := int32(-1)
	for i, col := range def.Cols {
		if col != nil && col.Name == column {
			columnPos = int32(i)
			break
		}
	}
	require.NotEqual(t, int32(-1), columnPos, "input column absent from independent declaration")
	var result []*planpb.Expr
	var visit func(*planpb.Expr)
	visit = func(expr *planpb.Expr) {
		if expr == nil {
			return
		}
		if col := expr.GetCol(); col != nil && col.RelPos == 0 && col.ColPos == columnPos {
			result = append(result, expr)
		}
		if fn := expr.GetF(); fn != nil {
			for _, arg := range fn.Args {
				visit(arg)
			}
		}
		if list := expr.GetList(); list != nil {
			for _, item := range list.List {
				visit(item)
			}
		}
		if literal := expr.GetLit(); literal != nil {
			visit(literal.Src)
		}
	}
	visit(expr)
	return result
}

func TestTableDumpDDLColumnScopeBindings(t *testing.T) {
	for _, tc := range []struct {
		name, declarations, input string
		inputNN, targetNN         bool
	}{
		{"json_not_null", "data json not null,g longtext generated always as (data ->> '$.kind') stored", "data", true, false},
		{"json_nullable", "data json,g longtext generated always as (data ->> '$.kind') stored", "data", false, false},
		{"numeric_not_null", "a bigint not null,g bigint generated always as (a+a) stored", "a", true, false},
		{"numeric_nullable", "a bigint,g bigint generated always as (a+a) stored", "a", false, false},
		{"numeric_target_not_null", "a bigint,g bigint not null generated always as (a+a) stored", "a", false, true},
		{"numeric_both_not_null", "a bigint not null,g bigint not null generated always as (a+a) stored", "a", true, true},
		{"check_not_null", "a bigint not null,constraint ck check(a+a>1)", "a", true, false},
		{"check_nullable", "a bigint,constraint ck check(a+a>1)", "a", false, false},
		{"default_not_null", "a bigint not null,g bigint default (a+1)", "a", true, false},
		{"default_nullable", "a bigint,g bigint default (a+1)", "a", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			sql := "create table ddl_scope(" + tc.declarations + ")"
			created := tableDumpDDLCreate(t, ctx, sql)
			createdBefore := proto.Clone(created)
			catalog := tableDumpDDLCatalog(created)
			require.False(t, tableDumpDDLColumn(t, created, tc.input).Typ.NotNullable)
			require.Equal(t, tc.inputNN, tableDumpDDLColumn(t, catalog, tc.input).Typ.NotNullable)
			if len(catalog.Checks) == 0 {
				require.Equal(t, tc.targetNN, tableDumpDDLColumn(t, catalog, "g").Typ.NotNullable)
			}
			catalogBefore := proto.Clone(catalog)
			alter := tableDumpDDLCatalogAuthored(t, ctx, catalog, sql)
			require.True(t, proto.Equal(catalogBefore, catalog), "ordinary catalog-scope authoring mutated its source")
			oldItems, newItems := tableDumpExpressions(catalog), tableDumpExpressions(alter)
			require.Len(t, oldItems, 1)
			require.Len(t, newItems, 1)
			createRefs := tableDumpDDLReferences(t, catalog, oldItems[0].expr, tc.input)
			alterRefs := tableDumpDDLReferences(t, alter, newItems[0].expr, tc.input)
			require.NotEmpty(t, createRefs)
			require.Len(t, alterRefs, len(createRefs))
			for i, ref := range createRefs {
				require.False(t, ref.Typ.NotNullable, "CREATE bound before catalog augmentation")
				require.Equal(t, tc.inputNN, alterRefs[i].Typ.NotNullable, "catalog-scope author must bind actual current types")
			}
			varies := !referenceTableDumpBoundExpression(oldItems[0].expr, newItems[0].expr)
			if tc.inputNN {
				require.True(t, varies, "positive case must expose a whole-binding NN difference")
			} else if !tc.targetNN {
				require.False(t, varies, "nullable control has only ignorable table lineage")
			}
			for _, target := range []*planpb.TableDef{catalog, alter} {
				for _, supplied := range []*planpb.TableDef{catalog, alter, nil} {
					targetBefore := proto.Clone(target)
					var suppliedBefore proto.Message
					if supplied != nil {
						suppliedBefore = proto.Clone(supplied)
					}
					sensitive, err := AnalyzeTableDumpBindings(ctx, target, supplied)
					require.NoError(t, err)
					require.Equal(t, varies, sensitive, "both whole owners and v1 classification must retain sensitivity")
					require.True(t, proto.Equal(targetBefore, target), "target declaration mutated")
					if supplied != nil {
						require.True(t, proto.Equal(suppliedBefore, supplied), "supplied expression was normalized or mutated")
					}
				}
			}
			require.True(t, proto.Equal(createdBefore, created))
		})
	}
}

func TestTableDumpDDLColumnScopeRejectsForgedTrees(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	sql := func(expression string) string {
		return "create table ddl_scope(a bigint not null,g bigint generated always as (" + expression + ") stored)"
	}
	catalog := tableDumpDDLCatalog(tableDumpDDLCreate(t, ctx, sql("a+a+1")))
	for _, corruption := range []string{"one_reference_nn", "literal", "operator"} {
		t.Run(corruption, func(t *testing.T) {
			forged := proto.Clone(catalog).(*planpb.TableDef)
			expr := &tableDumpDDLColumn(t, forged, "g").GeneratedCol.Expr
			if corruption == "one_reference_nn" {
				refs := tableDumpDDLReferences(t, forged, *expr, "a")
				require.Len(t, refs, 2)
				require.NotSame(t, refs[0], refs[1])
				require.False(t, refs[0].Typ.NotNullable)
				require.False(t, refs[1].Typ.NotNullable)
				refs[0].Typ.NotNullable = true
				require.False(t, refs[1].Typ.NotNullable, "forge only one occurrence of the same column")
			} else {
				replacement := "a+a+2"
				if corruption == "operator" {
					replacement = "a*a+1"
				}
				other := tableDumpDDLCreate(t, ctx, sql(replacement))
				*expr = proto.Clone(tableDumpDDLColumn(t, other, "g").GeneratedCol.Expr).(*planpb.Expr)
			}
			beforeTarget, beforeForged := proto.Clone(catalog), proto.Clone(forged)
			_, err := AnalyzeTableDumpBindings(ctx, catalog, forged)
			require.ErrorContains(t, err, "cannot verify bound generated expression g")
			require.True(t, proto.Equal(beforeTarget, catalog))
			require.True(t, proto.Equal(beforeForged, forged))
		})
	}
	empty := proto.Clone(catalog).(*planpb.TableDef)
	tableDumpDDLColumn(t, empty, "g").GeneratedCol = nil
	require.Empty(t, tableDumpExpressions(empty))
	for _, pair := range [][2]*planpb.TableDef{{empty, catalog}, {catalog, empty}} {
		beforeTarget, beforeSupplied := proto.Clone(pair[0]), proto.Clone(pair[1])
		_, err := AnalyzeTableDumpBindings(ctx, pair[0], pair[1])
		require.ErrorContains(t, err, "expression count does not match target")
		require.True(t, proto.Equal(beforeTarget, pair[0]))
		require.True(t, proto.Equal(beforeSupplied, pair[1]))
	}
}

func TestTableDumpDDLColumnVisitor(t *testing.T) {
	for _, tc := range []struct {
		expression string
		column     bool
	}{{"1", false}, {"abs(-1)", false}, {"a", true}, {"ddl_scope.a", true}} {
		t.Run(tc.expression, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "select "+tc.expression, 1)
			require.NoError(t, err)
			defer stmt.Free()
			expr := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			visitor := &tableDumpExpressionVisitor{}
			expr.Accept(visitor)
			require.Equal(t, tc.column, visitor.hasColumn)
		})
	}
}
