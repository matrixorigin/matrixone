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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func expressionDefaultIntType() planpb.Type {
	return planpb.Type{Id: int32(types.T_int64), Width: 64}
}

func expressionDefaultCol(pos int32, relPos int32) *planpb.Expr {
	return &planpb.Expr{
		Typ: expressionDefaultIntType(),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: relPos,
			ColPos: pos,
		}},
	}
}

func expressionDefaultAdd(left, right *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{
		Typ: expressionDefaultIntType(),
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: "+"},
			Args: []*planpb.Expr{left, right},
		}},
	}
}

func expressionDefaultInt(value int64) *planpb.Expr {
	return makePlan2Int64ConstExprWithType(value)
}

func expressionDefaultFindLocalCol(expr *planpb.Expr) *planpb.Expr {
	if expr == nil {
		return nil
	}
	switch impl := expr.Expr.(type) {
	case *planpb.Expr_Col:
		if impl.Col != nil && impl.Col.RelPos == 0 {
			return expr
		}
	case *planpb.Expr_F:
		for _, arg := range impl.F.Args {
			if found := expressionDefaultFindLocalCol(arg); found != nil {
				return found
			}
		}
	case *planpb.Expr_List:
		for _, item := range impl.List.List {
			if found := expressionDefaultFindLocalCol(item); found != nil {
				return found
			}
		}
	}
	return nil
}

func TestExpressionDefaultBindsAgainstCompleteRowSchema(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(),
		"create table t (id int primary key, a int default 5, b int default (a+1), c int default (b+1))", 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(NewMockCompilerContext(false), stmt, false)
	require.NoError(t, err)
	cols := p.GetDdl().GetCreateTable().GetTableDef().GetCols()
	require.Len(t, cols, 4)
	require.Equal(t, []int32{1}, collectRefColPos(cols[2].GetDefault().GetExpr()))
	require.Equal(t, []int32{2}, collectRefColPos(cols[3].GetDefault().GetExpr()))
	ref := expressionDefaultFindLocalCol(cols[2].GetDefault().GetExpr())
	require.NotNil(t, ref)
	require.Equal(t, cols[1].Typ, ref.Typ)
}

func TestExpressionDefaultAllowsForwardReferenceToBaseColumn(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(),
		"create table forward_base (b int default (a+1), a int)", 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(NewMockCompilerContext(false), stmt, false)
	require.NoError(t, err)
	cols := p.GetDdl().GetCreateTable().GetTableDef().GetCols()
	require.GreaterOrEqual(t, len(cols), 2)
	require.Equal(t, "b", cols[0].Name)
	require.Equal(t, "a", cols[1].Name)
	require.Equal(t, []int32{1}, collectRefColPos(cols[0].GetDefault().GetExpr()))
}

func TestExpressionDefaultRejectsInvalidDependencyGraph(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "self",
			sql:  "create table bad_self (id int primary key, a int default (a+1))",
			want: "cannot refer to itself",
		},
		{
			name: "cycle",
			sql:  "create table bad_cycle (id int primary key, a int default (b+1), b int default (a+1))",
			want: "circular dependency",
		},
		{
			name: "generated",
			sql:  "create table bad_generated (a int, b int as (a+1) stored, c int default (b+1))",
			want: "cannot refer to generated column",
		},
		{
			name: "auto increment",
			sql:  "create table bad_auto (a int auto_increment primary key, b int default (a+1))",
			want: "cannot refer to auto-increment column",
		},
		{
			name: "forward expression default",
			sql:  "create table bad_forward_expr (b int default (a+1), a int default (7))",
			want: "defined after it when that column has an expression default",
		},
		{
			name: "forward current timestamp default",
			sql:  "create table bad_forward_timestamp (b int default (a+1), a timestamp default current_timestamp)",
			want: "defined after it when that column has an expression default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(context.Background(), tt.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			_, err = BuildPlan(NewMockCompilerContext(false), stmt, false)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestDefaultExprExpanderResolvesChainsAndPreservesOuterRefs(t *testing.T) {
	columnExprs := map[int32]*planpb.Expr{
		0: expressionDefaultInt(10),
		1: expressionDefaultAdd(expressionDefaultCol(0, 0), expressionDefaultInt(1)),
		2: expressionDefaultAdd(expressionDefaultCol(1, 0), expressionDefaultInt(1)),
	}

	expanded, err := expandDefaultExprWithColumnExprs(
		context.Background(), columnExprs[2], columnExprs,
	)
	require.NoError(t, err)
	require.NotNil(t, expanded)
	require.Empty(t, collectRefColPos(expanded))
	require.Equal(t, int64(10), expanded.GetF().GetArgs()[0].GetF().GetArgs()[0].GetLit().GetI64Val())

	outer := expressionDefaultCol(7, 42)
	preserved, err := expandDefaultExprWithColumnExprs(
		context.Background(), outer, columnExprs,
	)
	require.NoError(t, err)
	require.Equal(t, int32(42), preserved.GetCol().GetRelPos())
	require.Equal(t, int32(7), preserved.GetCol().GetColPos())
}

func TestDefaultExprExpanderRejectsCycle(t *testing.T) {
	columnExprs := map[int32]*planpb.Expr{
		0: expressionDefaultAdd(expressionDefaultCol(1, 0), expressionDefaultInt(1)),
		1: expressionDefaultAdd(expressionDefaultCol(0, 0), expressionDefaultInt(1)),
	}

	_, err := expandDefaultExprWithColumnExprs(
		context.Background(), columnExprs[0], columnExprs,
	)
	require.ErrorContains(t, err, "circular dependency")
}

func TestExpandDefaultExprsInValueScanIsRowLocal(t *testing.T) {
	tableDef := &TableDef{
		Name2ColIndex: map[string]int32{"a": 0, "b": 1},
		Cols: []*ColDef{
			{Name: "a", Typ: expressionDefaultIntType()},
			{
				Name: "b",
				Typ:  expressionDefaultIntType(),
				Default: &planpb.Default{Expr: expressionDefaultAdd(
					expressionDefaultCol(0, 0), expressionDefaultInt(1),
				)},
			},
		},
	}
	rowset := &planpb.RowsetData{
		RowCount: 2,
		Cols: []*planpb.ColData{
			{Data: []*planpb.RowsetExpr{
				{Expr: expressionDefaultInt(10)},
				{Expr: expressionDefaultInt(20)},
			}},
			{Data: []*planpb.RowsetExpr{
				{Expr: expressionDefaultAdd(expressionDefaultCol(0, 0), expressionDefaultInt(1))},
				{Expr: expressionDefaultInt(99)},
			}},
		},
	}

	require.NoError(t, expandDefaultExprsInValueScan(
		context.Background(), tableDef, []string{"a", "b"}, rowset,
	))
	require.Empty(t, collectRefColPos(rowset.Cols[1].Data[0].Expr))
	require.Equal(t, int64(10), rowset.Cols[1].Data[0].Expr.GetF().GetArgs()[0].GetLit().GetI64Val())
	require.Equal(t, int64(99), rowset.Cols[1].Data[1].Expr.GetLit().GetI64Val())
}

func TestValidateDefaultColumnDependenciesCoversInvalidAndSharedGraphs(t *testing.T) {
	ctx := context.Background()
	intTyp := expressionDefaultIntType()
	autoTyp := intTyp
	autoTyp.AutoIncr = true

	tests := []struct {
		name string
		cols []*planpb.ColDef
		want string
	}{
		{
			name: "negative reference",
			cols: []*planpb.ColDef{{
				Name:    "a",
				Typ:     intTyp,
				Default: &planpb.Default{Expr: expressionDefaultCol(-1, 0)},
			}},
			want: "invalid column position -1",
		},
		{
			name: "out of range reference",
			cols: []*planpb.ColDef{{
				Name:    "a",
				Typ:     intTyp,
				Default: &planpb.Default{Expr: expressionDefaultCol(1, 0)},
			}},
			want: "invalid column position 1",
		},
		{
			name: "nil referenced column",
			cols: []*planpb.ColDef{
				{Name: "a", Typ: intTyp, Default: &planpb.Default{Expr: expressionDefaultCol(1, 0)}},
				nil,
			},
			want: "invalid column position 1",
		},
		{
			name: "generated reference",
			cols: []*planpb.ColDef{
				{Name: "a", Typ: intTyp, Default: &planpb.Default{Expr: expressionDefaultCol(1, 0)}},
				{Name: "g", Typ: intTyp, GeneratedCol: &planpb.GeneratedCol{}},
			},
			want: "cannot refer to generated column",
		},
		{
			name: "auto increment reference",
			cols: []*planpb.ColDef{
				{Name: "a", Typ: intTyp, Default: &planpb.Default{Expr: expressionDefaultCol(1, 0)}},
				{Name: "id", Typ: autoTyp},
			},
			want: "cannot refer to auto-increment column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDefaultColumnDependencies(ctx, tt.cols)
			require.ErrorContains(t, err, tt.want)
		})
	}

	// A shared dependency must be visited once and then accepted from the
	// second branch. All expression dependencies point backwards, so this is a
	// valid graph rather than a forward-reference rejection.
	shared := []*planpb.ColDef{
		{Name: "base", Typ: intTyp, Default: &planpb.Default{Expr: expressionDefaultInt(1)}},
		{Name: "left", Typ: intTyp, Default: &planpb.Default{Expr: expressionDefaultCol(0, 0)}},
		{Name: "right", Typ: intTyp, Default: &planpb.Default{Expr: expressionDefaultCol(0, 0)}},
		{Name: "top", Typ: intTyp, Default: &planpb.Default{Expr: expressionDefaultAdd(
			expressionDefaultCol(1, 0), expressionDefaultCol(2, 0),
		)}},
	}
	require.NoError(t, validateDefaultColumnDependencies(ctx, shared))
	require.NoError(t, validateDefaultColumnDependencies(ctx, nil))
}

func TestDefaultExprExpanderAndProjectionBoundaryCases(t *testing.T) {
	ctx := context.Background()
	literal := expressionDefaultInt(10)
	expander := newDefaultExprExpander(ctx, func(colIdx int32) (*planpb.Expr, bool) {
		if colIdx == 0 {
			return literal, true
		}
		return nil, false
	})

	nilExpr, err := expander.expandExpr(nil)
	require.NoError(t, err)
	require.Nil(t, nilExpr)

	unresolved := expressionDefaultCol(9, 0)
	got, err := expander.expandExpr(unresolved)
	require.NoError(t, err)
	require.Equal(t, int32(9), got.GetCol().GetColPos())

	withNilCol := &planpb.Expr{
		Typ:  expressionDefaultIntType(),
		Expr: &planpb.Expr_Col{Col: nil},
	}
	got, err = expander.expandExpr(withNilCol)
	require.NoError(t, err)
	require.NotNil(t, got.GetCol())

	outer := expressionDefaultCol(4, 2)
	got, err = expander.expandExpr(outer)
	require.NoError(t, err)
	require.Equal(t, int32(2), got.GetCol().GetRelPos())

	list := &planpb.Expr{
		Typ: expressionDefaultIntType(),
		Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
			expressionDefaultCol(0, 0),
			outer,
		}}},
	}
	got, err = expander.expandExpr(list)
	require.NoError(t, err)
	require.Equal(t, int64(10), got.GetList().GetList()[0].GetLit().GetI64Val())
	require.Equal(t, int32(2), got.GetList().GetList()[1].GetCol().GetRelPos())

	// The second expansion uses the memoized copy, while an unknown column
	// has no replacement and expandColumn returns nil.
	first, err := expander.expandColumn(0)
	require.NoError(t, err)
	second, err := expander.expandColumn(0)
	require.NoError(t, err)
	require.Equal(t, first, second)
	missing, err := expander.expandColumn(100)
	require.NoError(t, err)
	require.Nil(t, missing)

	require.False(t, exprHasLocalColumnRef(nil))
	require.False(t, exprHasLocalColumnRef(&planpb.Expr{
		Expr: &planpb.Expr_Col{Col: nil},
	}))
	require.False(t, exprHasLocalColumnRef(outer))
	require.True(t, exprHasLocalColumnRef(list))
	require.False(t, exprHasLocalColumnRef(literal))

	projection := []*planpb.Expr{expressionDefaultCol(0, 0), nil}
	require.NoError(t, expandDefaultExprsInProjection(
		ctx, projection, []int32{-1, 2, 1, 0},
		map[int32]*planpb.Expr{0: literal},
	))
	require.Equal(t, int64(10), projection[0].GetLit().GetI64Val())

	err = expandDefaultExprsInProjection(
		ctx,
		[]*planpb.Expr{expressionDefaultCol(0, 0)},
		[]int32{0},
		map[int32]*planpb.Expr{0: expressionDefaultCol(0, 0)},
	)
	require.ErrorContains(t, err, "circular dependency")
}

func TestExpandDefaultExprsInValueScanRejectsMalformedInputs(t *testing.T) {
	ctx := context.Background()
	intTyp := expressionDefaultIntType()
	tableDef := &TableDef{
		Name2ColIndex: map[string]int32{"a": 0},
		Cols: []*ColDef{
			{Name: "a", Typ: intTyp},
			{Name: "b", Typ: intTyp},
		},
	}

	require.NoError(t, expandDefaultExprsInValueScan(ctx, nil, []string{"a"}, nil))
	require.NoError(t, expandDefaultExprsInValueScan(ctx, tableDef, nil, &planpb.RowsetData{}))
	require.NoError(t, expandDefaultExprsInValueScan(ctx, tableDef, []string{"a"}, &planpb.RowsetData{
		RowCount: 0,
	}))

	// The fallback lookup is case-insensitive, and rows without local refs
	// take the cheap no-expansion path.
	require.NoError(t, expandDefaultExprsInValueScan(
		ctx, tableDef, []string{"A"}, &planpb.RowsetData{
			RowCount: 1,
			Cols: []*planpb.ColData{{Data: []*planpb.RowsetExpr{{
				Expr: expressionDefaultInt(7),
			}}}},
		},
	))

	bad := &planpb.RowsetData{RowCount: 1}
	err := expandDefaultExprsInValueScan(ctx, tableDef, []string{"missing"}, bad)
	require.ErrorContains(t, err, "does not exist")

	badPositionTable := &TableDef{
		Name2ColIndex: map[string]int32{"bad": -1},
		Cols:          []*ColDef{{Name: "a", Typ: intTyp}},
	}
	require.ErrorContains(t,
		expandDefaultExprsInValueScan(ctx, badPositionTable, []string{"bad"}, &planpb.RowsetData{}),
		"does not exist")

	for _, rowset := range []*planpb.RowsetData{
		{RowCount: 1},
		{RowCount: 1, Cols: []*planpb.ColData{{}}},
		{RowCount: 1, Cols: []*planpb.ColData{{Data: []*planpb.RowsetExpr{nil}}}},
	} {
		require.ErrorContains(t,
			expandDefaultExprsInValueScan(ctx, tableDef, []string{"a"}, rowset),
			"invalid VALUES rowset")
	}

	// A local reference to a non-nullable column without a default exercises
	// the error returned by the default resolver rather than a successful row
	// expansion.
	resolveErrorRowset := &planpb.RowsetData{
		RowCount: 1,
		Cols: []*planpb.ColData{{Data: []*planpb.RowsetExpr{{
			Expr: expressionDefaultCol(1, 0),
		}}}},
	}
	tableDef.Cols[1].Typ.NotNullable = true
	require.ErrorContains(t,
		expandDefaultExprsInValueScan(ctx, tableDef, []string{"a"}, resolveErrorRowset),
		"invalid default value")
}

func TestGetDefaultExprHandlesMissingAndNullableMetadata(t *testing.T) {
	ctx := context.Background()
	intTyp := expressionDefaultIntType()

	_, err := getDefaultExpr(ctx, nil)
	require.ErrorContains(t, err, "missing column definition")

	nonNullable := &planpb.ColDef{Name: "required", Typ: planpb.Type{
		Id: int32(types.T_int64), NotNullable: true,
	}}
	_, err = getDefaultExpr(ctx, nonNullable)
	require.ErrorContains(t, err, "invalid default value")

	nullable := &planpb.ColDef{Name: "nullable", Typ: intTyp}
	expr, err := getDefaultExpr(ctx, nullable)
	require.NoError(t, err)
	require.True(t, expr.GetLit().GetIsnull())
	require.False(t, expr.Typ.NotNullable)

	auto := &planpb.ColDef{Name: "id", Typ: planpb.Type{
		Id: int32(types.T_int64), NotNullable: true, AutoIncr: true,
	}}
	expr, err = getDefaultExpr(ctx, auto)
	require.NoError(t, err)
	require.True(t, expr.GetLit().GetIsnull())
	require.False(t, expr.Typ.NotNullable)

	invalidMetadata := &planpb.ColDef{
		Name: "bad",
		Typ:  intTyp,
		Default: &planpb.Default{
			NullAbility: false,
		},
	}
	_, err = getDefaultExpr(ctx, invalidMetadata)
	require.ErrorContains(t, err, "invalid default value")

	autoInvalidMetadata := &planpb.ColDef{
		Name: "auto",
		Typ:  planpb.Type{Id: int32(types.T_int64), AutoIncr: true},
		Default: &planpb.Default{
			NullAbility: false,
		},
	}
	expr, err = getDefaultExpr(ctx, autoInvalidMetadata)
	require.NoError(t, err)
	require.True(t, expr.GetLit().GetIsnull())

	expr, err = getDefaultExpr(ctx, &planpb.ColDef{
		Name:    "explicit_null",
		Typ:     intTyp,
		Default: &planpb.Default{NullAbility: true},
	})
	require.NoError(t, err)
	require.True(t, expr.GetLit().GetIsnull())

	expr, err = getDefaultExpr(ctx, &planpb.ColDef{
		Name:    "explicit_value",
		Typ:     intTyp,
		Default: &planpb.Default{Expr: expressionDefaultInt(7)},
	})
	require.NoError(t, err)
	require.Equal(t, int64(7), expr.GetLit().GetI64Val())
}

func TestDefaultBinderWithColumnsUsesSourceTypeAndRejectsInvalidNames(t *testing.T) {
	ctx := context.Background()
	sourceTyp := planpb.Type{Id: int32(types.T_int32), Width: 32}
	targetTyp := planpb.Type{Id: int32(types.T_float64)}
	binder := NewDefaultBinderWithColumns(ctx, targetTyp, []*ColDef{
		{Name: "Source", Typ: sourceTyp},
	})

	expr, err := binder.BindColRef(tree.NewUnresolvedColName("source"), 0, true)
	require.NoError(t, err)
	require.Equal(t, sourceTyp, expr.Typ)
	require.Equal(t, int32(0), expr.GetCol().GetColPos())

	qualified := tree.NewUnresolvedName(tree.NewCStr("t", 1), tree.NewCStr("source", 1))
	_, err = binder.BindColRef(qualified, 0, true)
	require.ErrorContains(t, err, "qualified column name")

	_, err = binder.BindColRef(tree.NewUnresolvedColName("missing"), 0, true)
	require.ErrorContains(t, err, "does not exist")
}

func TestRemapExpressionDefaultsAndReferencesAfterColumnChanges(t *testing.T) {
	ctx := context.Background()
	typ := expressionDefaultIntType()

	shiftColPosInExpr(nil, 1, 1)
	shiftColPosInExpr(&planpb.Expr{
		Typ:  typ,
		Expr: &planpb.Expr_Col{Col: nil},
	}, 1, 1)
	outer := expressionDefaultCol(2, 1)
	shiftColPosInExpr(outer, 1, 1)
	require.Equal(t, int32(2), outer.GetCol().GetColPos())

	newTable := func() *TableDef {
		return &TableDef{Cols: []*ColDef{
			{
				Name: "first",
				Default: &planpb.Default{Expr: expressionDefaultAdd(
					expressionDefaultCol(0, 0), expressionDefaultCol(2, 0),
				)},
			},
			{
				Name:         "generated",
				GeneratedCol: &planpb.GeneratedCol{Expr: expressionDefaultCol(2, 0)},
			},
			{
				Name:    "outer",
				Default: &planpb.Default{Expr: expressionDefaultCol(0, 1)},
			},
		}}
	}

	inserted := newTable()
	remapGeneratedColExprsAfterInsert(inserted, 1)
	require.Equal(t, []int32{0, 3}, collectRefColPos(inserted.Cols[0].Default.Expr))
	require.Equal(t, []int32{3}, collectRefColPos(inserted.Cols[1].GeneratedCol.Expr))
	require.Nil(t, collectRefColPos(inserted.Cols[2].Default.Expr))

	dropped := newTable()
	remapGeneratedColExprsAfterDrop(dropped, 1)
	require.Equal(t, []int32{0, 1}, collectRefColPos(dropped.Cols[0].Default.Expr))
	require.Equal(t, []int32{1}, collectRefColPos(dropped.Cols[1].GeneratedCol.Expr))

	cols := []*ColDef{{Name: "First"}, {Name: "Second"}}
	require.False(t, exprReferencesColumn(nil, "first", cols))
	require.False(t, exprReferencesColumn(expressionDefaultInt(1), "first", cols))
	require.False(t, exprReferencesColumn(&planpb.Expr{
		Expr: &planpb.Expr_Col{Col: nil},
	}, "first", cols))
	require.False(t, exprReferencesColumn(expressionDefaultCol(0, 1), "first", cols))
	require.False(t, exprReferencesColumn(expressionDefaultCol(9, 0), "first", cols))
	require.True(t, exprReferencesColumn(expressionDefaultCol(0, 0), "FIRST", cols))
	require.True(t, exprReferencesColumn(expressionDefaultAdd(
		expressionDefaultInt(1), expressionDefaultCol(1, 0),
	), "second", cols))
	require.False(t, exprReferencesColumn(&planpb.Expr{
		Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
			expressionDefaultInt(1),
		}}},
	}, "first", cols))

	defaultDependent := &TableDef{
		Cols: []*ColDef{{
			Name:    "value",
			Default: &planpb.Default{Expr: expressionDefaultCol(0, 0)},
		}},
	}
	require.ErrorContains(t,
		checkColumnWithDefaultDependency(ctx, defaultDependent, "value"),
		"depends on it")
	require.NoError(t, checkColumnWithDefaultDependency(ctx, &TableDef{
		Cols: []*ColDef{{Name: "value", Default: &planpb.Default{Expr: expressionDefaultInt(1)}}},
	}, "value"))
}

func TestExpressionDefaultClassification(t *testing.T) {
	require.False(t, isExpressionDefault(nil))
	require.False(t, isExpressionDefault(&planpb.Default{}))
	require.False(t, isExpressionDefault(&planpb.Default{Expr: expressionDefaultInt(1)}))
	require.True(t, isExpressionDefault(&planpb.Default{
		OriginString: "(1)",
		Expr:         expressionDefaultInt(1),
	}))
	require.True(t, isExpressionDefault(&planpb.Default{Expr: expressionDefaultCol(0, 0)}))
	require.True(t, isExpressionDefault(&planpb.Default{
		Expr: expressionDefaultAdd(expressionDefaultInt(1), expressionDefaultInt(2)),
	}))
}
