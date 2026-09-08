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
