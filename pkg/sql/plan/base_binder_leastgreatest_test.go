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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/stretchr/testify/require"
)

func TestBindLeastGreatestTemporalScale(t *testing.T) {
	for _, oid := range []types.T{types.T_time, types.T_datetime, types.T_timestamp} {
		t.Run(oid.String(), func(t *testing.T) {
			args := []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(oid), Width: 64, Scale: 1}},
				{Typ: planpb.Type{Id: int32(oid), Width: 64, Scale: 4}},
			}
			for _, name := range []string{"greatest", "least"} {
				expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
				require.NoError(t, err, name)
				require.Equal(t, int32(oid), expr.Typ.Id, name)
				require.Equal(t, int32(4), expr.Typ.Scale, name)
			}
		})
	}

	t.Run("mixed temporal oids preserve max scale", func(t *testing.T) {
		args := []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(types.T_date), Width: 64}},
			{Typ: planpb.Type{Id: int32(types.T_datetime), Width: 64, Scale: 1}},
			{Typ: planpb.Type{Id: int32(types.T_datetime), Width: 64, Scale: 6}},
		}
		for _, name := range []string{"greatest", "least"} {
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
			require.NoError(t, err, name)
			require.Equal(t, int32(types.T_datetime), expr.Typ.Id, name)
			require.Equal(t, int32(6), expr.Typ.Scale, name)
		}
	})
}

func TestBuildLeastGreatestTemporalScale(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select greatest(cast('10:00:00.1' as time(1)), cast('10:00:00.99' as time(2)))", 1)
	require.NoError(t, err)

	pl, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.NoError(t, err)

	var result *planpb.Expr
	for _, node := range pl.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			if expr.GetF() != nil && expr.GetF().GetFunc().GetObjName() == "greatest" {
				result = expr
			}
		}
	}
	require.NotNil(t, result)
	require.Equal(t, int32(types.T_time), result.Typ.Id)
	require.Equal(t, int32(2), result.Typ.Scale)
}

func TestMinMaxMixedTextLeastGreatestPreservesBinaryCollation(t *testing.T) {
	mock := NewMockOptimizer(true)
	mock.ctxt.tables["bind_select"].Cols[1].Typ = planpb.Type{
		Id:      int32(types.T_varchar),
		Width:   80,
		Charset: uint32(types.CharsetUTF8MB4Bin),
	}
	mock.ctxt.tables["bind_select"].Cols[2].Typ = planpb.Type{
		Id:      int32(types.T_text),
		Width:   types.MaxVarcharLen,
		Charset: uint32(types.CharsetUTF8MB4Bin),
	}

	p, err := runOneStmt(mock, t,
		"select min(least(b, c)), max(greatest(c, b)) from select_test.bind_select")
	require.NoError(t, err)

	var aggregates []*planpb.Expr
	for _, node := range p.GetQuery().Nodes {
		if node.NodeType == planpb.Node_AGG {
			aggregates = node.AggList
			break
		}
	}
	require.Len(t, aggregates, 2)
	for _, aggregate := range aggregates {
		require.Equal(t, int32(types.T_text), aggregate.Typ.Id)
		require.Equal(t, uint32(types.CharsetUTF8MB4Bin), aggregate.Typ.Charset)
		require.Len(t, aggregate.GetF().Args, 1)

		leastGreatest := aggregate.GetF().Args[0]
		require.Equal(t, int32(types.T_text), leastGreatest.Typ.Id)
		require.Equal(t, uint32(types.CharsetUTF8MB4Bin), leastGreatest.Typ.Charset)
		require.Contains(t, []string{"least", "greatest"}, leastGreatest.GetF().Func.ObjName)
		require.Len(t, leastGreatest.GetF().Args, 2)
		for _, argument := range leastGreatest.GetF().Args {
			require.Equal(t, int32(types.T_text), argument.Typ.Id)
			require.Equal(t, uint32(types.CharsetUTF8MB4Bin), argument.Typ.Charset)
		}
	}
}

func TestConstantFoldLeastGreatestTemporalScale(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select greatest(cast('10:00:00.1' as time(1)), cast('10:00:00.99' as time(2)))", 1)
	require.NoError(t, err)

	pl, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)

	var result *planpb.Expr
	for _, node := range pl.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			if expr.GetF() != nil && expr.GetF().GetFunc().GetObjName() == "greatest" {
				result = expr
			}
		}
	}
	require.NotNil(t, result)
	require.Len(t, result.GetF().Args, 2)
	require.Equal(t, int32(2), result.GetF().Args[0].Typ.Scale)
	require.Equal(t, int32(2), result.GetF().Args[1].Typ.Scale)
	require.Equal(t, int32(2), result.Typ.Scale)

	fold := rule.NewConstantFold(false)
	foldOne := func(expr *planpb.Expr) *planpb.Expr {
		node := &planpb.Node{ProjectList: []*planpb.Expr{DeepCopyExpr(expr)}}
		fold.Apply(node, nil, ctx.GetProcess())
		return node.ProjectList[0]
	}

	firstCast := foldOne(result.GetF().Args[0])
	secondCast := foldOne(result.GetF().Args[1])
	foldedGreatest := foldOne(result)

	// The resolver adds an implicit TIME(2) cast around TIME(1), so both
	// arguments must be TIME(2) before the outer function is folded.
	require.Equal(t, int32(2), firstCast.Typ.Scale, "first argument")
	require.Equal(t, int32(2), secondCast.Typ.Scale, "TIME(2) cast")
	require.Equal(t, int32(2), foldedGreatest.Typ.Scale, "GREATEST result")
}

func TestConstantFoldLeastGreatestBitUsesNumericText(t *testing.T) {
	tests := []struct {
		name     string
		function string
		sql      string
		want     string
	}{
		{
			name:     "greatest time and bit",
			function: "greatest",
			sql:      "select greatest(cast('10:00:00' as time), cast(2 as bit(4)))",
			want:     "2",
		},
		{
			name:     "greatest date and bit",
			function: "greatest",
			sql:      "select greatest(cast('2020-01-01' as date), cast(20200102 as bit(25)))",
			want:     "2020-01-02",
		},
		{
			name:     "least time and bit",
			function: "least",
			sql:      "select least(cast('10:00:00' as time), cast(2 as bit(4)))",
			want:     "10:00:00",
		},
		{
			name:     "least date and bit",
			function: "least",
			sql:      "select least(cast('2020-01-01' as date), cast(20200102 as bit(25)))",
			want:     "2020-01-01",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(true)
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, test.sql, 1)
			require.NoError(t, err)

			pl, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)

			var result *planpb.Expr
			for _, node := range pl.GetQuery().Nodes {
				for _, expr := range node.ProjectList {
					if expr.GetF() != nil && expr.GetF().GetFunc().GetObjName() == test.function {
						result = expr
					}
				}
			}
			require.NotNil(t, result)
			require.Len(t, result.GetF().Args, 2)
			bitTextCast := result.GetF().Args[1].GetF()
			require.NotNil(t, bitTextCast)
			require.Equal(t, "cast", bitTextCast.GetFunc().GetObjName())
			require.Equal(t, int32(types.T_varchar), result.GetF().Args[1].Typ.Id)
			require.Equal(t, int32(types.T_uint64), bitTextCast.Args[0].Typ.Id)

			node := &planpb.Node{ProjectList: []*planpb.Expr{DeepCopyExpr(result)}}
			rule.NewConstantFold(false).Apply(node, nil, ctx.GetProcess())
			folded := node.ProjectList[0]
			require.NotNil(t, folded.GetLit())
			require.Equal(t, test.want, folded.GetLit().GetSval())
		})
	}
}
