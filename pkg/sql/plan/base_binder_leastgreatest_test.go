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
