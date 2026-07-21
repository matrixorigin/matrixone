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
	"github.com/stretchr/testify/require"
)

func TestGenerateSeriesDatetimeLiteralScale(t *testing.T) {
	tests := []struct {
		value string
		want  int32
	}{
		{value: "2020-02-29 23:59:59", want: 0},
		{value: "2020-02-29 23:59:59.1", want: 1},
		{value: "2020-02-29 23:59:59.123", want: 3},
		{value: "2020-02-29 23:59:59.123456", want: 6},
		{value: "2020-02-29 23:59:59.123456789", want: 6},
		{value: "2020-02-29 23:59:59.123+08:00", want: 3},
	}
	for _, test := range tests {
		require.Equal(t, test.want, datetimeLiteralScale(test.value), test.value)
	}
}

func TestGenerateSeriesDatetimeScale(t *testing.T) {
	columnExpr := func(typ types.Type) *planpb.Expr {
		return &planpb.Expr{
			Typ: makePlan2Type(&typ),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
				RelPos: 0,
				ColPos: 0,
			}},
		}
	}
	str := MakePlan2StringConstExprWithType

	tests := []struct {
		name  string
		exprs []*planpb.Expr
		want  int32
	}{
		{
			name:  "whole seconds",
			exprs: []*planpb.Expr{str("2020-01-01 00:00:00"), str("2020-01-01 00:00:01"), str("1 second")},
			want:  0,
		},
		{
			name:  "fractional endpoint",
			exprs: []*planpb.Expr{str("2020-01-01 00:00:00.123"), str("2020-01-01 00:00:01.1"), str("1 second")},
			want:  3,
		},
		{
			name:  "microsecond step",
			exprs: []*planpb.Expr{str("2020-01-01 00:00:00"), str("2020-01-01 00:00:01"), str("1 MICROSECOND")},
			want:  6,
		},
		{
			name:  "dynamic string endpoint",
			exprs: []*planpb.Expr{columnExpr(types.T_varchar.ToType()), str("2020-01-01 00:00:01"), str("1 second")},
			want:  6,
		},
		{
			name:  "dynamic step",
			exprs: []*planpb.Expr{str("2020-01-01 00:00:00"), str("2020-01-01 00:00:01"), columnExpr(types.T_varchar.ToType())},
			want:  6,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, generateSeriesDatetimeScale(test.exprs))
		})
	}
}

func TestBindGenerateSeriesArgs(t *testing.T) {
	columnExpr := func(typ types.Type, colPos int32) *planpb.Expr {
		return &planpb.Expr{
			Typ: makePlan2Type(&typ),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
				RelPos: 0,
				ColPos: colPos,
			}},
		}
	}

	t.Run("numeric behavior is unchanged", func(t *testing.T) {
		exprs := []*planpb.Expr{
			MakePlan2Int64ConstExprWithType(1),
			MakePlan2Int64ConstExprWithType(2),
		}
		bound, typ, err := bindGenerateSeriesArgs(context.Background(), exprs)
		require.NoError(t, err)
		require.Same(t, exprs[0], bound[0])
		require.Equal(t, types.T_int64, typ.Oid)
	})

	t.Run("string endpoints are cast without modifying input", func(t *testing.T) {
		exprs := []*planpb.Expr{
			MakePlan2StringConstExprWithType("2020-02-29 23:59:59.124356"),
			MakePlan2StringConstExprWithType("2020-02-29 23:59:59.124360"),
			MakePlan2StringConstExprWithType("1 microsecond"),
		}
		bound, typ, err := bindGenerateSeriesArgs(context.Background(), exprs)
		require.NoError(t, err)
		require.Equal(t, types.T_varchar, typ.Oid)
		require.NotSame(t, exprs[0], bound[0])
		require.Same(t, exprs[2], bound[2])
		require.Equal(t, types.T_varchar, types.T(exprs[0].Typ.Id))
		for i := 0; i < 2; i++ {
			require.Equal(t, types.T_datetime, types.T(bound[i].Typ.Id))
			require.Equal(t, int32(6), bound[i].Typ.Scale)
			require.Equal(t, "cast", bound[i].GetF().GetFunc().GetObjName())
		}
	})

	t.Run("typed temporal input retains datetime result", func(t *testing.T) {
		startType := types.T_datetime.ToTypeWithScale(3)
		endType := types.T_datetime.ToTypeWithScale(6)
		exprs := []*planpb.Expr{
			columnExpr(startType, 0),
			columnExpr(endType, 1),
			MakePlan2StringConstExprWithType("1 second"),
		}
		bound, typ, err := bindGenerateSeriesArgs(context.Background(), exprs)
		require.NoError(t, err)
		require.Equal(t, types.T_datetime, typ.Oid)
		require.Equal(t, int32(6), typ.Scale)
		require.NotSame(t, exprs[0], bound[0])
		require.Same(t, exprs[1], bound[1])
		require.Equal(t, "cast", bound[0].GetF().GetFunc().GetObjName())
	})
}

func TestBuildGenerateSeriesOwnsStableResultSchema(t *testing.T) {
	build := func() *planpb.Node {
		logicPlan, err := runOneStmt(NewMockOptimizer(false), t,
			"select * from generate_series('2020-02-29 23:59:59.124356', '2020-02-29 23:59:59.124360', '1 microsecond') g")
		require.NoError(t, err)
		resultColumns := GetResultColumnsFromPlan(logicPlan)
		require.Len(t, resultColumns, 1)
		require.Equal(t, types.T_varchar, types.T(resultColumns[0].Typ.Id))
		query := resolveQueryPlan(logicPlan).GetQuery()
		for _, node := range query.Nodes {
			if node.NodeType == planpb.Node_FUNCTION_SCAN && node.GetTableDef().GetTblFunc().GetName() == "generate_series" {
				return node
			}
		}
		t.Fatal("generate_series function scan not found")
		return nil
	}

	first := build()
	second := build()
	require.Len(t, first.TableDef.Cols, 1)
	require.Equal(t, types.T_varchar, types.T(first.TableDef.Cols[0].Typ.Id))
	require.NotSame(t, first.TableDef.Cols[0], second.TableDef.Cols[0])
}
