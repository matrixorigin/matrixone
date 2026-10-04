// Copyright 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestBindTimestampAddFSPByUnit(t *testing.T) {
	ctx := context.Background()

	temporalExpr := func(oid types.T, scale int32) *plan.Expr {
		return &plan.Expr{
			Expr: &plan.Expr_Col{Col: &plan.ColRef{}},
			Typ:  plan.Type{Id: int32(oid), Scale: scale},
		}
	}

	tests := []struct {
		name      string
		unit      string
		inputType types.T
		inputFSP  int32
		resultOID types.T
		resultFSP int32
	}{
		{"datetime0_second", "SECOND", types.T_datetime, 0, types.T_datetime, 0},
		{"datetime3_second", "SECOND", types.T_datetime, 3, types.T_datetime, 3},
		{"datetime6_second", "SECOND", types.T_datetime, 6, types.T_datetime, 6},
		{"datetime0_microsecond", "MICROSECOND", types.T_datetime, 0, types.T_datetime, 6},
		{"datetime3_microsecond", "MICROSECOND", types.T_datetime, 3, types.T_datetime, 6},
		{"timestamp0_second", "SECOND", types.T_timestamp, 0, types.T_datetime, 0},
		{"timestamp3_second", "SECOND", types.T_timestamp, 3, types.T_datetime, 3},
		{"timestamp6_second", "SECOND", types.T_timestamp, 6, types.T_datetime, 6},
		{"timestamp0_microsecond", "MICROSECOND", types.T_timestamp, 0, types.T_datetime, 6},
		{"timestamp3_microsecond", "MICROSECOND", types.T_timestamp, 3, types.T_datetime, 6},
		{"date_second", "SECOND", types.T_date, 0, types.T_datetime, 0},
		{"date_microsecond", "MICROSECOND", types.T_date, 0, types.T_datetime, 6},
		{"date_day", "DAY", types.T_date, 0, types.T_date, 0},
		{"date_week", "WEEK", types.T_date, 0, types.T_date, 0},
		{"date_month", "MONTH", types.T_date, 0, types.T_date, 0},
		{"date_quarter", "QUARTER", types.T_date, 0, types.T_date, 0},
		{"date_year", "YEAR", types.T_date, 0, types.T_date, 0},
		{"date_hour", "HOUR", types.T_date, 0, types.T_datetime, 0},
		{"date_minute", "MINUTE", types.T_date, 0, types.T_datetime, 0},
		{"char_string", "SECOND", types.T_char, 0, types.T_varchar, 0},
		{"datetime_unknown_unit", "UNKNOWN", types.T_datetime, 3, types.T_datetime, 6},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := BindFuncExprImplByPlanExpr(ctx, "timestampadd", []*plan.Expr{
				MakePlan2StringConstExprWithType(tc.unit), MakePlan2Int64ConstExprWithType(1), temporalExpr(tc.inputType, tc.inputFSP),
			})
			require.NoError(t, err)
			require.Equal(t, int32(tc.resultOID), expr.Typ.Id)
			require.Equal(t, tc.resultFSP, expr.Typ.Scale)
			wantWidth := tc.resultFSP
			if tc.resultOID == types.T_varchar {
				wantWidth = 65535
			}
			require.Equal(t, wantWidth, expr.Typ.Width)
			columns := GetResultColumnsFromPlan(&plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
				StmtType: plan.Query_SELECT, Steps: []int32{0}, Headings: []string{"added"},
				Nodes: []*plan.Node{{NodeType: plan.Node_PROJECT, ProjectList: []*plan.Expr{expr}}},
			}}})
			require.Len(t, columns, 1)
			require.Equal(t, "added", columns[0].Name)
			require.Equal(t, int32(tc.resultOID), columns[0].Typ.Id)
			require.Equal(t, tc.resultFSP, columns[0].Typ.Scale)
			require.Equal(t, wantWidth, columns[0].Typ.Width)
		})
	}
}

func TestBindTemporalMetadataForTimestampArithmetic(t *testing.T) {
	ctx := context.Background()
	timestamp := &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{}}, Typ: plan.Type{
		Id: int32(types.T_timestamp), Scale: 6,
	}}
	convertTz, err := BindFuncExprImplByPlanExpr(ctx, "convert_tz", []*plan.Expr{
		timestamp, makeStringConst("+00:00"), makeStringConst("+08:00"),
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_datetime), convertTz.Typ.Id)
	require.Equal(t, int32(6), convertTz.Typ.Scale)
}
