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

func TestPreparedCompositeTemporalRangeDoesNotUseNumericPrefix(t *testing.T) {
	ctx := context.Background()

	for _, temporalType := range []types.T{types.T_date, types.T_datetime, types.T_timestamp} {
		t.Run(temporalType.String(), func(t *testing.T) {
			param := func(pos int32) *planpb.Expr {
				return &planpb.Expr{
					Typ:  planpb.Type{Id: int32(types.T_text)},
					Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}},
				}
			}
			castParam := func(pos int32) *planpb.Expr {
				target := temporalType.ToType()
				cast, err := makePlan2CastExpr(ctx, param(pos), makePlan2Type(&target))
				require.NoError(t, err)
				return cast
			}

			columnType := temporalType.ToType()
			column := GetColExpr(makePlan2Type(&columnType), 0, 0)
			originalRange, err := BindFuncExprImplByPlanExpr(ctx, "in_range", []*planpb.Expr{
				column, castParam(0), castParam(1), MakePlan2Uint8ConstExprWithType(0),
			})
			require.NoError(t, err)

			lower, err := BindFuncExprImplByPlanExpr(ctx, "serial", []*planpb.Expr{castParam(0)})
			require.NoError(t, err)
			upper, err := BindFuncExprImplByPlanExpr(ctx, "serial", []*planpb.Expr{castParam(1)})
			require.NoError(t, err)
			serializedRange, err := BindFuncExprImplByPlanExpr(ctx, "prefix_in_range", []*planpb.Expr{
				GetColExpr(planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}, 0, 1),
				lower, upper, MakePlan2Uint8ConstExprWithType(0),
			})
			require.NoError(t, err)

			prepared := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
				StmtType: planpb.Query_SELECT,
				Steps:    []int32{0},
				Nodes: []*planpb.Node{{
					NodeType:        planpb.Node_TABLE_SCAN,
					FilterList:      []*planpb.Expr{originalRange},
					BlockFilterList: []*planpb.Expr{serializedRange},
				}},
			}}}
			for _, test := range []struct {
				name  string
				lower any
				upper any
			}{
				{name: "valid strings", lower: "2026-05-01 08:55:23", upper: "2026-05-19 04:14:00"},
				{name: "invalid string", lower: "not-a-temporal-value", upper: "2026-05-19 04:14:00"},
				{name: "null", lower: nil, upper: "2026-05-19 04:14:00"},
			} {
				t.Run(test.name, func(t *testing.T) {
					values := []any{
						ParamValue{
							Value: test.lower, IsBinaryProtocol: true,
							PrepareParamKind: types.StringConversionDecimal, EnableNumericPrefix: true,
						},
						ParamValue{
							Value: test.upper, IsBinaryProtocol: true,
							PrepareParamKind: types.StringConversionDecimal, EnableNumericPrefix: true,
						},
					}

					require.False(t, PreparedPlanNeedsNumericPrefixSpecialization(prepared, values), prepared.String())
					filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(ctx, prepared, values)
					require.NoError(t, err)
					require.False(t, specialized, filled.String())

					args := filled.GetQuery().Nodes[0].FilterList[0].GetF().Args
					require.Equal(t, temporalType, types.T(args[0].Typ.Id), filled.String())
					require.Equal(t, temporalType, types.T(args[1].Typ.Id), filled.String())
					require.Equal(t, temporalType, types.T(args[2].Typ.Id), filled.String())

					serializedArgs := filled.GetQuery().Nodes[0].BlockFilterList[0].GetF().Args
					for _, bound := range serializedArgs[1:3] {
						serial := bound.GetF()
						require.NotNil(t, serial, filled.String())
						require.Equal(t, "serial", serial.Func.GetObjName())
						require.Equal(t, temporalType, types.T(serial.Args[0].Typ.Id), filled.String())
					}
				})
			}
		})
	}
}

func TestPreparedCompositeNumericRangeStillUsesNumericPrefix(t *testing.T) {
	ctx := context.Background()
	param := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_text)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{}},
	}
	target := types.New(types.T_decimal64, 10, 2)
	cast, err := makePlan2CastExpr(ctx, param, makePlan2Type(&target))
	require.NoError(t, err)
	serialized, err := BindFuncExprImplByPlanExpr(ctx, "serial", []*planpb.Expr{cast})
	require.NoError(t, err)
	prefix, err := BindFuncExprImplByPlanExpr(ctx, "prefix_eq", []*planpb.Expr{
		GetColExpr(planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}, 0, 0),
		serialized,
	})
	require.NoError(t, err)
	prepared := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*planpb.Node{{
			NodeType:        planpb.Node_TABLE_SCAN,
			BlockFilterList: []*planpb.Expr{prefix},
		}},
	}}}

	require.True(t, PreparedPlanNeedsNumericPrefixSpecialization(prepared, []any{ParamValue{
		Value: "12.34", PrepareParamKind: types.StringConversionDecimal, EnableNumericPrefix: true,
	}}))
}
