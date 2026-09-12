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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// TestIssue28454To28456CharPreparedBinding covers the prepared CHAR planner
// boundary shared by #28454 and #28456. Direct string overload behavior is
// checked separately in the function package; this test verifies that a
// prepared marker gets a numeric context and that execution-time source
// metadata is allowed to rebind it.
func TestIssue28454To28456CharPreparedBinding(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare issue_28454 from 'select char(?)'")
	require.NoError(t, err)
	charExpr := findPlanFunctionExpr(prepared.GetDcl().GetPrepare().Plan, "char")
	require.NotNil(t, charExpr)
	require.Len(t, charExpr.GetF().Args, 1)
	require.True(t, PreparedPlanNeedsRuntimeSpecialization(prepared.GetDcl().GetPrepare().Plan))
	for _, tc := range []struct {
		name             string
		param            ParamValue
		wantChild        types.T
		wantStringSource bool
	}{
		{
			name: "decimal source",
			param: ParamValue{
				Value: "65.5", SourceType: types.New(types.T_decimal64, 3, 1), HasSourceType: true,
			},
			wantChild: types.T_decimal64,
		},
		{
			name: "string numeric source",
			param: ParamValue{
				Value: "65.5", SourceType: types.T_varchar.ToType(), HasSourceType: true,
			},
			wantChild: types.T_decimal64,
		},
		{
			name: "string suffix source",
			param: ParamValue{
				Value: "65.5xyz", SourceType: types.T_varchar.ToType(), HasSourceType: true,
			},
			wantChild:        types.T_varchar,
			wantStringSource: true,
		},
		{
			name: "boolean source",
			param: ParamValue{
				Value: "1", SourceType: types.T_bool.ToType(), HasSourceType: true,
			},
			wantChild: types.T_bool,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filled, specialized, fillErr := FillValuesOfParamsInPlanWithSpecialization(context.Background(),
				prepared.GetDcl().GetPrepare().Plan, []any{tc.param})
			require.NoError(t, fillErr)
			filledChar := findPlanFunctionExpr(filled, "char")
			require.NotNil(t, filledChar)
			require.True(t, specialized, "CHAR must be rebound for source domain %v", tc.param.SourceType)
			if tc.wantStringSource {
				stringArg := filledChar.GetF().Args[0]
				require.Equal(t, tc.wantChild, types.T(stringArg.Typ.Id))
				stringLiteral := stringArg.GetLit()
				if stringLiteral == nil && stringArg.GetF() != nil && len(stringArg.GetF().Args) > 0 {
					stringLiteral = stringArg.GetF().Args[0].GetLit()
				}
				require.NotNil(t, stringLiteral, filledChar.String())
				require.Equal(t, "65.5xyz", stringLiteral.GetSval(), filledChar.String())
				return
			}
			require.Equal(t, types.T_int64, types.T(filledChar.GetF().Args[0].Typ.Id))
			cast := filledChar.GetF().Args[0].GetF()
			require.NotNil(t, cast)
			require.Equal(t, "cast", cast.GetFunc().GetObjName())
			require.Len(t, cast.Args, 2)
			child := cast.Args[0]
			if tc.wantChild == types.T_varchar {
				// SQL string variables use the approximate numeric-prefix source
				// before CHAR's numeric rounding cast.
				require.NotNil(t, child.GetF())
				require.Equal(t, "cast", child.GetF().GetFunc().GetObjName())
				require.Equal(t, types.T_float64, types.T(child.Typ.Id))
				child = child.GetF().Args[0]
			}
			require.Equal(t, tc.wantChild, types.T(child.Typ.Id), filledChar.String())
		})
	}

	// An invalid numeric prefix is not a cast error for CHAR. It follows the
	// ordinary string-prefix path and produces the byte 00, matching MySQL.
	filled, specialized, fillErr := FillValuesOfParamsInPlanWithSpecialization(
		context.Background(), prepared.GetDcl().GetPrepare().Plan, []any{ParamValue{
			Value: "abc", SourceType: types.T_varchar.ToType(), HasSourceType: true,
		}},
	)
	require.NoError(t, fillErr)
	require.True(t, specialized)
	filledChar := findPlanFunctionExpr(filled, "char")
	require.NotNil(t, filledChar)
	stringArg := filledChar.GetF().Args[0]
	require.Equal(t, types.T_varchar, types.T(stringArg.Typ.Id))
	stringLiteral := stringArg.GetLit()
	if stringLiteral == nil && stringArg.GetF() != nil && len(stringArg.GetF().Args) > 0 {
		stringLiteral = stringArg.GetF().Args[0].GetLit()
	}
	require.NotNil(t, stringLiteral)
	require.Equal(t, "abc", stringLiteral.GetSval())

	for _, input := range []types.Type{
		types.T_varchar.ToType(), types.T_int64.ToType(), types.T_bool.ToType(),
	} {
		_, resolveErr := function.GetFunctionByName(t.Context(), "char", []types.Type{input})
		if input.Oid == types.T_bool {
			require.NoError(t, resolveErr)
			continue
		}
		require.NoError(t, resolveErr, input.Oid.String())
	}
}

func TestIssue28454CharPreparedCOMStmtTextKeepsStringSemantics(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare issue_28454_com_stmt from 'select char(?)'")
	require.NoError(t, err)
	preparePlan := prepared.GetDcl().GetPrepare().Plan

	for _, tc := range []struct {
		name             string
		value            string
		hasRuntimeType   bool
		wantStringSource bool
	}{
		{name: "no numeric prefix", value: "abc", wantStringSource: true},
		{name: "numeric suffix", value: "65.5xyz", wantStringSource: true},
		{name: "complete decimal", value: "65.5"},
		{name: "complete exponent", value: "64.5e0"},
		{name: "complete decimal with text metadata", value: "65.5", hasRuntimeType: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			param := ParamValue{
				Value: tc.value, IsBinaryProtocol: true,
			}
			if tc.hasRuntimeType {
				param.RuntimeType = types.T_text.ToType()
				param.HasRuntimeType = true
			}
			filled, specialized, fillErr := FillValuesOfParamsInPlanWithSpecialization(
				context.Background(), preparePlan, []any{param})
			require.NoError(t, fillErr)
			require.True(t, specialized)
			charExpr := findPlanFunctionExpr(filled, "char")
			require.NotNil(t, charExpr)
			arg := charExpr.GetF().Args[0]
			if tc.wantStringSource {
				require.Equal(t, types.T_varchar, types.T(arg.Typ.Id), charExpr.String())
				literal := arg.GetLit()
				if literal == nil && arg.GetF() != nil && len(arg.GetF().Args) > 0 {
					literal = arg.GetF().Args[0].GetLit()
				}
				require.NotNil(t, literal, charExpr.String())
				require.Equal(t, tc.value, literal.GetSval())
				return
			}
			require.Equal(t, types.T_int64, types.T(arg.Typ.Id), charExpr.String())
			cast := arg.GetF()
			require.NotNil(t, cast, charExpr.String())
			require.Len(t, cast.Args, 2, charExpr.String())
			require.True(t, types.T(cast.Args[0].Typ.Id).IsDecimal(), charExpr.String())
		})
	}
}

func TestIssue28454CharPreparedNumericSourceRetainsRuntimeProvenance(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare issue_28454_retain from 'select char(?)'")
	require.NoError(t, err)

	filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(
		context.Background(), prepared.GetDcl().GetPrepare().Plan, []any{ParamValue{
			Value: "65.5", SourceType: types.T_varchar.ToType(), HasSourceType: true,
			RetainParamRef: true,
		}})
	require.NoError(t, err)
	require.True(t, specialized)

	charExpr := findPlanFunctionExpr(filled, "char")
	require.NotNil(t, charExpr)
	var retained bool
	require.NoError(t, planpb.VisitExprTree(charExpr, func(expr *planpb.Expr) error {
		literal := expr.GetLit()
		if literal != nil && literal.Src != nil && literal.Src.GetP() != nil {
			retained = true
			require.Equal(t, int32(0), literal.Src.GetP().Pos)
		}
		return nil
	}))
	require.True(t, retained, charExpr.String())

	require.NoError(t, RestorePreparedRuntimeParamRefs(context.Background(), filled))
	charExpr = findPlanFunctionExpr(filled, "char")
	require.NotNil(t, charExpr)
	var restored bool
	require.NoError(t, planpb.VisitExprTree(charExpr, func(expr *planpb.Expr) error {
		if expr.GetP() != nil && expr.GetP().Pos == 0 {
			restored = true
		}
		return nil
	}))
	require.True(t, restored, charExpr.String())
}
