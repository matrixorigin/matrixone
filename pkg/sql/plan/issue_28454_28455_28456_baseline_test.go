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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
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
	require.Equal(t, []int32{0}, PreparedPlanNumericFallbackParamPositions(prepared.GetDcl().GetPrepare().Plan))
	for _, tc := range []struct {
		name      string
		param     ParamValue
		wantChild types.T
		wantValue string
	}{
		{
			name: "decimal source",
			param: ParamValue{
				Value: "65.5", SourceType: types.New(types.T_decimal64, 3, 1), HasSourceType: true,
			},
			wantChild: types.T_decimal64,
			wantValue: "B",
		},
		{
			name: "string numeric source",
			param: ParamValue{
				Value: "65.5", SourceType: types.T_varchar.ToType(), HasSourceType: true,
			},
			wantChild: types.T_varchar,
			wantValue: "A",
		},
		{
			name: "string suffix source",
			param: ParamValue{
				Value: "65.5xyz", SourceType: types.T_varchar.ToType(), HasSourceType: true,
			},
			wantChild: types.T_varchar,
			wantValue: "A",
		},
		{
			name: "boolean source",
			param: ParamValue{
				Value: "1", SourceType: types.T_bool.ToType(), HasSourceType: true,
			},
			wantChild: types.T_bool,
			wantValue: "\u0001",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filled, specialized, fillErr := FillValuesOfParamsInPlanWithSpecialization(context.Background(),
				prepared.GetDcl().GetPrepare().Plan, []any{tc.param})
			require.NoError(t, fillErr)
			filledChar := findPlanFunctionExpr(filled, "char")
			require.NotNil(t, filledChar)
			require.True(t, specialized, "CHAR must be rebound for source domain %v", tc.param.SourceType)
			assertPreparedCharIntegerSource(t, filledChar, tc.wantChild, tc.wantValue)
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
	assertPreparedCharIntegerSource(t, filledChar, types.T_varchar, "\x00")

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
		name           string
		value          string
		hasRuntimeType bool
		wantValue      string
	}{
		{name: "no numeric prefix", value: "abc", wantValue: "\x00"},
		{name: "numeric suffix", value: "65.5xyz", wantValue: "A"},
		{name: "complete decimal", value: "65.5", wantValue: "A"},
		{name: "complete exponent", value: "64.5e0", wantValue: "@"},
		{name: "complete decimal with text metadata", value: "65.5", hasRuntimeType: true, wantValue: "A"},
		{name: "unsigned text", value: "18446744073709551615", wantValue: "\xff\xff\xff\xff"},
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
			assertPreparedCharIntegerSource(t, charExpr, types.T_text, tc.wantValue)
		})
	}
}

func assertPreparedCharIntegerSource(t *testing.T, expr *Expr, source types.T, want string) {
	t.Helper()
	logical := expr.GetF().Args[0]
	canonical := types.T_int64
	if source.IsUnsignedInt() || source == types.T_bit || source.IsMySQLString() {
		canonical = types.T_uint64
	}
	require.Equal(t, canonical, types.T(logical.Typ.Id), "numeric source domains must not collapse into a unified bit domain")
	require.True(t, isIntegerArgumentCast(logical))
	require.Equal(t, source, types.T(logical.GetF().Args[0].Typ.Id))
	proc := testutil.NewProcess(t)
	result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
	require.NoError(t, err)
	defer free()
	require.Equal(t, want, result.GetStringAt(0))
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
