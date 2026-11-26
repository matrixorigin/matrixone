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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func floatConversionProtocolExpr(id int32, source, target types.T) *planpb.Expr {
	return &planpb.Expr{Typ: target.ToType().PlanType(), Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(id, 0)},
		Args: []*planpb.Expr{{Typ: source.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}},
	}}}
}

func TestFloatConversionFeatureBoundaries(t *testing.T) {
	for _, tc := range []struct {
		id             int32
		source, target types.T
		want           bool
	}{
		{function.CAST, types.T_varchar, types.T_float64, true},
		{function.CAST_ASSIGN, types.T_text, types.T_float32, true},
		{function.CAST_STRICT, types.T_float64, types.T_decimal128, true},
		{function.CAST_IGNORE, types.T_float32, types.T_decimal64, true},
		{function.CAST, types.T_json, types.T_decimal64, true},
		{function.CAST, types.T_float64, types.T_decimal256, false},
		{function.CAST, types.T_int64, types.T_decimal128, false},
		{function.CAST, types.T_varchar, types.T_int64, false},
		{function.JSON_AGG_TO_DOUBLE, types.T_json, types.T_float64, true},
	} {
		expr := floatConversionProtocolExpr(tc.id, tc.source, tc.target)
		features, err := planpb.RequiredRemoteExpressionFeatures(expr)
		require.NoError(t, err)
		require.Equal(t, tc.want, features.FloatConversionSemantics)
		floor, err := plan2.RequiredPersistedExpressionProtocolVersion(expr)
		require.NoError(t, err)
		if tc.want {
			require.Equal(t, defines.MORPCVersion132, floor)
		} else {
			require.Less(t, floor, defines.MORPCVersion132)
		}
	}
	// Independent catalog contracts cannot lower the newly required float floor.
	comparison := floatConversionProtocolExpr(function.EQUAL, types.T_json, types.T_bool)
	comparison.GetF().Args = append(comparison.GetF().Args, &planpb.Expr{Typ: types.T_int64.ToType().PlanType()})
	owner := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{
		comparison, floatConversionProtocolExpr(function.CAST, types.T_varchar, types.T_float64),
	}}}}
	combined, err := planpb.RequiredRemoteExpressionFeatures(owner)
	require.NoError(t, err)
	require.True(t, combined.JSONNumericComparisons)
	require.True(t, combined.FloatConversionSemantics)
	floor, err := plan2.RequiredPersistedExpressionProtocolVersion(owner)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion132, floor)
	typedParam := &planpb.Expr{Typ: types.T_float64.ToType().PlanType(), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
	features, err := planpb.RequiredRemoteExpressionFeatures(typedParam)
	require.NoError(t, err)
	require.True(t, features.FloatConversionSemantics)
}

func TestGenerateProcessHelperRejectsConflictingStatementIntent(t *testing.T) {
	value := &pipeline.ProcessInfo{StatementRuntimeSelect: true, StatementRuntimeDml: true, PrepareParams: pipeline.PrepareParamInfo{Length: 1, Data: []byte{0xff}}}
	data, err := value.Marshal()
	require.NoError(t, err)
	// Nil txn client and invalid parameter data prove classification rejection
	// precedes transaction reconstruction and parameter ownership allocation.
	_, err = generateProcessHelper(context.Background(), data, nil)
	require.ErrorContains(t, err, "conflicting SELECT and DML")
}
