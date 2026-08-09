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

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestConstantFoldPreservesSerializedLiteralProvenance(t *testing.T) {
	proc := testutil.NewProcess(t)

	tests := []struct {
		name           string
		function       string
		inputType      types.Type
		input          *planpb.Literal
		wantNull       bool
		wantSerialized bool
	}{
		{
			name:           "serial",
			function:       function.SerialFunctionName,
			inputType:      types.T_bool.ToType(),
			input:          &planpb.Literal{Value: &planpb.Literal_Bval{Bval: true}},
			wantSerialized: true,
		},
		{
			name:           "serial null",
			function:       function.SerialFunctionName,
			inputType:      types.T_bool.ToType(),
			input:          &planpb.Literal{Isnull: true},
			wantNull:       true,
			wantSerialized: false,
		},
		{
			name:           "serial full",
			function:       function.SerialFullFunctionName,
			inputType:      types.T_bool.ToType(),
			input:          &planpb.Literal{Value: &planpb.Literal_Bval{Bval: true}},
			wantSerialized: true,
		},
		{
			name:           "serial full null",
			function:       function.SerialFullFunctionName,
			inputType:      types.T_bool.ToType(),
			input:          &planpb.Literal{Isnull: true},
			wantSerialized: true,
		},
		{
			name:           "ordinary string function control",
			function:       "lower",
			inputType:      types.T_varchar.ToType(),
			input:          &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "VISIBLE"}},
			wantSerialized: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registered, err := function.GetFunctionByName(context.Background(), test.function, []types.Type{test.inputType})
			require.NoError(t, err)

			expr := &planpb.Expr{
				Typ: planpb.Type{Id: int32(types.T_varchar)},
				Expr: &planpb.Expr_F{F: &planpb.Function{
					Func: &planpb.ObjectRef{Obj: registered.GetEncodedOverloadID(), ObjName: test.function},
					Args: []*planpb.Expr{{
						Typ:  planpb.Type{Id: int32(test.inputType.Oid)},
						Expr: &planpb.Expr_Lit{Lit: test.input},
					}},
				}},
			}

			folded, err := ConstantFold(batch.EmptyForConstFoldBatch, expr, proc, false, true)
			require.NoError(t, err)
			literal := folded.GetLit()
			require.NotNil(t, literal)
			require.Equal(t, test.wantNull, literal.GetIsnull())
			require.False(t, literal.GetIsBin(), "folding must not acquire SQL hex/bit semantics")
			require.Equal(t, test.wantSerialized, literal.GetIsSerialized())

			copied := DeepCopyExpr(folded)
			require.Equal(t, test.wantSerialized, copied.GetLit().GetIsSerialized())

			payload, err := proto.Marshal(folded)
			require.NoError(t, err)
			decoded := new(planpb.Expr)
			require.NoError(t, proto.Unmarshal(payload, decoded))
			require.Equal(t, test.wantSerialized, decoded.GetLit().GetIsSerialized())
		})
	}
}

func TestConstantFoldPreservesSerialCastSemantics(t *testing.T) {
	proc := testutil.NewProcess(t)
	registered, err := function.GetFunctionByName(
		context.Background(), function.SerialFunctionName, []types.Type{types.T_bool.ToType()},
	)
	require.NoError(t, err)

	serialExpr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{
				Obj:     registered.GetEncodedOverloadID(),
				ObjName: function.SerialFunctionName,
			},
			Args: []*planpb.Expr{{
				Typ:  planpb.Type{Id: int32(types.T_bool)},
				Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Bval{Bval: true}}},
			}},
		}},
	}
	castExpr, err := BindFuncExprImplByPlanExpr(context.Background(), "cast", []*planpb.Expr{
		serialExpr,
		{
			Typ:  planpb.Type{Id: int32(types.T_uint64)},
			Expr: &planpb.Expr_T{T: &planpb.TargetType{}},
		},
	})
	require.NoError(t, err)

	runtimeResult, runtimeFree, runtimeErr := colexec.GetReadonlyResultFromExpression(
		proc, DeepCopyExpr(castExpr), []*batch.Batch{batch.EmptyForConstFoldBatch},
	)
	if runtimeFree != nil {
		defer runtimeFree()
	}

	folded, foldErr := ConstantFold(
		batch.EmptyForConstFoldBatch, DeepCopyExpr(castExpr), proc, false, true,
	)
	require.Equal(t, runtimeErr != nil, foldErr != nil, "constant folding changed whether the expression fails")
	if runtimeErr != nil {
		return
	}

	foldedResult, foldedFree, err := colexec.GetReadonlyResultFromExpression(
		proc, folded, []*batch.Batch{batch.EmptyForConstFoldBatch},
	)
	require.NoError(t, err)
	defer foldedFree()
	require.Equal(
		t,
		rule.GetConstantValue(runtimeResult, false, 0),
		rule.GetConstantValue(foldedResult, false, 0),
		"constant folding changed the expression value",
	)
}
