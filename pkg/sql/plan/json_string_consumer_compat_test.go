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
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// A remote receiver must see a previously supported JSON text operation,
// whose existing protocol fence rejects receivers that cannot render it.
func TestJSONStringConsumersUseCompatibleRemoteExpression(t *testing.T) {
	for _, name := range []string{"md5", "sha", "sha1", "sha2", "compress", "quote", "soundex"} {
		t.Run(name, func(t *testing.T) {
			json := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_json)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
			args := []*planpb.Expr{json}
			if name == "sha2" {
				args = append(args, MakePlan2Int64ConstExprWithType(256))
			}
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
			require.NoError(t, err)
			features, err := planpb.RequiredRemoteExpressionFeatures(expr)
			require.NoError(t, err)
			require.True(t, features.JSONOpaqueTextContracts, "JSON rendering must retain the existing remote protocol fence")
			input := expr.GetF().Args[0].GetF()
			require.NotNil(t, input, "must not insert a bounded VARCHAR cast")
			require.Equal(t, "concat", input.Func.ObjName)
			encoded, err := expr.Marshal()
			require.NoError(t, err)
			var decoded planpb.Expr
			require.NoError(t, decoded.Unmarshal(encoded))
			features, err = planpb.RequiredRemoteExpressionFeatures(&decoded)
			require.NoError(t, err)
			require.True(t, features.JSONOpaqueTextContracts)
			floor, err := RequiredPersistedExpressionProtocolVersion(&decoded)
			require.NoError(t, err)
			require.Equal(t, defines.MORPCVersion129, floor)
		})
	}
}

func TestJSONStringConsumerCompatibilityTextControl(t *testing.T) {
	for _, name := range []string{"md5", "sha", "sha1", "sha2", "compress", "quote", "soundex"} {
		t.Run(name, func(t *testing.T) {
			text := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 30}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
			args := []*planpb.Expr{text}
			if name == "sha2" {
				args = append(args, MakePlan2Int64ConstExprWithType(256))
			}
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
			require.NoError(t, err)
			features, err := planpb.RequiredRemoteExpressionFeatures(expr)
			require.NoError(t, err)
			require.False(t, features.JSONOpaqueTextContracts)
		})
	}
}
