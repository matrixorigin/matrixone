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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestPersistedBinaryAssignmentProtocol(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   int32
		typ  planpb.Type
		want bool
	}{
		{"assignment", function.CAST_ASSIGN, planpb.Type{Id: int32(types.T_binary), Width: 4}, true},
		{"IGNORE", function.CAST_IGNORE, planpb.Type{Id: int32(types.T_varbinary), Width: 2}, true},
		{"strict", function.CAST_STRICT, planpb.Type{Id: int32(types.T_binary), Width: 0}, true},
		{"legacy IGNORE fallback", function.CAST, planpb.Type{Id: int32(types.T_varbinary), Width: 2}, true},
		{"opaque ABI", function.CAST, planpb.Type{Id: int32(types.T_varbinary), Width: 0}, false},
		{"explicit BINARY", function.CAST, planpb.Type{Id: int32(types.T_binary), Width: 4, Scale: -1}, false},
		{"ordinary VARCHAR", function.CAST_ASSIGN, planpb.Type{Id: int32(types.T_varchar), Width: 4}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := &planpb.Expr{Typ: tc.typ, Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(tc.id, 0)}, Args: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}, {Typ: tc.typ, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}}}}}}
			// A wider output annotation cannot erase the target argument's contract.
			expr.Typ.Width = 100
			owner := &planpb.TableDef{Cols: []*planpb.ColDef{{Default: &planpb.Default{Expr: expr}}}}
			data, err := owner.Marshal()
			require.NoError(t, err)
			var decoded planpb.TableDef
			require.NoError(t, decoded.Unmarshal(data))
			features, err := planpb.RequiredRemoteExpressionFeatures(&decoded)
			require.NoError(t, err)
			require.Equal(t, tc.want, features.BinaryAssignmentSemantics)
			required, err := RequiredPersistedExpressionProtocolVersion(&decoded)
			require.NoError(t, err)
			if tc.want {
				require.Equal(t, defines.MORPCVersion130, required)
			} else {
				require.Less(t, required, defines.MORPCVersion130)
			}
		})
	}
}
