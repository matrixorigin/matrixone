// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNativePADMergeKeepsIndependentCapabilities(t *testing.T) {
	for _, tc := range []struct {
		name         string
		id, overload int64
		required     int64
	}{
		{"load_fk", 602, 0, defines.MORPCVersion125},
		{"view_definition", 603, 0, defines.MORPCVersion126},
		{"view_check", 604, 0, defines.MORPCVersion126},
		{"json_numeric", 546, 1, defines.MORPCVersion128},
		{"json_elt", 384, 1, defines.MORPCVersion129},
		{"json_opaque_text", 16, 0, defines.MORPCVersion129},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := &pb.Expr{Typ: types.T_bool.ToType().PlanType(), Expr: &pb.Expr_F{F: &pb.Function{Func: &pb.ObjectRef{Obj: tc.id<<32 | tc.overload}}}}
			if tc.id == 16 {
				expr.Typ = types.T_varchar.ToType().PlanType()
				expr.GetF().Args = []*pb.Expr{{Typ: types.T_json.ToType().PlanType()}}
			}
			owner := &pb.Node{DedupKeyExprs: []*pb.Expr{expr}}
			features, err := pb.RequiredExecutableRemoteExpressionFeatures(owner)
			require.NoError(t, err)
			require.True(t, features.Any())
			require.False(t, features.NativePADCollation)
			require.Equal(t, tc.required, remoteExpressionProtocolVersion(features))
			native := types.NewWithCharset(types.T_varchar, 8, 0, 3).PlanType()
			native.CollationVersion = 1
			owner.PreInsertUkCtx = &pb.PreInsertUkCtx{KeyDiagnosticExprs: []*pb.ExprList{{List: []*pb.Expr{{Typ: native}}}}}
			features, err = pb.RequiredExecutableRemoteExpressionFeatures(owner)
			require.NoError(t, err)
			require.True(t, features.NativePADCollation)
			require.Equal(t, defines.MORPCVersion130, remoteExpressionProtocolVersion(features))
		})
	}
}
