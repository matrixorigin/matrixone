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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestPreparedUnnestArgumentDomains(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare unnest_args from 'select u.seq,u.path,u.value from unnest(?,?,?) u'")
	require.NoError(t, err)
	var args []*planpb.Expr
	for _, node := range prepared.GetDcl().GetPrepare().Plan.GetQuery().Nodes {
		if node.NodeType == planpb.Node_FUNCTION_SCAN && node.TableDef.GetTblFunc().GetName() == "unnest" {
			args = node.TblFuncExprList
			break
		}
	}
	require.Len(t, args, 3)
	for i, want := range []types.T{types.T_json, types.T_varchar, types.T_bool} {
		require.Equal(t, want, types.T(args[i].Typ.Id))
		if i == 1 {
			require.Equal(t, int32(types.MaxVarcharLen), args[i].Typ.Width)
		}
		require.Equal(t, "cast", args[i].GetF().GetFunc().GetObjName())
		require.NotNil(t, args[i].GetF().Args[0].GetP())
	}
}
