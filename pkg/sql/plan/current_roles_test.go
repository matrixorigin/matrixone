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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestBuildCurrentRolesProtocolGate(t *testing.T) {
	mock := NewMockOptimizer(false)
	builder := NewQueryBuilder(planpb.Query_SELECT, mock.CurrentContext(), false, true)
	ctx := NewBindContext(builder, nil)
	proc := builder.compCtx.GetProcess()
	rt := runtime.ServiceRuntime(proc.GetService())
	original, hadOriginal := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadOriginal {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	tf := &tree.TableFunction{Func: &tree.FuncExpr{}}
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion34)
	_, err := builder.buildCurrentRoles(tf, ctx, nil, nil)
	require.ErrorContains(t, err, "protocol version 35")

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion35)
	nodeID, err := builder.buildCurrentRoles(tf, ctx, nil, nil)
	require.NoError(t, err)
	require.Equal(t, planpb.Node_FUNCTION_SCAN, builder.qry.Nodes[nodeID].NodeType)
	require.Equal(t, "role_id", builder.qry.Nodes[nodeID].TableDef.Cols[0].Name)

	tf.Func.Exprs = tree.Exprs{tree.NewNumVal(int64(1), "1", false, tree.P_int64)}
	_, err = builder.buildCurrentRoles(tf, ctx, nil, nil)
	require.ErrorContains(t, err, "invalid input args length")
}
