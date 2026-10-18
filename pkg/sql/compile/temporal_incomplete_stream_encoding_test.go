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
	"testing"

	"github.com/google/uuid"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/vectorscan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestIncompleteTemporalExecutionStreamControls(t *testing.T) {
	for _, name := range []string{"typed week", "typed to_days", "literal"} {
		t.Run(name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			client.version = defines.MORPCVersion119
			fn := "week"
			if name == "typed to_days" {
				fn = "to_days"
			}
			scope := incompleteTemporalStreamScope(t, c, fn, types.T_date)
			if name == "literal" {
				scope.RootOp.(*projection.Projection).ProjectList = []*planpb.Expr{plan2.MakePlan2StringConstExprWithType("2001-00-00")}
			}
			stream := &fakeStreamSender{}
			installIncompleteTemporalTestStream(t, c, stream)
			require.NoError(t, scope.RemoteRun(c))
			require.Len(t, stream.sent, 1)
			require.Equal(t, pipeline.Method_PipelineMessage, stream.sent[0].(*pipeline.Message).GetCmd(), "unaffected projections must not add a handshake")
		})
	}
}

func TestIncompleteTemporalExecutionStreamNestedAndFolded(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	client.version = defines.MORPCVersion119
	scope := incompleteTemporalStreamScope(t, c, "week", types.T_varchar)
	expr := scope.RootOp.(*projection.Projection).ProjectList[0]
	mode := makeTestVarExprWithType("week_mode", types.T_int64.ToType())
	mode.GetV().System = false
	expr.GetF().Args[1] = mode
	c.proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) { return int64(3), nil })
	// Only the nested fragment contains the temporal expression. The floor must
	// follow the final, folded wire tree rather than the root's operator list.
	outer := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: scope.NodeInfo, PreScopes: []*Scope{scope}}
	var required int64
	data, _, _, folded, err := prepareRemoteRunSendingDataWithProtocol("", outer, c.proc, nil, uuid.Nil, &required)
	require.NoError(t, err)
	require.True(t, folded)
	require.Equal(t, defines.MORPCVersion119, required)
	var wire pipeline.Pipeline
	require.NoError(t, wire.Unmarshal(data))
	require.Len(t, wire.Children, 1)
	require.Nil(t, wire.Children[0].InstructionList[0].ProjectList[0].GetF().Args[1].GetV())
	require.NotNil(t, expr.GetF().Args[1].GetV(), "encoding must preserve the reusable source plan")
}

func TestIncompleteTemporalExecutionStreamComposesWithVector(t *testing.T) {
	c, client := vectorPlacementCompile(t, engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}})
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion119)
	client.version = defines.MORPCVersion119
	scopes, err := c.compileIndexSearchScan(vectorPlacementNode())
	require.NoError(t, err)
	t.Cleanup(func() { ReleaseScopes(scopes) })
	var required int64
	_, err = encodeRemoteScopeWithProtocol(scopes[0], c.proc, &required)
	require.NoError(t, err)
	require.Equal(t, vectorscan.IndexSearchScanProtocolVersion, required)
	combined := *scopes[0]
	temporal := incompleteTemporalStreamScope(t, c, "week", types.T_varchar)
	temporal.RootOp.GetOperatorBase().SetChildren([]vm.Operator{combined.RootOp})
	combined.RootOp = temporal.RootOp
	_, err = encodeRemoteScopeWithProtocol(&combined, c.proc, &required)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion119, required, "temporal floor must raise, not replace or lose, the vector requirement")
}
