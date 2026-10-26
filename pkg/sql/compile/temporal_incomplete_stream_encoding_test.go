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
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
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
	for _, tc := range []struct {
		name     string
		function string
		floor    int64
	}{
		{"typed week", "week", defines.MORPCVersion112},
		{"typed to_days", "to_days", defines.MORPCVersion98},
		{"literal", "week", 0},
	} {
		versions := []int64{tc.floor}
		if tc.floor > 0 {
			versions = append(versions, tc.floor-1)
		}
		for _, version := range versions {
			t.Run(fmt.Sprintf("%s/%d", tc.name, version), func(t *testing.T) {
				c, client := expressionProtocolTestCompile(t)
				client.version = defines.MORPCVersion130
				scope := incompleteTemporalStreamScope(t, c, tc.function, types.T_date)
				if tc.name == "literal" {
					scope.RootOp.(*projection.Projection).ProjectList = []*planpb.Expr{plan2.MakePlan2StringConstExprWithType("2001-00-00")}
				}
				// Typed DATE inputs do not acquire the incomplete-string floor130,
				// but retain their existing expression floors on the actual stream.
				checks, payloads, closed := 0, 0, 0
				replies := make(chan morpc.Message, 1)
				stream := mock_morpc.NewMockStream(gomock.NewController(t))
				stream.EXPECT().ID().Return(uint64(17)).AnyTimes()
				stream.EXPECT().Receive().Return(replies, nil)
				stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, m morpc.Message) error {
					message := m.(*pipeline.Message)
					switch message.GetCmd() {
					case pipeline.Method_PipelineProtocolCheck:
						checks++
						require.Positive(t, tc.floor)
						require.Zero(t, payloads)
						require.Equal(t, tc.floor, message.ProtocolVersion)
						replies <- &pipeline.Message{Id: 17, Cmd: pipeline.Method_PipelineProtocolCheck, Sid: pipeline.Status_Last, ProtocolVersion: version}
					case pipeline.Method_PipelineMessage:
						payloads++
						if tc.floor > 0 {
							require.Equal(t, 1, checks)
						}
						replies <- &pipeline.Message{Id: 17, Sid: pipeline.Status_MessageEnd}
					default:
						t.Fatalf("unexpected command %v", message.GetCmd())
					}
					return nil
				}).AnyTimes()
				stream.EXPECT().Close(true).DoAndReturn(func(bool) error { closed++; return nil })
				installIncompleteTemporalTestStream(t, c, stream)
				err := scope.RemoteRun(c)
				if version < tc.floor {
					require.ErrorContains(t, err, fmt.Sprintf("version %d", tc.floor))
					require.Zero(t, payloads)
				} else {
					require.NoError(t, err)
					require.Equal(t, 1, payloads)
				}
				if tc.floor == 0 {
					require.Zero(t, checks, "a literal must not add a handshake")
				} else {
					require.Equal(t, 1, checks)
				}
				require.Equal(t, 1, closed)
			})
		}
	}
}

func TestIncompleteTemporalExecutionStreamNestedAndFolded(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	client.version = defines.MORPCVersion130
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
	require.Equal(t, defines.MORPCVersion130, required)
	var wire pipeline.Pipeline
	require.NoError(t, wire.Unmarshal(data))
	require.Len(t, wire.Children, 1)
	require.Nil(t, wire.Children[0].InstructionList[0].ProjectList[0].GetF().Args[1].GetV())
	require.NotNil(t, expr.GetF().Args[1].GetV(), "encoding must preserve the reusable source plan")
}

func TestIncompleteTemporalExecutionStreamComposesWithVector(t *testing.T) {
	c, client := vectorPlacementCompile(t, engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}})
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion130)
	client.version = defines.MORPCVersion130
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
	require.Equal(t, defines.MORPCVersion130, required, "temporal floor must raise, not replace or lose, the vector requirement")
}
