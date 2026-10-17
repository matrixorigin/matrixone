// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFunctionalJSONMetadataOnlyDestinationAndReceiver(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	table := &plan.TableDef{Indexes: []*plan.IndexDef{{IndexName: "j", IndexAlgoParams: `{"functional_json_semantics":"1"}`}}}
	qry := &plan.Query{Nodes: []*plan.Node{{TableDef: table}}, Steps: []int32{0}}
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001"}}
	client.version = defines.MORPCVersion117
	c.execType = plan2.ExecTypeAP_MULTICN
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	client.version = defines.MORPCVersion118
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001"}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	p := &pipeline.Pipeline{DataSource: &pipeline.Source{TableDef: table}}
	features, err := pipelineExpressionFeatures(p)
	require.NoError(t, err)
	require.True(t, features.JSONFunctionalIndexSemantics)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion117)
	require.Error(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion118)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
	op := projection.NewArgument()
	defer op.Release()
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op, DataSource: &Source{TableDef: table}}
	client.version = defines.MORPCVersion117
	_, err = encodeRemoteScope(scope, c.proc)
	require.Error(t, err)
	client.version = defines.MORPCVersion118
	minimum := int64(0)
	data, err := encodeRemoteScopeWithVectorProtocol(scope, c.proc, &minimum)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion118, minimum)
	var decoded pipeline.Pipeline
	require.NoError(t, decoded.Unmarshal(data))
	features, err = pipelineExpressionFeatures(&decoded)
	require.NoError(t, err)
	require.True(t, features.JSONFunctionalIndexSemantics)
	scope.NodeInfo.Addr = "stale:6001"
	_, err = encodeRemoteScope(scope, c.proc)
	require.Error(t, err)
}

func TestFunctionalJSONRechecksActualStream(t *testing.T) {
	for _, version := range []int64{defines.MORPCVersion116, defines.MORPCVersion117, defines.MORPCVersion118} {
		stream := &fakeStreamSender{}
		replies := make(chan morpc.Message, 1)
		replies <- &pipeline.Message{Id: 0, Cmd: pipeline.Method_PipelineProtocolCheck, Sid: pipeline.Status_Last, ProtocolVersion: version}
		sender := &messageSenderOnClient{ctx: context.Background(), streamSender: stream, receiveCh: replies}
		err := sender.confirmProtocolOnStream(defines.MORPCVersion118)
		if version < defines.MORPCVersion118 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}
}

// A combined plan must preserve the highest capability rather than letting a
// subsequently visited inherited consumer overwrite its metadata-only floor.
func TestFunctionalJSONComposesInheritedFloors(t *testing.T) {
	features := plan.RemoteExpressionFeatures{JSONFunctionalIndexSemantics: true, ODKUStatementResult: true, SessionLastInsertIDBaseline: true, JSONMinMaxTypedComparison: true, JSONOpaqueTextContracts: true, JSONStringConsumerOverload: true, DateNumericResultContracts: true}
	require.Equal(t, defines.MORPCVersion118, remoteExpressionProtocolVersion(features))
	table := &plan.TableDef{Indexes: []*plan.IndexDef{{IndexName: "j", IndexAlgoParams: `{"functional_json_semantics":"1"}`}}}
	floor, err := plan2.RequiredPersistedExpressionProtocolVersion(table)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion118, floor)
	features.JSONFunctionalIndexSemantics = false
	require.Equal(t, defines.MORPCVersion118, remoteExpressionProtocolVersion(features))
}

func TestFunctionalJSONMetadataOnlyLocalCompileRunAdmission(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	queryPlan := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{TableDef: &plan.TableDef{Indexes: []*plan.IndexDef{{IndexName: "j", IndexAlgoParams: `{"functional_json_semantics":"1"}`}}}}}}}}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion117)
	cached := &Compile{proc: c.proc, pn: queryPlan}
	require.ErrorContains(t, cached.Compile(context.Background(), queryPlan, nil), "protocol version 118")
	_, err := cached.Run(0)
	require.ErrorContains(t, err, "protocol version 118")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion118)
	require.NoError(t, validateJSONStringConsumerProtocol(c.proc, queryPlan))
	queryPlan.GetQuery().Nodes[0].TableDef.Indexes[0].IndexAlgoParams = ""
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion107)
	require.NoError(t, validateJSONStringConsumerProtocol(c.proc, queryPlan), "ordinary functional index retains its existing admission")
}
