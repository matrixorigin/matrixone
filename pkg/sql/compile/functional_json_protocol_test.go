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
	client.version = defines.MORPCVersion114
	c.execType = plan2.ExecTypeAP_MULTICN
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	client.version = defines.MORPCVersion115
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001"}}
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	p := &pipeline.Pipeline{DataSource: &pipeline.Source{TableDef: table}}
	features, err := pipelineExpressionFeatures(p)
	require.NoError(t, err)
	require.True(t, features.JSONFunctionalIndexSemantics)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion114)
	require.Error(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion115)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
	op := projection.NewArgument()
	defer op.Release()
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op, DataSource: &Source{TableDef: table}}
	client.version = defines.MORPCVersion114
	_, err = encodeRemoteScope(scope, c.proc)
	require.Error(t, err)
	client.version = defines.MORPCVersion115
	minimum := int64(0)
	data, err := encodeRemoteScopeWithVectorProtocol(scope, c.proc, &minimum)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion115, minimum)
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
	for _, version := range []int64{defines.MORPCVersion114, defines.MORPCVersion115} {
		stream := &fakeStreamSender{}
		replies := make(chan morpc.Message, 1)
		replies <- &pipeline.Message{Id: 0, Cmd: pipeline.Method_PipelineProtocolCheck, Sid: pipeline.Status_Last, ProtocolVersion: version}
		sender := &messageSenderOnClient{ctx: context.Background(), streamSender: stream, receiveCh: replies}
		err := sender.confirmProtocolOnStream(defines.MORPCVersion115)
		if version < defines.MORPCVersion115 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}
}
