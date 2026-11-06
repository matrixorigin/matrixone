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

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestPartitionRouteDoesNotAddProtocolFloor(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion134)
	p := &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}, Children: []*pipeline.Pipeline{{
		InstructionList: []*pipeline.Instruction{{PreInsert: &pipeline.PreInsert{PreserveInput: true}},
			{MultiUpdate: &pipeline.MultiUpdate{UpdateCtxList: []*plan.UpdateCtx{{PartitionIndexCtx: &plan.PartitionIndexCtx{}}}}}},
	}}}
	client.version = defines.MORPCVersion134
	var required int64
	calls := client.calls
	require.NoError(t, validateRemoteExpressionDestinationWithProtocol(c.proc, p, plan.RemoteExpressionFeatures{}, &required))
	require.Zero(t, required)
	require.Equal(t, calls, client.calls, "routing alone must not initiate a capability probe")
	require.NoError(t, validateRemoteStatementLastInsertIDPipelineProtocol(c.proc, p))

	// Unrelated existing feature gates continue to reject unsupported peers.
	client.version = defines.MORPCVersion97 - 1
	features := plan.RemoteExpressionFeatures{DecimalDivisionSemantics: true}
	require.Error(t, validateRemoteExpressionDestinationWithProtocol(c.proc, p, features, &required))
	client.version = defines.MORPCVersion97
	require.NoError(t, validateRemoteExpressionDestinationWithProtocol(c.proc, p, features, &required))
	require.Equal(t, defines.MORPCVersion97, required)
}
