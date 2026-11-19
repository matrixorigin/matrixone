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

func TestPartitionRouteRequiresProtocol(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion134)
	p := &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}, Children: []*pipeline.Pipeline{{
		InstructionList: []*pipeline.Instruction{{PreInsert: &pipeline.PreInsert{PreserveInput: true}},
			{MultiUpdate: &pipeline.MultiUpdate{UpdateCtxList: []*plan.UpdateCtx{{PartitionIndexCtx: &plan.PartitionIndexCtx{}}}}}},
	}}}
	client.version = defines.MORPCVersion134
	require.Equal(t, defines.MORPCVersion135, minimumRemotePartitionUpdateProtocol(&pipeline.Pipeline{
		InstructionList: []*pipeline.Instruction{{PreInsert: &pipeline.PreInsert{PreserveInput: true}}},
	}))
	require.Equal(t, defines.MORPCVersion135, minimumRemotePartitionUpdateProtocol(&pipeline.Pipeline{
		InstructionList: []*pipeline.Instruction{{MultiUpdate: &pipeline.MultiUpdate{
			UpdateCtxList: []*plan.UpdateCtx{{PartitionIndexCtx: &plan.PartitionIndexCtx{}}},
		}}},
	}))
	require.Equal(t, defines.MORPCVersion135, minimumRemotePartitionUpdateProtocol(p))
	require.ErrorContains(t, validateRemotePartitionUpdatePipelineProtocol(c.proc, p), "version 135")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion135)
	var required int64
	calls := client.calls
	required = defines.MORPCVersion134
	require.ErrorContains(t, validatePartitionUpdateDestinationWithProtocol(c.proc, p, &required), "version 135")
	require.Equal(t, defines.MORPCVersion135, required)
	require.Equal(t, calls+1, client.calls, "the actual remote destination must be probed")

	client.version = defines.MORPCVersion135
	require.NoError(t, validateRemotePartitionUpdatePipelineProtocol(c.proc, p))
	required = defines.MORPCVersion134
	require.NoError(t, validatePartitionUpdateDestinationWithProtocol(c.proc, p, &required))
	require.Equal(t, defines.MORPCVersion135, required, "the stream floor must include routing fields")
	require.NoError(t, validateRemoteStatementLastInsertIDPipelineProtocol(c.proc, p))

	// Pipelines without the new fields retain their existing protocol floor.
	plain := &pipeline.Pipeline{Node: p.Node, InstructionList: []*pipeline.Instruction{{PreInsert: &pipeline.PreInsert{}}}}
	require.Zero(t, minimumRemotePartitionUpdateProtocol(plain))
	required = defines.MORPCVersion134
	require.NoError(t, validatePartitionUpdateDestinationWithProtocol(c.proc, plain, &required))
	require.Equal(t, defines.MORPCVersion134, required)

	// Unrelated existing feature gates continue to reject unsupported peers.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion97)
	client.version = defines.MORPCVersion97 - 1
	required = 0
	features := plan.RemoteExpressionFeatures{DecimalDivisionSemantics: true}
	require.Error(t, validateRemoteExpressionDestinationWithProtocol(c.proc, p, features, &required))
	client.version = defines.MORPCVersion97
	require.NoError(t, validateRemoteExpressionDestinationWithProtocol(c.proc, p, features, &required))
	require.Equal(t, defines.MORPCVersion97, required)
}
