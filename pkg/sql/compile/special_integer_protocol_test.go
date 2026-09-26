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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestSpecialIntegerProtocolPlacementAndSend(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	// No private CAST: the stable function identity itself owns this capability.
	expr := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.MAKEDATE, function.MakeDateIntegerOverload)},
		Args: []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
			{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
		},
	}}}
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.SpecialIntegerConsumers)
	require.False(t, features.IntegerParameterCoercion)
	version, err := plan2.RequiredPersistedExpressionProtocolVersion(expr)
	require.NoError(t, err)
	require.Equal(t, int64(defines.MORPCVersion98), version)
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	place := func(version int64) {
		client.version = version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
		require.NoError(t, c.constrainIntegerArgumentWorkers(qry))
	}
	place(defines.MORPCVersion98 - 1)
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 98")
	place(defines.MORPCVersion98)
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	_, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	client.version = defines.MORPCVersion98 - 1
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 98")
	require.Equal(t, client.calls, client.releases)
	require.ErrorContains(t, validateIntegerArgumentDestination(c.proc, nil), "versioned remote destination")
	p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion98-1)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "version 98")
	data, err := p.Marshal()
	require.NoError(t, err)
	_, err = decodeScope(data, c.proc, true, nil)
	require.ErrorContains(t, err, "version 98")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion98)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
}
