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
	"fmt"
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

func integerProtocolExpr(id int32) *planpb.Expr {
	source, target := types.T_float64, types.T_int64
	if id == function.TextIntegerBitsCastOverload {
		source, target = types.T_varchar, types.T_uint64
	}
	if id == function.TemporalIntegerArgumentCastOverload {
		source = types.T_time
	}
	typ := planpb.Type{Id: int32(target)}
	return &planpb.Expr{Typ: typ, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.CAST, id), ObjName: "cast"},
		Args: []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(source)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
			{Typ: typ, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}},
		},
	}}}
}

func TestIntegerArgumentProtocolBoundaries(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	require.Equal(t, int32(21), int32(function.CAST), "protobuf capability identity must track the registry")
	for id := int32(0); id <= function.TemporalIntegerArgumentCastOverload; id++ {
		t.Run(fmt.Sprint(id), func(t *testing.T) {
			expr := integerProtocolExpr(id)
			p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
			features, err := planpb.RequiredRemoteExpressionFeatures(p)
			require.NoError(t, err)
			require.Equal(t, id >= function.IntegerArgumentCastOverload, features.IntegerParameterCoercion)
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion75)
			err = validateRemoteExpressionPipelineProtocol(c.proc, p)
			if !features.IntegerParameterCoercion {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, "version 82")
			require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(nil, p), "version 82")
			data, err := p.Marshal()
			require.NoError(t, err)
			_, err = decodeScope(data, c.proc, true, nil)
			require.ErrorContains(t, err, "version 82")
			table := &planpb.TableDef{Cols: []*planpb.ColDef{{Default: &planpb.Default{Expr: expr}}}}
			require.ErrorContains(t, plan2.RequirePersistedExpressionProtocol(c.proc.Ctx, c.proc, table), "version 82")
			// Existing publication hooks must cover the newly shared feature too.
			require.ErrorContains(t, plan2.RequirePersistedIPFunctionProtocol(c.proc.Ctx, c.proc, table), "version 82")
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, nil)
			require.ErrorContains(t, plan2.RequirePersistedExpressionProtocol(nil, c.proc, expr), "version 82")
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion82)
			require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
			require.NoError(t, plan2.RequirePersistedExpressionProtocol(c.proc.Ctx, c.proc, table))
		})
	}
}

func TestIntegerArgumentProtocolPlacementAndSend(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr := integerProtocolExpr(function.IntegerArgumentCastOverload)
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
	place(defines.MORPCVersion75)
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	_, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")
	place(defines.MORPCVersion82)
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	// A downgrade/replacement after successful placement must fail at send time.
	client.version = defines.MORPCVersion75
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")
	require.Error(t, validateIntegerArgumentDestination(c.proc, nil))
	require.Equal(t, client.calls, client.releases)
	ctx, cancel := context.WithCancel(c.proc.Ctx)
	cancel()
	c.proc.Ctx = ctx
	require.ErrorIs(t, validateIntegerArgumentDestination(c.proc, &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}}), context.Canceled)
}
