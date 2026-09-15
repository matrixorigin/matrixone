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
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func remoteIPProtocolPipeline(functionID, overloadID int32) *pipeline.Pipeline {
	return &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{
		ProjectList: []*planpb.Expr{{
			Typ: planpb.Type{Id: 10},
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{
					Obj:     function.EncodeOverloadID(functionID, overloadID),
					ObjName: "ip-function",
				},
			}},
		}},
	}}}
}

func TestRemoteIPFunctionProtocolValidation(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	rt := runtime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, previous)
		} else {
			for _, value := range []int64{defines.MORPCVersion70, defines.MORPCVersion71, defines.MORPCVersion72} {
				rt.CompareAndDeleteGlobalVariables(runtime.MOProtocolVersion, value)
			}
		}
	})

	for _, functionID := range []int32{
		function.INET6_ATON,
		function.INET6_NTOA,
		function.INET_ATON,
		function.INET_NTOA,
		function.IS_IPV4,
		function.IS_IPV6,
		function.IS_IPV4_COMPAT,
	} {
		t.Run("function-"+strconv.Itoa(int(functionID)), func(t *testing.T) {
			remotePipeline := remoteIPProtocolPipeline(functionID, 0)

			rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion70)
			err := validateRemoteExpressionPipelineProtocol(proc, remotePipeline)
			require.ErrorContains(t, err, "corrected IP function semantics require MORPC protocol version 72")
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))

			rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion71)
			err = validateRemoteExpressionPipelineProtocol(proc, remotePipeline)
			require.ErrorContains(t, err, "corrected IP function semantics require MORPC protocol version 72")

			rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion72)
			require.NoError(t, validateRemoteExpressionPipelineProtocol(proc, remotePipeline))
		})
	}

	t.Run("new INET_NTOA overload", func(t *testing.T) {
		rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion70)
		err := validateRemoteExpressionPipelineProtocol(proc, remoteIPProtocolPipeline(function.INET_NTOA, 8))
		require.ErrorContains(t, err, "corrected IP function semantics require MORPC protocol version 72")
		rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion71)
		err = validateRemoteExpressionPipelineProtocol(proc, remoteIPProtocolPipeline(function.INET_NTOA, 8))
		require.ErrorContains(t, err, "corrected IP function semantics require MORPC protocol version 72")
	})

	t.Run("ordinary function is not fenced", func(t *testing.T) {
		rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion70)
		require.NoError(t, validateRemoteExpressionPipelineProtocol(proc, remoteIPProtocolPipeline(function.ABS, 0)))
	})
}

func TestIPFunctionDestinationProtocolValidation(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "inet_aton", []*planpb.Expr{{
		Typ:  planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}})
	require.NoError(t, err)
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{
		Magic:    Remote,
		Proc:     c.proc,
		NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"},
		RootOp:   op,
	}

	c.proc.Base.QueryClient = client
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	client.version = defines.MORPCVersion70
	require.NoError(t, c.constrainIPFunctionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")

	client.version = defines.MORPCVersion71
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainIPFunctionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)

	client.version = defines.MORPCVersion72
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	require.NoError(t, c.constrainIPFunctionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	require.Equal(t, client.calls, client.releases)
}
