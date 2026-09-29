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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func stringNumericResultPipeline(t *testing.T) (*planpb.Query, *Scope) {
	t.Helper()
	args := []*planpb.Expr{
		{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
		{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
	}
	expr, err := plan.BindFuncExprImplByPlanExpr(context.Background(), "find_in_set", args)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_int32), expr.Typ.Id)
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	op.ProjectList = []*planpb.Expr{expr}
	return qry, &Scope{
		Magic:    Remote,
		NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"},
		RootOp:   op,
	}
}

func TestStringNumericResultPlacementAndDestinationProtocol(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	qry, scope := stringNumericResultPipeline(t)
	scope.Proc = c.proc
	t.Cleanup(scope.RootOp.Release)

	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	client.version = defines.MORPCVersion79
	require.NoError(t, c.constrainStringNumericResultWorkers(qry))
	require.Equal(t, plan.ExecTypeAP_ONECN, c.execType,
		"a worker below the result-contract version must not receive a multi-CN plan")
	require.Equal(t, c.addr, c.cnList[0].Addr)
	_, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")

	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	client.version = defines.MORPCVersion80
	require.NoError(t, c.constrainStringNumericResultWorkers(qry))
	require.Equal(t, plan.ExecTypeAP_MULTICN, c.execType,
		"a v80 worker keeps the multi-CN placement")
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	client.version = defines.MORPCVersion79
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination",
		"a destination downgrade after placement must still be rejected")

	// An unaddressable worker is an unknown capability and must fail closed to
	// one-CN placement rather than assuming the current protocol is sufficient.
	c.execType = plan.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Mcpu: 4}}
	client.version = defines.MORPCVersion80
	require.NoError(t, c.constrainStringNumericResultWorkers(qry))
	require.Equal(t, plan.ExecTypeAP_ONECN, c.execType)
	require.Equal(t, c.addr, c.cnList[0].Addr)

	rt := runtime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion79)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc,
		&pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{
			ProjectList: []*planpb.Expr{qry.Nodes[0].ProjectList[0]},
		}}}), "version 80")
}
