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
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCalendarIntervalProtocolPlacementSendReceive(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "to_calendar_interval", []*planpb.Expr{{Typ: types.T_varchar.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}, plan2.MakePlan2Int64ConstExprWithType(int64(types.Day))})
	require.NoError(t, err)
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.CalendarIntervalNormalization)
	require.Equal(t, defines.MORPCVersion115, remoteExpressionProtocolVersion(features))
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	place := func(version int64, want plan2.ExecType) {
		client.version = version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
		require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
		require.Equal(t, want, c.execType)
	}
	place(defines.MORPCVersion114, plan2.ExecTypeAP_ONECN)
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 115")
	require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, nil, features), "versioned remote destination")
	place(defines.MORPCVersion115, plan2.ExecTypeAP_MULTICN)
	_, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	client.version = defines.MORPCVersion114
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 115", "send checks changed actual destination")
	client.customResponse = true
	client.response = &query.Response{}
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 115")
	require.Equal(t, client.calls, client.releases)
	p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
	data, err := p.Marshal()
	require.NoError(t, err)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, version := range []int64{defines.MORPCVersion114, defines.MORPCVersion115} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		validationErr := validateRemoteExpressionPipelineProtocol(c.proc, p)
		_, decodeErr := decodeScope(data, c.proc, true, nil)
		if version == defines.MORPCVersion114 {
			require.ErrorContains(t, validationErr, "version 115")
			require.ErrorContains(t, decodeErr, "version 115")
		} else {
			require.NoError(t, validationErr)
			require.NoError(t, decodeErr)
		}
	}
}
