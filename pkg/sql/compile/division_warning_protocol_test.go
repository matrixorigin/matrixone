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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestDivisionWarningProtocolPlacementSendReceive(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	c.proc.SetStmtProfile(&process.StmtProfile{})
	expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "div", []*planpb.Expr{
		{Typ: types.T_int64.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
		{Typ: types.T_int64.ToType().PlanType(), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 0}}}},
	})
	require.NoError(t, err)
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.ScalarDivision)
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, tc := range []struct {
		name, statement, query, mode string
		required                     bool
	}{
		{"enabled SELECT", "Select", "DQL", "ERROR_FOR_DIVISION_BY_ZERO", true},
		{"traditional SELECT", "Select", "DQL", "TRADITIONAL", true},
		{"disabled SELECT", "Select", "DQL", "", false},
		{"DML", "Insert", "DML", "TRADITIONAL", false},
		{"CTAS", "Create Table", "DDL", "TRADITIONAL", false},
		{"legacy missing classification", "", "", "TRADITIONAL", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c.proc.GetStmtProfile().SetStatementRuntimeProfile(tc.statement, tc.query, false)
			c.proc.GetSessionInfo().SqlMode = tc.mode
			c.proc.SetResolveVariableFunc(nil)
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
			client.version = defines.MORPCVersion115
			c.execType = plan2.ExecTypeAP_MULTICN
			c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
			require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
			wantExecution := plan2.ExecTypeAP_MULTICN
			if tc.required {
				wantExecution = plan2.ExecTypeAP_ONECN
			}
			require.Equal(t, wantExecution, c.execType)
			_, err := encodeRemoteScope(scope, c.proc)
			if tc.required {
				require.ErrorContains(t, err, "version 116")
			} else {
				require.NoError(t, err)
			}
			client.version = defines.MORPCVersion116
			_, err = encodeRemoteScope(scope, c.proc)
			require.NoError(t, err)
			// Re-observe a destination downgrade after an earlier successful send.
			client.version = defines.MORPCVersion115
			err = validateRemoteExpressionDestination(c.proc, &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}}, features)
			if tc.required {
				require.ErrorContains(t, err, "version 116")
			} else {
				require.NoError(t, err)
			}
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion115)
			err = validateRemoteExpressionPipelineProtocol(c.proc, p)
			if tc.required {
				require.ErrorContains(t, err, "version 116")
			} else {
				require.NoError(t, err)
			}
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion116)
			require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
		})
	}
	require.Equal(t, client.calls, client.releases)
}

// Exercise the real arithmetic entry using a process reconstructed by the
// pipeline receiver, rather than only asserting its transported policy bit.
func requireRemoteDivisionWarning(t *testing.T, proc *process.Process) {
	t.Helper()
	previousSink := proc.WarningSink
	defer func() { proc.WarningSink = previousSink }()
	sink := &remoteWarningSession{}
	proc.WarningSink = sink
	expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "div", []*planpb.Expr{
		{Typ: types.T_int64.ToType().PlanType(), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 1}}}},
		{Typ: types.T_int64.ToType().PlanType(), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 0}}}},
	})
	require.NoError(t, err)
	result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
	require.NoError(t, err)
	defer free()
	require.True(t, result.IsNull(0))
	require.Equal(t, uint64(1), sink.totalWarnings)
	require.Len(t, sink.warnings, 1)
	require.Equal(t, uint16(1365), sink.warnings[0].code)
	require.Equal(t, "Division by 0", sink.warnings[0].msg)
}
