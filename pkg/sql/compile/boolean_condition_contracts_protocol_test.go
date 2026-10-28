// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// bindEnumIFFContractExpr binds the REAL ENUM IF() contract expression the
// planner produces - a storage-typed T_enum condition argument through the
// same function binder runtime SQL uses, so every gate below observes the
// bound node instead of a hand-built surrogate.
func bindEnumIFFContractExpr(t *testing.T) *planpb.Expr {
	t.Helper()
	enumStorage := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_enum), Enumvalues: "low,mid,high"},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "if", []*planpb.Expr{
		enumStorage,
		plan2.MakePlan2Int64ConstExprWithType(1),
		plan2.MakePlan2Int64ConstExprWithType(0),
	})
	require.NoError(t, err, "the MySQL contracts bind IF(e,1,0) with the raw ENUM condition in-domain")
	require.Equal(t, int32(types.T_enum), expr.GetF().Args[0].Typ.Id,
		"iffCheck preserves the ENUM condition in-domain instead of scheduling a cast")
	return expr
}

// bindJSONCaseContractExpr binds the REAL JSON CASE-when contract expression
// (#30932) for comparison in the same gates.
func bindJSONCaseContractExpr(t *testing.T) *planpb.Expr {
	t.Helper()
	jsonColumn := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_json)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "case", []*planpb.Expr{
		jsonColumn,
		plan2.MakePlan2Int64ConstExprWithType(1),
		plan2.MakePlan2Int64ConstExprWithType(0),
	})
	require.NoError(t, err)
	return expr
}

// TestBooleanConditionContractDetectionOnBoundExpressions pins the feature
// walk over REAL bound expressions: the in-domain T_json CASE condition and
// the in-domain T_enum IFF condition both raise JSONBooleanConditionContracts
// and the minted v135 floor, so a represented pre-fix receiver (which has no
// ENUM branch in IffConditionTruthyAt and no in-domain JSON handling) refuses
// delivery instead of mis-executing. A plain numeric IF control raises
// nothing.
func TestBooleanConditionContractDetectionOnBoundExpressions(t *testing.T) {
	for _, tc := range []struct {
		name              string
		expr              *planpb.Expr
		wantContractFloor bool
	}{
		{"enum iff condition", bindEnumIFFContractExpr(t), true},
		{"json case condition", bindJSONCaseContractExpr(t), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			features, err := planpb.RequiredRemoteExpressionFeatures(tc.expr)
			require.NoError(t, err)
			require.True(t, features.JSONBooleanConditionContracts, tc.name)
			require.Equal(t, defines.MORPCVersion135, remoteExpressionProtocolVersion(features), tc.name)
			require.ErrorContains(t, validateRemoteExpressionFeatures(nil, features), "version 135", tc.name)

			version, err := plan2.RequiredPersistedExpressionProtocolVersion(tc.expr)
			require.NoError(t, err)
			require.Equal(t, int64(defines.MORPCVersion135), version, tc.name)
		})
	}

	numericControl := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	control, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "if", []*planpb.Expr{
		numericControl,
		plan2.MakePlan2Int64ConstExprWithType(1),
		plan2.MakePlan2Int64ConstExprWithType(0),
	})
	require.NoError(t, err)
	features, err := planpb.RequiredRemoteExpressionFeatures(control)
	require.NoError(t, err)
	require.False(t, features.JSONBooleanConditionContracts,
		"a numeric IF condition raises no conditional-boolean floor")
	version, err := plan2.RequiredPersistedExpressionProtocolVersion(control)
	require.NoError(t, err)
	require.Zero(t, version)
}

// TestBooleanConditionContractPlacementEncodeAndRefusal walks the ENUM IF
// contract expression through the remote pipeline paths: temporary placement
// falls back to single-CN below the minted version, scope encoding refuses
// below the floor, the destination refusal is independent of the coordinator,
// and the receiving decode keeps failing closed while its own runtime
// protocol is below the minted version.
func TestBooleanConditionContractPlacementEncodeAndRefusal(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr := bindEnumIFFContractExpr(t)

	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.JSONBooleanConditionContracts)
	require.Equal(t, defines.MORPCVersion135, remoteExpressionProtocolVersion(features))
	require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, nil, features), "versioned remote destination")

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
	place(defines.MORPCVersion131, plan2.ExecTypeAP_ONECN)
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 135")
	place(defines.MORPCVersion135, plan2.ExecTypeAP_MULTICN)
	_, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)

	p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
	data, err := p.Marshal()
	require.NoError(t, err)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, version := range []int64{
		defines.MORPCVersion128,
		defines.MORPCVersion129,
		defines.MORPCVersion130,
		defines.MORPCVersion131,
	} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		require.ErrorContainsf(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "version 135",
			"destination runtime at version %d must fail closed", version)
		_, decodeErr := decodeScope(data, c.proc, true, nil)
		require.ErrorContainsf(t, decodeErr, "version 135",
			"receiving decode at version %d must fail closed before evaluation", version)
		client.customResponse = false
		client.version = defines.MORPCVersion135
		destination := &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}}
		require.ErrorContainsf(t, validateRemoteExpressionDestination(c.proc, destination, features), "version 135",
			"coordinator rollout floor independently fences sends at version %d", version)
	}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion135)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))

	// A legacy numeric IF control stays deliverable at the old floor.
	numericControl := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	control, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "if", []*planpb.Expr{
		numericControl,
		plan2.MakePlan2Int64ConstExprWithType(1),
		plan2.MakePlan2Int64ConstExprWithType(0),
	})
	require.NoError(t, err)
	controlFeatures, err := planpb.RequiredRemoteExpressionFeatures(control)
	require.NoError(t, err)
	require.False(t, controlFeatures.Any())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion131)
	require.NoError(t, validateRemoteExpressionFeatures(c.proc, controlFeatures))
}

// TestBooleanConditionContractStreamConfirmationCoversGateVersion pins the
// stream confirmation for the minted gate version: the destination must
// confirm v135 on the live stream before any pipeline payload is delivered,
// and a below-gate or unknown capability must refuse.
func TestBooleanConditionContractStreamConfirmationCoversGateVersion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version int64
		wantErr bool
	}{
		{name: "worker at gate", version: defines.MORPCVersion135},
		{name: "worker at temporal gate, below boolean gate", version: defines.MORPCVersion131, wantErr: true},
		{name: "worker below gate", version: defines.MORPCVersion129, wantErr: true},
		{name: "unknown capability", version: 0, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := &fakeStreamSender{}
			receiveCh := make(chan morpc.Message, 1)
			receiveCh <- &pipeline.Message{
				Id: 0, Cmd: pipeline.Method_PipelineProtocolCheck,
				Sid: pipeline.Status_Last, ProtocolVersion: tc.version,
			}
			sender := &messageSenderOnClient{
				ctx: context.Background(), streamSender: stream, receiveCh: receiveCh,
			}
			err := sender.confirmProtocolOnStream(defines.MORPCVersion135)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, stream.sentCnt,
				"the confirmation must run before any pipeline payload")
			request := stream.sent[0].(*pipeline.Message)
			require.Equal(t, pipeline.Method_PipelineProtocolCheck, request.GetCmd())
			require.Equal(t, defines.MORPCVersion135, request.GetProtocolVersion())
		})
	}
}

// TestBooleanConditionContractPersistedGatesOnBoundExpressions drives the
// SAME bound ENUM IF expression through the persisted authoring and read
// gates: below the minted runtime protocol/floor the authoring gate refuses
// (nothing is persisted) and the read gate refuses the rebind, and an
// at-floor runtime admits both.
func TestBooleanConditionContractPersistedGatesOnBoundExpressions(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	expr := bindEnumIFFContractExpr(t)

	// The version table derives v135 from the SAME real bound expression the
	// gates protect.
	required, err := plan2.RequiredPersistedExpressionProtocolVersion(expr)
	require.NoError(t, err)
	require.Equal(t, int64(defines.MORPCVersion135), required)

	rt := moruntime.ServiceRuntime(c.proc.GetService())
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldReadFloor, hadReadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
	oldAuthoringFloor, hadAuthoringFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadReadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, oldReadFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolFloor, current)
		}
		if hadAuthoringFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldAuthoringFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion131)
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion131))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion131))
	require.ErrorContains(t, plan2.RequirePersistedProtocolVersionForAuthoring(
		c.proc.Ctx, c.proc, required), "protocol version 135")
	require.ErrorContains(t, plan2.RequirePersistedProtocolVersion(
		c.proc.Ctx, c.proc, required), "protocol version 135")

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion135)
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion135))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion135))
	require.NoError(t, plan2.RequirePersistedProtocolVersionForAuthoring(
		c.proc.Ctx, c.proc, required))
	require.NoError(t, plan2.RequirePersistedProtocolVersion(
		c.proc.Ctx, c.proc, required))
	require.NoError(t, plan2.RequirePersistedProtocolVersion(
		c.proc.Ctx, c.proc, 0), "a requirement-0 expression never rejects the rebind")

	// A bound numeric IF control never raises the requirement; the version
	// table therefore keeps it admitted even at the simulated old floor.
	numericControl := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	control, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "if", []*planpb.Expr{
		numericControl,
		plan2.MakePlan2Int64ConstExprWithType(1),
		plan2.MakePlan2Int64ConstExprWithType(0),
	})
	require.NoError(t, err)
	controlVersion, err := plan2.RequiredPersistedExpressionProtocolVersion(control)
	require.NoError(t, err)
	require.Zero(t, controlVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion131)
	require.NoError(t, plan2.RequirePersistedProtocolVersionForAuthoring(
		c.proc.Ctx, c.proc, controlVersion))
	require.NoError(t, plan2.RequirePersistedProtocolVersion(
		c.proc.Ctx, c.proc, controlVersion))
}
