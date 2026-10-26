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
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
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
	"testing"
	"time"
)

func floatConversionProtocolExpr(id int32, source, target types.T) *planpb.Expr {
	return &planpb.Expr{Typ: target.ToType().PlanType(), Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(id, 0)},
		Args: []*planpb.Expr{{Typ: source.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}},
	}}}
}

func TestFloatConversionFeatureBoundaries(t *testing.T) {
	for _, tc := range []struct {
		id             int32
		source, target types.T
		want           bool
	}{
		{function.CAST, types.T_varchar, types.T_float64, true},
		{function.CAST_ASSIGN, types.T_text, types.T_float32, true},
		{function.CAST_STRICT, types.T_float64, types.T_decimal128, true},
		{function.CAST_IGNORE, types.T_float32, types.T_decimal64, true},
		{function.CAST, types.T_json, types.T_decimal64, true},
		{function.CAST, types.T_float64, types.T_decimal256, false},
		{function.CAST, types.T_int64, types.T_decimal128, false},
		{function.CAST, types.T_varchar, types.T_int64, false},
		{function.JSON_AGG_TO_DOUBLE, types.T_json, types.T_float64, true},
	} {
		expr := floatConversionProtocolExpr(tc.id, tc.source, tc.target)
		features, err := planpb.RequiredRemoteExpressionFeatures(expr)
		require.NoError(t, err)
		require.Equal(t, tc.want, features.FloatConversionSemantics)
		floor, err := plan2.RequiredPersistedExpressionProtocolVersion(expr)
		require.NoError(t, err)
		if tc.want {
			require.Equal(t, defines.MORPCVersion132, floor)
		} else {
			require.Less(t, floor, defines.MORPCVersion132)
		}
	}
	// Independent catalog contracts cannot lower the newly required float floor.
	comparison := floatConversionProtocolExpr(function.EQUAL, types.T_json, types.T_bool)
	comparison.GetF().Args = append(comparison.GetF().Args, &planpb.Expr{Typ: types.T_int64.ToType().PlanType()})
	owner := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{
		comparison, floatConversionProtocolExpr(function.CAST, types.T_varchar, types.T_float64),
	}}}}
	combined, err := planpb.RequiredRemoteExpressionFeatures(owner)
	require.NoError(t, err)
	require.True(t, combined.JSONNumericComparisons)
	require.True(t, combined.FloatConversionSemantics)
	floor, err := plan2.RequiredPersistedExpressionProtocolVersion(owner)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion132, floor)
	typedParam := &planpb.Expr{Typ: types.T_float64.ToType().PlanType(), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
	features, err := planpb.RequiredRemoteExpressionFeatures(typedParam)
	require.NoError(t, err)
	require.True(t, features.FloatConversionSemantics)
}

func TestFloatConversionProtocolPlacementStreamAndReceive(t *testing.T) {
	for _, alias := range []string{"cast", "if", "iff"} {
		t.Run(alias, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			expr := floatConversionProtocolExpr(function.CAST, types.T_varchar, types.T_float64)
			if alias != "cast" {
				source := expr.GetF().Args[0]
				var err error
				expr, err = plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, alias, []*planpb.Expr{
					source,
					{Typ: types.T_int64.ToType().PlanType(), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 1}}}},
					{Typ: types.T_int64.ToType().PlanType(), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 2}}}},
				})
				require.NoError(t, err)
				require.Equal(t, int32(types.T_varchar), expr.GetF().Args[0].Typ.Id)
			}
			qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
			op := projection.NewArgument()
			defer op.Release()
			op.ProjectList = []*planpb.Expr{expr}
			scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
			for _, version := range []int64{defines.MORPCVersion131, defines.MORPCVersion132} {
				client.version = version
				c.execType = plan2.ExecTypeAP_MULTICN
				c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
				require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
				if version < defines.MORPCVersion132 {
					require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
					_, err := encodeRemoteScope(scope, c.proc)
					require.ErrorContains(t, err, "132")
				} else {
					require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
					floor := int64(0)
					_, err := encodeRemoteScopeWithProtocol(scope, c.proc, &floor)
					require.NoError(t, err)
					require.Equal(t, defines.MORPCVersion132, floor, "bound execution stream must reject a different, older worker generation")
					c.proc.Base.TxnOperator = fakeTxnOperator{}
					c.proc.Base.SessionInfo.TimeZone = time.UTC
					c.proc.Ctx = defines.AttachAccountId(context.Background(), 0)
					floor = 0
					_, _, _, _, err = prepareRemoteRunSendingDataWithProtocol("", scope, c.proc, nil, uuid.Nil, &floor)
					require.NoError(t, err)
					require.Equal(t, defines.MORPCVersion132, floor)
					for _, observed := range []int64{0, defines.MORPCVersion131, defines.MORPCVersion132} {
						stream := &fakeStreamSender{}
						ch := make(chan morpc.Message, 1)
						ch <- &pipeline.Message{Cmd: pipeline.Method_PipelineProtocolCheck, Sid: pipeline.Status_Last, ProtocolVersion: observed}
						sender := &messageSenderOnClient{ctx: context.Background(), streamSender: stream, receiveCh: ch}
						err := sender.confirmProtocolOnStream(floor)
						if observed < floor {
							require.Error(t, err)
						} else {
							require.NoError(t, err)
						}
						require.Equal(t, 1, stream.sentCnt)
						request := stream.sent[0].(*pipeline.Message)
						require.Equal(t, pipeline.Method_PipelineProtocolCheck, request.GetCmd())
						require.Equal(t, floor, request.GetProtocolVersion())
					}
				}
			}
			// Query RPC result changed after placement; actual send must re-observe it.
			client.version = defines.MORPCVersion131
			_, err := encodeRemoteScope(scope, c.proc)
			require.ErrorContains(t, err, "132")
			require.Equal(t, client.calls, client.releases)
			client.customResponse = true
			c.execType = plan2.ExecTypeAP_MULTICN
			c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
			require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
			require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
			_, err = encodeRemoteScope(scope, c.proc)
			require.Error(t, err, "unknown capability must reject at the actual destination too")
			p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			for _, version := range []int64{0, defines.MORPCVersion131} {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
				require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "132")
			}
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion132)
			require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
			// Combining independent contracts must retain the larger floor.
			features := planpb.RemoteExpressionFeatures{FloatConversionSemantics: true, IncompleteTemporalStrings: true, DecimalStatisticalResultContracts: true}
			require.Equal(t, defines.MORPCVersion132, executionRemoteExpressionProtocolVersion(c.proc, features))
		})
	}
}

func TestGenerateProcessHelperRejectsConflictingStatementIntent(t *testing.T) {
	value := &pipeline.ProcessInfo{StatementRuntimeSelect: true, StatementRuntimeDml: true, PrepareParams: pipeline.PrepareParamInfo{Length: 1, Data: []byte{0xff}}}
	data, err := value.Marshal()
	require.NoError(t, err)
	// Nil txn client and invalid parameter data prove classification rejection
	// precedes transaction reconstruction and parameter ownership allocation.
	_, err = generateProcessHelper(context.Background(), data, nil)
	require.ErrorContains(t, err, "conflicting SELECT and DML")
}
