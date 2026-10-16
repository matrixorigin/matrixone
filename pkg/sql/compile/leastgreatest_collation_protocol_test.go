// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestLeastGreatestCollationProtocolPlacementSendAndReceive(t *testing.T) {
	for _, name := range []string{"least", "greatest"} {
		t.Run(name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			typ := types.NewWithCharset(types.T_varchar, 20, 0, types.CharsetUTF8MB4UnicodeCI)
			typ.CollationVersion = 1
			args := []*planpb.Expr{
				{Typ: typ.PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
				{Typ: typ.PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
			}
			expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, name, args)
			require.NoError(t, err)
			_, overload := function.DecodeOverloadID(expr.GetF().Func.Obj)
			require.Equal(t, int32(function.LeastGreatestCollationOverload), overload)
			features, err := planpb.RequiredRemoteExpressionFeatures(expr)
			require.NoError(t, err)
			require.True(t, features.LeastGreatestCollationSemantics)
			features.DecimalStatisticalResultContracts = true
			features.CalendarIntervalNormalization = true
			features.ScalarDivision = true
			require.Equal(t, defines.MORPCVersion117, remoteExpressionProtocolVersion(features))
			require.ErrorContains(t, validateRemoteExpressionFeatures(nil, features), "version 117")
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
			place(defines.MORPCVersion116, plan2.ExecTypeAP_ONECN)
			_, err = encodeRemoteScope(scope, c.proc)
			require.ErrorContains(t, err, "version 117")
			place(defines.MORPCVersion117, plan2.ExecTypeAP_MULTICN)
			_, err = encodeRemoteScope(scope, c.proc)
			require.NoError(t, err)
			// A final successful probe cannot prevent replacement immediately
			// afterward. The appended ID is what the real old registry rejects.
			client.onSend = func() { client.version = defines.MORPCVersion116 }
			_, err = encodeRemoteScope(scope, c.proc)
			require.NoError(t, err)
			require.Equal(t, defines.MORPCVersion116, client.version)
			client.onSend = nil
			_, err = encodeRemoteScope(scope, c.proc)
			require.ErrorContains(t, err, "version 117", "selected endpoint was replaced by an older worker")
			client.customResponse, client.response = true, &query.Response{}
			place(defines.MORPCVersion117, plan2.ExecTypeAP_ONECN)
			_, err = encodeRemoteScope(scope, c.proc)
			require.ErrorContains(t, err, "version 117", "unknown actual capability must fail closed")
			require.Equal(t, client.calls, client.releases)

			p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
			data, err := p.Marshal()
			require.NoError(t, err)
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			for _, version := range []int64{defines.MORPCVersion113, defines.MORPCVersion114, defines.MORPCVersion115, defines.MORPCVersion116, defines.MORPCVersion117} {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
				validationErr := validateRemoteExpressionPipelineProtocol(c.proc, p)
				_, decodeErr := decodeScope(data, c.proc, true, nil)
				if version < defines.MORPCVersion117 {
					client.customResponse = false
					client.version = defines.MORPCVersion117
					destination := &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}}
					require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, destination, features), "version 117", "coordinator rollout floor independently fences sends")
					require.ErrorContains(t, validationErr, "version 117")
					require.ErrorContains(t, decodeErr, "version 117")
				} else {
					require.NoError(t, validationErr)
					require.NoError(t, decodeErr)
				}
			}
			// Released byte-order plans remain usable at the old floor.
			legacy := plan2.DeepCopyExpr(expr)
			id, _ := function.DecodeOverloadID(legacy.GetF().Func.Obj)
			legacy.GetF().Func.Obj = function.EncodeOverloadID(id, 0)
			legacyFeatures, err := planpb.RequiredRemoteExpressionFeatures(legacy)
			require.NoError(t, err)
			require.False(t, legacyFeatures.Any())
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion112)
			require.NoError(t, validateRemoteExpressionFeatures(c.proc, legacyFeatures))
		})
	}
}
