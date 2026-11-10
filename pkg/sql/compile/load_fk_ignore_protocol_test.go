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
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// Official main 743e749a1e uses MORPC124 and FUNCTION_END_NUMBER=602:
// that binary has no overload for the newly appended function602. Exercise its
// immediate predecessor floor at the real compiler/encoding boundaries.
func TestLoadFKIgnoreProtocolPlacementSendReceive(t *testing.T) {
	for _, ignore := range []bool{false, true} {
		name := "ordinary LOAD"
		if ignore {
			name = "LOAD IGNORE FK"
		}
		t.Run(name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			expr := &planpb.Expr{Typ: types.T_bool.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
			if ignore {
				var err error
				expr, err = plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "_load_fk_ignore_check", []*planpb.Expr{expr})
				require.NoError(t, err)
				require.Equal(t, function.EncodeOverloadID(602, 0), expr.GetF().Func.Obj)
			}
			qry := &planpb.Query{StmtType: planpb.Query_INSERT, Nodes: []*planpb.Node{{NodeType: planpb.Node_FILTER, FilterIsBarrier: ignore, FilterList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
			f, err := planpb.RequiredRemoteExpressionFeatures(qry)
			require.NoError(t, err)
			required := int64(0)
			if ignore {
				required = defines.MORPCVersion125
			}
			require.Equal(t, required, remoteExpressionProtocolVersion(f))
			op := filter.NewArgument()
			defer op.Release()
			op.FilterExprs = []*planpb.Expr{expr}
			scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion125)
			// Admission is based on each current destination, even after a
			// successful placement/send; no observation may be cached.
			for _, version := range []int64{defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion124} {
				client.version = version
				c.execType = plan2.ExecTypeAP_MULTICN
				c.cnList = engine.Nodes{scope.NodeInfo}
				require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
				want := plan2.ExecTypeAP_MULTICN
				if ignore && version < required {
					want = plan2.ExecTypeAP_ONECN
				}
				require.Equal(t, want, c.execType)
				var streamRequired int64
				data, err := encodeRemoteScopeWithProtocol(scope, c.proc, &streamRequired)
				if ignore && version < required {
					require.ErrorContains(t, err, "version 125")
					require.Empty(t, data, "unsupported payload must not be produced")
				} else {
					require.NoError(t, err)
					require.NotEmpty(t, data)
					require.Equal(t, required, streamRequired)
				}
			}
			// Replace a capable endpoint after placement, without scheduling
			// again. The send must independently observe the replacement.
			client.version = defines.MORPCVersion125
			c.execType, c.cnList = plan2.ExecTypeAP_MULTICN, engine.Nodes{scope.NodeInfo}
			require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
			require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
			client.version = defines.MORPCVersion124
			replacementData, replacementErr := encodeRemoteScope(scope, c.proc)
			if ignore {
				require.ErrorContains(t, replacementErr, "version 125")
				require.Empty(t, replacementData)
			} else {
				require.NoError(t, replacementErr)
			}
			if ignore {
				require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, nil, f), "versioned remote destination")
				client.customResponse = true
				client.response = &query.Response{}
				data, err := encodeRemoteScope(scope, c.proc)
				require.ErrorContains(t, err, "version 125")
				require.Empty(t, data)
				client.customResponse = false
			}
			client.version = defines.MORPCVersion125
			var streamRequired int64
			data, err := encodeRemoteScopeWithProtocol(scope, c.proc, &streamRequired)
			require.NoError(t, err)
			require.Equal(t, required, streamRequired)
			// The receiver inspects nested instruction expressions before
			// allocating operator owners, independently of sender admission.
			p := &pipeline.Pipeline{Children: []*pipeline.Pipeline{{InstructionList: []*pipeline.Instruction{{Op: int32(vm.Filter), Filters: []*planpb.Expr{expr}}}}}}
			wire, err := p.Marshal()
			require.NoError(t, err)
			for _, version := range []int64{defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion124} {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
				decoded, receiveErr := decodeScope(wire, c.proc, true, nil)
				if decoded != nil {
					decoded.release()
				}
				sent, sendErr := encodeRemoteScope(scope, c.proc)
				if ignore && version < required {
					require.ErrorContains(t, receiveErr, "version 125")
					require.Nil(t, decoded)
					require.ErrorContains(t, sendErr, "version 125")
					require.Empty(t, sent)
				} else {
					require.NoError(t, receiveErr)
					require.NoError(t, sendErr)
				}
			}
			require.Equal(t, client.calls, client.releases)
			require.NotEmpty(t, data)
		})
	}
}

func TestLoadFKIgnoreProtocolLocalAndRetainedPlan(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion125)
	expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "_load_fk_ignore_check", []*planpb.Expr{{Typ: types.T_bool.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}})
	require.NoError(t, err)
	p := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{StmtType: planpb.Query_INSERT, Nodes: []*planpb.Node{{NodeType: planpb.Node_FILTER, FilterIsBarrier: true, FilterList: []*planpb.Expr{expr}}}, Steps: []int32{0}}}}
	c.pn = p
	for _, version := range []any{nil, "125", defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion124} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		admissionErr := validateTemporalResultProtocol(c.proc, p)
		executor, buildErr := colexec.NewExpressionExecutor(c.proc, expr)
		if executor != nil {
			executor.Free()
		}
		if version == defines.MORPCVersion125 {
			require.NoError(t, admissionErr)
			require.NoError(t, buildErr)
		} else {
			require.ErrorContains(t, admissionErr, "version 125")
			require.ErrorContains(t, c.Compile(c.proc.Ctx, p, nil), "version 125")
			_, err := c.Run(0)
			require.ErrorContains(t, err, "version 125", "retained Run rejects before Prepare or source effects")
			require.ErrorContains(t, buildErr, "version 125")
			require.Nil(t, executor)
		}
	}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion124)
	ordinary := &planpb.Expr{Typ: types.T_bool.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	require.NoError(t, validateTemporalResultProtocol(c.proc, ordinary))
	executor, err := colexec.NewExpressionExecutor(c.proc, ordinary)
	require.NoError(t, err)
	executor.Free()
}
