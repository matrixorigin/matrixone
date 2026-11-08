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
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// The widened decimal multiplication carrier (issue #30938) adds NO remote
// expression feature bit, no execution fence, no placement demotion and no
// send/destination/decode gate of its own (design decision log 86 (ii),
// F-SHUTDOWN-UPGRADE-COMPATIBILITY; section 9 I9 untouched-gate guards). A
// widened-multiplication pipeline carries exactly the feature identity the
// unchanged base walk computes for an equivalent narrow multiplication, and
// the pre-existing gates keep guarding the base's own features untouched.

// decimalMultiplicationProtocolExpr binds one real decimal multiplication
// over two typed column slots, so the resolved overload and published result
// metadata match what a shipped pipeline carries (issue #30938).
func decimalMultiplicationProtocolExpr(t *testing.T, proc *process.Process, w1, s1, w2, s2 int32) *planpb.Expr {
	t.Helper()
	expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "*", []*planpb.Expr{
		{Typ: types.New(types.T_decimal256, w1, s1).PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
		{Typ: types.New(types.T_decimal256, w2, s2).PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
	})
	require.NoError(t, err)
	return expr
}

func decimalMultiplicationFeatures(t *testing.T, expr *planpb.Expr) planpb.RemoteExpressionFeatures {
	t.Helper()
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	return features
}

func TestDecimalMultiplicationRemoteFeatureIdentity(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)

	// A widened publication (38,10)x(38,10) publishes (76,12).
	wide := decimalMultiplicationProtocolExpr(t, c.proc, 38, 10, 38, 10)
	require.Equal(t, int32(types.T_decimal256), wide.Typ.Id)
	require.Equal(t, int32(76), wide.Typ.Width)
	require.Equal(t, int32(12), wide.Typ.Scale)

	// A narrow multiply (30,5)x(30,5) publishes (60,10): the parent's own
	// pre-change width. A bound decimal multiplication over typed column
	// slots carries no remote-expression feature bit at all.
	narrow := decimalMultiplicationProtocolExpr(t, c.proc, 30, 5, 30, 5)
	require.Equal(t, int32(types.T_decimal256), narrow.Typ.Id)
	require.Equal(t, int32(60), narrow.Typ.Width)
	require.Equal(t, int32(10), narrow.Typ.Scale)
	narrowFeatures := decimalMultiplicationFeatures(t, narrow)
	require.Equal(t, planpb.RemoteExpressionFeatures{}, narrowFeatures,
		"a decimal multiplication carries no remote-expression feature bit")

	// The widened publication adds no feature bit and raises no floor over
	// the equivalent narrow multiplication.
	wideFeatures := decimalMultiplicationFeatures(t, wide)
	require.Equal(t, narrowFeatures, wideFeatures,
		"the widened publication carries the narrow multiplication's exact feature identity")
	require.Equal(t, remoteExpressionProtocolVersion(narrowFeatures),
		remoteExpressionProtocolVersion(wideFeatures),
		"the widened publication raises no execution floor")

	// A statically fallback-reachable pair (65,40)x(65,40) has
	// (s1+s2)-max(12,s1,s2) = 80-40 = 40 > 38, the static form of the
	// kernel's scaleAdj < -38 dispatch. Publishing (76,40) sets nothing, and
	// pinning the node's published width at 65 shows the fallback-reachable
	// arm on its own sets nothing either.
	fallback := decimalMultiplicationProtocolExpr(t, c.proc, 65, 40, 65, 40)
	require.Equal(t, int32(76), fallback.Typ.Width)
	require.Equal(t, narrowFeatures, decimalMultiplicationFeatures(t, fallback))
	fallbackPinned := plan2.DeepCopyExpr(fallback)
	fallbackPinned.Typ.Width = 65
	require.Equal(t, narrowFeatures, decimalMultiplicationFeatures(t, fallbackPinned),
		"the fallback-reachable arm carries no feature bit of its own")

	// A decimal128 pair stays non-decimal256: the widened carrier family is
	// never involved.
	narrow128, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "*", []*planpb.Expr{
		{Typ: types.New(types.T_decimal128, 20, 3).PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
		{Typ: types.New(types.T_decimal128, 18, 2).PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal128), narrow128.Typ.Id)

	// Base-machinery control: a week(varchar, mode) call keeps its
	// pre-existing incomplete-temporal-string feature and v130 floor,
	// proving the untouched gate still computes floors for base features.
	temporal, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "week", []*planpb.Expr{
		{Typ: types.T_varchar.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
		plan2.MakePlan2Int64ConstExprWithType(3),
	})
	require.NoError(t, err)
	temporalFeatures := decimalMultiplicationFeatures(t, temporal)
	require.True(t, temporalFeatures.IncompleteTemporalStrings)
	require.Equal(t, defines.MORPCVersion130, remoteExpressionProtocolVersion(temporalFeatures))

	// A query combining the widened multiply with the temporal call carries
	// exactly the temporal call's own base feature set: the multiplication
	// (itself feature-free) contributes nothing, and the floor stays the
	// temporal feature's own v130.
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{wide, temporal}}}, Steps: []int32{0}}
	combined, err := planpb.RequiredRemoteExpressionFeatures(qry)
	require.NoError(t, err)
	require.Equal(t, temporalFeatures, combined,
		"the widened multiplication contributes no feature to a combined pipeline")
	require.Equal(t, defines.MORPCVersion130, remoteExpressionProtocolVersion(combined))
}

func TestDecimalMultiplicationPlacementAndGatesUntouched(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	// Production clusters resolve the local CN's own pipeline address
	// through cluster membership, so this cluster must contain the local
	// node as well: after the control's demote reschedules cnList to the
	// local node, the second probe resolves it by pipeline address and reads
	// the local (helper-set latest) protocol version. The helper's cleanup
	// still restores the original global.
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.ClusterService, &schedulerTestCluster{cns: []metadata.CNService{
		{ServiceID: "old-worker", QueryAddress: "worker:9000", PipelineServiceAddress: "remote:6001"},
		{ServiceID: c.proc.GetService(), QueryAddress: "local:9000", PipelineServiceAddress: "local:6001"},
	}})
	wide := decimalMultiplicationProtocolExpr(t, c.proc, 38, 10, 38, 10)
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{wide}}}, Steps: []int32{0}}

	// Placement: a below-latest worker keeps distributed placement for the
	// widened multiplication, which rides no execution fence.
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	client.version = defines.MORPCVersion126
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType,
		"the widened multiplication never demotes placement")

	// Placement control: the base's own v130 temporal feature still demotes
	// on a below-floor worker and keeps distributed placement on a
	// floor-satisfying one — the untouched gate machinery is intact.
	temporal, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "week", []*planpb.Expr{
		{Typ: types.T_varchar.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
		plan2.MakePlan2Int64ConstExprWithType(3),
	})
	require.NoError(t, err)
	temporalQry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{temporal}}}, Steps: []int32{0}}
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	client.version = defines.MORPCVersion126
	require.NoError(t, c.constrainRemoteExpressionWorkers(temporalQry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType,
		"the base temporal feature keeps its placement demotion")
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
	client.version = defines.MORPCVersion130
	require.NoError(t, c.constrainRemoteExpressionWorkers(temporalQry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)

	// Send: a remote scope carrying the widened multiplication encodes on
	// any destination generation and raises no stream fence.
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{wide}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	client.version = defines.MORPCVersion126
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.NotNil(t, data)
	// The shared stream accumulator is owned by the encode path: it is first
	// set to the pipeline's vector-partition minimum (zero here) and then
	// max-raised by each feature fence (vector_scan_protocol.go:103-107,
	// remoterun.go:161-201). A feature-free payload leaves it at zero.
	streamRequired := defines.MORPCVersion126
	data, err = encodeRemoteScopeWithProtocol(scope, c.proc, &streamRequired)
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Equal(t, int64(0), streamRequired,
		"the widened multiplication raises no stream fence")

	// Send control: the base temporal feature still fences its stream and
	// still rejects a below-floor destination.
	temporalOp := projection.NewArgument()
	defer temporalOp.Release()
	temporalOp.ProjectList = []*planpb.Expr{temporal}
	temporalScope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: temporalOp}
	client.version = defines.MORPCVersion126
	_, err = encodeRemoteScope(temporalScope, c.proc)
	require.ErrorContains(t, err, "incomplete temporal strings")
	client.version = defines.MORPCVersion130
	_, err = encodeRemoteScope(temporalScope, c.proc)
	require.NoError(t, err)
	_, err = encodeRemoteScopeWithProtocol(temporalScope, c.proc, &streamRequired)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion130, streamRequired,
		"the base temporal feature keeps its stream fence")

	// Decode: the receiving binary admits the widened multiplication's
	// pipeline at any minted protocol version.
	widePipeline := &pipeline.Pipeline{Children: []*pipeline.Pipeline{{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{wide}}}}}}
	wire, err := widePipeline.Marshal()
	require.NoError(t, err)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion126)
	decoded, err := decodeScope(wire, c.proc, true, nil)
	require.NoError(t, err)
	require.NotNil(t, decoded)
	decoded.release()

	// Decode control: the base temporal feature keeps its admission floor.
	temporalPipeline := &pipeline.Pipeline{Children: []*pipeline.Pipeline{{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{temporal}}}}}}
	temporalWire, err := temporalPipeline.Marshal()
	require.NoError(t, err)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion126)
	_, err = decodeScope(temporalWire, c.proc, true, nil)
	require.ErrorContains(t, err, "130")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion130)
	decoded, err = decodeScope(temporalWire, c.proc, true, nil)
	require.NoError(t, err)
	require.NotNil(t, decoded)
	decoded.release()
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)

	// Destination validation: the widened multiplication's (empty) feature
	// set passes at any local protocol version, while the base temporal
	// feature keeps its own floor.
	require.NoError(t, validateRemoteExpressionFeatures(c.proc,
		decimalMultiplicationFeatures(t, wide)))
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion126)
	require.NoError(t, validateRemoteExpressionFeatures(c.proc,
		decimalMultiplicationFeatures(t, wide)))
	require.ErrorContains(t,
		validateRemoteExpressionFeatures(c.proc, planpb.RemoteExpressionFeatures{IncompleteTemporalStrings: true}),
		"130")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion130)
	require.NoError(t,
		validateRemoteExpressionFeatures(c.proc, planpb.RemoteExpressionFeatures{IncompleteTemporalStrings: true}))
}
