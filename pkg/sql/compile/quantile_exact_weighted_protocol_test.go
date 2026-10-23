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

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// TestPreflightQuantileExactWeightedConfigsReturnsPreparedValueError checks
// the prepared-level preflight and the config construction boundary: the
// level is evaluated exactly once, its [0,1] contract rejects bad EXECUTE
// values, and the configuration extraction keeps the two data arguments in
// their original types.
func TestPreflightQuantileExactWeightedConfigsReturnsPreparedValueError(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	value := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}}
	weight := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_uint64)}}
	level := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_text)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	// The function registry is keyed by the lower-case SQL name, the same
	// form the SQL parser hands the binder.
	const registryName = "quantileexactweighted"
	bound, err := plan2.BindFuncExprImplByPlanExpr(
		context.Background(), registryName,
		[]*planpb.Expr{value, weight, level})
	require.NoError(t, err)
	node := &planpb.Node{AggList: []*planpb.Expr{bound}}

	check := func(param []byte, isNull bool) error {
		params := vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(params, param, isNull, proc.Mp()))
		proc.SetPrepareParams(params)
		baseline := proc.Mp().CurrNB()
		err := preflightPercentileConfigs(node, proc)
		require.Equal(t, baseline, proc.Mp().CurrNB(),
			"preflight evaluation must release its temporary vector")
		proc.SetPrepareParams(nil)
		params.Free(proc.Mp())
		return err
	}

	err = check([]byte("1.5"), false)
	require.ErrorContains(t, err,
		"percentile argument of quantileexactweighted must be finite and in [0,1]")
	err = check(nil, true)
	require.ErrorContains(t, err,
		"percentile argument of quantileexactweighted cannot be NULL")

	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("0.25"), false, proc.Mp()))
	proc.SetPrepareParams(params)
	require.NoError(t, preflightPercentileConfigs(node, proc),
		"the same prepared aggregate must remain reusable after rejected executions")
	args, config, err := constructQuantileExactWeightedConfig(bound.GetF(), proc)
	require.NoError(t, err)
	require.Len(t, args, 2,
		"only the level is stripped; value and weight stay as data arguments")
	require.Equal(t, int32(types.T_int64), args[0].Typ.Id)
	require.Equal(t, int32(types.T_uint64), args[1].Typ.Id)
	require.Equal(t, aggexec.EncodeQuantileExactWeightedConfig([]byte("0.25")), config)
	proc.SetPrepareParams(nil)
	params.Free(proc.Mp())

	// A constant level does not need the prepared parameter path and reaches
	// the same configuration through the ordinary construction boundary.
	constLevel := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_float64)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Value: &planpb.Literal_Dval{Dval: 0.5}}},
	}
	constBound, err := plan2.BindFuncExprImplByPlanExpr(
		context.Background(), registryName,
		[]*planpb.Expr{value, weight, constLevel})
	require.NoError(t, err)
	args, config, err = constructQuantileExactWeightedConfig(constBound.GetF(), proc)
	require.NoError(t, err)
	require.Len(t, args, 2)
	require.Equal(t, aggexec.EncodeQuantileExactWeightedConfig([]byte("0.5")), config)

	// Wrong arity and a non-configuration level are rejected with the
	// aggregate's own diagnostic.
	_, _, err = constructQuantileExactWeightedConfig(&planpb.Function{
		Func: &planpb.ObjectRef{ObjName: plan2.NameQuantileExactWeighted},
		Args: []*planpb.Expr{value, level},
	}, proc)
	require.ErrorContains(t, err,
		"quantileExactWeighted requires a value, a weight, and a level argument")

	columnLevel := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 3}},
	}
	_, err = plan2.BindFuncExprImplByPlanExpr(
		context.Background(), registryName,
		[]*planpb.Expr{value, weight, columnLevel})
	require.Error(t, err)
	require.Contains(t, err.Error(),
		"level argument of quantileexactweighted must be a non-null constant or parameter")
}

// quantileExactWeightedPipeline returns one remote group scope carrying the
// exact weighted percentile aggregate, the shape encodeRemoteScopeWithProtocol
// walks for its requiredProtocol floor.
func quantileExactWeightedPipelineAggs(t *testing.T) []aggexec.AggFuncExecExpression {
	t.Helper()
	args := []*planpb.Expr{
		{Typ: types.T_float64.ToType().PlanType(),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
		{Typ: types.T_uint64.ToType().PlanType(),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
	}
	return []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(
		aggexec.AggIdOfQuantileExactWeighted, false, args,
		aggexec.EncodeQuantileExactWeightedConfig([]byte("0.5")))}
}

// TestQuantileExactWeightedRemoteDispatchGate covers decision D10's layers
// with destination-observing checks: the requiredProtocol floor raised while
// encoding a pipeline that contains the weighted aggregate, the send-time
// stream confirmation rejecting a pre-gate destination (including a worker
// that rolled back between placement and sending), the compile-time
// AllCNsSupportProtocol fast-fail over a pre-gate inventory, and the
// coordinator self-gate message.
func TestQuantileExactWeightedRemoteDispatchGate(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion130)
	client.version = defines.MORPCVersion130

	aggs := quantileExactWeightedPipelineAggs(t)
	g := group.NewArgument()
	t.Cleanup(g.Release)
	g.NeedEval, g.Aggs = true, aggs
	leaf := &Scope{Magic: Remote, Proc: c.proc,
		NodeInfo: engine.Node{Id: "new-worker", Addr: "remote:6001"}, RootOp: g}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: leaf.NodeInfo,
		PreScopes: []*Scope{leaf}}

	// Layer 2: the encoded aggregate raises the required-protocol floor so the
	// pre-dispatch stream confirmation can reject a lower destination.
	minimum := int64(0)
	data, err := encodeRemoteScopeWithProtocol(scope, c.proc, &minimum)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion130, minimum,
		"the weighted aggregate must raise the destination floor to its gate version")
	decodedPipeline := new(pipeline.Pipeline)
	require.NoError(t, decodedPipeline.Unmarshal(data))
	require.True(t, pipelineContainsQuantileExactWeighted(decodedPipeline))
	require.False(t, pipelineContainsQuantileExactWeighted(&pipeline.Pipeline{}))

	// A destination that rolled back below the gate between placement and
	// sending is rejected by the raised floor at the pre-dispatch stream
	// confirmation seam (covered by the dedicated seam test below); the
	// compile-time inventory fast-fail below also fails closed for it.
	client.version = defines.MORPCVersion129

	// The coordinator self-gate supplies the coherent not-supported message:
	// a below-gate coordinator cannot dispatch the aggregate at all.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion129)
	require.ErrorContains(t, validateRemoteAggregateProtocol(c.proc, aggs),
		"quantileExactWeighted remote execution requires MORPC protocol version 130")

	// Layer 1: the compile-time inventory fast-fail. With a pre-gate CN in the
	// fake inventory the weighted aggregate is not remotely distributable, so
	// shuffle compilation and the remote group wire both fail closed; at the
	// gate version they both admit it.
	node := &planpb.Node{
		AggList: []*planpb.Expr{{
			Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{
				ObjName: plan2.NameQuantileExactWeighted}}}}},
	}
	require.True(t, hasQuantileExactWeighted(node))
	require.Nil(t, node.Stats)
	node.Stats = &planpb.Stats{
		HashmapStats: &planpb.HashMapStats{Shuffle: true}}

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion129)
	client.version = defines.MORPCVersion129
	require.False(t, c.supportsQuantileExactWeightedDispatch(),
		"a pre-gate CN in the inventory must block distribution")
	require.False(t, c.canCompileShuffleGroup(node))
	require.True(t, c.hasUnsupportedRemoteGroupWire(node))

	client.version = defines.MORPCVersion130
	require.False(t, c.supportsQuantileExactWeightedDispatch(),
		"a pre-gate coordinator must still block distribution")

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion130)
	client.version = defines.MORPCVersion130
	require.True(t, c.supportsQuantileExactWeightedDispatch())
	require.True(t, c.canCompileShuffleGroup(node))
	require.False(t, c.hasUnsupportedRemoteGroupWire(node))
	require.NoError(t, validateRemoteAggregateProtocol(c.proc, aggs),
		"a coordinator at the gate version dispatches the aggregate")

	// The single-stage exclusion sites keep their incumbent contract: a node
	// over the weighted aggregate is excluded exactly as the ordered-set
	// percentiles are, and plain nodes stay admissible.
	require.False(t, c.canCompileParallelUnionAllAggregate(node),
		"the weighted aggregate must take the single-stage merge path")
	plainNode := &planpb.Node{}
	require.False(t, hasQuantileExactWeighted(plainNode))
	require.True(t, c.canCompileParallelUnionAllAggregate(plainNode))
	percentileNode := &planpb.Node{AggList: []*planpb.Expr{{
		Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{
			ObjName: plan2.NamePercentileDisc}}}}}}
	require.True(t, hasOrderedSetPercentile(percentileNode))
	require.False(t, hasQuantileExactWeighted(percentileNode))
	require.False(t, c.canCompileParallelUnionAllAggregate(percentileNode),
		"the incumbent ordered-set percentile contract must be unchanged")
}

// TestQuantileExactWeightedStreamConfirmationCoversGateVersion stretches the
// generic pre-dispatch stream confirmation seam across the aggregate's gate
// version: the destination must confirm the raised floor on the live stream
// before the pipeline is delivered.
func TestQuantileExactWeightedStreamConfirmationCoversGateVersion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version int64
		wantErr bool
	}{
		{name: "worker at gate", version: defines.MORPCVersion130},
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
			err := sender.confirmProtocolOnStream(defines.MORPCVersion130)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, stream.sentCnt,
				"the confirmation must run before any pipeline payload")
			request := stream.sent[0].(*pipeline.Message)
			require.Equal(t, pipeline.Method_PipelineProtocolCheck, request.GetCmd())
			require.Equal(t, defines.MORPCVersion130, request.GetProtocolVersion())
		})
	}
}
