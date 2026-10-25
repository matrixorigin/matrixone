// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
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
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestNativePADMergeStreamFloor(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	moruntime.ServiceRuntime(c.proc.GetService()).SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion132)
	client.version = defines.MORPCVersion132
	for _, tc := range []struct {
		name             string
		native, weighted bool
	}{
		{"weighted_only", false, true},
		{"native_only", true, false},
		{"weighted_and_native", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := group.NewArgument()
			t.Cleanup(g.Release)
			g.NeedEval = true
			if tc.weighted {
				g.Aggs = quantileExactWeightedPipelineAggs(t)
			}
			scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: g}
			if tc.native {
				typ := types.NewWithCharset(types.T_varchar, 8, 0, types.CharsetUTF8MB4Bin)
				typ.CollationVersion = 1
				g.GroupBy = []*pb.Expr{{Typ: typ.PlanType(), Expr: &pb.Expr_Col{Col: &pb.ColRef{ColPos: 2}}}}
			}
			want := defines.MORPCVersion131
			if tc.native {
				want = defines.MORPCVersion132
			}
			for _, initial := range []int64{0, defines.MORPCVersion130, defines.MORPCVersion132 + 1} {
				floor := initial
				data, err := encodeRemoteScopeWithProtocol(scope, c.proc, &floor)
				require.NoError(t, err)
				require.Equal(t, max(initial, want), floor, "the bound stream must retain every independent capability")
				var decoded pipeline.Pipeline
				require.NoError(t, decoded.Unmarshal(data))
				require.Equal(t, tc.weighted, pipelineContainsQuantileExactWeighted(&decoded))
				features, err := requiredRemotePipelineExpressionFeatures(&decoded)
				require.NoError(t, err)
				require.Equal(t, tc.native, features.NativePADCollation)
			}
			// Placement probes cannot authorize a different worker generation
			// on the live stream. Refusal must precede the pipeline payload.
			for _, worker := range []int64{0, defines.MORPCVersion130, defines.MORPCVersion131, defines.MORPCVersion132} {
				stream := &fakeStreamSender{}
				replies := make(chan morpc.Message, 1)
				replies <- &pipeline.Message{Cmd: pipeline.Method_PipelineProtocolCheck, Sid: pipeline.Status_Last, ProtocolVersion: worker}
				sender := &messageSenderOnClient{ctx: context.Background(), streamSender: stream, receiveCh: replies}
				err := sender.confirmProtocolOnStream(want)
				if worker < want {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, 1, stream.sentCnt)
				request := stream.sent[0].(*pipeline.Message)
				require.Equal(t, pipeline.Method_PipelineProtocolCheck, request.Cmd)
				require.Equal(t, want, request.ProtocolVersion)
			}
		})
	}
}

func TestNativePADMergeKeepsIndependentCapabilities(t *testing.T) {
	for _, tc := range []struct {
		name         string
		id, overload int64
		required     int64
	}{
		{"load_fk", 602, 0, defines.MORPCVersion125},
		{"view_definition", 603, 0, defines.MORPCVersion126},
		{"view_check", 604, 0, defines.MORPCVersion126},
		{"json_numeric", 546, 1, defines.MORPCVersion128},
		{"json_elt", 384, 1, defines.MORPCVersion129},
		{"json_opaque_text", 16, 0, defines.MORPCVersion129},
		{"incomplete_date_format", 251, 2, defines.MORPCVersion130},
		{"incomplete_week", 216, 4, defines.MORPCVersion130},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := &pb.Expr{Typ: types.T_bool.ToType().PlanType(), Expr: &pb.Expr_F{F: &pb.Function{Func: &pb.ObjectRef{Obj: tc.id<<32 | tc.overload}}}}
			if tc.id == 216 {
				expr.Typ = types.T_int32.ToType().PlanType()
			}
			if tc.id == 16 {
				expr.Typ = types.T_varchar.ToType().PlanType()
				expr.GetF().Args = []*pb.Expr{{Typ: types.T_json.ToType().PlanType()}}
			}
			owner := &pb.Node{DedupKeyExprs: []*pb.Expr{expr}}
			features, err := pb.RequiredExecutableRemoteExpressionFeatures(owner)
			require.NoError(t, err)
			require.True(t, features.Any())
			require.False(t, features.NativePADCollation)
			require.Equal(t, tc.required == defines.MORPCVersion130, features.IncompleteTemporalStrings)
			if tc.id == 216 {
				require.True(t, features.WeekSessionDefault)
				require.True(t, features.DateNumericResultContracts)
			}
			require.Equal(t, tc.required, remoteExpressionProtocolVersion(features))
			native := types.NewWithCharset(types.T_varchar, 8, 0, 3).PlanType()
			native.CollationVersion = 1
			owner.PreInsertUkCtx = &pb.PreInsertUkCtx{KeyDiagnosticExprs: []*pb.ExprList{{List: []*pb.Expr{{Typ: native}}}}}
			features, err = pb.RequiredExecutableRemoteExpressionFeatures(owner)
			require.NoError(t, err)
			require.True(t, features.NativePADCollation)
			require.Equal(t, defines.MORPCVersion132, remoteExpressionProtocolVersion(features))
		})
	}
}
