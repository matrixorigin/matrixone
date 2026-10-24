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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/vectorscan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type expressionVersionClient struct {
	fakeQueryClient
	version         int64
	versions        map[string]int64
	calls, releases int
	customResponse  bool
	response        *query.Response
	sendErr         error
	onSend          func()
}

func TestJSONValueProtocolFloorComposesWithLandedContracts(t *testing.T) {
	features := planpb.RemoteExpressionFeatures{
		JSONValueContract:                 true,
		JSONInputContracts:                true,
		JSONScalarLiteralContracts:        true,
		MediumIntAssignmentBounds:         true,
		DateNumericResultContracts:        true,
		DecimalStatisticalResultContracts: true,
		CalendarIntervalNormalization:     true,
		LeastGreatestCollationSemantics:   true,
		LoadFKIgnoreCheck:                 true,
		ViewMetadataFunctions:             true,
		JSONNumericComparisons:            true,
	}
	require.Equal(t, defines.MORPCVersion130, requiredExpressionContractProtocolVersion(features))
	require.Equal(t, defines.MORPCVersion130, remoteExpressionProtocolVersion(features))
	features.JSONValueContract = false
	require.Equal(t, defines.MORPCVersion104, requiredExpressionContractProtocolVersion(features))
	require.Equal(t, defines.MORPCVersion128, remoteExpressionProtocolVersion(features))
	features.JSONNumericComparisons = false
	require.Equal(t, defines.MORPCVersion126, remoteExpressionProtocolVersion(features))
	features.ViewMetadataFunctions = false
	require.Equal(t, defines.MORPCVersion125, remoteExpressionProtocolVersion(features))
	features.LoadFKIgnoreCheck = false
	require.Equal(t, defines.MORPCVersion117, remoteExpressionProtocolVersion(features))
	features.LeastGreatestCollationSemantics = false
	require.Equal(t, defines.MORPCVersion115, remoteExpressionProtocolVersion(features))
}

func TestJSONValueActualStreamProtocolMinimum(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	client.version = defines.MORPCVersion130

	args := make([]*planpb.Expr, 7)
	for i := range args {
		args[i] = &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_varchar)},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}},
		}
	}
	jsonValue := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{
				Obj:     function.EncodeOverloadID(function.JSON_VALUE, 2),
				ObjName: "json_value",
			},
			Args: args,
		}},
	}
	makeScope := func(t *testing.T, expressions ...*planpb.Expr) *Scope {
		t.Helper()
		root := projection.NewArgument()
		root.ProjectList = expressions
		t.Cleanup(root.Release)
		return &Scope{
			Magic:    Remote,
			Proc:     c.proc,
			NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"},
			RootOp:   root,
		}
	}

	minimum := int64(0)
	data, err := encodeRemoteScopeWithProtocol(makeScope(t, jsonValue), c.proc, &minimum)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion130, minimum)
	require.NotEmpty(t, data)

	for _, tc := range []struct {
		name    string
		version int64
	}{
		{name: "replacement v129 refuses before pipeline message", version: defines.MORPCVersion129},
		{name: "v130 accepts pipeline message", version: defines.MORPCVersion130},
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
			err := sender.confirmProtocolOnStream(minimum)
			require.Len(t, stream.sent, 1)
			check := stream.sent[0].(*pipeline.Message)
			require.Equal(t, pipeline.Method_PipelineProtocolCheck, check.GetCmd())
			require.Equal(t, minimum, check.GetProtocolVersion())
			if tc.version < minimum {
				require.ErrorContains(t, err, "requires MORPC protocol version 130")
				return
			}
			require.NoError(t, err)
			require.NoError(t, sender.sendPipeline(data, nil, true, len(data)+1, "JSON_VALUE protocol minimum test"))
			require.Len(t, stream.sent, 2)
			require.Equal(t, pipeline.Method_PipelineMessage, stream.sent[1].(*pipeline.Message).GetCmd())
		})
	}

	elt := bindJSONStringConsumerExpr(t, "elt")
	for _, tc := range []struct {
		name        string
		expressions []*planpb.Expr
		vector      bool
		initial     int64
		want        int64
	}{
		{name: "independent v129 consumer", expressions: []*planpb.Expr{elt}, want: defines.MORPCVersion129},
		{name: "no feature remains unversioned", expressions: []*planpb.Expr{plan2.MakePlan2StringConstExprWithType("plain")}},
		{name: "combined v129 and JSON_VALUE uses maximum", expressions: []*planpb.Expr{elt, jsonValue}, want: defines.MORPCVersion130},
		{name: "vector v111 and JSON text v129 use maximum", expressions: []*planpb.Expr{elt}, vector: true, want: defines.MORPCVersion129},
		{name: "vector v111 and JSON_VALUE v130 use maximum", expressions: []*planpb.Expr{jsonValue}, vector: true, want: defines.MORPCVersion130},
		{name: "stale output is reset to current JSON_VALUE floor", expressions: []*planpb.Expr{jsonValue}, initial: defines.MORPCVersion130 + 1, want: defines.MORPCVersion130},
		{name: "stale output is reset when no feature is present", expressions: []*planpb.Expr{plan2.MakePlan2StringConstExprWithType("plain")}, initial: defines.MORPCVersion130 + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			minimum := tc.initial
			scope := makeScope(t, tc.expressions...)
			if tc.vector {
				scope.DataSource = &Source{node: &planpb.Node{
					NodeType:        planpb.Node_INDEX_SEARCH_SCAN,
					IndexSearchScan: &planpb.IndexSearchScan{AlgoOptions: []byte(`{}`)},
				}}
			}
			_, err := encodeRemoteScopeWithProtocol(scope, c.proc, &minimum)
			require.NoError(t, err)
			require.Equal(t, tc.want, minimum)
		})
	}
}

func TestJSONValueDestinationRequiresItsOwnProtocolFloor(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(fmt.Sprintf("mediumint=%t", mixed), func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			features := planpb.RemoteExpressionFeatures{JSONValueContract: true, MediumIntAssignmentBounds: mixed, DateNumericResultContracts: mixed, LeastGreatestCollationSemantics: mixed}
			p := &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}}
			client.version = defines.MORPCVersion122
			require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, p, features), "version 130")
			require.Equal(t, 1, client.calls)
			require.Equal(t, client.calls, client.releases)
			client.version = defines.MORPCVersion123
			require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, p, features), "version 130")
			require.Equal(t, 2, client.calls)
			require.Equal(t, client.calls, client.releases)
			client.version = defines.MORPCVersion128
			require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, p, features), "version 130")
			require.Equal(t, 3, client.calls)
			require.Equal(t, client.calls, client.releases)
			client.version = defines.MORPCVersion129
			require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, p, features), "version 130")
			require.Equal(t, 4, client.calls)
			require.Equal(t, client.calls, client.releases)
			client.version = defines.MORPCVersion130
			require.NoError(t, validateRemoteExpressionDestination(c.proc, p, features))
			require.Equal(t, 5, client.calls)
			require.Equal(t, client.calls, client.releases)
		})
	}
}

func TestJSONValueAndDateNumericProtocolPlacement(t *testing.T) {
	for _, name := range []string{"week", "dayofyear"} {
		t.Run(name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			args := []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_date)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}}
			if name == "week" {
				args = append(args, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 3}}}})
			}
			calendar, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, name, args)
			require.NoError(t, err)
			jsonValue := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.JSON_VALUE, 2), ObjName: "json_value"},
				Args: make([]*planpb.Expr, 7),
			}}}
			for _, combined := range []bool{false, true} {
				projection := []*planpb.Expr{calendar}
				if combined {
					projection = append(projection, jsonValue)
				}
				qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: projection}}, Steps: []int32{0}}
				features, err := planpb.RequiredRemoteExpressionFeatures(qry)
				require.NoError(t, err)
				require.True(t, features.DateNumericResultContracts)
				require.Equal(t, combined, features.JSONValueContract)
				for _, version := range []int64{defines.MORPCVersion112, defines.MORPCVersion113, defines.MORPCVersion116, defines.MORPCVersion117, defines.MORPCVersion118, defines.MORPCVersion119, defines.MORPCVersion120, defines.MORPCVersion121, defines.MORPCVersion122, defines.MORPCVersion123, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion127, defines.MORPCVersion128, defines.MORPCVersion129, defines.MORPCVersion130} {
					rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
					client.version = version
					c.execType = plan2.ExecTypeAP_MULTICN
					c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
					require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
					want := plan2.ExecTypeAP_MULTICN
					if combined && version < defines.MORPCVersion130 {
						want = plan2.ExecTypeAP_ONECN
					}
					require.Equal(t, want, c.execType)
				}
			}
			require.Equal(t, client.calls, client.releases)
		})
	}
}

func TestJSONValueAndLeastGreatestProtocolComposition(t *testing.T) {
	for _, name := range []string{"least", "greatest"} {
		t.Run(name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			typ := types.NewWithCharset(types.T_varchar, 20, 0, types.CharsetUTF8MB4UnicodeCI)
			typ.CollationVersion = 1
			collated, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, name, []*planpb.Expr{
				{Typ: typ.PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
				{Typ: typ.PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
			})
			require.NoError(t, err)
			args := make([]*planpb.Expr, 7)
			for i := range args {
				args[i] = &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}
			}
			jsonValue := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.JSON_VALUE, 2), ObjName: "json_value"}, Args: args,
			}}}
			op := projection.NewArgument()
			defer op.Release()
			scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
			for _, combined := range []bool{false, true} {
				op.ProjectList = []*planpb.Expr{collated}
				floor := defines.MORPCVersion117
				if combined {
					op.ProjectList = append(op.ProjectList, jsonValue)
					floor = defines.MORPCVersion130
				}
				qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: op.ProjectList}}, Steps: []int32{0}}
				p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: op.ProjectList}}}
				data, err := p.Marshal()
				require.NoError(t, err)
				features, err := planpb.RequiredRemoteExpressionFeatures(qry)
				require.NoError(t, err)
				require.True(t, features.LeastGreatestCollationSemantics)
				require.Equal(t, combined, features.JSONValueContract)
				require.Equal(t, floor, remoteExpressionProtocolVersion(features))
				for _, version := range []int64{defines.MORPCVersion117, defines.MORPCVersion118, defines.MORPCVersion122, defines.MORPCVersion123, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion127, defines.MORPCVersion128, defines.MORPCVersion129, defines.MORPCVersion130, defines.MORPCVersion117} {
					// Reuse the same plan/scope after capability changes: placement
					// and final send must not retain the prior successful admission.
					rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
					client.version = version
					c.execType = plan2.ExecTypeAP_MULTICN
					c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
					require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
					_, sendErr := encodeRemoteScope(scope, c.proc)
					decoded, receiveErr := decodeScope(data, c.proc, true, nil)
					if version < floor {
						require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
						require.ErrorContains(t, sendErr, "version 130")
						require.ErrorContains(t, receiveErr, "version 130")
					} else {
						require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
						require.NoError(t, sendErr)
						require.NoError(t, receiveErr)
						decoded.release()
					}
				}
			}
			require.Equal(t, client.calls, client.releases)
		})
	}
}

func TestJSONValueIndexSearchAlgoExprsProtocolComposition(t *testing.T) {
	require.Equal(t, defines.MORPCVersion111, int64(vectorscan.IndexSearchScanProtocolVersion))
	text := func(value string) *planpb.Expr {
		return &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: value}}}}
	}
	nullResult := func() *planpb.Expr {
		return &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}
	}
	errorPolicy := func() *planpb.Expr {
		return &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 2}}}}
	}
	jsonValue := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.JSON_VALUE, 2), ObjName: "json_value"},
		Args: []*planpb.Expr{text(`{"a":1}`), text("$.a"), nullResult(), errorPolicy(), nullResult(), errorPolicy(), nullResult()},
	}}}
	for _, shape := range []string{"data source", "apply", "nested"} {
		t.Run(shape, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			jsonInput := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.CONCAT, 0), ObjName: "concat"},
				Args: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_json)}}},
			}}}
			scan := &planpb.IndexSearchScan{AlgoOptions: []byte(`{}`), AlgoExprs: []*planpb.Expr{jsonInput}}
			node := &planpb.Node{NodeType: planpb.Node_INDEX_SEARCH_SCAN, IndexSearchScan: scan}
			p := &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}}
			if shape == "apply" {
				p.InstructionList = []*pipeline.Instruction{{Apply: &pipeline.Apply{IndexSearchScan: scan}}}
			} else if shape == "nested" {
				p.Children = []*pipeline.Pipeline{{DataSource: &pipeline.Source{Node: node}}}
			} else {
				p.DataSource = &pipeline.Source{Node: node}
			}
			require.Equal(t, defines.MORPCVersion111, minimumRemoteVectorProtocol(p))
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion110)
			require.ErrorContains(t, validateRemoteVectorPartitionProtocol(c.proc, p), "version 111")
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion111)
			client.version = defines.MORPCVersion111
			require.NoError(t, validateRemoteVectorPartitionProtocol(c.proc, p))
			require.NoError(t, validateVectorPartitionDestination(c.proc, p))
			features, err := planpb.RequiredRemoteExpressionFeatures(p)
			require.NoError(t, err)
			require.False(t, features.JSONValueContract)
			require.True(t, features.JSONInputContracts)
			// CONCAT(JSON) renders opaque JSON text as well as consuming JSON input,
			// so its actual expression floor is max(v101 input, v129 text contract).
			require.True(t, features.JSONOpaqueTextContracts)
			require.False(t, features.JSONStringConsumerOverload)
			require.Equal(t, defines.MORPCVersion129, remoteExpressionProtocolVersion(features))
			require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "version 129")
			require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, p, features), "version 129")
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion129)
			client.version = defines.MORPCVersion129
			require.NoError(t, validateRemoteVectorPartitionProtocol(c.proc, p))
			require.NoError(t, validateVectorPartitionDestination(c.proc, p))
			require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, p))
			require.NoError(t, validateRemoteExpressionDestination(c.proc, p, features))

			// No ProjectList, QueryPayload or candidate-limit expression can
			// accidentally supply the JSON feature missing from this owner.
			scan.AlgoExprs = []*planpb.Expr{jsonValue}
			data, err := p.Marshal()
			require.NoError(t, err)
			decoded := &pipeline.Pipeline{}
			require.NoError(t, decoded.Unmarshal(data))
			for _, owner := range []*pipeline.Pipeline{p, decoded} {
				features, err = planpb.RequiredRemoteExpressionFeatures(owner)
				require.NoError(t, err)
				require.True(t, features.JSONValueContract)
				require.Equal(t, defines.MORPCVersion130, remoteExpressionProtocolVersion(features))
				require.NoError(t, validateRemoteVectorPartitionProtocol(c.proc, owner))
				require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, owner), "version 130")
				require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, owner, features), "version 130")
			}
			qry := &planpb.Query{Nodes: []*planpb.Node{node}, Steps: []int32{0}}
			c.execType = plan2.ExecTypeAP_MULTICN
			c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
			require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
			require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)

			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion123)
			client.version = defines.MORPCVersion123
			c.execType = plan2.ExecTypeAP_MULTICN
			c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
			require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
			require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
			for _, owner := range []*pipeline.Pipeline{p, decoded} {
				require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, owner), "version 130")
				require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, owner, features), "version 130")
			}

			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion128)
			client.version = defines.MORPCVersion128
			c.execType = plan2.ExecTypeAP_MULTICN
			c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
			require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
			require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
			for _, owner := range []*pipeline.Pipeline{p, decoded} {
				require.NoError(t, validateRemoteVectorPartitionProtocol(c.proc, owner))
				require.NoError(t, validateVectorPartitionDestination(c.proc, owner))
				require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, owner), "version 130")
				require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, owner, features), "version 130")
			}

			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion129)
			client.version = defines.MORPCVersion129
			c.execType = plan2.ExecTypeAP_MULTICN
			c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
			require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
			require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
			for _, owner := range []*pipeline.Pipeline{p, decoded} {
				require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, owner), "version 130")
				require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, owner, features), "version 130")
			}

			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion130)
			client.version = defines.MORPCVersion130
			c.execType = plan2.ExecTypeAP_MULTICN
			c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
			require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
			require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
			for _, owner := range []*pipeline.Pipeline{p, decoded} {
				require.NoError(t, validateRemoteVectorPartitionProtocol(c.proc, owner))
				require.NoError(t, validateVectorPartitionDestination(c.proc, owner))
				require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, owner))
				require.NoError(t, validateRemoteExpressionDestination(c.proc, owner, features))
			}
			// The new JSON floor must not bypass main's independent rejection
			// of the older index-search representation.
			scan.AlgoOptions = nil
			require.ErrorContains(t, validateRemoteVectorPartitionProtocol(c.proc, p), "from an older version")
			require.Equal(t, client.calls, client.releases)
		})
	}
}

func (c *expressionVersionClient) NewRequest(m query.CmdMethod) *query.Request {
	return &query.Request{CmdMethod: m}
}
func (c *expressionVersionClient) SendMessage(ctx context.Context, addr string, _ *query.Request) (*query.Response, error) {
	c.calls++
	if c.onSend != nil {
		defer c.onSend()
	}
	if c.customResponse {
		return c.response, c.sendErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	version := c.version
	if c.versions != nil {
		version = c.versions[addr]
	}
	return &query.Response{GetProtocolVersion: &query.GetProtocolVersionResponse{Version: version}}, nil
}
func (c *expressionVersionClient) Release(*query.Response) { c.releases++ }
func expressionProtocolTestCompile(t testing.TB) (*Compile, *expressionVersionClient) {
	t.Helper()
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.ncpu = 4
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	oldVersion, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldCluster, hadCluster := rt.GetGlobalVariables(moruntime.ClusterService)
	cluster := &schedulerTestCluster{cns: []metadata.CNService{{ServiceID: "old-worker", QueryAddress: "worker:9000", PipelineServiceAddress: "remote:6001"}}}
	rt.SetGlobalVariables(moruntime.ClusterService, cluster)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	t.Cleanup(func() {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		if hadCluster {
			rt.SetGlobalVariables(moruntime.ClusterService, oldCluster)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.ClusterService, cluster)
		}
	})
	client := &expressionVersionClient{version: defines.MORPCVersion66}
	c.proc.Base.QueryClient = client
	return c, client
}
func TestExpressionProtocolUnknownAndCanceledWorkers(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	c.proc.Base.QueryClient = nil
	supported, err := remoteWorkersSupportProtocol(c.proc, engine.Nodes{{Id: "old-worker"}}, defines.MORPCVersion67)
	require.NoError(t, err)
	require.False(t, supported)
	c.proc.Base.QueryClient = client
	client.version = defines.MORPCVersion67
	supported, err = remoteWorkersSupportProtocol(c.proc, engine.Nodes{{Id: "old-worker", Addr: "stale:6001"}}, defines.MORPCVersion67)
	require.NoError(t, err)
	require.False(t, supported, "a new query endpoint cannot validate a stale pipeline address")
	supported, err = remoteWorkersSupportProtocol(c.proc, engine.Nodes{{Id: "wrong-worker", Addr: "remote:6001"}}, defines.MORPCVersion67)
	require.NoError(t, err)
	require.False(t, supported)
	supported, err = remoteWorkersSupportProtocol(c.proc, engine.Nodes{{}}, defines.MORPCVersion67)
	require.NoError(t, err)
	require.False(t, supported)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	value, _ := rt.GetGlobalVariables(moruntime.ClusterService)
	cluster := value.(*schedulerTestCluster)
	cluster.cns[0].QueryAddress = ""
	supported, err = remoteWorkersSupportProtocol(c.proc, engine.Nodes{{Id: "old-worker", Addr: "remote:6001"}}, defines.MORPCVersion67)
	require.NoError(t, err)
	require.False(t, supported)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, "unknown")
	supported, err = remoteWorkersSupportProtocol(c.proc, engine.Nodes{{Id: "old-worker", Addr: "remote:6001"}}, defines.MORPCVersion67)
	require.NoError(t, err)
	require.False(t, supported)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	expired, expire := context.WithDeadline(c.proc.Ctx, time.Now().Add(-time.Second))
	defer expire()
	parent := c.proc.Ctx
	c.proc.Ctx = expired
	_, err = remoteWorkersSupportProtocol(c.proc, nil, defines.MORPCVersion67)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	ctx, cancel := context.WithCancel(parent)
	cancel()
	c.proc.Ctx = ctx
	_, err = remoteWorkersSupportProtocol(c.proc, engine.Nodes{{Id: "old-worker"}}, defines.MORPCVersion67)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, client.calls)
}

func TestExpressionProtocolResolvesLegacyAddressOnlyScopes(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	client.version = defines.MORPCVersion67
	supported, err := remoteWorkersSupportProtocol(c.proc,
		engine.Nodes{{Addr: "remote:6001"}}, defines.MORPCVersion67)
	require.NoError(t, err)
	require.True(t, supported)
	require.Equal(t, 1, client.calls)
	require.Equal(t, client.calls, client.releases)
}

func TestAllCNsSupportProtocolProbesEveryCN(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	client.version = defines.MORPCVersion86

	supported, err := AllCNsSupportProtocol(c.proc, defines.MORPCVersion86)
	require.NoError(t, err)
	require.True(t, supported)
	require.Equal(t, 1, client.calls)

	client.version = defines.MORPCVersion85
	supported, err = AllCNsSupportProtocol(c.proc, defines.MORPCVersion86)
	require.NoError(t, err)
	require.False(t, supported)

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion85)
	supported, err = AllCNsSupportProtocol(c.proc, defines.MORPCVersion86)
	require.NoError(t, err)
	require.False(t, supported)
}

func TestAllCNsSupportProtocolWithContextIgnoresCanceledProcessContext(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	client.version = defines.MORPCVersion86
	procCtx, cancel := context.WithCancel(c.proc.Ctx)
	c.proc.Ctx = procCtx
	cancel()

	supported, err := AllCNsSupportProtocolWithContext(
		context.Background(), c.proc, defines.MORPCVersion86)
	require.NoError(t, err)
	require.True(t, supported)
	require.Equal(t, 1, client.calls)
}

func TestExpressionProtocolFailedResponsesAreReleased(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *query.Response
		err      error
	}{
		{"missing response", nil, nil},
		{"missing version", &query.Response{}, nil},
		{"error without response", nil, errors.New("probe failed")},
		{"error with response", &query.Response{}, errors.New("probe failed")},
		{"error with supported version", &query.Response{GetProtocolVersion: &query.GetProtocolVersionResponse{Version: defines.MORPCLatestVersion}}, errors.New("probe failed")},
		{"deadline with live parent", nil, context.DeadlineExceeded},
		{"canceled probe with live parent", nil, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			client.customResponse = true
			client.response = tc.response
			client.sendErr = tc.err
			ok, err := remoteWorkersSupportProtocol(c.proc, engine.Nodes{{Id: "old-worker", Addr: "remote:6001"}}, defines.MORPCVersion67)
			require.ErrorIs(t, err, tc.err)
			require.NoError(t, c.proc.Ctx.Err())
			require.False(t, ok, "failed or unknown observations cannot verify support")
			want := 0
			if tc.response != nil {
				want = 1
			}
			require.Equal(t, want, client.releases)
		})
	}
}

func TestExpressionProtocolDiscoveryFailure(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	old, _ := rt.GetGlobalVariables(moruntime.ClusterService)
	t.Cleanup(func() { rt.SetGlobalVariables(moruntime.ClusterService, old) })
	rt.SetGlobalVariables(moruntime.ClusterService, "invalid cluster")
	workers := engine.Nodes{{Id: "old-worker", Addr: "remote:6001"}}
	supported, err := remoteWorkersSupportProtocol(c.proc, workers, defines.MORPCVersion67)
	require.False(t, supported)
	require.ErrorContains(t, err, "invalid mocluster service")
	supported, err = remoteWorkersSupportProtocolForPlacement(c.proc, workers, defines.MORPCVersion67)
	require.False(t, supported)
	require.NoError(t, err)
	require.Zero(t, client.calls)
}

func TestRemoteExpressionPlacementRechecksGenerationAndWorkers(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	ip, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "inet_aton", []*planpb.Expr{{
		Typ:  planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}})
	require.NoError(t, err)
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{
		ip, decimalDivisionProtocolExpr(types.T_decimal128),
	}}}, Steps: []int32{0}}
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	cluster, _ := rt.GetGlobalVariables(moruntime.ClusterService)
	cluster.(*schedulerTestCluster).cns = append(cluster.(*schedulerTestCluster).cns,
		metadata.CNService{ServiceID: "second-worker", QueryAddress: "second:9000", PipelineServiceAddress: "second:6001"})
	workers := engine.Nodes{
		{Id: "old-worker", Addr: "remote:6001", Mcpu: 4},
		{Id: "second-worker", Addr: "second:6001", Mcpu: 4},
	}
	client.versions = map[string]int64{"worker:9000": defines.MORPCVersion97, "second:9000": defines.MORPCVersion96}
	place := func(want plan2.ExecType, probes int) {
		t.Helper()
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = workers
		calls, releases := client.calls, client.releases
		require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
		require.Equal(t, want, c.execType)
		require.Equal(t, probes, client.calls-calls)
		require.Equal(t, probes, client.releases-releases)
		if want == plan2.ExecTypeAP_ONECN {
			require.Len(t, c.cnList, 1)
			require.Equal(t, c.addr, c.cnList[0].Addr)
		}
	}
	// The lower-version feature succeeds on both workers, but DIV still
	// requires v97 on the second. One probe per worker proves both floors.
	place(plan2.ExecTypeAP_ONECN, 2)
	client.versions["second:9000"] = defines.MORPCVersion97
	place(plan2.ExecTypeAP_MULTICN, 2)
	// Mutating the same query and reusing Compile must recompute its floor.
	qry.Nodes[0].ProjectList = []*planpb.Expr{ip}
	client.versions["second:9000"] = defines.MORPCVersion72
	place(plan2.ExecTypeAP_MULTICN, 2)
	client.versions["second:9000"] = defines.MORPCVersion71
	place(plan2.ExecTypeAP_ONECN, 2)
	// View metadata joins the same placement floor and live worker checks.
	for _, id := range []int32{function.MO_VIEW_DEFINITION, function.MO_VIEW_CHECK_OPTION} {
		qry.Nodes[0].ProjectList = []*planpb.Expr{{Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(id, 0)},
		}}}}
		client.versions["worker:9000"] = defines.MORPCVersion126
		client.versions["second:9000"] = defines.MORPCVersion109
		place(plan2.ExecTypeAP_ONECN, 2)
		client.versions["second:9000"] = defines.MORPCVersion113
		place(plan2.ExecTypeAP_ONECN, 2)
		client.versions["second:9000"] = defines.MORPCVersion114
		place(plan2.ExecTypeAP_ONECN, 2)
		client.versions["second:9000"] = defines.MORPCVersion115
		place(plan2.ExecTypeAP_ONECN, 2)
		client.versions["second:9000"] = defines.MORPCVersion116
		place(plan2.ExecTypeAP_ONECN, 2)
		client.versions["second:9000"] = defines.MORPCVersion117
		place(plan2.ExecTypeAP_ONECN, 2)
		client.versions["second:9000"] = defines.MORPCVersion118
		place(plan2.ExecTypeAP_ONECN, 2)
		client.versions["second:9000"] = defines.MORPCVersion119
		place(plan2.ExecTypeAP_ONECN, 2)
		client.versions["second:9000"] = defines.MORPCVersion121
		place(plan2.ExecTypeAP_ONECN, 2)
		client.versions["second:9000"] = defines.MORPCVersion122
		place(plan2.ExecTypeAP_ONECN, 2)
		client.versions["second:9000"] = defines.MORPCVersion120
		place(plan2.ExecTypeAP_ONECN, 2)
		client.versions["second:9000"] = defines.MORPCVersion126
		place(plan2.ExecTypeAP_MULTICN, 2)
	}
	qry.Nodes[0].ProjectList = []*planpb.Expr{plan2.MakePlan2Int64ConstExprWithType(1)}
	place(plan2.ExecTypeAP_MULTICN, 0)
}

func TestRemoteExpressionPlacementLocalRebindAndErrors(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	legacy := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.TO_INTERVAL, 0)},
	}}}
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{legacy}}}}
	for _, kind := range []plan2.ExecType{plan2.ExecTypeTP, plan2.ExecTypeAP_ONECN, plan2.ExecTypeAP_MULTICN} {
		c.execType = kind
		require.ErrorContains(t, c.constrainRemoteExpressionWorkers(qry), "legacy interval")
		require.Equal(t, kind, c.execType)
	}
	malformed := integerProtocolExpr(function.IntegerArgumentCastOverload)
	malformed.GetF().Args = nil
	qry.Nodes[0].ProjectList = []*planpb.Expr{malformed}
	require.ErrorContains(t, c.constrainRemoteExpressionWorkers(qry), "CAST arity")
	require.Zero(t, client.calls)
	qry.Nodes[0].ProjectList = []*planpb.Expr{decimalDivisionProtocolExpr(types.T_decimal128)}
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001"}}
	ctx, cancel := context.WithCancel(c.proc.Ctx)
	cancel()
	c.proc.Ctx = ctx
	require.ErrorIs(t, c.constrainRemoteExpressionWorkers(qry), context.Canceled)
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	require.Zero(t, client.calls)
}

func TestRemoteExpressionPlacementProbeFailure(t *testing.T) {
	for _, probeErr := range []error{errors.New("transport unavailable"), context.DeadlineExceeded, context.Canceled} {
		t.Run(probeErr.Error(), func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{decimalDivisionProtocolExpr(types.T_decimal128)}}}}
			workers := engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
			client.customResponse, client.sendErr = true, probeErr
			c.execType, c.cnList = plan2.ExecTypeAP_MULTICN, workers
			require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
			require.NoError(t, c.proc.Ctx.Err())
			require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
			require.Len(t, c.cnList, 1)
			require.Equal(t, c.addr, c.cnList[0].Addr)
			ctx, cancel := context.WithCancel(c.proc.Ctx)
			defer cancel()
			c.proc.Ctx, client.onSend = ctx, cancel
			c.execType, c.cnList = plan2.ExecTypeAP_MULTICN, workers
			require.ErrorIs(t, c.constrainRemoteExpressionWorkers(qry), context.Canceled)
			require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
			require.Equal(t, workers, c.cnList)
		})
	}
}

// Exercise the real send boundary, including serialization and all fences;
// version probes are counted independently by the query-client fixture.
func BenchmarkRemoteExpressionSend(b *testing.B) {
	for _, shape := range []string{"plain", "mixed", "wide"} {
		b.Run(shape, func(b *testing.B) {
			c, client := expressionProtocolTestCompile(b)
			client.version = defines.MORPCLatestVersion
			integer := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_uint64)}, Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.PLUS, 2)},
				Args: []*planpb.Expr{
					{Typ: planpb.Type{Id: int32(types.T_uint64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
					{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
				},
			}}}
			ip, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "inet_aton", []*planpb.Expr{{
				Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 2}},
			}})
			require.NoError(b, err)
			expressions := []*planpb.Expr{integer, decimalDivisionProtocolExpr(types.T_decimal128), ip}
			if shape == "plain" {
				expressions = []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}}
			} else if shape == "wide" {
				expressions = append(expressions, expressions...)
				expressions = append(expressions, expressions...)
				expressions = append(expressions, expressions...)
				expressions = append(expressions, expressions...)
			}
			op := projection.NewArgument()
			op.ProjectList = expressions
			b.Cleanup(op.Release)
			scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := encodeRemoteScope(scope, c.proc); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(client.calls)/float64(b.N), "probes/op")
		})
	}
}

func TestRemoteExpressionSendReusesProbeAndRechecksNextSend(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	integer := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_uint64)}, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.PLUS, 2)},
		Args: []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(types.T_uint64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
			{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
		},
	}}}
	op := projection.NewArgument()
	op.ProjectList = []*planpb.Expr{integer, decimalDivisionProtocolExpr(types.T_decimal128)}
	t.Cleanup(op.Release)
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	for _, tc := range []struct {
		version int64
		message string
	}{
		{defines.MORPCVersion70, "checked integer arithmetic (MORPC version 71)"},
		{defines.MORPCVersion90, "decimal division (MORPC version 97)"},
		{defines.MORPCVersion97, ""},
	} {
		client.version = tc.version
		calls, releases := client.calls, client.releases
		data, err := encodeRemoteScope(scope, c.proc)
		if tc.message == "" {
			require.NoError(t, err)
			require.NotEmpty(t, data)
		} else {
			require.ErrorContains(t, err, tc.message)
		}
		require.Equal(t, 1, client.calls-calls)
		require.Equal(t, 1, client.releases-releases)
	}
	client.customResponse, client.sendErr = true, errors.New("destination unavailable")
	calls := client.calls
	data, err := encodeRemoteScope(scope, c.proc)
	require.ErrorIs(t, err, client.sendErr)
	require.Empty(t, data)
	require.Equal(t, calls+1, client.calls)
	client.customResponse, client.sendErr = false, nil
	// Reuse the same Scope, changing its expression generation after successful
	// encoding: an old observation cannot authorize a later send.
	op.ProjectList = []*planpb.Expr{integer}
	client.version = defines.MORPCVersion71
	before := client.calls
	_, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.Equal(t, 1, client.calls-before)
	op.ProjectList = nil
	_, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.Equal(t, 1, client.calls-before, "no expression floor means no probe")
}

func TestRemoteExpressionDestinationPreservesLiveGuards(t *testing.T) {
	for _, mode := range []string{"cancel", "cancel-error", "cancel-unknown", "downgrade", "upgrade"} {
		t.Run(mode, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			client.version = defines.MORPCLatestVersion
			p := &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}}
			features := planpb.RemoteExpressionFeatures{IntegerArithmeticDomains: true, RowDependentConvBases: true}
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			if mode == "cancel" || mode == "cancel-error" || mode == "cancel-unknown" {
				features = planpb.RemoteExpressionFeatures{IntegerArithmeticDomains: true}
				if mode != "cancel" {
					client.customResponse = true
				}
				if mode == "cancel-error" {
					client.sendErr = errors.New("transport failure during cancel")
				}
				ctx, cancel := context.WithCancel(c.proc.Ctx)
				defer cancel()
				c.proc.Ctx = ctx
				client.onSend = cancel
			} else if mode == "downgrade" {
				client.onSend = func() { rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion69) }
			} else {
				features = planpb.RemoteExpressionFeatures{IntegerArithmeticDomains: true, DecimalDivisionSemantics: true}
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion71)
				client.onSend = func() { rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion97) }
			}
			err := validateRemoteExpressionDestination(c.proc, p, features)
			if mode == "cancel" || mode == "cancel-error" || mode == "cancel-unknown" {
				require.ErrorIs(t, err, context.Canceled)
			} else if mode == "downgrade" {
				require.ErrorContains(t, err, "row-dependent CONV bases (MORPC version 70)")
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, client.calls)
			releases := 1
			if mode == "cancel-error" || mode == "cancel-unknown" {
				releases = 0
			}
			require.Equal(t, releases, client.releases)
		})
	}
}

func TestRemoteExpressionDestinationUnknownEvidence(t *testing.T) {
	for _, mode := range []string{"nil", "empty", "error", "error-response", "wrong-id", "stale-address", "no-client", "no-node", "no-features"} {
		t.Run(mode, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			client.version = defines.MORPCLatestVersion
			p := &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"}}
			f := planpb.RemoteExpressionFeatures{IntegerArithmeticDomains: true, DecimalDivisionSemantics: true}
			expectedCalls, expectedReleases := 0, 0
			switch mode {
			case "nil", "empty", "error", "error-response":
				client.customResponse = true
				expectedCalls = 1
				if mode == "empty" || mode == "error-response" {
					client.response = &query.Response{}
					expectedReleases = 1
				}
				if mode == "error" || mode == "error-response" {
					client.sendErr = errors.New("probe failed")
				}
			case "wrong-id":
				p.Node.Id = "other-worker"
			case "stale-address":
				p.Node.Addr = "stale:6001"
			case "no-client":
				c.proc.Base.QueryClient = nil
			case "no-node":
				p.Node = nil
			case "no-features":
				f = planpb.RemoteExpressionFeatures{}
			}
			err := validateRemoteExpressionDestination(c.proc, p, f)
			if mode == "no-features" {
				require.NoError(t, err)
			} else if client.sendErr != nil {
				require.ErrorIs(t, err, client.sendErr)
			} else if mode == "no-node" {
				require.ErrorContains(t, err, "versioned remote destination")
			} else {
				require.ErrorContains(t, err, "checked integer arithmetic (MORPC version 71)")
			}
			require.Equal(t, expectedCalls, client.calls)
			require.Equal(t, expectedReleases, client.releases)
		})
	}
}
