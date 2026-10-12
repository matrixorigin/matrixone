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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestViewMetadataSenderOrdinaryPayloadNeedsNoProbe(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	// Scalar bytes are opaque data, not expressions or function identities.
	p := &pipeline.Pipeline{
		Node:       &pipeline.NodeInfo{Id: "old-worker", Addr: "remote:6001"},
		DataSource: &pipeline.Source{MembershipFilter: []byte{1}},
		InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{
			{Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Data: []byte{1}}}},
			{Expr: &planpb.Expr_Fold{Fold: &planpb.FoldVal{Data: []byte{1}}}},
		}}},
	}
	// Main owns the previous VIEWS candidate IDs for hyperbolic functions.
	// Those functions must not acquire a VIEWS floor or capability probe.
	for _, id := range []int32{function.TANH, function.COSH, function.ASINH, function.ACOSH, function.ATANH} {
		p.InstructionList[0].ProjectList = append(p.InstructionList[0].ProjectList, &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_float64)},
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(id, 0)},
				Args: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_float64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}},
			}},
		})
	}
	features, err := planpb.RequiredRemoteExpressionFeatures(p)
	require.NoError(t, err)
	require.False(t, features.Any())
	require.NoError(t, validateRemoteExpressionFeatures(c.proc, features))
	require.NoError(t, validateRemoteExpressionDestination(c.proc, p, features))
	require.Zero(t, client.calls)
	require.Zero(t, client.releases)
	// An ordinary pipeline needs no capability evidence, even without a node.
	p.Node = nil
	require.NoError(t, validateRemoteExpressionDestination(nil, p, features))
	allocs := testing.AllocsPerRun(100, func() {
		if err := validateRemoteExpressionDestination(c.proc, p, features); err != nil {
			panic(err)
		}
	})
	require.Zero(t, allocs, "ordinary sender capability gating must not allocate")
}

func TestViewMetadataSenderSharesExpressionProbe(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	op := projection.NewArgument()
	t.Cleanup(op.Release)
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	for _, tc := range []struct {
		id   int32
		name string
		typ  types.T
	}{
		{function.MO_VIEW_DEFINITION, "mo_view_definition", types.T_text},
		{function.MO_VIEW_CHECK_OPTION, "mo_view_check_option", types.T_varchar},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := &planpb.Expr{Typ: planpb.Type{Id: int32(tc.typ)}, Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(tc.id, 0), ObjName: tc.name},
				Args: []*planpb.Expr{plan2.MakePlan2StringConstExprWithType("{}", false)},
			}}}
			for _, mode := range []string{"standalone", "mixed"} {
				t.Run(mode, func(t *testing.T) {
					op.ProjectList = []*planpb.Expr{expr}
					if mode == "mixed" {
						op.ProjectList = append(op.ProjectList, decimalDivisionProtocolExpr(types.T_decimal128))
					}
					features, err := planpb.RequiredRemoteExpressionFeatures(&pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: op.ProjectList}}})
					require.NoError(t, err)
					require.True(t, features.ViewMetadataFunctions, "shared analysis must match the actual registry IDs")
					require.Equal(t, mode == "mixed", features.DecimalDivisionSemantics)
					require.Equal(t, defines.MORPCVersion112, remoteExpressionProtocolVersion(features))
					// No capability observation survives a send, including after
					// success followed by rollback of the very same endpoint.
					for _, version := range []int64{defines.MORPCVersion109, defines.MORPCVersion110, defines.MORPCVersion111, defines.MORPCVersion112, defines.MORPCVersion111, 0} {
						client.version = version
						calls, releases := client.calls, client.releases
						data, err := encodeRemoteScope(scope, c.proc)
						if version == defines.MORPCVersion112 {
							require.NoError(t, err)
							require.NotEmpty(t, data)
						} else if version == 0 && mode == "mixed" {
							require.ErrorContains(t, err, "remote destination does not support decimal division")
						} else {
							require.ErrorContains(t, err, "remote destination does not support view metadata functions (MORPC version 112)")
						}
						require.Equal(t, 1, client.calls-calls)
						require.Equal(t, 1, client.releases-releases)
					}
				})
			}
		})
	}
}

// Measure feature analysis plus the sender gate, excluding protobuf marshaling
// and fixture allocation. Bytes must affect serialization, but not this check.
func BenchmarkViewMetadataSenderProtocol(b *testing.B) {
	for _, kind := range []string{"membership", "literal_vec", "fold"} {
		for _, size := range []int{0, 1 << 20, 4 << 20} {
			b.Run(fmt.Sprintf("%s_bytes=%d", kind, size), func(b *testing.B) {
				payload := make([]byte, size)
				p := &pipeline.Pipeline{}
				switch kind {
				case "membership":
					p.DataSource = &pipeline.Source{MembershipFilter: payload}
				case "literal_vec":
					p.InstructionList = []*pipeline.Instruction{{ProjectList: []*planpb.Expr{{Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Data: payload}}}}}}
				case "fold":
					p.InstructionList = []*pipeline.Instruction{{ProjectList: []*planpb.Expr{{Expr: &planpb.Expr_Fold{Fold: &planpb.FoldVal{Data: payload}}}}}}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					features, err := planpb.RequiredRemoteExpressionFeatures(p)
					if err != nil {
						b.Fatal(err)
					}
					if err := validateRemoteExpressionFeatures(nil, features); err != nil {
						b.Fatal(err)
					}
					if err := validateRemoteExpressionDestination(nil, p, features); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
