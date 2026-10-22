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

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestJSONGroupConcatLoweredDescriptorProtocol(t *testing.T) {
	for _, merge := range []bool{false, true} {
		for _, distinct := range []bool{false, true} {
			for _, oid := range []types.T{types.T_json, types.T_varchar} {
				t.Run(fmt.Sprintf("merge%t/distinct%t/%s", merge, distinct, oid), func(t *testing.T) {
					c, client := expressionProtocolTestCompile(t)
					rt := moruntime.ServiceRuntime(c.proc.GetService())
					rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion129)
					args := []*planpb.Expr{{Typ: oid.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}}
					aggs := []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(aggexec.AggIdOfGroupConcat, distinct, args, nil)}
					var op vm.Operator
					if merge {
						g := group.NewArgumentMergeGroup()
						t.Cleanup(g.Release)
						g.Aggs = aggs
						op = g
					} else {
						g := group.NewArgument()
						t.Cleanup(g.Release)
						g.NeedEval, g.Aggs = true, aggs
						op = g
					}
					leaf := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
					// A child pipeline must retain the descriptor's independent floor.
					scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: leaf.NodeInfo, PreScopes: []*Scope{leaf}}
					client.version = defines.MORPCVersion129
					minimum := int64(0)
					data, err := encodeRemoteScopeWithProtocol(scope, c.proc, &minimum)
					require.NoError(t, err)
					p := new(pipeline.Pipeline)
					require.NoError(t, p.Unmarshal(data))
					features, err := requiredRemotePipelineExpressionFeatures(p)
					require.NoError(t, err)
					affected := oid == types.T_json
					require.Equal(t, affected, features.JSONOpaqueTextContracts)
					if affected {
						require.Equal(t, defines.MORPCVersion129, minimum)
						client.version = defines.MORPCVersion128
						_, sendErr := encodeRemoteScope(scope, c.proc)
						require.ErrorContains(t, sendErr, "version 129")
						rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion128)
						require.ErrorContains(t, validateRemoteAggregateProtocol(c.proc, aggs), "version 129")
						require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, p), "version 129")
						_, receiveErr := decodeScope(data, c.proc, true, nil)
						require.ErrorContains(t, receiveErr, "version 129")
					} else {
						require.Zero(t, minimum)
						client.version = defines.MORPCVersion122
						_, sendErr := encodeRemoteScope(scope, c.proc)
						require.NoError(t, sendErr)
					}
				})
			}
		}
	}
}
