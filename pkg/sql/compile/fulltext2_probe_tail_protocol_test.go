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

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func ft2CfgExpr(sval string) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: sval}}}}
}

func ft2ProbeTailScope(c *Compile, cfg string) *Scope {
	op := table_function.NewArgument()
	op.FuncName = fulltext2SearchFuncName
	op.Args = []*plan.Expr{ft2CfgExpr(cfg), ft2CfgExpr("pattern")}
	return &Scope{
		Magic:    Remote,
		Proc:     c.proc,
		NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"},
		RootOp:   op,
	}
}

// A self-completing fulltext2 json probe planned at v73 must not be shipped unchanged to a CN whose
// current level fell back to v72 between plan build and serialization: the old CN drops the
// probe_tail fields and the mandatory INNER JOIN silently loses rows. encodeRemoteScope must reject.
func TestFulltext2ProbeTailRejectsOnRollback(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	c.proc.Base.QueryClient = client
	rt := runtime.ServiceRuntime(c.proc.GetService())

	scope := ft2ProbeTailScope(c, `{"probe_tail":true}`)
	defer scope.RootOp.Release()

	// Built at the latest level; the fleet then rolls back below the gate before the send.
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion72)
	_, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "MORPC version 73")

	// At the gate it serializes.
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion73)
	client.version = defines.MORPCVersion73
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.NotEmpty(t, data)
}

// A plain fulltext2_search (no probe_tail) is wire-compatible with older CNs and must not be fenced.
func TestFulltext2SearchWithoutProbeTailNotFenced(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	c.proc.Base.QueryClient = client
	rt := runtime.ServiceRuntime(c.proc.GetService())

	scope := ft2ProbeTailScope(c, `{"probe_tail":false}`)
	defer scope.RootOp.Release()

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion72)
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.NotEmpty(t, data)
}

func TestPipelineHasFulltext2ProbeTail(t *testing.T) {
	probe := &pipeline.TableFunction{Name: fulltext2SearchFuncName, Args: []*plan.Expr{ft2CfgExpr(`{"probe_tail":true}`)}}
	plainMatch := &pipeline.TableFunction{Name: fulltext2SearchFuncName, Args: []*plan.Expr{ft2CfgExpr(`{"probe_tail":false}`)}}
	otherTVF := &pipeline.TableFunction{Name: "unnest", Args: []*plan.Expr{ft2CfgExpr(`{"probe_tail":true}`)}}

	require.False(t, pipelineHasFulltext2ProbeTail(nil))
	require.False(t, pipelineHasFulltext2ProbeTail(&pipeline.Pipeline{}))
	require.True(t, pipelineHasFulltext2ProbeTail(&pipeline.Pipeline{
		InstructionList: []*pipeline.Instruction{{TableFunction: probe}},
	}))
	require.False(t, pipelineHasFulltext2ProbeTail(&pipeline.Pipeline{
		InstructionList: []*pipeline.Instruction{{TableFunction: plainMatch}},
	}))
	// Name mismatch: probe_tail on a non-fulltext2 TVF is not this contract.
	require.False(t, pipelineHasFulltext2ProbeTail(&pipeline.Pipeline{
		InstructionList: []*pipeline.Instruction{{TableFunction: otherTVF}},
	}))
	// Found in a child pipeline.
	require.True(t, pipelineHasFulltext2ProbeTail(&pipeline.Pipeline{
		Children: []*pipeline.Pipeline{{InstructionList: []*pipeline.Instruction{{TableFunction: probe}}}},
	}))
}
