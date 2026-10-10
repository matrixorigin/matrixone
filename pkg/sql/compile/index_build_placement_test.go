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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/apply"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestIndexBuildApplyOwnsCoordinatorWorkspace(t *testing.T) {
	nodes := engine.Nodes{
		{Id: "cn-local", Addr: "cn-local:6001", Mcpu: 4},
		{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 4},
	}
	for _, name := range []string{"fulltext2_create", "hnsw_create", "mo_view_columns"} {
		for _, applyType := range []plan.Node_ApplyType{plan.Node_CROSSAPPLY, plan.Node_OUTERAPPLY} {
			t.Run(name+"/"+applyType.String(), func(t *testing.T) {
				c := newCompileForShuffleJoinTest(t, nodes)
				c.execType = plan2.ExecTypeAP_MULTICN
				inputs := []*Scope{
					newRemoteMergeInputForTest(c, nodes[0], 0),
					newRemoteMergeInputForTest(c, nodes[1], 0),
				}
				inputs[0].NodeInfo.Mcpu = 4
				inputs[1].NodeInfo.Mcpu = 4
				right := &plan.Node{TableDef: &plan.TableDef{TblFunc: &plan.TableFunction{
					Name: name, IsSingle: true,
				}}}
				result := c.compileApply(&plan.Node{ApplyType: applyType}, right, inputs)
				require.Len(t, result, 1, "one transaction-owning builder, not one per input CN")
				scope := result[0]
				require.Equal(t, nodes[0].Addr, scope.NodeInfo.Addr)
				require.Equal(t, 1, scope.NodeInfo.Mcpu)
				require.Same(t, c.proc.Base, scope.Proc.Base, "must share the coordinator transaction")
				require.Equal(t, inputs, scope.PreScopes, "source scans stay distributed")
				require.Equal(t, nodes[1].Addr, inputs[1].NodeInfo.Addr)
				for _, input := range inputs {
					require.Equal(t, 4, input.NodeInfo.Mcpu, "builder placement must not serialize source scans")
				}
				op, ok := scope.RootOp.(*apply.Apply)
				require.True(t, ok)
				require.Equal(t, name, op.TableFunction.FuncName)
				_, ok = op.GetOperatorBase().GetChildren(0).(*merge.Merge)
				require.True(t, ok)
			})
		}
	}
}

func TestSearchApplyPreservesInputPlacement(t *testing.T) {
	nodes := engine.Nodes{
		{Id: "cn-local", Addr: "cn-local:6001", Mcpu: 4},
		{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 4},
	}
	for _, name := range []string{"unnest", "generate_series"} {
		t.Run(name, func(t *testing.T) {
			c := newCompileForShuffleJoinTest(t, nodes)
			inputs := []*Scope{
				newRemoteMergeInputForTest(c, nodes[0], 0),
				newRemoteMergeInputForTest(c, nodes[1], 0),
			}
			for _, input := range inputs {
				input.NodeInfo.Mcpu = 4
			}
			right := &plan.Node{TableDef: &plan.TableDef{TblFunc: &plan.TableFunction{Name: name}}}
			result := c.compileApply(&plan.Node{ApplyType: plan.Node_CROSSAPPLY}, right, inputs)
			require.Equal(t, inputs, result)
			for i, scope := range result {
				require.Equal(t, nodes[i], scope.NodeInfo)
				require.Empty(t, scope.PreScopes)
			}
		})
	}
}

func TestTableFunctionWriterPlacement(t *testing.T) {
	nodes := engine.Nodes{
		{Id: "cn-local", Addr: "cn-local:6001", Mcpu: 4},
		{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 4},
	}
	for _, name := range []string{"fulltext2_create", "hnsw_create", "unnest"} {
		for _, withInput := range []bool{false, true} {
			inputName := "/standalone"
			if withInput {
				inputName = "/with-input"
			}
			t.Run(name+inputName, func(t *testing.T) {
				c := newCompileForShuffleJoinTest(t, nodes)
				node := &plan.Node{TableDef: &plan.TableDef{TblFunc: &plan.TableFunction{Name: name}}}
				var inputs []*Scope
				if withInput {
					node.Children = []int32{0}
					inputs = []*Scope{newRemoteMergeInputForTest(c, nodes[1], 0)}
				}
				result, err := c.compileTableFunction(node, inputs)
				require.NoError(t, err)
				require.Len(t, result, 1)
				_, ok := result[0].RootOp.(*table_function.TableFunction)
				require.True(t, ok)
				if withInput && name == "unnest" {
					require.Equal(t, inputs, result, "search retains the remote input")
					return
				}
				require.Equal(t, nodes[0].Addr, result[0].NodeInfo.Addr)
				require.Equal(t, 1, result[0].NodeInfo.Mcpu)
				require.Same(t, c.proc.Base, result[0].Proc.Base)
				if withInput {
					require.Equal(t, inputs, result[0].PreScopes)
				}
			})
		}
	}
}
