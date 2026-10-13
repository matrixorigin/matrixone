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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/apply"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/stretchr/testify/require"
)

func TestJSONTableCoreCoordinatorPlacement(t *testing.T) {
	for _, correlated := range []bool{false, true} {
		t.Run(map[bool]string{false: "function_scan", true: "apply"}[correlated], func(t *testing.T) {
			c := newLazyUnionAllTestCompile(t)
			c.addr = "coordinator:6001"
			leaf := newLazyUnionAllLeaf(c, value_scan.NewArgument())
			leaf.NodeInfo.Addr = "remote-input:6001"
			leaf.NodeInfo.Mcpu = 2
			right := &plan.Node{NodeType: plan.Node_FUNCTION_SCAN, Children: []int32{0},
				TableDef: &plan.TableDef{TblFunc: &plan.TableFunction{Name: "json_table", IsSingle: true}}}
			var result []*Scope
			if correlated {
				result = c.compileApply(&plan.Node{ApplyType: plan.Node_CROSSAPPLY}, right, []*Scope{leaf})
				require.IsType(t, &apply.Apply{}, result[0].RootOp)
			} else {
				var err error
				result, err = c.compileTableFunction(right, []*Scope{leaf})
				require.NoError(t, err)
				require.IsType(t, &table_function.TableFunction{}, result[0].RootOp)
			}
			t.Cleanup(func() { freeLazyUnionAllTestScope(c, result[0]) })
			require.Len(t, result, 1)
			require.NotSame(t, leaf, result[0])
			require.Equal(t, c.addr, result[0].NodeInfo.Addr)
			require.Equal(t, 1, result[0].NodeInfo.Mcpu)
			require.Equal(t, "remote-input:6001", result[0].PreScopes[0].NodeInfo.Addr)
		})
	}
}
