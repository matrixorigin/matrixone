// Copyright 2021 - 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestBuildCheckConstraintsProtocolGate(t *testing.T) {
	mock := NewMockOptimizer(false)
	builder := NewQueryBuilder(planpb.Query_SELECT, mock.CurrentContext(), false, true)
	ctx := NewBindContext(builder, nil)
	proc := builder.compCtx.GetProcess()
	rt := runtime.ServiceRuntime(proc.GetService())
	original, hadOriginal := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadOriginal {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion11)
	_, err := builder.buildCheckConstraints(&tree.TableFunction{Func: &tree.FuncExpr{}}, ctx, nil, nil)
	require.ErrorContains(t, err, "protocol version 14")

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion14)
	nodeID, err := builder.buildCheckConstraints(&tree.TableFunction{Func: &tree.FuncExpr{}}, ctx, nil, nil)
	require.NoError(t, err)
	require.Equal(t, planpb.Node_FUNCTION_SCAN, builder.qry.Nodes[nodeID].NodeType)
}

func TestPushdownLimitToCheckConstraintsFunctionScan(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false), false, true)
	limit := MakePlan2Uint64ConstExprWithType(1)
	functionScan := &planpb.Node{
		NodeType: planpb.Node_FUNCTION_SCAN,
		TableDef: &planpb.TableDef{TblFunc: &planpb.TableFunction{Name: "mo_check_constraints"}},
	}
	project := &planpb.Node{
		NodeType: planpb.Node_PROJECT,
		Children: []int32{0},
		Limit:    limit,
	}
	builder.qry.Nodes = []*planpb.Node{functionScan, project}

	builder.pushdownLimitToTableScan(1)
	require.Same(t, limit, functionScan.Limit)
	require.Nil(t, project.Limit)

	// OFFSET cannot be evaluated by the metadata producer without changing
	// which rows the outer query observes, so it must stay on the project.
	offset := MakePlan2Uint64ConstExprWithType(2)
	functionScan.Limit = nil
	project.Limit = limit
	project.Offset = offset
	builder.pushdownLimitToTableScan(1)
	require.Nil(t, functionScan.Limit)
	require.Same(t, limit, project.Limit)
}
