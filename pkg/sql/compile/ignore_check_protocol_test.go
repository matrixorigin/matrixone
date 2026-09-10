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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func ignoreCheckTestExpr() *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.CHECK_CONSTRAINT_ASSERT, 0)},
			Args: []*plan.Expr{
				{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
				plan2.MakePlan2StringConstExprWithType("Check constraint 'ck_pos' is violated."),
			},
		}},
	}
}

func TestIgnoreCheckCoordinatorCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version any
		ignore  bool
		local   bool
	}{
		{"old-ignore", defines.MORPCVersion58, true, true},
		{"unknown-ignore", nil, true, true},
		{"malformed-ignore", "59", true, true},
		{"pending-v60", int64(60), true, true},
		{"pending-v61", int64(61), true, true},
		{"new-ignore", defines.MORPCVersion62, true, false},
		{"old-ordinary", defines.MORPCVersion58, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newLazyUnionAllTestCompile(t)
			c.addr = "coordinator:6001"
			c.proc.SetStmtProfile(&process.StmtProfile{})
			c.proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", tc.ignore)
			restoreRuntimeVariableForTest(t, c.proc.GetService(), runtime.MOProtocolVersion, tc.version)
			leaf := newLazyUnionAllLeaf(c, value_scan.NewArgument())
			leaf.NodeInfo.Addr = "older-cn:6001"
			node := &plan.Node{NodeType: plan.Node_FILTER, FilterList: []*plan.Expr{ignoreCheckTestExpr()}, FilterIsBarrier: true}
			result := c.compileRestrict(node, []*Scope{leaf})
			require.Len(t, result, 1)
			require.IsType(t, &filter.Filter{}, result[0].RootOp)
			if tc.local {
				require.NotSame(t, leaf, result[0])
				require.True(t, c.scopesRunOnCoordinator(result))
				require.IsType(t, &value_scan.ValueScan{}, leaf.RootOp.GetOperatorBase().GetChildren(0))
				require.Same(t, result[0], c.ensureCoordinatorOnlyFunctions(node, result)[0], "do not merge twice")
			} else {
				require.Same(t, leaf, result[0], "preserve distributed execution")
			}
			freeLazyUnionAllTestScope(c, result[0])
		})
	}
}

func TestStatementIgnoreEnabledHandlesIncompleteProcess(t *testing.T) {
	require.False(t, statementIgnoreEnabled(nil))
	require.False(t, statementIgnoreEnabled(&process.Process{}))
}

func TestIgnoreCheckRemoteEncodingRechecksCapability(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetStmtProfile(&process.StmtProfile{})
	proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)
	rt := runtime.ServiceRuntime(proc.GetService())
	restoreRuntimeVariableForTest(t, proc.GetService(), runtime.MOProtocolVersion, defines.MORPCVersion62)
	op := filter.NewArgument()
	op.FilterExprs = []*plan.Expr{ignoreCheckTestExpr()}
	defer op.Release()
	scope := &Scope{Proc: proc, RootOp: op}
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion62)
	_, err := encodeRemoteScope(scope, proc)
	require.NoError(t, err)
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion58)
	_, err = encodeRemoteScope(scope, proc)
	require.ErrorContains(t, err, "INSERT IGNORE CHECK semantics require MORPC protocol version 62")
	_, err = encodeScope(scope)
	require.ErrorContains(t, err, "INSERT IGNORE CHECK semantics require MORPC protocol version 62")
	proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", false)
	_, err = encodeRemoteScope(scope, proc)
	require.NoError(t, err, "ordinary CHECK keeps its existing throwing semantics")
	proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)
	op.FilterExprs = []*plan.Expr{plan2.MakePlan2BoolConstExprWithType(true)}
	_, err = encodeRemoteScope(scope, proc)
	require.NoError(t, err, "IGNORE without CHECK is not gated")
}

func TestIgnoreCheckCompiledFilterRows(t *testing.T) {
	for _, ignore := range []bool{true, false} {
		t.Run(map[bool]string{true: "ignore", false: "ordinary"}[ignore], func(t *testing.T) {
			c := newLazyUnionAllTestCompile(t)
			c.proc.SetStmtProfile(&process.StmtProfile{})
			c.proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", ignore)
			session := &remoteWarningSession{}
			c.proc.Session = session
			restoreRuntimeVariableForTest(t, c.proc.GetService(), runtime.MOProtocolVersion, defines.MORPCVersion58)
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = testutil.MakeBoolVector([]bool{false, true}, nil, c.proc.Mp())
			bat.SetRowCount(2)
			leaf := newLazyUnionAllLeaf(c, colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat}))
			leaf.NodeInfo.Addr = c.addr
			node := &plan.Node{NodeType: plan.Node_FILTER, FilterList: []*plan.Expr{ignoreCheckTestExpr()}, FilterIsBarrier: true}
			if !ignore {
				node.NodeType = plan.Node_ASSERT
			}
			result := c.compileRestrict(node, []*Scope{leaf})
			defer freeLazyUnionAllTestScope(c, result[0])
			op := result[0].RootOp
			require.NoError(t, op.Prepare(c.proc))
			out, err := vm.Exec(op, c.proc)
			if ignore {
				require.NoError(t, err)
				require.Equal(t, 1, out.Batch.RowCount())
				require.Equal(t, []bool{true}, vector.MustFixedColWithTypeCheck[bool](out.Batch.Vecs[0]))
				require.EqualValues(t, 1, session.totalWarnings)
				require.Equal(t, moerr.ER_CHECK_CONSTRAINT_VIOLATED, session.warnings[0].code)
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation))
				require.Zero(t, session.totalWarnings)
			}
		})
	}
}

func TestIgnoreCheckProtocolFastPath(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetStmtProfile(&process.StmtProfile{})
	proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)
	restoreRuntimeVariableForTest(t, proc.GetService(), runtime.MOProtocolVersion, defines.MORPCVersion62)
	wide := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: make([]*plan.Expr, 1000)}}}
	var err error
	allocs := testing.AllocsPerRun(100, func() {
		err = validateRemoteIgnoreCheckPipelineProtocol(proc, wide)
	})
	require.NoError(t, err)
	require.Zero(t, allocs, "upgraded clusters must not walk or allocate for the pipeline")
}
