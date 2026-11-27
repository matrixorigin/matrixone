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

func TestSnapshotAssertionExecutesOnCoordinator(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	c.addr = "coordinator:6001"
	leaf := newLazyUnionAllLeaf(c, value_scan.NewArgument())
	leaf.NodeInfo.Addr = "worker:6001"
	assertion := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.MO_ASSERT_SNAPSHOT_AFTER, 0)},
			Args: []*plan.Expr{plan2.MakePlan2StringConstExprWithType("1-0")},
		}},
	}
	node := &plan.Node{NodeType: plan.Node_FILTER, FilterList: []*plan.Expr{assertion}}
	result := c.compileRestrict(node, []*Scope{leaf})
	require.Len(t, result, 1)
	defer freeLazyUnionAllTestScope(c, result[0])
	require.NotSame(t, leaf, result[0])
	require.True(t, c.scopesRunOnCoordinator(result))
	require.IsType(t, &filter.Filter{}, result[0].RootOp)
	require.False(t, c.isRetryErr(moerr.NewSnapshotNotAfter(t.Context())))
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
