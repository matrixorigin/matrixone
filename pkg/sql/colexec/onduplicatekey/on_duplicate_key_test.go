// Copyright 2021 Matrix Origin
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

package onduplicatekey

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// add unit tests for cases

type onDupTestCase struct {
	arg      *OnDuplicatekey
	proc     *process.Process
	rowCount int
}

func makeTestCases(t *testing.T) []onDupTestCase {
	return []onDupTestCase{
		newTestCase(t),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestOnDuplicateKey(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		resetChildren(tc.arg, tc.proc.Mp())
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		ret, _ := vm.Exec(tc.arg, tc.proc)
		require.Equal(t, tc.rowCount, ret.Batch.RowCount())

		tc.arg.Reset(tc.proc, false, nil)

		resetChildren(tc.arg, tc.proc.Mp())
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		ret, _ = vm.Exec(tc.arg, tc.proc)
		require.Equal(t, tc.rowCount, ret.Batch.RowCount())

		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func TestOnDuplicateKeyIgnoreCleansConflictBatch(t *testing.T) {
	tc := newTestCase(t)
	tc.arg.IsIgnore = true
	tc.rowCount = 1

	resetChildren(tc.arg, tc.proc.Mp())
	err := tc.arg.Prepare(tc.proc)
	require.NoError(t, err)

	ret, execErr := vm.Exec(tc.arg, tc.proc)
	require.NoError(t, execErr)
	require.Equal(t, tc.rowCount, ret.Batch.RowCount())

	tc.arg.Free(tc.proc, false, nil)
	tc.proc.Free()
	require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
}

func TestCheckConflictReturnsBatchRowIndex(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	executors := newUniqueCheckExecutors(t, proc)

	newBatch := batch.New([]string{"b", "c"})
	newBatch.Vecs = []*vector.Vector{
		testutil.MakeInt64Vector([]int64{10}, nil, proc.Mp()),
		testutil.MakeInt64Vector([]int64{21}, nil, proc.Mp()),
	}
	newBatch.SetRowCount(1)
	defer newBatch.Clean(proc.Mp())

	checkConflictBatch := batch.New([]string{"b", "c", "b", "c"})
	checkConflictBatch.Vecs = []*vector.Vector{
		testutil.MakeInt64Vector([]int64{99}, nil, proc.Mp()),
		testutil.MakeInt64Vector([]int64{21}, nil, proc.Mp()),
		testutil.MakeInt64Vector([]int64{0}, nil, proc.Mp()),
		testutil.MakeInt64Vector([]int64{0}, nil, proc.Mp()),
	}
	checkConflictBatch.SetRowCount(1)
	defer checkConflictBatch.Clean(proc.Mp())

	conflictRowIdx, conflictMsg, err := checkConflict(proc, newBatch, checkConflictBatch, executors, []string{"b", "c"}, 2)
	require.NoError(t, err)
	require.Equal(t, 0, conflictRowIdx)
	require.Equal(t, "Duplicate entry for key 'c'", conflictMsg)
}

func TestCheckConflictReturnsLaterBatchRowIndex(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	executors := newUniqueCheckExecutors(t, proc)

	newBatch := batch.New([]string{"b", "c"})
	newBatch.Vecs = []*vector.Vector{
		testutil.MakeInt64Vector([]int64{99}, nil, proc.Mp()),
		testutil.MakeInt64Vector([]int64{22}, nil, proc.Mp()),
	}
	newBatch.SetRowCount(1)
	defer newBatch.Clean(proc.Mp())

	checkConflictBatch := batch.New([]string{"b", "c", "b", "c"})
	checkConflictBatch.Vecs = []*vector.Vector{
		testutil.MakeInt64Vector([]int64{10, 11, 12}, nil, proc.Mp()),
		testutil.MakeInt64Vector([]int64{20, 21, 22}, nil, proc.Mp()),
		testutil.MakeInt64Vector([]int64{0, 0, 0}, nil, proc.Mp()),
		testutil.MakeInt64Vector([]int64{0, 0, 0}, nil, proc.Mp()),
	}
	checkConflictBatch.SetRowCount(3)
	defer checkConflictBatch.Clean(proc.Mp())

	conflictRowIdx, conflictMsg, err := checkConflict(proc, newBatch, checkConflictBatch, executors, []string{"b", "c"}, 2)
	require.NoError(t, err)
	require.Equal(t, 2, conflictRowIdx)
	require.Equal(t, "Duplicate entry for key 'c'", conflictMsg)
}

func newUniqueCheckExecutors(t *testing.T, proc *process.Process) []colexec.ExpressionExecutor {
	t.Helper()

	intType := types.T_int64.ToType()
	oldBExpr := newPlanColExpr(&intType, 0, 0)
	newBExpr := newPlanColExpr(&intType, 1, 2)
	oldCExpr := newPlanColExpr(&intType, 0, 1)
	newCExpr := newPlanColExpr(&intType, 1, 3)

	bExpr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "=", []*plan.Expr{oldBExpr, newBExpr})
	require.NoError(t, err)
	cExpr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "=", []*plan.Expr{oldCExpr, newCExpr})
	require.NoError(t, err)

	executors, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, []*plan.Expr{bExpr, cExpr})
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, executor := range executors {
			executor.Free()
		}
	})
	return executors
}

func newPlanColExpr(typ *types.Type, relPos, colPos int32) *plan.Expr {
	return &plan.Expr{
		Typ: plan2.MakePlan2Type(typ),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: relPos,
				ColPos: colPos,
			},
		},
	}
}

func resetChildren(arg *OnDuplicatekey, m *mpool.MPool) {
	bat := batch.New([]string{"a", "b", "a", "b", catalog.Row_ID})
	vecs := make([]*vector.Vector, 5)
	vecs[0] = testutil.MakeInt64Vector([]int64{1, 1}, nil, m)
	vecs[1] = testutil.MakeInt64Vector([]int64{2, 2}, nil, m)
	vecs[2] = testutil.MakeInt64Vector([]int64{1, 1}, []uint64{0, 1}, m)
	vecs[3] = testutil.MakeInt64Vector([]int64{2, 2}, []uint64{0, 1}, m)
	uuid1 := objectio.NewSegmentid()
	blkId1 := objectio.NewBlockid(uuid1, 0, 0)
	rowid1 := objectio.NewRowid(blkId1, 0)
	rowid2 := objectio.NewRowid(blkId1, 0)
	vecs[4] = testutil.MakeRowIdVector([]types.Rowid{rowid1, rowid2}, []uint64{0, 1}, m)
	bat.Vecs = vecs
	bat.SetRowCount(vecs[0].Length())

	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}

func newTestCase(t *testing.T) onDupTestCase {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	pkType := types.T_int64.ToType()
	leftExpr := &plan.Expr{
		Typ: plan2.MakePlan2Type(&pkType),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: int32(0),
			},
		},
	}
	rightExpr := &plan.Expr{
		Typ: plan2.MakePlan2Type(&pkType),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 1,
				ColPos: int32(2),
			},
		},
	}
	eqExpr, _ := plan2.BindFuncExprImplByPlanExpr(context.TODO(), "=", []*plan.Expr{leftExpr, rightExpr})

	onDupMap := make(map[string]*plan.Expr)
	onDupMap["b"] = plan2.MakePlan2Int64ConstExprWithType(10)

	return onDupTestCase{
		proc: proc,
		arg: &OnDuplicatekey{
			Attrs:              []string{"a", "b", "a", "b", catalog.Row_ID}, //create table t1(a int primary key, b int)
			InsertColCount:     2,
			UniqueColCheckExpr: []*plan.Expr{eqExpr},
			UniqueCols:         []string{"a"},
			OnDuplicateIdx:     []int32{3, 4, 5},
			OnDuplicateExpr:    onDupMap, // on duplicate key update b = 10 -》 here is b = 10
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{
					Idx:     0,
					IsFirst: false,
					IsLast:  false,
				},
			},
		},
		rowCount: 1,
	}
}
