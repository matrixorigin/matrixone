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

package projection

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10 // default rows
)

// add unit tests for cases
type projectionTestCase struct {
	arg   *Projection
	types []types.Type
	proc  *process.Process
}

func makeTestCases(t *testing.T) []projectionTestCase {
	return []projectionTestCase{
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int8.ToType(),
			},
			arg: &Projection{
				ProjectList: []*plan.Expr{
					{
						Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
						Typ: plan.Type{
							Id: int32(types.T_int8),
						},
					},
				},
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
		},
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

func TestProjection(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		nb0 := tc.proc.Mp().CurrNB()
		op := resetChildren(tc.arg, tc.proc.Mp())
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, _ = vm.Exec(tc.arg, tc.proc)

		tc.arg.Reset(tc.proc, false, nil)
		op.Free(tc.proc, false, nil)

		op = resetChildren(tc.arg, tc.proc.Mp())
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, _ = vm.Exec(tc.arg, tc.proc)
		tc.arg.Free(tc.proc, false, nil)
		op.Free(tc.proc, false, nil)
		tc.proc.Free()
		nb1 := tc.proc.Mp().CurrNB()
		require.Equal(t, nb0, nb1)
	}
}

func TestGroupingSetProjectionExpandsOneInputBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := batch.NewWithSize(3)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeInt32Vector([]int32{10, 20}, nil, proc.Mp())
	input.Vecs[2] = testutil.MakeInt32Vector([]int32{100, 200}, nil, proc.Mp())
	input.SetRowCount(2)

	childCalls := 0
	child := colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{input}).
		WithBatchCallback(func(int) { childCalls++ })
	arg := NewArgument()
	arg.ProjectList = []*plan.Expr{
		makeProjectionCol(0, types.T_int32),
		makeProjectionCol(1, types.T_int32),
		makeProjectionCol(2, types.T_int32),
		{
			Typ:  plan.Type{Id: int32(types.T_bool), NotNullable: true},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}},
		},
		{
			Typ:  plan.Type{Id: int32(types.T_int64), NotNullable: true},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 0}}},
		},
	}
	arg.GroupingSetCount = 3
	arg.GroupingFlags = []bool{
		true, true,
		true, false,
		false, false,
	}
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	for set := 0; set < 3; set++ {
		result, err := arg.Call(proc)
		require.NoError(t, err)
		require.Equal(t, vm.ExecNext, result.Status)
		require.Equal(t, 2, result.Batch.RowCount())
		require.Equal(t, 1, childCalls, "all grouping sets must reuse one child batch")

		for key := 0; key < 2; key++ {
			active := arg.GroupingFlags[set*2+key]
			require.Equal(t, !active, result.Batch.Vecs[key].HasGrouping())
			if active {
				require.Equal(t, []int32{int32(1 + key*9), int32(2 + key*18)},
					vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[key]))
			} else {
				require.True(t, result.Batch.Vecs[key].GetGrouping().Contains(0))
				require.True(t, result.Batch.Vecs[key].GetGrouping().Contains(1))
			}
		}
		require.Equal(t, []int32{100, 200},
			vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[2]))
		require.False(t, vector.GetFixedAtNoTypeCheck[bool](result.Batch.Vecs[3], 0))
		require.Equal(t, int64(set), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[4], 0))
	}

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)
	require.Equal(t, 1, childCalls)

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestGroupingSetProjectionEmitsEmptySetOnRuntimeEmptyInput(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	childCalls := 0
	child := colexec.NewMockOperator().
		WithBatchCallback(func(int) { childCalls++ })
	arg := NewArgument()
	arg.ProjectList = []*plan.Expr{
		makeProjectionCol(0, types.T_int32),
		makeProjectionCol(1, types.T_int32),
		{
			Typ:  plan.Type{Id: int32(types.T_bool), NotNullable: true},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}},
		},
		{
			Typ:  plan.Type{Id: int32(types.T_int64), NotNullable: true},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 0}}},
		},
	}
	arg.GroupingSetCount = 3
	arg.GroupingFlags = []bool{true, true, false}
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.Equal(t, 1, result.Batch.RowCount())
	require.True(t, result.Batch.Vecs[0].GetGrouping().Contains(0))
	require.True(t, result.Batch.Vecs[1].IsNull(0))
	require.True(t, vector.GetFixedAtNoTypeCheck[bool](result.Batch.Vecs[2], 0))
	require.Equal(t, int64(2), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[3], 0))
	require.Zero(t, childCalls)

	result, err = arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)
	require.Nil(t, result.Batch)

	arg.Reset(proc, false, nil)
	child.Reset(proc, false, nil)
	result, err = arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.Equal(t, 1, result.Batch.RowCount())
	require.True(t, vector.GetFixedAtNoTypeCheck[bool](result.Batch.Vecs[2], 0))

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestGroupingSetProjectionRejectsInvalidMetadata(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := NewArgument()
	arg.ProjectList = []*plan.Expr{makeProjectionCol(0, types.T_int32)}
	arg.GroupingSetCount = 2
	arg.GroupingFlags = []bool{true, false}
	require.ErrorContains(t, arg.Prepare(proc), "invalid grouping-set projection metadata")
	arg.Free(proc, false, nil)
	proc.Free()
}

func makeProjectionCol(pos int32, typ types.T) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(typ)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}},
	}
}

func resetChildren(arg *Projection, m *mpool.MPool) *colexec.MockOperator {
	bat := colexec.MakeMockBatchs(m)
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
	return op
}
