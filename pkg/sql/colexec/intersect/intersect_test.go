// Copyright 2022 Matrix Origin
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

package intersect

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type intersectTestCase struct {
	proc *process.Process
	arg  *Intersect
}

func TestIntersect(t *testing.T) {
	proc := testutil.NewProcess(t)
	// [2 rows + 2 row, 3 columns] intersect [1 row + 1 rows, 3 columns]
	/*
		{1, 2, 3}	    {1, 2, 3}
		{1, 2, 3} intersect {4, 5, 6} ==> {1, 2, 3}
		{3, 4, 5}
		{3, 4, 5}
	*/
	var end vm.CallResult
	c := newIntersectTestCase(proc)

	setProcForTest(proc, c.arg)
	err := c.arg.Prepare(c.proc)
	require.NoError(t, err)
	cnt := 0
	end, err = vm.Exec(c.arg, c.proc)
	require.NoError(t, err)
	result := end.Batch
	if result != nil && !result.IsEmpty() {
		cnt += result.RowCount()
		require.Equal(t, 3, len(result.Vecs)) // 3 column
	}
	require.Equal(t, 1, cnt) // 1 row

	c.arg.Reset(c.proc, false, nil)

	setProcForTest(proc, c.arg)
	err = c.arg.Prepare(c.proc)
	require.NoError(t, err)
	cnt = 0
	end, err = vm.Exec(c.arg, c.proc)
	require.NoError(t, err)
	result = end.Batch
	if result != nil && !result.IsEmpty() {
		cnt += result.RowCount()
		require.Equal(t, 3, len(result.Vecs)) // 3 column
	}
	require.Equal(t, 1, cnt) // 1 row

	for _, child := range c.arg.Children {
		child.Free(proc, false, nil)
	}
	c.arg.Reset(c.proc, false, nil)
	c.arg.Free(c.proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), c.proc.Mp().CurrNB())
}

func TestIntersectPreservesSparseBinaryStringProvenance(t *testing.T) {
	proc := testutil.NewProcess(t)
	left := batch.NewWithSize(1)
	left.Vecs[0] = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytesList(left.Vecs[0],
		[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("match")}, nil, proc.Mp()))
	require.NoError(t, left.Vecs[0].SetIsBinaryStringAt(3, true, proc.Mp()))
	left.SetRowCount(4)
	right := batch.NewWithSize(1)
	right.Vecs[0] = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(right.Vecs[0], []byte("match"), false, proc.Mp()))
	right.SetRowCount(1)

	leftChild := colexec.NewMockOperator().WithBatchs([]*batch.Batch{left})
	rightChild := colexec.NewMockOperator().WithBatchs([]*batch.Batch{right})
	arg := new(Intersect)
	arg.AppendChild(leftChild)
	arg.AppendChild(rightChild)
	t.Cleanup(func() {
		leftChild.Free(proc, false, nil)
		rightChild.Free(proc, false, nil)
		arg.Free(proc, false, nil)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})

	require.NoError(t, arg.Prepare(proc))
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, 1, result.Batch.RowCount())
	require.Equal(t, "match", result.Batch.Vecs[0].GetStringAt(0))
	require.True(t, result.Batch.Vecs[0].GetBinaryStringMetadataAt(0))
}

func TestAuditIntersectFreeReleasesBuildState(t *testing.T) {
	proc := testutil.NewProcess(t)
	c := newIntersectTestCase(proc)
	setProcForTest(proc, c.arg)

	cleaned := false
	t.Cleanup(func() {
		if cleaned {
			return
		}
		for _, child := range c.arg.Children {
			child.Free(proc, true, nil)
		}
		c.arg.Free(proc, true, nil)
		proc.Free()
	})

	require.NoError(t, c.arg.Prepare(proc))
	_, err := vm.Exec(c.arg, proc)
	require.NoError(t, err)
	require.NotNil(t, c.arg.ctr.hashTable)
	require.Len(t, c.arg.ctr.unmatched, 2)

	for _, child := range c.arg.Children {
		child.Free(proc, true, nil)
	}
	c.arg.Children = nil
	c.arg.Free(proc, true, nil)
	c.arg.Free(proc, true, nil)
	proc.Free()
	cleaned = true

	require.Nil(t, c.arg.ctr.hashTable)
	require.Nil(t, c.arg.ctr.unmatched)
	require.Nil(t, c.arg.ctr.buf)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func newIntersectTestCase(proc *process.Process) intersectTestCase {
	arg := new(Intersect)
	arg.OperatorBase.OperatorInfo = vm.OperatorInfo{
		Idx:     0,
		IsFirst: false,
		IsLast:  false,
	}
	return intersectTestCase{
		proc: proc,
		arg:  arg,
	}
}

func setProcForTest(proc *process.Process, interset *Intersect) {
	for _, child := range interset.Children {
		child.Free(proc, false, nil)
	}
	interset.Children = nil
	leftBatches := []*batch.Batch{
		testutil.NewBatchWithVectors(
			[]*vector.Vector{
				testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 1}),
				testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{2, 2}),
				testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{3, 3}),
			}, nil),
		testutil.NewBatchWithVectors(
			[]*vector.Vector{
				testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{3, 3}),
				testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{4, 4}),
				testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{5, 5}),
			}, nil),
	}

	rightBatches := []*batch.Batch{
		testutil.NewBatchWithVectors(
			[]*vector.Vector{
				testutil.NewVector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{1}),
				testutil.NewVector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{2}),
				testutil.NewVector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{3}),
			}, nil),
		testutil.NewBatchWithVectors(
			[]*vector.Vector{
				testutil.NewVector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{4}),
				testutil.NewVector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{5}),
				testutil.NewVector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{6}),
			}, nil),
	}

	leftChild := colexec.NewMockOperator().WithBatchs(leftBatches)
	rightChild := colexec.NewMockOperator().WithBatchs(rightBatches)
	interset.AppendChild(leftChild)
	interset.AppendChild(rightChild)
}
