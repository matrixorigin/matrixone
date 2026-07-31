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

package mergecte

import (
	"bytes"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type failAfterBatchesOperator struct {
	*colexec.MockOperator
	failAfter int
	calls     int
	err       error
}

func (op *failAfterBatchesOperator) Call(proc *process.Process) (vm.CallResult, error) {
	if op.calls == op.failAfter {
		return vm.CancelResult, op.err
	}
	op.calls++
	return op.MockOperator.Call(proc)
}

// add unit tests for cases
type mergeCTETestCase struct {
	arg  *MergeCTE
	proc *process.Process
}

func makeTestCases(t *testing.T) []mergeCTETestCase {
	return []mergeCTETestCase{
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			arg:  &MergeCTE{},
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

func TestMergeCTEDistinctFiltersAcrossBatchesAndReset(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := NewArgument().WithNodeCnt(1).WithDistinct(true)
	analyzer := process.NewAnalyzer(0, false, false, "merge cte distinct test")

	first := makeMergeCTEDistinctBatch(
		t,
		proc,
		[]int64{1, 1, 2},
		[]string{"a", "a", ""},
		[]bool{false, false, true},
	)
	second := makeMergeCTEDistinctBatch(
		t,
		proc,
		[]int64{1, 2, 3},
		[]string{"a", "", "c"},
		[]bool{false, true, false},
	)
	defer first.Clean(proc.Mp())
	defer second.Clean(proc.Mp())

	require.NoError(t, arg.Prepare(proc))
	firstOut, err := arg.ctr.cacheBatch(proc, analyzer, first)
	require.NoError(t, err)
	require.Equal(t, 2, firstOut.RowCount())
	require.Equal(t, []int64{1, 2}, vector.MustFixedColWithTypeCheck[int64](firstOut.Vecs[0]))
	require.False(t, firstOut.Vecs[1].GetNulls().Contains(0))
	require.True(t, firstOut.Vecs[1].GetNulls().Contains(1))

	secondOut, err := arg.ctr.cacheBatch(proc, analyzer, second)
	require.NoError(t, err)
	require.Equal(t, 1, secondOut.RowCount())
	require.Equal(t, []int64{3}, vector.MustFixedColWithTypeCheck[int64](secondOut.Vecs[0]))
	require.Equal(t, uint64(3), arg.ctr.hashTable.GroupCount())

	arg.Reset(proc, false, nil)
	require.Nil(t, arg.ctr.hashTable)
	require.NoError(t, arg.Prepare(proc))
	replayed, err := arg.ctr.cacheBatch(proc, analyzer, first)
	require.NoError(t, err)
	require.Equal(t, 2, replayed.RowCount())
	require.Equal(t, uint64(2), arg.ctr.hashTable.GroupCount())

	arg.Free(proc, false, nil)
	arg.Release()
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func makeMergeCTEDistinctBatch(
	t *testing.T,
	proc *process.Process,
	intValues []int64,
	stringValues []string,
	stringNulls []bool,
) *batch.Batch {
	require.Len(t, stringValues, len(intValues))
	require.Len(t, stringNulls, len(intValues))

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	for i := range intValues {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], intValues[i], false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(
			bat.Vecs[1],
			[]byte(stringValues[i]),
			stringNulls[i],
			proc.Mp(),
		))
	}
	bat.SetRowCount(len(intValues))
	return bat
}

func TestMergeCTE(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		resetChildren(tc.arg, tc.proc.Mp())
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, err = vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)

		tc.arg.Reset(tc.proc, false, nil)

		resetChildren(tc.arg, tc.proc.Mp())
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, err = vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func TestAuditMergeCTERecursiveErrorThenRetryDoesNotEmitStaleBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &MergeCTE{NodeCnt: 1}
	cleaned := false
	t.Cleanup(func() {
		if cleaned {
			return
		}
		freeMergeCTEChildren(arg, proc, true)
		arg.Free(proc, true, nil)
		proc.Free()
	})

	firstErr := errors.New("recursive input failed")
	arg.AppendChild(colexec.NewMockOperator())
	arg.AppendChild(&failAfterBatchesOperator{
		MockOperator: colexec.NewMockOperator().WithBatchs([]*batch.Batch{
			colexec.MakeMockBatchs(proc.Mp()),
			colexec.MakeMockBatchs(proc.Mp()),
		}),
		failAfter: 2,
		err:       firstErr,
	})
	require.NoError(t, arg.Prepare(proc))

	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.True(t, result.Batch.Last())

	_, err = vm.Exec(arg, proc)
	require.ErrorIs(t, err, firstErr)
	require.Len(t, arg.ctr.bats, 2)
	require.NotNil(t, arg.ctr.buf)

	arg.Reset(proc, true, firstErr)
	require.Nil(t, arg.ctr.bats)
	require.Nil(t, arg.ctr.buf)
	freeMergeCTEChildren(arg, proc, true)
	retryLast := colexec.MakeMockBatchs(proc.Mp())
	retryLast.SetLast()
	arg.AppendChild(colexec.NewMockOperator())
	arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{retryLast}))
	require.NoError(t, arg.Prepare(proc))

	result, err = vm.Exec(arg, proc)
	require.NoError(t, err)
	require.True(t, result.Batch.Last())

	result, err = vm.Exec(arg, proc)
	require.NoError(t, err)
	require.True(t, result.Batch.Last())
	require.Equal(t, retryLast.RowCount(), result.Batch.RowCount())

	freeMergeCTEChildren(arg, proc, false)
	arg.Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	cleaned = true
	require.Nil(t, arg.ctr.freeBats)
	require.Nil(t, arg.ctr.bats)
	require.Nil(t, arg.ctr.buf)
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestMergeCTEResetReusesChangedBatchLayout(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &MergeCTE{NodeCnt: 1}
	cleaned := false
	t.Cleanup(func() {
		if cleaned {
			return
		}
		freeMergeCTEChildren(arg, proc, true)
		arg.Free(proc, true, nil)
		proc.Free()
	})

	arg.AppendChild(colexec.NewMockOperator())
	arg.AppendChild(colexec.NewMockOperator())
	require.NoError(t, arg.Prepare(proc))
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.True(t, result.Batch.Last())

	arg.Reset(proc, false, nil)
	freeMergeCTEChildren(arg, proc, false)
	initial := colexec.MakeMockBatchs(proc.Mp())
	arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{initial}))
	arg.AppendChild(colexec.NewMockOperator())
	require.NoError(t, arg.Prepare(proc))

	result, err = vm.Exec(arg, proc)
	require.NoError(t, err)
	require.False(t, result.Batch.Last())
	require.Len(t, result.Batch.Vecs, len(initial.Vecs))
	require.Equal(t, initial.RowCount(), result.Batch.RowCount())

	result, err = vm.Exec(arg, proc)
	require.NoError(t, err)
	require.True(t, result.Batch.Last())

	arg.Reset(proc, false, nil)
	freeMergeCTEChildren(arg, proc, false)
	arg.AppendChild(colexec.NewMockOperator())
	arg.AppendChild(colexec.NewMockOperator())
	require.NoError(t, arg.Prepare(proc))

	result, err = vm.Exec(arg, proc)
	require.NoError(t, err)
	require.True(t, result.Batch.Last())
	require.Len(t, result.Batch.Vecs, 1)
	require.Equal(t, 1, result.Batch.RowCount())

	freeMergeCTEChildren(arg, proc, false)
	arg.Free(proc, false, nil)
	proc.Free()
	cleaned = true
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestMergeCTEResetClearsRecursiveFlagOnCompatibleBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &MergeCTE{NodeCnt: 1}
	cleaned := false
	t.Cleanup(func() {
		if cleaned {
			return
		}
		freeMergeCTEChildren(arg, proc, true)
		arg.Free(proc, true, nil)
		proc.Free()
	})

	arg.AppendChild(colexec.NewMockOperator())
	arg.AppendChild(colexec.NewMockOperator())
	require.NoError(t, arg.Prepare(proc))
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.True(t, result.Batch.Last())

	arg.Reset(proc, false, nil)
	freeMergeCTEChildren(arg, proc, false)
	initial := batch.New([]string{"value"})
	initial.Vecs[0] = testutil.MakeVarcharVector([]string{"new generation"}, nil, proc.Mp())
	initial.SetRowCount(1)
	arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{initial}))
	arg.AppendChild(colexec.NewMockOperator())
	require.NoError(t, arg.Prepare(proc))

	result, err = vm.Exec(arg, proc)
	require.NoError(t, err)
	require.False(t, result.Batch.Last())
	require.Equal(t, initial.Attrs, result.Batch.Attrs)
	require.Equal(t, initial.RowCount(), result.Batch.RowCount())

	freeMergeCTEChildren(arg, proc, false)
	arg.Free(proc, false, nil)
	proc.Free()
	cleaned = true
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func freeMergeCTEChildren(arg *MergeCTE, proc *process.Process, pipelineFailed bool) {
	for _, child := range arg.Children {
		child.Free(proc, pipelineFailed, nil)
	}
	arg.Children = nil
}

func resetChildren(arg *MergeCTE, m *mpool.MPool) {
	bat := colexec.MakeMockBatchs(m)
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}
