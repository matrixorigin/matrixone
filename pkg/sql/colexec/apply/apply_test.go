// Copyright 2024 Matrix Origin
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

package apply

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type applyTestCase struct {
	arg  *Apply
	proc *process.Process
}

func makeTestCases(t *testing.T) []applyTestCase {
	return []applyTestCase{
		newTestCase(t, CROSS),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestApply(t *testing.T) {
}

func TestNilTableFunctionLifecycle(t *testing.T) {
	proc := testutil.NewProc(t)
	arg := NewArgument()

	err := arg.Prepare(proc)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState))
	require.ErrorContains(t, err, "apply operator missing table function or subquery runner")

	_, err = arg.Call(proc)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState))
	require.ErrorContains(t, err, "apply operator missing table function or subquery runner")

	require.NotPanics(t, func() {
		arg.Reset(proc, false, nil)
	})

	require.NotPanics(t, func() {
		arg.Free(proc, false, nil)
	})
}

func newTestCase(t *testing.T, applyType int) applyTestCase {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := NewArgument()
	arg.ApplyType = applyType
	arg.TableFunction = table_function.NewArgument()
	return applyTestCase{
		arg:  arg,
		proc: proc,
	}
}

type mockRunner struct {
	batches []*batch.Batch
	idx     int
}

func (m *mockRunner) Prepare(proc *process.Process) error { return nil }
func (m *mockRunner) Start(input *batch.Batch, row int, proc *process.Process, analyzer process.Analyzer) error {
	m.idx = 0
	return nil
}
func (m *mockRunner) Call(proc *process.Process) (vm.CallResult, error) {
	if m.idx >= len(m.batches) {
		return vm.CallResult{Batch: batch.EmptyBatch}, nil
	}
	result := vm.NewCallResult()
	result.Batch = m.batches[m.idx]
	m.idx++
	return result, nil
}
func (m *mockRunner) End(proc *process.Process) error                             { return nil }
func (m *mockRunner) Reset(proc *process.Process, pipelineFailed bool, err error) {}
func (m *mockRunner) Free(proc *process.Process, pipelineFailed bool, err error)  {}

func TestRunnerLifecycle(t *testing.T) {
	proc := testutil.NewProcess(t)
	arg := NewArgument()
	arg.ApplyType = CROSS
	arg.Result = []colexec.ResultPos{{Rel: 0, Pos: 0}, {Rel: 1, Pos: 0}}
	arg.Typs = []types.Type{types.T_int64.ToType()}
	arg.Runner = &mockRunner{
		batches: []*batch.Batch{
			testutil.NewBatchWithVectors(
				[]*vector.Vector{
					testutil.MakeInt64Vector([]int64{100}, nil, proc.Mp()),
				},
				[]int64{1},
			),
		},
	}

	input := testutil.NewBatchWithVectors(
		[]*vector.Vector{
			testutil.MakeInt64Vector([]int64{1}, nil, proc.Mp()),
		},
		[]int64{1},
	)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input, nil})
	arg.AppendChild(child)

	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, 1, result.Batch.RowCount())
	require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[0], 0))
	require.Equal(t, int64(100), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[1], 0))
}

/*
func resetChildren(arg *Apply) {
	bat := colexec.MakeMockBatchs()
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}
*/
