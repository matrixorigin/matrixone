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

package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type dictionaryPipelineSource struct {
	*colexec.MockOperator
	boundary vm.DictionaryOutputState
}

func (p *dictionaryPipelineSource) DictionaryOutputState() *vm.DictionaryOutputState {
	return &p.boundary
}
func (p *dictionaryPipelineSource) Call(proc *process.Process) (vm.CallResult, error) {
	p.boundary.BeginCall()
	return p.MockOperator.Call(proc)
}
func (p *dictionaryPipelineSource) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	output, err := p.MockOperator.ExecProjection(proc, input)
	if err != nil {
		return nil, err
	}
	return p.boundary.Normalize(output, proc.Mp())
}

func (*dictionaryPipelineSource) DictionaryOutput() (bool, *vector.AllocationAccountSelection) {
	return true, nil
}

func TestDictionaryBoundaryRealOutputAndFailedPipelineCleanup(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.BuildPipelineContext(context.Background())
	mp := proc.Mp()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	source := vector.NewVec(types.T_int64.ToType())
	t.Cleanup(func() { source.Free(mp) })
	require.NoError(t, vector.AppendFixedList(source, []int64{1, 2, 3}, nil, mp))
	base, err := vector.FreezeVector(source, mp, nil)
	require.NoError(t, err)
	defer base.Release()
	rows, err := vector.NewRowSelection([]int64{2, 0, 2}, 3, mp, nil)
	require.NoError(t, err)
	defer rows.Release()
	selected, err := vector.NewDictionaryVector(base, rows, mp, nil)
	require.NoError(t, err)
	input := batch.NewWithSize(1)
	input.Vecs[0] = selected
	input.SetRowCount(3)
	child := &dictionaryPipelineSource{MockOperator: colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})}
	failure := errors.New("injected output error")
	calls := 0
	root := output.NewArgument().WithFunc(func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
		calls++
		require.NotSame(t, input, bat)
		require.False(t, bat.HasDictionary())
		require.True(t, input.HasDictionary())
		require.Equal(t, []int64{3, 1, 3}, vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0]))
		return failure
	})
	root.AppendChild(child)
	p := NewMerge(root)
	t.Cleanup(func() { root.Release() })
	t.Cleanup(func() { p.Cleanup(proc, true, false, failure) })
	_, err = p.Run(proc)
	require.ErrorIs(t, err, failure)
	require.Equal(t, 1, calls)
	// Exercise the actual framework terminal path before the next child Call.
	p.Cleanup(proc, true, false, failure)
	require.Nil(t, input.Vecs)
	// Repeated disposal is harmless, as with partially prepared executions.
}
