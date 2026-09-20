// Copyright 2026 Matrix Origin
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

package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type continuationTestOperator struct {
	*colexec.MockOperator
	steps int
}

func (op *continuationTestOperator) Call(proc *process.Process) (vm.CallResult, error) {
	op.steps++
	if op.steps >= 3 {
		return vm.CallResult{Status: vm.ExecStop}, nil
	}
	return vm.CallResult{Status: vm.ExecNext, Batch: batch.EmptyBatch}, nil
}

type waitingContinuationTestOperator struct {
	*colexec.MockOperator
	ready chan func()
	steps int
}

func (op *waitingContinuationTestOperator) Call(proc *process.Process) (vm.CallResult, error) {
	op.steps++
	if op.steps == 1 {
		return vm.CallResult{
			Status: vm.ExecWaiting,
			OnReady: func(ready func()) error {
				op.ready <- ready
				return nil
			},
		}, nil
	}
	return vm.CallResult{Status: vm.ExecStop}, nil
}

func TestContinuationAdvancesOneVMQuantumAtATime(t *testing.T) {
	proc := process.NewTopProcess(
		context.Background(),
		mpool.MustNewZeroNoFixed(),
		nil, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	proc.BuildPipelineContext(context.Background())
	t.Cleanup(func() { proc.Free() })

	op := &continuationTestOperator{MockOperator: colexec.NewMockOperator()}
	p := NewMerge(op)
	continuation, err := p.NewContinuation(proc)
	require.NoError(t, err)
	require.False(t, continuation.Done())

	for i := 0; i < 2; i++ {
		step, err := continuation.Step()
		require.NoError(t, err)
		require.Equal(t, StepReady, step.Status)
		require.Equal(t, i+1, op.steps)
		require.False(t, continuation.Done())
	}

	step, err := continuation.Step()
	require.NoError(t, err)
	require.Equal(t, StepDone, step.Status)
	require.Equal(t, 3, op.steps)
	require.True(t, continuation.Done())

	step, err = continuation.Step()
	require.NoError(t, err)
	require.Equal(t, StepDone, step.Status)
	require.Equal(t, 3, op.steps)
}

func TestPipelineRunUsesContinuationAdapter(t *testing.T) {
	proc := process.NewTopProcess(
		context.Background(),
		mpool.MustNewZeroNoFixed(),
		nil, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	proc.BuildPipelineContext(context.Background())
	t.Cleanup(func() { proc.Free() })

	op := &continuationTestOperator{MockOperator: colexec.NewMockOperator()}
	p := NewMerge(op)
	end, err := p.Run(proc)
	require.NoError(t, err)
	require.True(t, end)
	require.Equal(t, 3, op.steps)
}

func TestContinuationPropagatesOperatorReadiness(t *testing.T) {
	proc := process.NewTopProcess(
		context.Background(),
		mpool.MustNewZeroNoFixed(),
		nil, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	proc.BuildPipelineContext(context.Background())
	t.Cleanup(func() { proc.Free() })

	op := &waitingContinuationTestOperator{
		MockOperator: colexec.NewMockOperator(),
		ready:        make(chan func(), 1),
	}
	go func() {
		ready := <-op.ready
		ready()
	}()
	end, err := NewMerge(op).Run(proc)
	require.NoError(t, err)
	require.True(t, end)
	require.Equal(t, 2, op.steps)
}

func TestContinuationStepPropagatesOperatorReadiness(t *testing.T) {
	proc := process.NewTopProcess(
		context.Background(),
		mpool.MustNewZeroNoFixed(),
		nil, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	proc.BuildPipelineContext(context.Background())
	t.Cleanup(func() { proc.Free() })

	op := &waitingContinuationTestOperator{
		MockOperator: colexec.NewMockOperator(),
		ready:        make(chan func(), 1),
	}
	continuation, err := NewMerge(op).NewContinuation(proc)
	require.NoError(t, err)
	step, err := continuation.Step()
	require.NoError(t, err)
	require.Equal(t, StepWaiting, step.Status)
	require.NotNil(t, step.OnReady)
	require.False(t, continuation.Done())

	var ready func()
	registered := make(chan struct{})
	require.NoError(t, step.OnReady(func() { close(registered) }))
	select {
	case ready = <-op.ready:
	case <-time.After(time.Second):
		t.Fatal("operator did not register readiness")
	}
	ready()
	select {
	case <-registered:
	case <-time.After(time.Second):
		t.Fatal("readiness callback was not delivered")
	}
	step, err = continuation.Step()
	require.NoError(t, err)
	require.Equal(t, StepDone, step.Status)
	require.True(t, continuation.Done())
}
