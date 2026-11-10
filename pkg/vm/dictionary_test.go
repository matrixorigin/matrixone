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

package vm

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type dictionaryTestProducer struct {
	testOperator
	boundary      DictionaryOutputState
	possible      bool
	allocation    *vector.AllocationAccountSelection
	projectionErr error
}

func (p *dictionaryTestProducer) DictionaryOutputState() *DictionaryOutputState { return &p.boundary }
func (p *dictionaryTestProducer) Call(proc *process.Process) (CallResult, error) {
	p.boundary.BeginCall()
	return p.testOperator.Call(proc)
}
func (p *dictionaryTestProducer) Reset(proc *process.Process, failed bool, err error) {
	p.testOperator.Reset(proc, failed, err)
	p.boundary = DictionaryOutputState{}
}
func (p *dictionaryTestProducer) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	if p.projectionErr != nil {
		return nil, p.projectionErr
	}
	return p.boundary.Normalize(input, proc.Mp())
}

func (p *dictionaryTestProducer) DictionaryOutput() (bool, *vector.AllocationAccountSelection) {
	return p.possible, p.allocation
}

type dictionaryTestConsumer struct {
	testOperator
	accepts bool
}

func (p *dictionaryTestConsumer) AcceptsDictionary(child int) bool { return p.accepts && child == 0 }

func dictionaryRuntimeFixture(t *testing.T) (*process.Process, *batch.Batch) {
	t.Helper()
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	proc := &process.Process{Ctx: context.Background(), Base: &process.BaseProcess{}}
	proc.SetMPool(mp)
	source := vector.NewVec(types.T_int64.ToType())
	t.Cleanup(func() { source.Free(mp) })
	require.NoError(t, vector.AppendFixedList(source, []int64{10, 20, 30}, nil, mp))
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
	t.Cleanup(func() { input.Clean(mp) })
	return proc, input
}

func newDictionaryTestProducer(input *batch.Batch) *dictionaryTestProducer {
	return &dictionaryTestProducer{
		testOperator: testOperator{OperatorBase: OperatorBase{OpAnalyzer: process.NewTempAnalyzer()}, result: CallResult{Status: ExecNext, Batch: input}},
		possible:     true,
	}
}

func TestDictionaryRuntimeCapabilityEdges(t *testing.T) {
	proc, input := dictionaryRuntimeFixture(t)
	producer := newDictionaryTestProducer(input)
	t.Cleanup(func() { Free(producer, proc, false, nil) })
	consumer := &dictionaryTestConsumer{}
	consumer.AppendChild(producer)
	require.NoError(t, Prepare(consumer, proc))
	require.True(t, producer.boundary.required, "unknown input capability defaults to flat")
	output, err := Exec(producer, proc)
	require.NoError(t, err)
	require.NotSame(t, input, output.Batch)
	require.Equal(t, []int64{30, 10, 30}, vector.MustFixedColNoTypeCheck[int64](output.Batch.Vecs[0]))
	require.True(t, input.HasDictionary())
	consumer.accepts = true
	second := newDictionaryTestProducer(input)
	t.Cleanup(func() { Free(second, proc, false, nil) })
	consumer.AppendChild(second)
	require.NoError(t, Prepare(consumer, proc))
	require.True(t, second.boundary.required, "capability is specific to each input edge")
	require.False(t, producer.boundary.required)
	_, producerState := any(consumer).(DictionaryProducer)
	require.False(t, producerState, "flat operator has no producer state")
	output, err = Exec(producer, proc)
	require.NoError(t, err)
	require.Same(t, input, output.Batch)
	// A producer used as the external root must also normalize.
	require.NoError(t, Prepare(producer, proc))
	require.True(t, producer.boundary.required)
	output, err = Exec(producer, proc)
	require.NoError(t, err)
	require.False(t, output.Batch.HasDictionary())
	producer.possible = false
	require.NoError(t, Prepare(producer, proc))
	require.False(t, producer.boundary.required, "disabled producer incurs no batch scan")
}

func TestDictionaryRuntimeOutputLifetimeAndTerminalPaths(t *testing.T) {
	for _, terminal := range []string{"next", "eof", "yield", "error", "projection_error", "cancel", "reset", "free", "prepare"} {
		t.Run(terminal, func(t *testing.T) {
			proc, input := dictionaryRuntimeFixture(t)
			op := newDictionaryTestProducer(input)
			t.Cleanup(func() { Free(op, proc, false, nil) })
			require.NoError(t, Prepare(op, proc))
			before := proc.Mp().CurrNB()
			output, err := Exec(op, proc)
			require.NoError(t, err)
			require.Greater(t, proc.Mp().CurrNB(), before)
			prior := output.Batch
			switch terminal {
			case "next":
				flat := batch.NewWithSize(0)
				op.result.Batch = flat
				output, err = Exec(op, proc)
				require.NoError(t, err)
				require.Same(t, flat, output.Batch)
			case "eof":
				op.result = CancelResult
				output, err = Exec(op, proc)
				require.NoError(t, err)
				require.Nil(t, output.Batch)
			case "yield":
				op.result = CallResult{Status: ExecYield}
				output, err = Exec(op, proc)
				require.NoError(t, err)
				require.Equal(t, ExecYield, output.Status)
			case "projection_error":
				op.projectionErr = errors.New("injected projection error")
				_, err = Exec(op, proc)
				require.ErrorIs(t, err, op.projectionErr)
			case "error":
				op.callErr = errors.New("injected call error")
				_, err = Exec(op, proc)
				require.ErrorIs(t, err, op.callErr)
			case "cancel":
				ctx, cancel := context.WithCancel(proc.Ctx)
				cancel()
				proc.Ctx = ctx
				_, err = Exec(op, proc)
				require.ErrorIs(t, err, context.Canceled)
				Reset(op, proc, true, err)
			case "reset":
				Reset(op, proc, false, nil)
			case "free":
				Free(op, proc, true, errors.New("injected failure"))
			case "prepare":
				require.NoError(t, Prepare(op, proc))
			}
			require.Nil(t, prior.Vecs, "last borrowed output is retired")
			require.Equal(t, before, proc.Mp().CurrNB())
			require.True(t, input.HasDictionary(), "borrowed producer data remains live")
			Reset(op, proc, false, nil)
			Free(op, proc, false, nil)
		})
	}
}

func TestDictionaryRuntimeProjectionNormalizesBeforePublication(t *testing.T) {
	proc, input := dictionaryRuntimeFixture(t)
	// The boundary must see the actual post-projection batch, not Call's batch.
	projected := &dictionaryProjectedProducer{dictionaryTestProducer: *newDictionaryTestProducer(nil), projected: input}
	t.Cleanup(func() { Free(projected, proc, false, nil) })
	require.NoError(t, Prepare(projected, proc))
	output, err := Exec(projected, proc)
	require.NoError(t, err)
	require.False(t, output.Batch.HasDictionary())
	require.Equal(t, []int64{30, 10, 30}, vector.MustFixedColNoTypeCheck[int64](output.Batch.Vecs[0]))
	Free(projected, proc, false, nil)
}

type dictionaryProjectedProducer struct {
	dictionaryTestProducer
	projected *batch.Batch
}

func (p *dictionaryProjectedProducer) ExecProjection(proc *process.Process, _ *batch.Batch) (*batch.Batch, error) {
	return p.boundary.Normalize(p.projected, proc.Mp())
}

type dictionaryCapacity struct {
	reject bool
	used   uint64
}

func (c *dictionaryCapacity) AcquireAllocationCapacity(n uint64) error {
	if c.reject {
		return mpool.ErrAllocationAccountCapacity
	}
	c.used += n
	return nil
}
func (c *dictionaryCapacity) ReleaseAllocationCapacity(n uint64) { c.used -= n }

func TestDictionaryProducerCopyFailureAndReuse(t *testing.T) {
	proc, input := dictionaryRuntimeFixture(t)
	registry, err := mpool.NewAllocationAccountRegistry(1, 32)
	require.NoError(t, err)
	capacity := &dictionaryCapacity{reject: true}
	account, err := registry.OpenWithController(1<<20, capacity)
	require.NoError(t, err)
	op := newDictionaryTestProducer(input)
	t.Cleanup(func() {
		Free(op, proc, false, nil)
		snapshot := account.Seal()
		require.Zero(t, snapshot.Used)
		require.Zero(t, capacity.used)
		require.Zero(t, registry.LiveAllocationMetadata())
		_, err := registry.Finalize(account)
		require.NoError(t, err)
	})
	op.allocation, err = vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)
	require.NoError(t, Prepare(op, proc))
	_, err = Exec(op, proc)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Nil(t, op.boundary.output)
	require.Zero(t, account.Snapshot().Used)
	require.True(t, input.HasDictionary())
	capacity.reject = false
	output, err := Exec(op, proc)
	require.NoError(t, err)
	require.Equal(t, []int64{30, 10, 30}, vector.MustFixedColNoTypeCheck[int64](output.Batch.Vecs[0]))
	require.True(t, input.HasDictionary())
}
