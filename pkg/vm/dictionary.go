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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// DictionaryProducer declares possible output after ExecProjection, including
// its own safe handling of dictionary inputs to that projection. The selection
// belongs to the producing execution generation and charges its compatibility
// copy. No production operator implements this contract in the boundary stage.
type DictionaryProducer interface {
	DictionaryOutput() (possible bool, allocation *vector.AllocationAccountSelection)
	// DictionaryOutputState returns non-nil execution-lane-owned state.
	DictionaryOutputState() *DictionaryOutputState
}

// DictionaryConsumer admits read-only dictionary inputs on one child edge.
// Unknown operators, codecs and external root destinations require flat data.
// Declaring this also requires explicit normalization at unsupported internal
// function/native boundaries; read capability never grants mutation rights.
type DictionaryConsumer interface {
	AcceptsDictionary(child int) bool
}

// DictionaryOutputState is embedded only by declared producers. Call must invoke
// BeginCall, and ExecProjection must normalize its final result before returning.
// Each execution lane owns its state and its one borrowed output.
type DictionaryOutputState struct {
	required   bool
	allocation *vector.AllocationAccountSelection
	output     *batch.Batch
	pool       *mpool.MPool
}

func (b *DictionaryOutputState) BeginCall() {
	if b.output != nil {
		b.output.Clean(b.pool)
		b.output, b.pool = nil, nil
	}
}

func (b *DictionaryOutputState) Normalize(input *batch.Batch, mp *mpool.MPool) (*batch.Batch, error) {
	if !b.required || !input.HasDictionary() {
		return input, nil
	}
	if input.HasAllocationAccount() && b.allocation == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	output, err := input.FlatCopy(mp, b.allocation)
	if err != nil {
		return nil, err
	}
	b.output, b.pool = output, mp
	return output, nil
}

// ConfigureDictionaryBoundaries runs after local/remote operator construction
// and preparation, never during encoding. It also serves native kernels that
// prepare operators individually. Root output defaults to flat.
func ConfigureDictionaryBoundaries(root Operator) {
	configureDictionaryBoundary(root, false)
}

func configureDictionaryBoundary(op Operator, acceptsOutput bool) {
	if op == nil {
		return
	}
	consumer, accepts := op.(DictionaryConsumer)
	for i, child := range op.GetOperatorBase().Children {
		configureDictionaryBoundary(child, accepts && consumer.AcceptsDictionary(i))
	}
	configureDictionaryProducer(op, acceptsOutput)
}

func configureDictionaryProducer(op Operator, accepts bool) {
	if producer, ok := op.(DictionaryProducer); ok {
		state := producer.DictionaryOutputState()
		state.BeginCall()
		possible, allocation := producer.DictionaryOutput()
		state.required = possible && !accepts
		state.allocation = allocation
	}
}

func configureDictionaryEdges(op Operator) {
	configureDictionaryProducer(op, false)
	for i, child := range op.GetOperatorBase().Children {
		if _, ok := child.(DictionaryProducer); !ok {
			continue
		}
		accepts := false
		if consumer, ok := op.(DictionaryConsumer); ok {
			accepts = consumer.AcceptsDictionary(i)
		}
		configureDictionaryProducer(child, accepts)
	}
}

// Reset cleans private output after producer work has stopped.
func Reset(op Operator, proc *process.Process, failed bool, err error) {
	if producer, ok := op.(DictionaryProducer); ok {
		state := producer.DictionaryOutputState()
		prior := *state
		state.output, state.pool = nil, nil
		defer func() {
			prior.BeginCall()
			*producer.DictionaryOutputState() = prior
		}()
	}
	op.Reset(proc, failed, err)
}

// Free cleans private output before its allocation account can be retired.
func Free(op Operator, proc *process.Process, failed bool, err error) {
	if producer, ok := op.(DictionaryProducer); ok {
		state := producer.DictionaryOutputState()
		state.BeginCall()
		*state = DictionaryOutputState{}
	}
	op.Free(proc, failed, err)
}
