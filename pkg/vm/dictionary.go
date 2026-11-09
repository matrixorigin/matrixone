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
}

// DictionaryConsumer admits read-only dictionary inputs on one child edge.
// Unknown operators, codecs and external root destinations require flat data.
// Declaring this also requires explicit normalization at unsupported internal
// function/native boundaries; read capability never grants mutation rights.
type DictionaryConsumer interface {
	AcceptsDictionary(child int) bool
}

type dictionaryBoundary struct {
	allocation *vector.AllocationAccountSelection
	output     *batch.Batch
	pool       *mpool.MPool
}

func (b *dictionaryBoundary) clean() {
	if b.output != nil {
		b.output.Clean(b.pool)
		b.output, b.pool = nil, nil
	}
}

func (b *dictionaryBoundary) normalize(input *batch.Batch, mp *mpool.MPool) (*batch.Batch, error) {
	if !input.HasDictionary() {
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
	base := op.GetOperatorBase()
	if base.dictionaryOutput != nil {
		base.dictionaryOutput.clean()
		base.dictionaryOutput = nil
	}
	if producer, ok := op.(DictionaryProducer); ok && !acceptsOutput {
		if possible, allocation := producer.DictionaryOutput(); possible {
			base.dictionaryOutput = &dictionaryBoundary{allocation: allocation}
		}
	}
	consumer, accepts := op.(DictionaryConsumer)
	for i, child := range base.Children {
		configureDictionaryBoundary(child, accepts && consumer.AcceptsDictionary(i))
	}
}

// Reset retires runtime-owned output after the operator has stopped its work.
// All execution owners must use this rather than invoking Operator.Reset alone.
func Reset(op Operator, proc *process.Process, failed bool, err error) {
	op.Reset(proc, failed, err)
	if boundary := op.GetOperatorBase().dictionaryOutput; boundary != nil {
		boundary.clean()
	}
}

// Free retires compatibility output before the operator can retire its account.
// It is safe after Reset and for partially prepared operators.
func Free(op Operator, proc *process.Process, failed bool, err error) {
	if boundary := op.GetOperatorBase().dictionaryOutput; boundary != nil {
		boundary.clean()
		op.GetOperatorBase().dictionaryOutput = nil
	}
	op.Free(proc, failed, err)
}
