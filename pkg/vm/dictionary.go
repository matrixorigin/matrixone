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
	required    bool
	allowed     bool
	preferOwned bool
	allocation  *vector.AllocationAccountSelection
	output      *batch.Batch
	pool        *mpool.MPool
}

// AllowsDictionaryOutput is false until preparation admits this producer's
// destination. Producers must consult it before retaining a selected backing.
func (b *DictionaryOutputState) AllowsDictionaryOutput() bool { return b.allowed }

// PrefersOwnedOutput is a consumer demand hint, not a representation requirement.
// Selected filtering stays admitted; producers may compact private owned inputs
// instead of allocating immutable views for values a consumer copies anyway.
func (b *DictionaryOutputState) PrefersOwnedOutput() bool { return b.preferOwned }

// Consumers request this only when input values do not escape their borrowed
// input lifetime. Unknown consumers preserve the existing representation choice.
type dictionaryOwnedInputConsumer interface{ PrefersOwnedDictionaryInput(child int) bool }

func dictionaryInputPrefersOwned(op Operator, child int, inherited, accepted bool) bool {
	if !accepted {
		return false
	}
	if inherited {
		return true
	}
	if consumer, ok := op.(dictionaryOwnedInputConsumer); ok {
		return consumer.PrefersOwnedDictionaryInput(child)
	}
	return false
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
	if b.output != nil && reusableDictionaryOutput(b.output, input, b.allocation) {
		for i, src := range input.Vecs {
			if src == nil {
				continue
			}
			if err := b.output.Vecs[i].UnionBatch(src, 0, src.Length(), nil, mp); err != nil {
				b.BeginCall()
				return nil, err
			}
			src.CopyExpressionMetadataTo(b.output.Vecs[i])
			b.output.Vecs[i].SetSorted(src.GetSorted())
		}
		b.output.Attrs = append(b.output.Attrs[:0], input.Attrs...)
		b.output.Recursive, b.output.ShuffleIDX = input.Recursive, input.ShuffleIDX
		b.output.SetRowCount(input.RowCount())
		return b.output, nil
	}
	b.BeginCall()
	output, err := input.FlatCopy(mp, b.allocation)
	if err != nil {
		return nil, err
	}
	b.output, b.pool = output, mp
	return output, nil
}

// BeginReusableCall has the same borrowed-output boundary as BeginCall but
// retains owned flat capacity. Owners must still use vm.Reset/vm.Free at terminal
// cleanup; ordinary/native producers keep BeginCall's existing release behavior.
func (b *DictionaryOutputState) BeginReusableCall() {
	if b.output != nil && len(b.output.ExtraBuf) == 0 {
		b.output.CleanOnlyData()
		return
	}
	b.BeginCall()
}

func reusableDictionaryOutput(dst, src *batch.Batch, allocation *vector.AllocationAccountSelection) bool {
	if len(dst.Vecs) != len(src.Vecs) || dst.RowCount() != 0 || len(src.ExtraBuf) > 0 || !vector.AllocationAccountSelectionsEqual(dst.AllocationAccountSelection(), allocation) {
		return false
	}
	for i, v := range src.Vecs {
		out := dst.Vecs[i]
		if v == nil {
			if out != nil {
				return false
			}
			continue
		}
		if out == nil || v.IsConst() || out.IsConst() || out.IsDictionary() || *v.GetType() != *out.GetType() {
			return false
		}
	}
	return true
}

// ConfigureDictionaryBoundaries runs after local/remote operator construction
// and preparation, never during encoding. It also serves native kernels that
// prepare operators individually. Root output defaults to flat.
func ConfigureDictionaryBoundaries(root Operator) {
	configureDictionaryBoundary(root, false, false)
}

func configureDictionaryBoundary(op Operator, acceptsOutput, preferOwned bool) {
	if op == nil {
		return
	}
	configureDictionaryProducer(op, acceptsOutput, preferOwned)
	consumer, accepts := op.(DictionaryConsumer)
	for i, child := range op.GetOperatorBase().Children {
		accepted := accepts && consumer.AcceptsDictionary(i)
		configureDictionaryBoundary(child, accepted, dictionaryInputPrefersOwned(op, i, preferOwned, accepted))
	}
}

func configureDictionaryProducer(op Operator, accepts, preferOwned bool) {
	if producer, ok := op.(DictionaryProducer); ok {
		state := producer.DictionaryOutputState()
		state.BeginCall()
		possible, allocation := producer.DictionaryOutput()
		wasAllowed, wasOwned := state.allowed, state.preferOwned
		state.allowed = possible && accepts
		state.required = possible && !accepts
		state.allocation = allocation
		state.preferOwned = preferOwned && state.allowed
		// Pure forwarding consumers can now re-evaluate their child edges. This
		// touches declared producers only, never walks an ordinary flat subtree.
		if wasAllowed != state.allowed || wasOwned != state.preferOwned {
			configureDictionaryChildren(op)
		}
	}
}

func configureDictionaryEdges(op Operator) {
	configureDictionaryProducer(op, false, false)
	configureDictionaryChildren(op)
}

func configureDictionaryChildren(op Operator) {
	inherited := false
	if producer, ok := op.(DictionaryProducer); ok {
		inherited = producer.DictionaryOutputState().PrefersOwnedOutput()
	}
	for i, child := range op.GetOperatorBase().Children {
		if _, ok := child.(DictionaryProducer); !ok {
			continue
		}
		accepts := false
		if consumer, ok := op.(DictionaryConsumer); ok {
			accepts = consumer.AcceptsDictionary(i)
		}
		configureDictionaryProducer(child, accepts, dictionaryInputPrefersOwned(op, i, inherited, accepts))
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
