// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dependent

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"reflect"
	"unsafe"
)

// KernelFactory instantiates already bound operator metadata, attaching one
// DemandInput per logical child. Only kernels with an explicit ExecYield
// checkpoint may be returned. All native state stays with that operator.
type KernelFactory func(*process.Process, []*DemandInput) (vm.Operator, error)
type KernelCompiler func(*plan.Node, []*plan.Node) (KernelFactory, [][]*plan.Expr, error)

type DemandInput struct {
	vm.OperatorBase
	batch     *batch.Batch
	ready     bool
	eof       bool
	requested bool
}

func (d *DemandInput) String(b *bytes.Buffer)            { b.WriteString("dependent input") }
func (d *DemandInput) OpType() vm.OpType                 { return vm.Dependent }
func (d *DemandInput) GetOperatorBase() *vm.OperatorBase { return &d.OperatorBase }
func (d *DemandInput) Release()                          {}
func (d *DemandInput) Reset(*process.Process, bool, error) {
	d.batch = nil
	d.ready = false
	d.eof = false
	d.requested = false
}
func (d *DemandInput) Free(p *process.Process, b bool, e error) { d.Reset(p, b, e) }
func (d *DemandInput) ExecProjection(_ *process.Process, b *batch.Batch) (*batch.Batch, error) {
	return b, nil
}
func (d *DemandInput) Prepare(*process.Process) error {
	d.OpAnalyzer = process.NewAnalyzer(d.GetIdx(), false, false, "dependent input")
	return nil
}
func (d *DemandInput) Call(*process.Process) (vm.CallResult, error) {
	if d.ready {
		d.ready = false
		result := vm.CallResult{Status: vm.ExecNext, Batch: d.batch}
		d.batch = nil
		if d.eof {
			result.Status = vm.ExecStop
		}
		return result, nil
	}
	if d.eof {
		return vm.CancelResult, nil
	}
	d.requested = true
	return vm.CallResult{Status: vm.ExecYield}, nil
}

type nativeKernel struct {
	budget    *process.ExecutionResourceGeneration
	charged   uint64
	root      vm.Operator
	inputs    []*DemandInput
	operators []vm.Operator
	proc      *process.Process
	account   *mpool.AllocationAccount
}

func openKernel(proc *process.Process, factory KernelFactory, children int, account *mpool.AllocationAccount, execution *Execution) (k *nativeKernel, err error) {
	if account == nil {
		return nil, moerr.NewInternalErrorNoCtx("dependent kernel requires statement account")
	}
	budget, err := proc.GetExecutionResourceBudget()
	if err != nil {
		return nil, err
	}
	size := uint64(unsafe.Sizeof(nativeKernel{})) + uint64(children)*uint64(unsafe.Sizeof(DemandInput{})+unsafe.Sizeof((*DemandInput)(nil)))
	if err = budget.AcquireAllocationCapacity(size); err != nil {
		return nil, err
	}
	k = &nativeKernel{proc: proc, account: account, inputs: make([]*DemandInput, children), budget: budget, charged: size}
	defer func() {
		if err != nil {
			err = joinCloseError(err, k.close(err))
			k = nil
		}
	}()
	for i := range k.inputs {
		k.inputs[i] = &DemandInput{}
	}
	k.root, err = factory(proc, k.inputs)
	constructionErr := err
	if k.root == nil {
		if err == nil {
			err = moerr.NewInternalErrorNoCtx("nil dependent native root")
		}
		return k, err
	}
	// This is the fixed tree produced by one native adapter, never a nested
	// SQL call tree. Count it before allocating its ownership directory.
	count := 0
	var metadata uint64
	_ = vm.HandleAllOp(k.root, func(_ vm.Operator, op vm.Operator) error {
		count++
		metadata += uint64(unsafe.Sizeof(vm.Operator(nil)))
		if _, input := op.(*DemandInput); !input {
			metadata += uint64(reflect.TypeOf(op).Elem().Size())
		}
		metadata += uint64(cap(op.GetOperatorBase().Children)) * uint64(unsafe.Sizeof(vm.Operator(nil)))
		return nil
	})
	if err = budget.AcquireAllocationCapacity(metadata); err != nil {
		return k, err
	}
	k.charged += metadata
	k.operators = make([]vm.Operator, count)
	_ = vm.HandleAllOp(k.root, func(_ vm.Operator, op vm.Operator) error {
		count--
		k.operators[count] = op
		return nil
	})
	if constructionErr != nil {
		return k, constructionErr
	}
	for _, op := range k.operators {
		if binder, ok := op.(interface {
			BindDependent(colexec.DependentProgramFactory, *vector.AllocationAccountSelection)
		}); ok {
			binder.BindDependent(execution, execution.allocation)
		}
	}
	for i := len(k.operators) - 1; i >= 0; i-- {
		op := k.operators[i]
		if owner, ok := op.(interface {
			SetAllocationAccount(*mpool.AllocationAccount) error
		}); ok {
			if err = owner.SetAllocationAccount(account); err != nil {
				return k, err
			}
		}
		if err = op.Prepare(proc); err != nil {
			return k, err
		}
	}
	return k, nil
}
func (k *nativeKernel) read() (*batch.Batch, int, vm.Continuation, error) {
	result, err := vm.Exec(k.root, k.proc)
	if err != nil {
		return nil, -1, nil, err
	}
	if result.Status == vm.ExecYield {
		for i, input := range k.inputs {
			if input.requested {
				return nil, i, nil, nil
			}
		}
		for _, op := range k.operators {
			if demander, ok := op.(interface{ TakeContinuation() vm.Continuation }); ok {
				if child := demander.TakeContinuation(); child != nil {
					return nil, -1, child, nil
				}
			}
		}
		return nil, -1, nil, moerr.NewInternalErrorNoCtx("kernel yielded without demand")
	}
	return result.Batch, -1, nil, nil
}
func (k *nativeKernel) supply(i int, b *batch.Batch) error {
	if i < 0 || i >= len(k.inputs) || !k.inputs[i].requested || k.inputs[i].ready {
		return moerr.NewInternalErrorNoCtx("invalid kernel input lease")
	}
	d := k.inputs[i]
	d.batch = b
	d.ready = true
	d.eof = b == nil
	d.requested = false
	return nil
}
func (k *nativeKernel) close(cause error) error {
	if k == nil {
		return nil
	}
	var err error
	reset := func(_ vm.Operator, op vm.Operator) error {
		op.Reset(k.proc, cause != nil, cause)
		return nil
	}
	free := func(_ vm.Operator, op vm.Operator) error {
		op.Free(k.proc, cause != nil, cause)
		if owner, ok := op.(interface {
			ClearAllocationAccount(*mpool.AllocationAccount) error
		}); ok {
			err = joinCloseError(err, owner.ClearAllocationAccount(k.account))
		}
		op.Release()
		return nil
	}
	if k.operators == nil {
		// Directory admission can fail after a factory returned private native
		// objects. They still need cleanup although Prepare never ran.
		_ = vm.HandleAllOp(k.root, reset)
		_ = vm.HandleAllOp(k.root, free)
	} else {
		for _, op := range k.operators {
			_ = reset(nil, op)
		}
		for _, op := range k.operators {
			_ = free(nil, op)
		}
	}
	if k.budget != nil {
		k.budget.ReleaseAllocationCapacity(k.charged)
	}
	k.charged = 0
	k.operators = nil
	k.root = nil
	k.inputs = nil
	return err
}

func (k *nativeKernel) affectedRows() uint64 {
	var result uint64
	for _, op := range k.operators {
		if writer, ok := op.(vm.ModificationArgument); ok {
			result += writer.GetAffectedRows()
		}
	}
	return result
}
