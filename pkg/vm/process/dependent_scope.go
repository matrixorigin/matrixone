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

package process

import (
	"context"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
)

// dependentScope contains invocation-local state only. It borrows BaseProcess,
// the statement budget and input values from the caller, never copies mutexes
// or session state. A fresh scope is created for each invocation; it is retired
// after its tasks/readers quiesce, before the caller releases captured values.
type dependentScope struct {
	program   uint32
	slots     []*vector.Vector
	budget    *ExecutionResourceGeneration
	charged   uint64
	board     *message.MessageBoard
	context   QueryBaseContext
	runtime   bool
	cloneTxn  client.TxnOperator
	groupRows []atomic.Uint64
	closed    atomic.Bool
}

// NewDependentProcess acquires the invocation overlay before publishing it.
// operatorCount is the compiled analyzer-index domain, including every group
// producer. Slots must be owned by the caller until this process is closed.
func (proc *Process) NewDependentProcess(program uint32, slots []*vector.Vector, operatorCount int) (*Process, error) {
	if proc == nil || proc.Base == nil || proc.Ctx == nil || program == 0 && len(slots) != 0 || operatorCount < 0 {
		return nil, moerr.NewInternalErrorNoCtx("invalid dependent process")
	}
	if err := context.Cause(proc.Ctx); err != nil {
		return nil, err
	}
	for _, slot := range slots {
		if slot == nil || slot.Length() != 1 {
			return nil, moerr.NewInternalErrorNoCtx("dependent slot must contain one value")
		}
	}
	budget, err := proc.GetExecutionResourceBudget()
	if err != nil {
		return nil, err
	}
	size := uint64(unsafe.Sizeof(dependentScope{})) + uint64(unsafe.Sizeof(Process{})) + uint64(len(slots))*uint64(unsafe.Sizeof((*vector.Vector)(nil)))
	size += uint64(unsafe.Sizeof(message.MessageBoard{})) + uint64(unsafe.Sizeof(sync.RWMutex{})) + 32*uint64(unsafe.Sizeof(uintptr(0)))
	// Each compiled join publishes at most one terminal message in this
	// generation; dependent adapters disable runtime-filter broadcasts. Reserve
	// the board's growing pointer directory, message/receiver headers and their
	// allocation rounding along with the per-operator aggregate row counter.
	// Payload vectors and hash tables retain their native allocation owners.
	perOperator := uint64(unsafe.Sizeof(atomic.Uint64{})) + 256
	operatorSize := uint64(operatorCount) * perOperator
	if operatorSize/perOperator != uint64(operatorCount) || size > ^uint64(0)-operatorSize {
		return nil, moerr.NewOOM(proc.Ctx)
	}
	size += operatorSize
	if err = budget.AcquireAllocationCapacity(size); err != nil {
		return nil, err
	}
	child := proc.NewContextChildProc(0)
	child.dependent = &dependentScope{
		program: program, slots: append([]*vector.Vector(nil), slots...), budget: budget, charged: size,
		board: message.NewMessageBoard(), runtime: proc.GetBaseProcessRunningStatus(), groupRows: make([]atomic.Uint64, operatorCount),
	}
	child.dependentOwner = true
	child.dependent.context.outerContext = proc.GetTopContext()
	child.dependent.context.queryContext = child.Ctx
	child.dependent.context.queryCancel = func() { child.Cancel(context.Canceled) }
	return child, nil
}

// DependentSlot returns a borrowed native value. A retired scope cannot expose
// the previous invocation's captures even if a late holder kept its Process.
func (proc *Process) DependentSlot(program, ordinal uint32) (*vector.Vector, error) {
	if proc == nil || proc.dependent == nil || proc.dependent.closed.Load() || proc.dependent.program != program || uint64(ordinal) >= uint64(len(proc.dependent.slots)) {
		return nil, moerr.NewInternalErrorNoCtx("invalid or retired dependent slot")
	}
	return proc.dependent.slots[ordinal], nil
}

// CloseDependentProcess is called by the invocation owner after every child
// has quiesced. Child pipeline processes only borrow the overlay.
func (proc *Process) CloseDependentProcess() {
	if proc == nil || !proc.dependentOwner || proc.dependent == nil {
		return
	}
	d := proc.dependent
	if !d.closed.CompareAndSwap(false, true) {
		return
	}
	if proc.Cancel != nil {
		proc.Cancel(ErrPipelineStopped)
	}
	if d.board != nil {
		d.board.CloseAndDrain()
		d.board = nil
	}
	d.slots = nil
	d.groupRows = nil
	d.cloneTxn = nil
	d.budget.ReleaseAllocationCapacity(d.charged)
	d.charged = 0
}

func (proc *Process) ExecutionContextBase() *QueryBaseContext {
	if proc.dependent != nil {
		return &proc.dependent.context
	}
	return proc.Base.GetContextBase()
}

// NewDependentGeneration retains lexical captures while obtaining fresh local
// boards and cancellation state for a recursive CTE round. No transaction or
// statement budget is created here.
func (proc *Process) NewDependentGeneration(operatorCount int) (*Process, error) {
	if proc.dependent == nil {
		return proc.NewDependentProcess(0, nil, operatorCount)
	}
	if proc.dependent.closed.Load() {
		return nil, moerr.NewInternalErrorNoCtx("retired dependent generation")
	}
	return proc.NewDependentProcess(proc.dependent.program, proc.dependent.slots, operatorCount)
}

// RequireDependentProtocol is checked both on compilation and on every cached
// execution generation. An old cluster member must not rebind a persisted view
// or admit a prepared template with a different subquery contract.
func (proc *Process) RequireDependentProtocol() error {
	if proc != nil {
		if rt := moruntime.ServiceRuntime(proc.GetService()); rt != nil {
			raw, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			version, valid := raw.(int64)
			if ok && valid && version >= defines.MORPCVersion119 {
				return nil
			}
		}
	}
	return moerr.NewNotSupportedNoCtx("dependent subqueries require coordinator protocol version 119")
}
