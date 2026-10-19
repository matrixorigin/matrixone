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
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Operator is the coordinator lane's entry to an immutable Template. Every
// execution/retry gets a new Execution and the statement allocation account.
type Operator struct {
	affectedRows uint64
	vm.OperatorBase
	Template    *Template
	account     *mpool.AllocationAccount
	allocation  *vector.AllocationAccountSelection
	execution   *Execution
	terminalErr error
	done        bool
}

var _ vm.Operator = (*Operator)(nil)

func (o *Operator) String(b *bytes.Buffer)            { b.WriteString("dependent") }
func (o *Operator) OpType() vm.OpType                 { return vm.Dependent }
func (o *Operator) GetOperatorBase() *vm.OperatorBase { return &o.OperatorBase }
func (o *Operator) Release()                          { o.Template = nil }
func (o *Operator) ExecProjection(_ *process.Process, b *batch.Batch) (*batch.Batch, error) {
	return b, nil
}
func (o *Operator) SetAllocationAccount(account *mpool.AllocationAccount) error {
	if account == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if o.account != nil && o.account != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if o.execution != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	selection, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerDependent, 64, 65, 66, 67)
	if err != nil {
		return err
	}
	o.account, o.allocation, o.terminalErr = account, selection, nil
	return nil
}
func (o *Operator) ClearAllocationAccount(account *mpool.AllocationAccount) error {
	if o.account == nil {
		return nil
	}
	if o.account != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if o.execution != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	o.account, o.allocation = nil, nil
	return o.terminalErr
}
func (o *Operator) Prepare(proc *process.Process) error {
	if err := proc.RequireDependentProtocol(); err != nil {
		return err
	}
	if o.Template == nil || o.allocation == nil || o.execution != nil {
		return moerr.NewInternalErrorNoCtx("invalid dependent operator generation")
	}
	if o.OpAnalyzer == nil {
		o.OpAnalyzer = process.NewAnalyzer(o.GetIdx(), o.IsFirst, o.IsLast, "dependent")
	} else {
		o.OpAnalyzer.Reset()
	}
	var err error
	o.done = false
	o.affectedRows = 0
	o.execution, err = o.Template.NewExecution(proc, o.allocation, o.account)
	return err
}
func (o *Operator) Call(proc *process.Process) (vm.CallResult, error) {
	if o.done {
		return vm.CancelResult, o.terminalErr
	}
	b, err := o.execution.Next(proc.Ctx)
	if err != nil || b == nil {
		o.done = true
		closeErr := o.close(err)
		if err == nil {
			err = closeErr
		}
		return vm.CancelResult, err
	}
	return vm.CallResult{Batch: b, Status: vm.ExecNext}, nil
}
func (o *Operator) close(cause error) error {
	if o.execution != nil {
		if err := o.execution.close(cause); err != nil && o.terminalErr == nil {
			o.terminalErr = err
		}
		o.affectedRows = o.execution.affectedRows
		o.execution = nil
	}
	return o.terminalErr
}
func (o *Operator) Reset(proc *process.Process, failed bool, cause error) {
	if failed && cause == nil {
		cause = moerr.NewQueryInterrupted(proc.Ctx)
	}
	_ = o.close(cause)
	o.done = false
}
func (o *Operator) Free(proc *process.Process, failed bool, cause error) {
	if failed && cause == nil {
		cause = moerr.NewQueryInterrupted(proc.Ctx)
	}
	_ = o.close(cause)
}

func (o *Operator) GetAffectedRows() uint64 { return o.affectedRows }
