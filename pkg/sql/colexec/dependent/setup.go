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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"unsafe"
)

// SetupKernel evaluates invocation constants on the same continuation lane as
// row expressions. The native consumer retains its own input/state machine.
type SetupKernel struct {
	vm.OperatorBase
	Program    *colexec.DependentExpressionProgram
	Count      int
	Native     vm.Operator
	Install    func(func(*process.Process) ([]*vector.Vector, error))
	factory    colexec.DependentProgramFactory
	allocation *vector.AllocationAccountSelection
	frame      *colexec.DependentExpressionFrame
	pending    vm.Continuation
	values     []*vector.Vector
	next       int
	budget     *process.ExecutionResourceGeneration
	charged    uint64
}

func (s *SetupKernel) BindDependent(f colexec.DependentProgramFactory, a *vector.AllocationAccountSelection) {
	s.factory, s.allocation = f, a
	s.Install(s.evaluate)
}
func (s *SetupKernel) evaluate(p *process.Process) ([]*vector.Vector, error) {
	if s.frame == nil {
		budget, err := p.GetExecutionResourceBudget()
		if err != nil {
			return nil, err
		}
		size := uint64(s.Count) * uint64(unsafe.Sizeof((*vector.Vector)(nil)))
		if err = budget.AcquireAllocationCapacity(size); err != nil {
			return nil, err
		}
		s.budget, s.charged = budget, size
		s.values = make([]*vector.Vector, s.Count)
		s.frame, err = s.Program.NewFrame(p, []colexec.DependentInput{{Batch: batch.EmptyForConstFoldBatch}}, s.factory, s.allocation)
		if err != nil {
			return nil, err
		}
	}
	for s.next < s.Count {
		if s.values[s.next] == nil {
			var err error
			s.pending, err = s.frame.Task(s.next)
			if err != nil {
				return nil, err
			}
			// The next call happens only after the driver completed this task.
			s.next++
			return nil, nil
		}
	}
	return s.values, nil
}
func (s *SetupKernel) TakeContinuation() vm.Continuation {
	child := s.pending
	s.pending = nil
	return child
}
func (s *SetupKernel) Prepare(p *process.Process) error {
	s.OpAnalyzer = process.NewAnalyzer(s.GetIdx(), false, false, "dependent setup")
	return nil
}
func (s *SetupKernel) Call(p *process.Process) (vm.CallResult, error) {
	if s.next > 0 && s.values[s.next-1] == nil {
		value, err := s.frame.Value(s.next - 1)
		if err != nil {
			return vm.CancelResult, err
		}
		s.values[s.next-1] = value
	}
	return vm.Exec(s.Native, p)
}
func (s *SetupKernel) Reset(_ *process.Process, _ bool, cause error) {
	if s.pending != nil {
		_ = s.pending.Close(cause)
		s.pending = nil
	}
	if s.frame != nil {
		s.frame.Close()
		s.frame = nil
	}
	if s.budget != nil {
		s.budget.ReleaseAllocationCapacity(s.charged)
	}
	s.budget = nil
	s.charged = 0
	s.values = nil
	s.next = 0
}
func (s *SetupKernel) Free(p *process.Process, failed bool, cause error) { s.Reset(p, failed, cause) }
func (*SetupKernel) Release()                                            {}
func (*SetupKernel) OpType() vm.OpType                                   { return vm.Dependent }
func (s *SetupKernel) GetOperatorBase() *vm.OperatorBase                 { return &s.OperatorBase }
func (*SetupKernel) String(b *bytes.Buffer)                              { b.WriteString("dependent setup") }
func (*SetupKernel) ExecProjection(_ *process.Process, b *batch.Batch) (*batch.Batch, error) {
	return b, nil
}
