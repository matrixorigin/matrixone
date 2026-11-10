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
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergecte"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"unsafe"
)

// recursiveCursor owns the feedback edges and round generations. MergeCTE
// remains the authoritative owner of UNION DISTINCT and recursion-depth state.
type recursiveCursor struct {
	parent       *cursor
	kernel       *nativeKernel
	frontierStep int32
	frontier     *sourceEdge
	next         *sourceEdge
	round        *region
	roundProc    *process.Process
	member       int
	pending      int
	done         bool
	marker       batch.Batch
}

func (c *cursor) openRecursive() (*recursiveCursor, error) {
	r := c.region
	n := c.template.node
	if len(n.SourceStep) < 2 {
		return nil, moerr.NewInternalError(r.proc.Ctx, "recursive source arity")
	}
	if err := r.reserve(uint64(unsafe.Sizeof(recursiveCursor{}))); err != nil {
		return nil, err
	}
	state := &recursiveCursor{parent: c, frontierStep: -1, member: 1}
	for step, root := range r.execution.template.query.Steps {
		sink := r.execution.template.query.Nodes[root]
		if sink.NodeType == plan.Node_SINK && sink.RecursiveSink && len(sink.Children) == 1 && sink.Children[0] == n.NodeId {
			state.frontierStep = int32(step)
			break
		}
	}
	if state.frontierStep < 0 {
		return nil, moerr.NewInternalError(r.proc.Ctx, "recursive sink ownership")
	}
	var err error
	state.kernel, err = openKernel(r.proc, func(p *process.Process, ports []*DemandInput) (vm.Operator, error) {
		op := mergecte.NewArgument().WithNodeCnt(len(n.SourceStep) - 1).WithDistinct(n.RecursiveUnionDistinct)
		op.SetAnalyzeControl(int(n.NodeId), false)
		for _, port := range ports {
			port.SetIdx(int(n.NodeId))
			op.AppendChild(port)
		}
		return op, nil
	}, 2, r.execution.account, r.execution)
	if err != nil {
		return nil, err
	}
	state.marker.SetLast()
	return state, nil
}
func (s *recursiveCursor) closeRound(cause error) error {
	var err error
	if s.round != nil {
		err = s.round.close(cause)
		s.round = nil
	}
	if s.roundProc != nil {
		s.roundProc.CloseDependentProcess()
		s.roundProc = nil
	}
	return err
}
func (s *recursiveCursor) close(cause error) error {
	err := s.closeRound(cause)
	err = joinCloseError(err, s.kernel.close(cause))
	s.kernel = nil
	if s.frontier != nil {
		s.frontier.close()
		s.frontier = nil
	}
	if s.next != nil {
		s.next.close()
		s.next = nil
	}
	return err
}

type recursivePull struct {
	state   *recursiveCursor
	result  *batch.Batch
	charged uint64
}

func (s *recursiveCursor) task() (*recursivePull, error) {
	size := uint64(unsafe.Sizeof(recursivePull{}))
	if err := s.parent.region.budget.AcquireAllocationCapacity(size); err != nil {
		return nil, err
	}
	return &recursivePull{state: s, charged: size}, nil
}
func (t *recursivePull) Step(ctx context.Context) (vm.ContinuationStep, error) {
	s := t.state
	r := s.parent.region
	n := s.parent.template.node
	for !s.done {
		if err := context.Cause(ctx); err != nil {
			return vm.ContinuationStep{}, err
		}
		b, need, task, err := s.kernel.read()
		if err != nil {
			return vm.ContinuationStep{}, err
		}
		if task != nil {
			return vm.ContinuationStep{Child: task}, nil
		}
		if need >= 0 {
			s.pending = need
			if need == 0 {
				child, err := r.pull(r.execution.template.query.Steps[n.SourceStep[0]])
				return vm.ContinuationStep{Child: child}, err
			}
			if s.frontier == nil {
				return vm.ContinuationStep{}, moerr.NewInternalError(ctx, "recursive input before initial round")
			}
			if s.round == nil {
				s.roundProc, err = r.proc.NewDependentGeneration(len(r.execution.template.query.Nodes))
				if err != nil {
					return vm.ContinuationStep{}, err
				}
				s.round, err = r.execution.newRegion(s.roundProc, r.template.owner)
				if err != nil {
					s.roundProc.CloseDependentProcess()
					s.roundProc = nil
					return vm.ContinuationStep{}, err
				}
				s.round.frontier, s.round.frontierStep = s.frontier, s.frontierStep
			}
			child, err := s.round.pull(r.execution.template.query.Steps[n.SourceStep[s.member]])
			return vm.ContinuationStep{Child: child}, err
		}
		if b == nil {
			s.done = true
			break
		}
		if b.Last() {
			if err = s.closeRound(nil); err != nil {
				return vm.ContinuationStep{}, err
			}
			if s.frontier != nil {
				s.frontier.close()
				s.frontier = nil
			}
			if s.next == nil || s.next.count == 0 {
				s.done = true
				break
			}
			s.next.done = true
			s.next.source.Finish(nil)
			s.frontier, s.next = s.next, nil
			s.member = 1
			continue
		}
		if b.IsEmpty() {
			continue
		}
		if s.next == nil {
			s.next, err = r.newSource(r.template.sourceReaders[s.frontierStep])
			if err != nil {
				return vm.ContinuationStep{}, err
			}
		}
		if err = s.next.accept(b); err != nil {
			return vm.ContinuationStep{}, err
		}
		t.result = b
		return vm.ContinuationStep{Done: true}, nil
	}
	return vm.ContinuationStep{Done: true}, nil
}
func (t *recursivePull) Accept(child vm.Continuation) error {
	p, ok := child.(*pullTask)
	if !ok {
		return moerr.NewInternalErrorNoCtx("recursive source result")
	}
	s := t.state
	if s.pending == 1 && p.result == nil {
		if err := s.closeRound(nil); err != nil {
			return err
		}
		s.member++
		return s.kernel.supply(1, &s.marker)
	}
	return s.kernel.supply(s.pending, p.result)
}
func (t *recursivePull) Close(error) error {
	t.state.parent.region.budget.ReleaseAllocationCapacity(t.charged)
	t.charged = 0
	return nil
}
