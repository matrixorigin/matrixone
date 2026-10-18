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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dedupjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"unsafe"
)

// JoinKernel sequences the native build and probe on one lane. The native join
// owns candidate cursors, matched bits and null-extension decisions.
type JoinKernel struct {
	vm.OperatorBase
	Build          *hashbuild.HashBuild
	Join           vm.Operator
	Condition      *colexec.DependentExpressionProgram
	Updates        []*colexec.DependentExpressionProgram
	UpdateCols     []int32
	updateFrames   []*colexec.DependentExpressionFrame
	updateValues   []*vector.Vector
	updateImage    *batch.Batch
	updatePC       int
	updateConsumed bool
	updateBudget   *process.ExecutionResourceGeneration
	updateCharged  uint64
	factory        colexec.DependentProgramFactory
	allocation     *vector.AllocationAccountSelection
	frame          *colexec.DependentExpressionFrame
	pending        vm.Continuation
	built          bool
	consumed       bool
}

func (j *JoinKernel) BindDependent(factory colexec.DependentProgramFactory, allocation *vector.AllocationAccountSelection) {
	j.factory, j.allocation = factory, allocation
	if j.Condition != nil {
		j.Join.(*loopjoin.LoopJoin).PairCondition = j.pair
	}
	if len(j.Updates) > 0 {
		j.Join.(*dedupjoin.DedupJoin).UpdateExpressions = j.update
	}
}

// Each assignment has a fresh expression frame and reads the preceding
// assignment's image. Frames stay alive until the native conflict owner has
// materialized the completed action; no partial value is published to it.
func (j *JoinKernel) update(p *process.Process, current, incoming *batch.Batch) ([]*vector.Vector, error) {
	if j.updateConsumed {
		j.closeUpdate()
	}
	if j.updateImage == nil {
		budget, err := p.GetExecutionResourceBudget()
		if err != nil {
			return nil, err
		}
		size := uint64(unsafe.Sizeof(batch.Batch{})) + uint64(len(current.Vecs))*uint64(unsafe.Sizeof((*vector.Vector)(nil))) +
			uint64(len(j.Updates))*uint64(unsafe.Sizeof((*colexec.DependentExpressionFrame)(nil))+unsafe.Sizeof((*vector.Vector)(nil)))
		if err := budget.AcquireAllocationCapacity(size); err != nil {
			return nil, err
		}
		j.updateBudget, j.updateCharged = budget, size
		j.updateImage = &batch.Batch{Vecs: append([]*vector.Vector(nil), current.Vecs...)}
		j.updateImage.SetRowCount(1)
		j.updateFrames = make([]*colexec.DependentExpressionFrame, len(j.Updates))
		j.updateValues = make([]*vector.Vector, len(j.Updates))
	}
	for j.updatePC < len(j.Updates) {
		pc := j.updatePC
		if j.UpdateCols[pc] < 0 || int(j.UpdateCols[pc]) >= len(j.updateImage.Vecs) {
			return nil, moerr.NewInternalError(p.Ctx, "dependent assignment column out of range")
		}
		if j.updateFrames[pc] == nil {
			frame, err := j.Updates[pc].NewFrame(p,
				[]colexec.DependentInput{{Batch: j.updateImage}, {Batch: incoming}}, j.factory, j.allocation)
			if err != nil {
				return nil, err
			}
			j.updateFrames[pc] = frame
			j.pending, err = frame.Task(0)
			return nil, err
		}
		value, err := j.updateFrames[pc].Value(0)
		if err != nil {
			return nil, err
		}
		j.updateValues[pc] = value
		j.updateImage.Vecs[j.UpdateCols[pc]] = value
		j.updatePC++
	}
	j.updateConsumed = true
	return j.updateValues, nil
}

func (j *JoinKernel) closeUpdate() {
	for _, frame := range j.updateFrames {
		frame.Close()
	}
	j.updateFrames, j.updateValues, j.updateImage = nil, nil, nil
	j.updatePC, j.updateConsumed = 0, false
	if j.updateBudget != nil {
		j.updateBudget.ReleaseAllocationCapacity(j.updateCharged)
		j.updateBudget, j.updateCharged = nil, 0
	}
}
func (j *JoinKernel) pair(p *process.Process, left *batch.Batch, lrow int, right *batch.Batch, rrow int) (*vector.Vector, error) {
	if j.consumed {
		j.frame.Close()
		j.frame = nil
		j.consumed = false
	}
	if j.frame == nil {
		var err error
		j.frame, err = j.Condition.NewFrame(p, []colexec.DependentInput{{Batch: left, Row: lrow}, {Batch: right, Row: rrow}}, j.factory, j.allocation)
		if err != nil {
			return nil, err
		}
		j.pending, err = j.frame.Task(0)
		return nil, err
	}
	value, err := j.frame.Value(0)
	if err != nil {
		return nil, err
	}
	j.consumed = true
	return value, nil
}
func (j *JoinKernel) TakeContinuation() vm.Continuation {
	pending := j.pending
	j.pending = nil
	return pending
}
func (j *JoinKernel) String(b *bytes.Buffer)            { b.WriteString("dependent join") }
func (j *JoinKernel) OpType() vm.OpType                 { return vm.Dependent }
func (j *JoinKernel) GetOperatorBase() *vm.OperatorBase { return &j.OperatorBase }
func (j *JoinKernel) Prepare(p *process.Process) error {
	j.OpAnalyzer = process.NewAnalyzer(j.GetIdx(), false, false, "dependent join")
	return nil
}
func (j *JoinKernel) Call(p *process.Process) (vm.CallResult, error) {
	if !j.built {
		result, err := vm.Exec(j.Build, p)
		if err != nil || result.Status == vm.ExecYield {
			return result, err
		}
		j.built = true
	}
	return vm.Exec(j.Join, p)
}
func (j *JoinKernel) ExecProjection(_ *process.Process, b *batch.Batch) (*batch.Batch, error) {
	return b, nil
}
func (j *JoinKernel) Reset(*process.Process, bool, error) {
	j.closeUpdate()
	if j.frame != nil {
		j.frame.Close()
		j.frame = nil
	}
	if j.pending != nil {
		_ = j.pending.Close(nil)
		j.pending = nil
	}
	j.built, j.consumed = false, false
}
func (j *JoinKernel) Free(p *process.Process, failed bool, err error) { j.Reset(p, failed, err) }
func (j *JoinKernel) Release()                                        {}
