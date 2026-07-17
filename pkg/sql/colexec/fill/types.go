// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fill

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Fill)

type container struct {

	// value
	valVecs []*vector.Vector

	// prev
	prevVecs []*vector.Vector
	// prevValid marks which prevVecs hold a value from the current partition.
	// A partition boundary invalidates them without freeing the vectors.
	prevValid []bool
	// prevPartKey / prevPartNull snapshot the partition key of the last row of
	// the previous batch, so the first row of the next batch can detect a
	// boundary without keeping the old batch alive.
	prevPartKey  [][]byte
	prevPartNull []bool
	prevPartSet  bool

	// next / linear: the materialized input, the gather cursor i, and the
	// emit cursor idx.
	bats []*batch.Batch
	idx  int
	buf  *batch.Batch
	i    int

	// linear
	exes []colexec.ExpressionExecutor
	done bool

	process func(ctr *container, ap *Fill, proc *process.Process, anal process.Analyzer) (vm.CallResult, error)
}

type Fill struct {
	ctr container

	ColLen   int
	FillType plan.Node_FillType
	FillVal  []*plan.Expr
	// PartitionColIdx locates the time window's partition keys inside the
	// input batch. fill(prev/next/linear) treats a change in these columns as
	// a hard boundary: values never cross it in either direction.
	PartitionColIdx []int32

	vm.OperatorBase
	colexec.Projection
}

func (fill *Fill) GetOperatorBase() *vm.OperatorBase {
	return &fill.OperatorBase
}

func init() {
	reuse.CreatePool[Fill](
		func() *Fill {
			return &Fill{}
		},
		func(a *Fill) {
			*a = Fill{}
		},
		reuse.DefaultOptions[Fill]().
			WithEnableChecker(),
	)
}

func (fill Fill) TypeName() string {
	return opName
}

func NewArgument() *Fill {
	return reuse.Alloc[Fill](nil)
}

func (fill *Fill) Release() {
	if fill != nil {
		reuse.Free[Fill](fill, nil)
	}
}

func (fill *Fill) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &fill.ctr
	ctr.resetCtrParma()
	ctr.resetExes()
	if ctr.buf != nil {
		ctr.buf.CleanOnlyData()
	}
	for _, b := range ctr.bats {
		if b != nil {
			b.Clean(proc.GetMPool())
		}
	}
	ctr.bats = ctr.bats[:0]

	if fill.ProjectList != nil {
		if fill.OpAnalyzer != nil {
			fill.OpAnalyzer.Alloc(fill.ProjectAllocSize)
		}
		fill.ResetProjection(proc)
	}
}

func (fill *Fill) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &fill.ctr
	ctr.freeBatch(proc.Mp())
	ctr.freeExes()
	ctr.freeVectors(proc.Mp())

	fill.FreeProjection(proc)
}

func (fill *Fill) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	batch := input
	var err error
	if fill.ProjectList != nil {
		batch, err = fill.EvalProjection(input, proc)
	}
	return batch, err
}

func (ctr *container) freeBatch(mp *mpool.MPool) {
	for _, b := range ctr.bats {
		if b != nil {
			b.Clean(mp)
		}
	}
	if ctr.buf != nil {
		ctr.buf.Clean(mp)
		ctr.buf = nil
	}
}

func (ctr *container) freeVectors(mp *mpool.MPool) {
	for _, vec := range ctr.prevVecs {
		if vec != nil {
			vec.Free(mp)
		}
	}
	ctr.prevVecs = nil
}

func (ctr *container) freeExes() {
	for i := range ctr.exes {
		if ctr.exes[i] != nil {
			ctr.exes[i].Free()
		}
	}
	ctr.exes = nil
}

func (ctr *container) resetExes() {
	for i := range ctr.exes {
		if ctr.exes[i] != nil {
			ctr.exes[i].ResetForNextQuery()
		}
	}
}

func (ctr *container) resetCtrParma() {
	ctr.initIndex()
	ctr.done = false
	for i := range ctr.prevValid {
		ctr.prevValid[i] = false
	}
	ctr.prevPartKey = nil
	ctr.prevPartNull = nil
	ctr.prevPartSet = false
}
