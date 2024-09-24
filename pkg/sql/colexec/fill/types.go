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

const (
	receiveBat  = 0
	findNull    = 2
	findValue   = 3
	fillValue   = 4
	findNullPre = 5
)

type container struct {

	// value
	valVecs []*vector.Vector

	// prev
	prevVecs []*vector.Vector

	// next
	bats      []*batch.Batch
	preIdx    int
	preRow    int
	curIdx    int
	curRow    int
	status    int
	subStatus int
	idx       int
	buf       *batch.Batch

	// linear
	nullIdx int
	nullRow int
	exes    []colexec.ExpressionExecutor
	done    bool

	process func(ctr *container, ap *Fill, proc *process.Process, anal process.Analyzer) (vm.CallResult, error)
}

type Fill struct {
	ctr container

	ColLen   int
	FillType plan.Node_FillType
	FillVal  []*plan.Expr
	AggIds   []int32

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
}
