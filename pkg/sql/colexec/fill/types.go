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
	receiveBat    = 0
	withoutNewBat = 1
	findNull      = 2
	findValue     = 3
	fillValue     = 4
	findNullPre   = 5
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
	colIdx    int
	idx       int
	buf       *batch.Batch

	// linear
	nullIdx int
	nullRow int
	exes    []colexec.ExpressionExecutor
	done    bool

	process func(ctr *container, ap *Fill, proc *process.Process, anal process.Analyze) (vm.CallResult, error)
}

type Fill struct {
	ctr *container

	ColLen   int
	FillType plan.Node_FillType
	FillVal  []*plan.Expr
	AggIds   []int32
	vm.OperatorBase
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
	fill.Free(proc, pipelineFailed, err)
}

func (fill *Fill) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := fill.ctr
	if ctr != nil {
		ctr.cleanBatch(proc.Mp())
		ctr.cleanExes()

		fill.ctr = nil
	}
}

func (ctr *container) cleanBatch(mp *mpool.MPool) {
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

func (ctr *container) cleanExes() {
	for i := range ctr.exes {
		if ctr.exes[i] != nil {
			ctr.exes[i].Free()
		}
	}
	ctr.exes = nil
}
