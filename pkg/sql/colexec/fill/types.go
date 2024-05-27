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

var _ vm.Operator = new(Argument)

const (
	receiveBat    = 0
	withoutNewBat = 1
	findNull      = 2
	findValue     = 3
	fillValue     = 4
	findNullPre   = 5
)

type container struct {
	colexec.ReceiverOperator

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
	buf       *batch.Batch
	idx       int

	// linear
	nullIdx int
	nullRow int
	exes    []colexec.ExpressionExecutor
	done    bool

	process func(ctr *container, ap *Argument, proc *process.Process, anal process.Analyze) (vm.CallResult, error)
}

type Argument struct {
	ctr *container

	ColLen   int
	FillType plan.Node_FillType
	FillVal  []*plan.Expr
	AggIds   []int32
	vm.OperatorBase
}

func (arg *Argument) GetOperatorBase() *vm.OperatorBase {
	return &arg.OperatorBase
}

func init() {
	reuse.CreatePool[Argument](
		func() *Argument {
			return &Argument{}
		},
		func(a *Argument) {
			*a = Argument{}
		},
		reuse.DefaultOptions[Argument]().
			WithEnableChecker(),
	)
}

func (arg Argument) TypeName() string {
	return argName
}

func NewArgument() *Argument {
	return reuse.Alloc[Argument](nil)
}

func (arg *Argument) Release() {
	if arg != nil {
		reuse.Free[Argument](arg, nil)
	}
}

func (arg *Argument) Reset(proc *process.Process, pipelineFailed bool, err error) {
	arg.Free(proc, pipelineFailed, err)
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := arg.ctr
	if ctr != nil {
		ctr.FreeMergeTypeOperator(pipelineFailed)
		ctr.cleanBatch(proc.Mp())
		ctr.cleanExes()

		arg.ctr = nil
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
