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

package offset

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Offset)

type container struct {
	seen           uint64 // seen is the number of tuples seen so far
	offset         uint64
	offsetExecutor colexec.ExpressionExecutor
	buf            *batch.Batch
}
type Offset struct {
	ctr        container
	OffsetExpr *plan.Expr

	vm.OperatorBase
}

func (offset *Offset) GetOperatorBase() *vm.OperatorBase {
	return &offset.OperatorBase
}

func init() {
	reuse.CreatePool[Offset](
		func() *Offset {
			return &Offset{}
		},
		func(a *Offset) {
			*a = Offset{}
		},
		reuse.DefaultOptions[Offset]().
			WithEnableChecker(),
	)
}

func (offset Offset) TypeName() string {
	return opName
}

func NewArgument() *Offset {
	return reuse.Alloc[Offset](nil)
}

func (offset *Offset) WithOffset(offsetExpr *plan.Expr) *Offset {
	offset.OffsetExpr = offsetExpr
	return offset
}

func (offset *Offset) Release() {
	if offset != nil {
		reuse.Free[Offset](offset, nil)
	}
}

func (offset *Offset) Reset(proc *process.Process, pipelineFailed bool, err error) {
	if offset.ctr.offsetExecutor != nil {
		offset.ctr.offsetExecutor.ResetForNextQuery()
	}
	if offset.ctr.buf != nil {
		offset.ctr.buf.CleanOnlyData()
	}
	offset.ctr.seen = 0
}

func (offset *Offset) Free(proc *process.Process, pipelineFailed bool, err error) {
	if offset.ctr.offsetExecutor != nil {
		offset.ctr.offsetExecutor.Free()
		offset.ctr.offsetExecutor = nil
	}

	if offset.ctr.buf != nil {
		offset.ctr.buf.Clean(proc.GetMPool())
		offset.ctr.buf = nil
	}
}
