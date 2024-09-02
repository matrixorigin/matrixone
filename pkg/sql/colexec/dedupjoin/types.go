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

package dedupjoin

import (
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(DedupJoin)

const (
	Build = iota
	Probe
	SendResult
	End
)

type evalVector struct {
	executor colexec.ExpressionExecutor
	vec      *vector.Vector
}

type container struct {
	colexec.ReceiverOperator

	state   int
	lastpos int

	batches       []*batch.Batch
	batchRowCount int64
	rbat          *batch.Batch

	expr colexec.ExpressionExecutor

	joinBat1 *batch.Batch
	cfs1     []func(*vector.Vector, *vector.Vector, int64, int) error

	joinBat2 *batch.Batch
	cfs2     []func(*vector.Vector, *vector.Vector, int64, int) error

	evecs []evalVector
	vecs  []*vector.Vector

	mp *message.JoinMap

	matched     *bitmap.Bitmap
	handledLast bool

	tmpBatches []*batch.Batch // for reuse

	maxAllocSize int64
	buf          []*batch.Batch
}

type DedupJoin struct {
	ctr        container
	Result     []int32
	RightTypes []types.Type
	Conditions [][]*plan.Expr

	IsShuffle          bool
	ShuffleIdx         int32
	RuntimeFilterSpecs []*plan.RuntimeFilterSpec
	JoinMapTag         int32

	OnDupAction plan.Node_OnDuplicateAction
	pkColName   string

	vm.OperatorBase
	colexec.Projection
}

func (dedupJoin *DedupJoin) GetOperatorBase() *vm.OperatorBase {
	return &dedupJoin.OperatorBase
}

func init() {
	reuse.CreatePool[DedupJoin](
		func() *DedupJoin {
			return &DedupJoin{}
		},
		func(a *DedupJoin) {
			*a = DedupJoin{}
		},
		reuse.DefaultOptions[DedupJoin]().
			WithEnableChecker(),
	)
}

func (dedupJoin DedupJoin) TypeName() string {
	return opName
}

func NewArgument() *DedupJoin {
	return reuse.Alloc[DedupJoin](nil)
}

func (dedupJoin *DedupJoin) Release() {
	if dedupJoin != nil {
		reuse.Free[DedupJoin](dedupJoin, nil)
	}
}

func (dedupJoin *DedupJoin) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &dedupJoin.ctr
	anal := proc.GetAnalyze(dedupJoin.GetIdx(), dedupJoin.GetParallelIdx(), dedupJoin.GetParallelMajor())
	anal.Alloc(ctr.maxAllocSize)
	ctr.maxAllocSize = 0

	ctr.cleanBuf(proc)
	ctr.cleanHashMap()
	ctr.resetExprExecutor()
	ctr.resetEvalVectors()
	ctr.handledLast = false
	ctr.state = Build
	ctr.lastpos = 0
}

func (dedupJoin *DedupJoin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &dedupJoin.ctr
	ctr.cleanBuf(proc)
	ctr.cleanBatch(proc)
	ctr.cleanEvalVectors()
	ctr.cleanHashMap()
	ctr.cleanExprExecutor()
	ctr.tmpBatches = nil

}

func (ctr *container) resetExprExecutor() {
	if ctr.expr != nil {
		ctr.expr.ResetForNextQuery()
	}
}

func (ctr *container) cleanExprExecutor() {
	if ctr.expr != nil {
		ctr.expr.Free()
		ctr.expr = nil
	}
}

func (ctr *container) cleanBuf(proc *process.Process) {
	for _, bat := range ctr.buf {
		if bat != ctr.rbat {
			bat.Clean(proc.GetMPool())
		}
	}
	ctr.buf = nil
}

func (ctr *container) cleanBatch(proc *process.Process) {
	ctr.batches = nil

	if ctr.rbat != nil {
		ctr.rbat.Clean(proc.GetMPool())
		ctr.rbat = nil
	}
	if ctr.joinBat1 != nil {
		ctr.joinBat1.Clean(proc.GetMPool())
		ctr.joinBat1 = nil
	}
	if ctr.joinBat2 != nil {
		ctr.joinBat2.Clean(proc.GetMPool())
		ctr.joinBat2 = nil
	}
}

func (ctr *container) cleanHashMap() {
	if ctr.mp != nil {
		ctr.mp.Free()
		ctr.mp = nil
	}
}

func (ctr *container) cleanEvalVectors() {
	for i := range ctr.evecs {
		if ctr.evecs[i].executor != nil {
			ctr.evecs[i].executor.Free()
		}
		ctr.evecs[i].vec = nil
	}
	ctr.evecs = nil
}

func (ctr *container) resetEvalVectors() {
	for i := range ctr.evecs {
		if ctr.evecs[i].executor != nil {
			ctr.evecs[i].executor.ResetForNextQuery()
		}
	}
}
