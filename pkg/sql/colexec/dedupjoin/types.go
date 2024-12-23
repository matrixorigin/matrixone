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
	Finalize
	End
)

type evalVector struct {
	executor colexec.ExpressionExecutor
	vec      *vector.Vector
}

type container struct {
	state   int
	lastPos int

	batches       []*batch.Batch
	batchRowCount int64

	exprExecs []colexec.ExpressionExecutor

	joinBat1 *batch.Batch
	cfs1     []func(*vector.Vector, *vector.Vector, int64, int) error

	joinBat2 *batch.Batch
	cfs2     []func(*vector.Vector, *vector.Vector, int64, int) error

	evecs []evalVector
	vecs  []*vector.Vector

	mp *message.JoinMap

	matched     *bitmap.Bitmap
	handledLast bool

	maxAllocSize int64
	rbat         *batch.Batch
	buf          []*batch.Batch
}

type DedupJoin struct {
	ctr        container
	Result     []colexec.ResultPos
	LeftTypes  []types.Type
	RightTypes []types.Type
	Conditions [][]*plan.Expr

	IsShuffle          bool
	ShuffleIdx         int32
	RuntimeFilterSpecs []*plan.RuntimeFilterSpec
	JoinMapTag         int32

	Channel  chan *bitmap.Bitmap
	NumCPU   uint64
	IsMerger bool

	OnDuplicateAction plan.Node_OnDuplicateAction
	DedupColName      string
	DedupColTypes     []plan.Type
	UpdateColIdxList  []int32
	UpdateColExprList []*plan.Expr

	vm.OperatorBase
}

func (dedupJoin *DedupJoin) GetOperatorBase() *vm.OperatorBase {
	return &dedupJoin.OperatorBase
}

func init() {
	reuse.CreatePool(
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
	if !ctr.handledLast && dedupJoin.NumCPU > 1 && !dedupJoin.IsMerger {
		dedupJoin.Channel <- nil
	}
	if dedupJoin.OpAnalyzer != nil {
		dedupJoin.OpAnalyzer.Alloc(ctr.maxAllocSize)
	}
	ctr.maxAllocSize = 0

	ctr.cleanBuf(proc)
	ctr.cleanHashMap()
	ctr.resetExprExecutor()
	ctr.resetEvalVectors()
	ctr.handledLast = false
	ctr.state = Build
	ctr.lastPos = 0
}

func (dedupJoin *DedupJoin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &dedupJoin.ctr
	ctr.cleanBuf(proc)
	ctr.cleanBatch(proc)
	ctr.cleanHashMap()
	ctr.cleanExprExecutor()
	ctr.cleanEvalVectors()
}

func (dedupJoin *DedupJoin) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (ctr *container) resetExprExecutor() {
	for i := range ctr.exprExecs {
		ctr.exprExecs[i].ResetForNextQuery()
	}
}

func (ctr *container) cleanExprExecutor() {
	for i := range ctr.exprExecs {
		ctr.exprExecs[i].Free()
		ctr.exprExecs[i] = nil
	}
}

func (ctr *container) cleanBuf(proc *process.Process) {
	if ctr.matched != nil && ctr.matched.Count() == 0 {
		// hash map will free these batches
		ctr.buf = nil
		return
	}
	for _, bat := range ctr.buf {
		bat.Clean(proc.GetMPool())
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
