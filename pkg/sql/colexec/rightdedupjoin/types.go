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

package rightdedupjoin

import (
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
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

var _ vm.Operator = new(RightDedupJoin)

const (
	Build = iota
	Probe
	End
)

type evalVector struct {
	executor colexec.ExpressionExecutor
	vec      *vector.Vector
}

type container struct {
	state int
	itr   hashmap.Iterator
	rbat  *batch.Batch

	exprExecs []colexec.ExpressionExecutor

	evecs []evalVector
	vecs  []*vector.Vector

	mp *message.JoinMap

	matched *bitmap.Bitmap

	maxAllocSize int64

	groupCount      uint64
	buildGroupCount uint64
}

type RightDedupJoin struct {
	ctr        container
	Result     []colexec.ResultPos
	LeftTypes  []types.Type
	RightTypes []types.Type
	Conditions [][]*plan.Expr

	IsShuffle          bool
	ShuffleIdx         int32
	RuntimeFilterSpecs []*plan.RuntimeFilterSpec
	JoinMapTag         int32

	OnDuplicateAction plan.Node_OnDuplicateAction
	DedupColName      string
	DedupColTypes     []plan.Type
	DelColIdx         int32
	UpdateColIdxList  []int32
	UpdateColExprList []*plan.Expr

	vm.OperatorBase
}

func (rightDedupJoin *RightDedupJoin) GetOperatorBase() *vm.OperatorBase {
	return &rightDedupJoin.OperatorBase
}

func init() {
	reuse.CreatePool(
		func() *RightDedupJoin {
			return &RightDedupJoin{}
		},
		func(a *RightDedupJoin) {
			*a = RightDedupJoin{}
		},
		reuse.DefaultOptions[RightDedupJoin]().
			WithEnableChecker(),
	)
}

func (rightDedupJoin RightDedupJoin) TypeName() string {
	return opName
}

func NewArgument() *RightDedupJoin {
	return reuse.Alloc[RightDedupJoin](nil)
}

func (rightDedupJoin *RightDedupJoin) Release() {
	if rightDedupJoin != nil {
		reuse.Free[RightDedupJoin](rightDedupJoin, nil)
	}
}

func (rightDedupJoin *RightDedupJoin) Reset(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &rightDedupJoin.ctr
	if rightDedupJoin.OpAnalyzer != nil {
		rightDedupJoin.OpAnalyzer.Alloc(ctr.maxAllocSize)
	}
	ctr.maxAllocSize = 0
	ctr.itr = nil

	ctr.cleanBitmap(proc)
	ctr.cleanHashMap()
	ctr.resetExprExecutor()
	ctr.resetEvalVectors()
	ctr.state = Build
}

func (rightDedupJoin *RightDedupJoin) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := &rightDedupJoin.ctr
	ctr.cleanBitmap(proc)
	ctr.cleanHashMap()
	ctr.cleanExprExecutor()
	ctr.cleanEvalVectors()
	ctr.cleanBatch(proc)
}

func (rightDedupJoin *RightDedupJoin) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
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

func (ctr *container) cleanBatch(proc *process.Process) {
	if ctr.rbat != nil {
		ctr.rbat.Clean(proc.Mp())
	}
}

func (ctr *container) cleanHashMap() {
	if ctr.mp != nil {
		ctr.mp.Free()
		ctr.mp = nil
	}
}

func (ctr *container) cleanBitmap(proc *process.Process) {
	ctr.matched = nil
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
