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

package shufflebuild

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Argument)

const (
	ReceiveBatch = iota
	BuildHashMap
	SendHashMap
	SendBatch
	End
)

type container struct {
	colexec.ReceiverOperator
	state              int
	hasNull            bool
	isMerge            bool
	multiSels          [][]int32
	batches            []*batch.Batch
	batchIdx           int
	tmpBatch           *batch.Batch
	inputBatchRowCount int
	executor           []colexec.ExpressionExecutor
	vecs               [][]*vector.Vector
	intHashMap         *hashmap.IntHashMap
	strHashMap         *hashmap.StrHashMap
	keyWidth           int // keyWidth is the width of hash columns, it determines which hash map to use.
	uniqueJoinKeys     []*vector.Vector
	runtimeFilterIn    bool
}

type Argument struct {
	ctr *container
	// need to generate a push-down filter expression
	NeedExpr          bool
	IsDup             bool
	Typs              []types.Type
	Conditions        []*plan.Expr
	HashOnPK          bool
	NeedMergedBatch   bool
	NeedAllocateSels  bool
	RuntimeFilterSpec *pbplan.RuntimeFilterSpec
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
	proc.FinalizeRuntimeFilter(arg.RuntimeFilterSpec)
	if ctr != nil {
		ctr.cleanBatches(proc)
		ctr.cleanEvalVectors()
		ctr.cleanHashMap()
		ctr.FreeMergeTypeOperator(pipelineFailed)
		if ctr.isMerge {
			ctr.FreeMergeTypeOperator(pipelineFailed)
		} else {
			ctr.FreeAllReg()
		}
		arg.ctr = nil
	}
}

func (ctr *container) cleanBatches(proc *process.Process) {
	for i := range ctr.batches {
		proc.PutBatch(ctr.batches[i])
	}
	ctr.batches = nil
}

func (ctr *container) cleanEvalVectors() {
	for i := range ctr.executor {
		if ctr.executor[i] != nil {
			ctr.executor[i].Free()
		}
	}
	ctr.executor = nil
}

func (ctr *container) cleanHashMap() {
	if ctr.intHashMap != nil {
		ctr.intHashMap.Free()
		ctr.intHashMap = nil
	}
	if ctr.strHashMap != nil {
		ctr.strHashMap.Free()
		ctr.strHashMap = nil
	}
}
