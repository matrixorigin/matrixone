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

package hashbuild

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Argument)

const (
	BuildHashMap = iota
	HandleRuntimeFilter
	Eval
	End
)

type evalVector struct {
	executor colexec.ExpressionExecutor
	vec      *vector.Vector
}

type container struct {
	colexec.ReceiverOperator

	state int

	hasNull            bool
	isMerge            bool
	multiSels          [][]int32
	bat                *batch.Batch
	inputBatchRowCount int

	evecs []evalVector
	vecs  []*vector.Vector

	intHashMap *hashmap.IntHashMap
	strHashMap *hashmap.StrHashMap
	keyWidth   int // keyWidth is the width of hash columns, it determines which hash map to use.

	uniqueJoinKeys []*vector.Vector
}

type Argument struct {
	ctr *container
	// need to generate a push-down filter expression
	NeedExpr    bool
	NeedHashMap bool
	IsDup       bool
	Ibucket     uint64
	Nbucket     uint64
	Typs        []types.Type
	Conditions  []*plan.Expr

	HashOnPK             bool
	NeedMergedBatch      bool
	RuntimeFilterSenders []*colexec.RuntimeFilterChan

	Info     *vm.OperatorInfo
	children []vm.Operator
}

func (arg *Argument) SetRuntimeFilterSenders(rfs []*colexec.RuntimeFilterChan) {
	arg.RuntimeFilterSenders = rfs
}

func (arg *Argument) SetInfo(info *vm.OperatorInfo) {
	arg.Info = info
}

func (arg *Argument) AppendChild(child vm.Operator) {
	arg.children = append(arg.children, child)
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := arg.ctr
	if ctr != nil {
		mp := proc.Mp()
		ctr.cleanBatch(mp)
		ctr.cleanEvalVectors(mp)
		if !arg.NeedHashMap {
			ctr.cleanHashMap()
		}
		ctr.FreeMergeTypeOperator(pipelineFailed)
		if ctr.isMerge {
			ctr.FreeMergeTypeOperator(pipelineFailed)
		} else {
			ctr.FreeAllReg()
		}
	}
}

func (ctr *container) cleanBatch(mp *mpool.MPool) {
	if ctr.bat != nil {
		ctr.bat.Clean(mp)
		ctr.bat = nil
	}
}

func (ctr *container) cleanEvalVectors(mp *mpool.MPool) {
	for i := range ctr.evecs {
		if ctr.evecs[i].executor != nil {
			ctr.evecs[i].executor.Free()
		}
	}
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
