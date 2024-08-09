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

package mergegroup

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(MergeGroup)

const (
	Build = iota
	Eval
	End
)

const (
	H0 = iota
	H8
	HStr
)

type container struct {
	state     int
	typ       int
	inserted  []uint8
	zInserted []uint8

	intHashMap *hashmap.IntHashMap
	strHashMap *hashmap.StrHashMap

	bat *batch.Batch
}

type MergeGroup struct {
	NeedEval bool // need to projection the aggregate column
	ctr      *container

	PartialResults     []any
	PartialResultTypes []types.T

	vm.OperatorBase
	colexec.Projection
}

func (mergeGroup *MergeGroup) GetOperatorBase() *vm.OperatorBase {
	return &mergeGroup.OperatorBase
}

func init() {
	reuse.CreatePool[MergeGroup](
		func() *MergeGroup {
			return &MergeGroup{}
		},
		func(a *MergeGroup) {
			*a = MergeGroup{}
		},
		reuse.DefaultOptions[MergeGroup]().
			WithEnableChecker(),
	)
}

func (mergeGroup MergeGroup) TypeName() string {
	return opName
}

func NewArgument() *MergeGroup {
	return reuse.Alloc[MergeGroup](nil)
}

func (mergeGroup *MergeGroup) WithNeedEval(needEval bool) *MergeGroup {
	mergeGroup.NeedEval = needEval
	return mergeGroup
}

func (mergeGroup *MergeGroup) Release() {
	if mergeGroup != nil {
		reuse.Free[MergeGroup](mergeGroup, nil)
	}
}

func (mergeGroup *MergeGroup) Reset(proc *process.Process, pipelineFailed bool, err error) {
	mergeGroup.Free(proc, pipelineFailed, err)
}

func (mergeGroup *MergeGroup) Free(proc *process.Process, pipelineFailed bool, err error) {
	ctr := mergeGroup.ctr
	if ctr != nil {
		mp := proc.Mp()
		ctr.cleanBatch(mp)
		ctr.cleanHashMap()
		mergeGroup.ctr = nil
	}
	if mergeGroup.ProjectList != nil {
		anal := proc.GetAnalyze(mergeGroup.GetIdx(), mergeGroup.GetParallelIdx(), mergeGroup.GetParallelMajor())
		anal.Alloc(mergeGroup.ProjectAllocSize)
		mergeGroup.FreeProjection(proc)
	}
}

func (ctr *container) cleanBatch(mp *mpool.MPool) {
	if ctr.bat != nil {
		ctr.bat.Clean(mp)
		ctr.bat = nil
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
