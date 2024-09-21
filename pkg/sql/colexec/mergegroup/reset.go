// Copyright 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	NeedCalculationForKeyWidth = -1
)

func NewArgument() *MergeGroup {
	r := reuse.Alloc[MergeGroup](nil)
	r.ctr.hashKeyWidth = NeedCalculationForKeyWidth
	r.ctr.groupByCol = 0
	return r
}

func (mergeGroup *MergeGroup) Reset(
	proc *process.Process, isPipelineFail bool, pipelineErr error) {

	if isPipelineFail {
		mergeGroup.Free(proc, isPipelineFail, pipelineErr)
		return
	}

	mergeGroup.ctr.state = Build
	// reuse the batch except its agg fields.
	if bat := mergeGroup.ctr.bat; bat != nil {
		for _, agg := range bat.Aggs {
			if agg != nil {
				agg.Free()
			}
		}
		bat.Aggs = nil

		// reset the group-by vectors.
		for i := mergeGroup.ctr.groupByCol; i < len(bat.Vecs); i++ {
			if bat.Vecs[i] != nil {
				bat.Vecs[i].Free(proc.Mp())
				bat.Vecs[i] = nil
			}
		}
		bat.Vecs = bat.Vecs[:mergeGroup.ctr.groupByCol]

		for i := 0; i < len(bat.Vecs); i++ {
			if bat.Vecs[i] != nil {
				bat.Vecs[i].CleanOnlyData()
			}
		}

		// reset the vectors.
		bat.SetRowCount(0)
	}

	// cannot reuse the hash map.
	mergeGroup.ctr.cleanHashMap()

	// cannot reuse the projection.
	if mergeGroup.ProjectList != nil {
		if mergeGroup.OpAnalyzer != nil {
			mergeGroup.OpAnalyzer.Alloc(mergeGroup.ProjectAllocSize)
		}
		mergeGroup.FreeProjection(proc)
	}
}

func (mergeGroup *MergeGroup) Free(proc *process.Process, pipelineFailed bool, err error) {
	mp := proc.Mp()
	mergeGroup.ctr.cleanBatch(mp)
	mergeGroup.ctr.cleanHashMap()
	mergeGroup.ctr.hashKeyWidth = NeedCalculationForKeyWidth

	mergeGroup.FreeProjection(proc)
}

func (mergeGroup *MergeGroup) Release() {
	if mergeGroup != nil {
		reuse.Free[MergeGroup](mergeGroup, nil)
	}
}

func (ctr *container) initEmptyBatchFromInput(bat *batch.Batch) {
	if ctr.bat != nil {
		return
	}

	ctr.bat = batch.NewWithSize(len(bat.Vecs))
	for i := range bat.Vecs {
		ctr.bat.Vecs[i] = vector.NewVec(*bat.Vecs[i].GetType())
	}
}
