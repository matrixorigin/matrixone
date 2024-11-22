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

	mergeGroup.ctr.state = Build

	if isPipelineFail {
		mergeGroup.Free(proc, isPipelineFail, pipelineErr)
		return
	}
	mergeGroup.ctr.res.reset(proc.Mp())

	// cannot reuse the projection.
	if mergeGroup.ProjectList != nil {
		if mergeGroup.OpAnalyzer != nil {
			mergeGroup.OpAnalyzer.Alloc(mergeGroup.ProjectAllocSize)
		}
		mergeGroup.FreeProjection(proc)
	}
}

func (mergeGroup *MergeGroup) Free(proc *process.Process, pipelineFailed bool, err error) {
	mergeGroup.ctr.res.free(proc.Mp())
	mergeGroup.ctr.hashKeyWidth = NeedCalculationForKeyWidth
	mergeGroup.FreeProjection(proc)
}

func (mergeGroup *MergeGroup) Release() {
	if mergeGroup != nil {
		reuse.Free[MergeGroup](mergeGroup, nil)
	}
}
