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

package group

import (
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (group *Group) Reset(
	proc *process.Process, isPipelineFail bool, pipelineErr error) {

	group.ctr.skipInitReusableMem = true
	group.ctr.state = vm.Build

	// reused the bat.
	if group.ctr.bat != nil {
		if group.NeedEval {
			if len(group.ctr.bat.Vecs) > len(group.Exprs) {
				mp := proc.Mp()

				for i := len(group.Exprs); i < len(group.ctr.bat.Vecs); i++ {
					if group.ctr.bat.Vecs[i] != nil {
						group.ctr.bat.Vecs[i].Free(mp)
						group.ctr.bat.Vecs[i] = nil
					}
				}

				group.ctr.bat.Vecs = group.ctr.bat.Vecs[:len(group.Exprs)]
				for i := 0; i < len(group.ctr.bat.Vecs); i++ {
					group.ctr.bat.Vecs[i].CleanOnlyData()
				}
			}
			group.ctr.bat.SetRowCount(0)
		}

		for i := 0; i < len(group.ctr.bat.Aggs); i++ {
			if group.ctr.bat.Aggs[i] != nil {
				group.ctr.bat.Aggs[i].Free()
				group.ctr.bat.Aggs[i] = nil
			}
		}
		group.ctr.bat.Aggs = nil
	}

	// reset the executors.
	for i := 0; i < len(group.ctr.groupVecs.Executor); i++ {
		if group.ctr.groupVecs.Executor[i] != nil {
			group.ctr.groupVecs.Executor[i].ResetForNextQuery()
		}
	}
	for i := 0; i < len(group.ctr.aggVecs); i++ {
		for j := 0; j < len(group.ctr.aggVecs[i].Executor); j++ {
			if group.ctr.aggVecs[i].Executor[j] != nil {
				group.ctr.aggVecs[i].Executor[j].ResetForNextQuery()
			}
		}
	}

	group.ctr.cleanHashMap()
	if group.ProjectList != nil {
		if group.OpAnalyzer != nil {
			group.OpAnalyzer.Alloc(group.ProjectAllocSize)
		}
		group.FreeProjection(proc)
	}
}
