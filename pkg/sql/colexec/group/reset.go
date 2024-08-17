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

import "github.com/matrixorigin/matrixone/pkg/vm/process"

func (group *Group) Reset(
    proc *process.Process, isPipelineFail bool, pipelineErr error) {

    group.ctr.skipInitReusableMem = true
    mp := proc.Mp()
    group.ctr.cleanBatch(mp)
    group.ctr.cleanHashMap()

    if group.ProjectList != nil {
        anal := proc.GetAnalyze(group.GetIdx(), group.GetParallelIdx(), group.GetParallelMajor())
        anal.Alloc(group.ProjectAllocSize)
        group.FreeProjection(proc)
    }
}