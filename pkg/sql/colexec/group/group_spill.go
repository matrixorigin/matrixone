// Copyright 2025 Matrix Origin
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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (group *Group) shouldSpill() bool {
	return group.ctr.currentMemUsage > group.SpillThreshold && len(group.ctr.result1.AggList) > 0
}

func (group *Group) updateMemoryUsage(proc *process.Process) {
	usage := int64(0)
	if !group.ctr.hr.IsEmpty() && group.ctr.hr.Hash != nil {
		usage += int64(group.ctr.hr.Hash.Size())
	}
	for _, bat := range group.ctr.result1.ToPopped {
		if bat != nil {
			usage += int64(bat.Size())
		}
	}
	for _, agg := range group.ctr.result1.AggList {
		if agg != nil {
			usage += agg.Size()
		}
	}
	group.ctr.currentMemUsage = usage
}

func (group *Group) spillPartialResults(proc *process.Process) error {
	if len(group.ctr.result1.AggList) == 0 || len(group.ctr.result1.ToPopped) == 0 {
		return nil
	}

	partialStates := make([]any, len(group.ctr.result1.AggList))
	for i, agg := range group.ctr.result1.AggList {
		if agg != nil {
			partial, err := aggexec.MarshalAggFuncExec(agg)
			if err != nil {
				return err
			}
			partialStates[i] = partial
		}
	}

	groupVecs := make([]*vector.Vector, 0)
	totalGroups := 0
	for _, bat := range group.ctr.result1.ToPopped {
		if bat != nil && bat.RowCount() > 0 {
			totalGroups += bat.RowCount()
			for _, vec := range bat.Vecs {
				if vec != nil {
					groupVecs = append(groupVecs, vec)
				}
			}
		}
	}

	spillData := &SpillableAggState{
		GroupVectors:  groupVecs,
		PartialStates: partialStates,
		GroupCount:    totalGroups,
	}

	spillID, err := group.SpillManager.Spill(spillData)
	if err != nil {
		return err
	}

	group.ctr.spilledStates = append(group.ctr.spilledStates, spillID)

	for _, agg := range group.ctr.result1.AggList {
		if agg != nil {
			agg.Free()
		}
	}
	group.ctr.result1.AggList = nil

	for _, bat := range group.ctr.result1.ToPopped {
		if bat != nil {
			bat.CleanOnlyData()
		}
	}
	group.ctr.result1.ToPopped = group.ctr.result1.ToPopped[:0]

	if group.ctr.hr.Hash != nil {
		group.ctr.hr.Hash.Free()
		group.ctr.hr.Hash = nil
	}

	group.ctr.currentMemUsage = 0
	return nil
}

func (group *Group) mergeSpilledResults(proc *process.Process) error {
	if len(group.ctr.spilledStates) == 0 {
		return nil
	}

	if group.ctr.result1.IsEmpty() {
		aggs, err := group.generateAggExec(proc)
		if err != nil {
			return err
		}
		if err = group.ctr.result1.InitWithGroupBy(
			proc.Mp(),
			aggexec.GetMinAggregatorsChunkSize(group.ctr.groupByEvaluate.Vec, aggs), aggs, group.ctr.groupByEvaluate.Vec, 0); err != nil {
			return err
		}
	}

	for _, spillID := range group.ctr.spilledStates {
		spillData, err := group.SpillManager.Retrieve(spillID)
		if err != nil {
			return err
		}

		spillState, ok := spillData.(*SpillableAggState)
		if !ok {
			return fmt.Errorf("invalid spilled data type")
		}

		if err = group.ctr.result1.DealPartialResult(spillState.PartialStates); err != nil {
			return err
		}

		spillData.Free(proc.Mp())
		if err = group.SpillManager.Delete(spillID); err != nil {
			return err
		}
	}

	group.ctr.spilledStates = nil
	return nil
}
