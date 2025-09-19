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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (group *Group) shouldSpill() bool {
	return group.SpillThreshold > 0 &&
		group.ctr.currentMemUsage > group.SpillThreshold &&
		len(group.ctr.result1.AggList) > 0 &&
		len(group.ctr.result1.ToPopped) > 0
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

	marshaledAggStates := make([][]byte, len(group.ctr.result1.AggList))
	for i, agg := range group.ctr.result1.AggList {
		if agg != nil {
			marshaledData, err := aggexec.MarshalAggFuncExec(agg)
			if err != nil {
				return fmt.Errorf("failed to marshal aggregator %d: %v", i, err)
			}
			marshaledAggStates[i] = marshaledData
		}
	}

	totalGroups := 0
	for _, bat := range group.ctr.result1.ToPopped {
		if bat != nil {
			totalGroups += bat.RowCount()
		}
	}

	if totalGroups == 0 {
		return nil
	}

	var groupVecs []*vector.Vector
	var groupVecTypes []types.Type
	if len(group.ctr.result1.ToPopped) > 0 && group.ctr.result1.ToPopped[0] != nil {
		numGroupByCols := len(group.ctr.result1.ToPopped[0].Vecs)
		groupVecs = make([]*vector.Vector, numGroupByCols)
		groupVecTypes = make([]types.Type, numGroupByCols)

		for i := 0; i < numGroupByCols; i++ {
			if len(group.ctr.result1.ToPopped[0].Vecs) > i && group.ctr.result1.ToPopped[0].Vecs[i] != nil {
				vecType := *group.ctr.result1.ToPopped[0].Vecs[i].GetType()
				groupVecs[i] = vector.NewOffHeapVecWithType(vecType)
				groupVecTypes[i] = vecType
			}
		}

		for _, bat := range group.ctr.result1.ToPopped {
			if bat != nil && bat.RowCount() > 0 {
				for i, vec := range bat.Vecs {
					if i < len(groupVecs) && groupVecs[i] != nil && vec != nil {
						if err := groupVecs[i].UnionBatch(vec, 0, vec.Length(), nil, proc.Mp()); err != nil {
							for j := range groupVecs {
								if groupVecs[j] != nil {
									groupVecs[j].Free(proc.Mp())
								}
							}
							return err
						}
					}
				}
			}
		}
	}

	spillData := &SpillableAggState{
		GroupVectors:       groupVecs,
		GroupVectorTypes:   groupVecTypes,
		MarshaledAggStates: marshaledAggStates,
		GroupCount:         totalGroups,
	}

	spillID, err := group.SpillManager.Spill(spillData)
	if err != nil {
		spillData.Free(proc.Mp())
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

	for _, spillID := range group.ctr.spilledStates {
		spillData, err := group.SpillManager.Retrieve(spillID, proc.Mp())
		if err != nil {
			return err
		}

		spillState, ok := spillData.(*SpillableAggState)
		if !ok {
			spillData.Free(proc.Mp())
			return fmt.Errorf("invalid spilled data type")
		}

		if err = group.restoreAndMergeSpilledAggregators(proc, spillState); err != nil {
			spillData.Free(proc.Mp())
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

func (group *Group) restoreAndMergeSpilledAggregators(proc *process.Process, spillState *SpillableAggState) error {
	if len(spillState.MarshaledAggStates) == 0 {
		return nil
	}

	if len(group.ctr.result1.AggList) == 0 {
		aggs := make([]aggexec.AggFuncExec, len(spillState.MarshaledAggStates))
		defer func() {
			if group.ctr.result1.AggList == nil {
				for _, agg := range aggs {
					if agg != nil {
						agg.Free()
					}
				}
			}
		}()

		for i, marshaledState := range spillState.MarshaledAggStates {
			if len(marshaledState) == 0 {
				continue
			}

			agg, err := aggexec.UnmarshalAggFuncExec(aggexec.NewSimpleAggMemoryManager(proc.Mp()), marshaledState)
			if err != nil {
				return err
			}

			aggExpr := group.Aggs[i]
			if config := aggExpr.GetExtraConfig(); config != nil {
				if err = agg.SetExtraInformation(config, 0); err != nil {
					agg.Free()
					return err
				}
			}

			aggs[i] = agg
		}

		chunkSize := aggexec.GetMinAggregatorsChunkSize(spillState.GroupVectors, aggs)
		aggexec.SyncAggregatorsToChunkSize(aggs, chunkSize)
		group.ctr.result1.ChunkSize = chunkSize

		group.ctr.result1.AggList = aggs

		if len(spillState.GroupVectors) > 0 && spillState.GroupCount > 0 {
			batchesToAdd := make([]*batch.Batch, 0)

			for offset := 0; offset < spillState.GroupCount; offset += chunkSize {
				size := chunkSize
				if offset+size > spillState.GroupCount {
					size = spillState.GroupCount - offset
				}

				bat := getInitialBatchWithSameTypeVecs(spillState.GroupVectors)
				success := true
				for i, vec := range spillState.GroupVectors {
					if vec != nil && i < len(bat.Vecs) {
						if err := bat.Vecs[i].UnionBatch(vec, int64(offset), size, nil, proc.Mp()); err != nil {
							bat.Clean(proc.Mp())
							for _, b := range batchesToAdd {
								b.Clean(proc.Mp())
							}
							return err
						}
					}
				}
				if success {
					bat.SetRowCount(size)
					batchesToAdd = append(batchesToAdd, bat)
				}
			}
			group.ctr.result1.ToPopped = append(group.ctr.result1.ToPopped, batchesToAdd...)
		}

		return nil
	}

	for _, currentAgg := range group.ctr.result1.AggList {
		if currentAgg != nil {
			if err := currentAgg.GroupGrow(spillState.GroupCount); err != nil {
				return err
			}
		}
	}

	tempAggs := make([]aggexec.AggFuncExec, len(spillState.MarshaledAggStates))
	defer func() {
		for _, agg := range tempAggs {
			if agg != nil {
				agg.Free()
			}
		}
	}()

	for i, marshaledState := range spillState.MarshaledAggStates {
		if len(marshaledState) == 0 {
			continue
		}

		agg, err := aggexec.UnmarshalAggFuncExec(aggexec.NewSimpleAggMemoryManager(proc.Mp()), marshaledState)
		if err != nil {
			return err
		}

		aggExpr := group.Aggs[i]
		if config := aggExpr.GetExtraConfig(); config != nil {
			if err = agg.SetExtraInformation(config, 0); err != nil {
				agg.Free()
				return err
			}
		}

		tempAggs[i] = agg
	}

	currentGroupCount := 0
	for _, bat := range group.ctr.result1.ToPopped {
		if bat != nil {
			currentGroupCount += bat.RowCount()
		}
	}

	for i, tempAgg := range tempAggs {
		if tempAgg == nil {
			continue
		}

		currentAgg := group.ctr.result1.AggList[i]
		if currentAgg == nil {
			continue
		}

		for spilledGroupIdx := 0; spilledGroupIdx < spillState.GroupCount; spilledGroupIdx++ {
			currentGroupIdx := currentGroupCount + spilledGroupIdx
			if err := currentAgg.Merge(tempAgg, currentGroupIdx, spilledGroupIdx); err != nil {
				return err
			}
		}
	}

	if len(spillState.GroupVectors) > 0 && spillState.GroupCount > 0 {
		chunkSize := group.ctr.result1.ChunkSize
		if chunkSize == 0 {
			chunkSize = spillState.GroupCount
		}

		batchesToAdd := make([]*batch.Batch, 0)
		for offset := 0; offset < spillState.GroupCount; offset += chunkSize {
			size := chunkSize
			if offset+size > spillState.GroupCount {
				size = spillState.GroupCount - offset
			}

			bat := getInitialBatchWithSameTypeVecs(spillState.GroupVectors)
			success := true
			for i, vec := range spillState.GroupVectors {
				if vec != nil && i < len(bat.Vecs) {
					if err := bat.Vecs[i].UnionBatch(vec, int64(offset), size, nil, proc.Mp()); err != nil {
						bat.Clean(proc.Mp())
						for _, b := range batchesToAdd {
							b.Clean(proc.Mp())
						}
						return err
					}
				}
			}
			if success {
				bat.SetRowCount(size)
				batchesToAdd = append(batchesToAdd, bat)
			}
		}
		group.ctr.result1.ToPopped = append(group.ctr.result1.ToPopped, batchesToAdd...)
	}

	return nil
}
