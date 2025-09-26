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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

func (group *Group) shouldSpill() bool {
	shouldSpill := group.SpillThreshold > 0 &&
		group.SpillManager != nil &&
		group.ctr.currentMemUsage > group.SpillThreshold &&
		len(group.ctr.result1.AggList) > 0 &&
		len(group.ctr.result1.ToPopped) > 0

	if shouldSpill {
		logutil.Debug("Group operator triggering spill",
			zap.Int64("current_memory_usage", group.ctr.currentMemUsage),
			zap.Int64("spill_threshold", group.SpillThreshold),
			zap.Int("agg_count", len(group.ctr.result1.AggList)),
			zap.Int("batch_count", len(group.ctr.result1.ToPopped)))
	}

	return shouldSpill
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

	previousUsage := group.ctr.currentMemUsage
	group.ctr.currentMemUsage = usage

	if usage > previousUsage && usage > group.SpillThreshold/2 {
		logutil.Debug("Group operator memory usage update",
			zap.Int64("previous_usage", previousUsage),
			zap.Int64("current_usage", usage),
			zap.Int64("spill_threshold", group.SpillThreshold))
	}
}

func (group *Group) spillPartialResults(proc *process.Process) error {
	if len(group.ctr.result1.AggList) == 0 || len(group.ctr.result1.ToPopped) == 0 {
		logutil.Debug("Group operator spill called but no data to spill")
		return nil
	}

	logutil.Info("Group operator starting spill operation",
		zap.Int64("memory_usage", group.ctr.currentMemUsage),
		zap.Int64("spill_threshold", group.SpillThreshold),
		zap.Int("agg_count", len(group.ctr.result1.AggList)))

	marshaledAggStates := make([][]byte, len(group.ctr.result1.AggList))
	for i, agg := range group.ctr.result1.AggList {
		if agg != nil {
			marshaledData, err := aggexec.MarshalAggFuncExec(agg)
			if err != nil {
				logutil.Error("Group operator failed to marshal aggregator",
					zap.Int("agg_index", i), zap.Error(err))
				return err
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
		logutil.Debug("Group operator spill found no groups to spill")
		for _, agg := range group.ctr.result1.AggList {
			if agg != nil {
				agg.Free()
			}
		}
		group.ctr.result1.AggList = nil
		group.ctr.result1.ToPopped = group.ctr.result1.ToPopped[:0]
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
							logutil.Error("Group operator failed to union batch during spill",
								zap.Int("vec_index", i), zap.Error(err))
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
		logutil.Error("Group operator failed to spill data", zap.Error(err))
		spillData.Free(proc.Mp())
		return err
	}

	logutil.Info("Group operator successfully spilled data",
		zap.String("spill_id", string(spillID)),
		zap.Int("total_groups", totalGroups),
		zap.Int64("estimated_size", spillData.EstimateSize()))

	group.ctr.spilledStates = append(group.ctr.spilledStates, spillID)

	for _, agg := range group.ctr.result1.AggList {
		if agg != nil {
			agg.Free()
		}
	}
	group.ctr.result1.AggList = nil

	for _, bat := range group.ctr.result1.ToPopped {
		if bat != nil {
			bat.Clean(proc.Mp())
		}
	}
	group.ctr.result1.ToPopped = group.ctr.result1.ToPopped[:0]

	if group.ctr.hr.Hash != nil {
		group.ctr.hr.Hash.Free()
		group.ctr.hr.Hash = nil
	}

	group.ctr.currentMemUsage = 0
	logutil.Debug("Group operator completed spill cleanup",
		zap.Int("spilled_states_count", len(group.ctr.spilledStates)))
	return nil
}

func (group *Group) mergeSpilledResults(proc *process.Process) error {
	if len(group.ctr.spilledStates) == 0 {
		return nil
	}

	logutil.Info("Group operator starting merge of spilled results",
		zap.Int("spilled_states_count", len(group.ctr.spilledStates)))

	for i, spillID := range group.ctr.spilledStates {
		logutil.Debug("Group operator merging spilled state",
			zap.Int("state_index", i),
			zap.String("spill_id", string(spillID)))

		spillData, err := group.SpillManager.Retrieve(spillID, proc.Mp())
		if err != nil {
			logutil.Error("Group operator failed to retrieve spilled data",
				zap.String("spill_id", string(spillID)), zap.Error(err))
			return err
		}

		spillState, ok := spillData.(*SpillableAggState)
		if !ok {
			logutil.Error("Group operator retrieved invalid spilled data type",
				zap.String("spill_id", string(spillID)))
			spillData.Free(proc.Mp())
			panic(fmt.Sprintf("invalid spilled data type"))
		}

		logutil.Debug("Group operator retrieved spilled state",
			zap.String("spill_id", string(spillID)),
			zap.Int("group_count", spillState.GroupCount),
			zap.Int64("estimated_size", spillState.EstimateSize()))

		if err = group.restoreAndMergeSpilledAggregators(proc, spillState); err != nil {
			logutil.Error("Group operator failed to restore and merge spilled aggregators",
				zap.String("spill_id", string(spillID)), zap.Error(err))
			spillState.Free(proc.Mp())
			return err
		}

		spillState.Free(proc.Mp())
		if err = group.SpillManager.Delete(spillID); err != nil {
			logutil.Error("Group operator failed to delete spilled data",
				zap.String("spill_id", string(spillID)), zap.Error(err))
			return err
		}

		logutil.Debug("Group operator completed merge of spilled state",
			zap.String("spill_id", string(spillID)))
	}

	logutil.Info("Group operator completed merge of all spilled results",
		zap.Int("merged_states_count", len(group.ctr.spilledStates)))

	group.ctr.spilledStates = nil
	return nil
}

func (group *Group) restoreAndMergeSpilledAggregators(proc *process.Process, spillState *SpillableAggState) error {
	if len(spillState.MarshaledAggStates) == 0 {
		logutil.Debug("Group operator restore found no marshaled aggregator states")
		return nil
	}

	logutil.Debug("Group operator restoring spilled aggregators",
		zap.Int("agg_states_count", len(spillState.MarshaledAggStates)),
		zap.Int("group_count", spillState.GroupCount))

	if len(group.ctr.result1.AggList) == 0 {
		logutil.Debug("Group operator initializing aggregators from spilled state")
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
				logutil.Error("Group operator failed to unmarshal aggregator",
					zap.Int("agg_index", i), zap.Error(err))
				return err
			}

			if i < len(group.Aggs) {
				aggExpr := group.Aggs[i]
				if config := aggExpr.GetExtraConfig(); config != nil {
					if err = agg.SetExtraInformation(config, 0); err != nil {
						logutil.Error("Group operator failed to set extra information for aggregator",
							zap.Int("agg_index", i), zap.Error(err))
						agg.Free()
						return err
					}
				}
			}

			aggs[i] = agg
		}

		chunkSize := aggexec.GetMinAggregatorsChunkSize(spillState.GroupVectors, aggs)
		aggexec.SyncAggregatorsToChunkSize(aggs, chunkSize)
		group.ctr.result1.ChunkSize = chunkSize
		group.ctr.result1.AggList = aggs

		logutil.Debug("Group operator initialized aggregators from spilled state",
			zap.Int("chunk_size", chunkSize),
			zap.Int("agg_count", len(aggs)))

		if len(spillState.GroupVectors) > 0 && spillState.GroupCount > 0 {
			batchesToAdd := make([]*batch.Batch, 0)

			for offset := 0; offset < spillState.GroupCount; offset += chunkSize {
				size := chunkSize
				if offset+size > spillState.GroupCount {
					size = spillState.GroupCount - offset
				}

				bat := getInitialBatchWithSameTypeVecs(spillState.GroupVectors)
				for i, vec := range spillState.GroupVectors {
					if vec != nil && i < len(bat.Vecs) {
						if err := bat.Vecs[i].UnionBatch(vec, int64(offset), size, nil, proc.Mp()); err != nil {
							logutil.Error("Group operator failed to union batch during restore",
								zap.Int("vec_index", i), zap.Int("offset", offset), zap.Error(err))
							bat.Clean(proc.Mp())
							for _, b := range batchesToAdd {
								b.Clean(proc.Mp())
							}
							return err
						}
					}
				}
				bat.SetRowCount(size)
				batchesToAdd = append(batchesToAdd, bat)
			}
			group.ctr.result1.ToPopped = append(group.ctr.result1.ToPopped, batchesToAdd...)
		}

		return nil
	}

	logutil.Debug("Group operator merging spilled aggregators with existing ones",
		zap.Int("existing_agg_count", len(group.ctr.result1.AggList)),
		zap.Int("spilled_group_count", spillState.GroupCount))

	for _, currentAgg := range group.ctr.result1.AggList {
		if currentAgg != nil {
			if err := currentAgg.GroupGrow(spillState.GroupCount); err != nil {
				logutil.Error("Group operator failed to grow aggregator groups", zap.Error(err))
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
			logutil.Error("Group operator failed to unmarshal aggregator for merge",
				zap.Int("agg_index", i), zap.Error(err))
			return err
		}

		if i < len(group.Aggs) {
			aggExpr := group.Aggs[i]
			if config := aggExpr.GetExtraConfig(); config != nil {
				if err = agg.SetExtraInformation(config, 0); err != nil {
					logutil.Error("Group operator failed to set extra information for temp aggregator",
						zap.Int("agg_index", i), zap.Error(err))
					agg.Free()
					return err
				}
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
				logutil.Error("Group operator failed to merge aggregator groups",
					zap.Int("agg_index", i),
					zap.Int("current_group_idx", currentGroupIdx),
					zap.Int("spilled_group_idx", spilledGroupIdx),
					zap.Error(err))
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
			for i, vec := range spillState.GroupVectors {
				if vec != nil && i < len(bat.Vecs) {
					if err := bat.Vecs[i].UnionBatch(vec, int64(offset), size, nil, proc.Mp()); err != nil {
						logutil.Error("Group operator failed to union batch during merge",
							zap.Int("vec_index", i), zap.Int("offset", offset), zap.Error(err))
						bat.Clean(proc.Mp())
						for _, b := range batchesToAdd {
							b.Clean(proc.Mp())
						}
						return err
					}
				}
			}
			bat.SetRowCount(size)
			batchesToAdd = append(batchesToAdd, bat)
		}
		group.ctr.result1.ToPopped = append(group.ctr.result1.ToPopped, batchesToAdd...)
	}

	logutil.Debug("Group operator completed restore and merge of spilled aggregators",
		zap.Int("final_batch_count", len(group.ctr.result1.ToPopped)))

	return nil
}
