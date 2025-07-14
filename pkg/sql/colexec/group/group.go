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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

func (group *Group) MemoryUsed() int64 {
	var size int64
	size += group.ctr.MemoryUsed()
	return size
}

func (group *Group) ShouldSpill() bool {
	if group.SpillThreshold == 0 {
		return false
	}
	current := group.MemoryUsed()
	return current >= group.SpillThreshold
}

func (group *Group) checkAndSpill(proc *process.Process) error {
	logutil.Info("Group: check spill",
		zap.Any("used", group.MemoryUsed()),
		zap.Any("threshold", group.SpillThreshold),
	)
	if group.ShouldSpill() {
		// Trigger spilling
		logutil.Infof("Group: Memory threshold exceeded, triggering spill operation. Current memory: %d bytes, Threshold: %d bytes",
			group.MemoryUsed(), group.SpillThreshold)
		return group.spillToDisk(proc)
	}
	return nil
}

func (group *Group) spillToDisk(proc *process.Process) error {
	if group.ctr.result1.IsEmpty() {
		return nil
	}

	logutil.Infof("Group: Starting spill to disk operation with hash table preservation")
	startTime := time.Now()
	beforeMemory := group.MemoryUsed()

	var groups []*batch.Batch
	for _, b := range group.ctr.result1.ToPopped {
		if b != nil && b.RowCount() > 0 {
			groups = append(groups, b)
		}
	}

	logutil.Infof("Group: Preparing %d batches and %d aggregators for spilling", len(groups), len(group.ctr.result1.AggList))

	if len(groups) == 0 && len(group.ctr.result1.AggList) > 0 {
		emptyBatch := batch.NewOffHeapEmpty()
		groups = append(groups, emptyBatch)
	}

	if len(groups) == 0 && len(group.ctr.result1.AggList) == 0 {
		return nil
	}

	// Serialize hash table state before spilling
	var hashTableData []byte
	var err error
	if !group.ctr.hr.IsEmpty() {
		hashTableData, err = group.ctr.hr.MarshalHashTable()
		if err != nil {
			return moerr.NewInternalErrorNoCtxf("failed to marshal hash table: %v", err)
		}
	}

	// Spill with hash table data
	err = group.ctr.spillManager.SpillToDiskWithHashTable(
		groups,
		group.ctr.result1.AggList,
		hashTableData,
		group.ctr.mtyp == HStr,
		group.ctr.keyNullable,
		group.ctr.keyWidth,
	)
	if err != nil {
		return err
	}

	// Clean up current state and create fresh state for continued processing
	var newHr ResHashRelated
	var newResult GroupResultBuffer

	aggs, err := group.generateAggExec(proc)
	if err != nil {
		for _, agg := range aggs {
			if agg != nil {
				agg.Free()
			}
		}
		return err
	}

	newResult.InitOnlyAgg(aggexec.GetMinAggregatorsChunkSize(nil, aggs), aggs)

	err = newHr.BuildHashTable(true, group.ctr.mtyp == HStr, group.ctr.keyNullable, 0)
	if err != nil {
		for _, agg := range newResult.AggList {
			if agg != nil {
				agg.Free()
			}
		}
		newHr.Free0()
		return err
	}

	oldHr := group.ctr.hr
	oldResult := group.ctr.result1

	group.ctr.hr = newHr
	group.ctr.result1 = newResult

	oldHr.Free0()
	oldResult.Free0(proc.Mp())

	duration := time.Since(startTime)
	afterMemory := group.MemoryUsed()
	logutil.Infof("Group: Successfully completed spill to disk operation with hash table preservation. Duration: %v, Memory before: %d bytes, Memory after: %d bytes",
		duration, beforeMemory, afterMemory)

	return nil
}

func (group *Group) mergeSpilledData(proc *process.Process) error {
	if group.ctr.spillManager == nil || !group.ctr.spillManager.HasSpilledData() {
		return nil
	}

	logutil.Infof("Group: Starting merge of spilled data from %d spill files with hash table preservation", len(group.ctr.spillManager.spillFiles))
	startTime := time.Now()
	beforeMemory := group.MemoryUsed()

	for _, filePath := range group.ctr.spillManager.spillFiles {
		logutil.Infof("Group: Merging spilled data from file %s", filePath)
		groups, aggs, hashTableData, isStrHash, keyNullable, keyWidth, err := group.ctr.spillManager.ReadSpilledDataWithHashTable(filePath)
		if err != nil {
			return err
		}

		if err := group.mergeSpilledGroupsAndAggsWithHashTable(proc, groups, aggs, hashTableData, isStrHash, keyNullable, keyWidth); err != nil {
			return err
		}
	}

	duration := time.Since(startTime)
	afterMemory := group.MemoryUsed()
	logutil.Infof("Group: Successfully completed merge of spilled data with hash table preservation. Duration: %v, Memory before: %d bytes, Memory after: %d bytes",
		duration, beforeMemory, afterMemory)

	return group.ctr.spillManager.Cleanup(proc.Ctx)
}

func (group *Group) mergeSpilledGroupsAndAggs(proc *process.Process, groups []*batch.Batch, aggs []aggexec.AggFuncExec) error {
	return group.mergeSpilledGroupsAndAggsWithHashTable(proc, groups, aggs, nil, false, false, 0)
}

func (group *Group) mergeSpilledGroupsAndAggsWithHashTable(proc *process.Process, groups []*batch.Batch, aggs []aggexec.AggFuncExec, hashTableData []byte, isStrHash bool, keyNullable bool, keyWidth int) error {
	if len(groups) == 0 && len(aggs) == 0 && len(hashTableData) == 0 {
		return nil
	}

	if group.ctr.result1.IsEmpty() {
		return group.restoreSpilledDataAsCurrentStateWithHashTable(proc, groups, aggs, hashTableData, isStrHash, keyNullable, keyWidth)
	}

	if group.ctr.hr.Hash == nil || group.ctr.hr.Itr == nil {
		return moerr.NewInternalError(proc.Ctx, "hash table or iterator is nil during merge")
	}

	// If we have hash table data, try to merge it efficiently
	if len(hashTableData) > 0 {
		if err := group.mergeHashTableData(proc, hashTableData, isStrHash, keyNullable, keyWidth); err != nil {
			logutil.Infof("Group: Failed to merge hash table data, falling back to group re-insertion: %v", err)
		}
	}

	if err := group.mergeSpilledGroups(proc, groups); err != nil {
		return err
	}

	return group.mergeSpilledAggregations(aggs)
}

func (group *Group) restoreSpilledDataAsCurrentState(proc *process.Process, groups []*batch.Batch, aggs []aggexec.AggFuncExec) error {
	return group.restoreSpilledDataAsCurrentStateWithHashTable(proc, groups, aggs, nil, false, false, 0)
}

func (group *Group) restoreSpilledDataAsCurrentStateWithHashTable(proc *process.Process, groups []*batch.Batch, aggs []aggexec.AggFuncExec, hashTableData []byte, isStrHash bool, keyNullable bool, keyWidth int) error {
	if len(groups) > 0 {
		duplicatedBatches := make([]*batch.Batch, 0, len(groups))
		var batchesOwnershipTransferred bool
		defer func() {
			if !batchesOwnershipTransferred {
				for _, batch := range duplicatedBatches {
					if batch != nil {
						batch.Clean(proc.Mp())
					}
				}
			}
		}()

		for _, bat := range groups {
			if bat != nil && bat.RowCount() > 0 {
				newBatch, err := bat.Dup(proc.Mp())
				if err != nil {
					return err
				}
				duplicatedBatches = append(duplicatedBatches, newBatch)
			}
		}

		if len(duplicatedBatches) > 0 {
			// Try to restore hash table from spilled data
			if len(hashTableData) > 0 {
				if err := group.ctr.hr.UnmarshalHashTable(hashTableData, isStrHash, keyNullable, keyWidth); err != nil {
					logutil.Infof("Group: Failed to restore hash table from spilled data, rebuilding: %v", err)
					if err := group.ctr.hr.BuildHashTable(true, isStrHash, keyNullable, 0); err != nil {
						return err
					}
				}
			} else {
				// Fallback to building new hash table
				if err := group.ctr.hr.BuildHashTable(true, group.ctr.mtyp == HStr, group.ctr.keyNullable, 0); err != nil {
					return err
				}
			}

			if group.ctr.hr.Itr == nil {
				return moerr.NewInternalError(proc.Ctx, "hash table iterator is nil after rebuild")
			}

			group.ctr.result1.ToPopped = duplicatedBatches
			batchesOwnershipTransferred = true

			// Re-insert groups into hash table to restore mappings
			for _, batch := range group.ctr.result1.ToPopped {
				if batch == nil || batch.RowCount() == 0 {
					continue
				}

				count := batch.RowCount()
				for i := 0; i < count; i += hashmap.UnitLimit {
					n := count - i
					if n > hashmap.UnitLimit {
						n = hashmap.UnitLimit
					}

					_, _, err := group.ctr.hr.Itr.Insert(i, n, batch.Vecs)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	if len(aggs) > 0 {
		for _, agg := range group.ctr.result1.AggList {
			if agg != nil {
				agg.Free()
			}
		}

		group.ctr.result1.AggList = make([]aggexec.AggFuncExec, len(aggs))
		copy(group.ctr.result1.AggList, aggs)
	}

	return nil
}

func (group *Group) mergeSpilledGroups(proc *process.Process, groups []*batch.Batch) error {
	for _, spilledBatch := range groups {
		if spilledBatch == nil || spilledBatch.RowCount() == 0 {
			continue
		}

		count := spilledBatch.RowCount()
		for i := 0; i < count; i += hashmap.UnitLimit {
			n := count - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}

			originGroupCount := group.ctr.hr.Hash.GroupCount()
			vals, _, err := group.ctr.hr.Itr.Insert(i, n, spilledBatch.Vecs)
			if err != nil {
				return err
			}

			insertList, newGroupCount := group.ctr.hr.GetBinaryInsertList(vals[:n], originGroupCount)
			if newGroupCount > 0 {
				if group.ctr.result1.ToPopped == nil {
					group.ctr.result1.ToPopped = make([]*batch.Batch, 0, 1)
				}

				_, err := group.ctr.result1.AppendBatch(proc.Mp(), spilledBatch.Vecs, i, insertList)
				if err != nil {
					return err
				}

				for _, agg := range group.ctr.result1.AggList {
					if agg != nil {
						if err := agg.GroupGrow(int(newGroupCount)); err != nil {
							return err
						}
					}
				}
			}
		}
	}

	return nil
}

func (group *Group) mergeSpilledAggregations(aggs []aggexec.AggFuncExec) error {
	if len(aggs) == 0 || len(group.ctr.result1.AggList) == 0 {
		return nil
	}

	minLen := len(aggs)
	if len(group.ctr.result1.AggList) < minLen {
		minLen = len(group.ctr.result1.AggList)
	}

	for i := 0; i < minLen; i++ {
		if aggs[i] != nil && group.ctr.result1.AggList[i] != nil {
			if err := group.ctr.result1.AggList[i].Merge(aggs[i], 0, 0); err != nil {
				return err
			}
		}
	}

	return nil
}

func (group *Group) mergeHashTableData(proc *process.Process, hashTableData []byte, isStrHash bool, keyNullable bool, keyWidth int) error {
	if len(hashTableData) == 0 {
		return nil
	}

	//TODO re-insert groups
	return moerr.NewInternalErrorNoCtx("not implemented")
}
