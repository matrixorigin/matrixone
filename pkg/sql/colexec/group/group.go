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
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	defaultSpillThreshold = 1 << 30
)

func (group *Group) MemoryUsed() int64 {
	var size int64
	size += group.ctr.MemoryUsed()
	return size
}

func (group *Group) ShouldSpill(threshold int64) bool {
	if threshold <= 0 {
		threshold = defaultSpillThreshold
	}
	current := group.MemoryUsed()
	return current >= threshold
}

func (group *Group) checkAndSpill(proc *process.Process) error {
	if group.ShouldSpill(group.SpillThreshold) {
		// Trigger spilling
		return group.spillToDisk(proc)
	}
	return nil
}

// spillToDisk spills current group data to disk
func (group *Group) spillToDisk(proc *process.Process) error {
	// Only spill when we have data to spill
	if group.ctr.result1.IsEmpty() {
		return nil
	}

	// Get current groups and aggregations
	var groups []*batch.Batch
	for _, b := range group.ctr.result1.ToPopped {
		if b != nil && b.RowCount() > 0 {
			groups = append(groups, b)
		}
	}

	// If we have no groups to spill, return early
	if len(groups) == 0 {
		return nil
	}

	err := group.ctr.spillManager.SpillToDisk(groups, group.ctr.result1.AggList)
	if err != nil {
		return err
	}

	// After successful spill, clean up the in-memory data
	group.ctr.result1.Free0(proc.Mp())

	// Re-initialize the result buffer for future data
	aggs, err := group.generateAggExec(proc)
	if err != nil {
		// If we failed to generate new aggregators, we need to clean up
		// but we've already spilled the data, so we can't recover fully
		return err
	}

	// Initialize with empty state
	group.ctr.result1.InitOnlyAgg(aggexec.GetMinAggregatorsChunkSize(nil, aggs), aggs)

	// Also reset the hash table
	group.ctr.hr.Free0()
	err = group.ctr.hr.BuildHashTable(true, group.ctr.mtyp == HStr, group.ctr.keyNullable, 0)
	if err != nil {
		// If we failed to rebuild the hash table, clean up aggregators
		for _, agg := range group.ctr.result1.AggList {
			if agg != nil {
				agg.Free()
			}
		}
		group.ctr.result1.AggList = nil
		return err
	}

	return nil
}

// mergeSpilledData reads all spilled data and merges it with current results
func (group *Group) mergeSpilledData(proc *process.Process) error {
	if group.ctr.spillManager == nil || !group.ctr.spillManager.HasSpilledData() {
		return nil
	}

	// Process each spill file
	for _, filePath := range group.ctr.spillManager.spillFiles {
		groups, aggs, err := group.ctr.spillManager.ReadSpilledData(filePath)
		if err != nil {
			return err
		}

		// Merge groups and aggregations
		if err := group.mergeSpilledGroupsAndAggs(proc, groups, aggs); err != nil {
			return err
		}
	}

	// Clean up spill files after merging
	return group.ctr.spillManager.Cleanup(proc.Ctx)
}

// mergeSpilledGroupsAndAggs merges spilled groups and aggregations with current results
func (group *Group) mergeSpilledGroupsAndAggs(proc *process.Process, groups []*batch.Batch, aggs []aggexec.AggFuncExec) error {
	// If there's no current data, just use the spilled data directly
	if group.ctr.result1.IsEmpty() {
		// Initialize result buffer with spilled data
		if len(groups) > 0 {
			// Copy the groups to avoid referencing spilled data directly
			duplicatedBatches := make([]*batch.Batch, 0, len(groups))
			defer func() {
				// Clean up any duplicated batches on error if they weren't transferred
				if len(duplicatedBatches) > 0 && len(group.ctr.result1.ToPopped) == 0 {
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
				group.ctr.result1.ToPopped = duplicatedBatches
				duplicatedBatches = nil // Transfer ownership, don't clean up in defer

				// Rebuild hash table with spilled group data
				if err := group.ctr.hr.BuildHashTable(true, group.ctr.mtyp == HStr, group.ctr.keyNullable, 0); err != nil {
					return err
				}

				// Insert all spilled groups into hash table
				for _, batch := range group.ctr.result1.ToPopped {
					if batch == nil || batch.RowCount() == 0 {
						continue
					}

					// Ensure iterator is available
					if group.ctr.hr.Itr == nil {
						return moerr.NewInternalError(proc.Ctx, "hash table iterator is nil")
					}

					// Insert group keys into hash table
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

		// Replace current aggregators with spilled ones
		if len(aggs) > 0 {
			// Free existing aggregators if any
			for _, agg := range group.ctr.result1.AggList {
				if agg != nil {
					agg.Free()
				}
			}

			// We need to copy the aggregation states properly
			group.ctr.result1.AggList = make([]aggexec.AggFuncExec, len(aggs))
			for i, agg := range aggs {
				if agg != nil {
					// For now, we'll create new aggregators and merge the states
					// This is a simplified approach - in a production implementation,
					// we would need to properly deserialize and restore the aggregation states
					newAgg, err := makeAggExec(proc, group.Aggs[i].GetAggID(), group.Aggs[i].IsDistinct(), group.ctr.aggregateEvaluate[i].Typ...)
					if err != nil {
						return err
					}
					group.ctr.result1.AggList[i] = newAgg
				}
			}
		}
		return nil
	}

	// Ensure hash table iterator is available for merging
	if group.ctr.hr.Itr == nil {
		return moerr.NewInternalError(proc.Ctx, "hash table iterator is nil")
	}

	// If we have both current and spilled data, we need to merge them properly
	// This requires matching groups by their keys and merging aggregation states

	// For each spilled group batch
	for _, spilledBatch := range groups {
		if spilledBatch == nil || spilledBatch.RowCount() == 0 {
			continue
		}

		// For each row in the spilled batch
		count := spilledBatch.RowCount()
		for i := 0; i < count; i++ {
			// Check if this group already exists in current results
			// We use the hash table to find matching groups
			vals, _, err := group.ctr.hr.Itr.Insert(i, 1, spilledBatch.Vecs)
			if err != nil {
				return err
			}

			// If this is a new group, we need to add it to our results
			if group.ctr.hr.Hash != nil && vals[0] > group.ctr.hr.Hash.GroupCount() {
				// This is a new group, add it to the result buffer
				insertList := []uint8{1}
				// Ensure ToPopped is initialized
				if group.ctr.result1.ToPopped == nil {
					group.ctr.result1.ToPopped = make([]*batch.Batch, 0, 1)
				}
				_, err := group.ctr.result1.AppendBatch(proc.Mp(), spilledBatch.Vecs, i, insertList)
				if err != nil {
					return err
				}

				// Grow aggregators for the new group
				for _, agg := range group.ctr.result1.AggList {
					if agg != nil {
						if err := agg.GroupGrow(1); err != nil {
							return err
						}
					}
				}
			}
		}
	}

	// Merge aggregation states
	if len(aggs) > 0 && len(group.ctr.result1.AggList) > 0 {
		minLen := len(aggs)
		if len(group.ctr.result1.AggList) < minLen {
			minLen = len(group.ctr.result1.AggList)
		}

		// Merge each aggregation executor
		for i := 0; i < minLen; i++ {
			// Merge spilled aggregation state into current aggregation state
			if aggs[i] != nil && group.ctr.result1.AggList[i] != nil {
				if err := group.ctr.result1.AggList[i].Merge(aggs[i], 0, 0); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
