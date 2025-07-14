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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestGroup_DiskSpill(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	// Set a very low spill threshold to force spilling
	g := &Group{
		OperatorBase:   vm.OperatorBase{},
		NeedEval:       true,
		Exprs:          []*plan.Expr{newColumnExpression(0)},
		GroupingFlag:   []bool{true},
		SpillThreshold: 1024, // Very low threshold to trigger spilling
		Aggs: []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(
				aggexec.AggIdOfCountStar,
				false,
				[]*plan.Expr{
					newColumnExpression(0),
				},
				nil,
			),
		},
	}

	require.NoError(t, g.Prepare(proc))

	// Test that memory tracking works
	initialMemory := g.MemoryUsed()
	if initialMemory < 0 {
		t.Errorf("MemoryUsed should return non-negative value, got %d", initialMemory)
	}

	// Test that spill manager is properly initialized
	if g.ctr.spillManager == nil {
		t.Error("Spill manager should be initialized")
	}

	// Test spill threshold checking
	shouldSpill := g.ShouldSpill()
	// Initially should not spill since memory usage is low
	if shouldSpill {
		t.Error("Should not spill initially with low memory usage")
	}

	// Test size calculation methods
	spillManagerSize := g.ctr.spillManager.Size()
	if spillManagerSize < 0 {
		t.Errorf("SpillManager.Size() should return non-negative value, got %d", spillManagerSize)
	}
}

func TestGroup_SpillAndMerge(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	// Create a group operator with a very low spill threshold
	g := &Group{
		OperatorBase:   vm.OperatorBase{},
		NeedEval:       true,
		Exprs:          []*plan.Expr{newColumnExpression(0)},
		GroupingFlag:   []bool{true},
		SpillThreshold: 1024, // Very low threshold to force spilling
		Aggs: []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(
				aggexec.AggIdOfCountStar,
				false,
				[]*plan.Expr{
					newColumnExpression(0),
				},
				nil,
			),
		},
	}

	require.NoError(t, g.Prepare(proc))

	// Verify spill manager is initialized
	require.NotNil(t, g.ctr.spillManager)

	// Test memory usage tracking
	initialMemory := g.MemoryUsed()
	require.True(t, initialMemory >= 0, "MemoryUsed should return non-negative value")

	// Test spill threshold checking
	shouldSpill := g.ShouldSpill()
	// Initially should not spill since memory usage is low
	require.False(t, shouldSpill, "Should not spill initially with low memory usage")

	// Test spill manager size calculation
	spillManagerSize := g.ctr.spillManager.Size()
	require.True(t, spillManagerSize >= 0, "SpillManager.Size() should return non-negative value")

	// Clean up
	g.Free(proc, false, nil)
}

func TestGroup_SpillToDisk(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	// Create group operator with aggregations
	g := &Group{
		OperatorBase:   vm.OperatorBase{},
		NeedEval:       true,
		Exprs:          []*plan.Expr{newColumnExpression(0)},
		GroupingFlag:   []bool{true},
		SpillThreshold: 1024, // Low threshold to force spilling
		Aggs: []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(
				aggexec.AggIdOfCountStar,
				false,
				[]*plan.Expr{newColumnExpression(0)},
				nil,
			),
		},
	}

	require.NoError(t, g.Prepare(proc))

	// Initialize container with actual group data to spill
	// Create simple aggregator
	aggExec, err := aggexec.MakeAgg(proc, aggexec.AggIdOfCountStar, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, aggExec.GroupGrow(1))

	// Initialize result buffer with aggregator only
	g.ctr.result1.InitOnlyAgg(10, []aggexec.AggFuncExec{aggExec})

	// Add actual group data to spill - create a batch with group data
	groupBatch := batch.NewWithSize(1)
	groupBatch.Vecs[0] = testutil.NewInt64Vector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{1})
	groupBatch.SetRowCount(1)
	g.ctr.result1.ToPopped[0] = groupBatch

	// Fill aggregator with data
	aggExec.Fill(0, 0, []*vector.Vector{testutil.NewInt64Vector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{1})})

	// Initialize hash table with correct type
	g.ctr.mtyp = HStr // Use string hash map to avoid type issues
	g.ctr.keyNullable = false
	require.NoError(t, g.ctr.hr.BuildHashTable(true, true, false, 0))

	// Test spilling to disk
	err = g.spillToDisk(proc)
	require.NoError(t, err)

	// Verify spill file was created
	require.True(t, g.ctr.spillManager.HasSpilledData())
	require.Equal(t, 1, g.ctr.spillManager.GetSpillFileCount())

	// Verify in-memory data was cleaned up
	// After spilling, the data should be cleaned up
	hasData := false
	for _, batch := range g.ctr.result1.ToPopped {
		if batch != nil && batch.RowCount() > 0 {
			hasData = true
			break
		}
	}
	require.False(t, hasData, "Data should be cleaned up after spilling")

	// Clean up
	g.Free(proc, false, nil)
}

func TestGroup_MergeSpilledData(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	// Create group operator
	g := &Group{
		OperatorBase:   vm.OperatorBase{},
		NeedEval:       true,
		Exprs:          []*plan.Expr{newColumnExpression(0)},
		GroupingFlag:   []bool{true},
		SpillThreshold: 1024,
		Aggs: []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(
				aggexec.AggIdOfCountStar,
				false,
				[]*plan.Expr{newColumnExpression(0)},
				nil,
			),
		},
	}

	require.NoError(t, g.Prepare(proc))

	// Create simple test data to spill
	testBatch := batch.NewWithSize(1)
	testBatch.Vecs[0] = testutil.NewInt64Vector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3})
	testBatch.SetRowCount(3)

	// Create aggregator with data
	aggExec, err := aggexec.MakeAgg(proc, aggexec.AggIdOfCountStar, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, aggExec.GroupGrow(3))

	// Fill with test data
	fillBatch := batch.NewWithSize(1)
	fillBatch.Vecs[0] = testutil.NewInt64Vector(5, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 1, 1, 1, 1})
	fillBatch.SetRowCount(5)
	require.NoError(t, aggExec.BulkFill(0, []*vector.Vector{fillBatch.Vecs[0]}))
	require.NoError(t, aggExec.BulkFill(1, []*vector.Vector{fillBatch.Vecs[0]}))
	require.NoError(t, aggExec.BulkFill(2, []*vector.Vector{fillBatch.Vecs[0]}))

	// Spill the data
	groups := []*batch.Batch{testBatch}
	aggs := []aggexec.AggFuncExec{aggExec}

	err = g.ctr.spillManager.SpillToDisk(groups, aggs)
	require.NoError(t, err)

	// Verify data was spilled
	require.True(t, g.ctr.spillManager.HasSpilledData())

	// Test merging spilled data
	err = g.mergeSpilledData(proc)
	require.NoError(t, err)

	// Verify spill files were cleaned up
	require.False(t, g.ctr.spillManager.HasSpilledData())

	// Clean up
	g.Free(proc, false, nil)
}

func TestGroup_SpillAndMergeCycle(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	// Create group operator
	g := &Group{
		OperatorBase:   vm.OperatorBase{},
		NeedEval:       true,
		Exprs:          []*plan.Expr{newColumnExpression(0)},
		GroupingFlag:   []bool{true},
		SpillThreshold: 1024,
		Aggs: []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(
				aggexec.AggIdOfCountStar,
				false,
				[]*plan.Expr{newColumnExpression(0)},
				nil,
			),
		},
	}

	require.NoError(t, g.Prepare(proc))

	// Simplified test - just verify the methods can be called without errors
	// when there's no data to spill
	err := g.spillToDisk(proc)
	require.NoError(t, err)

	// Test merging when there's no spilled data
	err = g.mergeSpilledData(proc)
	require.NoError(t, err)

	// Clean up
	g.Free(proc, false, nil)
}

func TestGroup_MergeSpilledDataWithEmptyResults(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	// Create group operator
	g := &Group{
		OperatorBase:   vm.OperatorBase{},
		NeedEval:       true,
		Exprs:          []*plan.Expr{newColumnExpression(0)},
		GroupingFlag:   []bool{true},
		SpillThreshold: 1024,
		Aggs: []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(
				aggexec.AggIdOfCountStar,
				false,
				[]*plan.Expr{newColumnExpression(0)},
				nil,
			),
		},
	}

	require.NoError(t, g.Prepare(proc))

	// Test merging when result buffer has nil ToPopped slice
	// This should not panic
	g.ctr.result1.ToPopped = nil

	// Create simple test data to spill
	testBatch := batch.NewWithSize(1)
	testBatch.Vecs[0] = testutil.NewInt64Vector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3})
	testBatch.SetRowCount(3)

	// Create aggregator with data
	aggExec, err := aggexec.MakeAgg(proc, aggexec.AggIdOfCountStar, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, aggExec.GroupGrow(3))

	// Fill with test data
	fillBatch := batch.NewWithSize(1)
	fillBatch.Vecs[0] = testutil.NewInt64Vector(5, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 1, 1, 1, 1})
	fillBatch.SetRowCount(5)
	require.NoError(t, aggExec.BulkFill(0, []*vector.Vector{fillBatch.Vecs[0]}))
	require.NoError(t, aggExec.BulkFill(1, []*vector.Vector{fillBatch.Vecs[0]}))
	require.NoError(t, aggExec.BulkFill(2, []*vector.Vector{fillBatch.Vecs[0]}))

	// Spill the data
	groups := []*batch.Batch{testBatch}
	aggs := []aggexec.AggFuncExec{aggExec}

	err = g.ctr.spillManager.SpillToDisk(groups, aggs)
	require.NoError(t, err)

	// Verify data was spilled
	require.True(t, g.ctr.spillManager.HasSpilledData())

	// Test merging spilled data - this should not panic even with nil ToPopped
	err = g.mergeSpilledData(proc)
	require.NoError(t, err)

	// Clean up
	g.Free(proc, false, nil)
}

func TestGroup_MergeSpilledGroupsAndAggs(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	// Test 1: Empty result buffer - should call restoreSpilledDataAsCurrentState
	t.Run("EmptyResultBuffer", func(t *testing.T) {
		g := createTestGroupOperator(t, proc)

		// Ensure result buffer is empty
		require.True(t, g.ctr.result1.IsEmpty())

		// Create test spilled data
		groups, aggs := createTestSpilledData(t, proc)

		err := g.mergeSpilledGroupsAndAggs(proc, groups, aggs)
		require.NoError(t, err)

		// Verify data was restored
		require.False(t, g.ctr.result1.IsEmpty())
		require.Equal(t, 1, len(g.ctr.result1.ToPopped))
		require.Equal(t, 1, len(g.ctr.result1.AggList))

		// Cleanup
		cleanupTestData(proc.Mp(), groups, aggs)
	})

	// Test 2: Non-empty result buffer - should merge with existing data
	t.Run("NonEmptyResultBuffer", func(t *testing.T) {
		g := createTestGroupOperatorWithData(t, proc)

		// Ensure result buffer has data
		require.False(t, g.ctr.result1.IsEmpty())
		initialGroupCount := len(g.ctr.result1.ToPopped)
		initialAggCount := len(g.ctr.result1.AggList)

		// Create test spilled data
		groups, aggs := createTestSpilledData(t, proc)

		err := g.mergeSpilledGroupsAndAggs(proc, groups, aggs)
		require.NoError(t, err)

		// Verify data was merged (counts should remain the same for aggregators)
		require.Equal(t, initialAggCount, len(g.ctr.result1.AggList))
		require.GreaterOrEqual(t, len(g.ctr.result1.ToPopped), initialGroupCount)

		// Cleanup
		cleanupTestData(proc.Mp(), groups, aggs)
	})

	// Test 3: Empty input data
	t.Run("EmptyInputData", func(t *testing.T) {
		g := createTestGroupOperator(t, proc)

		err := g.mergeSpilledGroupsAndAggs(proc, nil, nil)
		require.NoError(t, err)

		err = g.mergeSpilledGroupsAndAggs(proc, []*batch.Batch{}, []aggexec.AggFuncExec{})
		require.NoError(t, err)
	})

	// Test 4: Error condition - nil hash table
	t.Run("NilHashTable", func(t *testing.T) {
		g := createTestGroupOperatorWithData(t, proc)
		g.ctr.hr.Hash = nil

		groups, aggs := createTestSpilledData(t, proc)

		err := g.mergeSpilledGroupsAndAggs(proc, groups, aggs)
		require.Error(t, err)
		require.Contains(t, err.Error(), "hash table or iterator is nil")

		// Cleanup
		cleanupTestData(proc.Mp(), groups, aggs)
	})
}

func TestGroup_RestoreSpilledDataAsCurrentState(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	// Test 1: Restore groups only
	t.Run("RestoreGroupsOnly", func(t *testing.T) {
		g := createTestGroupOperator(t, proc)

		groups := createTestBatches(t, proc, 3)

		err := g.restoreSpilledDataAsCurrentState(proc, groups, nil)
		require.NoError(t, err)

		// Verify groups were restored
		require.False(t, g.ctr.result1.IsEmpty())
		require.Equal(t, len(groups), len(g.ctr.result1.ToPopped))
		require.NotNil(t, g.ctr.hr.Hash)
		require.NotNil(t, g.ctr.hr.Itr)

		// Cleanup
		cleanupTestData(proc.Mp(), groups, nil)
	})

	// Test 2: Restore aggregators only
	t.Run("RestoreAggregatorsOnly", func(t *testing.T) {
		g := createTestGroupOperator(t, proc)

		aggs := createTestAggregators(t, proc, 2)

		err := g.restoreSpilledDataAsCurrentState(proc, nil, aggs)
		require.NoError(t, err)

		// Verify aggregators were restored
		require.Equal(t, len(aggs), len(g.ctr.result1.AggList))

		// Note: Don't cleanup aggs as ownership was transferred
	})

	// Test 3: Restore both groups and aggregators
	t.Run("RestoreBothGroupsAndAggregators", func(t *testing.T) {
		g := createTestGroupOperator(t, proc)

		groups := createTestBatches(t, proc, 2)
		aggs := createTestAggregators(t, proc, 1)

		err := g.restoreSpilledDataAsCurrentState(proc, groups, aggs)
		require.NoError(t, err)

		// Verify both were restored
		require.False(t, g.ctr.result1.IsEmpty())
		require.Equal(t, len(groups), len(g.ctr.result1.ToPopped))
		require.Equal(t, len(aggs), len(g.ctr.result1.AggList))

		// Cleanup groups only (aggs ownership transferred)
		cleanupTestData(proc.Mp(), groups, nil)
	})

	// Test 4: Empty/nil batches filtering
	t.Run("EmptyBatchFiltering", func(t *testing.T) {
		g := createTestGroupOperator(t, proc)

		// Create mix of valid, empty, and nil batches
		groups := []*batch.Batch{
			nil,
			batch.NewOffHeapEmpty(),
			createTestBatch(t, proc, 2),
			nil,
			createTestBatch(t, proc, 1),
		}

		err := g.restoreSpilledDataAsCurrentState(proc, groups, nil)
		require.NoError(t, err)

		// Should only restore non-empty batches
		require.Equal(t, 2, len(g.ctr.result1.ToPopped))

		// Cleanup valid batches
		groups[2].Clean(proc.Mp())
		groups[4].Clean(proc.Mp())
	})
}

func TestGroup_MergeSpilledGroups(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	// Test 1: Merge single batch with existing data
	t.Run("MergeSingleBatch", func(t *testing.T) {
		g := createTestGroupOperatorWithData(t, proc)

		initialGroupCount := g.ctr.hr.Hash.GroupCount()

		// Create spilled batch with new groups
		spilledBatch := createTestBatch(t, proc, 2)
		groups := []*batch.Batch{spilledBatch}

		err := g.mergeSpilledGroups(proc, groups)
		require.NoError(t, err)

		// Verify groups were merged
		newGroupCount := g.ctr.hr.Hash.GroupCount()
		require.GreaterOrEqual(t, newGroupCount, initialGroupCount)

		// Cleanup
		spilledBatch.Clean(proc.Mp())
	})

	// Test 2: Merge multiple batches
	t.Run("MergeMultipleBatches", func(t *testing.T) {
		g := createTestGroupOperatorWithData(t, proc)

		initialGroupCount := g.ctr.hr.Hash.GroupCount()

		// Create multiple spilled batches
		groups := createTestBatches(t, proc, 3)

		err := g.mergeSpilledGroups(proc, groups)
		require.NoError(t, err)

		// Verify groups were merged
		newGroupCount := g.ctr.hr.Hash.GroupCount()
		require.GreaterOrEqual(t, newGroupCount, initialGroupCount)

		// Cleanup
		cleanupTestData(proc.Mp(), groups, nil)
	})

	// Test 3: Handle empty and nil batches
	t.Run("HandleEmptyBatches", func(t *testing.T) {
		g := createTestGroupOperatorWithData(t, proc)

		initialGroupCount := g.ctr.hr.Hash.GroupCount()

		// Mix of empty, nil, and valid batches
		groups := []*batch.Batch{
			nil,
			batch.NewOffHeapEmpty(),
			createTestBatch(t, proc, 1),
		}

		err := g.mergeSpilledGroups(proc, groups)
		require.NoError(t, err)

		// Should handle gracefully
		newGroupCount := g.ctr.hr.Hash.GroupCount()
		require.GreaterOrEqual(t, newGroupCount, initialGroupCount)

		// Cleanup
		groups[2].Clean(proc.Mp())
	})

	// Test 4: Large batch processing (multiple chunks)
	t.Run("LargeBatchProcessing", func(t *testing.T) {
		g := createTestGroupOperatorWithData(t, proc)

		// Create a large batch that will be processed in chunks
		largeBatch := createLargeTestBatch(t, proc, 3000) // Larger than UnitLimit
		groups := []*batch.Batch{largeBatch}

		err := g.mergeSpilledGroups(proc, groups)
		require.NoError(t, err)

		// Cleanup
		largeBatch.Clean(proc.Mp())
	})
}

func TestGroup_MergeSpilledAggregations(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	// Test 1: Merge single aggregator
	t.Run("MergeSingleAggregator", func(t *testing.T) {
		g := createTestGroupOperatorWithData(t, proc)

		// Create spilled aggregator
		spilledAggs := createTestAggregators(t, proc, 1)

		err := g.mergeSpilledAggregations(spilledAggs)
		require.NoError(t, err)

		// Cleanup spilled aggs
		for _, agg := range spilledAggs {
			if agg != nil {
				agg.Free()
			}
		}
	})

	// Test 2: Merge multiple aggregators
	t.Run("MergeMultipleAggregators", func(t *testing.T) {
		g := createTestGroupOperatorWithData(t, proc)

		// Ensure we have multiple aggregators in current state
		for len(g.ctr.result1.AggList) < 2 {
			agg := createTestAggregator(t, proc)
			g.ctr.result1.AggList = append(g.ctr.result1.AggList, agg)
		}

		// Create spilled aggregators
		spilledAggs := createTestAggregators(t, proc, 2)

		err := g.mergeSpilledAggregations(spilledAggs)
		require.NoError(t, err)

		// Cleanup
		for _, agg := range spilledAggs {
			if agg != nil {
				agg.Free()
			}
		}
	})

	// Test 3: Handle size mismatches
	t.Run("HandleSizeMismatches", func(t *testing.T) {
		g := createTestGroupOperatorWithData(t, proc)

		// Test with more spilled aggregators than current
		spilledAggs := createTestAggregators(t, proc, 5)

		err := g.mergeSpilledAggregations(spilledAggs)
		require.NoError(t, err)

		// Test with fewer spilled aggregators than current
		spilledAggs2 := createTestAggregators(t, proc, 0)

		err = g.mergeSpilledAggregations(spilledAggs2)
		require.NoError(t, err)

		// Cleanup
		for _, agg := range spilledAggs {
			if agg != nil {
				agg.Free()
			}
		}
	})

	// Test 4: Handle nil aggregators
	t.Run("HandleNilAggregators", func(t *testing.T) {
		g := createTestGroupOperatorWithData(t, proc)

		// Create mix of nil and valid aggregators
		spilledAggs := []aggexec.AggFuncExec{
			nil,
			createTestAggregator(t, proc),
			nil,
		}

		err := g.mergeSpilledAggregations(spilledAggs)
		require.NoError(t, err)

		// Cleanup non-nil aggregators
		for _, agg := range spilledAggs {
			if agg != nil {
				agg.Free()
			}
		}
	})

	// Test 5: Empty aggregator lists
	t.Run("EmptyAggregatorLists", func(t *testing.T) {
		g := createTestGroupOperatorWithData(t, proc)

		err := g.mergeSpilledAggregations(nil)
		require.NoError(t, err)

		err = g.mergeSpilledAggregations([]aggexec.AggFuncExec{})
		require.NoError(t, err)
	})
}

// Helper functions for test setup and data creation

func createTestGroupOperator(t *testing.T, proc *process.Process) *Group {
	g := &Group{
		OperatorBase:   vm.OperatorBase{},
		NeedEval:       true,
		Exprs:          []*plan.Expr{newColumnExpression(0)},
		GroupingFlag:   []bool{true},
		SpillThreshold: 1024,
		Aggs: []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(
				aggexec.AggIdOfCountStar,
				false,
				[]*plan.Expr{newColumnExpression(0)},
				nil,
			),
		},
	}

	require.NoError(t, g.Prepare(proc))
	return g
}

func createTestGroupOperatorWithData(t *testing.T, proc *process.Process) *Group {
	g := createTestGroupOperator(t, proc)

	// Initialize with some data
	aggExec, err := aggexec.MakeAgg(proc, aggexec.AggIdOfCountStar, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, aggExec.GroupGrow(1))

	g.ctr.result1.InitOnlyAgg(10, []aggexec.AggFuncExec{aggExec})

	// Add a test batch
	testBatch := createTestBatch(t, proc, 1)
	g.ctr.result1.ToPopped[0] = testBatch

	// Initialize hash table
	g.ctr.mtyp = H8
	g.ctr.keyNullable = false
	require.NoError(t, g.ctr.hr.BuildHashTable(true, false, false, 0))

	return g
}

func createTestBatch(t *testing.T, proc *process.Process, rowCount int) *batch.Batch {
	values := make([]int64, rowCount)
	for i := 0; i < rowCount; i++ {
		values[i] = int64(i + 1)
	}

	testBatch := batch.NewWithSize(1)
	testBatch.Vecs[0] = testutil.NewInt64Vector(rowCount, types.T_int64.ToType(), proc.Mp(), false, values)
	testBatch.SetRowCount(rowCount)

	return testBatch
}

func createLargeTestBatch(t *testing.T, proc *process.Process, rowCount int) *batch.Batch {
	values := make([]int64, rowCount)
	for i := 0; i < rowCount; i++ {
		values[i] = int64(i%100 + 1) // Create groups with some repetition
	}

	testBatch := batch.NewWithSize(1)
	testBatch.Vecs[0] = testutil.NewInt64Vector(rowCount, types.T_int64.ToType(), proc.Mp(), false, values)
	testBatch.SetRowCount(rowCount)

	return testBatch
}

func createTestBatches(t *testing.T, proc *process.Process, count int) []*batch.Batch {
	batches := make([]*batch.Batch, count)
	for i := 0; i < count; i++ {
		batches[i] = createTestBatch(t, proc, i+1)
	}
	return batches
}

func createTestAggregator(t *testing.T, proc *process.Process) aggexec.AggFuncExec {
	agg, err := aggexec.MakeAgg(proc, aggexec.AggIdOfCountStar, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, agg.GroupGrow(1))
	return agg
}

func createTestAggregators(t *testing.T, proc *process.Process, count int) []aggexec.AggFuncExec {
	aggs := make([]aggexec.AggFuncExec, count)
	for i := 0; i < count; i++ {
		aggs[i] = createTestAggregator(t, proc)
	}
	return aggs
}

func createTestSpilledData(t *testing.T, proc *process.Process) ([]*batch.Batch, []aggexec.AggFuncExec) {
	groups := createTestBatches(t, proc, 1)
	aggs := createTestAggregators(t, proc, 1)
	return groups, aggs
}

func cleanupTestData(mp *mpool.MPool, groups []*batch.Batch, aggs []aggexec.AggFuncExec) {
	for _, batch := range groups {
		if batch != nil {
			batch.Clean(mp)
		}
	}
	for _, agg := range aggs {
		if agg != nil {
			agg.Free()
		}
	}
}

func TestGroup_HashTableSpilling(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	// Test hash table serialization
	t.Run("HashTableSerialization", func(t *testing.T) {
		hr := &ResHashRelated{}

		// Test with nil hash table
		data, err := hr.MarshalHashTable()
		require.NoError(t, err)
		require.Nil(t, data)

		// Test with actual hash table
		err = hr.BuildHashTable(true, false, false, 10)
		require.NoError(t, err)

		data, err = hr.MarshalHashTable()
		require.NoError(t, err)
		require.NotNil(t, data)
		require.True(t, len(data) > 0)

		// Test unmarshaling
		hr2 := &ResHashRelated{}
		err = hr2.UnmarshalHashTable(data, false, false, 8)
		require.NoError(t, err)
		require.NotNil(t, hr2.Hash)

		hr.Free0()
		hr2.Free0()
	})

	// Test spill manager with hash table
	t.Run("SpillManagerWithHashTable", func(t *testing.T) {
		groupByTypes := []types.Type{types.T_int64.ToType()}
		aggInfos := []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(
				aggexec.AggIdOfCountStar,
				false,
				[]*plan.Expr{newColumnExpression(0)},
				nil,
			),
		}

		sm, err := NewSpillManager(proc, groupByTypes, aggInfos)
		require.NoError(t, err)

		// Create test data
		testBatch := batch.NewWithSize(1)
		testBatch.Vecs[0] = testutil.NewInt64Vector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3})
		testBatch.SetRowCount(3)

		groups := []*batch.Batch{testBatch}

		// Create aggregator
		aggExec, err := aggexec.MakeAgg(proc, aggexec.AggIdOfCountStar, false, types.T_int64.ToType())
		require.NoError(t, err)
		require.NoError(t, aggExec.GroupGrow(3))
		aggs := []aggexec.AggFuncExec{aggExec}

		// Create hash table data
		hr := &ResHashRelated{}
		err = hr.BuildHashTable(true, false, false, 3)
		require.NoError(t, err)

		hashTableData, err := hr.MarshalHashTable()
		require.NoError(t, err)

		// Test spilling with hash table
		err = sm.SpillToDiskWithHashTable(groups, aggs, hashTableData, false, false, 8)
		require.NoError(t, err)
		require.Equal(t, 1, sm.GetSpillFileCount())

		// Test reading with hash table
		readGroups, readAggs, readHashTableData, isStrHash, keyNullable, keyWidth, err := sm.ReadSpilledDataWithHashTable(sm.spillFiles[0])
		require.NoError(t, err)
		require.Len(t, readGroups, 1)
		require.Len(t, readAggs, 1)
		require.NotNil(t, readHashTableData)
		require.False(t, isStrHash)
		require.False(t, keyNullable)
		require.Equal(t, 8, keyWidth)

		// Cleanup
		hr.Free0()
		require.NoError(t, sm.Cleanup(t.Context()))
	})

	// Test group operator with hash table spilling
	t.Run("GroupOperatorHashTableSpill", func(t *testing.T) {
		g := createTestGroupOperator(t, proc)

		// Initialize with data that includes hash table
		aggExec, err := aggexec.MakeAgg(proc, aggexec.AggIdOfCountStar, false, types.T_int64.ToType())
		require.NoError(t, err)
		require.NoError(t, aggExec.GroupGrow(3))

		g.ctr.result1.InitOnlyAgg(10, []aggexec.AggFuncExec{aggExec})

		// Add test batches
		testBatch := createTestBatch(t, proc, 3)
		g.ctr.result1.ToPopped[0] = testBatch

		// Initialize hash table with test data
		g.ctr.mtyp = H8
		g.ctr.keyNullable = false
		g.ctr.keyWidth = 8
		require.NoError(t, g.ctr.hr.BuildHashTable(true, false, false, 3))

		// Insert test groups into hash table
		_, _, err = g.ctr.hr.Itr.Insert(0, 3, testBatch.Vecs)
		require.NoError(t, err)

		// Test spilling preserves hash table
		err = g.spillToDisk(proc)
		require.NoError(t, err)

		// Verify spill file was created
		require.True(t, g.ctr.spillManager.HasSpilledData())
		require.Equal(t, 1, g.ctr.spillManager.GetSpillFileCount())

		// Test merging spilled data restores hash table
		err = g.mergeSpilledData(proc)
		require.NoError(t, err)

		// Verify hash table was restored
		require.NotNil(t, g.ctr.hr.Hash)
		require.False(t, g.ctr.spillManager.HasSpilledData())

		// Clean up
		g.Free(proc, false, nil)
	})
}
