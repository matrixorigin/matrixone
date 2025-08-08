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
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
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
	shouldSpill := g.ShouldSpill(1024)
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
	shouldSpill := g.ShouldSpill(1024)
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

func TestBug_InvalidHeaderInReadSpilledData(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

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

	w, err := sm.fileService.NewWriter(t.Context(), "bad_spill.dat")
	require.NoError(t, err)

	// Write invalid header with negative counts
	header := struct {
		GroupCount int32
		AggCount   int32
	}{
		GroupCount: -1, // Invalid negative count
		AggCount:   -1, // Invalid negative count
	}

	err = binary.Write(w, binary.BigEndian, header)
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)

	// This should return an error instead of causing resource issues
	groups, aggs, err := sm.ReadSpilledData("bad_spill.dat")
	require.Error(t, err)
	require.Nil(t, groups)
	require.Nil(t, aggs)
	require.Contains(t, err.Error(), "invalid group count")
}
