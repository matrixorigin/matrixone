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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSpillManager_CreateSpillFile(t *testing.T) {
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

	// Test creating spill file
	filePath := sm.generateSpillFilePath()
	require.NotEmpty(t, filePath)

	// Test that we can create multiple spill files
	filePath2 := sm.generateSpillFilePath()
	require.NotEmpty(t, filePath2)
	require.NotEqual(t, filePath, filePath2)
}

func TestSpillManager_SpillAndRead(t *testing.T) {
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

	// Create some test data
	testBatch := batch.NewWithSize(1)
	testBatch.Vecs[0] = testutil.NewInt64Vector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3})
	testBatch.SetRowCount(3)

	groups := []*batch.Batch{testBatch}

	// Create a simple agg executor for testing
	aggExec, err := aggexec.MakeAgg(proc, aggexec.AggIdOfCountStar, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, aggExec.GroupGrow(3))
	aggs := []aggexec.AggFuncExec{aggExec}

	// Test spilling data
	err = sm.SpillToDisk(groups, aggs)
	require.NoError(t, err)
	require.Equal(t, 1, sm.GetSpillFileCount())

	// Test reading spilled data
	spilledGroups, spilledAggs, err := sm.ReadSpilledData(sm.spillFiles[0])
	require.NoError(t, err)
	require.Len(t, spilledGroups, 1)
	require.Len(t, spilledAggs, 1)
	require.Equal(t, 3, spilledGroups[0].RowCount())

	// Cleanup
	require.NoError(t, sm.Cleanup(t.Context()))
	require.Equal(t, 0, sm.GetSpillFileCount())
}

func TestSpillManager_BasicOperations(t *testing.T) {
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
	require.NotNil(t, sm)

	// Test that we can get group by types
	require.Equal(t, groupByTypes, sm.groupByTypes)

	// Test that we can get agg infos
	require.Equal(t, aggInfos, sm.aggInfos)

	// Test spill file count methods
	require.Equal(t, 0, sm.GetSpillFileCount())
	require.Equal(t, 0, len(sm.spillFiles))
	require.False(t, sm.HasSpilledData())

	// Clean up
	require.NoError(t, sm.Cleanup(t.Context()))
}

func TestSpillManager_SpillToDisk(t *testing.T) {
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

	// Create test data
	testBatch := batch.NewWithSize(1)
	testBatch.Vecs[0] = testutil.NewInt64Vector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3})
	testBatch.SetRowCount(3)

	groups := []*batch.Batch{testBatch}

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

	aggs := []aggexec.AggFuncExec{aggExec}

	// Test spilling to disk
	err = sm.SpillToDisk(groups, aggs)
	require.NoError(t, err)

	// Verify spill file was created
	require.Equal(t, 1, sm.GetSpillFileCount())
	require.True(t, sm.HasSpilledData())

	// Test reading spilled data
	spilledGroups, spilledAggs, err := sm.ReadSpilledData(sm.spillFiles[0])
	require.NoError(t, err)
	require.Len(t, spilledGroups, 1)
	require.Len(t, spilledAggs, 1)

	require.Equal(t, 3, spilledGroups[0].RowCount())

	// Clean up
	require.NoError(t, sm.Cleanup(t.Context()))
	require.Equal(t, 0, sm.GetSpillFileCount())
	require.False(t, sm.HasSpilledData())
}

func TestSpillManager_ErrorHandling(t *testing.T) {
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

	// Test with nil groups and aggregators (should handle gracefully)
	err = sm.SpillToDisk(nil, nil)
	require.NoError(t, err)

	// Test with empty groups and aggregators
	err = sm.SpillToDisk([]*batch.Batch{}, []aggexec.AggFuncExec{})
	require.NoError(t, err)

	// Clean up
	require.NoError(t, sm.Cleanup(t.Context()))
}

func TestSpillManager_CleanupNonExistentFiles(t *testing.T) {
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

	// Add a non-existent file path to the spill files list
	sm.spillFiles = append(sm.spillFiles, "/tmp/non_existent_spill_file.dat")

	// Cleanup should not return an error even if file doesn't exist
	err = sm.Cleanup(t.Context())
	require.NoError(t, err)
	require.Equal(t, 0, len(sm.spillFiles))
}
