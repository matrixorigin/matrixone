// Copyright 2021 Matrix Origin
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

package interactive

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGotoRowWithRangeFilter(t *testing.T) {
	ctx := context.Background()

	// Create test object file
	dir := t.TempDir()
	objFile := createTestObjectFileWithRows(t, dir, "test.obj", 100)

	// Create reader
	reader, err := objecttool.Open(ctx, objFile)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	require.NotNil(t, state)
	defer state.Close()

	// Test 1: Without range filter, GotoRow(-1) goes to global last row
	err = state.GotoRow(-1)
	require.NoError(t, err)
	assert.Equal(t, int64(99), state.GlobalRowOffset()) // 0-based, so 99 is last row

	// Test 2: With range filter [10-19], GotoRow(-1) goes to range end
	state.SetRowRange(10, 19)
	err = state.GotoRow(-1)
	require.NoError(t, err)
	assert.Equal(t, int64(19), state.GlobalRowOffset()) // Should be at row 19

	// Test 3: With only start range [50-], GotoRow(-1) goes to global last row
	state.SetRowRange(50, -1)
	err = state.GotoRow(-1)
	require.NoError(t, err)
	assert.Equal(t, int64(99), state.GlobalRowOffset()) // Should be at global last row

	// Test 4: GotoRow(0) should work normally
	err = state.GotoRow(0)
	require.NoError(t, err)
	assert.Equal(t, int64(0), state.GlobalRowOffset())
}

func TestStatusBarCalculation(t *testing.T) {
	ctx := context.Background()

	// Create test object file
	dir := t.TempDir()
	objFile := createTestObjectFileWithRows(t, dir, "test.obj", 50)

	// Create reader
	reader, err := objecttool.Open(ctx, objFile)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	require.NotNil(t, state)
	defer state.Close()

	// Test 1: Without range filter
	err = state.GotoRow(0)
	require.NoError(t, err)

	rows, _, err := state.CurrentRows()
	require.NoError(t, err)

	globalStart := state.GlobalRowOffset() + 1
	globalEnd := globalStart + int64(len(rows)) - 1
	totalRows := int64(reader.Info().RowCount)

	assert.Equal(t, int64(1), globalStart)
	assert.True(t, globalEnd >= globalStart)
	assert.Equal(t, int64(50), totalRows)

	// Test 2: With range filter [5-9] (5 rows)
	state.SetRowRange(5, 9)
	err = state.GotoRow(5) // Go to start of range
	require.NoError(t, err)

	filteredTotal := state.FilteredRowCount()
	assert.Equal(t, int64(5), filteredTotal) // 5 rows in range [5-9]

	// Test 3: Go to end of filtered range
	err = state.GotoRow(-1) // Should go to row 9
	require.NoError(t, err)
	assert.Equal(t, int64(9), state.GlobalRowOffset())
}

func TestNavigationAfterGotoRow(t *testing.T) {
	ctx := context.Background()

	// Create test object file
	dir := t.TempDir()
	objFile := createTestObjectFileWithRows(t, dir, "test.obj", 20)

	// Create reader
	reader, err := objecttool.Open(ctx, objFile)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	require.NotNil(t, state)
	defer state.Close()

	// Set range filter [5-9]
	state.SetRowRange(5, 9)

	// Go to end of range
	err = state.GotoRow(-1)
	require.NoError(t, err)
	assert.Equal(t, int64(9), state.GlobalRowOffset())

	// Test navigation up should work
	err = state.ScrollUp()
	require.NoError(t, err)
	assert.Equal(t, int64(8), state.GlobalRowOffset())
}

func TestEdgeCasesInStatusCalculation(t *testing.T) {
	ctx := context.Background()

	// Create test object file
	dir := t.TempDir()
	objFile := createTestObjectFileWithRows(t, dir, "test.obj", 10)

	// Create reader
	reader, err := objecttool.Open(ctx, objFile)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	require.NotNil(t, state)
	defer state.Close()

	// Test 1: Single row range
	state.SetRowRange(5, 5)
	err = state.GotoRow(-1)
	require.NoError(t, err)
	assert.Equal(t, int64(5), state.GlobalRowOffset())

	filteredTotal := state.FilteredRowCount()
	assert.Equal(t, int64(1), filteredTotal)
}
