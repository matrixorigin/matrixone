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
	"fmt"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestViewModeSwitch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_viewmode_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	objectPath := createTestObjectFileWithRowsAndCols(t, tmpDir, "test_viewmode.obj", 10, 3)

	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()

	// Test initial mode is Data
	assert.Equal(t, ViewModeData, state.viewMode)

	// Test switch to BlkMeta
	cmd := &SwitchModeCommand{Mode: ViewModeBlkMeta}
	output, quit, err := cmd.Execute(state)
	require.NoError(t, err)
	assert.False(t, quit)
	assert.Contains(t, output, "Block Metadata mode")
	assert.Equal(t, ViewModeBlkMeta, state.viewMode)
	assert.NotEmpty(t, state.metaRows)
	assert.NotEmpty(t, state.metaCols)
	t.Logf("BlkMeta mode: %d rows, %d cols", len(state.metaRows), len(state.metaCols))

	// Test switch to ObjMeta
	cmd = &SwitchModeCommand{Mode: ViewModeObjMeta}
	output, quit, err = cmd.Execute(state)
	require.NoError(t, err)
	assert.False(t, quit)
	assert.Contains(t, output, "Object Metadata mode")
	assert.Equal(t, ViewModeObjMeta, state.viewMode)
	assert.NotEmpty(t, state.metaRows)
	t.Logf("ObjMeta mode: %d rows", len(state.metaRows))

	// Test switch back to Data
	cmd = &SwitchModeCommand{Mode: ViewModeData}
	output, quit, err = cmd.Execute(state)
	require.NoError(t, err)
	assert.False(t, quit)
	assert.Contains(t, output, "Data mode")
	assert.Equal(t, ViewModeData, state.viewMode)
}

func TestBlkMetaContent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_blkmeta_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	objectPath := createTestObjectFileWithRowsAndCols(t, tmpDir, "test_blkmeta.obj", 10, 3)

	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()

	// Switch to BlkMeta mode
	err = state.SwitchToBlkMeta()
	require.NoError(t, err)

	// Verify schema
	assert.Equal(t, 9, len(state.metaCols))
	assert.Equal(t, "ColIdx", state.metaCols[0].Name)
	assert.Equal(t, "Name", state.metaCols[1].Name)
	assert.Equal(t, "OriginSize", state.metaCols[2].Name)
	assert.Equal(t, "CompSize", state.metaCols[3].Name)
	assert.Equal(t, "CompRatio", state.metaCols[4].Name)
	assert.Equal(t, "Rows", state.metaCols[5].Name)
	assert.Equal(t, "NullCnt", state.metaCols[6].Name)
	assert.Equal(t, "Min", state.metaCols[7].Name)
	assert.Equal(t, "Max", state.metaCols[8].Name)

	// Verify data
	info := reader.Info()
	// Each block has info.ColCount column rows (summary is shown outside table)
	expectedRows := int(info.BlockCount) * int(info.ColCount)
	assert.Equal(t, expectedRows, len(state.metaRows))

	// Check first row
	if len(state.metaRows) > 0 {
		row := state.metaRows[0]
		assert.Equal(t, "0", row[0])    // ColIdx 0
		assert.Equal(t, "Col0", row[1]) // Name Col0
		t.Logf("First row: %v", row)
	}
}

func TestObjMetaContent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_objmeta_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	objectPath := createTestObjectFileWithRowsAndCols(t, tmpDir, "test_objmeta.obj", 10, 3)

	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()

	// Switch to ObjMeta mode
	err = state.SwitchToObjMeta()
	require.NoError(t, err)

	// Verify schema
	assert.Equal(t, 2, len(state.metaCols))
	assert.Equal(t, "Property", state.metaCols[0].Name)
	assert.Equal(t, "Value", state.metaCols[1].Name)

	// Verify data
	// Should have at least: Path, Blocks, TotalRows, Columns, TotalOriginSize, TotalCompSize, Sorted, Appendable
	// Plus Min/Max for each column (2 per column)
	info := reader.Info()
	minExpectedRows := 8 + int(info.ColCount)*2
	assert.True(t, len(state.metaRows) >= minExpectedRows)

	// Check properties
	properties := make(map[string]string)
	for _, row := range state.metaRows {
		properties[row[0]] = row[1]
	}

	assert.Contains(t, properties, "Path")
	assert.Contains(t, properties, "Blocks")
	assert.Contains(t, properties, "TotalRows")
	assert.Contains(t, properties, "Columns")
	assert.Contains(t, properties, "TotalOriginSize")
	assert.Contains(t, properties, "TotalCompSize")
	assert.Contains(t, properties, "Sorted")
	assert.Contains(t, properties, "Appendable")

	// Check Min/Max for each column
	for colIdx := uint16(0); colIdx < info.ColCount; colIdx++ {
		colName := fmt.Sprintf("Col%d", colIdx)
		assert.Contains(t, properties, colName+".Min")
		assert.Contains(t, properties, colName+".Max")
	}

	t.Logf("Object metadata: %v", properties)
}

func TestMetadataNavigation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_metanav_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	objectPath := createTestObjectFileWithRowsAndCols(t, tmpDir, "test_metanav.obj", 50, 3)

	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()

	// Switch to BlkMeta mode
	err = state.SwitchToBlkMeta()
	require.NoError(t, err)

	// Test scroll down
	initialOffset := state.metaOffset
	err = state.ScrollDown()
	require.NoError(t, err)
	assert.Equal(t, initialOffset+1, state.metaOffset)

	// Test scroll up
	err = state.ScrollUp()
	require.NoError(t, err)
	assert.Equal(t, initialOffset, state.metaOffset)

	// Test next page (only if there are enough rows)
	if len(state.metaRows) > state.pageSize {
		err = state.NextPage()
		require.NoError(t, err)
		assert.Equal(t, state.pageSize, state.metaOffset)

		// Test prev page
		err = state.PrevPage()
		require.NoError(t, err)
		assert.Equal(t, 0, state.metaOffset)
	}
}

func TestRenameInBlkMeta(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_rename_blkmeta_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	objectPath := createTestObjectFileWithRowsAndCols(t, tmpDir, "test_rename.obj", 10, 3)

	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()

	// Rename column 0
	cmd := &RenameCommand{ColIndex: 0, NewName: "UserID"}
	_, _, err = cmd.Execute(state)
	require.NoError(t, err)

	// Switch to BlkMeta mode
	err = state.SwitchToBlkMeta()
	require.NoError(t, err)

	// Check that Column column shows "UserID" for column 0
	found := false
	for _, row := range state.metaRows {
		if row[0] == "0" && row[1] == "UserID" {
			found = true
			break
		}
	}
	assert.True(t, found, "Should find UserID in Column column")
}

func TestMetadataCommandParsing(t *testing.T) {
	tests := []struct {
		input    string
		expected ViewMode
	}{
		{":data", ViewModeData},
		{":blkmeta", ViewModeBlkMeta},
		{":objmeta", ViewModeObjMeta},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			cmd, err := ParseCommand(tt.input)
			require.NoError(t, err)
			require.NotNil(t, cmd)

			switchCmd, ok := cmd.(*SwitchModeCommand)
			require.True(t, ok)
			assert.Equal(t, tt.expected, switchCmd.Mode)
		})
	}
}

func TestCurrentRowsInMetadataMode(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_currentrows_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	objectPath := createTestObjectFileWithRowsAndCols(t, tmpDir, "test_currentrows.obj", 10, 3)

	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()

	// Switch to BlkMeta mode
	err = state.SwitchToBlkMeta()
	require.NoError(t, err)

	// Get current rows
	rows, rowNumbers, err := state.CurrentRows()
	require.NoError(t, err)
	assert.NotEmpty(t, rows)
	assert.Equal(t, len(rows), len(rowNumbers))

	// Verify row numbers format
	// In BlkMeta mode, row numbers are block numbers (no parentheses)
	for _, rowNum := range rowNumbers {
		// Should be numeric (block number)
		assert.NotEmpty(t, rowNum)
	}

	t.Logf("Got %d rows in BlkMeta mode", len(rows))
}
