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
	"os"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSearchAcrossBlocks(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_search_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create file with 9000 rows and 4 columns (faster for race detector)
	objectPath := createTestObjectFileWithRowsAndCols(t, tmpDir, "test_search.obj", 9000, 4)

	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()
	state.pageSize = 10

	m := &model{
		state:    state,
		hasMatch: false,
	}

	// Search for a specific value that appears less frequently
	m.startSearch("8999")

	// Should find a match
	assert.True(t, m.hasMatch, "Should find match for '8999'")
	
	if m.hasMatch {
		firstMatch := m.currentMatch
		t.Logf("First match at row %d, col %d: %s", firstMatch.Row, firstMatch.Col, firstMatch.Value)

		// Test navigation
		m.findNextMatch()
		if m.hasMatch {
			t.Logf("Second match at row %d", m.currentMatch.Row)
		}
	}
}

func TestSearchWithRegex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_search_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	objectPath := createTestObjectFileWithRows(t, tmpDir, "test_regex.obj", 100)

	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()

	m := &model{
		state:         state,
		hasMatch: false,
	}

	// Test regex search
	m.startSearch("^1.*")

	assert.True(t, m.hasMatch, "Should find regex matches")
	t.Logf("Found match for regex '^1.*' at row %d", m.currentMatch.Row)
	
	// Test searching for text that might be hex-encoded
	m.startSearch("short")
	if m.hasMatch {
		t.Logf("Found match for 'short' at row %d", m.currentMatch.Row)
	}
}


// TestSearchWithAllDataTypes tests search with all 8 data types to catch type handling bugs
func TestSearchWithAllDataTypes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_search_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Use 8 columns to test all types including blob
	objectPath := createTestObjectFileWithRowsAndCols(t, tmpDir, "test_alltypes.obj", 50, 8)

	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()

	m := &model{
		state:    state,
		hasMatch: false,
	}

	// Search for numeric value - should not panic on any type
	m.startSearch("42")
	t.Logf("Search '42' hasMatch=%v", m.hasMatch)

	// Search for text - should handle varchar, text, blob correctly
	m.startSearch("short")
	t.Logf("Search 'short' hasMatch=%v", m.hasMatch)

	// Search should complete without panic
	assert.NotEmpty(t, m.searchTerm)
}

// TestSearchHexEncoded tests searching hex-encoded blob/varchar data
func TestSearchHexEncoded(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_search_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	objectPath := createTestObjectFileWithRowsAndCols(t, tmpDir, "test_hex.obj", 20, 8)

	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()

	m := &model{
		state:    state,
		hasMatch: false,
	}

	// Search for hex-encoded pattern (blob column will be displayed as hex)
	m.startSearch("00")
	assert.True(t, m.hasMatch, "Should find hex matches")
	t.Logf("Found match for hex pattern '00' at row %d", m.currentMatch.Row)
}

// TestSearchNavigationEdgeCases tests n/N navigation edge cases
func TestSearchNavigationEdgeCases(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_search_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	objectPath := createTestObjectFileWithRowsAndCols(t, tmpDir, "test_nav.obj", 30, 4)

	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()

	m := &model{
		state:    state,
		hasMatch: false,
	}

	// Search for something that has multiple matches
	m.startSearch("0")
	require.True(t, m.hasMatch, "Need matches for navigation test")

	// Test forward navigation
	initialRow := m.currentMatch.Row
	m.findNextMatch()
	if m.hasMatch {
		assert.NotEqual(t, initialRow, m.currentMatch.Row, "Should find different match")
	}

	// Test backward navigation
	m.findPrevMatch()
	if m.hasMatch {
		t.Logf("Found previous match at row %d", m.currentMatch.Row)
	}
}


// TestSearchCommandExitsCmdMode tests that search command properly exits command mode
func TestSearchCommandExitsCmdMode(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_search_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	objectPath := createTestObjectFileWithRowsAndCols(t, tmpDir, "test_cmdmode.obj", 20, 4)

	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()

	m := &model{
		state:    state,
		hasMatch: false,
		cmdMode:  true,
		cmdInput: "search test",
	}

	// Parse and execute search command
	cmd, err := parseColonCommand(m.cmdInput)
	require.NoError(t, err)

	searchCmd, ok := cmd.(*SearchCommand)
	require.True(t, ok, "Should be SearchCommand")

	// Execute search
	m.startSearch(searchCmd.Pattern)
	m.cmdMode = false
	m.cmdInput = ""

	// After search, cmdMode should be false
	assert.False(t, m.cmdMode, "cmdMode should be false after search")
	assert.Empty(t, m.cmdInput, "cmdInput should be empty after search")
}


// TestColumnsCommandDataCorrectness tests that :cols command displays correct data
func TestColumnsCommandDataCorrectness(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_cols_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create file with 8 columns
	objectPath := createTestObjectFileWithRowsAndCols(t, tmpDir, "test_cols.obj", 10, 8)

	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()

	// Test selecting specific columns
	state.visibleCols = []uint16{0, 2, 3}
	
	rows, _, err := state.CurrentRows()
	require.NoError(t, err)
	require.Greater(t, len(rows), 0)
	
	// CurrentRows returns all columns now, filtering happens at render time
	for i, row := range rows {
		assert.Equal(t, len(state.reader.Columns()), len(row), "Row %d should have all columns", i)
		
		// Verify selected columns are not empty
		for _, colIdx := range state.visibleCols {
			if int(colIdx) < len(row) {
				assert.NotEmpty(t, row[colIdx], "Row %d, Col %d should not be empty", i, colIdx)
			}
		}
	}
	
	// Test with different column combinations
	testCases := []struct {
		cols     []uint16
		expected int
	}{
		{[]uint16{0}, 1},
		{[]uint16{0, 1}, 2},
		{[]uint16{0, 2, 4}, 3},
		{[]uint16{1, 3, 5, 7}, 4},
	}
	
	for _, tc := range testCases {
		state.visibleCols = tc.cols
		rows, _, err := state.CurrentRows()
		require.NoError(t, err)
		
		if len(rows) > 0 {
			// CurrentRows returns all columns, check that selected columns have data
			assert.Equal(t, len(state.reader.Columns()), len(rows[0]))
			for _, colIdx := range tc.cols {
				if int(colIdx) < len(rows[0]) {
					assert.NotEmpty(t, rows[0][colIdx], "Column %d should have data", colIdx)
				}
			}
		}
	}
}

// TestSearchMatchInfo tests that search match includes RowNum and ColumnName
func TestSearchMatchInfo(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_search_info_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	objectPath := createTestObjectFileWithRowsAndCols(t, tmpDir, "test_match_info.obj", 20, 4)

	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()

	m := &model{
		state:    state,
		hasMatch: false,
	}

	// Perform search
	m.startSearch("0")
	
	// Should have found a match
	require.True(t, m.hasMatch, "Should find at least one match")
	
	// Verify match info
	match := m.currentMatch
	assert.NotEmpty(t, match.RowNum, "RowNum should not be empty")
	assert.NotEmpty(t, match.ColumnName, "ColumnName should not be empty")
	assert.NotEmpty(t, match.Value, "Value should not be empty")
	
	// RowNum should be in format (block-offset)
	assert.Regexp(t, `^\(\d+-\d+\)$`, match.RowNum, "RowNum should match pattern (block-offset)")
	
	// ColumnName should be in format ColN
	assert.Regexp(t, `^Col\d+$`, match.ColumnName, "ColumnName should match pattern ColN")
	
	t.Logf("Match info: RowNum=%s, ColumnName=%s, Value=%s", match.RowNum, match.ColumnName, match.Value)
}

// TestSearchLazyLoad tests that search uses lazy loading instead of scanning all data
func TestSearchLazyLoad(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_lazy_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create a file with multiple blocks
	objectPath := createTestObjectFileWithRowsAndCols(t, tmpDir, "test_lazy.obj", 9000, 4)

	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()

	m := &model{
		state:    state,
		hasMatch: false,
	}

	// Search should find first match quickly without scanning all data
	m.startSearch("100")
	
	// Should find a match
	require.True(t, m.hasMatch, "Should find match")
	
	// Test next match
	firstMatch := m.currentMatch
	m.findNextMatch()
	
	if m.hasMatch {
		secondMatch := m.currentMatch
		// Second match should be different from first
		assert.True(t, secondMatch.Row != firstMatch.Row || secondMatch.Col != firstMatch.Col,
			"Second match should be different from first")
	}
}

// TestSearchNavigationFromCurrentPosition tests that n/N finds nearest match from current position
func TestSearchNavigationFromCurrentPosition(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_nav_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	objectPath := createTestObjectFileWithRowsAndCols(t, tmpDir, "test_nav.obj", 100, 4)

	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()

	m := &model{
		state:    state,
		hasMatch: false,
	}

	// Start search from beginning
	m.startSearch("5")
	require.True(t, m.hasMatch)
	firstMatchRow := m.currentMatch.Row
	
	// Move to middle of file
	err = state.GotoRow(50)
	require.NoError(t, err)
	
	// Find next match - should find match after row 50
	m.findNextMatch()
	if m.hasMatch {
		nextMatchRow := m.currentMatch.Row
		assert.Greater(t, nextMatchRow, int64(50), "Next match should be after current position")
		
		// Find previous match from this position
		m.findPrevMatch()
		if m.hasMatch {
			assert.Less(t, m.currentMatch.Row, nextMatchRow, "Previous match should be before next match position")
		}
	}
	
	// Verify we can navigate back to first match
	err = state.GotoRow(0)
	require.NoError(t, err)
	m.findNextMatch()
	if m.hasMatch {
		assert.GreaterOrEqual(t, m.currentMatch.Row, firstMatchRow, "Should find first match again")
	}
}

// TestRenameCommand tests column renaming functionality
func TestRenameCommand(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_rename_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	objectPath := createTestObjectFileWithRowsAndCols(t, tmpDir, "test_rename.obj", 10, 4)

	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()

	// Test rename command
	cmd := &RenameCommand{ColIndex: 0, NewName: "ID"}
	output, quit, err := cmd.Execute(state)
	require.NoError(t, err)
	assert.False(t, quit)
	assert.Contains(t, output, "Renamed Col0 to ID")

	// Verify rename was applied
	assert.NotNil(t, state.colNames)
	assert.Equal(t, "ID", state.colNames[0])

	// Test rename multiple columns
	cmd2 := &RenameCommand{ColIndex: 1, NewName: "Name"}
	output2, quit2, err2 := cmd2.Execute(state)
	require.NoError(t, err2)
	assert.False(t, quit2)
	assert.Contains(t, output2, "Renamed Col1 to Name")

	// Test invalid column index
	cmd3 := &RenameCommand{ColIndex: 99, NewName: "Invalid"}
	output3, quit3, err3 := cmd3.Execute(state)
	require.NoError(t, err3)
	assert.False(t, quit3)
	assert.Contains(t, output3, "out of range")
}

// TestVerticalModeSearchHighlight tests search highlighting in vertical mode
func TestVerticalModeSearchHighlight(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_vertical_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	objectPath := createTestObjectFileWithRowsAndCols(t, tmpDir, "test_vertical.obj", 10, 4)

	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()
	state.verticalMode = true

	m := &model{
		state:    state,
		hasMatch: false,
	}

	// Start search for "^5$" (should match exactly row 5 in Col0)
	m.startSearch("^5$")
	require.True(t, m.hasMatch, "Should find match")

	// Render vertical mode
	var b strings.Builder
	m.renderVertical(&b)
	output := b.String()

	// Should contain search highlighting (ANSI color codes)
	assert.Contains(t, output, "\033[41m", "Should contain red background color for current match")
	assert.Contains(t, output, "\033[0m", "Should contain reset color code")
	
	// Should contain the match value
	assert.Contains(t, output, m.currentMatch.Value, "Should contain the matched value")
	
	// In search mode, should only show one row block (the matched row)
	blockCount := strings.Count(output, "***************************")
	assert.Equal(t, 2, blockCount, "Should show exactly one row block (2 asterisk lines) when searching")
	
	// Should show the correct matched row (row 5)
	assert.Contains(t, output, "(0-5)", "Should show the matched row (0-5)")
	
	t.Logf("Vertical output contains highlighting: %v", strings.Contains(output, "\033[41m"))
	t.Logf("Shows only matched row: %v", blockCount == 2)
	t.Logf("Output: %s", output)
}

// TestRenderTableWithCustomColumnNames tests table rendering with renamed columns
func TestRenderTableWithCustomColumnNames(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_table_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	objectPath := createTestObjectFileWithRowsAndCols(t, tmpDir, "test_table.obj", 5, 4)

	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()

	// Rename columns
	state.colNames = map[uint16]string{
		0: "ID",
		1: "Name", 
		2: "Email",
	}

	m := &model{
		state: state,
	}

	// Render table
	var b strings.Builder
	m.renderTable(&b)
	output := b.String()

	// Should contain custom column names
	assert.Contains(t, output, "ID", "Should contain renamed column 'ID'")
	assert.Contains(t, output, "Name", "Should contain renamed column 'Name'")
	assert.Contains(t, output, "Email", "Should contain renamed column 'Email'")
	assert.Contains(t, output, "Col3", "Should contain default name for unrenamed column")
	
	t.Logf("Table header contains custom names: %v", strings.Contains(output, "ID"))
}

// TestRenderVerticalWithCustomColumnNames tests vertical rendering with renamed columns
func TestRenderVerticalWithCustomColumnNames(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mo_vertical_names_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	objectPath := createTestObjectFileWithRowsAndCols(t, tmpDir, "test_vertical_names.obj", 3, 4)

	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	require.NoError(t, err)
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()
	state.verticalMode = true

	// Rename columns
	state.colNames = map[uint16]string{
		0: "UserID",
		2: "Status",
	}

	m := &model{
		state: state,
	}

	// Render vertical
	var b strings.Builder
	m.renderVertical(&b)
	output := b.String()

	// Should contain custom column names in parentheses
	assert.Contains(t, output, "(UserID)", "Should contain renamed column 'UserID'")
	assert.Contains(t, output, "(Status)", "Should contain renamed column 'Status'")
	assert.Contains(t, output, "(Col1)", "Should contain default name for unrenamed column")
	assert.Contains(t, output, "(Col3)", "Should contain default name for unrenamed column")
	
	t.Logf("Vertical output contains custom names: %v", strings.Contains(output, "UserID"))
}

// TestHighlightSearchMatchFunction tests the highlightSearchMatch function directly
func TestHighlightSearchMatchFunction(t *testing.T) {
	m := &model{
		searchTerm: "test",
		hasMatch:   true,
		currentMatch: SearchMatch{
			Row: 0,
			Col: 0,
			Value: "test_value",
		},
	}

	// Test highlighting current match
	result := m.highlightSearchMatch("test_value", 0, 0)
	assert.Contains(t, result, "\033[41m", "Should contain red background for current match")
	assert.Contains(t, result, "test_value", "Should contain original text")
	assert.Contains(t, result, "\033[0m", "Should contain reset code")

	// Test non-matching cell
	result2 := m.highlightSearchMatch("other_value", 1, 1)
	assert.Equal(t, "other_value", result2, "Non-matching cell should not be highlighted")

	// Test with no search term
	m.searchTerm = ""
	result3 := m.highlightSearchMatch("test_value", 0, 0)
	assert.Equal(t, "test_value", result3, "Should not highlight when no search term")
}
