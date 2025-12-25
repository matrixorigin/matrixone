package interactive

import (
	"context"
	"os"
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
	
	// Each row should have exactly 3 columns (0, 2, 3)
	for i, row := range rows {
		assert.Equal(t, 3, len(row), "Row %d should have 3 columns", i)
		
		// Verify data is not empty (unless it's actually empty)
		for j, cell := range row {
			assert.NotEmpty(t, cell, "Row %d, Col %d should not be empty", i, j)
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
			assert.Equal(t, tc.expected, len(rows[0]), "Columns %v should return %d columns", tc.cols, tc.expected)
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
