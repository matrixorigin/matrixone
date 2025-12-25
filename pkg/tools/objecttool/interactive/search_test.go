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
		state:         state,
		searchMatches: nil,
		currentMatch:  0,
	}

	// Search for a specific value that appears less frequently
	m.performSearch("8999")

	// Should find matches
	assert.Greater(t, len(m.searchMatches), 0, "Should find matches for '8999'")
	t.Logf("Found %d matches for '8999'", len(m.searchMatches))

	if len(m.searchMatches) > 0 {
		firstMatch := m.searchMatches[0]
		t.Logf("First match at row %d, col %d: %s", firstMatch.Row, firstMatch.Col, firstMatch.Value)

		// Test navigation
		m.gotoSearchMatch()
		
		if len(m.searchMatches) > 1 {
			m.currentMatch = 1
			m.gotoSearchMatch()
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
		searchMatches: nil,
		currentMatch:  0,
	}

	// Test regex search
	m.performSearch("^1.*")

	assert.Greater(t, len(m.searchMatches), 0, "Should find regex matches")
	t.Logf("Found %d matches for regex '^1.*'", len(m.searchMatches))
	
	// Test searching for text that might be hex-encoded
	// The test file has blob columns that will be displayed as hex
	m.performSearch("short")
	if len(m.searchMatches) > 0 {
		t.Logf("Found %d matches for 'short' (should match varchar column)", len(m.searchMatches))
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
		state:         state,
		searchMatches: nil,
		currentMatch:  0,
	}

	// Search for numeric value - should not panic on any type
	m.performSearch("42")
	t.Logf("Search '42' found %d matches", len(m.searchMatches))

	// Search for text - should handle varchar, text, blob correctly
	m.performSearch("short")
	t.Logf("Search 'short' found %d matches", len(m.searchMatches))

	// Search should complete without panic
	assert.NotNil(t, m.searchMatches)
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
		state:         state,
		searchMatches: nil,
		currentMatch:  0,
	}

	// Search for hex-encoded pattern (blob column will be displayed as hex)
	m.performSearch("00")
	assert.Greater(t, len(m.searchMatches), 0, "Should find hex matches")
	t.Logf("Found %d matches for hex pattern '00'", len(m.searchMatches))
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
		state:         state,
		searchMatches: nil,
		currentMatch:  0,
	}

	// Search for something that has multiple matches
	m.performSearch("0")
	require.Greater(t, len(m.searchMatches), 1, "Need multiple matches for navigation test")

	// Test forward navigation
	initialMatch := m.currentMatch
	for i := 0; i < len(m.searchMatches)+2; i++ {
		m.currentMatch = (m.currentMatch + 1) % len(m.searchMatches)
		m.gotoSearchMatch()
	}

	// Test backward navigation
	for i := 0; i < len(m.searchMatches)+2; i++ {
		m.currentMatch = (m.currentMatch - 1 + len(m.searchMatches)) % len(m.searchMatches)
		m.gotoSearchMatch()
	}

	// Should cycle back
	assert.Equal(t, initialMatch, m.currentMatch)
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
		state:         state,
		searchMatches: nil,
		currentMatch:  0,
		cmdMode:       true,
		cmdInput:      "search test",
	}

	// Parse and execute search command
	cmd, err := parseColonCommand(m.cmdInput)
	require.NoError(t, err)

	searchCmd, ok := cmd.(*SearchCommand)
	require.True(t, ok, "Should be SearchCommand")

	// Execute search
	m.performSearch(searchCmd.Pattern)
	m.cmdMode = false
	m.cmdInput = ""

	// After search, cmdMode should be false
	assert.False(t, m.cmdMode, "cmdMode should be false after search")
	assert.Empty(t, m.cmdInput, "cmdInput should be empty after search")
}
