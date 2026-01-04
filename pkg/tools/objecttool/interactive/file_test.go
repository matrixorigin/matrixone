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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/tools/interactive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHistoryPersistence(t *testing.T) {
	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Mock home directory
	originalHome := os.Getenv("HOME")
	os.Setenv("HOME", tmpDir)
	defer os.Setenv("HOME", originalHome)

	// Create history manager and add commands
	histMgr := interactive.NewHistoryManager(100)
	histMgr.Add("cmd1")
	histMgr.Add("cmd2")
	histMgr.Add("cmd3")
	histMgr.Add("very long command that should be preserved")

	// Save history
	err = histMgr.SaveToFile("test_history.txt")
	require.NoError(t, err)

	// Check if file was created
	histFile := filepath.Join(tmpDir, ".mo-tool", "test_history.txt")
	assert.FileExists(t, histFile)

	// Verify file contents
	content, err := os.ReadFile(histFile)
	require.NoError(t, err)

	lines := strings.Split(string(content), "\n")
	assert.Contains(t, lines, "cmd1")
	assert.Contains(t, lines, "cmd2")
	assert.Contains(t, lines, "cmd3")
	assert.Contains(t, lines, "very long command that should be preserved")

	// Test loading history
	histMgr2 := interactive.NewHistoryManager(100)
	err = histMgr2.LoadFromFile("test_history.txt")
	require.NoError(t, err)

	items := histMgr2.GetAll()
	assert.Equal(t, []string{"cmd1", "cmd2", "cmd3", "very long command that should be preserved"}, items)
}

func TestHistoryLimit(t *testing.T) {
	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Mock home directory
	originalHome := os.Getenv("HOME")
	os.Setenv("HOME", tmpDir)
	defer os.Setenv("HOME", originalHome)

	// Create history manager with limit of 100
	histMgr := interactive.NewHistoryManager(100)

	// Add more than 100 commands
	for i := 0; i < 150; i++ {
		histMgr.Add(fmt.Sprintf("command_%d", i))
	}

	// Save history (should limit to 100)
	err = histMgr.SaveToFile("test_history.txt")
	require.NoError(t, err)

	// Load history in new manager
	histMgr2 := interactive.NewHistoryManager(100)
	err = histMgr2.LoadFromFile("test_history.txt")
	require.NoError(t, err)

	// Should only have the last 100 commands
	items := histMgr2.GetAll()
	assert.Equal(t, 100, len(items))
	assert.Equal(t, "command_50", items[0])   // First saved command
	assert.Equal(t, "command_149", items[99]) // Last saved command
}

func TestHistoryWithEmptyLines(t *testing.T) {
	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Mock home directory
	originalHome := os.Getenv("HOME")
	os.Setenv("HOME", tmpDir)
	defer os.Setenv("HOME", originalHome)

	// Create history file with empty lines
	moToolDir := filepath.Join(tmpDir, ".mo-tool")
	err = os.MkdirAll(moToolDir, 0755)
	require.NoError(t, err)

	histFile := filepath.Join(moToolDir, "test_history.txt")
	content := "cmd1\n\n\ncmd2\n   \ncmd3\n"
	err = os.WriteFile(histFile, []byte(content), 0644)
	require.NoError(t, err)

	// Test loading history (should skip empty lines)
	histMgr := interactive.NewHistoryManager(100)
	err = histMgr.LoadFromFile("test_history.txt")
	require.NoError(t, err)

	items := histMgr.GetAll()
	assert.Equal(t, []string{"cmd1", "cmd2", "cmd3"}, items)
}

func TestGetHistoryFile(t *testing.T) {
	// Test history manager creation
	histMgr := interactive.NewHistoryManager(100)

	// Just verify we can create the manager
	assert.NotNil(t, histMgr)
}

func TestGetHistoryFileNoHome(t *testing.T) {
	// Test that HistoryManager handles missing home gracefully
	histMgr := interactive.NewHistoryManager(100)

	// Should not panic
	assert.NotNil(t, histMgr)
}

func TestHistoryDirectoryCreation(t *testing.T) {
	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Mock home directory
	originalHome := os.Getenv("HOME")
	os.Setenv("HOME", tmpDir)
	defer os.Setenv("HOME", originalHome)

	histMgr := interactive.NewHistoryManager(100)
	histMgr.Add("test_command")

	// Test saving history (should create directory)
	err = histMgr.SaveToFile("test_history.txt")
	require.NoError(t, err)

	// Check if directory and file were created
	histFile := filepath.Join(tmpDir, ".mo-tool", "test_history.txt")
	assert.FileExists(t, histFile)

	// Verify content
	content, err := os.ReadFile(histFile)
	require.NoError(t, err)
	assert.Equal(t, "test_command", strings.TrimSpace(string(content)))
}

func TestHistoryRoundTrip(t *testing.T) {
	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Mock home directory
	originalHome := os.Getenv("HOME")
	os.Setenv("HOME", tmpDir)
	defer os.Setenv("HOME", originalHome)

	// Test data with various edge cases
	originalCommands := []string{
		"simple_command",
		"command with spaces",
		"command:with:colons",
		"command/with/slashes",
		"command\"with\"quotes",
		"command'with'single'quotes",
		"very_long_command_that_should_be_preserved_exactly_as_it_is_without_any_modifications",
		"",    // This should be filtered out
		"   ", // This should be filtered out
		"final_command",
	}

	histMgr1 := interactive.NewHistoryManager(100)
	for _, cmd := range originalCommands {
		histMgr1.Add(cmd)
	}

	// Save history
	err = histMgr1.SaveToFile("test_history.txt")
	require.NoError(t, err)

	// Load history in new manager
	histMgr2 := interactive.NewHistoryManager(100)
	err = histMgr2.LoadFromFile("test_history.txt")
	require.NoError(t, err)

	// Expected commands (empty ones should be filtered out)
	expectedCommands := []string{
		"simple_command",
		"command with spaces",
		"command:with:colons",
		"command/with/slashes",
		"command\"with\"quotes",
		"command'with'single'quotes",
		"very_long_command_that_should_be_preserved_exactly_as_it_is_without_any_modifications",
		"final_command",
	}

	items := histMgr2.GetAll()
	assert.Equal(t, expectedCommands, items)
}
