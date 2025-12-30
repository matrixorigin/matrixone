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

	m := &model{
		cmdHistory: []string{"cmd1", "cmd2", "cmd3", "very long command that should be preserved"},
	}

	// Test saving history
	m.saveHistory()

	// Check if file was created
	historyFile := filepath.Join(tmpDir, ".mo_object_history")
	assert.FileExists(t, historyFile)

	// Verify file contents
	content, err := os.ReadFile(historyFile)
	require.NoError(t, err)

	lines := strings.Split(string(content), "\n")
	assert.Contains(t, lines, "cmd1")
	assert.Contains(t, lines, "cmd2")
	assert.Contains(t, lines, "cmd3")
	assert.Contains(t, lines, "very long command that should be preserved")

	// Test loading history
	m2 := &model{}
	m2.loadHistory()

	assert.Equal(t, []string{"cmd1", "cmd2", "cmd3", "very long command that should be preserved"}, m2.cmdHistory)
	assert.Equal(t, 4, m2.historyIndex)
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

	// Create model with more than 100 commands
	var commands []string
	for i := 0; i < 150; i++ {
		commands = append(commands, fmt.Sprintf("command_%d", i))
	}

	m := &model{
		cmdHistory: commands,
	}

	// Test saving history (should limit to 100)
	m.saveHistory()

	// Test loading history
	m2 := &model{}
	m2.loadHistory()

	// Should only have the last 100 commands
	assert.Equal(t, 100, len(m2.cmdHistory))
	assert.Equal(t, "command_50", m2.cmdHistory[0])   // First saved command
	assert.Equal(t, "command_149", m2.cmdHistory[99]) // Last saved command
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
	historyFile := filepath.Join(tmpDir, ".mo_object_history")
	content := "cmd1\n\n\ncmd2\n   \ncmd3\n"
	err = os.WriteFile(historyFile, []byte(content), 0644)
	require.NoError(t, err)

	// Test loading history (should skip empty lines)
	m := &model{}
	m.loadHistory()

	assert.Equal(t, []string{"cmd1", "cmd2", "cmd3"}, m.cmdHistory)
	assert.Equal(t, 3, m.historyIndex)
}

func TestGetHistoryFile(t *testing.T) {
	// Test with valid home directory
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	originalHome := os.Getenv("HOME")
	os.Setenv("HOME", tmpDir)
	defer os.Setenv("HOME", originalHome)

	historyFile := getHistoryFile()
	expected := filepath.Join(tmpDir, ".mo_object_history")
	assert.Equal(t, expected, historyFile)
}

func TestGetHistoryFileNoHome(t *testing.T) {
	// Test with no HOME environment variable
	originalHome := os.Getenv("HOME")
	os.Unsetenv("HOME")
	defer os.Setenv("HOME", originalHome)

	historyFile := getHistoryFile()
	assert.Empty(t, historyFile)
}

func TestHistoryDirectoryCreation(t *testing.T) {
	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "mo_object_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create nested directory structure
	homeDir := filepath.Join(tmpDir, "user", "home")

	// Mock home directory (directory doesn't exist yet)
	originalHome := os.Getenv("HOME")
	os.Setenv("HOME", homeDir)
	defer os.Setenv("HOME", originalHome)

	m := &model{
		cmdHistory: []string{"test_command"},
	}

	// Test saving history (should create directory)
	m.saveHistory()

	// Check if directory and file were created
	historyFile := filepath.Join(homeDir, ".mo_object_history")
	assert.FileExists(t, historyFile)

	// Verify content
	content, err := os.ReadFile(historyFile)
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

	m1 := &model{
		cmdHistory: originalCommands,
	}

	// Save history
	m1.saveHistory()

	// Load history in new model
	m2 := &model{}
	m2.loadHistory()

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

	assert.Equal(t, expectedCommands, m2.cmdHistory)
	assert.Equal(t, len(expectedCommands), m2.historyIndex)
}
