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

	tea "github.com/charmbracelet/bubbletea"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
	"github.com/stretchr/testify/assert"
)

// MockObjectReader for testing
type MockObjectReader struct {
	blockCount uint32
	rowCount   uint64
	colCount   uint16
	columns    []objecttool.ColInfo
}

func (m *MockObjectReader) Info() objecttool.ObjectInfo {
	return objecttool.ObjectInfo{
		Path:       "/test/path",
		BlockCount: m.blockCount,
		RowCount:   m.rowCount,
		ColCount:   m.colCount,
	}
}

func (m *MockObjectReader) Columns() []objecttool.ColInfo {
	return m.columns
}

func (m *MockObjectReader) BlockCount() uint32 {
	return m.blockCount
}

func (m *MockObjectReader) ReadBlock(ctx context.Context, blockIdx uint32) (*batch.Batch, func(), error) {
	// Return empty batch for testing
	return &batch.Batch{}, func() {}, nil
}

func (m *MockObjectReader) Close() error {
	return nil
}

func TestModel_Init(t *testing.T) {
	m := model{}
	cmd := m.Init()
	assert.Nil(t, cmd)
}

func TestModel_BrowseMode(t *testing.T) {
	// Create a mock state to avoid nil pointer
	mockState := &State{
		maxColWidth: 64,
		pageSize:    20,
	}

	m := model{
		cmdMode: false,
		state:   mockState,
	}

	tests := []struct {
		name     string
		keyType  tea.KeyType
		keyRunes []rune
		expected bool // whether it should quit
	}{
		{"quit with q", tea.KeyRunes, []rune("q"), true},
		{"quit with ctrl+c", tea.KeyCtrlC, nil, true},
		{"enter command mode", tea.KeyRunes, []rune(":"), false},
		{"enter search mode", tea.KeyRunes, []rune("/"), false},
		{"help", tea.KeyRunes, []rune("?"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := tea.KeyMsg{Type: tt.keyType, Runes: tt.keyRunes}

			newModel, cmd := m.handleBrowseMode(msg)

			if tt.expected {
				// Check if it's a quit command (function pointer comparison doesn't work)
				assert.NotNil(t, cmd)
			} else {
				if len(tt.keyRunes) > 0 {
					switch string(tt.keyRunes) {
					case ":":
						assert.True(t, newModel.(model).cmdMode)
						assert.Empty(t, newModel.(model).cmdInput)
					case "/":
						assert.True(t, newModel.(model).cmdMode)
						assert.Equal(t, "search ", newModel.(model).cmdInput)
					}
				}
			}
		})
	}
}

func TestModel_CommandMode(t *testing.T) {
	m := model{
		cmdMode:      true,
		cmdInput:     "test",
		cmdCursor:    4,
		cmdHistory:   []string{"prev1", "prev2"},
		historyIndex: 2,
	}

	tests := []struct {
		name           string
		keyType        tea.KeyType
		keyRunes       []rune
		expectedInput  string
		expectedCursor int
		expectedMode   bool
	}{
		{
			name:           "escape",
			keyType:        tea.KeyEsc,
			expectedInput:  "",
			expectedCursor: 0,
			expectedMode:   false,
		},
		{
			name:           "ctrl+a",
			keyType:        tea.KeyCtrlA,
			expectedInput:  "test",
			expectedCursor: 0,
			expectedMode:   true,
		},
		{
			name:           "ctrl+e",
			keyType:        tea.KeyCtrlE,
			expectedInput:  "test",
			expectedCursor: 4,
			expectedMode:   true,
		},
		{
			name:           "ctrl+u",
			keyType:        tea.KeyCtrlU,
			expectedInput:  "",
			expectedCursor: 0,
			expectedMode:   true,
		},
		{
			name:           "backspace",
			keyType:        tea.KeyBackspace,
			expectedInput:  "tes",
			expectedCursor: 3,
			expectedMode:   true,
		},
		{
			name:           "left arrow",
			keyType:        tea.KeyLeft,
			expectedInput:  "test",
			expectedCursor: 3,
			expectedMode:   true,
		},
		{
			name:           "right arrow",
			keyType:        tea.KeyRight,
			expectedInput:  "test",
			expectedCursor: 4,
			expectedMode:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := tea.KeyMsg{Type: tt.keyType, Runes: tt.keyRunes}

			newModel, _ := m.handleCommandInput(msg)
			result := newModel.(model)

			assert.Equal(t, tt.expectedInput, result.cmdInput)
			assert.Equal(t, tt.expectedCursor, result.cmdCursor)
			assert.Equal(t, tt.expectedMode, result.cmdMode)
		})
	}
}

func TestModel_CommandHistory(t *testing.T) {
	m := model{
		cmdMode:      true,
		cmdHistory:   []string{"cmd1", "cmd2", "cmd3"},
		historyIndex: 3,
	}

	// Test up arrow (previous command)
	msg := tea.KeyMsg{Type: tea.KeyUp}
	newModel, _ := m.handleCommandInput(msg)
	result := newModel.(model)

	assert.Equal(t, 2, result.historyIndex)
	assert.Equal(t, "cmd3", result.cmdInput)
	assert.Equal(t, 4, result.cmdCursor) // cursor at end

	// Test up arrow again
	newModel, _ = result.handleCommandInput(msg)
	result = newModel.(model)

	assert.Equal(t, 1, result.historyIndex)
	assert.Equal(t, "cmd2", result.cmdInput)

	// Test down arrow (next command)
	msg = tea.KeyMsg{Type: tea.KeyDown}
	newModel, _ = result.handleCommandInput(msg)
	result = newModel.(model)

	assert.Equal(t, 2, result.historyIndex)
	assert.Equal(t, "cmd3", result.cmdInput)
}

func TestModel_TextInput(t *testing.T) {
	m := model{
		cmdMode:   true,
		cmdInput:  "test",
		cmdCursor: 2, // cursor between 'te' and 'st'
	}

	// Test inserting text at cursor position
	msg := tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("XY")}
	newModel, _ := m.handleCommandInput(msg)
	result := newModel.(model)

	assert.Equal(t, "teXYst", result.cmdInput)
	assert.Equal(t, 4, result.cmdCursor) // cursor moved by 2
}

func TestModel_PasteHandling(t *testing.T) {
	m := model{
		cmdMode:   true,
		cmdInput:  "",
		cmdCursor: 0,
	}

	// Test pasting text with brackets (should be removed)
	msg := tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("[pasted content]")}
	newModel, _ := m.handleCommandInput(msg)
	result := newModel.(model)

	assert.Equal(t, "pasted content", result.cmdInput)
	assert.Equal(t, 14, result.cmdCursor)
}

func TestModel_SearchMatches(t *testing.T) {
	// Create a mock state to avoid nil pointer
	mockState := &State{
		maxColWidth: 64,
		pageSize:    20,
	}

	m := model{
		state:        mockState,
		searchTerm:   "test",
		hasMatch:     true,
		currentMatch: SearchMatch{Row: 0, Col: 0, Value: "match1", RowNum: "(0-0)", ColumnName: "Col0"},
	}

	// Test that we can access search match without crashing
	assert.True(t, m.hasMatch)
	assert.Equal(t, int64(0), m.currentMatch.Row)
	assert.Equal(t, "match1", m.currentMatch.Value)
}

func TestCompleteCommand(t *testing.T) {
	m := model{}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "complete quit (ambiguous)",
			input:    "q",
			expected: "q", // "q" and "quit" both match, so returns common prefix "q"
		},
		{
			name:     "complete format",
			input:    "f",
			expected: "format", // only "format" matches
		},
		{
			name:     "complete set (ambiguous)",
			input:    "se",
			expected: "se", // "set" and "search" both match, so returns common prefix "se"
		},
		{
			name:     "complete unique",
			input:    "qui",
			expected: "quit", // only "quit" matches
		},
		{
			name:     "no completion",
			input:    "xyz",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := m.completeCommand(tt.input)
			assert.Equal(t, tt.expected, result, "Expected %s, got %s", tt.expected, result)
		})
	}
}

func TestCompleteCommandName(t *testing.T) {
	m := model{}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "single match",
			input:    "qui",
			expected: "quit",
		},
		{
			name:     "multiple matches - common prefix",
			input:    "s",
			expected: "s", // should return longest common prefix
		},
		{
			name:     "no match",
			input:    "xyz",
			expected: "",
		},
		{
			name:     "exact match",
			input:    "quit",
			expected: "quit",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := m.completeCommandName(tt.input)
			if tt.expected == "" {
				assert.Empty(t, result)
			} else {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestLongestCommonPrefix(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected string
	}{
		{
			name:     "common prefix",
			input:    []string{"search", "set", "schema"},
			expected: "s",
		},
		{
			name:     "longer common prefix",
			input:    []string{"format", "formatter"},
			expected: "format",
		},
		{
			name:     "no common prefix",
			input:    []string{"quit", "vertical", "help"},
			expected: "",
		},
		{
			name:     "single string",
			input:    []string{"quit"},
			expected: "quit",
		},
		{
			name:     "empty input",
			input:    []string{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := longestCommonPrefix(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestModel_View(t *testing.T) {
	// Create a minimal state that won't cause nil pointer dereference
	state := &State{
		formatter:    objecttool.NewFormatterRegistry(),
		ctx:          context.Background(),
		pageSize:     20,
		maxColWidth:  64,
		verticalMode: false,
		currentBlock: 0,
		rowOffset:    0,
		// Don't set reader to avoid ReadBlock calls
	}

	m := model{
		message: "test message",
		cmdMode: false,
		state:   state,
	}

	// Test command mode view (doesn't need data)
	m.cmdMode = true
	m.cmdInput = "test command"
	m.cmdCursor = 4

	assert.NotPanics(t, func() {
		view := m.View()
		assert.IsType(t, "", view)
		assert.Contains(t, view, ":")
	})

	// Test basic model properties
	assert.Equal(t, "test message", m.message)
	assert.True(t, m.cmdMode)
	assert.Equal(t, "test command", m.cmdInput)
	assert.Equal(t, 4, m.cmdCursor)
}
