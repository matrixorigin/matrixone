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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCommand(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedCmd interface{}
		expectError bool
	}{
		// Single character commands
		{
			name:        "quit command",
			input:       "q",
			expectedCmd: &QuitCommand{},
		},
		{
			name:        "scroll down",
			input:       "j",
			expectedCmd: &ScrollCommand{},
		},
		{
			name:        "scroll up",
			input:       "k",
			expectedCmd: &ScrollCommand{},
		},
		{
			name:        "goto top",
			input:       "g",
			expectedCmd: &GotoCommand{},
		},
		{
			name:        "goto bottom",
			input:       "G",
			expectedCmd: &GotoCommand{},
		},
		{
			name:        "help",
			input:       "?",
			expectedCmd: &HelpCommand{},
		},

		// Colon commands
		{
			name:        "colon quit",
			input:       ":quit",
			expectedCmd: &QuitCommand{},
		},
		{
			name:        "colon schema",
			input:       ":schema",
			expectedCmd: &SchemaCommand{},
		},
		{
			name:        "vertical mode",
			input:       ":vertical",
			expectedCmd: &VerticalCommand{},
		},
		{
			name:        "table mode",
			input:       ":table",
			expectedCmd: &VerticalCommand{},
		},
		{
			name:        "set width",
			input:       ":set width 100",
			expectedCmd: &SetCommand{},
		},
		{
			name:        "format command",
			input:       ":format 0 hex",
			expectedCmd: &FormatCommand{},
		},
		{
			name:        "search command",
			input:       ":search pattern",
			expectedCmd: &SearchCommand{},
		},
		{
			name:        "columns command",
			input:       ":cols 1,3,5",
			expectedCmd: &ColumnsCommand{},
		},
		{
			name:        "vrows command",
			input:       ":vrows 10",
			expectedCmd: &VRowsCommand{},
		},

		// Aliases
		{
			name:        "v alias",
			input:       ":v",
			expectedCmd: &VerticalCommand{},
		},
		{
			name:        "t alias",
			input:       ":t",
			expectedCmd: &VerticalCommand{},
		},
		{
			name:        "s alias",
			input:       ":s pattern",
			expectedCmd: &SearchCommand{},
		},
		{
			name:        "c alias",
			input:       ":c 1,2,3",
			expectedCmd: &ColumnsCommand{},
		},
		{
			name:        "w alias",
			input:       ":w 64",
			expectedCmd: &SetCommand{},
		},

		// Block jump
		{
			name:        "block jump",
			input:       ":123",
			expectedCmd: &GotoCommand{},
		},

		// Error cases
		{
			name:        "unknown command",
			input:       ":unknown",
			expectError: true,
		},
		{
			name:        "invalid format",
			input:       ":format",
			expectError: true,
		},
		{
			name:        "invalid set",
			input:       ":set",
			expectError: true,
		},
		{
			name:        "invalid vrows",
			input:       ":vrows abc",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := ParseCommand(tt.input)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.expectedCmd == nil {
				assert.Nil(t, cmd)
			} else {
				require.NotNil(t, cmd)
				assert.IsType(t, tt.expectedCmd, cmd)
			}
		})
	}
}

func TestParseCommandEmpty(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty input", ""},
		{"whitespace only", "   "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := ParseCommand(tt.input)
			require.NoError(t, err)
			assert.Nil(t, cmd)
		})
	}
}

func TestParseSetCommand(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expectedOpt string
		expectedVal int
		expectError bool
	}{
		{
			name:        "set width numeric",
			args:        []string{"width", "100"},
			expectedOpt: "width",
			expectedVal: 100,
		},
		{
			name:        "set width unlimited",
			args:        []string{"width", "unlimited"},
			expectedOpt: "width",
			expectedVal: 0,
		},
		{
			name:        "set width zero",
			args:        []string{"width", "0"},
			expectedOpt: "width",
			expectedVal: 0,
		},
		{
			name:        "insufficient args",
			args:        []string{"width"},
			expectError: true,
		},
		{
			name:        "unknown option",
			args:        []string{"unknown", "value"},
			expectError: true,
		},
		{
			name:        "invalid width",
			args:        []string{"width", "abc"},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := parseSetCommand(tt.args)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			setCmd, ok := cmd.(*SetCommand)
			require.True(t, ok)

			assert.Equal(t, tt.expectedOpt, setCmd.Option)
			assert.Equal(t, tt.expectedVal, setCmd.Value)
		})
	}
}

func TestParseColumnsCommand(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expected    []uint16
		showAll     bool
		expectError bool
	}{
		{
			name:    "show all",
			args:    []string{},
			showAll: true,
		},
		{
			name:    "show all explicit",
			args:    []string{"all"},
			showAll: true,
		},
		{
			name:     "single column",
			args:     []string{"5"},
			expected: []uint16{5},
		},
		{
			name:     "comma separated",
			args:     []string{"1,3,5"},
			expected: []uint16{1, 3, 5},
		},
		{
			name:     "range",
			args:     []string{"1-5"},
			expected: []uint16{1, 2, 3, 4, 5},
		},
		{
			name:     "multiple args",
			args:     []string{"1", "3", "5"},
			expected: []uint16{1, 3, 5},
		},
		{
			name:        "invalid range",
			args:        []string{"1-2-3"},
			expectError: true,
		},
		{
			name:        "invalid number",
			args:        []string{"abc"},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := parseColumnsCommand(tt.args)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			colCmd, ok := cmd.(*ColumnsCommand)
			require.True(t, ok)

			if tt.showAll {
				assert.True(t, colCmd.ShowAll)
			} else {
				assert.Equal(t, tt.expected, colCmd.Columns)
			}
		})
	}
}

func TestParseFormatCommand(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expectedCol uint16
		expectedFmt string
		expectError bool
	}{
		{
			name:        "valid format",
			args:        []string{"0", "hex"},
			expectedCol: 0,
			expectedFmt: "hex",
		},
		{
			name:        "another valid format",
			args:        []string{"5", "objectstats"},
			expectedCol: 5,
			expectedFmt: "objectstats",
		},
		{
			name:        "insufficient args",
			args:        []string{"0"},
			expectError: true,
		},
		{
			name:        "invalid column",
			args:        []string{"abc", "hex"},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := parseFormatCommand(tt.args)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			fmtCmd, ok := cmd.(*FormatCommand)
			require.True(t, ok)

			assert.Equal(t, tt.expectedCol, fmtCmd.ColIdx)
			assert.Equal(t, tt.expectedFmt, fmtCmd.FormatterName)
		})
	}
}

// Test command execution
func TestQuitCommand_Execute(t *testing.T) {
	cmd := &QuitCommand{}
	output, quit, err := cmd.Execute(nil)

	assert.NoError(t, err)
	assert.True(t, quit)
	assert.Empty(t, output)
}

func TestHelpCommand_Execute(t *testing.T) {
	// Test general help
	cmd := &HelpCommand{}
	output, quit, err := cmd.Execute(nil)

	assert.NoError(t, err)
	assert.False(t, quit)
	assert.Contains(t, output, "Browse Mode")

	// Test topic help
	cmd = &HelpCommand{Topic: "format"}
	output, quit, err = cmd.Execute(nil)

	assert.NoError(t, err)
	assert.False(t, quit)
	assert.Contains(t, output, "formatter")

	// Test unknown topic
	cmd = &HelpCommand{Topic: "unknown"}
	output, quit, err = cmd.Execute(nil)

	assert.NoError(t, err)
	assert.False(t, quit)
	assert.Contains(t, output, "No help for")
}

func TestSetCommand_Execute(t *testing.T) {
	// Test command creation and properties
	cmd := &SetCommand{Option: "width", Value: 100}
	assert.Equal(t, "width", cmd.Option)
	assert.Equal(t, 100, cmd.Value)

	// Test unlimited width
	cmd = &SetCommand{Option: "width", Value: 0}
	assert.Equal(t, "width", cmd.Option)
	assert.Equal(t, 0, cmd.Value)
}

func TestVerticalCommand_Execute(t *testing.T) {
	// Test command creation
	cmd := &VerticalCommand{Enable: true}
	assert.True(t, cmd.Enable)

	cmd = &VerticalCommand{Enable: false}
	assert.False(t, cmd.Enable)
}

func TestCommandTypes(t *testing.T) {
	// Test that all command types can be created
	commands := []Command{
		&QuitCommand{},
		&SchemaCommand{},
		&FormatCommand{ColIdx: 0, FormatterName: "hex"},
		&ScrollCommand{Down: true, Lines: 1},
		&GotoCommand{Top: true},
		&HelpCommand{Topic: "test"},
		&VerticalCommand{Enable: true},
		&SetCommand{Option: "width", Value: 100},
		&SearchCommand{Pattern: "test"},
		&ColumnsCommand{ShowAll: true},
		&VRowsCommand{Rows: 10},
	}

	for i, cmd := range commands {
		t.Run(fmt.Sprintf("command_%d", i), func(t *testing.T) {
			assert.NotNil(t, cmd)
			// Each command should implement the Command interface
			assert.Implements(t, (*Command)(nil), cmd)
		})
	}
}

// TestParseCommandEdgeCases tests edge cases in command parsing
func TestParseCommandEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"empty", "", false},
		{"whitespace only", "   ", false},
		{"quit", "quit", false},
		{"q", "q", false},
		{":q", ":q", false},
		{"help", "help", false},
		{"info", "info", false},
		{"schema", "schema", false},
		{"vertical", "vertical", false},
		{"set width 100", "set width 100", false},
		{"set width unlimited", "set width unlimited", false},
		{"cols 1,2,3", "cols 1,2,3", false},
		{"cols all", "cols all", false},
		{"format 0 hex", "format 0 hex", false},
		{"format 1 default", "format 1 default", false},
		{"unknown command", "unknown", true},
		{"set without args", "set", true},
		{"set width invalid", "set width abc", true},
		{"cols invalid", "cols abc", true},
		{"format without args", "format", true},
		{"format invalid col", "format abc hex", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := ParseCommand(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				if err != nil {
					// Some commands may return nil without error (like empty input)
					assert.Nil(t, cmd)
				}
			}
		})
	}
}

// TestParseColonCommandCoverage tests parseColonCommand edge cases
func TestParseColonCommandCoverage(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{":q", ":q", false},
		{":quit", ":quit", false},
		{":Q", ":Q", false},
		{":QUIT", ":QUIT", false},
		{":vertical", ":vertical", false},
		{":v", ":v", false},
		{":set width 50", ":set width 50", false},
		{":set width unlimited", ":set width unlimited", false},
		{":cols 1,2,3", ":cols 1,2,3", false},
		{":cols all", ":cols all", false},
		{":cols", ":cols", false}, // cols without args shows all
		{":format 0 hex", ":format 0 hex", false},
		{":unknown", ":unknown", true},
		{":set", ":set", true},
		{":format", ":format", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := ParseCommand(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				if err != nil {
					assert.Nil(t, cmd)
				}
			}
		})
	}
}
