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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCommandBasic(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		expectNil   bool
	}{
		{"empty input", "", false, true},
		{"whitespace", "   ", false, true},
		{"quit", "q", false, false},
		{"colon quit", ":quit", false, false},
		{"vertical", ":vertical", false, false},
		{"unknown command", ":unknown", true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := ParseCommand(tt.input)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.expectNil {
				assert.Nil(t, cmd)
			} else {
				assert.NotNil(t, cmd)
			}
		})
	}
}

func TestParseSetCommandBasic(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expectError bool
	}{
		{"valid width", []string{"width", "100"}, false},
		{"width unlimited", []string{"width", "unlimited"}, false},
		{"insufficient args", []string{"width"}, true},
		{"unknown option", []string{"unknown", "value"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseSetCommand(tt.args)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLongestCommonPrefixBasic(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected string
	}{
		{"empty", []string{}, ""},
		{"single", []string{"test"}, "test"},
		{"common prefix", []string{"test1", "test2"}, "test"},
		{"no common", []string{"abc", "def"}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := longestCommonPrefix(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestQuitCommandBasic(t *testing.T) {
	cmd := &QuitCommand{}
	output, quit, err := cmd.Execute(nil)

	assert.NoError(t, err)
	assert.True(t, quit)
	assert.Empty(t, output)
}

func TestHelpCommandBasic(t *testing.T) {
	cmd := &HelpCommand{}
	output, quit, err := cmd.Execute(nil)

	assert.NoError(t, err)
	assert.False(t, quit)
	assert.Contains(t, output, "Browse Mode")
}

func TestCommandAliases(t *testing.T) {
	aliases := map[string]bool{
		":v": true, // vertical
		":t": true, // table
		":q": true, // quit
		":h": true, // help
	}

	for input := range aliases {
		t.Run(input, func(t *testing.T) {
			cmd, err := ParseCommand(input)
			assert.NoError(t, err)
			assert.NotNil(t, cmd)
		})
	}
}
