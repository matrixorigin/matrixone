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
	"strings"
	"testing"
)

func TestVisibleLen(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{"plain text", "hello", 5},
		{"with ANSI color", "\033[41m\033[97mhello\033[0m", 5},
		{"empty", "", 0},
		{"only ANSI", "\033[41m\033[0m", 0},
		{"mixed", "ab\033[41mcd\033[0mef", 6},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := visibleLen(tt.input)
			if result != tt.expected {
				t.Errorf("visibleLen(%q) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestPadRight(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		width    int
		expected string
	}{
		{"plain text", "hello", 10, "hello     "},
		{"with ANSI", "\033[41mhello\033[0m", 10, "\033[41mhello\033[0m     "},
		{"exact width", "hello", 5, "hello"},
		{"longer than width", "hello world", 5, "hello world"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := padRight(tt.input, tt.width)
			if result != tt.expected {
				t.Errorf("padRight(%q, %d) = %q, want %q", tt.input, tt.width, result, tt.expected)
			}
			// Verify visible length
			if visibleLen(result) < tt.width && len(tt.input) <= tt.width {
				t.Errorf("padRight result has visible length %d, want at least %d", visibleLen(result), tt.width)
			}
		})
	}
}

func TestHighlightSearchMatch(t *testing.T) {
	m := model{
		searchTerm: "test",
		hasMatch:   true,
		currentMatch: SearchMatch{
			Row: 0,
			Col: 0,
		},
	}

	// Test current match highlighting
	result := m.highlightSearchMatch("test_value", 0, 0)
	if !strings.Contains(result, "\033[41m") {
		t.Error("Expected ANSI color code in highlighted text")
	}
	if !strings.Contains(result, "test_value") {
		t.Error("Expected original text in result")
	}

	// Test non-match
	result2 := m.highlightSearchMatch("other_value", 1, 1)
	if strings.Contains(result2, "\033[41m") {
		t.Error("Expected no ANSI color code for non-match")
	}
	if result2 != "other_value" {
		t.Errorf("Expected unchanged text, got %q", result2)
	}
}

func TestMatchPattern(t *testing.T) {
	tests := []struct {
		name       string
		searchTerm string
		useRegex   bool
		cell       string
		expected   bool
	}{
		{"plain match", "test", false, "this is a test", true},
		{"plain no match", "test", false, "this is a demo", false},
		{"case insensitive", "TEST", false, "this is a test", true},
		{"hex decode match", "test", false, "74657374", true}, // "test" in hex
		{"partial match", "123", false, "abc123def", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := model{
				searchTerm: tt.searchTerm,
				useRegex:   tt.useRegex,
			}
			result := m.matchPattern(tt.cell)
			if result != tt.expected {
				t.Errorf("matchPattern(%q) = %v, want %v", tt.cell, result, tt.expected)
			}
		})
	}
}
