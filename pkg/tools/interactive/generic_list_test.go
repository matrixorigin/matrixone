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

	tea "github.com/charmbracelet/bubbletea"
)

// TestPasteHandling tests that paste input is handled correctly
func TestPasteHandling(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Normal text",
			input:    "hello",
			expected: "hello",
		},
		{
			name:     "Bracketed paste",
			input:    "[hello world]",
			expected: "hello world",
		},
		{
			name:     "Text with control chars",
			input:    "hello\x01\x02world",
			expected: "helloworld",
		},
		{
			name:     "UUID paste",
			input:    "[019b70f5-0f0d-763a-b699-8b6176d91090]",
			expected: "019b70f5-0f0d-763a-b699-8b6176d91090",
		},
		{
			name:     "Mixed printable and non-printable",
			input:    "test\x1bvalue",
			expected: "testvalue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a simple page
			config := PageConfig{
				Title:        "Test",
				Headers:      []string{"Col1"},
				EnableSearch: true,
			}
			provider := &testProvider{rows: [][]string{{"val1"}}}
			handler := &testHandler{}
			page := NewGenericPage(config, provider, handler)

			// Enter search mode
			page.inputMode = "search"
			page.inputBuffer = ""
			page.inputCursor = 0

			// Simulate paste input
			msg := tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(tt.input)}
			page, _ = page.handleInputMode(msg.String())

			if page.inputBuffer != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, page.inputBuffer)
			}
		})
	}
}

// TestSearchHistory tests search history navigation
func TestSearchHistory(t *testing.T) {
	list := NewGenericList(ListOptions{
		Headers:      []string{"Col1"},
		EnableSearch: true,
	})

	// Add some search history
	list.Search("first", func(row []string, query string) bool {
		return strings.Contains(row[0], query)
	})
	list.Search("second", func(row []string, query string) bool {
		return strings.Contains(row[0], query)
	})
	list.Search("third", func(row []string, query string) bool {
		return strings.Contains(row[0], query)
	})

	history := list.GetSearchHistory()
	// Check that our 3 entries are at the end (history may have previous entries from file)
	if len(history) < 3 {
		t.Errorf("Expected at least 3 history entries, got %d", len(history))
	}

	// Check last 3 entries
	last3 := history[len(history)-3:]
	if last3[0] != "first" || last3[1] != "second" || last3[2] != "third" {
		t.Errorf("Last 3 history entries incorrect: %v", last3)
	}

	// Test last search
	if list.GetLastSearch() != "third" {
		t.Errorf("Expected last search to be 'third', got %q", list.GetLastSearch())
	}
}

// TestHorizontalScrollMethods tests horizontal scroll methods
func TestHorizontalScrollMethods(t *testing.T) {
	list := NewGenericList(ListOptions{
		Headers: []string{"Col0", "Col1", "Col2", "Col3", "Col4"},
	})

	// Initial offset should be 0
	if list.GetHScroll() != 0 {
		t.Errorf("Expected initial offset 0, got %d", list.GetHScroll())
	}

	// Scroll right
	list.ScrollRight()
	if list.GetHScroll() != 1 {
		t.Errorf("Expected offset 1 after ScrollRight, got %d", list.GetHScroll())
	}

	// Scroll right again
	list.ScrollRight()
	if list.GetHScroll() != 2 {
		t.Errorf("Expected offset 2, got %d", list.GetHScroll())
	}

	// Scroll left
	list.ScrollLeft()
	if list.GetHScroll() != 1 {
		t.Errorf("Expected offset 1 after ScrollLeft, got %d", list.GetHScroll())
	}

	// Scroll left beyond 0
	list.ScrollLeft()
	list.ScrollLeft()
	if list.GetHScroll() != 0 {
		t.Errorf("Expected offset 0 (clamped), got %d", list.GetHScroll())
	}

	// Scroll right beyond max
	for i := 0; i < 10; i++ {
		list.ScrollRight()
	}
	// Should be clamped to len(headers)-1 = 4
	if list.GetHScroll() > 4 {
		t.Errorf("Expected offset <= 4, got %d", list.GetHScroll())
	}
}

// TestCustomRowNumsWithList tests that GenericList handles custom row numbers
func TestCustomRowNumsWithList(t *testing.T) {
	list := NewGenericList(ListOptions{
		Headers:       []string{"Col1"},
		ShowRowNumber: true,
	})

	rows := [][]string{{"val1"}, {"val2"}}
	rowNums := []string{"(0-18)", "(0-19)"}
	list.SetDataWithRowNums(rows, rowNums)

	output := list.Render()

	if !strings.Contains(output, "(0-18)") {
		t.Error("Expected to find custom row number (0-18)")
	}
	if !strings.Contains(output, "(0-19)") {
		t.Error("Expected to find custom row number (0-19)")
	}
}

// Test helpers
type testProvider struct {
	rows [][]string
}

func (p *testProvider) GetRows() [][]string {
	return p.rows
}

func (p *testProvider) GetRowNums() []string {
	return nil
}

func (p *testProvider) GetOverview() string {
	return "Test Overview"
}

type testHandler struct{}

func (h *testHandler) OnSelect(rowIdx int) tea.Cmd {
	return nil
}

func (h *testHandler) OnBack() tea.Cmd {
	return nil
}

func (h *testHandler) OnCustomKey(key string) tea.Cmd {
	return nil
}

func (h *testHandler) MatchRow(row []string, query string) bool {
	for _, cell := range row {
		if strings.Contains(strings.ToLower(cell), strings.ToLower(query)) {
			return true
		}
	}
	return false
}

func (h *testHandler) FilterRow(row []string, filter string) bool {
	return h.MatchRow(row, filter)
}

// TestSearchHighlight tests that search matches are highlighted
func TestSearchHighlight(t *testing.T) {
	list := NewGenericList(ListOptions{
		Headers:       []string{"Name", "Value"},
		ShowRowNumber: true,
		EnableCursor:  true,
		EnableSearch:  true,
		PageSize:      10,
	})

	rows := [][]string{
		{"apple", "100"},
		{"banana", "200"},
		{"apple pie", "300"},
		{"cherry", "400"},
	}
	list.SetData(rows)

	// Search for "apple"
	list.Search("apple", func(row []string, query string) bool {
		for _, cell := range row {
			if strings.Contains(strings.ToLower(cell), strings.ToLower(query)) {
				return true
			}
		}
		return false
	})

	// Should have 2 matches (row 0 and row 2)
	if list.MatchCount() != 2 {
		t.Errorf("Expected 2 matches, got %d", list.MatchCount())
	}

	// Move to first match
	list.NextMatch()

	// Render and check for highlight
	output := list.Render()

	// Should contain ANSI color codes for highlight
	if !strings.Contains(output, "\033[43m") {
		t.Error("Expected yellow highlight ANSI code in output")
	}
	if !strings.Contains(output, "\033[0m") {
		t.Error("Expected reset ANSI code in output")
	}

	// Should contain the matched data
	if !strings.Contains(output, "apple") {
		t.Error("Should contain matched text 'apple'")
	}
}

// TestSearchNextPrevMatch tests navigation between search matches
func TestSearchNextPrevMatch(t *testing.T) {
	list := NewGenericList(ListOptions{
		Headers:      []string{"Col1"},
		EnableCursor: true,
		EnableSearch: true,
		PageSize:     10,
	})

	rows := [][]string{
		{"yes"},
		{"no"},
		{"yes"},
		{"no"},
		{"yes"},
	}
	list.SetData(rows)

	// Search for exact "yes"
	list.Search("yes", func(row []string, query string) bool {
		return row[0] == query
	})

	// Should have 3 matches (rows 0, 2, 4)
	if list.MatchCount() != 3 {
		t.Errorf("Expected 3 matches, got %d", list.MatchCount())
	}

	// Initial cursor at 0 (which is a match)
	if list.GetCursor() != 0 {
		t.Errorf("Expected cursor at 0, got %d", list.GetCursor())
	}

	// NextMatch should go to row 2 (next match after 0)
	list.NextMatch()
	if list.GetCursor() != 2 {
		t.Errorf("Expected cursor at 2 after NextMatch, got %d", list.GetCursor())
	}

	// NextMatch should go to row 4
	list.NextMatch()
	if list.GetCursor() != 4 {
		t.Errorf("Expected cursor at 4 after NextMatch, got %d", list.GetCursor())
	}

	// NextMatch should wrap to row 0
	list.NextMatch()
	if list.GetCursor() != 0 {
		t.Errorf("Expected cursor at 0 after wrap, got %d", list.GetCursor())
	}

	// PrevMatch should go to row 4
	list.PrevMatch()
	if list.GetCursor() != 4 {
		t.Errorf("Expected cursor at 4 after PrevMatch, got %d", list.GetCursor())
	}
}

// TestCurrentMatchHighlightDecorator tests the highlight decorator
func TestCurrentMatchHighlightDecorator(t *testing.T) {
	decorator := CurrentMatchHighlightDecorator(2)

	// Row 2 should be highlighted
	prefix, suffix := decorator(2, false)
	if prefix != "\033[43m" {
		t.Errorf("Expected yellow highlight prefix, got %q", prefix)
	}
	if suffix != "\033[0m" {
		t.Errorf("Expected reset suffix, got %q", suffix)
	}

	// Other rows should not be highlighted
	prefix, suffix = decorator(0, false)
	if prefix != "" || suffix != "" {
		t.Error("Non-match row should not have highlight")
	}

	prefix, suffix = decorator(1, true) // Even if selected
	if prefix != "" || suffix != "" {
		t.Error("Non-match row should not have highlight even if selected")
	}
}

// TestInputEditingShortcuts tests ctrl+u, ctrl+k, ctrl+w shortcuts
func TestInputEditingShortcuts(t *testing.T) {
	config := PageConfig{
		Title:        "Test",
		Headers:      []string{"Col1"},
		EnableSearch: true,
	}
	provider := &testProvider{rows: [][]string{{"val1"}}}
	handler := &testHandler{}
	page := NewGenericPage(config, provider, handler)

	// Enter search mode with some text
	page.inputMode = "search"
	page.inputBuffer = "hello world test"
	page.inputCursor = 11 // After "world"

	// Test ctrl+w - delete word before cursor
	page, _ = page.handleInputMode("ctrl+w")
	if page.inputBuffer != "hello  test" {
		t.Errorf("ctrl+w: expected 'hello  test', got %q", page.inputBuffer)
	}
	if page.inputCursor != 6 {
		t.Errorf("ctrl+w: expected cursor at 6, got %d", page.inputCursor)
	}

	// Reset
	page.inputBuffer = "hello world"
	page.inputCursor = 11 // At end

	// Test ctrl+u - delete to beginning
	page, _ = page.handleInputMode("ctrl+u")
	if page.inputBuffer != "" {
		t.Errorf("ctrl+u: expected empty, got %q", page.inputBuffer)
	}
	if page.inputCursor != 0 {
		t.Errorf("ctrl+u: expected cursor at 0, got %d", page.inputCursor)
	}

	// Reset
	page.inputBuffer = "hello world"
	page.inputCursor = 5 // After "hello"

	// Test ctrl+k - delete to end
	page, _ = page.handleInputMode("ctrl+k")
	if page.inputBuffer != "hello" {
		t.Errorf("ctrl+k: expected 'hello', got %q", page.inputBuffer)
	}
	if page.inputCursor != 5 {
		t.Errorf("ctrl+k: expected cursor at 5, got %d", page.inputCursor)
	}

	// Test ctrl+a - move to beginning
	page.inputBuffer = "hello"
	page.inputCursor = 5
	page, _ = page.handleInputMode("ctrl+a")
	if page.inputCursor != 0 {
		t.Errorf("ctrl+a: expected cursor at 0, got %d", page.inputCursor)
	}

	// Test ctrl+e - move to end
	page, _ = page.handleInputMode("ctrl+e")
	if page.inputCursor != 5 {
		t.Errorf("ctrl+e: expected cursor at 5, got %d", page.inputCursor)
	}
}
