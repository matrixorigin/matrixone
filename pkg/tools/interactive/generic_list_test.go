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
	if len(history) != 3 {
		t.Errorf("Expected 3 history entries, got %d", len(history))
	}

	if history[0] != "first" || history[1] != "second" || history[2] != "third" {
		t.Errorf("History order incorrect: %v", history)
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
