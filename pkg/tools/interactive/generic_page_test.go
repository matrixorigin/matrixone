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
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

// Example: How to use GenericPage for any list view
// All you need: Config + DataProvider + ActionHandler

// === 1. DataProvider: just returns rows ===
type testDataProvider struct {
	rows     [][]string
	overview string
}

func (p *testDataProvider) GetRows() [][]string {
	return p.rows
}

func (p *testDataProvider) GetRowNums() []string {
	return nil // Use default row numbers
}

func (p *testDataProvider) GetOverview() string {
	return p.overview
}

// === 2. ActionHandler: handles user actions ===
type testActionHandler struct {
	selectedIdx int
}

func (h *testActionHandler) OnSelect(rowIdx int) tea.Cmd {
	h.selectedIdx = rowIdx
	return nil
}

func (h *testActionHandler) OnBack() tea.Cmd {
	return nil
}

func (h *testActionHandler) OnCustomKey(key string) tea.Cmd {
	return nil
}

func (h *testActionHandler) MatchRow(row []string, query string) bool {
	for _, cell := range row {
		if strings.Contains(strings.ToLower(cell), strings.ToLower(query)) {
			return true
		}
	}
	return false
}

func (h *testActionHandler) FilterRow(row []string, filter string) bool {
	return h.MatchRow(row, filter)
}

// === Test: Create a page with config + data ===
func TestGenericPageUsage(t *testing.T) {
	// 1. Prepare data
	data := &testDataProvider{
		rows: [][]string{
			{"1", "Alice", "100"},
			{"2", "Bob", "200"},
			{"3", "Charlie", "300"},
		},
		overview: "Total: 3 users",
	}

	// 2. Create handler
	handler := &testActionHandler{}

	// 3. Create page with config (搭积木)
	config := PageConfig{
		Title:         "═══ User List ═══",
		Headers:       []string{"ID", "Name", "Score"},
		ShowRowNumber: true,
		EnableCursor:  true,
		EnableSearch:  true,
		EnableFilter:  true,
		EnableBack:    true,
	}

	page := NewGenericPage(config, data, handler)

	// 4. Render
	view := page.View()
	t.Logf("Page view:\n%s", view)

	// Verify
	if !strings.Contains(view, "User List") {
		t.Error("Should contain title")
	}
	if !strings.Contains(view, "Alice") {
		t.Error("Should contain data")
	}
	if !strings.Contains(view, "[j/k] Navigate") {
		t.Error("Should contain hints")
	}
}

// === Test: Different configs for different pages ===
func TestDifferentPageConfigs(t *testing.T) {
	data := &testDataProvider{
		rows: [][]string{
			{"A", "B"},
			{"C", "D"},
		},
	}
	handler := &testActionHandler{}

	// Config 1: Simple list (no search, no filter)
	simpleConfig := PageConfig{
		Title:        "Simple List",
		Headers:      []string{"Col1", "Col2"},
		EnableCursor: true,
	}
	simplePage := NewGenericPage(simpleConfig, data, handler)
	simpleView := simplePage.View()

	if strings.Contains(simpleView, "[/] Search") {
		t.Error("Simple page should not have search hint")
	}

	// Config 2: Full featured
	fullConfig := PageConfig{
		Title:         "Full Featured",
		Headers:       []string{"Col1", "Col2"},
		ShowRowNumber: true,
		EnableCursor:  true,
		EnableSearch:  true,
		EnableFilter:  true,
		EnableBack:    true,
	}
	fullPage := NewGenericPage(fullConfig, data, handler)
	fullView := fullPage.View()

	if !strings.Contains(fullView, "[/] Search") {
		t.Error("Full page should have search hint")
	}
	if !strings.Contains(fullView, "[f] Filter") {
		t.Error("Full page should have filter hint")
	}

	t.Logf("Simple:\n%s\n\nFull:\n%s", simpleView, fullView)
}

// === Test: Navigation ===
func TestGenericPageNavigation(t *testing.T) {
	data := &testDataProvider{
		rows: [][]string{
			{"1", "First"},
			{"2", "Second"},
			{"3", "Third"},
		},
	}
	handler := &testActionHandler{}

	config := PageConfig{
		Headers:      []string{"ID", "Name"},
		EnableCursor: true,
	}
	page := NewGenericPage(config, data, handler)

	// Initial cursor at 0
	if page.GetCursor() != 0 {
		t.Errorf("Initial cursor should be 0, got %d", page.GetCursor())
	}

	// Move down
	page.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'j'}})
	if page.GetCursor() != 1 {
		t.Errorf("After j, cursor should be 1, got %d", page.GetCursor())
	}

	// Move up
	page.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'k'}})
	if page.GetCursor() != 0 {
		t.Errorf("After k, cursor should be 0, got %d", page.GetCursor())
	}

	// Go to bottom
	page.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'G'}})
	if page.GetCursor() != 2 {
		t.Errorf("After G, cursor should be 2, got %d", page.GetCursor())
	}

	// Go to top
	page.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'g'}})
	if page.GetCursor() != 0 {
		t.Errorf("After g, cursor should be 0, got %d", page.GetCursor())
	}
}

// === Example: How checkpoint list would use this ===
func ExampleGenericPage() {
	// This is how checkpoint list would be implemented:
	//
	// config := PageConfig{
	//     Title:         "═══ Checkpoint Viewer ═══",
	//     Headers:       []string{"Type", "Start", "End", "State", "Version", "LSN"},
	//     Overview:      fmt.Sprintf("Dir: %s │ Total: %d entries", dir, count),
	//     ShowRowNumber: true,
	//     EnableCursor:  true,
	//     EnableSearch:  false,  // Checkpoints don't need search
	//     EnableBack:    false,  // This is the root page
	// }
	//
	// provider := &CheckpointDataProvider{reader: reader}
	// handler := &CheckpointActionHandler{onSelect: func(idx int) { ... }}
	//
	// page := NewGenericPage(config, provider, handler)

	fmt.Println("See TestGenericPageUsage for working example")
	// Output: See TestGenericPageUsage for working example
}
