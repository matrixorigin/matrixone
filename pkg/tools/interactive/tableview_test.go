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

// Mock item for testing
type mockItem struct {
	fields []string
}

func (m *mockItem) GetFields() []string {
	return m.fields
}

func (m *mockItem) MatchesSearch(term string) bool {
	for _, f := range m.fields {
		if strings.Contains(f, term) {
			return true
		}
	}
	return false
}

func (m *mockItem) GetSearchableText() string {
	return strings.Join(m.fields, " ")
}

func (m *mockItem) IsSelectable() bool {
	return true
}

// Mock data source for testing
type mockDataSource struct {
	items []Item
}

func (m *mockDataSource) GetItems() []Item {
	return m.items
}

func (m *mockDataSource) GetFilteredItems() []Item {
	return m.items
}

func (m *mockDataSource) HasFilter() bool {
	return false
}

func (m *mockDataSource) GetFilterInfo() string {
	return ""
}

func (m *mockDataSource) GetItemCount() int {
	return len(m.items)
}

func TestTableView_AutoWidth(t *testing.T) {
	// Create test data with varying column widths
	items := []Item{
		&mockItem{fields: []string{"short", "medium text", "very long text that should expand column"}},
		&mockItem{fields: []string{"x", "y", "z"}},
		&mockItem{fields: []string{"test", "another test", "final test"}},
	}

	dataSource := &mockDataSource{items: items}

	config := TableConfig{
		Headers:      []string{"Col1", "Col2", "Col3"},
		ColumnWidths: []int{5, 10, 15}, // Initial widths
		AllowCursor:  true,
	}

	tv := NewTableView(config, dataSource, 40)

	// Check that auto widths are calculated
	if len(tv.autoWidths) != 3 {
		t.Fatalf("Expected 3 auto widths, got %d", len(tv.autoWidths))
	}

	// Col1: max("Col1", "short", "x", "test") = "short" = 5
	if tv.autoWidths[0] < 5 {
		t.Errorf("Col1 width should be at least 5, got %d", tv.autoWidths[0])
	}

	// Col2: max("Col2", "medium text", "y", "another test") = "another test" = 12
	if tv.autoWidths[1] < 12 {
		t.Errorf("Col2 width should be at least 12, got %d", tv.autoWidths[1])
	}

	// Col3: max("Col3", "very long text...", "z", "final test") = 44
	expectedWidth := len("very long text that should expand column")
	if tv.autoWidths[2] < expectedWidth {
		t.Errorf("Col3 width should be at least %d, got %d", expectedWidth, tv.autoWidths[2])
	}

	// Render and check output contains full text
	output := tv.Render()
	if !strings.Contains(output, "very long text that should expand column") {
		t.Error("Output should contain full long text without truncation")
	}
}

func TestTableView_RefreshData(t *testing.T) {
	items := []Item{
		&mockItem{fields: []string{"a", "b", "c"}},
	}

	dataSource := &mockDataSource{items: items}

	config := TableConfig{
		Headers:      []string{"Col1", "Col2", "Col3"},
		ColumnWidths: []int{5, 5, 5},
		AllowCursor:  false,
	}

	tv := NewTableView(config, dataSource, 40)

	initialWidth := tv.autoWidths[0]

	// Add longer data
	dataSource.items = append(dataSource.items, &mockItem{fields: []string{"very long text", "b", "c"}})

	// Refresh should recalculate widths
	tv.RefreshData()

	if tv.autoWidths[0] <= initialWidth {
		t.Errorf("Width should have increased after refresh, was %d, now %d", initialWidth, tv.autoWidths[0])
	}
}
