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

func TestTableRenderer_Basic(t *testing.T) {
	renderer := NewTableRenderer()
	renderer.Headers = []string{"Col1", "Col2", "Col3"}
	renderer.Rows = [][]string{
		{"a", "b", "c"},
		{"longer text", "y", "z"},
	}

	output := renderer.Render()

	// Check borders
	if !strings.Contains(output, "┌") || !strings.Contains(output, "└") {
		t.Error("Output should contain borders")
	}

	// Check headers
	if !strings.Contains(output, "Col1") {
		t.Error("Output should contain Col1")
	}

	// Check data
	if !strings.Contains(output, "longer text") {
		t.Error("Output should contain data")
	}
}

func TestTableRenderer_HorizontalScroll(t *testing.T) {
	renderer := NewTableRenderer()
	renderer.Headers = []string{"Col1", "Col2", "Col3", "Col4"}
	renderer.Rows = [][]string{
		{"a", "b", "c", "d"},
		{"e", "f", "g", "h"},
	}
	renderer.HScrollOffset = 1 // Skip Col1

	output := renderer.Render()

	// Should NOT contain Col1
	if strings.Contains(output, "Col1") {
		t.Error("Should not show Col1 after scroll")
	}

	// Should contain Col2
	if !strings.Contains(output, "Col2") {
		t.Error("Should show Col2")
	}
}

func TestTableRenderer_Cursor(t *testing.T) {
	renderer := NewTableRenderer()
	renderer.Headers = []string{"Col1", "Col2"}
	renderer.Rows = [][]string{
		{"a", "b"},
		{"c", "d"},
		{"e", "f"},
	}
	renderer.ShowRowNumber = true
	renderer.CursorEnabled = true
	renderer.CursorPos = 1 // Cursor on row 1

	output := renderer.Render()

	// Should contain cursor marker
	if !strings.Contains(output, "▶") {
		t.Error("Should show cursor marker")
	}
}

func TestTableRenderer_Search(t *testing.T) {
	renderer := NewTableRenderer()
	renderer.Headers = []string{"Col1", "Col2"}
	renderer.Rows = [][]string{
		{"a", "b"},
		{"c", "d"},
		{"e", "f"},
	}
	renderer.ShowRowNumber = true
	renderer.SearchEnabled = true
	renderer.SearchMatches = map[int]bool{0: true, 2: true} // Rows 0 and 2 match

	output := renderer.Render()

	// Should contain search marker
	if !strings.Contains(output, "✓") {
		t.Error("Should show search marker")
	}
}

func TestTableRenderer_Pagination(t *testing.T) {
	renderer := NewTableRenderer()
	renderer.Headers = []string{"Col1"}
	renderer.Rows = [][]string{
		{"row0"},
		{"row1"},
		{"row2"},
		{"row3"},
		{"row4"},
	}
	renderer.VScrollOffset = 2 // Start from row 2
	renderer.PageSize = 2      // Show 2 rows

	output := renderer.Render()

	// Should contain row2 and row3
	if !strings.Contains(output, "row2") {
		t.Error("Should show row2")
	}
	if !strings.Contains(output, "row3") {
		t.Error("Should show row3")
	}

	// Should NOT contain row0, row1, row4
	if strings.Contains(output, "row0") || strings.Contains(output, "row1") || strings.Contains(output, "row4") {
		t.Error("Should not show rows outside page")
	}
}

func TestTableRenderer_Composable(t *testing.T) {
	// Test combining multiple features
	renderer := NewTableRenderer()
	renderer.Headers = []string{"ID", "Name", "Status"}
	renderer.Rows = [][]string{
		{"1", "Alice", "Active"},
		{"2", "Bob", "Inactive"},
		{"3", "Charlie", "Active"},
		{"4", "David", "Active"},
	}

	// Enable all features
	renderer.ShowRowNumber = true
	renderer.CursorEnabled = true
	renderer.CursorPos = 1
	renderer.SearchEnabled = true
	renderer.SearchMatches = map[int]bool{0: true, 2: true}
	renderer.HScrollOffset = 0
	renderer.VScrollOffset = 0
	renderer.PageSize = 3
	renderer.FilterEnabled = true
	renderer.FilterInfo = "Filter: Status=Active"

	output := renderer.Render()

	// Should have all features
	if !strings.Contains(output, "▶") {
		t.Error("Should show cursor")
	}
	if !strings.Contains(output, "✓") {
		t.Error("Should show search markers")
	}
	if !strings.Contains(output, "Filter: Status=Active") {
		t.Error("Should show filter info")
	}
}
