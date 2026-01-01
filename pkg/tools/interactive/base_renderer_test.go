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

// TestRowNumWidthWithCursor tests that row number width accounts for cursor prefix
func TestRowNumWidthWithCursor(t *testing.T) {
	r := NewBaseRenderer()
	r.Headers = []string{"Col1", "Col2"}
	r.Rows = [][]string{
		{"val1", "val2"},
		{"val3", "val4"},
	}
	r.RowNums = []string{"(0-0)", "(0-1)"}
	r.ShowRowNumber = true
	r.RowNumLabel = "RowNum"
	r.SelectedRow = 0 // Enable cursor
	r.RowDecorators = append(r.RowDecorators, CursorDecorator())

	output := r.Render()

	// Verify output contains expected content
	if !strings.Contains(output, "RowNum") {
		t.Error("Should contain RowNum label")
	}
	if !strings.Contains(output, "▶(0-0)") {
		t.Error("Should contain cursor and row number")
	}
	if !strings.Contains(output, "(0-1)") {
		t.Error("Should contain second row number")
	}
	if !strings.Contains(output, "val1") {
		t.Error("Should contain data")
	}

	// Verify width calculation accounts for cursor
	// With decorator, width should be at least len("(0-0)") + 2 = 7
	if r.cachedRowNumWidth < 7 {
		t.Errorf("Expected cachedRowNumWidth >= 7, got %d", r.cachedRowNumWidth)
	}
}

// TestCustomRowNumbers tests that custom row numbers are displayed correctly
func TestCustomRowNumbers(t *testing.T) {
	r := NewBaseRenderer()
	r.Headers = []string{"Col1"}
	r.Rows = [][]string{{"val1"}, {"val2"}}
	r.RowNums = []string{"(0-18)", "(0-19)"}
	r.ShowRowNumber = true
	r.RowNumLabel = "RowNum"

	output := r.Render()

	// Check that custom row numbers are displayed
	if !strings.Contains(output, "(0-18)") {
		t.Error("Expected to find custom row number (0-18)")
	}
	if !strings.Contains(output, "(0-19)") {
		t.Error("Expected to find custom row number (0-19)")
	}
	if !strings.Contains(output, "RowNum") {
		t.Error("Expected to find RowNum label")
	}
}

// TestHorizontalScrollByColumn tests column-based horizontal scrolling
func TestHorizontalScrollByColumn(t *testing.T) {
	r := NewBaseRenderer()
	r.Headers = []string{"Col0", "Col1", "Col2", "Col3", "Col4"}
	r.Rows = [][]string{
		{"a0", "a1", "a2", "a3", "a4"},
		{"b0", "b1", "b2", "b3", "b4"},
	}
	r.ShowRowNumber = false
	r.HScrollOffset = 2  // Start from Col2
	r.MaxVisibleCols = 2 // Show 2 columns

	output := r.Render()

	// Should show Col2 and Col3, not Col0 and Col1
	if strings.Contains(output, "Col0") || strings.Contains(output, "Col1") {
		t.Error("Should not show Col0 or Col1 when scrolled")
	}
	if !strings.Contains(output, "Col2") || !strings.Contains(output, "Col3") {
		t.Error("Should show Col2 and Col3")
	}
	if strings.Contains(output, "Col4") {
		t.Error("Should not show Col4 (beyond MaxVisibleCols)")
	}

	// Check data
	if strings.Contains(output, "a0") || strings.Contains(output, "a1") {
		t.Error("Should not show data from Col0 or Col1")
	}
	if !strings.Contains(output, "a2") || !strings.Contains(output, "a3") {
		t.Error("Should show data from Col2 and Col3")
	}
}

// TestRowNumWidthCalculation tests that row number width is calculated correctly
func TestRowNumWidthCalculation(t *testing.T) {
	tests := []struct {
		name         string
		label        string
		rowNums      []string
		hasDecorator bool
		expectedMin  int
	}{
		{
			name:         "Default label",
			label:        "#",
			rowNums:      nil,
			hasDecorator: false,
			expectedMin:  4, // Minimum width
		},
		{
			name:         "RowNum label",
			label:        "RowNum",
			rowNums:      nil,
			hasDecorator: false,
			expectedMin:  6,
		},
		{
			name:         "Custom row numbers",
			label:        "#",
			rowNums:      []string{"(0-0)", "(0-1)"},
			hasDecorator: false,
			expectedMin:  5, // len("(0-0)")
		},
		{
			name:         "Custom row numbers with decorator",
			label:        "#",
			rowNums:      []string{"(0-0)", "(0-1)"},
			hasDecorator: true,
			expectedMin:  7, // len("(0-0)") + 2 for UTF-8 cursor overhead
		},
		{
			name:         "RowNum label with custom numbers and decorator",
			label:        "RowNum",
			rowNums:      []string{"(0-18)", "(0-19)"},
			hasDecorator: true,
			expectedMin:  8, // max(len("RowNum"), len("(0-18)")+2)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewBaseRenderer()
			r.Headers = []string{"Col1"}
			r.Rows = [][]string{{"val1"}}
			r.RowNums = tt.rowNums
			r.ShowRowNumber = true
			r.RowNumLabel = tt.label
			if tt.hasDecorator {
				r.RowDecorators = append(r.RowDecorators, CursorDecorator())
			}

			r.calcRowNumWidth()

			if r.cachedRowNumWidth < tt.expectedMin {
				t.Errorf("Expected row num width >= %d, got %d",
					tt.expectedMin, r.cachedRowNumWidth)
			}
		})
	}
}
