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

	"github.com/stretchr/testify/assert"
)

// TestListItem for testing
type TestListItem struct {
	cols []string
}

func (t *TestListItem) GetColumns() []string {
	return t.cols
}

func (t *TestListItem) IsSelectable() bool {
	return true
}

// TestListViewAlignment tests that columns are properly aligned
func TestListViewAlignment(t *testing.T) {
	config := ListConfig{
		Headers:      []string{"#", "Type", "Start", "End", "State"},
		ColumnWidths: []int{7, 6, 51, 51, 8},
		ShowCursor:   true,
		CursorWidth:  2,
	}

	items := []ListItem{
		&TestListItem{cols: []string{"0", "I", "1767141334018510083-1(2025/12/31 08:35:34.018510)", "1767141659018427182-0(2025/12/31 08:40:59.018427)", "Finished"}},
		&TestListItem{cols: []string{"1", "G", "1767141659018427182-0(2025/12/31 08:40:59.018427)", "1767141984018344281-0(2025/12/31 08:46:24.018344)", "Finished"}},
	}

	listView := NewListView(config, items, 20)
	output := listView.Render()

	lines := strings.Split(output, "\n")
	assert.GreaterOrEqual(t, len(lines), 4, "Should have at least header, separator, and 2 data rows")

	// Check that all lines have the same length (proper alignment)
	headerLine := lines[0]
	separatorLine := lines[1]
	dataLine1 := lines[2]
	dataLine2 := lines[3]

	t.Logf("Header:    %q (len=%d, runes=%d)", headerLine, len(headerLine), len([]rune(headerLine)))
	t.Logf("Separator: %q (len=%d, runes=%d)", separatorLine, len(separatorLine), len([]rune(separatorLine)))
	t.Logf("Data1:     %q (len=%d, runes=%d)", dataLine1, len(dataLine1), len([]rune(dataLine1)))
	t.Logf("Data2:     %q (len=%d, runes=%d)", dataLine2, len(dataLine2), len([]rune(dataLine2)))

	// Debug: print config
	t.Logf("Config: Headers=%v, ColumnWidths=%v, ShowCursor=%v, CursorWidth=%d",
		config.Headers, config.ColumnWidths, config.ShowCursor, config.CursorWidth)

	// All lines should have the same visual width (rune count)
	headerRunes := len([]rune(headerLine))
	separatorRunes := len([]rune(separatorLine))
	data1Runes := len([]rune(dataLine1))
	data2Runes := len([]rune(dataLine2))

	assert.Equal(t, headerRunes, separatorRunes, "Header and separator should have same rune count")
	assert.Equal(t, headerRunes, data1Runes, "Header and data line 1 should have same rune count")
	assert.Equal(t, headerRunes, data2Runes, "Header and data line 2 should have same rune count")

	// Visual alignment is correct if rune counts match
	// (Byte positions may differ due to multi-byte UTF-8 characters like ▶ and ─)
}

// findAllPositions finds all positions of a character in a string
func findAllPositions(s string, ch rune) []int {
	positions := []int{}
	for i, c := range s {
		if c == ch {
			positions = append(positions, i)
		}
	}
	return positions
}
