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
)

// ListItem represents an item in a list view
type ListItem interface {
	// GetColumns returns column values for display
	GetColumns() []string
	// IsSelectable returns whether this item can be selected
	IsSelectable() bool
}

// ListConfig defines list view configuration
type ListConfig struct {
	Headers      []string // Column headers
	ColumnWidths []int    // Column widths
	ShowCursor   bool     // Whether to show cursor
	CursorWidth  int      // Width of cursor column (default 2)
}

// ListView renders a scrollable list with cursor
type ListView struct {
	config       ListConfig
	items        []ListItem
	cursor       int
	scrollOffset int
	pageSize     int
}

// NewListView creates a new list view
func NewListView(config ListConfig, items []ListItem, pageSize int) *ListView {
	if config.CursorWidth == 0 {
		config.CursorWidth = 2
	}
	return &ListView{
		config:       config,
		items:        items,
		cursor:       0,
		scrollOffset: 0,
		pageSize:     pageSize,
	}
}

// SetItems updates the items
func (lv *ListView) SetItems(items []ListItem) {
	lv.items = items
	if lv.cursor >= len(items) {
		lv.cursor = len(items) - 1
	}
	if lv.cursor < 0 {
		lv.cursor = 0
	}
	lv.adjustScroll()
}

// SetCursor sets the cursor position
func (lv *ListView) SetCursor(pos int) {
	if pos < 0 {
		lv.cursor = 0
	} else if pos >= len(lv.items) {
		lv.cursor = len(lv.items) - 1
	} else {
		lv.cursor = pos
	}
	lv.adjustScroll()
}

// GetCursor returns the current cursor position
func (lv *ListView) GetCursor() int {
	return lv.cursor
}

// MoveCursor moves the cursor by delta
func (lv *ListView) MoveCursor(delta int) {
	lv.SetCursor(lv.cursor + delta)
}

// SetScrollOffset sets the scroll offset
func (lv *ListView) SetScrollOffset(offset int) {
	lv.scrollOffset = offset
	if lv.scrollOffset < 0 {
		lv.scrollOffset = 0
	}
	if lv.scrollOffset >= len(lv.items) {
		lv.scrollOffset = len(lv.items) - 1
	}
}

// GetScrollOffset returns the current scroll offset
func (lv *ListView) GetScrollOffset() int {
	return lv.scrollOffset
}

// adjustScroll adjusts scroll offset to keep cursor visible
func (lv *ListView) adjustScroll() {
	if lv.cursor < lv.scrollOffset {
		lv.scrollOffset = lv.cursor
	} else if lv.cursor >= lv.scrollOffset+lv.pageSize {
		lv.scrollOffset = lv.cursor - lv.pageSize + 1
	}
}

// Render renders the list view
func (lv *ListView) Render() string {
	var b strings.Builder

	// Render header
	lv.renderHeader(&b)

	// Calculate visible range
	start := lv.scrollOffset
	end := start + lv.pageSize
	if end > len(lv.items) {
		end = len(lv.items)
	}

	// Render rows
	for i := start; i < end; i++ {
		lv.renderRow(&b, i, lv.items[i])
	}

	return b.String()
}

// renderHeader renders the list header
func (lv *ListView) renderHeader(b *strings.Builder) {
	// Cursor column
	if lv.config.ShowCursor {
		b.WriteString(strings.Repeat(" ", lv.config.CursorWidth))
	}

	// Data columns
	for i, header := range lv.config.Headers {
		width := lv.config.ColumnWidths[i]
		if i == 0 {
			b.WriteString(fmt.Sprintf("%-*s", width, header))
		} else {
			b.WriteString(fmt.Sprintf(" │ %-*s", width, header))
		}
	}
	b.WriteString("\n")

	// Separator - must match header format exactly
	// Header format: "cursor" + "col1" + " │ col2" + " │ col3" + ...
	// Separator:     "──────" + "────" + "─┼─────" + "─┼─────" + ...
	if lv.config.ShowCursor {
		dashes := strings.Repeat("─", lv.config.CursorWidth)
		b.WriteString(dashes)
		// Debug: would log here but can't in production code
	}
	for i, width := range lv.config.ColumnWidths {
		if i == 0 {
			// First column: just width dashes
			dashes := strings.Repeat("─", width)
			b.WriteString(dashes)
		} else {
			// Other columns: " │ " becomes "─┼─", total 3 chars
			// Then width more dashes for the column content
			b.WriteString("─┼─")
			dashes := strings.Repeat("─", width)
			b.WriteString(dashes)
		}
	}
	b.WriteString("\n")
}

// renderRow renders a single list row
func (lv *ListView) renderRow(b *strings.Builder, index int, item ListItem) {
	// Cursor marker
	if lv.config.ShowCursor {
		if index == lv.cursor {
			b.WriteString("▶ ")
		} else {
			b.WriteString("  ")
		}
	}

	// Data columns
	columns := item.GetColumns()
	for i, col := range columns {
		width := lv.config.ColumnWidths[i]
		// Truncate if too long
		if len(col) > width {
			col = col[:width-3] + "..."
		}
		if i == 0 {
			b.WriteString(fmt.Sprintf("%-*s", width, col))
		} else {
			b.WriteString(fmt.Sprintf(" │ %-*s", width, col))
		}
	}
	b.WriteString("\n")
}

// GetItemCount returns the number of items
func (lv *ListView) GetItemCount() int {
	return len(lv.items)
}
