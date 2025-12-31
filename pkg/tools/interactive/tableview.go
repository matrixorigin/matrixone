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

// TableView renders a table with cursor, search highlights, and filter support
type TableView struct {
	config       TableConfig
	dataSource   DataSource
	searchEngine *SearchEngine
	cursor       int
	scrollOffset int
	screenHeight int
}

// NewTableView creates a new table view
func NewTableView(config TableConfig, dataSource DataSource, screenHeight int) *TableView {
	return &TableView{
		config:       config,
		dataSource:   dataSource,
		searchEngine: NewSearchEngine(),
		cursor:       0,
		scrollOffset: 0,
		screenHeight: screenHeight,
	}
}

// SetCursor sets the cursor position
func (tv *TableView) SetCursor(pos int) {
	items := tv.dataSource.GetFilteredItems()
	if pos < 0 {
		tv.cursor = 0
	} else if pos >= len(items) {
		tv.cursor = len(items) - 1
	} else {
		tv.cursor = pos
	}
	tv.adjustScroll()
}

// GetCursor returns the current cursor position
func (tv *TableView) GetCursor() int {
	return tv.cursor
}

// MoveCursor moves the cursor by delta
func (tv *TableView) MoveCursor(delta int) {
	tv.SetCursor(tv.cursor + delta)
}

// adjustScroll adjusts scroll offset to keep cursor visible
func (tv *TableView) adjustScroll() {
	maxVisible := tv.screenHeight - 10 // Reserve space for header, footer, etc.
	if maxVisible < 5 {
		maxVisible = 5
	}

	if tv.cursor < tv.scrollOffset {
		tv.scrollOffset = tv.cursor
	} else if tv.cursor >= tv.scrollOffset+maxVisible {
		tv.scrollOffset = tv.cursor - maxVisible + 1
	}
}

// Search performs a search
func (tv *TableView) Search(term string) error {
	items := tv.dataSource.GetFilteredItems()
	if err := tv.searchEngine.SetSearchTerm(term, items); err != nil {
		return err
	}
	if tv.searchEngine.IsActive() && tv.searchEngine.GetMatchCount() > 0 {
		tv.SetCursor(tv.searchEngine.GetCurrentIndex())
	}
	return nil
}

// NextMatch jumps to next search match
func (tv *TableView) NextMatch() {
	if idx := tv.searchEngine.NextMatch(); idx >= 0 {
		tv.SetCursor(idx)
	}
}

// PrevMatch jumps to previous search match
func (tv *TableView) PrevMatch() {
	if idx := tv.searchEngine.PrevMatch(); idx >= 0 {
		tv.SetCursor(idx)
	}
}

// ClearSearch clears the search
func (tv *TableView) ClearSearch() {
	tv.searchEngine.Clear()
}

// GetSearchStatus returns search status text
func (tv *TableView) GetSearchStatus() string {
	return tv.searchEngine.GetStatusText()
}

// Render renders the table
func (tv *TableView) Render() string {
	var b strings.Builder

	items := tv.dataSource.GetFilteredItems()

	// Render header
	tv.renderHeader(&b)

	// Calculate visible range
	maxVisible := tv.screenHeight - 10
	if maxVisible < 5 {
		maxVisible = 5
	}
	start := tv.scrollOffset
	end := start + maxVisible
	if end > len(items) {
		end = len(items)
	}

	// Render rows
	for i := start; i < end; i++ {
		tv.renderRow(&b, i, items[i])
	}

	// Render footer
	tv.renderFooter(&b, len(items))

	return b.String()
}

// renderHeader renders table header
func (tv *TableView) renderHeader(b *strings.Builder) {
	// First column (cursor + number)
	if tv.config.AllowCursor {
		b.WriteString("  # │")
	} else {
		b.WriteString(" # │")
	}

	// Data columns
	for i, header := range tv.config.Headers {
		width := tv.config.ColumnWidths[i]
		b.WriteString(fmt.Sprintf(" %-*s │", width, header))
	}
	b.WriteString("\n")

	// Separator
	if tv.config.AllowCursor {
		b.WriteString("────┼")
	} else {
		b.WriteString("───┼")
	}
	for _, width := range tv.config.ColumnWidths {
		b.WriteString(strings.Repeat("─", width+2))
		b.WriteString("┼")
	}
	b.WriteString("\n")
}

// renderRow renders a single table row
func (tv *TableView) renderRow(b *strings.Builder, index int, item Item) {
	// Cursor marker
	cursor := " "
	if tv.config.AllowCursor && index == tv.cursor {
		cursor = "▶"
	}

	// Search highlight marker
	highlight := " "
	if tv.searchEngine.IsMatch(index) {
		highlight = "✓"
	}

	// Row number
	if tv.config.AllowCursor {
		b.WriteString(fmt.Sprintf("%s%s%2d│", cursor, highlight, index))
	} else {
		b.WriteString(fmt.Sprintf("%s%2d│", cursor, index))
	}

	// Data columns
	fields := item.GetFields()
	for i, field := range fields {
		width := tv.config.ColumnWidths[i]
		// Truncate if too long
		if len(field) > width {
			field = field[:width-3] + "..."
		}
		b.WriteString(fmt.Sprintf(" %-*s │", width, field))
	}
	b.WriteString("\n")
}

// renderFooter renders table footer with status info
func (tv *TableView) renderFooter(b *strings.Builder, totalItems int) {
	b.WriteString("\n")

	// Filter status
	if tv.dataSource.HasFilter() {
		b.WriteString(tv.dataSource.GetFilterInfo())
		b.WriteString("\n")
	}

	// Search status
	if tv.searchEngine.IsActive() {
		b.WriteString(tv.GetSearchStatus())
		b.WriteString("\n")
	}

	// Item count
	b.WriteString(fmt.Sprintf("Total: %d items", totalItems))
}
