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

// TableRenderer is a generic table rendering framework
// It handles: borders, column widths, horizontal scroll, cursor, search highlights
type TableRenderer struct {
	// Data
	Headers []string
	Rows    [][]string

	// Display options
	MaxColWidth     int  // 0 = unlimited
	ShowRowNumber   bool // Show row number column
	RowNumLabel     string
	RowNumberOffset int // Offset for row numbers (e.g., if showing rows 10-20, set to 10)

	// Scroll state
	HScrollOffset int // Horizontal scroll offset
	VScrollOffset int // Vertical scroll offset (row index to start from)
	PageSize      int // Number of rows to display

	// Cursor state
	CursorEnabled bool
	CursorPos     int // Current cursor position (absolute row index)

	// Search state
	SearchEnabled bool
	SearchMatches map[int]bool // Row indices that match search

	// Filter state
	FilterEnabled bool
	FilterInfo    string
}

// NewTableRenderer creates a new table renderer
func NewTableRenderer() *TableRenderer {
	return &TableRenderer{
		PageSize:      30,
		RowNumLabel:   "#",
		SearchMatches: make(map[int]bool),
	}
}

// Render renders the table to a string
func (tr *TableRenderer) Render() string {
	if len(tr.Rows) == 0 {
		return ""
	}

	// Apply horizontal scroll
	visibleHeaders, visibleRows := tr.applyHorizontalScroll()

	// Apply vertical scroll (pagination)
	start := tr.VScrollOffset
	end := start + tr.PageSize
	if end > len(visibleRows) {
		end = len(visibleRows)
	}
	pageRows := visibleRows[start:end]

	// Calculate column widths
	widths := tr.calcWidths(visibleHeaders, visibleRows)

	var b strings.Builder

	// Top border
	tr.renderTopBorder(&b, widths)

	// Header
	tr.renderHeader(&b, visibleHeaders, widths)

	// Header separator
	tr.renderHeaderSeparator(&b, widths)

	// Data rows
	for i, row := range pageRows {
		absoluteRowIdx := start + i
		tr.renderRow(&b, absoluteRowIdx, row, widths)
	}

	// Bottom border
	tr.renderBottomBorder(&b, widths)

	// Status line
	if tr.FilterEnabled && tr.FilterInfo != "" {
		b.WriteString("\n")
		b.WriteString(tr.FilterInfo)
	}

	return b.String()
}

func (tr *TableRenderer) applyHorizontalScroll() ([]string, [][]string) {
	if tr.HScrollOffset >= len(tr.Headers) {
		tr.HScrollOffset = len(tr.Headers) - 1
	}
	if tr.HScrollOffset < 0 {
		tr.HScrollOffset = 0
	}

	visibleHeaders := tr.Headers[tr.HScrollOffset:]
	visibleRows := make([][]string, len(tr.Rows))
	for i, row := range tr.Rows {
		if tr.HScrollOffset < len(row) {
			visibleRows[i] = row[tr.HScrollOffset:]
		} else {
			visibleRows[i] = []string{}
		}
	}

	return visibleHeaders, visibleRows
}

func (tr *TableRenderer) calcWidths(headers []string, rows [][]string) []int {
	widths := make([]int, len(headers))

	// Start with header widths
	for i, h := range headers {
		widths[i] = len(h)
		if widths[i] < 8 {
			widths[i] = 8
		}
	}

	// Adjust based on data
	for _, row := range rows {
		for i := 0; i < len(widths) && i < len(row); i++ {
			cellLen := len(row[i])
			if tr.MaxColWidth > 0 && cellLen > tr.MaxColWidth {
				cellLen = tr.MaxColWidth
			}
			if cellLen > widths[i] {
				widths[i] = cellLen
			}
		}
	}

	return widths
}

func (tr *TableRenderer) renderTopBorder(b *strings.Builder, widths []int) {
	b.WriteString("┌")
	if tr.ShowRowNumber {
		rowNumWidth := len(tr.RowNumLabel)
		if rowNumWidth < 8 {
			rowNumWidth = 8
		}
		b.WriteString(strings.Repeat("─", rowNumWidth+2))
		b.WriteString("┬")
	}
	for i, w := range widths {
		if i > 0 {
			b.WriteString("┬")
		}
		b.WriteString(strings.Repeat("─", w+2))
	}
	b.WriteString("┐\n")
}

func (tr *TableRenderer) renderHeader(b *strings.Builder, headers []string, widths []int) {
	b.WriteString("│")
	if tr.ShowRowNumber {
		rowNumWidth := len(tr.RowNumLabel)
		if rowNumWidth < 8 {
			rowNumWidth = 8
		}
		fmt.Fprintf(b, " %-*s │", rowNumWidth, tr.RowNumLabel)
	}
	for i, h := range headers {
		if i < len(widths) {
			fmt.Fprintf(b, " %-*s │", widths[i], h)
		}
	}
	b.WriteString("\n")
}

func (tr *TableRenderer) renderHeaderSeparator(b *strings.Builder, widths []int) {
	b.WriteString("├")
	if tr.ShowRowNumber {
		rowNumWidth := len(tr.RowNumLabel)
		if rowNumWidth < 8 {
			rowNumWidth = 8
		}
		b.WriteString(strings.Repeat("─", rowNumWidth+2))
		b.WriteString("┼")
	}
	for i, w := range widths {
		if i > 0 {
			b.WriteString("┼")
		}
		b.WriteString(strings.Repeat("─", w+2))
	}
	b.WriteString("┤\n")
}

func (tr *TableRenderer) renderRow(b *strings.Builder, rowIdx int, row []string, widths []int) {
	b.WriteString("│")

	// Row number column with cursor and search indicator
	if tr.ShowRowNumber {
		// Calculate display row number
		displayRowNum := rowIdx + tr.RowNumberOffset

		cursor := " "
		if tr.CursorEnabled && displayRowNum == tr.CursorPos {
			cursor = "▶"
		}
		searchMark := " "
		if tr.SearchEnabled && tr.SearchMatches[displayRowNum] {
			searchMark = "✓"
		}
		// Calculate proper width: cursor(1) + search(1) + number
		rowNumWidth := len(tr.RowNumLabel)
		if rowNumWidth < 8 {
			rowNumWidth = 8
		}
		rowNum := fmt.Sprintf("%s%s%d", cursor, searchMark, displayRowNum)
		fmt.Fprintf(b, " %-*s │", rowNumWidth, rowNum)
	}

	// Data columns
	for i := 0; i < len(widths); i++ {
		cell := ""
		if i < len(row) {
			cell = row[i]
			if tr.MaxColWidth > 0 && len(cell) > tr.MaxColWidth {
				cell = cell[:tr.MaxColWidth-3] + "..."
			}
		}
		fmt.Fprintf(b, " %-*s │", widths[i], cell)
	}
	b.WriteString("\n")
}

func (tr *TableRenderer) renderBottomBorder(b *strings.Builder, widths []int) {
	b.WriteString("└")
	if tr.ShowRowNumber {
		rowNumWidth := len(tr.RowNumLabel)
		if rowNumWidth < 8 {
			rowNumWidth = 8
		}
		b.WriteString(strings.Repeat("─", rowNumWidth+2))
		b.WriteString("┴")
	}
	for i, w := range widths {
		if i > 0 {
			b.WriteString("┴")
		}
		b.WriteString(strings.Repeat("─", w+2))
	}
	b.WriteString("┘")
}
