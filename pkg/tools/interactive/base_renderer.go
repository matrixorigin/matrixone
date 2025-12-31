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

// RowDecorator adds prefix/suffix to rows (cursor, search highlight, etc.)
type RowDecorator func(rowIdx int, isSelected bool) (prefix, suffix string)

// BaseRenderer is the lowest level table renderer
// It only knows how to draw a table with borders
// All features (cursor, search, filter) are added via decorators and options
type BaseRenderer struct {
	// Data
	Headers []string
	Rows    [][]string

	// Basic options
	ShowBorder    bool
	ShowRowNumber bool
	RowNumLabel   string
	MaxColWidth   int

	// Pagination (which rows to show)
	StartRow int
	EndRow   int // 0 means all

	// Row number offset (for display, e.g., showing rows 10-20 as "10, 11, 12...")
	RowNumOffset int

	// Horizontal scroll - column-based (which columns to show)
	HScrollOffset int // Starting column index
	MaxVisibleCols int // Max columns to display (0 = all)

	// Decorators - add features without modifying base
	RowDecorators []RowDecorator

	// Selected row (for decorators to use)
	SelectedRow int
}

// NewBaseRenderer creates a minimal renderer
func NewBaseRenderer() *BaseRenderer {
	return &BaseRenderer{
		ShowBorder:    true,
		ShowRowNumber: false,
		RowNumLabel:   "#",
		MaxColWidth:   0,
		SelectedRow:   -1,
	}
}

// Render renders the table
func (r *BaseRenderer) Render() string {
	if len(r.Rows) == 0 {
		return ""
	}

	// Determine row range
	start := r.StartRow
	end := r.EndRow
	if end <= 0 || end > len(r.Rows) {
		end = len(r.Rows)
	}
	if start < 0 {
		start = 0
	}
	if start >= end {
		return ""
	}

	rows := r.Rows[start:end]

	// Determine visible columns
	visibleHeaders, visibleColIndices := r.getVisibleColumns()
	widths := r.calcWidths(visibleColIndices)

	var b strings.Builder

	if r.ShowBorder {
		r.renderTopBorder(&b, widths)
	}
	r.renderHeader(&b, visibleHeaders, widths)
	if r.ShowBorder {
		r.renderSeparator(&b, widths)
	}

	for i, row := range rows {
		absIdx := start + i
		r.renderRow(&b, absIdx, row, visibleColIndices, widths)
	}

	if r.ShowBorder {
		r.renderBottomBorder(&b, widths)
	}

	return b.String()
}

func (r *BaseRenderer) getVisibleColumns() ([]string, []int) {
	totalCols := len(r.Headers)
	if totalCols == 0 {
		return nil, nil
	}

	startCol := r.HScrollOffset
	if startCol < 0 {
		startCol = 0
	}
	if startCol >= totalCols {
		startCol = totalCols - 1
	}

	maxCols := r.MaxVisibleCols
	if maxCols <= 0 {
		maxCols = totalCols // Show all
	}

	endCol := startCol + maxCols
	if endCol > totalCols {
		endCol = totalCols
	}

	visibleHeaders := r.Headers[startCol:endCol]
	visibleColIndices := make([]int, endCol-startCol)
	for i := range visibleColIndices {
		visibleColIndices[i] = startCol + i
	}

	return visibleHeaders, visibleColIndices
}

func (r *BaseRenderer) calcWidths(colIndices []int) []int {
	widths := make([]int, len(colIndices))
	for i, colIdx := range colIndices {
		if colIdx < len(r.Headers) {
			widths[i] = len(r.Headers[colIdx])
		}
		if widths[i] < 4 {
			widths[i] = 4
		}
	}
	for _, row := range r.Rows {
		for i, colIdx := range colIndices {
			if colIdx < len(row) {
				l := len(row[colIdx])
				if r.MaxColWidth > 0 && l > r.MaxColWidth {
					l = r.MaxColWidth
				}
				if l > widths[i] {
					widths[i] = l
				}
			}
		}
	}
	return widths
}

func (r *BaseRenderer) rowNumWidth() int {
	w := len(r.RowNumLabel)
	if w < 4 {
		w = 4
	}
	return w
}

func (r *BaseRenderer) renderTopBorder(b *strings.Builder, widths []int) {
	b.WriteString("┌")
	if r.ShowRowNumber {
		b.WriteString(strings.Repeat("─", r.rowNumWidth()+2))
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

func (r *BaseRenderer) renderSeparator(b *strings.Builder, widths []int) {
	b.WriteString("├")
	if r.ShowRowNumber {
		b.WriteString(strings.Repeat("─", r.rowNumWidth()+2))
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

func (r *BaseRenderer) renderBottomBorder(b *strings.Builder, widths []int) {
	b.WriteString("└")
	if r.ShowRowNumber {
		b.WriteString(strings.Repeat("─", r.rowNumWidth()+2))
		b.WriteString("┴")
	}
	for i, w := range widths {
		if i > 0 {
			b.WriteString("┴")
		}
		b.WriteString(strings.Repeat("─", w+2))
	}
	b.WriteString("┘\n")
}

func (r *BaseRenderer) renderHeader(b *strings.Builder, headers []string, widths []int) {
	b.WriteString("│")
	if r.ShowRowNumber {
		fmt.Fprintf(b, " %-*s │", r.rowNumWidth(), r.RowNumLabel)
	}
	for i, h := range headers {
		fmt.Fprintf(b, " %-*s │", widths[i], h)
	}
	b.WriteString("\n")
}

func (r *BaseRenderer) renderRow(b *strings.Builder, absIdx int, row []string, colIndices []int, widths []int) {
	isSelected := absIdx == r.SelectedRow

	// Get decorations
	prefix, suffix := "", ""
	for _, dec := range r.RowDecorators {
		p, s := dec(absIdx, isSelected)
		prefix += p
		suffix += s
	}

	b.WriteString("│")
	if r.ShowRowNumber {
		displayNum := absIdx + r.RowNumOffset
		numStr := fmt.Sprintf("%d", displayNum)
		// Apply prefix to row number cell
		cell := prefix + numStr
		fmt.Fprintf(b, " %-*s │", r.rowNumWidth(), cell)
	}

	for i, colIdx := range colIndices {
		val := ""
		if colIdx < len(row) {
			val = row[colIdx]
			if r.MaxColWidth > 0 && len(val) > r.MaxColWidth {
				val = val[:r.MaxColWidth-3] + "..."
			}
		}
		fmt.Fprintf(b, " %-*s │", widths[i], val)
	}
	b.WriteString(suffix + "\n")
}

// === Built-in Decorators ===

// CursorDecorator adds "▶" prefix to selected row
func CursorDecorator() RowDecorator {
	return func(rowIdx int, isSelected bool) (string, string) {
		if isSelected {
			return "▶", ""
		}
		return " ", ""
	}
}

// SearchHighlightDecorator adds "*" to matching rows
func SearchHighlightDecorator(matches map[int]bool) RowDecorator {
	return func(rowIdx int, isSelected bool) (string, string) {
		if matches[rowIdx] {
			return "*", ""
		}
		return "", ""
	}
}
