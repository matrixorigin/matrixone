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

// RenderSimpleTable renders a table with borders (like objecttool style)
// headers: column names
// rows: data rows (each row is []string)
// maxColWidth: max width per column (0 = unlimited)
// hScrollOffset: horizontal scroll offset (number of columns to skip from left)
func RenderSimpleTable(headers []string, rows [][]string, maxColWidth int, hScrollOffset int) string {
	if len(rows) == 0 {
		return ""
	}

	// Apply horizontal scroll offset
	if hScrollOffset >= len(headers) {
		hScrollOffset = len(headers) - 1
	}
	if hScrollOffset < 0 {
		hScrollOffset = 0
	}

	visibleHeaders := headers[hScrollOffset:]
	visibleRows := make([][]string, len(rows))
	for i, row := range rows {
		if hScrollOffset < len(row) {
			visibleRows[i] = row[hScrollOffset:]
		} else {
			visibleRows[i] = []string{}
		}
	}

	// Calculate column widths
	widths := make([]int, len(visibleHeaders))
	for i, h := range visibleHeaders {
		widths[i] = len(h)
		if widths[i] < 8 {
			widths[i] = 8
		}
	}

	for _, row := range visibleRows {
		for i := 0; i < len(widths) && i < len(row); i++ {
			cellLen := len(row[i])
			if maxColWidth > 0 && cellLen > maxColWidth {
				cellLen = maxColWidth
			}
			if cellLen > widths[i] {
				widths[i] = cellLen
			}
		}
	}

	var b strings.Builder

	// Top border: ┌────┬────┐
	b.WriteString("┌")
	for i, w := range widths {
		if i > 0 {
			b.WriteString("┬")
		}
		b.WriteString(strings.Repeat("─", w+2))
	}
	b.WriteString("┐\n")

	// Header row: │Col1│Col2│
	b.WriteString("│")
	for i, h := range visibleHeaders {
		if i < len(widths) {
			fmt.Fprintf(&b, " %-*s │", widths[i], h)
		}
	}
	b.WriteString("\n")

	// Header separator: ├────┼────┤
	b.WriteString("├")
	for i, w := range widths {
		if i > 0 {
			b.WriteString("┼")
		}
		b.WriteString(strings.Repeat("─", w+2))
	}
	b.WriteString("┤\n")

	// Data rows
	for _, row := range visibleRows {
		b.WriteString("│")
		for i := 0; i < len(widths); i++ {
			cell := ""
			if i < len(row) {
				cell = row[i]
				if maxColWidth > 0 && len(cell) > maxColWidth {
					cell = cell[:maxColWidth-3] + "..."
				}
			}
			fmt.Fprintf(&b, " %-*s │", widths[i], cell)
		}
		b.WriteString("\n")
	}

	// Bottom border: └────┴────┘
	b.WriteString("└")
	for i, w := range widths {
		if i > 0 {
			b.WriteString("┴")
		}
		b.WriteString(strings.Repeat("─", w+2))
	}
	b.WriteString("┘")

	return b.String()
}
