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
	"os"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
)

// formatSize formats size in bytes to human-readable format
func formatSize(size uint64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	div, exp := uint64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	units := []string{"KB", "MB", "GB", "TB", "PB"}
	if exp < len(units) {
		return fmt.Sprintf("%.2f %s", float64(size)/float64(div), units[exp])
	}
	return fmt.Sprintf("%d B", size)
}

// View is a simplified terminal view
type View struct {
	width        int
	height       int
	verticalMode bool // \G mode: vertical display
}

func NewView() *View {
	return &View{
		width:  120,
		height: 30,
	}
}

// Render renders the current state
func (v *View) Render(state *State, message string) error {
	v.clear()

	// Sync vertical mode state
	v.verticalMode = state.verticalMode

	// 1. Header
	v.renderHeader(state)

	// 2. Data table
	if err := v.renderTable(state); err != nil {
		return err
	}

	// 3. Status bar
	v.renderStatus(state)

	// 4. Message/command line
	if message != "" {
		fmt.Fprintln(os.Stdout)
		fmt.Fprintln(os.Stdout, message)
		fmt.Fprintln(os.Stdout)
	}

	return nil
}

func (v *View) clear() {
	fmt.Fprint(os.Stdout, "\033[2J\033[H") // ANSI clear screen
}

func (v *View) renderHeader(state *State) {
	info := state.reader.Info()
	fmt.Fprintln(os.Stdout, strings.Repeat("=", v.width))
	fmt.Fprintf(os.Stdout, "Object: %s\n", info.Path)
	fmt.Fprintf(os.Stdout, "Blocks: %d | Rows: %d | Cols: %d\n",
		info.BlockCount, info.RowCount, info.ColCount)
	fmt.Fprintln(os.Stdout, strings.Repeat("=", v.width))
}

func (v *View) renderTable(state *State) error {
	rows, rowNumbers, err := state.CurrentRows()
	if err != nil {
		return err
	}

	if v.verticalMode {
		return v.renderVertical(state, rows, rowNumbers)
	}

	cols := state.Columns()

	// For objmeta mode, only show Value column, so adjust displayCols
	var displayCols []uint16
	var displayColInfo []objecttool.ColInfo
	var widths []int

	if state.viewMode == ViewModeObjMeta {
		// Only show Value column (index 1), skip Property column (index 0)
		// For objmeta, cols should have 2 columns: [Property, Value]
		// We only want to display Value column in the data rows
		displayCols = []uint16{1} // Only Value column
		if len(cols) > 1 {
			displayColInfo = []objecttool.ColInfo{cols[1]} // Only Value column info
		} else {
			// Fallback if cols structure is unexpected
			displayColInfo = []objecttool.ColInfo{}
		}

		// Calculate width for both Property and Value columns
		// For objmeta, use a reasonable max width to avoid overly wide tables
		maxValueWidth := 80 // Reasonable max width for Value column in objmeta
		if state.maxColWidth > 0 && state.maxColWidth < maxValueWidth {
			maxValueWidth = state.maxColWidth
		}

		propertyWidth := len("Property")
		valueWidth := len("Value")

		// Use all metaRows to calculate width, not just current page
		allMetaRows := state.metaRows
		if len(allMetaRows) > 0 {
			for _, row := range allMetaRows {
				// Calculate Property column width (from row[0])
				if len(row) > 0 {
					propLen := len(row[0])
					if propLen > propertyWidth {
						propertyWidth = propLen
					}
				}
				// Calculate Value column width
				if len(row) > 1 {
					cellLen := len(row[1])
					if cellLen > maxValueWidth {
						cellLen = maxValueWidth
					}
					if cellLen > valueWidth {
						valueWidth = cellLen
					}
				}
			}
		}

		// Ensure minimum widths
		if propertyWidth < 8 {
			propertyWidth = 8
		}
		if valueWidth < 8 {
			valueWidth = 8
		}

		// Store both widths: [propertyWidth, valueWidth]
		// But for displayCols, we only use valueWidth (index 1)
		widths = []int{propertyWidth, valueWidth}
	} else {
		// Determine columns to display - table mode shows all columns but limits width
		displayCols = state.visibleCols
		if displayCols == nil {
			displayCols = make([]uint16, len(cols))
			for i := range cols {
				displayCols[i] = uint16(i)
			}
		}

		// Build display column info
		displayColInfo = make([]objecttool.ColInfo, len(displayCols))
		for i, colIdx := range displayCols {
			if int(colIdx) < len(cols) {
				displayColInfo[i] = cols[colIdx]
			}
		}

		// Calculate column widths normally
		widths = v.calcColWidths(state, displayColInfo, rows, state.maxColWidth)
	}

	// Table header
	v.renderTableHeader(state, displayColInfo, widths)

	// Data rows (with row numbers)
	for i, row := range rows {
		v.renderTableRowWithNumber(state, rowNumbers[i], row, displayCols, widths)
	}

	// Table footer
	if state.viewMode == ViewModeObjMeta {
		v.renderTableFooterObjMeta(widths)
	} else {
		v.renderTableFooter(widths)
	}

	// Render summary for BlkMeta mode (outside the table)
	if state.viewMode == ViewModeBlkMeta && state.blkSummary != nil {
		v.renderBlkSummary(state)
	}

	return nil
}

// renderVertical renders vertical display (similar to MySQL \G)
func (v *View) renderVertical(state *State, rows [][]string, rowNumbers []string) error {
	cols := state.Columns()

	// Determine columns to display
	displayCols := state.visibleCols
	if displayCols == nil {
		// Vertical mode displays all columns
		displayCols = make([]uint16, len(cols))
		for i := range cols {
			displayCols[i] = uint16(i)
		}
	}

	// Display each row
	for rowIdx, row := range rows {
		fmt.Fprintf(os.Stdout, "*************************** %s ***************************\n", rowNumbers[rowIdx])
		for i, colIdx := range displayCols {
			if int(colIdx) < len(cols) && int(colIdx) < len(row) {
				col := cols[colIdx]
				value := row[colIdx] // Always use colIdx now

				// Apply width limit in vertical mode
				if state.maxColWidth > 0 && len(value) > state.maxColWidth {
					value = value[:state.maxColWidth-3] + "..."
				}

				// Get column name
				var colName string
				if state.viewMode == ViewModeBlkMeta || state.viewMode == ViewModeObjMeta {
					if i < len(state.metaCols) {
						colName = state.metaCols[i].Name
					} else {
						colName = fmt.Sprintf("Col%d", colIdx)
					}
				} else {
					colName = state.getColumnName(colIdx)
				}

				fmt.Fprintf(os.Stdout, "%15s (%s): %s\n", col.Type.String(), colName, value)
			}
		}
		if rowIdx < len(rows)-1 {
			fmt.Fprintln(os.Stdout) // Empty line between rows
		}
	}

	return nil
}

func (v *View) renderTableRowWithNumber(state *State, rowNum string, row []string, displayCols []uint16, widths []int) {
	// For objmeta mode, first column shows Property (rowNum), then only Value (row[1])
	// rowNum is already the Property value, row[0] is Property name, row[1] is Value
	if state.viewMode == ViewModeObjMeta {
		// Get widths for Property and Value columns
		propertyWidth := 10
		valueWidth := 8
		if len(widths) >= 2 {
			propertyWidth = widths[0]
			valueWidth = widths[1]
		} else if len(widths) >= 1 {
			valueWidth = widths[0]
		}

		// Truncate Property name if too long
		propertyCell := rowNum
		if len(propertyCell) > propertyWidth {
			propertyCell = propertyCell[:propertyWidth-3] + "..."
		}

		fmt.Fprintf(os.Stdout, "│ %-*s │", propertyWidth, propertyCell)
		if len(row) > 1 {
			// Only show Value column (row[1]), skip row[0] which is Property name
			cell := row[1]
			if state.maxColWidth > 0 && len(cell) > state.maxColWidth {
				cell = cell[:state.maxColWidth-3] + "..."
			}
			fmt.Fprintf(os.Stdout, " %-*s │", valueWidth, cell)
		}
		fmt.Fprintln(os.Stdout)
		return
	}

	// For other modes
	{
		fmt.Fprintf(os.Stdout, "│ %-10s ", rowNum)
		for j, colIdx := range displayCols {
			if j < len(widths) && int(colIdx) < len(row) {
				cell := row[colIdx] // Always use colIdx now
				// Truncate if needed
				if state.maxColWidth > 0 && len(cell) > state.maxColWidth {
					cell = cell[:state.maxColWidth-3] + "..."
				}
				fmt.Fprintf(os.Stdout, "│ %-*s ", widths[j], cell)
			}
		}
		fmt.Fprintln(os.Stdout, "│")
	}
}

func (v *View) renderStatus(state *State) {
	info := state.reader.Info()
	start := state.GlobalRowOffset() + 1
	rows, _, _ := state.CurrentRows()
	end := start + int64(len(rows)) - 1

	// Display mode
	mode := "Table"
	if state.verticalMode {
		mode = "Vertical"
	}

	// View mode prefix
	switch state.viewMode {
	case ViewModeBlkMeta:
		mode = "BlkMeta-" + mode
	case ViewModeObjMeta:
		mode = "ObjMeta-" + mode
	}

	// Width setting
	widthStr := fmt.Sprintf("%d", state.maxColWidth)
	if state.maxColWidth == 0 {
		widthStr = "unlimited"
	}

	// Different status bar for metadata modes
	if state.viewMode == ViewModeBlkMeta || state.viewMode == ViewModeObjMeta {
		fmt.Fprintf(os.Stdout, "\n[%d-%d of %d] Mode: %s | Width: %s\n",
			start, end, len(state.metaRows), mode, widthStr)
	} else {
		fmt.Fprintf(os.Stdout, "\n[%d-%d of %d] Block %d/%d | Mode: %s | Width: %s\n",
			start, end, info.RowCount, state.currentBlock+1, info.BlockCount, mode, widthStr)
	}
}

func (v *View) calcColWidths(state *State, cols []objecttool.ColInfo, rows [][]string, maxColWidth int) []int {
	widths := make([]int, len(cols))

	// Initial width: column name length
	for i, col := range cols {
		var colName string
		if state.viewMode == ViewModeBlkMeta || state.viewMode == ViewModeObjMeta {
			// Metadata mode: use metaCols Name by array index
			if i < len(state.metaCols) {
				colName = state.metaCols[i].Name
			} else {
				colName = fmt.Sprintf("Col%d", col.Idx)
			}
		} else {
			// Data mode: use renamed name or default
			colName = state.getColumnName(col.Idx)
		}
		widths[i] = len(colName)
		if widths[i] < 8 {
			widths[i] = 8
		}
	}

	// Adjust based on data
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) {
				cellLen := len(cell)
				// If user set width limit, use that value; otherwise use content length
				if maxColWidth > 0 && cellLen > maxColWidth {
					cellLen = maxColWidth
				}
				if cellLen > widths[i] {
					widths[i] = cellLen
				}
			}
		}
	}

	return widths
}

func (v *View) renderTableHeader(state *State, cols []objecttool.ColInfo, widths []int) {
	// For objmeta mode, only two columns: Property and Value
	if state.viewMode == ViewModeObjMeta {
		// Get widths for Property and Value columns
		propertyWidth := 10
		valueWidth := 8
		if len(widths) >= 2 {
			propertyWidth = widths[0]
			valueWidth = widths[1]
		} else if len(widths) >= 1 {
			valueWidth = widths[0]
		}

		// ┌────────────┬─────────────────────────────────────────────────┐
		fmt.Fprint(os.Stdout, "┌")
		fmt.Fprint(os.Stdout, strings.Repeat("─", propertyWidth+2))
		fmt.Fprint(os.Stdout, "┬")
		fmt.Fprint(os.Stdout, strings.Repeat("─", valueWidth+2))
		fmt.Fprintln(os.Stdout, "┐")

		// │Property│Value│
		fmt.Fprintf(os.Stdout, "│ %-*s │", propertyWidth, "Property")
		fmt.Fprintf(os.Stdout, " %-*s │", valueWidth, "Value")
		fmt.Fprintln(os.Stdout)

		// ├────────────┼─────────────────────────────────────────────────┤
		fmt.Fprint(os.Stdout, "├")
		fmt.Fprint(os.Stdout, strings.Repeat("─", propertyWidth+2))
		fmt.Fprint(os.Stdout, "┼")
		fmt.Fprint(os.Stdout, strings.Repeat("─", valueWidth+2))
		fmt.Fprintln(os.Stdout, "┤")
		return
	}

	// ┌────┬────┐
	fmt.Fprint(os.Stdout, "┌────────────")
	for _, w := range widths {
		fmt.Fprint(os.Stdout, "┬")
		fmt.Fprint(os.Stdout, strings.Repeat("─", w+2))
	}
	fmt.Fprintln(os.Stdout, "┐")

	// For other modes
	{
		// First column name depends on view mode
		var firstColName string
		switch state.viewMode {
		case ViewModeBlkMeta:
			firstColName = "BlockNo"
		case ViewModeObjMeta:
			firstColName = "Property"
		default:
			firstColName = "RowNum"
		}

		// │RowNum│Col0│Col1│
		fmt.Fprintf(os.Stdout, "│ %-10s ", firstColName)

		// For objmeta mode, don't iterate through cols to avoid duplicate headers
		if state.viewMode != ViewModeObjMeta {
			for i, col := range cols {
				if i >= len(widths) {
					break
				}
				// Get column name based on view mode
				var header string
				if state.viewMode == ViewModeBlkMeta {
					// Metadata mode: use metaCols Name by array index
					if i < len(state.metaCols) {
						header = state.metaCols[i].Name
					} else {
						header = fmt.Sprintf("Col%d", col.Idx)
					}
				} else {
					// Data mode: check for renamed columns
					header = state.getColumnName(col.Idx)
				}
				fmt.Fprintf(os.Stdout, "│ %-*s ", widths[i], header)
			}
		} else {
			// For objmeta mode, only show Value column
			if len(widths) > 0 {
				fmt.Fprintf(os.Stdout, "│ %-*s ", widths[0], "Value")
			}
		}
		fmt.Fprintln(os.Stdout, "│")
	}

	// ├────┼────┤
	fmt.Fprint(os.Stdout, "├────────────")
	for _, w := range widths {
		fmt.Fprint(os.Stdout, "┼")
		fmt.Fprint(os.Stdout, strings.Repeat("─", w+2))
	}
	fmt.Fprintln(os.Stdout, "┤")
}

func (v *View) renderTableFooter(widths []int) {
	// └────┴────┘
	fmt.Fprint(os.Stdout, "└────────────")
	for _, w := range widths {
		fmt.Fprint(os.Stdout, "┴")
		fmt.Fprint(os.Stdout, strings.Repeat("─", w+2))
	}
	fmt.Fprintln(os.Stdout, "┘")
}

func (v *View) renderTableFooterObjMeta(widths []int) {
	propertyWidth := 10
	valueWidth := 8
	if len(widths) >= 2 {
		propertyWidth = widths[0]
		valueWidth = widths[1]
	} else if len(widths) >= 1 {
		valueWidth = widths[0]
	}

	// └────────────┴─────────────────────────────────────────────────┘
	fmt.Fprint(os.Stdout, "└")
	fmt.Fprint(os.Stdout, strings.Repeat("─", propertyWidth+2))
	fmt.Fprint(os.Stdout, "┴")
	fmt.Fprint(os.Stdout, strings.Repeat("─", valueWidth+2))
	fmt.Fprintln(os.Stdout, "┘")
}

func (v *View) renderBlkSummary(state *State) {
	if len(state.blkSummary) == 0 {
		return
	}

	info := state.reader.Info()
	fmt.Fprintln(os.Stdout)
	fmt.Fprintln(os.Stdout, "Block Summary:")

	for blockID := uint32(0); blockID < info.BlockCount; blockID++ {
		if summary, exists := state.blkSummary[blockID]; exists {
			fmt.Fprintf(os.Stdout, "  Block %d:\n", blockID)
			fmt.Fprintf(os.Stdout, "    Origin Size:       %s (%d bytes)\n", formatSize(uint64(summary.TotalOriginSize)), summary.TotalOriginSize)
			fmt.Fprintf(os.Stdout, "    Compressed Size:   %s (%d bytes)\n", formatSize(uint64(summary.TotalCompSize)), summary.TotalCompSize)
			fmt.Fprintf(os.Stdout, "    Compression Ratio: %.2f%%\n", summary.CompressionRatio)
		}
	}
}
