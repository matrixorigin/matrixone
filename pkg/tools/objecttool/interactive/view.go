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

	cols := state.reader.Columns()

	// Determine columns to display - table mode shows all columns but limits width
	displayCols := state.visibleCols
	if displayCols == nil {
		displayCols = make([]uint16, len(cols))
		for i := range cols {
			displayCols[i] = uint16(i)
		}
	}

	// Build display column info
	displayColInfo := make([]objecttool.ColInfo, len(displayCols))
	for i, colIdx := range displayCols {
		if int(colIdx) < len(cols) {
			displayColInfo[i] = cols[colIdx]
		}
	}

	// Calculate column widths
	widths := v.calcColWidths(displayColInfo, rows, state.maxColWidth)

	// Table header
	v.renderTableHeader(displayColInfo, widths)

	// Data rows (with row numbers)
	for i, row := range rows {
		v.renderTableRowWithNumber(rowNumbers[i], row, widths)
	}

	// Table footer
	v.renderTableFooter(widths)

	return nil
}

// renderVertical renders vertical display (similar to MySQL \G)
func (v *View) renderVertical(state *State, rows [][]string, rowNumbers []string) error {
	cols := state.reader.Columns()

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
			if int(colIdx) < len(cols) {
				col := cols[colIdx]
				value := ""
				if i < len(row) {
					value = row[i]
					// Apply width limit in vertical mode
					if state.maxColWidth > 0 && len(value) > state.maxColWidth {
						value = value[:state.maxColWidth-3] + "..."
					}
				}
				fmt.Fprintf(os.Stdout, "%15s (Col%d): %s\n", col.Type.String(), colIdx, value)
			}
		}
		if rowIdx < len(rows)-1 {
			fmt.Fprintln(os.Stdout) // Empty line between rows
		}
	}

	return nil
}

func (v *View) renderTableRowWithNumber(rowNum string, row []string, widths []int) {
	fmt.Fprintf(os.Stdout, "│ %-10s ", rowNum)
	for i, cell := range row {
		if i < len(widths) {
			// Don't truncate here, let content display fully
			fmt.Fprintf(os.Stdout, "│ %-*s ", widths[i], cell)
		}
	}
	fmt.Fprintln(os.Stdout, "│")
}

func (v *View) renderStatusToBuilder(state *State, b *strings.Builder) {
	info := state.reader.Info()
	start := state.GlobalRowOffset() + 1
	rows, _, _ := state.CurrentRows()
	end := start + int64(len(rows)) - 1

	mode := "Table"
	if state.verticalMode {
		mode = "Vertical"
	}

	widthStr := fmt.Sprintf("%d", state.maxColWidth)
	if state.maxColWidth == 0 {
		widthStr = "unlimited"
	}

	fmt.Fprintf(b, "\n[%d-%d of %d] Block %d/%d | Mode: %s | Width: %s",
		start, end, info.RowCount, state.currentBlock+1, info.BlockCount, mode, widthStr)
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

	// Width setting
	widthStr := fmt.Sprintf("%d", state.maxColWidth)
	if state.maxColWidth == 0 {
		widthStr = "unlimited"
	}

	fmt.Fprintf(os.Stdout, "\n[%d-%d of %d] Block %d/%d | Mode: %s | Width: %s\n",
		start, end, info.RowCount, state.currentBlock+1, info.BlockCount, mode, widthStr)
}

func (v *View) calcColWidths(cols []objecttool.ColInfo, rows [][]string, maxColWidth int) []int {
	widths := make([]int, len(cols))

	// Initial width: column name length
	for i, col := range cols {
		widths[i] = len(fmt.Sprintf("Col%d", col.Idx))
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

func (v *View) renderTableHeader(cols []objecttool.ColInfo, widths []int) {
	// ┌────┬────┐
	fmt.Fprint(os.Stdout, "┌────────────┬")
	for i, w := range widths {
		fmt.Fprint(os.Stdout, strings.Repeat("─", w+2))
		if i < len(widths)-1 {
			fmt.Fprint(os.Stdout, "┬")
		}
	}
	fmt.Fprintln(os.Stdout, "┐")

	// │RowNum│Col0│Col1│
	fmt.Fprint(os.Stdout, "│ RowNum     │")
	for i, col := range cols {
		header := fmt.Sprintf("Col%d", col.Idx)
		fmt.Fprintf(os.Stdout, " %-*s │", widths[i], header)
	}
	fmt.Fprintln(os.Stdout)

	// ├────┼────┤
	fmt.Fprint(os.Stdout, "├────────────┼")
	for i, w := range widths {
		fmt.Fprint(os.Stdout, strings.Repeat("─", w+2))
		if i < len(widths)-1 {
			fmt.Fprint(os.Stdout, "┼")
		}
	}
	fmt.Fprintln(os.Stdout, "┤")
}

func (v *View) renderTableRow(row []string, widths []int) {
	fmt.Fprint(os.Stdout, "│")
	for i, cell := range row {
		if i < len(widths) {
			// Truncate overly long content
			if len(cell) > widths[i] {
				cell = cell[:widths[i]-3] + "..."
			}
			fmt.Fprintf(os.Stdout, " %-*s │", widths[i], cell)
		}
	}
	fmt.Fprintln(os.Stdout)
}

func (v *View) renderTableFooter(widths []int) {
	// └────┴────┘
	fmt.Fprint(os.Stdout, "└────────────┴")
	for i, w := range widths {
		fmt.Fprint(os.Stdout, strings.Repeat("─", w+2))
		if i < len(widths)-1 {
			fmt.Fprint(os.Stdout, "┴")
		}
	}
	fmt.Fprintln(os.Stdout, "┘")
}
