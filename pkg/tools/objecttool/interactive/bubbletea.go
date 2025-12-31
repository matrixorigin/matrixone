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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
)

type model struct {
	state         *State
	message       string
	cmdMode       bool
	cmdInput      string
	cmdCursor     int // Command line cursor position
	cmdHistory    []string
	historyIndex  int
	hScrollOffset int // Horizontal scroll offset

	// Search related
	searchTerm   string
	searchRegex  *regexp.Regexp
	useRegex     bool
	currentMatch SearchMatch
	hasMatch     bool
}

type SearchMatch struct {
	Row        int64
	Col        int
	Value      string
	RowNum     string // Row number display (block-offset)
	ColumnName string // Column name
}

func (m model) Init() tea.Cmd {
	return nil
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if m.cmdMode {
			return m.handleCommandInput(msg)
		}
		return m.handleBrowseMode(msg)
	}
	return m, nil
}

func (m model) handleBrowseMode(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	m.message = ""
	switch msg.String() {
	case "q", "ctrl+c":
		return m, tea.Quit
	case "j", "down":
		m.state.ScrollDown()
	case "k", "up":
		m.state.ScrollUp()
	case "g":
		m.state.GotoRow(0)
	case "G":
		m.state.GotoRow(-1)
	case "ctrl+f", "pgdown", "enter":
		m.state.NextPage()
	case "ctrl+b", "pgup":
		m.state.PrevPage()
	case "left", "h":
		// Scroll left horizontally
		if m.hScrollOffset > 0 {
			m.hScrollOffset--
		}
	case "right", "l":
		// Scroll right horizontally
		cols := m.state.reader.Columns()
		if m.hScrollOffset < len(cols)-1 {
			m.hScrollOffset++
		}
	case ":":
		m.cmdMode = true
		m.cmdInput = ""
		m.cmdCursor = 0
		m.historyIndex = len(m.cmdHistory)
	case "/":
		m.cmdMode = true
		m.cmdInput = "search "
		m.cmdCursor = len(m.cmdInput)
		m.historyIndex = len(m.cmdHistory)
	case "n":
		// Next search result
		if m.searchTerm != "" {
			m.findNextMatch()
		}
	case "N":
		// Previous search result
		if m.searchTerm != "" {
			m.findPrevMatch()
		}
	case "?":
		m.message = generalHelp
	default:
		// Support :N quick jump
		if len(msg.String()) == 1 && msg.String()[0] >= '0' && msg.String()[0] <= '9' {
			m.cmdMode = true
			m.cmdInput = msg.String()
			m.cmdCursor = 1
		}
	}
	return m, nil
}

func (m model) handleCommandInput(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "enter":
		m.cmdMode = false
		if m.cmdInput == "" {
			return m, nil
		}
		cmd, err := ParseCommand(":" + m.cmdInput)
		if err != nil {
			m.message = fmt.Sprintf("Error: %v", err)
			return m, nil
		}
		if cmd == nil {
			return m, nil
		}

		// Add to history
		m.cmdHistory = append(m.cmdHistory, m.cmdInput)
		m.historyIndex = len(m.cmdHistory)

		// Special handling for search command
		if searchCmd, ok := cmd.(*SearchCommand); ok {
			m.startSearch(searchCmd.Pattern)
			m.cmdMode = false
			m.cmdInput = ""
			m.cmdCursor = 0
			return m, nil
		}

		output, quit, err := cmd.Execute(m.state)
		if err != nil {
			m.message = fmt.Sprintf("Error: %v", err)
			return m, nil
		}
		if quit {
			return m, tea.Quit
		}

		// Reset horizontal scroll when switching view modes
		if _, ok := cmd.(*SwitchModeCommand); ok {
			m.hScrollOffset = 0
		}

		m.message = output
		m.cmdInput = ""
		m.cmdCursor = 0
		return m, nil
	case "up":
		// History up
		if len(m.cmdHistory) > 0 && m.historyIndex > 0 {
			m.historyIndex--
			m.cmdInput = m.cmdHistory[m.historyIndex]
			m.cmdCursor = len(m.cmdInput)
		}
		return m, nil
	case "down":
		// History down
		if len(m.cmdHistory) > 0 && m.historyIndex < len(m.cmdHistory)-1 {
			m.historyIndex++
			m.cmdInput = m.cmdHistory[m.historyIndex]
			m.cmdCursor = len(m.cmdInput)
		} else if m.historyIndex == len(m.cmdHistory)-1 {
			m.historyIndex = len(m.cmdHistory)
			m.cmdInput = ""
			m.cmdCursor = 0
		}
		return m, nil
	case "left":
		// Move cursor left
		if m.cmdCursor > 0 {
			m.cmdCursor--
		}
		return m, nil
	case "right":
		// Move cursor right
		if m.cmdCursor < len(m.cmdInput) {
			m.cmdCursor++
		}
		return m, nil
	case "esc", "ctrl+c":
		m.cmdMode = false
		m.cmdInput = ""
		m.cmdCursor = 0
		m.historyIndex = len(m.cmdHistory)
	case "ctrl+a":
		// Move cursor to beginning of line
		m.cmdCursor = 0
	case "ctrl+e":
		// Move cursor to end of line
		m.cmdCursor = len(m.cmdInput)
	case "ctrl+u":
		// Clear current input
		m.cmdInput = ""
		m.cmdCursor = 0
	case "ctrl+k":
		// Delete from cursor to end of line
		m.cmdInput = m.cmdInput[:m.cmdCursor]
	case "ctrl+w":
		// Delete word before cursor
		if m.cmdCursor > 0 {
			// Find previous space or beginning of line
			pos := m.cmdCursor - 1
			for pos > 0 && m.cmdInput[pos] == ' ' {
				pos--
			}
			for pos > 0 && m.cmdInput[pos] != ' ' {
				pos--
			}
			if pos > 0 {
				pos++
			}
			m.cmdInput = m.cmdInput[:pos] + m.cmdInput[m.cmdCursor:]
			m.cmdCursor = pos
		}
	case "backspace":
		if m.cmdCursor > 0 {
			m.cmdInput = m.cmdInput[:m.cmdCursor-1] + m.cmdInput[m.cmdCursor:]
			m.cmdCursor--
		}
	case "tab":
		// Tab completion
		if m.cmdInput != "" {
			completed := m.completeCommand(m.cmdInput)
			if completed != "" && completed != m.cmdInput {
				m.cmdInput = completed
				m.cmdCursor = len(completed)
			}
		}
		return m, nil
	default:
		// Support paste and multi-character input
		input := msg.String()

		// Handle paste content, remove possible bracket wrapping
		if strings.HasPrefix(input, "[") && strings.HasSuffix(input, "]") && len(input) > 2 {
			input = input[1 : len(input)-1]
		}

		// Filter out control characters, keep only printable characters and spaces
		var filtered strings.Builder
		for _, r := range input {
			if r >= 32 && r <= 126 { // Printable ASCII characters including space
				filtered.WriteRune(r)
			}
		}
		if filtered.Len() > 0 {
			text := filtered.String()
			// Insert text at cursor position
			m.cmdInput = m.cmdInput[:m.cmdCursor] + text + m.cmdInput[m.cmdCursor:]
			m.cmdCursor += len(text)
		}
	}
	return m, nil
}

func (m model) View() string {
	var b strings.Builder

	// Data table - fills screen, but only renders when there's a valid reader
	if m.state != nil && m.state.reader != nil {
		if m.state.verticalMode {
			m.renderVertical(&b)
		} else {
			m.renderTable(&b)
		}
	} else {
		// Show placeholder when no data
		b.WriteString("No data available\n")
	}

	// Message display (if any)
	if m.message != "" {
		b.WriteString("\n► ")
		b.WriteString(m.message)
		b.WriteString("\n")
	}

	// Bottom status bar - always at the bottom
	if m.state != nil && m.state.reader != nil {
		info := m.state.reader.Info()
		rows, _, _ := m.state.CurrentRows()
		start := m.state.GlobalRowOffset() + 1
		end := start + int64(len(rows)) - 1
		mode := "Table"
		if m.state.verticalMode {
			mode = "Vertical"
		}

		cols := m.state.reader.Columns()
		visibleCols := m.getVisibleColumns(cols)
		termWidth := 120

		// Status bar colors - dark gray background, white text
		bgColor := "\033[100m"  // Dark gray background
		textColor := "\033[97m" // Bright white text
		reset := "\033[0m"      // Reset color

		// Status information - show filtered row count if range is set
		totalRows := int64(info.RowCount)
		rangeInfo := ""
		if m.state.rowRangeStart >= 0 || m.state.rowRangeEnd >= 0 {
			totalRows = m.state.FilteredRowCount()
			rangeInfo = fmt.Sprintf(" [Range: %d-%d]", m.state.rowRangeStart, m.state.rowRangeEnd)
		}
		statusText := fmt.Sprintf(" %s │ Rows %d-%d/%d%s │ Block %d/%d │ Cols %d-%d/%d ",
			mode, start, end, totalRows, rangeInfo, m.state.currentBlock+1, info.BlockCount,
			m.hScrollOffset+1, m.hScrollOffset+len(visibleCols), len(cols))

		// Calculate number of spaces to fill
		padding := termWidth - len(statusText)
		if padding < 0 {
			padding = 0
		}

		// Colored status bar - fills entire line
		b.WriteString("\n")
		b.WriteString(bgColor + textColor)
		b.WriteString(statusText)
		b.WriteString(strings.Repeat(" ", padding))
		b.WriteString(reset)
	} else {
		// Simple status bar
		b.WriteString("\nNo data loaded")
	}

	// Command line - normal style, no background color
	b.WriteString("\n")
	if m.cmdMode {
		b.WriteString(":")
		// Insert cursor character at cursor position
		if m.cmdCursor <= len(m.cmdInput) {
			before := m.cmdInput[:m.cmdCursor]
			after := m.cmdInput[m.cmdCursor:]
			b.WriteString(before)
			b.WriteString("█")
			b.WriteString(after)
		} else {
			b.WriteString(m.cmdInput)
			b.WriteString("█")
		}
	}

	return b.String()
}

// gotoSearchMatch jumps to current search match
func (m *model) gotoSearchMatch() {
	if !m.hasMatch {
		return
	}

	match := m.currentMatch
	// Jump to matching row
	m.state.GotoRow(match.Row)

	// Adjust horizontal scroll to show matching column
	cols := m.state.reader.Columns()
	if match.Col < len(cols) {
		visibleCols := m.getVisibleColumns(cols)
		if match.Col < m.hScrollOffset || match.Col >= m.hScrollOffset+len(visibleCols) {
			m.hScrollOffset = match.Col
			if m.hScrollOffset > len(cols)-5 {
				m.hScrollOffset = len(cols) - 5
			}
			if m.hScrollOffset < 0 {
				m.hScrollOffset = 0
			}
		}
	}

	m.message = fmt.Sprintf("Match at %s [%s]: %s", match.RowNum, match.ColumnName, match.Value)
}

// startSearch starts search
func (m *model) startSearch(pattern string) {
	m.searchTerm = pattern
	m.hasMatch = false

	// Try to compile regex
	m.useRegex = false
	if strings.ContainsAny(pattern, ".*+?^${}[]|()\\") {
		regex, err := regexp.Compile("(?i)" + pattern)
		if err == nil {
			m.useRegex = true
			m.searchRegex = regex
		}
	}

	// Find first match from current position
	m.findNextMatch()
}

// findNextMatch finds next match (from current position backward)
func (m *model) findNextMatch() {
	if m.searchTerm == "" {
		return
	}

	currentRow := m.state.GlobalRowOffset()
	currentBlock := m.state.currentBlock
	blockCount := m.state.reader.BlockCount()

	// Start search from next row of current position
	startRow := currentRow + 1

	// Search from current position to end of file
	for blockIdx := currentBlock; blockIdx < blockCount; blockIdx++ {
		batch, release, err := m.state.reader.ReadBlock(m.state.ctx, blockIdx)
		if err != nil {
			continue
		}

		rowCount := batch.RowCount()
		cols := m.state.reader.Columns()

		// Calculate starting row number of this block
		blockStartRow := int64(0)
		for i := uint32(0); i < blockIdx; i++ {
			b, r, e := m.state.reader.ReadBlock(m.state.ctx, i)
			if e == nil {
				blockStartRow += int64(b.RowCount())
				r()
			}
		}

		// Determine search start row
		startRowInBlock := 0
		if blockIdx == currentBlock {
			startRowInBlock = int(startRow - blockStartRow)
			if startRowInBlock < 0 {
				startRowInBlock = 0
			}
		}

		// Apply row range filter
		if m.state.rowRangeStart >= 0 && startRowInBlock < int(m.state.rowRangeStart) {
			startRowInBlock = int(m.state.rowRangeStart)
		}
		endRowInBlock := rowCount
		if m.state.rowRangeEnd >= 0 && endRowInBlock > int(m.state.rowRangeEnd)+1 {
			endRowInBlock = int(m.state.rowRangeEnd) + 1
		}

		// Search this block
		for rowIdx := startRowInBlock; rowIdx < endRowInBlock; rowIdx++ {
			for colIdx := 0; colIdx < batch.VectorCount(); colIdx++ {
				if colIdx >= len(cols) {
					continue
				}

				vec := batch.GetVector(int32(colIdx))
				if vec.IsNull(uint64(rowIdx)) {
					continue
				}

				cell := m.formatCell(vec, rowIdx, cols[colIdx])

				if m.matchPattern(cell) {
					rowNum := fmt.Sprintf("(%d-%d)", blockIdx, rowIdx)
					colName := fmt.Sprintf("Col%d", colIdx)
					if colIdx < len(cols) {
						colName = fmt.Sprintf("Col%d", cols[colIdx].Idx)
					}

					m.currentMatch = SearchMatch{
						Row:        blockStartRow + int64(rowIdx),
						Col:        colIdx,
						Value:      cell,
						RowNum:     rowNum,
						ColumnName: colName,
					}
					m.hasMatch = true
					release()
					m.gotoSearchMatch()
					return
				}
			}
		}

		release()
	}

	m.message = "No more matches"
}

// findPrevMatch finds previous match (from current position forward)
func (m *model) findPrevMatch() {
	if m.searchTerm == "" {
		return
	}

	currentRow := m.state.GlobalRowOffset()
	currentBlock := m.state.currentBlock

	// Start search from previous row of current position
	endRow := currentRow - 1
	if endRow < 0 {
		m.message = "No previous matches"
		return
	}

	// Search forward from current block
	for blockIdx := int(currentBlock); blockIdx >= 0; blockIdx-- {
		batch, release, err := m.state.reader.ReadBlock(m.state.ctx, uint32(blockIdx))
		if err != nil {
			continue
		}

		rowCount := batch.RowCount()
		cols := m.state.reader.Columns()

		// Calculate starting row number of this block
		blockStartRow := int64(0)
		for i := uint32(0); i < uint32(blockIdx); i++ {
			b, r, e := m.state.reader.ReadBlock(m.state.ctx, i)
			if e == nil {
				blockStartRow += int64(b.RowCount())
				r()
			}
		}

		// Determine search end row
		endRowInBlock := rowCount - 1
		if uint32(blockIdx) == currentBlock {
			endRowInBlock = int(endRow - blockStartRow)
			if endRowInBlock >= rowCount {
				endRowInBlock = rowCount - 1
			}
		}

		// Apply row range filter
		startRowInBlock := 0
		if m.state.rowRangeStart >= 0 {
			startRowInBlock = int(m.state.rowRangeStart)
		}
		if m.state.rowRangeEnd >= 0 && endRowInBlock > int(m.state.rowRangeEnd) {
			endRowInBlock = int(m.state.rowRangeEnd)
		}

		// Search this block from back to front
		for rowIdx := endRowInBlock; rowIdx >= startRowInBlock; rowIdx-- {
			for colIdx := batch.VectorCount() - 1; colIdx >= 0; colIdx-- {
				if colIdx >= len(cols) {
					continue
				}

				vec := batch.GetVector(int32(colIdx))
				if vec.IsNull(uint64(rowIdx)) {
					continue
				}

				cell := m.formatCell(vec, rowIdx, cols[colIdx])

				if m.matchPattern(cell) {
					rowNum := fmt.Sprintf("(%d-%d)", blockIdx, rowIdx)
					colName := fmt.Sprintf("Col%d", colIdx)
					if colIdx < len(cols) {
						colName = fmt.Sprintf("Col%d", cols[colIdx].Idx)
					}

					m.currentMatch = SearchMatch{
						Row:        blockStartRow + int64(rowIdx),
						Col:        colIdx,
						Value:      cell,
						RowNum:     rowNum,
						ColumnName: colName,
					}
					m.hasMatch = true
					release()
					m.gotoSearchMatch()
					return
				}
			}
		}

		release()
	}

	m.message = "No previous matches"
}

// formatCell formats cell value
func (m *model) formatCell(vec *vector.Vector, rowIdx int, col objecttool.ColInfo) string {
	var value any
	switch col.Type.Oid {
	case types.T_bool:
		value = vector.GetFixedAtNoTypeCheck[bool](vec, rowIdx)
	case types.T_int8:
		value = vector.GetFixedAtNoTypeCheck[int8](vec, rowIdx)
	case types.T_int16:
		value = vector.GetFixedAtNoTypeCheck[int16](vec, rowIdx)
	case types.T_int32:
		value = vector.GetFixedAtNoTypeCheck[int32](vec, rowIdx)
	case types.T_int64:
		value = vector.GetFixedAtNoTypeCheck[int64](vec, rowIdx)
	case types.T_uint8:
		value = vector.GetFixedAtNoTypeCheck[uint8](vec, rowIdx)
	case types.T_uint16:
		value = vector.GetFixedAtNoTypeCheck[uint16](vec, rowIdx)
	case types.T_uint32:
		value = vector.GetFixedAtNoTypeCheck[uint32](vec, rowIdx)
	case types.T_uint64:
		value = vector.GetFixedAtNoTypeCheck[uint64](vec, rowIdx)
	case types.T_float32:
		value = vector.GetFixedAtNoTypeCheck[float32](vec, rowIdx)
	case types.T_float64:
		value = vector.GetFixedAtNoTypeCheck[float64](vec, rowIdx)
	case types.T_varchar, types.T_char, types.T_text, types.T_json:
		value = vec.GetBytesAt(rowIdx)
	case types.T_blob:
		value = vec.GetBytesAt(rowIdx)
	case types.T_uuid:
		value = vector.GetFixedAtNoTypeCheck[types.Uuid](vec, rowIdx)
	case types.T_date:
		value = vector.GetFixedAtNoTypeCheck[types.Date](vec, rowIdx)
	case types.T_datetime:
		value = vector.GetFixedAtNoTypeCheck[types.Datetime](vec, rowIdx)
	case types.T_time:
		value = vector.GetFixedAtNoTypeCheck[types.Time](vec, rowIdx)
	case types.T_timestamp:
		value = vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, rowIdx)
	case types.T_decimal64:
		value = vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, rowIdx)
	case types.T_decimal128:
		value = vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, rowIdx)
	default:
		if col.Type.IsFixedLen() {
			value = fmt.Sprintf("<binary:%d bytes>", col.Type.TypeSize())
		} else {
			value = vec.GetBytesAt(rowIdx)
		}
	}

	formatter := m.state.formatter.GetFormatter(col.Idx, col.Type, value)
	return formatter.Format(value)
}

// matchPattern checks if matches search pattern
func (m *model) matchPattern(cell string) bool {
	if m.useRegex {
		return m.searchRegex.MatchString(cell)
	}

	// Plain text search (case insensitive)
	matched := strings.Contains(strings.ToLower(cell), strings.ToLower(m.searchTerm))

	// If no match and cell looks like hex, try decoding and searching
	if !matched && len(cell) > 0 && len(cell)%2 == 0 {
		isHex := true
		for _, c := range cell {
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
				isHex = false
				break
			}
		}

		if isHex {
			decoded := make([]byte, len(cell)/2)
			for i := 0; i < len(cell); i += 2 {
				var b byte
				fmt.Sscanf(cell[i:i+2], "%02x", &b)
				decoded[i/2] = b
			}
			decodedStr := string(decoded)
			matched = strings.Contains(strings.ToLower(decodedStr), strings.ToLower(m.searchTerm))
		}
	}

	return matched
}

// highlightSearchMatch highlights search matched text
func (m model) highlightSearchMatch(text string, row int64, col int) string {
	if m.searchTerm == "" {
		return text
	}

	// Check if it's current match
	isCurrentMatch := m.hasMatch && m.currentMatch.Row == row && m.currentMatch.Col == col

	// Highlight colors
	currentColor := "\033[41m\033[97m" // Red background, white text (current match)
	reset := "\033[0m"

	// Simple match highlighting
	if isCurrentMatch {
		return currentColor + text + reset
	}

	return text
}

func (m model) renderTable(b *strings.Builder) {
	rows, rowNumbers, _ := m.state.CurrentRows()
	if len(rows) == 0 {
		b.WriteString("No data\n")
		return
	}

	cols := m.state.Columns()

	// Determine columns to display - based on horizontal scroll offset and terminal width
	displayCols := m.getVisibleColumns(cols)

	// In metadata mode, limit displayCols to actual data columns
	if m.state.viewMode == ViewModeBlkMeta || m.state.viewMode == ViewModeObjMeta {
		if len(rows) > 0 {
			maxCols := len(rows[0])
			if len(displayCols) > maxCols {
				displayCols = displayCols[:maxCols]
			}
			// For blkmeta mode, if displayCols is empty, show all columns
			if m.state.viewMode == ViewModeBlkMeta && len(displayCols) == 0 && maxCols > 0 {
				displayCols = make([]uint16, maxCols)
				for i := 0; i < maxCols; i++ {
					displayCols[i] = uint16(i)
				}
			}
		}
	}

	// For objmeta mode, calculate Property column width separately
	var propertyWidth int
	if m.state.viewMode == ViewModeObjMeta {
		propertyWidth = len("Property")
		// Calculate max width from all property names (stored in rowNumbers)
		for _, propName := range rowNumbers {
			if len(propName) > propertyWidth {
				propertyWidth = len(propName)
			}
		}
		if propertyWidth < 8 {
			propertyWidth = 8
		}
	} else {
		propertyWidth = 10 // Default width for RowNum/BlockNo
	}

	// Calculate column width - use user setting or actual content length
	widths := make([]int, len(displayCols))
	for i, colIdx := range displayCols {
		if int(colIdx) < len(cols) {
			// For blkmeta mode, use metaCols Name as initial width
			if m.state.viewMode == ViewModeBlkMeta && int(colIdx) < len(m.state.metaCols) {
				widths[i] = len(m.state.metaCols[colIdx].Name)
			} else if m.state.colNames != nil {
				// Use custom column name if set
				if customName, exists := m.state.colNames[colIdx]; exists {
					widths[i] = len(customName)
				} else {
					widths[i] = len(fmt.Sprintf("Col%d", colIdx))
				}
			} else {
				widths[i] = len(fmt.Sprintf("Col%d", colIdx))
			}
			if widths[i] < 8 {
				widths[i] = 8
			}
		}
	}

	// Adjust column width based on data
	for _, row := range rows {
		for i, colIdx := range displayCols {
			if int(colIdx) < len(row) {
				cellLen := len(row[colIdx])
				// If user set width limit, use that value; otherwise use content length
				if m.state.maxColWidth > 0 {
					if cellLen > m.state.maxColWidth {
						cellLen = m.state.maxColWidth
					}
				}
				if cellLen > widths[i] {
					widths[i] = cellLen
				}
			}
		}
	}

	// Table header
	b.WriteString("┌")
	b.WriteString(strings.Repeat("─", propertyWidth+2))
	b.WriteString("┬")
	for i, w := range widths {
		b.WriteString(strings.Repeat("─", w+2))
		if i < len(widths)-1 {
			b.WriteString("┬")
		}
	}
	b.WriteString("┐\n")

	// Column headers
	// First column name depends on view mode
	var firstColName string
	switch m.state.viewMode {
	case ViewModeBlkMeta:
		firstColName = "BlockNo"
	case ViewModeObjMeta:
		firstColName = "Property"
	default:
		firstColName = "RowNum"
	}

	fmt.Fprintf(b, "│ %-*s │", propertyWidth, firstColName)

	// For objmeta mode, only show Value column to avoid duplicate Property column
	if m.state.viewMode == ViewModeObjMeta {
		// Only show Value column
		if len(widths) > 0 {
			fmt.Fprintf(b, " %-*s │", widths[0], "Value")
		}
	} else {
		// For other modes, show all columns
		for i, colIdx := range displayCols {
			if int(colIdx) < len(cols) {
				var header string
				if m.state.viewMode == ViewModeBlkMeta {
					// Metadata mode: use metaCols Name
					if int(colIdx) < len(m.state.metaCols) {
						header = m.state.metaCols[colIdx].Name
					} else {
						header = fmt.Sprintf("Col%d", colIdx)
					}
				} else {
					// Data mode: check for renamed columns
					header = fmt.Sprintf("Col%d", colIdx)
					if m.state.colNames != nil {
						if customName, exists := m.state.colNames[colIdx]; exists {
							header = customName
						}
					}
				}
				fmt.Fprintf(b, " %-*s │", widths[i], header)
			}
		}
	}
	b.WriteString("\n")

	// Separator line
	b.WriteString("├")
	b.WriteString(strings.Repeat("─", propertyWidth+2))
	b.WriteString("┼")
	for i, w := range widths {
		b.WriteString(strings.Repeat("─", w+2))
		if i < len(widths)-1 {
			b.WriteString("┼")
		}
	}
	b.WriteString("┤\n")

	// Data rows
	for i, row := range rows {
		fmt.Fprintf(b, "│ %-*s │", propertyWidth, rowNumbers[i])
		for j, colIdx := range displayCols {
			if j < len(widths) && int(colIdx) < len(row) {
				cell := row[colIdx] // Always use colIdx now

				// Truncate content based on user-set width
				if m.state.maxColWidth > 0 && len(cell) > m.state.maxColWidth {
					cell = cell[:m.state.maxColWidth-3] + "..."
				}

				// Highlight search match
				cell = m.highlightSearchMatch(cell, int64(i)+m.state.GlobalRowOffset(), int(colIdx))

				fmt.Fprintf(b, " %-*s │", widths[j], cell)
			}
		}
		b.WriteString("\n")
	}

	// Table footer
	b.WriteString("└")
	b.WriteString(strings.Repeat("─", propertyWidth+2))
	b.WriteString("┴")
	for i, w := range widths {
		b.WriteString(strings.Repeat("─", w+2))
		if i < len(widths)-1 {
			b.WriteString("┴")
		}
	}
	b.WriteString("┘\n")

	// Render summary for BlkMeta mode (outside the table)
	if m.state.viewMode == ViewModeBlkMeta && m.state.blkSummary != nil {
		m.renderBlkSummary(b)
	}
}

func (m model) renderBlkSummary(b *strings.Builder) {
	if len(m.state.blkSummary) == 0 {
		return
	}

	info := m.state.reader.Info()
	b.WriteString("\n")
	b.WriteString("Block Summary:\n")

	for blockID := uint32(0); blockID < info.BlockCount; blockID++ {
		if summary, exists := m.state.blkSummary[blockID]; exists {
			fmt.Fprintf(b, "  Block %d:\n", blockID)
			fmt.Fprintf(b, "    Origin Size:       %s (%d bytes)\n", formatSize(uint64(summary.TotalOriginSize)), summary.TotalOriginSize)
			fmt.Fprintf(b, "    Compressed Size:   %s (%d bytes)\n", formatSize(uint64(summary.TotalCompSize)), summary.TotalCompSize)
			fmt.Fprintf(b, "    Compression Ratio: %.2f%%\n", summary.CompressionRatio)
		}
	}
}

func (m model) renderVertical(b *strings.Builder) {
	rows, rowNumbers, _ := m.state.CurrentRows()
	if len(rows) == 0 {
		b.WriteString("No data\n")
		return
	}

	cols := m.state.reader.Columns()
	displayCols := m.state.visibleCols
	if displayCols == nil {
		displayCols = make([]uint16, len(cols))
		for i := range cols {
			displayCols[i] = uint16(i)
		}
	}

	// In search mode, only show matching rows
	if m.hasMatch && m.searchTerm != "" {
		// Find index of matching row in current page
		matchRowInPage := -1
		for i, rowNum := range rowNumbers {
			if rowNum == m.currentMatch.RowNum {
				matchRowInPage = i
				break
			}
		}

		if matchRowInPage >= 0 && matchRowInPage < len(rows) {
			// Only show matching row
			rows = [][]string{rows[matchRowInPage]}
			rowNumbers = []string{rowNumbers[matchRowInPage]}
		}
	}

	// Display row
	for rowIdx, row := range rows {
		fmt.Fprintf(b, "*************************** %s ***************************\n", rowNumbers[rowIdx])
		for j, colIdx := range displayCols {
			if j < len(row) && int(colIdx) < len(cols) {
				col := cols[colIdx]
				value := row[j]

				// Truncate content based on user-set width
				if m.state.maxColWidth > 0 && len(value) > m.state.maxColWidth {
					value = value[:m.state.maxColWidth-3] + "..."
				}

				// Highlight search match
				value = m.highlightSearchMatch(value, int64(rowIdx)+m.state.GlobalRowOffset(), int(colIdx))

				// Use custom column name or default column name
				colName := fmt.Sprintf("Col%d", colIdx)
				if m.state.colNames != nil {
					if customName, exists := m.state.colNames[colIdx]; exists {
						colName = customName
					}
				}

				fmt.Fprintf(b, "%15s (%s): %s\n", col.Type.String(), colName, value)
			}
		}
		if rowIdx < len(rows)-1 {
			b.WriteString("\n") // Empty line between rows
		}
	}
}

// RunBubbletea runs interactive interface using Bubbletea
func RunBubbletea(path string) error {
	return RunBubbleteaWithOptions(path, nil)
}

// ViewOptions contains options for viewing object data
type ViewOptions struct {
	StartRow      int64               // Start row (0-based)
	EndRow        int64               // End row (inclusive, -1 means all)
	ColumnNames   map[uint16]string   // Custom column names
	ColumnFormats map[uint16]string   // Custom column formats (e.g., "ts", "objectstats", "hex")
}

// RunBubbleteaWithOptions runs interactive interface with options
func RunBubbleteaWithOptions(path string, opts *ViewOptions) error {
	ctx := context.Background()

	reader, err := objecttool.Open(ctx, path)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to open object: %v", err)
	}
	defer reader.Close()

	state := NewState(ctx, reader)
	defer state.Close()

	// Apply options
	if opts != nil {
		if opts.ColumnNames != nil {
			state.colNames = opts.ColumnNames
		}
		if opts.ColumnFormats != nil {
			for colIdx, fmtName := range opts.ColumnFormats {
				state.SetFormat(colIdx, fmtName)
			}
		}
		if opts.StartRow >= 0 || opts.EndRow >= 0 {
			state.SetRowRange(opts.StartRow, opts.EndRow)
		}
		if opts.StartRow > 0 {
			state.GotoRow(opts.StartRow)
		}
	}

	m := model{
		state: state,
	}

	// Load history
	m.loadHistory()

	p := tea.NewProgram(m, tea.WithAltScreen())
	result, err := p.Run()

	// Save history
	if finalModel, ok := result.(model); ok {
		finalModel.saveHistory()
	}

	return err
}

// completeCommand implements smart command completion
func (m model) completeCommand(input string) string {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return ""
	}

	// If only one word, try to complete command name
	if len(parts) == 1 {
		completed := m.completeCommandName(parts[0])
		// If completion differs from input, return completion result
		if completed != parts[0] && completed != "" {
			return completed
		}
		// If it's a known complete command, try to complete parameters
		commands := []string{"quit", "info", "schema", "format", "vertical", "table", "set", "vrows", "search", "cols", "help"}
		for _, cmd := range commands {
			if parts[0] == cmd {
				return m.completeCommandArgs(parts)
			}
		}
		// Otherwise return original completion result (might be common prefix)
		return completed
	}

	// Multiple words, complete parameters
	return m.completeCommandArgs(parts)
}

// completeCommandName completes command name
func (m model) completeCommandName(input string) string {
	commands := []string{
		"quit", "q", "info", "schema", "format", "vertical", "v",
		"table", "t", "set", "vrows", "search", "cols", "help",
		"data", "blkmeta", "objmeta",
	}

	var matches []string
	for _, cmd := range commands {
		if strings.HasPrefix(cmd, input) {
			matches = append(matches, cmd)
		}
	}

	if len(matches) == 1 {
		return matches[0]
	}

	if len(matches) > 1 {
		return longestCommonPrefix(matches)
	}

	return ""
}

// completeCommandArgs completes command arguments
func (m model) completeCommandArgs(parts []string) string {
	cmd := parts[0]

	switch cmd {
	case "format":
		if len(parts) == 1 {
			return cmd + " 0"
		}
		if len(parts) == 2 {
			return cmd + " " + parts[1] + " auto"
		}
		if len(parts) == 3 {
			formatters := []string{"auto", "objectstats", "rowid", "ts", "hex"}
			for _, fmt := range formatters {
				if strings.HasPrefix(fmt, parts[2]) {
					return cmd + " " + parts[1] + " " + fmt
				}
			}
		}
	case "set":
		if len(parts) == 1 {
			return cmd + " width"
		}
		if len(parts) == 2 && parts[1] == "width" {
			return cmd + " width 32"
		}
	case "cols":
		if len(parts) == 1 {
			return cmd + " all"
		}
	case "vrows":
		if len(parts) == 1 {
			return cmd + " 10"
		}
	}

	return ""
}

// longestCommonPrefix calculates longest common prefix of string array
func longestCommonPrefix(strs []string) string {
	if len(strs) == 0 {
		return ""
	}

	prefix := strs[0]
	for _, str := range strs[1:] {
		for len(prefix) > 0 && !strings.HasPrefix(str, prefix) {
			prefix = prefix[:len(prefix)-1]
		}
		if prefix == "" {
			break
		}
	}
	return prefix
}

// getVisibleColumns calculates visible columns based on horizontal scroll offset
func (m model) getVisibleColumns(cols []objecttool.ColInfo) []uint16 {
	// For objmeta mode, always only show Value column (index 1)
	if m.state.viewMode == ViewModeObjMeta {
		if len(cols) > 1 {
			return []uint16{1} // Only Value column
		}
		return []uint16{}
	}

	// If user set specific visible columns, use them first
	if m.state.visibleCols != nil {
		return m.state.visibleCols
	}

	// Show more columns, not limited by terminal width (truncation handled by table rendering)
	var visibleCols []uint16
	startCol := m.hScrollOffset
	maxCols := 15 // Display at most 15 columns at once

	for i := startCol; i < len(cols) && len(visibleCols) < maxCols; i++ {
		visibleCols = append(visibleCols, uint16(i))
	}

	return visibleCols
}

// loadHistory loads command history
func (m *model) loadHistory() {
	historyFile := getHistoryFile()
	if historyFile == "" {
		return
	}

	data, err := os.ReadFile(historyFile)
	if err != nil {
		return
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			m.cmdHistory = append(m.cmdHistory, line)
		}
	}
	m.historyIndex = len(m.cmdHistory)
}

// saveHistory saves command history
func (m *model) saveHistory() {
	historyFile := getHistoryFile()
	if historyFile == "" {
		return
	}

	// Ensure directory exists
	dir := filepath.Dir(historyFile)
	os.MkdirAll(dir, 0755)

	// Only save last 100 records
	start := 0
	if len(m.cmdHistory) > 100 {
		start = len(m.cmdHistory) - 100
	}

	content := strings.Join(m.cmdHistory[start:], "\n")
	os.WriteFile(historyFile, []byte(content), 0644)
}

// getHistoryFile gets history file path
func getHistoryFile() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(homeDir, ".mo_object_history")
}

// SearchCommand is the search command
type SearchCommand struct {
	Pattern string
}

func (c *SearchCommand) Execute(state *State) (string, bool, error) {
	// Simple text search implementation
	rows, _, err := state.CurrentRows()
	if err != nil {
		return "", false, err
	}

	var matches []string
	for i, row := range rows {
		for j, cell := range row {
			if strings.Contains(strings.ToLower(cell), strings.ToLower(c.Pattern)) {
				matches = append(matches, fmt.Sprintf("Row %d, Col %d: %s", i, j, cell))
				if len(matches) >= 10 { // Limit to first 10 matches
					break
				}
			}
		}
		if len(matches) >= 10 {
			break
		}
	}

	if len(matches) == 0 {
		return fmt.Sprintf("No matches found for: %s", c.Pattern), false, nil
	}

	result := fmt.Sprintf("Found %d matches for '%s':\n", len(matches), c.Pattern)
	for _, match := range matches {
		result += "  " + match + "\n"
	}

	return result, false, nil
}

// ColumnsCommand is the column filter command
type ColumnsCommand struct {
	ShowAll bool
	Columns []uint16
}

func (c *ColumnsCommand) Execute(state *State) (string, bool, error) {
	if c.ShowAll {
		state.visibleCols = nil
		return "Showing all columns", false, nil
	}

	// Validate column indices
	totalCols := len(state.reader.Columns())
	for _, col := range c.Columns {
		if int(col) >= totalCols {
			return fmt.Sprintf("Column %d out of range (0-%d)", col, totalCols-1), false, nil
		}
	}

	state.visibleCols = c.Columns
	return fmt.Sprintf("Showing columns: %v", c.Columns), false, nil
}

// VRowsCommand sets number of rows to display in vertical mode
type VRowsCommand struct {
	Rows int
}

func (c *VRowsCommand) Execute(state *State) (string, bool, error) {
	if state.verticalMode {
		state.pageSize = c.Rows
		return fmt.Sprintf("Vertical mode now shows %d rows per page", c.Rows), false, nil
	}
	return "Command only works in vertical mode. Use :vertical first", false, nil
}

// RenameCommand renames a column
type RenameCommand struct {
	ColIndex uint16
	NewName  string
}

func (c *RenameCommand) Execute(state *State) (string, bool, error) {
	cols := state.reader.Columns()
	if int(c.ColIndex) >= len(cols) {
		return fmt.Sprintf("Column %d out of range (0-%d)", c.ColIndex, len(cols)-1), false, nil
	}

	if state.colNames == nil {
		state.colNames = make(map[uint16]string)
	}
	state.colNames[c.ColIndex] = c.NewName

	// If in blkmeta mode, rebuild metadata rows to reflect the new column name
	if state.viewMode == ViewModeBlkMeta {
		state.metaRows, state.blkSummary = state.buildBlkMetaRows()
	}

	return fmt.Sprintf("Renamed Col%d to %s", c.ColIndex, c.NewName), false, nil
}
