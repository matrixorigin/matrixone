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
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
)

type model struct {
	state        *State
	message      string
	cmdMode      bool
	cmdInput     string
	cmdHistory   []string
	historyIndex int
	hScrollOffset int  // 水平滚动偏移
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
		// 水平向左滚动
		if m.hScrollOffset > 0 {
			m.hScrollOffset--
		}
	case "right", "l":
		// 水平向右滚动
		cols := m.state.reader.Columns()
		if m.hScrollOffset < len(cols)-1 {
			m.hScrollOffset++
		}
	case ":":
		m.cmdMode = true
		m.cmdInput = ""
		m.historyIndex = len(m.cmdHistory)
	case "/":
		m.cmdMode = true
		m.cmdInput = "search "
		m.historyIndex = len(m.cmdHistory)
	case "?":
		m.message = generalHelp
	default:
		// 支持 :N 快捷跳转
		if len(msg.String()) == 1 && msg.String()[0] >= '0' && msg.String()[0] <= '9' {
			m.cmdMode = true
			m.cmdInput = msg.String()
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
		
		// 添加到历史记录
		m.cmdHistory = append(m.cmdHistory, m.cmdInput)
		m.historyIndex = len(m.cmdHistory)
		
		output, quit, err := cmd.Execute(m.state)
		if err != nil {
			m.message = fmt.Sprintf("Error: %v", err)
			return m, nil
		}
		if quit {
			return m, tea.Quit
		}
		m.message = output
		m.cmdInput = ""
		return m, nil
	case "up":
		// 历史记录向上
		if len(m.cmdHistory) > 0 && m.historyIndex > 0 {
			m.historyIndex--
			m.cmdInput = m.cmdHistory[m.historyIndex]
		}
		return m, nil
	case "down":
		// 历史记录向下
		if len(m.cmdHistory) > 0 && m.historyIndex < len(m.cmdHistory)-1 {
			m.historyIndex++
			m.cmdInput = m.cmdHistory[m.historyIndex]
		} else if m.historyIndex == len(m.cmdHistory)-1 {
			m.historyIndex = len(m.cmdHistory)
			m.cmdInput = ""
		}
		return m, nil
	case "esc", "ctrl+c":
		m.cmdMode = false
		m.cmdInput = ""
		m.historyIndex = len(m.cmdHistory)
	case "backspace":
		if len(m.cmdInput) > 0 {
			m.cmdInput = m.cmdInput[:len(m.cmdInput)-1]
		}
	case "tab":
		// Tab 补全
		if m.cmdInput != "" {
			completed := m.completeCommand(m.cmdInput)
			if completed != "" && completed != m.cmdInput {
				m.cmdInput = completed
			}
		}
		return m, nil
	default:
		if len(msg.String()) == 1 {
			m.cmdInput += msg.String()
		}
	}
	return m, nil
}

func (m model) View() string {
	var b strings.Builder
	
	// 数据表格 - 撑满屏幕
	if m.state.verticalMode {
		m.renderVertical(&b)
	} else {
		m.renderTable(&b)
	}
	
	// 消息显示（如果有）
	if m.message != "" {
		b.WriteString("\n► ")
		b.WriteString(m.message)
		b.WriteString("\n")
	}
	
	// 底部状态栏 - 始终在最下面
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
	
	// 状态栏颜色 - 深灰背景，白字
	bgColor := "\033[100m"   // 深灰背景
	textColor := "\033[97m"  // 亮白文字
	reset := "\033[0m"       // 重置颜色
	
	// 状态信息
	statusText := fmt.Sprintf(" %s │ Rows %d-%d/%d │ Block %d/%d │ Cols %d-%d/%d ", 
		mode, start, end, info.RowCount, m.state.currentBlock+1, info.BlockCount,
		m.hScrollOffset+1, m.hScrollOffset+len(visibleCols), len(cols))
	
	// 计算需要填充的空格数
	padding := termWidth - len(statusText)
	if padding < 0 {
		padding = 0
	}
	
	// 带颜色的状态栏 - 撑满整行
	b.WriteString("\n")
	b.WriteString(bgColor + textColor)
	b.WriteString(statusText)
	b.WriteString(strings.Repeat(" ", padding))
	b.WriteString(reset)
	
	// 命令行 - 普通样式，不带背景色
	b.WriteString("\n")
	if m.cmdMode {
		b.WriteString(":")
		b.WriteString(m.cmdInput)
		b.WriteString("█")
	}
	
	return b.String()
}

func (m model) renderTable(b *strings.Builder) {
	rows, rowNumbers, _ := m.state.CurrentRows()
	if len(rows) == 0 {
		b.WriteString("No data\n")
		return
	}
	
	cols := m.state.reader.Columns()
	
	// 确定显示的列 - 根据水平滚动偏移和终端宽度
	displayCols := m.getVisibleColumns(cols)
	
	// 计算列宽
	widths := make([]int, len(displayCols))
	for i, colIdx := range displayCols {
		if int(colIdx) < len(cols) {
			widths[i] = len(fmt.Sprintf("Col%d", colIdx))
			if widths[i] < 8 {
				widths[i] = 8
			}
		}
	}
	
	// 根据数据调整列宽
	for _, row := range rows {
		for i, cell := range row {
			if i+m.hScrollOffset < len(widths) {
				cellLen := len(cell)
				if cellLen > widths[i] {
					widths[i] = cellLen
				}
			}
		}
	}
	
	// 应用最大宽度限制
	for i := range widths {
		if m.state.maxColWidth > 0 && widths[i] > m.state.maxColWidth {
			widths[i] = m.state.maxColWidth
		} else if m.state.maxColWidth == 0 {
			if widths[i] > 200 {
				widths[i] = 200
			}
		}
	}
	
	// 表头
	b.WriteString("┌────────────┬")
	for i, w := range widths {
		b.WriteString(strings.Repeat("─", w+2))
		if i < len(widths)-1 {
			b.WriteString("┬")
		}
	}
	b.WriteString("┐\n")
	
	// 列标题
	b.WriteString("│ RowNum     │")
	for i, colIdx := range displayCols {
		if int(colIdx) < len(cols) {
			header := fmt.Sprintf("Col%d", colIdx)
			fmt.Fprintf(b, " %-*s │", widths[i], header)
		}
	}
	b.WriteString("\n")
	
	// 分隔线
	b.WriteString("├────────────┼")
	for i, w := range widths {
		b.WriteString(strings.Repeat("─", w+2))
		if i < len(widths)-1 {
			b.WriteString("┼")
		}
	}
	b.WriteString("┤\n")
	
	// 数据行
	for i, row := range rows {
		fmt.Fprintf(b, "│ %-10s │", rowNumbers[i])
		for j, colIdx := range displayCols {
			if j < len(widths) && int(colIdx) < len(row) {
				cell := row[colIdx]
				// 截断过长的内容
				if len(cell) > widths[j] {
					cell = cell[:widths[j]-3] + "..."
				}
				fmt.Fprintf(b, " %-*s │", widths[j], cell)
			}
		}
		b.WriteString("\n")
	}
	
	// 表尾
	b.WriteString("└────────────┴")
	for i, w := range widths {
		b.WriteString(strings.Repeat("─", w+2))
		if i < len(widths)-1 {
			b.WriteString("┴")
		}
	}
	b.WriteString("┘\n")
}

func (m model) renderVertical(b *strings.Builder) {
	rows, rowNumbers, _ := m.state.CurrentRows()
	if len(rows) == 0 {
		b.WriteString("No data\n")
		return
	}
	
	cols := m.state.reader.Columns()
	
	// 显示所有行
	for rowIdx, row := range rows {
		fmt.Fprintf(b, "*************************** %s ***************************\n", rowNumbers[rowIdx])
		for i, cell := range row {
			if i < len(cols) {
				col := cols[i]
				value := cell
				if m.state.maxColWidth > 0 && len(value) > m.state.maxColWidth {
					value = value[:m.state.maxColWidth-3] + "..."
				}
				fmt.Fprintf(b, "%15s (Col%d): %s\n", col.Type.String(), i, value)
			}
		}
		if rowIdx < len(rows)-1 {
			b.WriteString("\n") // 行之间空一行
		}
	}
}

// RunBubbletea 使用 Bubbletea 运行交互界面
func RunBubbletea(path string) error {
	ctx := context.Background()
	
	reader, err := objecttool.Open(ctx, path)
	if err != nil {
		return fmt.Errorf("failed to open object: %w", err)
	}
	defer reader.Close()
	
	state := NewState(ctx, reader)
	defer state.Close()
	
	m := model{
		state: state,
	}
	
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err = p.Run()
	return err
}

// completeCommand 实现智能命令补全
func (m model) completeCommand(input string) string {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return ""
	}
	
	// 如果只有一个词且不是完整命令，补全命令名
	if len(parts) == 1 {
		completed := m.completeCommandName(parts[0])
		if completed != parts[0] {
			return completed
		}
		// 如果是完整命令，尝试补全参数
		return m.completeCommandArgs(parts)
	}
	
	// 多个词，补全参数
	return m.completeCommandArgs(parts)
}

// completeCommandName 补全命令名
func (m model) completeCommandName(input string) string {
	commands := []string{
		"quit", "q", "info", "schema", "format", "vertical", "v", 
		"table", "t", "set", "vrows", "search", "cols", "help",
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

// completeCommandArgs 补全命令参数
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

// longestCommonPrefix 计算字符串数组的最长公共前缀
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

// getVisibleColumns 根据水平滚动偏移和终端宽度计算可见的列
func (m model) getVisibleColumns(cols []objecttool.ColInfo) []uint16 {
	// 如果用户设置了特定的可见列，优先使用
	if m.state.visibleCols != nil {
		return m.state.visibleCols
	}
	
	// 使用更大的终端宽度估算，显示更多列
	terminalWidth := 200  // 增加到200
	usedWidth := 12 // RowNum 列的宽度
	
	var visibleCols []uint16
	startCol := m.hScrollOffset
	
	for i := startCol; i < len(cols); i++ {
		colWidth := m.state.maxColWidth
		if colWidth == 0 || colWidth > 30 {
			colWidth = 20 // 限制单列最大宽度为20，显示更多列
		}
		
		if usedWidth + colWidth + 3 > terminalWidth { // +3 for borders
			break
		}
		
		visibleCols = append(visibleCols, uint16(i))
		usedWidth += colWidth + 3
	}
	
	// 至少显示8列（如果有的话）
	if len(visibleCols) < 8 && startCol < len(cols) {
		maxCols := len(cols) - startCol
		if maxCols > 8 {
			maxCols = 8
		}
		visibleCols = make([]uint16, maxCols)
		for i := 0; i < maxCols; i++ {
			visibleCols[i] = uint16(startCol + i)
		}
	}
	
	return visibleCols
}

// SearchCommand 搜索命令
type SearchCommand struct {
	Pattern string
}

func (c *SearchCommand) Execute(state *State) (string, bool, error) {
	// 简单的文本搜索实现
	rows, _, err := state.CurrentRows()
	if err != nil {
		return "", false, err
	}
	
	var matches []string
	for i, row := range rows {
		for j, cell := range row {
			if strings.Contains(strings.ToLower(cell), strings.ToLower(c.Pattern)) {
				matches = append(matches, fmt.Sprintf("Row %d, Col %d: %s", i, j, cell))
				if len(matches) >= 10 { // 限制显示前10个匹配
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

// ColumnsCommand 列过滤命令
type ColumnsCommand struct {
	ShowAll bool
	Columns []uint16
}

func (c *ColumnsCommand) Execute(state *State) (string, bool, error) {
	if c.ShowAll {
		state.visibleCols = nil
		return "Showing all columns", false, nil
	}
	
	// 验证列索引
	totalCols := len(state.reader.Columns())
	for _, col := range c.Columns {
		if int(col) >= totalCols {
			return fmt.Sprintf("Column %d out of range (0-%d)", col, totalCols-1), false, nil
		}
	}
	
	state.visibleCols = c.Columns
	return fmt.Sprintf("Showing columns: %v", c.Columns), false, nil
}

// VRowsCommand 设置 vertical 模式显示行数
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
