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
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
)

type model struct {
	state         *State
	message       string
	cmdMode       bool
	cmdInput      string
	cmdCursor     int    // 命令行光标位置
	cmdHistory    []string
	historyIndex  int
	hScrollOffset int  // 水平滚动偏移
	
	// 搜索相关
	searchTerm    string
	searchMatches []SearchMatch
	currentMatch  int
}

type SearchMatch struct {
	Row    int64
	Col    int
	Value  string
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
		m.cmdCursor = 0
		m.historyIndex = len(m.cmdHistory)
	case "/":
		m.cmdMode = true
		m.cmdInput = "search "
		m.cmdCursor = len(m.cmdInput)
		m.historyIndex = len(m.cmdHistory)
	case "n":
		// 下一个搜索结果
		if len(m.searchMatches) > 0 {
			m.currentMatch = (m.currentMatch + 1) % len(m.searchMatches)
			m.gotoSearchMatch()
		}
	case "N":
		// 上一个搜索结果
		if len(m.searchMatches) > 0 {
			m.currentMatch = (m.currentMatch - 1 + len(m.searchMatches)) % len(m.searchMatches)
			m.gotoSearchMatch()
		}
	case "?":
		m.message = generalHelp
	default:
		// 支持 :N 快捷跳转
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
		
		// 添加到历史记录
		m.cmdHistory = append(m.cmdHistory, m.cmdInput)
		m.historyIndex = len(m.cmdHistory)
		
		// 特殊处理搜索命令
		if searchCmd, ok := cmd.(*SearchCommand); ok {
			m.performSearch(searchCmd.Pattern)
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
		m.message = output
		m.cmdInput = ""
		m.cmdCursor = 0
		return m, nil
	case "up":
		// 历史记录向上
		if len(m.cmdHistory) > 0 && m.historyIndex > 0 {
			m.historyIndex--
			m.cmdInput = m.cmdHistory[m.historyIndex]
			m.cmdCursor = len(m.cmdInput)
		}
		return m, nil
	case "down":
		// 历史记录向下
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
		// 光标左移
		if m.cmdCursor > 0 {
			m.cmdCursor--
		}
		return m, nil
	case "right":
		// 光标右移
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
		// 移动光标到行首
		m.cmdCursor = 0
	case "ctrl+e":
		// 移动光标到行尾
		m.cmdCursor = len(m.cmdInput)
	case "ctrl+u":
		// 清空当前输入
		m.cmdInput = ""
		m.cmdCursor = 0
	case "ctrl+k":
		// 删除光标到行尾的内容
		m.cmdInput = m.cmdInput[:m.cmdCursor]
	case "ctrl+w":
		// 删除光标前的一个单词
		if m.cmdCursor > 0 {
			// 找到前一个空格或行首
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
		// Tab 补全
		if m.cmdInput != "" {
			completed := m.completeCommand(m.cmdInput)
			if completed != "" && completed != m.cmdInput {
				m.cmdInput = completed
				m.cmdCursor = len(completed)
			}
		}
		return m, nil
	default:
		// 支持粘贴和多字符输入
		input := msg.String()
		
		// 处理粘贴内容，去掉可能的方括号包围
		if strings.HasPrefix(input, "[") && strings.HasSuffix(input, "]") && len(input) > 2 {
			input = input[1 : len(input)-1]
		}
		
		// 过滤掉控制字符，只保留可打印字符和空格
		var filtered strings.Builder
		for _, r := range input {
			if r >= 32 && r <= 126 { // 可打印 ASCII 字符包括空格
				filtered.WriteRune(r)
			}
		}
		if filtered.Len() > 0 {
			text := filtered.String()
			// 在光标位置插入文本
			m.cmdInput = m.cmdInput[:m.cmdCursor] + text + m.cmdInput[m.cmdCursor:]
			m.cmdCursor += len(text)
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
		// 在光标位置插入光标字符
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

// gotoSearchMatch 跳转到当前搜索匹配
func (m *model) gotoSearchMatch() {
	if len(m.searchMatches) == 0 || m.currentMatch < 0 || m.currentMatch >= len(m.searchMatches) {
		return
	}
	
	match := m.searchMatches[m.currentMatch]
	// 跳转到匹配的行
	m.state.GotoRow(match.Row)
	
	// 调整水平滚动以显示匹配的列
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
	
	m.message = fmt.Sprintf("Match %d/%d: %s", m.currentMatch+1, len(m.searchMatches), match.Value)
}

// performSearch 执行搜索并保存结果
func (m *model) performSearch(pattern string) {
	m.searchTerm = pattern
	m.searchMatches = nil
	m.currentMatch = 0
	
	// 尝试编译正则表达式
	var regex *regexp.Regexp
	var err error
	useRegex := false
	
	// 如果包含正则特殊字符，尝试作为正则处理
	if strings.ContainsAny(pattern, ".*+?^${}[]|()\\") {
		regex, err = regexp.Compile("(?i)" + pattern) // 忽略大小写
		if err == nil {
			useRegex = true
		}
	}
	
	// 搜索当前页面
	rows, _, err := m.state.CurrentRows()
	if err != nil {
		m.message = fmt.Sprintf("Search error: %v", err)
		return
	}
	
	rowOffset := m.state.GlobalRowOffset()
	for i, row := range rows {
		for j, cell := range row {
			var matched bool
			if useRegex {
				matched = regex.MatchString(cell)
			} else {
				matched = strings.Contains(strings.ToLower(cell), strings.ToLower(pattern))
			}
			
			if matched {
				m.searchMatches = append(m.searchMatches, SearchMatch{
					Row:   rowOffset + int64(i),
					Col:   j,
					Value: cell,
				})
			}
		}
	}
	
	if len(m.searchMatches) == 0 {
		searchType := "text"
		if useRegex {
			searchType = "regex"
		}
		m.message = fmt.Sprintf("No matches found for %s: %s", searchType, pattern)
	} else {
		searchType := "matches"
		if useRegex {
			searchType = "regex matches"
		}
		m.message = fmt.Sprintf("Found %d %s", len(m.searchMatches), searchType)
		m.gotoSearchMatch()
	}
}

// highlightSearchMatch 高亮搜索匹配的文本
func (m model) highlightSearchMatch(text string, row int64, col int) string {
	if m.searchTerm == "" {
		return text
	}
	
	// 检查是否是当前匹配
	isCurrentMatch := false
	if len(m.searchMatches) > 0 && m.currentMatch >= 0 && m.currentMatch < len(m.searchMatches) {
		match := m.searchMatches[m.currentMatch]
		isCurrentMatch = (match.Row == row && match.Col == col)
	}
	
	// 高亮颜色
	highlightColor := "\033[43m\033[30m"  // 黄色背景，黑色文字
	currentColor := "\033[41m\033[97m"    // 红色背景，白色文字 (当前匹配)
	reset := "\033[0m"
	
	// 尝试正则匹配
	var regex *regexp.Regexp
	var err error
	useRegex := false
	
	if strings.ContainsAny(m.searchTerm, ".*+?^${}[]|()\\") {
		regex, err = regexp.Compile("(?i)" + m.searchTerm)
		if err == nil {
			useRegex = true
		}
	}
	
	color := highlightColor
	if isCurrentMatch {
		color = currentColor
	}
	
	if useRegex {
		// 正则表达式高亮
		return regex.ReplaceAllStringFunc(text, func(match string) string {
			return color + match + reset
		})
	} else {
		// 简单文本匹配高亮
		lowerText := strings.ToLower(text)
		lowerPattern := strings.ToLower(m.searchTerm)
		
		if strings.Contains(lowerText, lowerPattern) {
			result := ""
			remaining := text
			lowerRemaining := lowerText
			
			for {
				index := strings.Index(lowerRemaining, lowerPattern)
				if index == -1 {
					result += remaining
					break
				}
				
				result += remaining[:index]
				result += color + remaining[index:index+len(m.searchTerm)] + reset
				
				remaining = remaining[index+len(m.searchTerm):]
				lowerRemaining = lowerRemaining[index+len(m.searchTerm):]
			}
			
			return result
		}
	}
	
	return text
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
	
	// 计算列宽 - 使用用户设置或内容实际长度
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
			if i < len(widths) {
				cellLen := len(cell)
				// 如果用户设置了宽度限制，使用设置值；否则使用内容长度
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
				// 根据用户设置的宽度截断内容
				if m.state.maxColWidth > 0 && len(cell) > m.state.maxColWidth {
					cell = cell[:m.state.maxColWidth-3] + "..."
				}
				
				// 高亮搜索匹配
				cell = m.highlightSearchMatch(cell, int64(i)+m.state.GlobalRowOffset(), int(colIdx))
				
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
				// 根据用户设置的宽度截断内容
				if m.state.maxColWidth > 0 && len(value) > m.state.maxColWidth {
					value = value[:m.state.maxColWidth-3] + "..."
				}
				
				// 高亮搜索匹配
				value = m.highlightSearchMatch(value, int64(rowIdx)+m.state.GlobalRowOffset(), i)
				
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
	
	// 加载历史记录
	m.loadHistory()
	
	p := tea.NewProgram(m, tea.WithAltScreen())
	result, err := p.Run()
	
	// 保存历史记录
	if finalModel, ok := result.(model); ok {
		finalModel.saveHistory()
	}
	
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

// getVisibleColumns 根据水平滚动偏移计算可见的列
func (m model) getVisibleColumns(cols []objecttool.ColInfo) []uint16 {
	// 如果用户设置了特定的可见列，优先使用
	if m.state.visibleCols != nil {
		return m.state.visibleCols
	}
	
	// 显示更多列，不受终端宽度限制（由表格渲染时处理截断）
	var visibleCols []uint16
	startCol := m.hScrollOffset
	maxCols := 15 // 一次最多显示15列
	
	for i := startCol; i < len(cols) && len(visibleCols) < maxCols; i++ {
		visibleCols = append(visibleCols, uint16(i))
	}
	
	return visibleCols
}

// loadHistory 加载历史记录
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

// saveHistory 保存历史记录
func (m *model) saveHistory() {
	historyFile := getHistoryFile()
	if historyFile == "" {
		return
	}
	
	// 确保目录存在
	dir := filepath.Dir(historyFile)
	os.MkdirAll(dir, 0755)
	
	// 只保存最近100条记录
	start := 0
	if len(m.cmdHistory) > 100 {
		start = len(m.cmdHistory) - 100
	}
	
	content := strings.Join(m.cmdHistory[start:], "\n")
	os.WriteFile(historyFile, []byte(content), 0644)
}

// getHistoryFile 获取历史记录文件路径
func getHistoryFile() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(homeDir, ".mo_object_history")
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
