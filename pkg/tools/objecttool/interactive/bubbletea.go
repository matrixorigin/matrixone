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
	state    *State
	message  string
	cmdMode  bool
	cmdInput string
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
	case ":":
		m.cmdMode = true
		m.cmdInput = ""
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
		output, quit, err := cmd.Execute(m.state)
		if err != nil {
			m.message = fmt.Sprintf("Error: %v", err)
			return m, nil
		}
		if quit {
			return m, tea.Quit
		}
		m.message = output
		return m, nil
	case "esc", "ctrl+c":
		m.cmdMode = false
		m.cmdInput = ""
	case "backspace":
		if len(m.cmdInput) > 0 {
			m.cmdInput = m.cmdInput[:len(m.cmdInput)-1]
		}
	default:
		if len(msg.String()) == 1 {
			m.cmdInput += msg.String()
		}
	}
	return m, nil
}

func (m model) View() string {
	var b strings.Builder
	
	// 标题
	info := m.state.reader.Info()
	b.WriteString(strings.Repeat("=", 120))
	b.WriteString(fmt.Sprintf("\nObject: %s\n", info.Path))
	b.WriteString(fmt.Sprintf("Blocks: %d | Rows: %d | Cols: %d\n", info.BlockCount, info.RowCount, info.ColCount))
	b.WriteString(strings.Repeat("=", 120))
	b.WriteString("\n")
	
	// 数据表格
	if m.state.verticalMode {
		m.renderVertical(&b)
	} else {
		m.renderTable(&b)
	}
	
	// 状态栏
	rows, _, _ := m.state.CurrentRows()
	start := m.state.GlobalRowOffset() + 1
	end := start + int64(len(rows)) - 1
	mode := "Table"
	if m.state.verticalMode {
		mode = "Vertical"
	}
	widthStr := fmt.Sprintf("%d", m.state.maxColWidth)
	if m.state.maxColWidth == 0 {
		widthStr = "unlimited"
	}
	b.WriteString(fmt.Sprintf("\n[%d-%d of %d] Block %d/%d | Mode: %s | Width: %s\n",
		start, end, info.RowCount, m.state.currentBlock+1, info.BlockCount, mode, widthStr))
	
	// 消息
	if m.message != "" {
		b.WriteString("\n")
		b.WriteString(m.message)
		b.WriteString("\n")
	}
	
	// 提示符
	b.WriteString("\n")
	if m.cmdMode {
		b.WriteString(":")
		b.WriteString(m.cmdInput)
	} else {
		b.WriteString("(j/k/Enter/q or : for command) ")
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
	
	// 确定显示的列 - 显示所有列
	displayCols := m.state.visibleCols
	if displayCols == nil {
		displayCols = make([]uint16, len(cols))
		for i := range cols {
			displayCols[i] = uint16(i)
		}
	}
	
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
			if i < len(widths) {
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
			// unlimited 模式，但仍然有一个合理的上限避免显示问题
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
		for j, cell := range row {
			if j < len(widths) {
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
