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

	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
)

// View 终端视图（简化版）
type View struct {
	width        int
	height       int
	verticalMode bool // \G 模式：垂直显示
}

func NewView() *View {
	return &View{
		width:  120,
		height: 30,
	}
}

// Render 渲染当前状态
func (v *View) Render(state *State, message string) error {
	v.clear()

	// 同步垂直模式状态
	v.verticalMode = state.verticalMode

	// 1. 标题栏
	v.renderHeader(state)

	// 2. 数据表格
	if err := v.renderTable(state); err != nil {
		return err
	}

	// 3. 状态栏
	v.renderStatus(state)

	// 4. 消息/命令行
	if message != "" {
		fmt.Println()
		fmt.Println(message)
		fmt.Println()
	}

	return nil
}

func (v *View) clear() {
	fmt.Print("\033[2J\033[H") // ANSI clear screen
}

func (v *View) renderHeader(state *State) {
	info := state.reader.Info()
	fmt.Println(strings.Repeat("=", v.width))
	fmt.Printf("Object: %s\n", info.Path)
	fmt.Printf("Blocks: %d | Rows: %d | Cols: %d\n",
		info.BlockCount, info.RowCount, info.ColCount)
	fmt.Println(strings.Repeat("=", v.width))
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
	
	// 确定显示的列 - table模式也显示所有列，但限制宽度
	displayCols := state.visibleCols
	if displayCols == nil {
		displayCols = make([]uint16, len(cols))
		for i := range cols {
			displayCols[i] = uint16(i)
		}
	}
	
	// 构建显示列信息
	displayColInfo := make([]objecttool.ColInfo, len(displayCols))
	for i, colIdx := range displayCols {
		if int(colIdx) < len(cols) {
			displayColInfo[i] = cols[colIdx]
		}
	}

	// 计算列宽
	widths := v.calcColWidths(displayColInfo, rows, state.maxColWidth)

	// 表头
	v.renderTableHeader(displayColInfo, widths)

	// 数据行（带行号）
	for i, row := range rows {
		v.renderTableRowWithNumber(rowNumbers[i], row, widths)
	}

	// 表尾
	v.renderTableFooter(widths)

	return nil
}

// renderVertical 垂直显示（类似 MySQL \G）
func (v *View) renderVertical(state *State, rows [][]string, rowNumbers []string) error {
	cols := state.reader.Columns()
	
	// 确定显示的列
	displayCols := state.visibleCols
	if displayCols == nil {
		// 垂直模式显示所有列
		displayCols = make([]uint16, len(cols))
		for i := range cols {
			displayCols[i] = uint16(i)
		}
	}
	
	// 显示每一行
	for rowIdx, row := range rows {
		fmt.Printf("*************************** %s ***************************\n", rowNumbers[rowIdx])
		for i, colIdx := range displayCols {
			if int(colIdx) < len(cols) {
				col := cols[colIdx]
				value := ""
				if i < len(row) {
					value = row[i]
					// 在 vertical 模式下应用宽度限制
					if state.maxColWidth > 0 && len(value) > state.maxColWidth {
						value = value[:state.maxColWidth-3] + "..."
					}
				}
				fmt.Printf("%15s (Col%d): %s\n", col.Type.String(), colIdx, value)
			}
		}
		if rowIdx < len(rows)-1 {
			fmt.Println() // 行之间空一行
		}
	}
	
	return nil
}

func (v *View) renderTableRowWithNumber(rowNum string, row []string, widths []int) {
	fmt.Printf("│ %-10s ", rowNum)
	for i, cell := range row {
		if i < len(widths) {
			// 截断过长的内容
			if len(cell) > widths[i] {
				cell = cell[:widths[i]-3] + "..."
			}
			fmt.Printf("│ %-*s ", widths[i], cell)
		}
	}
	fmt.Println("│")
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

	// 显示模式
	mode := "Table"
	if state.verticalMode {
		mode = "Vertical"
	}
	
	// 宽度设置
	widthStr := fmt.Sprintf("%d", state.maxColWidth)
	if state.maxColWidth == 0 {
		widthStr = "unlimited"
	}

	fmt.Printf("\n[%d-%d of %d] Block %d/%d | Mode: %s | Width: %s\n",
		start, end, info.RowCount, state.currentBlock+1, info.BlockCount, mode, widthStr)
}

func (v *View) calcColWidths(cols []objecttool.ColInfo, rows [][]string, maxColWidth int) []int {
	widths := make([]int, len(cols))

	// 初始宽度：列名长度
	for i, col := range cols {
		widths[i] = len(fmt.Sprintf("Col%d", col.Idx))
		if widths[i] < 8 {
			widths[i] = 8
		}
	}

	// 根据数据调整
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
		if maxColWidth > 0 && widths[i] > maxColWidth {
			widths[i] = maxColWidth
		} else if maxColWidth == 0 {
			// unlimited 模式，但仍然有一个合理的上限避免显示问题
			if widths[i] > 200 {
				widths[i] = 200
			}
		}
	}

	return widths
}

func (v *View) renderTableHeader(cols []objecttool.ColInfo, widths []int) {
	// ┌────┬────┐
	fmt.Print("┌────────────┬")
	for i, w := range widths {
		fmt.Print(strings.Repeat("─", w+2))
		if i < len(widths)-1 {
			fmt.Print("┬")
		}
	}
	fmt.Println("┐")

	// │RowNum│Col0│Col1│
	fmt.Print("│ RowNum     │")
	for i, col := range cols {
		header := fmt.Sprintf("Col%d", col.Idx)
		fmt.Printf(" %-*s │", widths[i], header)
	}
	fmt.Println()

	// ├────┼────┤
	fmt.Print("├────────────┼")
	for i, w := range widths {
		fmt.Print(strings.Repeat("─", w+2))
		if i < len(widths)-1 {
			fmt.Print("┼")
		}
	}
	fmt.Println("┤")
}

func (v *View) renderTableRow(row []string, widths []int) {
	fmt.Print("│")
	for i, cell := range row {
		if i < len(widths) {
			// 截断过长的内容
			if len(cell) > widths[i] {
				cell = cell[:widths[i]-3] + "..."
			}
			fmt.Printf(" %-*s │", widths[i], cell)
		}
	}
	fmt.Println()
}

func (v *View) renderTableFooter(widths []int) {
	// └────┴────┘
	fmt.Print("└────────────┴")
	for i, w := range widths {
		fmt.Print(strings.Repeat("─", w+2))
		if i < len(widths)-1 {
			fmt.Print("┴")
		}
	}
	fmt.Println("┘")
}
