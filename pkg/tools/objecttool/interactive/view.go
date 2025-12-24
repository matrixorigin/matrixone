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
	width  int
	height int
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
	rows, err := state.CurrentRows()
	if err != nil {
		return err
	}

	cols := state.reader.Columns()

	// 计算列宽
	widths := v.calcColWidths(cols, rows)

	// 表头
	v.renderTableHeader(cols, widths)

	// 数据行
	for _, row := range rows {
		v.renderTableRow(row, widths)
	}

	// 表尾
	v.renderTableFooter(widths)

	return nil
}

func (v *View) renderStatus(state *State) {
	info := state.reader.Info()
	start := state.GlobalRowOffset() + 1
	rows, _ := state.CurrentRows()
	end := start + int64(len(rows)) - 1

	fmt.Printf("\n[%d-%d of %d] Block %d/%d\n",
		start, end, info.RowCount, state.currentBlock+1, info.BlockCount)
}

func (v *View) calcColWidths(cols []objecttool.ColInfo, rows [][]string) []int {
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
				// 最大宽度限制
				if widths[i] > 40 {
					widths[i] = 40
				}
			}
		}
	}

	return widths
}

func (v *View) renderTableHeader(cols []objecttool.ColInfo, widths []int) {
	// ┌────┬────┐
	fmt.Print("┌")
	for i, w := range widths {
		fmt.Print(strings.Repeat("─", w+2))
		if i < len(widths)-1 {
			fmt.Print("┬")
		}
	}
	fmt.Println("┐")

	// │Col0│Col1│
	fmt.Print("│")
	for i, col := range cols {
		header := fmt.Sprintf("Col%d", col.Idx)
		fmt.Printf(" %-*s │", widths[i], header)
	}
	fmt.Println()

	// ├────┼────┤
	fmt.Print("├")
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
	fmt.Print("└")
	for i, w := range widths {
		fmt.Print(strings.Repeat("─", w+2))
		if i < len(widths)-1 {
			fmt.Print("┴")
		}
	}
	fmt.Println("┘")
}
