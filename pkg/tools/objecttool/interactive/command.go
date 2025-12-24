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
	"strconv"
	"strings"
)

// Command 命令接口
type Command interface {
	Execute(state *State) (output string, quit bool, err error)
}

// ParseCommand 解析命令
func ParseCommand(input string) (Command, error) {
	input = strings.TrimSpace(input)

	if input == "" {
		return nil, nil
	}

	// 单字符命令（浏览模式）
	switch input {
	case "q":
		return &QuitCommand{}, nil
	case "j":
		return &ScrollCommand{Down: true, Lines: 1}, nil
	case "k":
		return &ScrollCommand{Down: false, Lines: 1}, nil
	case "g":
		return &GotoCommand{Top: true}, nil
	case "G":
		return &GotoCommand{Bottom: true}, nil
	case "?":
		return &HelpCommand{}, nil
	}

	// : 命令（命令模式）
	if strings.HasPrefix(input, ":") {
		return parseColonCommand(input[1:])
	}

	// 命令模式下不带 : 也支持
	return parseColonCommand(input)
}

func parseColonCommand(cmd string) (Command, error) {
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return nil, nil
	}

	// 检查是否是纯数字（:123 跳转到 block 123）
	if num, err := strconv.ParseUint(parts[0], 10, 32); err == nil {
		return &GotoCommand{Block: uint32(num)}, nil
	}

	switch parts[0] {
	case "q", "quit":
		return &QuitCommand{}, nil
	case "info":
		return &InfoCommand{}, nil
	case "schema":
		return &SchemaCommand{}, nil
	case "format":
		return parseFormatCommand(parts[1:])
	case "vertical", "v", "\\G":
		return &VerticalCommand{Enable: true}, nil
	case "table", "t":
		return &VerticalCommand{Enable: false}, nil
	case "set":
		return parseSetCommand(parts[1:])
	case "help":
		topic := ""
		if len(parts) > 1 {
			topic = strings.Join(parts[1:], " ")
		}
		return &HelpCommand{Topic: topic}, nil
	default:
		return nil, fmt.Errorf("unknown command: %s", parts[0])
	}
}

func parseSetCommand(args []string) (Command, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("usage: set <option> <value>")
	}
	
	switch args[0] {
	case "width":
		if args[1] == "unlimited" || args[1] == "0" {
			return &SetCommand{Option: "width", Value: 0}, nil
		}
		width, err := strconv.Atoi(args[1])
		if err != nil {
			return nil, fmt.Errorf("invalid width: %s", args[1])
		}
		return &SetCommand{Option: "width", Value: width}, nil
	default:
		return nil, fmt.Errorf("unknown option: %s", args[0])
	}
}

func parseFormatCommand(args []string) (Command, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("usage: :format <col> <formatter>")
	}

	colIdx, err := strconv.ParseUint(args[0], 10, 16)
	if err != nil {
		return nil, fmt.Errorf("invalid column index: %s", args[0])
	}

	return &FormatCommand{
		ColIdx:        uint16(colIdx),
		FormatterName: args[1],
	}, nil
}

// QuitCommand 退出命令
type QuitCommand struct{}

func (c *QuitCommand) Execute(state *State) (string, bool, error) {
	return "", true, nil
}

// InfoCommand 显示信息
type InfoCommand struct{}

func (c *InfoCommand) Execute(state *State) (string, bool, error) {
	info := state.reader.Info()
	return fmt.Sprintf(`Object: %s
Blocks: %d
Rows:   %d
Cols:   %d`,
		info.Path, info.BlockCount, info.RowCount, info.ColCount), false, nil
}

// SchemaCommand 显示schema
type SchemaCommand struct{}

func (c *SchemaCommand) Execute(state *State) (string, bool, error) {
	cols := state.reader.Columns()
	var sb strings.Builder
	sb.WriteString("Schema:\n")
	sb.WriteString("  #    Type              Format\n")
	sb.WriteString("  ---- ----------------- ----------\n")
	for _, col := range cols {
		formatterName := state.formatter.GetFormatterName(col.Idx)
		fmt.Fprintf(&sb, "  %-4d %-17s %s\n",
			col.Idx, col.Type.String(), formatterName)
	}
	return sb.String(), false, nil
}

// FormatCommand 设置格式
type FormatCommand struct {
	ColIdx        uint16
	FormatterName string
}

func (c *FormatCommand) Execute(state *State) (string, bool, error) {
	if err := state.SetFormat(c.ColIdx, c.FormatterName); err != nil {
		return "", false, err
	}
	return fmt.Sprintf("Column %d format set to %s", c.ColIdx, c.FormatterName), false, nil
}

// ScrollCommand 滚动命令
type ScrollCommand struct {
	Down  bool
	Lines int
}

func (c *ScrollCommand) Execute(state *State) (string, bool, error) {
	var err error
	if c.Down {
		for i := 0; i < c.Lines; i++ {
			err = state.ScrollDown()
			if err != nil {
				break
			}
		}
	} else {
		for i := 0; i < c.Lines; i++ {
			err = state.ScrollUp()
			if err != nil {
				break
			}
		}
	}
	// 忽略边界错误
	return "", false, nil
}

// GotoCommand 跳转命令
type GotoCommand struct {
	Top    bool
	Bottom bool
	Block  uint32 // block number
}

func (c *GotoCommand) Execute(state *State) (string, bool, error) {
	if c.Top {
		return "", false, state.GotoRow(0)
	} else if c.Bottom {
		return "", false, state.GotoRow(-1)
	} else {
		return "", false, state.GotoBlock(c.Block)
	}
}

// HelpCommand 帮助命令
type HelpCommand struct {
	Topic string
}

func (c *HelpCommand) Execute(state *State) (string, bool, error) {
	if c.Topic == "" {
		return generalHelp, false, nil
	}
	if help, ok := topicHelp[c.Topic]; ok {
		return help, false, nil
	}
	return fmt.Sprintf("No help for: %s", c.Topic), false, nil
}

// VerticalCommand 切换垂直/表格模式
type VerticalCommand struct {
	Enable bool
}

func (c *VerticalCommand) Execute(state *State) (string, bool, error) {
	state.verticalMode = c.Enable
	if c.Enable {
		// 垂直模式使用更大的宽度
		state.maxColWidth = 128
		return "Switched to vertical mode (\\G), width=128", false, nil
	}
	// 表格模式恢复默认宽度
	state.maxColWidth = 64
	return "Switched to table mode, width=64", false, nil
}

// SetCommand 设置选项
type SetCommand struct {
	Option string
	Value  int
}

func (c *SetCommand) Execute(state *State) (string, bool, error) {
	switch c.Option {
	case "width":
		state.maxColWidth = c.Value
		if c.Value == 0 {
			return "Column width set to unlimited", false, nil
		}
		return fmt.Sprintf("Column width set to %d", c.Value), false, nil
	default:
		return fmt.Sprintf("Unknown option: %s", c.Option), false, nil
	}
}

var generalHelp = `
Browse Mode (default):
  j/k/↑/↓     Scroll down/up one line
  Enter       Next page
  Ctrl+F      Next page (forward)
  Ctrl+B      Previous page (backward)
  g/G         Go to top/bottom
  q           Quit
  :           Enter command mode

Command Mode (press : to enter):
  info        Show object info
  schema      Show schema
  format N F  Set column N format to F
  N           Go to block N (e.g., :0, :1, :2)
  vertical    Switch to vertical mode (width=128)
  table       Switch to table mode (width=64)
  set width N Set column width (0=unlimited)
  help [cmd]  Show help
  q           Quit

Row Number Format: (block-offset)
  Example: (0-0) = block 0, offset 0
           (1-100) = block 1, offset 100

Column Width:
  Table mode: 64 chars (default)
  Vertical mode: 128 chars
  Unlimited: :set width 0
`

var topicHelp = map[string]string{
	"format": `
:format <col> <formatter>

Set display format for a column.

Formatters:
  auto        Auto detect (default)
  objectstats ObjectStats.String()
  rowid       Rowid.String()
  ts          Timestamp string
  hex         Hexadecimal

Example:
  :format 0 objectstats
  :format 2 hex
`,
}
