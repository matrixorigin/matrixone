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

// Command is the command interface
type Command interface {
	Execute(state *State) (output string, quit bool, err error)
}

// ParseCommand parses a command
func ParseCommand(input string) (Command, error) {
	input = strings.TrimSpace(input)

	if input == "" {
		return nil, nil
	}

	// Single character commands (browse mode)
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

	// : commands (command mode)
	if strings.HasPrefix(input, ":") {
		return parseColonCommand(input[1:])
	}

	// Command mode also supports commands without :
	return parseColonCommand(input)
}

func parseColonCommand(cmd string) (Command, error) {
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return nil, nil
	}

	// Command alias mapping
	aliases := map[string]string{
		"v": "vertical",
		"t": "table",
		"s": "search",
		"c": "cols",
		"h": "help",
		"i": "info",
		"?": "help",
		"/": "search",
	}

	// Handle aliases
	if alias, exists := aliases[parts[0]]; exists {
		parts[0] = alias
	}

	// Special handling for "w" alias -> "set width"
	if parts[0] == "w" {
		if len(parts) > 1 {
			// "w 64" -> "set width 64"
			parts = append([]string{"set", "width"}, parts[1:]...)
		} else {
			parts = []string{"set", "width"}
		}
	}

	// Check if it's a pure number (:123 jumps to block 123)
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
	case "vrows":
		if len(parts) < 2 {
			return nil, fmt.Errorf("usage: vrows <number>")
		}
		rows, err := strconv.Atoi(parts[1])
		if err != nil || rows < 1 {
			return nil, fmt.Errorf("invalid number of rows: %s", parts[1])
		}
		return &VRowsCommand{Rows: rows}, nil
	case "search", "/":
		if len(parts) < 2 {
			return nil, fmt.Errorf("usage: search <pattern> or /<pattern>")
		}
		pattern := strings.Join(parts[1:], " ")
		return &SearchCommand{Pattern: pattern}, nil
	case "cols", "columns":
		if len(parts) < 2 {
			return &ColumnsCommand{ShowAll: true}, nil
		}
		if parts[1] == "all" {
			return &ColumnsCommand{ShowAll: true}, nil
		}
		// Parse column indices "1,3,5" or "1-5"
		return parseColumnsCommand(parts[1:])
	case "rename":
		if len(parts) < 3 {
			return nil, fmt.Errorf("usage: rename <col_index> <new_name>")
		}
		colIdx, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid column index: %s", parts[1])
		}
		return &RenameCommand{ColIndex: uint16(colIdx), NewName: parts[2]}, nil
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

func parseColumnsCommand(args []string) (Command, error) {
	if len(args) == 0 {
		return &ColumnsCommand{ShowAll: true}, nil
	}

	// Handle "all" explicitly
	if len(args) == 1 && args[0] == "all" {
		return &ColumnsCommand{ShowAll: true}, nil
	}

	var cols []uint16
	for _, arg := range args {
		if strings.Contains(arg, "-") {
			// Range format "1-5"
			parts := strings.Split(arg, "-")
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid range format: %s", arg)
			}
			start, err := strconv.Atoi(parts[0])
			if err != nil {
				return nil, fmt.Errorf("invalid start index: %s", parts[0])
			}
			end, err := strconv.Atoi(parts[1])
			if err != nil {
				return nil, fmt.Errorf("invalid end index: %s", parts[1])
			}
			for i := start; i <= end; i++ {
				cols = append(cols, uint16(i))
			}
		} else if strings.Contains(arg, ",") {
			// Comma-separated format "1,3,5"
			indices := strings.Split(arg, ",")
			for _, idx := range indices {
				col, err := strconv.Atoi(strings.TrimSpace(idx))
				if err != nil {
					return nil, fmt.Errorf("invalid column index: %s", idx)
				}
				cols = append(cols, uint16(col))
			}
		} else {
			// Single index
			col, err := strconv.Atoi(arg)
			if err != nil {
				return nil, fmt.Errorf("invalid column index: %s", arg)
			}
			cols = append(cols, uint16(col))
		}
	}

	return &ColumnsCommand{Columns: cols}, nil
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

// QuitCommand is the quit command
type QuitCommand struct{}

func (c *QuitCommand) Execute(state *State) (string, bool, error) {
	return "", true, nil
}

// InfoCommand shows object information
type InfoCommand struct{}

func (c *InfoCommand) Execute(state *State) (string, bool, error) {
	info := state.reader.Info()
	return fmt.Sprintf(`Object: %s
Blocks: %d
Rows:   %d
Cols:   %d`,
		info.Path, info.BlockCount, info.RowCount, info.ColCount), false, nil
}

// SchemaCommand shows schema
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

// FormatCommand sets format
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

// ScrollCommand is the scroll command
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
	// Ignore boundary errors
	return "", false, nil
}

// GotoCommand is the goto command
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
		// Check block range
		blockCount := state.reader.BlockCount()
		if c.Block >= blockCount {
			return fmt.Sprintf("Block %d out of range. Available blocks: 0-%d", c.Block, blockCount-1), false, nil
		}
		return "", false, state.GotoBlock(c.Block)
	}
}

// HelpCommand is the help command
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

// VerticalCommand switches vertical/table mode
type VerticalCommand struct {
	Enable bool
}

func (c *VerticalCommand) Execute(state *State) (string, bool, error) {
	state.verticalMode = c.Enable
	if c.Enable {
		// Vertical mode uses larger width, default 10 rows per page
		state.maxColWidth = 128
		state.pageSize = 10
		return "Switched to vertical mode (\\G), width=128, 10 rows/page", false, nil
	}
	// Table mode restores default width and page size
	state.maxColWidth = 64
	state.pageSize = 20
	return "Switched to table mode, width=64, 20 rows/page", false, nil
}

// SetCommand sets options
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
  h/l/←/→     Scroll left/right (horizontal)
  Enter       Next page
  Ctrl+F      Next page (forward)
  Ctrl+B      Previous page (backward)
  g/G         Go to top/bottom
  q           Quit
  :           Enter command mode

Command Mode (press : to enter):
  info (i)    Show object info
  schema      Show schema
  format N F  Set column N format to F
  N           Go to block N (e.g., :0, :1, :2)
  vertical (v) Switch to vertical mode (width=128, 10 rows/page)
  table (t)   Switch to table mode (width=64, 20 rows/page)
  set width N Set column width (0=unlimited)
  w N         Shortcut for 'set width N' (e.g., :w 100)
  vrows N     Set vertical mode rows per page (default=10)
  cols (c) <list> Show specific columns (e.g., 1,3,5 or 1-5)
  cols all    Show all columns
  search (s) <pattern>  Search for text in current page
  /<pattern>  Same as search
  help (h,?)  Show help
  quit (q)    Quit

Command History & Completion:
  ↑/↓         Navigate command history
  Tab         Smart auto-complete commands and parameters
  Ctrl+A      Move cursor to beginning of line
  Ctrl+E      Move cursor to end of line
  Ctrl+U      Clear current input
  Ctrl+K      Delete from cursor to end of line
  Ctrl+W      Delete word before cursor
  ←/→         Move cursor left/right

Tab Completion Examples:
  :format <Tab>     → Show column indices
  :format 5 <Tab>   → Show available formatters
  :set <Tab>        → Show 'width' option
  :set width <Tab>  → Show common width values
  :cols <Tab>       → Show column range suggestions
  :vrows <Tab>      → Show row count suggestions

Horizontal Scrolling:
  h/←         Scroll left to see previous columns
  l/→         Scroll right to see next columns
  Status bar shows current column range

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
