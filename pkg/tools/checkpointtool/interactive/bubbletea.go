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
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
)

const helpText = `Checkpoint Viewer - Keyboard Shortcuts:
  Navigation:
    j/↓     - Move down          k/↑     - Move up
    Enter   - Select/Drill down  b/Bksp  - Go back
    Esc     - Back to list       q       - Quit

  View Switching (in Entry view):
    a       - Account view       t       - Table view
    l       - Logical view

  Scrolling:
    PgUp/PgDn - Page up/down     g/G     - Top/Bottom

  Help:
    ?       - Show this help
`

type model struct {
	state    *State
	message  string
	cursor   int // Selection cursor
	quitting bool
}

func newModel(reader *checkpointtool.CheckpointReader) model {
	return model{
		state: NewState(reader),
	}
}

func (m model) Init() tea.Cmd {
	return nil
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

func (m model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	m.message = ""

	switch msg.String() {
	case "q", "ctrl+c":
		m.quitting = true
		return m, tea.Quit

	case "?":
		m.message = helpText
		return m, nil

	case "j", "down":
		m.moveCursor(1)
	case "k", "up":
		m.moveCursor(-1)
	case "g":
		m.cursor = 0
		m.state.scrollOffset = 0
	case "G":
		m.cursor = m.maxItems() - 1
	case "pgdown", "ctrl+f":
		m.state.PageDown(m.maxItems())
		m.cursor = m.state.scrollOffset
	case "pgup", "ctrl+b":
		m.state.PageUp()
		m.cursor = m.state.scrollOffset

	case "enter":
		return m.handleEnter()

	case "b", "backspace":
		m.state.Back()
		m.cursor = 0

	case "esc":
		m.state.BackToList()
		m.cursor = 0

	case "a":
		if m.state.mode == ViewModeEntry {
			if err := m.state.SwitchToAccounts(); err != nil {
				m.message = fmt.Sprintf("Error: %v", err)
			}
			m.cursor = 0
		}

	case "t":
		if m.state.mode == ViewModeEntry {
			if err := m.state.SwitchToTables(); err != nil {
				m.message = fmt.Sprintf("Error: %v", err)
			}
			m.cursor = 0
		}

	case "l":
		if m.state.mode == ViewModeList || m.state.mode == ViewModeEntry {
			if err := m.state.SwitchToLogical(); err != nil {
				m.message = fmt.Sprintf("Error: %v", err)
			}
			m.cursor = 0
		}
	}
	return m, nil
}

func (m *model) moveCursor(delta int) {
	m.cursor += delta
	max := m.maxItems()
	if m.cursor < 0 {
		m.cursor = 0
	}
	if m.cursor >= max {
		m.cursor = max - 1
	}
	if m.cursor < 0 {
		m.cursor = 0
	}
	// Adjust scroll
	if m.cursor < m.state.scrollOffset {
		m.state.scrollOffset = m.cursor
	}
	if m.cursor >= m.state.scrollOffset+m.state.pageSize {
		m.state.scrollOffset = m.cursor - m.state.pageSize + 1
	}
}

func (m model) maxItems() int {
	switch m.state.mode {
	case ViewModeList:
		return len(m.state.entries)
	case ViewModeAccount:
		return len(m.state.accounts)
	case ViewModeAccountTables, ViewModeTable:
		return len(m.state.tables)
	default:
		return 1
	}
}

func (m model) handleEnter() (tea.Model, tea.Cmd) {
	switch m.state.mode {
	case ViewModeList:
		if err := m.state.SelectEntry(m.cursor); err != nil {
			m.message = fmt.Sprintf("Error: %v", err)
		}
		m.cursor = 0

	case ViewModeAccount:
		if m.cursor < len(m.state.accounts) {
			acc := m.state.accounts[m.cursor]
			if err := m.state.SelectAccount(acc.AccountID); err != nil {
				m.message = fmt.Sprintf("Error: %v", err)
			}
			m.cursor = 0
		}

	case ViewModeAccountTables, ViewModeTable:
		if m.cursor < len(m.state.tables) {
			tbl := m.state.tables[m.cursor]
			if err := m.state.SelectTable(tbl.TableID); err != nil {
				m.message = fmt.Sprintf("Error: %v", err)
			}
			m.cursor = 0
		}
	}
	return m, nil
}

func (m model) View() string {
	if m.quitting {
		return ""
	}

	var b strings.Builder

	// Header
	b.WriteString(m.renderHeader())
	b.WriteString("\n")

	// Content
	switch m.state.mode {
	case ViewModeList:
		b.WriteString(m.renderList())
	case ViewModeEntry:
		b.WriteString(m.renderEntry())
	case ViewModeAccount:
		b.WriteString(m.renderAccounts())
	case ViewModeAccountTables, ViewModeTable:
		b.WriteString(m.renderTables())
	case ViewModeTableDetail:
		b.WriteString(m.renderTableDetail())
	case ViewModeLogical:
		b.WriteString(m.renderLogical())
	}

	// Footer
	b.WriteString("\n")
	b.WriteString(m.renderFooter())

	// Message
	if m.message != "" {
		b.WriteString("\n")
		b.WriteString(m.message)
	}

	return b.String()
}

func (m model) renderHeader() string {
	info := m.state.info
	return fmt.Sprintf("═══ Checkpoint Viewer ═══  Dir: %s  Entries: %d (G:%d I:%d C:%d)",
		info.Dir, info.TotalEntries, info.GlobalCount, info.IncrCount, info.CompactCount)
}

func (m model) renderFooter() string {
	mode := ""
	switch m.state.mode {
	case ViewModeList:
		mode = "LIST"
	case ViewModeEntry:
		mode = "ENTRY"
	case ViewModeAccount:
		mode = "ACCOUNT"
	case ViewModeAccountTables:
		mode = fmt.Sprintf("ACCOUNT[%d]", m.state.selectedAccount)
	case ViewModeTable:
		mode = "TABLE"
	case ViewModeTableDetail:
		mode = fmt.Sprintf("TABLE[%d]", m.state.selectedTable)
	case ViewModeLogical:
		mode = "LOGICAL"
	}
	return fmt.Sprintf("─── [%s] ─── Press ? for help, q to quit", mode)
}

func (m model) renderList() string {
	var b strings.Builder
	b.WriteString("     # │ Type │ Start                                             │ End                                               │ State\n")
	b.WriteString("───────┼──────┼───────────────────────────────────────────────────┼───────────────────────────────────────────────────┼────────\n")

	entries := m.state.entries
	start := m.state.scrollOffset
	end := start + m.state.pageSize
	if end > len(entries) {
		end = len(entries)
	}

	for i := start; i < end; i++ {
		e := entries[i]
		cursor := "  "
		if i == m.cursor {
			cursor = "▶ "
		}
		b.WriteString(fmt.Sprintf("%s%4d │  %s   │ %-49s │ %-49s │ %s\n",
			cursor, i,
			e.GetType().String(),
			formatTS(e.GetStart()),
			formatTS(e.GetEnd()),
			stateStr(e.GetState()),
		))
	}
	return b.String()
}

func (m model) renderEntry() string {
	if m.state.selectedEntry >= len(m.state.entries) {
		return "No entry selected"
	}
	e := m.state.entries[m.state.selectedEntry]
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Entry #%d\n", m.state.selectedEntry))
	b.WriteString(fmt.Sprintf("  Type:     %s\n", e.GetType().String()))
	b.WriteString(fmt.Sprintf("  Start:    %s\n", formatTS(e.GetStart())))
	b.WriteString(fmt.Sprintf("  End:      %s\n", formatTS(e.GetEnd())))
	b.WriteString(fmt.Sprintf("  State:    %s\n", stateStr(e.GetState())))
	b.WriteString(fmt.Sprintf("  Version:  %d\n", e.GetVersion()))
	b.WriteString(fmt.Sprintf("  CN Loc:   %s\n", e.GetLocation().String()))

	tnLoc := e.GetTNLocation()
	if !tnLoc.IsEmpty() && tnLoc.String() != e.GetLocation().String() {
		b.WriteString(fmt.Sprintf("  TN Loc:   %s\n", tnLoc.String()))
	}

	tableIDLocs := e.GetTableIDLocation()
	if tableIDLocs.Len() > 0 {
		b.WriteString(fmt.Sprintf("  TableID Locations (%d):\n", tableIDLocs.Len()))
		for i := 0; i < tableIDLocs.Len(); i++ {
			b.WriteString(fmt.Sprintf("    [%d] %s\n", i, tableIDLocs.Get(i).String()))
		}
	}

	b.WriteString(fmt.Sprintf("  CKP LSN:  %d\n", e.LSN()))
	b.WriteString(fmt.Sprintf("  Trunc LSN:%d\n", e.GetTruncateLsn()))
	b.WriteString("\nPress: [a] Accounts  [t] Tables  [l] Logical  [b] Back")
	return b.String()
}

func (m model) renderAccounts() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Accounts in Entry #%d\n", m.state.selectedEntry))
	b.WriteString("  # │ AccountID │ Tables │ Data │ Tomb\n")
	b.WriteString("────┼───────────┼────────┼──────┼──────\n")

	accounts := m.state.accounts
	start := m.state.scrollOffset
	end := start + m.state.pageSize
	if end > len(accounts) {
		end = len(accounts)
	}

	for i := start; i < end; i++ {
		acc := accounts[i]
		cursor := "  "
		if i == m.cursor {
			cursor = "▶ "
		}
		b.WriteString(fmt.Sprintf("%s%2d │ %9d │ %6d │ %4d │ %4d\n",
			cursor, i, acc.AccountID, acc.TableCount, acc.DataRanges, acc.TombRanges))
	}
	return b.String()
}

func (m model) renderTables() string {
	var b strings.Builder
	if m.state.mode == ViewModeAccountTables {
		b.WriteString(fmt.Sprintf("Tables for Account %d\n", m.state.selectedAccount))
	} else {
		b.WriteString(fmt.Sprintf("All Tables in Entry #%d\n", m.state.selectedEntry))
	}
	b.WriteString("  # │ TableID            │ Account │ Data │ Tomb\n")
	b.WriteString("────┼────────────────────┼─────────┼──────┼──────\n")

	tables := m.state.tables
	start := m.state.scrollOffset
	end := start + m.state.pageSize
	if end > len(tables) {
		end = len(tables)
	}

	for i := start; i < end; i++ {
		tbl := tables[i]
		cursor := "  "
		if i == m.cursor {
			cursor = "▶ "
		}
		b.WriteString(fmt.Sprintf("%s%2d │ %18d │ %7d │ %4d │ %4d\n",
			cursor, i, tbl.TableID, tbl.AccountID, len(tbl.DataRanges), len(tbl.TombRanges)))
	}
	return b.String()
}

func (m model) renderTableDetail() string {
	tbl := m.state.GetSelectedTable()
	if tbl == nil {
		return "No table selected"
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf("Table %d (Account: %d)\n", tbl.TableID, tbl.AccountID))
	b.WriteString(fmt.Sprintf("  Data Ranges: %d\n", len(tbl.DataRanges)))
	b.WriteString(fmt.Sprintf("  Tomb Ranges: %d\n", len(tbl.TombRanges)))

	if len(tbl.DataRanges) > 0 {
		b.WriteString("\nData Ranges:\n")
		for i, r := range tbl.DataRanges {
			if i >= 10 {
				b.WriteString(fmt.Sprintf("  ... and %d more\n", len(tbl.DataRanges)-10))
				break
			}
			b.WriteString(fmt.Sprintf("  %s\n", rangeStr(r)))
		}
	}

	if len(tbl.TombRanges) > 0 {
		b.WriteString("\nTombstone Ranges:\n")
		for i, r := range tbl.TombRanges {
			if i >= 10 {
				b.WriteString(fmt.Sprintf("  ... and %d more\n", len(tbl.TombRanges)-10))
				break
			}
			b.WriteString(fmt.Sprintf("  %s\n", rangeStr(r)))
		}
	}
	return b.String()
}

func (m model) renderLogical() string {
	var b strings.Builder
	b.WriteString("Logical View (Composed Checkpoints)\n")
	b.WriteString("────────────────────────────────────\n")

	// Find latest GCKP
	var latestGCKP *checkpoint.CheckpointEntry
	for i := len(m.state.entries) - 1; i >= 0; i-- {
		if m.state.entries[i].IsGlobal() {
			latestGCKP = m.state.entries[i]
			break
		}
	}

	if latestGCKP == nil {
		b.WriteString("No Global Checkpoint found\n")
		return b.String()
	}

	b.WriteString(fmt.Sprintf("Base GCKP: [0, %s]\n", latestGCKP.GetEnd().ToString()))

	// Count ICKPs after GCKP
	gckpEnd := latestGCKP.GetEnd()
	incrCount := 0
	for _, e := range m.state.entries {
		start := e.GetStart()
		if e.IsIncremental() && start.GT(&gckpEnd) {
			incrCount++
		}
	}
	b.WriteString(fmt.Sprintf("Incremental CKPs: %d\n", incrCount))

	return b.String()
}

func stateStr(s checkpoint.State) string {
	switch s {
	case checkpoint.ST_Running:
		return "Running"
	case checkpoint.ST_Pending:
		return "Pending"
	case checkpoint.ST_Finished:
		return "Finished"
	default:
		return "Unknown"
	}
}

func truncStr(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}

func formatTS(ts types.TS) string {
	raw := ts.ToString()
	physical := ts.Physical()
	if physical == 0 {
		return raw
	}
	t := time.Unix(0, physical)
	return fmt.Sprintf("%s(%s)", raw, t.Format("2006/01/02 15:04:05.000000"))
}

func formatTSShort(ts types.TS) string {
	physical := ts.Physical()
	if physical == 0 {
		return "0-0                 "
	}
	t := time.Unix(0, physical)
	return t.Format("01/02 15:04:05.0000")
}

func rangeStr(r ckputil.TableRange) string {
	return fmt.Sprintf("[%d-%d, %d-%d] %s",
		r.Start.GetBlockOffset(), r.Start.GetRowOffset(),
		r.End.GetBlockOffset(), r.End.GetRowOffset(),
		r.ObjectStats.ObjectName().String())
}

// Run starts the interactive checkpoint viewer
func Run(reader *checkpointtool.CheckpointReader) error {
	p := tea.NewProgram(newModel(reader), tea.WithAltScreen())
	_, err := p.Run()
	return err
}
