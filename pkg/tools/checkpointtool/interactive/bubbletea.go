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
	"path/filepath"
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

  Search (Table/Account views):
    /       - Enter search mode  Esc     - Exit search
    ↑/↓     - Navigate history   Enter   - Confirm & jump to first
    n       - Next match         N       - Previous match
    Supports regex: .* + ? ^ $ {} [] | () \
    Examples: /100.*  /^10  /[0-9]{3}
    History: ~/.mo_checkpoint_search_history

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

	// Search history
	searchHistory []string
	historyIndex  int
	matchIndex    int // Current match index in search results
}

func newModel(reader *checkpointtool.CheckpointReader) model {
	m := model{
		state: NewState(reader),
	}
	m.loadSearchHistory()
	return m
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

	// Handle search mode
	if m.state.IsSearchMode() {
		switch msg.String() {
		case "esc":
			m.state.ExitSearchMode()
			m.historyIndex = len(m.searchHistory)
			return m, nil
		case "enter":
			// Save to history
			query := m.state.SearchQuery()
			if query != "" && (len(m.searchHistory) == 0 || m.searchHistory[len(m.searchHistory)-1] != query) {
				m.searchHistory = append(m.searchHistory, query)
				m.historyIndex = len(m.searchHistory)
			}
			m.state.ExitSearchMode()
			// Jump to first result if any (even if not on current page)
			if results := m.state.SearchResult(); len(results) > 0 {
				m.cursor = results[0]
				// Adjust scroll to show the result
				m.state.scrollOffset = m.cursor
				if m.state.scrollOffset > 0 {
					m.state.scrollOffset-- // Show one line above for context
				}
			}
			return m, nil
		case "backspace":
			m.state.BackspaceSearchQuery()
			m.historyIndex = len(m.searchHistory)
			return m, nil
		case "up":
			// History up
			if len(m.searchHistory) > 0 && m.historyIndex > 0 {
				m.historyIndex--
				m.state.SetSearchQuery(m.searchHistory[m.historyIndex])
			}
			return m, nil
		case "down":
			// History down
			if len(m.searchHistory) > 0 && m.historyIndex < len(m.searchHistory)-1 {
				m.historyIndex++
				m.state.SetSearchQuery(m.searchHistory[m.historyIndex])
			} else if m.historyIndex == len(m.searchHistory)-1 {
				m.historyIndex = len(m.searchHistory)
				m.state.SetSearchQuery("")
			}
			return m, nil
		case "n":
			// Next match
			m.jumpToNextMatch()
			return m, nil
		case "N":
			// Previous match
			m.jumpToPrevMatch()
			return m, nil
		default:
			// Append character to search query
			if len(msg.String()) == 1 {
				m.state.AppendSearchQuery(rune(msg.String()[0]))
				m.historyIndex = len(m.searchHistory)
			}
			return m, nil
		}
	}

	switch msg.String() {
	case "q", "ctrl+c":
		m.quitting = true
		return m, tea.Quit

	case "?":
		m.message = helpText
		return m, nil

	case "/":
		// Enter search mode for searchable views
		if m.state.mode == ViewModeTable || m.state.mode == ViewModeAccountTables || m.state.mode == ViewModeAccount {
			m.state.EnterSearchMode()
		}
		return m, nil

	case "n":
		// Next match (works even after exiting search mode)
		if len(m.state.SearchResult()) > 0 {
			m.jumpToNextMatch()
		}
		return m, nil

	case "N":
		// Previous match (works even after exiting search mode)
		if len(m.state.SearchResult()) > 0 {
			m.jumpToPrevMatch()
		}
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

	// Breadcrumb
	b.WriteString(m.state.Breadcrumb())
	b.WriteString(strings.Repeat(" ", 60))
	b.WriteString("[?]Help [q]Quit\n")
	b.WriteString(strings.Repeat("─", 80))
	b.WriteString("\n")

	// Search bar (if in search mode)
	if m.state.IsSearchMode() {
		searchType := "Text"
		if m.state.IsRegexSearch() {
			searchType = "Regex"
		}
		b.WriteString(fmt.Sprintf("🔍 Search (%s): %s_\n", searchType, m.state.SearchQuery()))
		if results := m.state.SearchResult(); len(results) > 0 {
			b.WriteString(fmt.Sprintf("   Found %d matches (Press Enter to jump, n/N to navigate)\n", len(results)))
		} else if m.state.SearchQuery() != "" {
			b.WriteString("   No matches\n")
		}
		b.WriteString(strings.Repeat("─", 80))
		b.WriteString("\n")
	}

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
	b.WriteString(strings.Repeat("─", 80))
	b.WriteString("\n")

	// Message
	if m.message != "" {
		b.WriteString(m.message)
		b.WriteString("\n")
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
	
	// Title
	entryType := "Global"
	if e.IsIncremental() {
		entryType = "Incremental"
	}
	b.WriteString(fmt.Sprintf("Entry #%d (%s)\n", m.state.selectedEntry, entryType))
	b.WriteString(strings.Repeat("─", 80))
	b.WriteString("\n")
	
	// Properties
	b.WriteString(fmt.Sprintf("  Type:     %s\n", e.GetType().String()))
	b.WriteString(fmt.Sprintf("  Start:    %s\n", formatTS(e.GetStart())))
	b.WriteString(fmt.Sprintf("  End:      %s\n", formatTS(e.GetEnd())))
	b.WriteString(fmt.Sprintf("  State:    %s\n", stateStr(e.GetState())))
	b.WriteString(fmt.Sprintf("  Version:  %d\n", e.GetVersion()))
	b.WriteString(fmt.Sprintf("  CKP LSN:  %d\n", e.LSN()))
	b.WriteString(fmt.Sprintf("  Trunc LSN:%d\n", e.GetTruncateLsn()))
	
	b.WriteString("\nLocations:\n")
	b.WriteString(fmt.Sprintf("  CN Loc:   %s\n", e.GetLocation().String()))

	tnLoc := e.GetTNLocation()
	if !tnLoc.IsEmpty() && tnLoc.String() != e.GetLocation().String() {
		b.WriteString(fmt.Sprintf("  TN Loc:   %s\n", tnLoc.String()))
	} else {
		b.WriteString("  TN Loc:   (same as CN)\n")
	}

	tableIDLocs := e.GetTableIDLocation()
	if tableIDLocs.Len() > 0 {
		b.WriteString(fmt.Sprintf("  TableID Locations (%d files):\n", tableIDLocs.Len()))
		maxShow := 5
		for i := 0; i < tableIDLocs.Len() && i < maxShow; i++ {
			b.WriteString(fmt.Sprintf("    [%d] %s\n", i, tableIDLocs.Get(i).String()))
		}
		if tableIDLocs.Len() > maxShow {
			b.WriteString(fmt.Sprintf("    ... and %d more\n", tableIDLocs.Len()-maxShow))
		}
	}
	
	b.WriteString("\nPress: [a] Accounts  [t] Tables  [l] Logical  [b] Back")
	return b.String()
}

func (m model) renderAccounts() string {
	var b strings.Builder
	
	// Overview panel
	accounts := m.state.accounts
	totalTables := 0
	totalData := 0
	totalTomb := 0
	for _, acc := range accounts {
		totalTables += acc.TableCount
		totalData += acc.DataRanges
		totalTomb += acc.TombRanges
	}
	
	b.WriteString(fmt.Sprintf("Accounts in Entry #%d\n", m.state.selectedEntry))
	b.WriteString(fmt.Sprintf("Overview: %d accounts │ %d tables │ %d data objects │ %d tombstone objects\n",
		len(accounts), totalTables, totalData, totalTomb))
	b.WriteString(strings.Repeat("─", 80))
	b.WriteString("\n")
	
	b.WriteString("  # │ AccountID │ Tables │ Data │ Tomb\n")
	b.WriteString("────┼───────────┼────────┼──────┼──────\n")

	searchResults := make(map[int]bool)
	// Always show search results if they exist (even in search mode)
	for _, idx := range m.state.SearchResult() {
		searchResults[idx] = true
	}

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
		// Highlight search results
		highlight := ""
		if searchResults[i] {
			highlight = "✓ "
		}
		b.WriteString(fmt.Sprintf("%s%s%2d │ %9d │ %6d │ %4d │ %4d\n",
			cursor, highlight, i, acc.AccountID, acc.TableCount, acc.DataRanges, acc.TombRanges))
	}

	// Show search hint
	if !m.state.IsSearchMode() && m.state.mode == ViewModeAccount {
		// Show active search info if exists
		if results := m.state.SearchResult(); len(results) > 0 {
			b.WriteString(fmt.Sprintf("\n🔍 Active search: \"%s\" (%d matches) - Press n/N to navigate, / for new search", 
				m.state.SearchQuery(), len(results)))
		} else {
			b.WriteString("\nPress [/] to search by Account ID")
		}
	}

	return b.String()
}

func (m model) renderTables() string {
	var b strings.Builder
	
	// Overview panel
	tables := m.state.tables
	totalData := 0
	totalTomb := 0
	for _, tbl := range tables {
		totalData += len(tbl.DataRanges)
		totalTomb += len(tbl.TombRanges)
	}
	
	if m.state.mode == ViewModeAccountTables {
		b.WriteString(fmt.Sprintf("Tables for Account %d\n", m.state.selectedAccount))
	} else {
		b.WriteString(fmt.Sprintf("All Tables in Entry #%d\n", m.state.selectedEntry))
	}
	b.WriteString(fmt.Sprintf("Overview: %d tables │ %d data objects │ %d tombstone objects\n", 
		len(tables), totalData, totalTomb))
	b.WriteString(strings.Repeat("─", 80))
	b.WriteString("\n")
	
	b.WriteString("  # │ TableID            │ Account │ Data │ Tomb\n")
	b.WriteString("────┼────────────────────┼─────────┼──────┼──────\n")

	searchResults := make(map[int]bool)
	// Always show search results if they exist (even in search mode)
	for _, idx := range m.state.SearchResult() {
		searchResults[idx] = true
	}

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
		// Highlight search results
		highlight := ""
		if searchResults[i] {
			highlight = "✓ "
		}
		b.WriteString(fmt.Sprintf("%s%s%2d │ %18d │ %7d │ %4d │ %4d\n",
			cursor, highlight, i, tbl.TableID, tbl.AccountID, len(tbl.DataRanges), len(tbl.TombRanges)))
	}

	// Show search hint
	if !m.state.IsSearchMode() && (m.state.mode == ViewModeTable || m.state.mode == ViewModeAccountTables) {
		// Show active search info if exists
		if results := m.state.SearchResult(); len(results) > 0 {
			b.WriteString(fmt.Sprintf("\n🔍 Active search: \"%s\" (%d matches) - Press n/N to navigate, / for new search", 
				m.state.SearchQuery(), len(results)))
		} else {
			b.WriteString("\nPress [/] to search by Table ID")
		}
	}

	return b.String()
}

func (m model) renderTableDetail() string {
	tbl := m.state.GetSelectedTable()
	if tbl == nil {
		return "No table selected"
	}

	var b strings.Builder
	
	// Overview
	b.WriteString(fmt.Sprintf("Table %d (Account: %d)\n", tbl.TableID, tbl.AccountID))
	b.WriteString(fmt.Sprintf("Overview: %d data objects │ %d tombstone objects\n",
		len(tbl.DataRanges), len(tbl.TombRanges)))
	b.WriteString(strings.Repeat("─", 80))
	b.WriteString("\n")

	if len(tbl.DataRanges) > 0 {
		b.WriteString(fmt.Sprintf("\nData Ranges (%d):\n", len(tbl.DataRanges)))
		maxShow := 10
		for i, r := range tbl.DataRanges {
			if i >= maxShow {
				b.WriteString(fmt.Sprintf("  ... and %d more\n", len(tbl.DataRanges)-maxShow))
				break
			}
			b.WriteString(fmt.Sprintf("  %s\n", rangeStr(r)))
		}
	}

	if len(tbl.TombRanges) > 0 {
		b.WriteString(fmt.Sprintf("\nTombstone Ranges (%d):\n", len(tbl.TombRanges)))
		maxShow := 10
		for i, r := range tbl.TombRanges {
			if i >= maxShow {
				b.WriteString(fmt.Sprintf("  ... and %d more\n", len(tbl.TombRanges)-maxShow))
				break
			}
			b.WriteString(fmt.Sprintf("  %s\n", rangeStr(r)))
		}
	}
	
	b.WriteString("\nPress [b] Back")
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
	finalModel, err := p.Run()
	if err == nil {
		if m, ok := finalModel.(model); ok {
			m.saveSearchHistory()
		}
	}
	return err
}

// loadSearchHistory loads search history from file
func (m *model) loadSearchHistory() {
	historyFile := getSearchHistoryFile()
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
			m.searchHistory = append(m.searchHistory, line)
		}
	}
	m.historyIndex = len(m.searchHistory)
}

// saveSearchHistory saves search history to file
func (m *model) saveSearchHistory() {
	historyFile := getSearchHistoryFile()
	if historyFile == "" {
		return
	}

	// Ensure directory exists
	dir := filepath.Dir(historyFile)
	os.MkdirAll(dir, 0755)

	// Only save last 100 records
	start := 0
	if len(m.searchHistory) > 100 {
		start = len(m.searchHistory) - 100
	}

	content := strings.Join(m.searchHistory[start:], "\n")
	os.WriteFile(historyFile, []byte(content), 0644)
}

// getSearchHistoryFile returns search history file path
func getSearchHistoryFile() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(homeDir, ".mo_checkpoint_search_history")
}

// jumpToNextMatch jumps to next search match
func (m *model) jumpToNextMatch() {
	results := m.state.SearchResult()
	if len(results) == 0 {
		return
	}

	// Find next match after current cursor
	for i, idx := range results {
		if idx > m.cursor {
			m.matchIndex = i
			m.cursor = idx
			m.adjustScrollToShowCursor()
			m.message = fmt.Sprintf("Match %d/%d", i+1, len(results))
			return
		}
	}

	// Wrap to first match
	m.matchIndex = 0
	m.cursor = results[0]
	m.adjustScrollToShowCursor()
	m.message = fmt.Sprintf("Match 1/%d (wrapped)", len(results))
}

// jumpToPrevMatch jumps to previous search match
func (m *model) jumpToPrevMatch() {
	results := m.state.SearchResult()
	if len(results) == 0 {
		return
	}

	// Find previous match before current cursor
	for i := len(results) - 1; i >= 0; i-- {
		if results[i] < m.cursor {
			m.matchIndex = i
			m.cursor = results[i]
			m.adjustScrollToShowCursor()
			m.message = fmt.Sprintf("Match %d/%d", i+1, len(results))
			return
		}
	}

	// Wrap to last match
	m.matchIndex = len(results) - 1
	m.cursor = results[m.matchIndex]
	m.adjustScrollToShowCursor()
	m.message = fmt.Sprintf("Match %d/%d (wrapped)", len(results), len(results))
}

// adjustScrollToShowCursor adjusts scroll offset to show cursor
func (m *model) adjustScrollToShowCursor() {
	// Ensure cursor is visible
	if m.cursor < m.state.scrollOffset {
		m.state.scrollOffset = m.cursor
		if m.state.scrollOffset > 0 {
			m.state.scrollOffset-- // Show one line above for context
		}
	} else if m.cursor >= m.state.scrollOffset+m.state.pageSize {
		m.state.scrollOffset = m.cursor - m.state.pageSize + 1
	}
}
