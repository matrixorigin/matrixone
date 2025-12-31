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
	"path/filepath"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	"github.com/matrixorigin/matrixone/pkg/tools/interactive"
	objectinteractive "github.com/matrixorigin/matrixone/pkg/tools/objecttool/interactive"
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

  Filter (Table view):
    f       - Filter by Account ID
    Esc     - Clear filter

  Search (Table/Account views):
    /       - Enter search mode  Esc     - Exit search
    ↑/↓     - Navigate history   Enter   - Confirm & jump to first
    n       - Next match         N       - Previous match
    Supports regex: .* + ? ^ $ {} [] | () \
    Examples: /100.*  /^10  /[0-9]{3}
    History: ~/.mo_checkpoint_search_history

  Object Expansion (Table Detail view):
    Enter   - Toggle expand      e       - Expand all
    c       - Collapse all

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

	// Shared view components
	tableView *interactive.TableView
	listView  *interactive.ListView

	// Filter input mode
	filterInputMode bool
	filterInput     string

	// Search history
	searchHistory *interactive.HistoryManager
	matchIndex    int // Current match index in search results

	// Object expansion state (for table detail view)
	expandedObjects map[int]bool // Index -> expanded

	// Object to open after quit
	objectToOpen string
	rangeToOpen  *ckputil.TableRange // Range info for filtering rows
}

func newModel(reader *checkpointtool.CheckpointReader) model {
	state := NewState(reader)
	m := model{
		state:           state,
		expandedObjects: make(map[int]bool),
		searchHistory:   interactive.NewHistoryManager(100),
	}

	// Initialize table view with config
	tableConfig := interactive.TableConfig{
		Headers:      []string{"TableID", "Account", "Data", "Tomb"},
		ColumnWidths: []int{18, 7, 4, 4},
		AllowCursor:  true,
		AllowSearch:  true,
		AllowFilter:  true,
	}
	dataSource := NewTableDataSource(state)
	m.tableView = interactive.NewTableView(tableConfig, dataSource, 40)

	// Initialize list view for entry list
	listConfig := interactive.ListConfig{
		Headers:      []string{"#", "Type", "Start", "End", "State"},
		ColumnWidths: []int{7, 6, 51, 51, 8},
		ShowCursor:   true,
		CursorWidth:  2,
	}
	entryItems := make([]interactive.ListItem, len(state.entries))
	for i, entry := range state.entries {
		entryItems[i] = &EntryListItem{entry: entry, index: i}
	}
	m.listView = interactive.NewListView(listConfig, entryItems, 20)

	// Load search history
	m.searchHistory.LoadFromFile("checkpoint_search_history.txt")

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

	// Handle filter input mode
	if m.filterInputMode {
		switch msg.String() {
		case "esc":
			m.filterInputMode = false
			m.filterInput = ""
			return m, nil
		case "enter":
			if m.filterInput == "" {
				// Clear filter
				m.state.ClearAccountFilter()
				m.message = "Filter cleared"
			} else {
				// Parse and apply filter
				var accountID int64
				if _, err := fmt.Sscanf(m.filterInput, "%d", &accountID); err == nil {
					m.state.SetAccountFilter(accountID)
					m.cursor = 0
					m.message = fmt.Sprintf("Filtered by Account ID: %d", accountID)
				} else {
					m.message = "Invalid Account ID"
				}
			}
			m.filterInputMode = false
			m.filterInput = ""
			return m, nil
		case "backspace":
			if len(m.filterInput) > 0 {
				m.filterInput = m.filterInput[:len(m.filterInput)-1]
			}
			return m, nil
		default:
			// Append digit
			if len(msg.String()) == 1 && msg.String()[0] >= '0' && msg.String()[0] <= '9' {
				m.filterInput += msg.String()
			}
			return m, nil
		}
	}

	// Handle search mode
	if m.state.IsSearchMode() {
		switch msg.String() {
		case "esc":
			m.state.ExitSearchMode()
			m.searchHistory.Reset()
			return m, nil
		case "enter":
			// Save to history
			query := m.state.SearchQuery()
			m.searchHistory.Add(query)
			m.state.ExitSearchMode()

			// Sync search with tableView if in table mode
			if m.state.mode == ViewModeTable || m.state.mode == ViewModeAccountTables {
				m.tableView.Search(query)
				m.cursor = m.tableView.GetCursor()
			} else {
				// Jump to first result if any (even if not on current page)
				if results := m.state.SearchResult(); len(results) > 0 {
					m.cursor = results[0]
					// Adjust scroll to show the result
					m.state.scrollOffset = m.cursor
					if m.state.scrollOffset > 0 {
						m.state.scrollOffset-- // Show one line above for context
					}
				}
			}
			return m, nil
		case "backspace":
			m.state.BackspaceSearchQuery()
			m.searchHistory.Reset()
			return m, nil
		case "up":
			// History up
			if query := m.searchHistory.Up(); query != "" {
				m.state.SetSearchQuery(query)
			}
			return m, nil
		case "down":
			// History down
			query := m.searchHistory.Down()
			m.state.SetSearchQuery(query)
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
				m.searchHistory.Reset()
			}
			return m, nil
		}
	}

	switch msg.String() {
	case "q", "ctrl+c":
		m.quitting = true
		m.searchHistory.SaveToFile("checkpoint_search_history.txt")
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

	case "f":
		// Enter filter mode (only in table view)
		if m.state.mode == ViewModeTable || m.state.mode == ViewModeAccountTables {
			m.filterInputMode = true
			m.filterInput = ""
		}
		return m, nil

	case "e":
		// Expand all (in table detail view)
		if m.state.mode == ViewModeTableDetail {
			totalEntries := len(m.state.DataEntries()) + len(m.state.TombEntries())
			for i := 0; i < totalEntries; i++ {
				m.expandedObjects[i] = true
			}
		}
		return m, nil

	case "c":
		// Collapse all (in table detail view)
		if m.state.mode == ViewModeTableDetail {
			m.expandedObjects = make(map[int]bool)
		}
		return m, nil

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

	// Sync with tableView if in table mode
	if m.state.mode == ViewModeTable || m.state.mode == ViewModeAccountTables {
		m.tableView.SetCursor(m.cursor)
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
		return len(m.state.FilteredTables())
	case ViewModeTableDetail:
		return len(m.state.DataEntries()) + len(m.state.TombEntries())
	default:
		return 1
	}
}

func (m model) handleEnter() (tea.Model, tea.Cmd) {
	switch m.state.mode {
	case ViewModeList:
		// Go directly to table view instead of entry overview
		m.state.selectedEntry = m.cursor
		if err := m.state.SwitchToTables(); err != nil {
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
		tables := m.state.FilteredTables()
		if m.cursor < len(tables) {
			tbl := tables[m.cursor]
			if err := m.state.SelectTable(tbl.TableID); err != nil {
				m.message = fmt.Sprintf("Error: %v", err)
			}
			m.cursor = 0
			m.expandedObjects = make(map[int]bool)
		}

	case ViewModeTableDetail:
		// Open object file with objecttool viewer
		entry := m.state.GetRangeEntry(m.cursor)
		if entry != nil {
			objPath := filepath.Join(m.state.reader.Dir(), entry.Range.ObjectStats.ObjectName().String())
			m.objectToOpen = objPath
			m.rangeToOpen = &entry.Range
			return m, tea.Quit
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

	// Filter input bar (if in filter mode)
	if m.filterInputMode {
		b.WriteString(fmt.Sprintf("🔍 Filter by Account ID: %s_\n", m.filterInput))
		b.WriteString("   Enter number and press Enter (or just Enter to clear filter)\n")
		b.WriteString(strings.Repeat("─", 80))
		b.WriteString("\n")
	}

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
	// Sync cursor with listView
	m.listView.SetCursor(m.cursor)
	m.listView.SetScrollOffset(m.state.scrollOffset)

	return m.listView.Render()
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
	tables := m.state.FilteredTables()
	totalData := 0
	totalTomb := 0
	for _, tbl := range tables {
		totalData += len(tbl.DataRanges)
		totalTomb += len(tbl.TombRanges)
	}

	// Title with context
	if m.state.mode == ViewModeAccountTables {
		b.WriteString(fmt.Sprintf("📊 Tables for Account %d", m.state.selectedAccount))
	} else {
		b.WriteString(fmt.Sprintf("📊 All Tables in Entry #%d", m.state.selectedEntry))
	}

	// Show filter status inline
	if m.state.HasAccountFilter() {
		b.WriteString(fmt.Sprintf(" │ 🔍 Filter: Account=%d", m.state.GetAccountFilter()))
	}
	b.WriteString("\n")

	// Compact overview
	b.WriteString(fmt.Sprintf("   %d tables │ %d data objects │ %d tombstone objects\n",
		len(tables), totalData, totalTomb))
	b.WriteString(strings.Repeat("─", 80))
	b.WriteString("\n")

	// Use shared TableView component to render table
	b.WriteString(m.tableView.Render())

	// Show filter/search hint
	if !m.state.IsSearchMode() && (m.state.mode == ViewModeTable || m.state.mode == ViewModeAccountTables) {
		// Show active search info if exists
		if results := m.state.SearchResult(); len(results) > 0 {
			b.WriteString(fmt.Sprintf("\n🔍 Active search: \"%s\" (%d matches) - Press n/N to navigate, / for new search",
				m.state.SearchQuery(), len(results)))
		} else {
			b.WriteString("\nPress [/] to search by Table ID │ [f] to filter by Account ID")
			if m.state.HasAccountFilter() {
				b.WriteString(" │ [f]+Enter to clear filter")
			}
		}
	}

	return b.String()
}

func (m model) renderTableDetail() string {
	tbl := m.state.GetSelectedTable()
	if tbl == nil {
		return "No table selected"
	}

	dataEntries := m.state.DataEntries()
	tombEntries := m.state.TombEntries()

	var b strings.Builder

	// Overview
	b.WriteString(fmt.Sprintf("Table %d (Account: %d)\n", tbl.TableID, tbl.AccountID))
	b.WriteString(fmt.Sprintf("Overview: %d data objects │ %d tombstone objects\n",
		len(dataEntries), len(tombEntries)))
	b.WriteString(strings.Repeat("─", 80))
	b.WriteString("\n")

	// Data objects list
	if len(dataEntries) > 0 {
		b.WriteString(fmt.Sprintf("\nData Objects (%d):\n", len(dataEntries)))
		b.WriteString(" # │ Object Name                                │ Range (Block-Row)    │ Rows │ Size   │ Create Time\n")
		b.WriteString("───┼────────────────────────────────────────────┼──────────────────────┼──────┼────────┼────────────────────\n")

		start := m.state.scrollOffset
		end := start + m.state.pageSize
		if end > len(dataEntries) {
			end = len(dataEntries)
		}

		for i := start; i < end; i++ {
			entry := dataEntries[i]
			cursor := " "
			if i == m.cursor {
				cursor = "▶"
			}

			objName := entry.Range.ObjectStats.ObjectName().String()
			if len(objName) > 42 {
				objName = objName[:39] + "..."
			}

			rangeStr := fmt.Sprintf("%d-%d ~ %d-%d",
				entry.Range.Start.GetBlockOffset(), entry.Range.Start.GetRowOffset(),
				entry.Range.End.GetBlockOffset(), entry.Range.End.GetRowOffset())

			b.WriteString(fmt.Sprintf("%s%2d│ %-42s │ %-20s │ %4d │ %6s │ %s\n",
				cursor, i, objName, rangeStr,
				entry.Range.ObjectStats.Rows(),
				formatSize(entry.Range.ObjectStats.Size()),
				formatTSShort(entry.CreateTime)))
		}
	}

	// Tombstone objects list
	if len(tombEntries) > 0 {
		b.WriteString(fmt.Sprintf("\nTombstone Objects (%d):\n", len(tombEntries)))
		b.WriteString(" # │ Object Name                                │ Range (Block-Row)    │ Rows │ Size   │ Create Time         │ Delete Time\n")
		b.WriteString("───┼────────────────────────────────────────────┼──────────────────────┼──────┼────────┼─────────────────────┼────────────────────\n")

		dataCount := len(dataEntries)
		start := m.state.scrollOffset - dataCount
		if start < 0 {
			start = 0
		}
		end := start + m.state.pageSize
		if end > len(tombEntries) {
			end = len(tombEntries)
		}

		for i := start; i < end; i++ {
			entry := tombEntries[i]
			idx := dataCount + i
			cursor := " "
			if idx == m.cursor {
				cursor = "▶"
			}

			objName := entry.Range.ObjectStats.ObjectName().String()
			if len(objName) > 42 {
				objName = objName[:39] + "..."
			}

			rangeStr := fmt.Sprintf("%d-%d ~ %d-%d",
				entry.Range.Start.GetBlockOffset(), entry.Range.Start.GetRowOffset(),
				entry.Range.End.GetBlockOffset(), entry.Range.End.GetRowOffset())

			deleteTime := "-"
			if !entry.DeleteTime.IsEmpty() {
				deleteTime = formatTSShort(entry.DeleteTime)
			}

			b.WriteString(fmt.Sprintf("%s%2d│ %-42s │ %-20s │ %4d │ %6s │ %s │ %s\n",
				cursor, i, objName, rangeStr,
				entry.Range.ObjectStats.Rows(),
				formatSize(entry.Range.ObjectStats.Size()),
				formatTSShort(entry.CreateTime),
				deleteTime))
		}
	}

	b.WriteString("\nPress: [b] Back  [/] Search")
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

func truncStr(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
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
	m := newModel(reader)

	for {
		// Reset object to open
		m.objectToOpen = ""
		m.rangeToOpen = nil

		p := tea.NewProgram(m, tea.WithAltScreen())
		finalModel, err := p.Run()
		if err != nil {
			return err
		}

		var ok bool
		m, ok = finalModel.(model)
		if !ok {
			return nil
		}
		m.searchHistory.SaveToFile("checkpoint_search_history.txt")

		// Check if we need to open an object file
		if m.objectToOpen != "" && m.rangeToOpen != nil {
			// Build options with range filtering, column names and formats
			opts := &objectinteractive.ViewOptions{
				StartRow: int64(m.rangeToOpen.Start.GetRowOffset()),
				EndRow:   int64(m.rangeToOpen.End.GetRowOffset()),
				ColumnNames: map[uint16]string{
					0: ckputil.TableObjectsAttr_Accout,
					1: ckputil.TableObjectsAttr_DB,
					2: ckputil.TableObjectsAttr_Table,
					3: ckputil.TableObjectsAttr_ObjectType,
					4: ckputil.TableObjectsAttr_ID,
					5: ckputil.TableObjectsAttr_CreateTS,
					6: ckputil.TableObjectsAttr_DeleteTS,
					7: ckputil.TableObjectsAttr_Cluster,
				},
				ColumnFormats: map[uint16]string{
					4: "objectstats", // id -> objectstats format
					5: "ts",          // create_ts -> timestamp format
					6: "ts",          // delete_ts -> timestamp format
				},
			}
			if err := objectinteractive.RunBubbleteaWithOptions(m.objectToOpen, opts); err != nil {
				return err
			}
			continue
		}

		// Normal quit
		return nil
	}
}

// loadSearchHistory loads search history from file
// jumpToNextMatch jumps to next search match
func (m *model) jumpToNextMatch() {
	// Use tableView's NextMatch if in table mode
	if m.state.mode == ViewModeTable || m.state.mode == ViewModeAccountTables {
		m.tableView.NextMatch()
		m.cursor = m.tableView.GetCursor()
		m.message = m.tableView.GetSearchStatus()
		return
	}

	// Fallback to old logic for other modes
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
	// Use tableView's PrevMatch if in table mode
	if m.state.mode == ViewModeTable || m.state.mode == ViewModeAccountTables {
		m.tableView.PrevMatch()
		m.cursor = m.tableView.GetCursor()
		m.message = m.tableView.GetSearchStatus()
		return
	}

	// Fallback to old logic for other modes
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

// formatSize formats byte size to human readable format
func formatSize(bytes uint32) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := uint32(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
