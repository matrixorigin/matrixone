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
	"sync"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	"github.com/matrixorigin/matrixone/pkg/tools/interactive"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
)

// Messages for page navigation
type selectCheckpointMsg struct{ idx int }
type selectTableMsg struct{ tableID uint64 }
type openObjectMsg struct{ path string }
type openLogicalTableMsg struct{}
type logicalTableLoadedMsg struct {
	loadID     uint64
	entryIndex int
	tableID    uint64
	view       *checkpointtool.LogicalTableView
	err        error
}
type goBackMsg struct{}

// UnifiedModel uses GenericPage for all views
type UnifiedModel struct {
	state       *State
	currentPage *interactive.GenericPage
	pageStack   []*interactive.GenericPage

	// For opening objects
	objectToOpen      string
	rangeToOpen       *ckputil.TableRange
	quitting          bool
	logicalLoadCancel context.CancelFunc
	logicalLoadWG     sync.WaitGroup
	logicalLoading    bool
	logicalLoadID     uint64
}

// NewUnifiedModel creates a new unified model
func NewUnifiedModel(reader *checkpointtool.CheckpointReader) *UnifiedModel {
	state := NewState(reader)
	m := &UnifiedModel{
		state:     state,
		pageStack: make([]*interactive.GenericPage, 0),
	}

	// Create initial checkpoint list page
	m.currentPage = m.createCheckpointListPage()
	return m
}

func (m *UnifiedModel) createCheckpointListPage() *interactive.GenericPage {
	config := interactive.PageConfig{
		Title:         "═══ Checkpoint List ═══",
		Headers:       []string{"Type", "LSN", "Start", "End", "State", "Ver"},
		ShowRowNumber: true,
		EnableCursor:  true,
		EnableSearch:  true,
		EnableHScroll: true,
		EnableBack:    false,
	}
	provider := &CheckpointListProvider{state: m.state}
	handler := newCheckpointListHandler()
	return interactive.NewGenericPage(config, provider, handler)
}

func (m *UnifiedModel) createTablesListPage() *interactive.GenericPage {
	config := interactive.PageConfig{
		Title:         "═══ Tables ═══",
		Headers:       []string{"Account", "TableID", "Data", "Tomb"},
		ShowRowNumber: true,
		EnableCursor:  true,
		EnableSearch:  true,
		EnableFilter:  true,
		EnableHScroll: true,
		EnableBack:    true,
	}
	provider := &TablesListProvider{state: m.state}
	handler := &tablesListHandler{state: m.state}
	return interactive.NewGenericPage(config, provider, handler)
}

func (m *UnifiedModel) createTableDetailPage() *interactive.GenericPage {
	config := interactive.PageConfig{
		Title:         "═══ Table Detail ═══",
		Headers:       []string{"Type", "Object", "Blocks", "Rows", "Size", "Created"},
		ShowRowNumber: true,
		EnableCursor:  true,
		EnableSearch:  true,
		EnableHScroll: true,
		EnableBack:    true,
		MaxColWidth:   50, // Limit column width, use h/l to scroll
		CustomHints:   "[j/k] Navigate [Enter] Open object [L] Logical table [h/l] Scroll [/] Search [b/ESC] Back [q] Quit",
	}
	provider := &TableDetailProvider{state: m.state}
	handler := &tableDetailHandler{state: m.state}
	return interactive.NewGenericPage(config, provider, handler)
}

func (m *UnifiedModel) createLogicalTablePage() *interactive.GenericPage {
	headers := []string{"object", "block", "row"}
	if view := m.state.LogicalView(); view != nil && len(view.Headers) > 0 {
		headers = view.Headers
	}
	config := interactive.PageConfig{
		Title:         "═══ Logical Table View ═══",
		Headers:       headers,
		ShowRowNumber: true,
		EnableCursor:  false,
		EnableSearch:  true,
		EnableHScroll: true,
		EnableBack:    true,
		MaxColWidth:   40,
	}
	provider := &LogicalTableProvider{state: m.state}
	handler := &logicalTableHandler{}
	return interactive.NewGenericPage(config, provider, handler)
}

func (m *UnifiedModel) Init() tea.Cmd {
	return m.currentPage.Init()
}

func (m *UnifiedModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.currentPage.SetSize(msg.Width, msg.Height)
		return m, nil

	case selectCheckpointMsg:
		m.state.selectedEntry = msg.idx
		if err := m.state.SwitchToTables(); err != nil {
			return m, nil
		}
		m.pageStack = append(m.pageStack, m.currentPage)
		m.currentPage = m.createTablesListPage()
		m.currentPage.Refresh()
		return m, nil

	case selectTableMsg:
		if err := m.state.SelectTable(msg.tableID); err != nil {
			return m, nil
		}
		m.pageStack = append(m.pageStack, m.currentPage)
		m.currentPage = m.createTableDetailPage()
		m.currentPage.Refresh()
		return m, nil

	case openObjectMsg:
		m.objectToOpen = msg.path
		idx := m.currentPage.GetCursor()
		dataEntries := m.state.DataEntries()
		tombEntries := m.state.TombEntries()
		if idx < len(dataEntries) {
			m.rangeToOpen = &dataEntries[idx].Range
		} else if idx < len(dataEntries)+len(tombEntries) {
			m.rangeToOpen = &tombEntries[idx-len(dataEntries)].Range
		}
		m.quitting = true
		return m, tea.Quit

	case openLogicalTableMsg:
		if m.state.LogicalView() != nil {
			m.pageStack = append(m.pageStack, m.currentPage)
			m.currentPage = m.createLogicalTablePage()
			m.currentPage.Refresh()
			return m, nil
		}
		if m.logicalLoading {
			return m, nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		m.logicalLoadCancel = cancel
		m.logicalLoadWG.Add(1)
		m.logicalLoading = true
		m.logicalLoadID++
		loadID := m.logicalLoadID
		entryIndex := m.state.selectedEntry
		tableID := m.state.selectedTable
		return m, func() tea.Msg {
			defer m.logicalLoadWG.Done()
			view, err := m.state.BuildLogicalView(ctx, entryIndex, tableID)
			return logicalTableLoadedMsg{loadID: loadID, entryIndex: entryIndex, tableID: tableID, view: view, err: err}
		}

	case logicalTableLoadedMsg:
		if msg.loadID != m.logicalLoadID {
			return m, nil
		}
		m.logicalLoading = false
		m.logicalLoadCancel = nil
		if msg.err != nil || msg.entryIndex != m.state.selectedEntry || msg.tableID != m.state.selectedTable {
			return m, nil
		}
		m.state.logicalView = msg.view
		m.pageStack = append(m.pageStack, m.currentPage)
		m.currentPage = m.createLogicalTablePage()
		m.currentPage.Refresh()
		return m, nil

	case goBackMsg:
		m.cancelLogicalLoad()
		if len(m.pageStack) > 0 {
			m.currentPage = m.pageStack[len(m.pageStack)-1]
			m.pageStack = m.pageStack[:len(m.pageStack)-1]
			if len(m.pageStack) == 0 {
				m.state.mode = ViewModeList
			} else {
				m.state.mode = ViewModeTable
			}
		}
		return m, nil

	case tea.KeyMsg:
		if msg.String() == "q" {
			m.cancelLogicalLoad()
			return m, tea.Quit
		}
	}

	// Delegate to current page
	newPage, cmd := m.currentPage.Update(msg)
	m.currentPage = newPage
	return m, cmd
}

func (m *UnifiedModel) cancelLogicalLoad() {
	if m.logicalLoadCancel != nil {
		m.logicalLoadCancel()
		m.logicalLoadCancel = nil
		m.logicalLoadID++
	}
	m.logicalLoading = false
}

// CancelAndWaitLogicalLoad transfers shutdown ownership back from the Bubble
// Tea command before the caller closes the checkpoint reader and file service.
func (m *UnifiedModel) CancelAndWaitLogicalLoad() {
	m.cancelLogicalLoad()
	m.logicalLoadWG.Wait()
}

func (m *UnifiedModel) View() string {
	return m.currentPage.View()
}

// GetObjectToOpen returns the object path to open (if any)
func (m *UnifiedModel) GetObjectToOpen() string {
	return m.objectToOpen
}

// GetRangeToOpen returns the range to open (if any)
func (m *UnifiedModel) GetRangeToOpen() *ckputil.TableRange {
	return m.rangeToOpen
}

// ClearObjectToOpen clears the object to open flag
func (m *UnifiedModel) ClearObjectToOpen() {
	m.objectToOpen = ""
	m.rangeToOpen = nil
}
