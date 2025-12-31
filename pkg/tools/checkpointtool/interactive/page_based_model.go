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
	tea "github.com/charmbracelet/bubbletea"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	"github.com/matrixorigin/matrixone/pkg/tools/interactive"
)

// PageBasedModel uses the page framework for all views
type PageBasedModel struct {
	state       *State
	currentPage *interactive.Page
	
	// Navigation stack for back functionality
	pageStack []*interactive.Page
}

func NewPageBasedModel(reader *checkpointtool.CheckpointReader) *PageBasedModel {
	state := NewState(reader)
	
	// Create initial accounts page
	accountsProvider := NewAccountsDataProvider(state)
	accountsHandler := NewAccountsActionHandler(state, 
		func(accountID uint32) tea.Cmd {
			// Switch to tables page for this account
			return func() tea.Msg { return SwitchToTablesMsg{AccountID: accountID} }
		},
		func() tea.Cmd {
			// Switch to all tables
			return func() tea.Msg { return SwitchToAllTablesMsg{} }
		})
	
	config := interactive.PageConfig{
		EnableRowNumbers: true,
		EnableCursor:     true,
		EnableSearch:     true,
		MaxColWidth:      50,
	}
	
	accountsPage := interactive.NewPage(config, accountsProvider, accountsHandler)
	
	return &PageBasedModel{
		state:       state,
		currentPage: accountsPage,
		pageStack:   []*interactive.Page{},
	}
}

// Custom messages for page navigation
type SwitchToTablesMsg struct {
	AccountID uint32
}

type SwitchToAllTablesMsg struct{}

type BackToPreviousPageMsg struct{}

func (m *PageBasedModel) Init() tea.Cmd {
	return m.currentPage.Init()
}

func (m *PageBasedModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case SwitchToTablesMsg:
		// Push current page to stack
		m.pageStack = append(m.pageStack, m.currentPage)
		
		// Create tables page
		tablesProvider := NewFilteredTablesDataProvider(m.state, msg.AccountID)
		tablesHandler := NewTablesActionHandler(m.state,
			func(tableIndex int) tea.Cmd {
				// Handle table selection
				return nil
			},
			func() tea.Cmd {
				// Back to accounts
				return func() tea.Msg { return BackToPreviousPageMsg{} }
			})
		
		config := interactive.PageConfig{
			EnableRowNumbers: true,
			EnableCursor:     true,
			EnableSearch:     true,
			MaxColWidth:      50,
		}
		
		m.currentPage = interactive.NewPage(config, tablesProvider, tablesHandler)
		return m, m.currentPage.Init()
		
	case SwitchToAllTablesMsg:
		// Push current page to stack
		m.pageStack = append(m.pageStack, m.currentPage)
		
		// Create all tables page
		tablesProvider := NewTablesDataProvider(m.state)
		tablesHandler := NewTablesActionHandler(m.state,
			func(tableIndex int) tea.Cmd {
				// Handle table selection
				return nil
			},
			func() tea.Cmd {
				// Back to accounts
				return func() tea.Msg { return BackToPreviousPageMsg{} }
			})
		
		config := interactive.PageConfig{
			EnableRowNumbers: true,
			EnableCursor:     true,
			EnableSearch:     true,
			MaxColWidth:      50,
		}
		
		m.currentPage = interactive.NewPage(config, tablesProvider, tablesHandler)
		return m, m.currentPage.Init()
		
	case BackToPreviousPageMsg:
		// Pop from stack
		if len(m.pageStack) > 0 {
			m.currentPage = m.pageStack[len(m.pageStack)-1]
			m.pageStack = m.pageStack[:len(m.pageStack)-1]
		}
		return m, nil
		
	case tea.KeyMsg:
		if msg.String() == "q" {
			return m, tea.Quit
		}
		// Handle ESC for back navigation
		if msg.String() == "esc" && len(m.pageStack) > 0 {
			return m.Update(BackToPreviousPageMsg{})
		}
	}
	
	// Delegate to current page
	newPage, cmd := m.currentPage.Update(msg)
	m.currentPage = newPage
	return m, cmd
}

func (m *PageBasedModel) View() string {
	return m.currentPage.View()
}
