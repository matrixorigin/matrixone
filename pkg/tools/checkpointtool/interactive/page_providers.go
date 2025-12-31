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
	"regexp"
	"strconv"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/matrixorigin/matrixone/pkg/tools/interactive"
)

// AccountsDataProvider provides data for accounts view
type AccountsDataProvider struct {
	state *State
}

func NewAccountsDataProvider(state *State) *AccountsDataProvider {
	return &AccountsDataProvider{state: state}
}

func (p *AccountsDataProvider) GetRows() [][]string {
	accounts := p.state.accounts
	rows := make([][]string, len(accounts))

	for i, acc := range accounts {
		rows[i] = []string{
			strconv.Itoa(i),
			strconv.Itoa(int(acc.AccountID)),
			strconv.Itoa(acc.TableCount),
			strconv.Itoa(acc.DataRanges),
			strconv.Itoa(acc.TombRanges),
		}
	}

	return rows
}

func (p *AccountsDataProvider) GetHeaders() []string {
	return []string{"#", "AccountID", "Tables", "Data", "Tomb"}
}

func (p *AccountsDataProvider) GetTitle() string {
	return fmt.Sprintf("Accounts in Entry #%d", p.state.selectedEntry)
}

func (p *AccountsDataProvider) GetOverview() string {
	accounts := p.state.accounts
	totalTables := 0
	totalData := 0
	totalTomb := 0
	for _, acc := range accounts {
		totalTables += acc.TableCount
		totalData += acc.DataRanges
		totalTomb += acc.TombRanges
	}

	return fmt.Sprintf("Overview: %d accounts │ %d tables │ %d data objects │ %d tombstone objects",
		len(accounts), totalTables, totalData, totalTomb)
}

func (p *AccountsDataProvider) GetHints() string {
	return "Press [/] to search by Account ID, [Enter] to view tables, [t] for all tables"
}

// AccountsActionHandler handles actions for accounts view
type AccountsActionHandler struct {
	state            *State
	onSelectAccount  func(accountID uint32) tea.Cmd
	onSwitchToTables func() tea.Cmd
}

func NewAccountsActionHandler(state *State, onSelectAccount func(uint32) tea.Cmd, onSwitchToTables func() tea.Cmd) *AccountsActionHandler {
	return &AccountsActionHandler{
		state:            state,
		onSelectAccount:  onSelectAccount,
		onSwitchToTables: onSwitchToTables,
	}
}

func (h *AccountsActionHandler) HandleSelect(rowIndex int) tea.Cmd {
	if rowIndex < len(h.state.accounts) {
		accountID := h.state.accounts[rowIndex].AccountID
		if h.onSelectAccount != nil {
			return h.onSelectAccount(accountID)
		}
	}
	return nil
}

func (h *AccountsActionHandler) HandleCustomKey(key string) tea.Cmd {
	switch key {
	case "t":
		if h.onSwitchToTables != nil {
			return h.onSwitchToTables()
		}
	}
	return nil
}

func (h *AccountsActionHandler) CanFilter() bool {
	return false
}

func (h *AccountsActionHandler) ApplyFilter(filter string) interactive.PageDataProvider {
	return nil
}

func (h *AccountsActionHandler) CanSearch() bool {
	return true
}

func (h *AccountsActionHandler) Search(query string) []int {
	var matches []int

	// Try to compile as regex
	regex, err := regexp.Compile(query)
	if err != nil {
		// Fallback to simple string search
		for i, acc := range h.state.accounts {
			accountStr := strconv.Itoa(int(acc.AccountID))
			if strings.Contains(accountStr, query) {
				matches = append(matches, i)
			}
		}
		return matches
	}

	// Use regex search
	for i, acc := range h.state.accounts {
		accountStr := strconv.Itoa(int(acc.AccountID))
		if regex.MatchString(accountStr) {
			matches = append(matches, i)
		}
	}

	return matches
}

// TablesDataProvider provides data for tables view
type TablesDataProvider struct {
	state *State
}

func NewTablesDataProvider(state *State) *TablesDataProvider {
	return &TablesDataProvider{state: state}
}

func (p *TablesDataProvider) GetRows() [][]string {
	tables := p.state.FilteredTables()
	rows := make([][]string, len(tables))

	for i, tbl := range tables {
		rows[i] = []string{
			strconv.Itoa(i),
			strconv.Itoa(int(tbl.AccountID)),
			strconv.Itoa(int(tbl.TableID)),
			fmt.Sprintf("Table_%d", tbl.TableID), // Use TableID as name
			strconv.Itoa(len(tbl.DataRanges)),
			strconv.Itoa(len(tbl.TombRanges)),
		}
	}

	return rows
}

func (p *TablesDataProvider) GetHeaders() []string {
	return []string{"#", "Account", "TableID", "Name", "Data", "Tomb"}
}

func (p *TablesDataProvider) GetTitle() string {
	if p.state.mode == ViewModeAccountTables {
		return fmt.Sprintf("📊 Tables for Account %d", p.state.selectedAccount)
	}
	return fmt.Sprintf("📊 All Tables in Entry #%d", p.state.selectedEntry)
}

func (p *TablesDataProvider) GetOverview() string {
	tables := p.state.FilteredTables()
	totalData := 0
	totalTomb := 0
	for _, tbl := range tables {
		totalData += len(tbl.DataRanges)
		totalTomb += len(tbl.TombRanges)
	}

	overview := fmt.Sprintf("   %d tables │ %d data objects │ %d tombstone objects",
		len(tables), totalData, totalTomb)

	// Show filter status
	if p.state.HasAccountFilter() {
		overview += fmt.Sprintf(" │ 🔍 Filter: Account=%d", p.state.GetAccountFilter())
	}

	return overview
}

func (p *TablesDataProvider) GetHints() string {
	return "Press [f] to filter by Account, [/] to search, [Enter] to view details"
}

// FilteredTablesDataProvider provides filtered tables data
type FilteredTablesDataProvider struct {
	*TablesDataProvider
	accountFilter uint32
}

func NewFilteredTablesDataProvider(state *State, accountFilter uint32) *FilteredTablesDataProvider {
	// Create a copy of state with filter applied
	filteredState := *state
	filteredState.filterAccountID = int64(accountFilter)

	return &FilteredTablesDataProvider{
		TablesDataProvider: &TablesDataProvider{state: &filteredState},
		accountFilter:      accountFilter,
	}
}

func (p *FilteredTablesDataProvider) GetTitle() string {
	return fmt.Sprintf("📊 Tables for Account %d (Filtered)", p.accountFilter)
}

// TablesActionHandler handles actions for tables view
type TablesActionHandler struct {
	state              *State
	onSelectTable      func(tableIndex int) tea.Cmd
	onSwitchToAccounts func() tea.Cmd
}

func NewTablesActionHandler(state *State, onSelectTable func(int) tea.Cmd, onSwitchToAccounts func() tea.Cmd) *TablesActionHandler {
	return &TablesActionHandler{
		state:              state,
		onSelectTable:      onSelectTable,
		onSwitchToAccounts: onSwitchToAccounts,
	}
}

func (h *TablesActionHandler) HandleSelect(rowIndex int) tea.Cmd {
	if h.onSelectTable != nil {
		return h.onSelectTable(rowIndex)
	}
	return nil
}

func (h *TablesActionHandler) HandleCustomKey(key string) tea.Cmd {
	switch key {
	case "a":
		if h.onSwitchToAccounts != nil {
			return h.onSwitchToAccounts()
		}
	case "escape":
		// Clear filter
		h.state.ClearAccountFilter()
	}
	return nil
}

func (h *TablesActionHandler) CanFilter() bool {
	return true
}

func (h *TablesActionHandler) ApplyFilter(filter string) interactive.PageDataProvider {
	// Try to parse as account ID
	if accountID, err := strconv.ParseUint(filter, 10, 32); err == nil {
		return NewFilteredTablesDataProvider(h.state, uint32(accountID))
	}
	return nil
}

func (h *TablesActionHandler) CanSearch() bool {
	return true
}

func (h *TablesActionHandler) Search(query string) []int {
	var matches []int
	tables := h.state.FilteredTables()

	// Try to compile as regex
	regex, err := regexp.Compile(query)
	if err != nil {
		// Fallback to simple string search
		for i, tbl := range tables {
			searchText := fmt.Sprintf("%d %d Table_%d", tbl.AccountID, tbl.TableID, tbl.TableID)
			if strings.Contains(strings.ToLower(searchText), strings.ToLower(query)) {
				matches = append(matches, i)
			}
		}
		return matches
	}

	// Use regex search
	for i, tbl := range tables {
		searchText := fmt.Sprintf("%d %d Table_%d", tbl.AccountID, tbl.TableID, tbl.TableID)
		if regex.MatchString(searchText) {
			matches = append(matches, i)
		}
	}

	return matches
}
