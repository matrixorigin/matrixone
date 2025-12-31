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
	"fmt"
	"regexp"
	"strconv"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/matrixorigin/matrixone/pkg/tools/interactive"
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
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
// ObjectListDataProvider provides data for object list view
type ObjectListDataProvider struct {
	objectPath string
	reader     *objecttool.ObjectReader
	rows       [][]string
	headers    []string
}

func NewObjectListDataProvider(objectPath string, startRow, endRow int64) (*ObjectListDataProvider, error) {
	ctx := context.Background()
	reader, err := objecttool.Open(ctx, objectPath)
	if err != nil {
		return nil, err
	}

	provider := &ObjectListDataProvider{
		objectPath: objectPath,
		reader:     reader,
		headers:    []string{"Row", "Data"},
	}

	// Load data with range
	if err := provider.loadData(startRow, endRow); err != nil {
		reader.Close()
		return nil, err
	}

	return provider, nil
}

func (p *ObjectListDataProvider) loadData(startRow, endRow int64) error {
	ctx := context.Background()
	info := p.reader.Info()
	if startRow < 0 {
		startRow = 0
	}
	if endRow < 0 || endRow >= int64(info.RowCount) {
		endRow = int64(info.RowCount) - 1
	}

	// Read data by blocks
	p.rows = make([][]string, 0, endRow-startRow+1)
	currentRow := int64(0)
	
	for blockIdx := uint32(0); blockIdx < info.BlockCount; blockIdx++ {
		batch, release, err := p.reader.ReadBlock(ctx, blockIdx)
		if err != nil {
			continue
		}
		
		blockRowCount := int64(batch.RowCount())
		
		// Check if this block contains rows we need
		if currentRow+blockRowCount <= startRow {
			currentRow += blockRowCount
			release()
			continue
		}
		
		if currentRow > endRow {
			release()
			break
		}
		
		// Process rows in this block
		for i := int64(0); i < blockRowCount; i++ {
			rowIdx := currentRow + i
			if rowIdx < startRow {
				continue
			}
			if rowIdx > endRow {
				break
			}
			
			// Convert row data to string representation
			rowData := make([]string, len(batch.Vecs))
			for j, vec := range batch.Vecs {
				if vec.Length() > int(i) {
					// Use vector's String method for display
					rowData[j] = vec.String()
				} else {
					rowData[j] = "NULL"
				}
			}
			
			p.rows = append(p.rows, []string{
				fmt.Sprintf("%d", rowIdx),
				strings.Join(rowData, " | "),
			})
		}
		
		currentRow += blockRowCount
		release()
	}

	return nil
}

func (p *ObjectListDataProvider) GetRows() [][]string {
	return p.rows
}

func (p *ObjectListDataProvider) GetHeaders() []string {
	return p.headers
}

func (p *ObjectListDataProvider) GetTitle() string {
	return fmt.Sprintf("Object Data: %s", p.objectPath)
}

func (p *ObjectListDataProvider) GetOverview() string {
	if p.reader == nil {
		return "No data"
	}
	info := p.reader.Info()
	return fmt.Sprintf("Total rows: %d, Blocks: %d", info.RowCount, info.BlockCount)
}

func (p *ObjectListDataProvider) GetHints() string {
	return "Press ESC to go back, q to quit"
}

func (p *ObjectListDataProvider) Close() {
	if p.reader != nil {
		p.reader.Close()
	}
}

// ObjectListActionHandler handles actions for object list view
type ObjectListActionHandler struct {
	provider *ObjectListDataProvider
}

func NewObjectListActionHandler(provider *ObjectListDataProvider) *ObjectListActionHandler {
	return &ObjectListActionHandler{provider: provider}
}

func (h *ObjectListActionHandler) HandleSelect(rowIndex int) tea.Cmd {
	// Could implement row detail view here
	return nil
}

func (h *ObjectListActionHandler) HandleCustomKey(key string) tea.Cmd {
	return nil
}

func (h *ObjectListActionHandler) CanFilter() bool {
	return false
}

func (h *ObjectListActionHandler) ApplyFilter(filter string) interactive.PageDataProvider {
	return h.provider
}

func (h *ObjectListActionHandler) CanSearch() bool {
	return true
}

func (h *ObjectListActionHandler) Search(query string) []int {
	var matches []int
	for i, row := range h.provider.GetRows() {
		for _, cell := range row {
			if strings.Contains(strings.ToLower(cell), strings.ToLower(query)) {
				matches = append(matches, i)
				break
			}
		}
	}
	return matches
}
