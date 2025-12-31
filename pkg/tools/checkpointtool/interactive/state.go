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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
)

// ViewMode represents the current view
type ViewMode int

const (
	ViewModeList          ViewMode = iota // Checkpoint list
	ViewModeEntry                         // Single entry detail
	ViewModeAccount                       // Account list
	ViewModeAccountTables                 // Tables under account
	ViewModeTable                         // All tables
	ViewModeTableDetail                   // Table detail (range list)
	ViewModeLogical                       // Logical view
)

// ViewState for navigation history
type ViewState struct {
	Mode         ViewMode
	EntryIndex   int
	AccountID    uint32
	TableID      uint64
	ScrollOffset int
}

// State manages checkpoint viewer state
type State struct {
	reader *checkpointtool.CheckpointReader

	// View state
	mode         ViewMode
	history      []ViewState
	scrollOffset int
	pageSize     int

	// Selection state
	selectedEntry   int
	selectedAccount uint32
	selectedTable   uint64

	// Filter state
	filterAccountID int64 // -1 means no filter, >= 0 means filter by account id

	// Search state
	searchMode   bool
	searchQuery  string
	searchRegex  *regexp.Regexp
	useRegex     bool
	searchResult []int // Indices of matching items

	// Cached data
	entries  []*checkpoint.CheckpointEntry
	accounts []*checkpointtool.AccountInfo
	tables   []*checkpointtool.TableInfo
	info     *checkpointtool.CheckpointInfo

	// Object entries cache (for table detail view)
	dataEntries []*checkpointtool.ObjectEntryInfo
	tombEntries []*checkpointtool.ObjectEntryInfo
}

// NewState creates a new state
func NewState(reader *checkpointtool.CheckpointReader) *State {
	s := &State{
		reader:          reader,
		mode:            ViewModeList,
		pageSize:        20,
		entries:         reader.Entries(),
		info:            reader.Info(),
		filterAccountID: -1, // No filter by default
	}
	return s
}

// Mode returns current view mode
func (s *State) Mode() ViewMode { return s.mode }

// SetPageSize updates the page size for pagination
func (s *State) SetPageSize(size int) {
	if size < 5 {
		size = 5
	}
	s.pageSize = size
}

// Info returns checkpoint info
func (s *State) Info() *checkpointtool.CheckpointInfo { return s.info }

// Entries returns all entries
func (s *State) Entries() []*checkpoint.CheckpointEntry { return s.entries }

// SelectedEntry returns selected entry index
func (s *State) SelectedEntry() int { return s.selectedEntry }

// SelectedAccount returns selected account ID
func (s *State) SelectedAccount() uint32 { return s.selectedAccount }

// Accounts returns cached accounts
func (s *State) Accounts() []*checkpointtool.AccountInfo { return s.accounts }

// Tables returns cached tables
func (s *State) Tables() []*checkpointtool.TableInfo { return s.tables }

// GetSelectedTable returns the currently selected table
func (s *State) GetSelectedTable() *checkpointtool.TableInfo {
	for _, tbl := range s.tables {
		if tbl.TableID == s.selectedTable {
			return tbl
		}
	}
	return nil
}

// GetRangeEntry returns the range entry at given index
func (s *State) GetRangeEntry(index int) *checkpointtool.ObjectEntryInfo {
	dataCount := len(s.dataEntries)
	if index < dataCount {
		return s.dataEntries[index]
	}
	tombIdx := index - dataCount
	if tombIdx < len(s.tombEntries) {
		return s.tombEntries[tombIdx]
	}
	return nil
}

// DataEntries returns cached data entries
func (s *State) DataEntries() []*checkpointtool.ObjectEntryInfo {
	return s.dataEntries
}

// TombEntries returns cached tombstone entries
func (s *State) TombEntries() []*checkpointtool.ObjectEntryInfo {
	return s.tombEntries
}

// ScrollOffset returns current scroll offset
func (s *State) ScrollOffset() int { return s.scrollOffset }

// PageSize returns page size
func (s *State) PageSize() int { return s.pageSize }

// Navigation methods

// SelectEntry selects an entry and switches to entry detail view
func (s *State) SelectEntry(index int) error {
	if index < 0 || index >= len(s.entries) {
		return nil
	}
	s.pushHistory()
	s.selectedEntry = index
	s.mode = ViewModeEntry
	s.scrollOffset = 0
	s.ClearSearch() // Clear search when changing view
	return nil
}

// SwitchToAccounts switches to account view
func (s *State) SwitchToAccounts() error {
	if s.selectedEntry < 0 || s.selectedEntry >= len(s.entries) {
		return nil
	}
	s.pushHistory()
	accounts, err := s.reader.GetAccounts(s.entries[s.selectedEntry])
	if err != nil {
		return err
	}
	s.accounts = accounts
	s.mode = ViewModeAccount
	s.scrollOffset = 0
	s.ClearSearch() // Clear search when changing view
	return nil
}

// SelectAccount selects an account and shows its tables
func (s *State) SelectAccount(accountID uint32) error {
	s.pushHistory()
	s.selectedAccount = accountID
	tables, err := s.reader.GetTablesByAccount(s.entries[s.selectedEntry], accountID)
	if err != nil {
		return err
	}
	s.tables = tables
	s.mode = ViewModeAccountTables
	s.scrollOffset = 0
	s.ClearSearch() // Clear search when changing view
	return nil
}

// SwitchToTables switches to all tables view
func (s *State) SwitchToTables() error {
	if s.selectedEntry < 0 || s.selectedEntry >= len(s.entries) {
		return nil
	}
	s.pushHistory()
	tables, err := s.reader.GetTables(s.entries[s.selectedEntry])
	if err != nil {
		return err
	}
	s.tables = tables
	s.mode = ViewModeTable
	s.scrollOffset = 0
	s.ClearSearch() // Clear search when changing view
	return nil
}

// FilteredTables returns tables filtered by account id if filter is active
func (s *State) FilteredTables() []*checkpointtool.TableInfo {
	if s.filterAccountID < 0 {
		return s.tables
	}
	filtered := make([]*checkpointtool.TableInfo, 0)
	for _, tbl := range s.tables {
		if int64(tbl.AccountID) == s.filterAccountID {
			filtered = append(filtered, tbl)
		}
	}
	return filtered
}

// SetAccountFilter sets account id filter
func (s *State) SetAccountFilter(accountID int64) {
	s.filterAccountID = accountID
	s.scrollOffset = 0
	s.ClearSearch()
}

// ClearAccountFilter clears account id filter
func (s *State) ClearAccountFilter() {
	s.filterAccountID = -1
	s.scrollOffset = 0
	s.ClearSearch()
}

// HasAccountFilter returns true if account filter is active
func (s *State) HasAccountFilter() bool {
	return s.filterAccountID >= 0
}

// GetAccountFilter returns current account filter value
func (s *State) GetAccountFilter() int64 {
	return s.filterAccountID
}

// SelectTable selects a table for detail view
func (s *State) SelectTable(tableID uint64) error {
	s.pushHistory()
	s.selectedTable = tableID
	s.mode = ViewModeTableDetail
	s.scrollOffset = 0

	// Load object entries with timestamps
	dataEntries, tombEntries, err := s.reader.GetObjectEntries(s.entries[s.selectedEntry], tableID)
	if err != nil {
		return err
	}
	s.dataEntries = dataEntries
	s.tombEntries = tombEntries

	return nil
}

// SwitchToLogical switches to logical view
func (s *State) SwitchToLogical() error {
	s.pushHistory()
	s.mode = ViewModeLogical
	s.scrollOffset = 0
	return nil
}

// Back returns to previous view
func (s *State) Back() {
	if len(s.history) == 0 {
		s.mode = ViewModeList
		s.scrollOffset = 0
		return
	}
	prev := s.history[len(s.history)-1]
	s.history = s.history[:len(s.history)-1]
	s.mode = prev.Mode
	s.selectedEntry = prev.EntryIndex
	s.selectedAccount = prev.AccountID
	s.selectedTable = prev.TableID
	s.scrollOffset = prev.ScrollOffset
}

// BackToList returns to checkpoint list
func (s *State) BackToList() {
	s.history = nil
	s.mode = ViewModeList
	s.scrollOffset = 0
}

func (s *State) pushHistory() {
	s.history = append(s.history, ViewState{
		Mode:         s.mode,
		EntryIndex:   s.selectedEntry,
		AccountID:    s.selectedAccount,
		TableID:      s.selectedTable,
		ScrollOffset: s.scrollOffset,
	})
}

// Scroll methods

func (s *State) ScrollUp() {
	if s.scrollOffset > 0 {
		s.scrollOffset--
	}
}

func (s *State) ScrollDown(maxItems int) {
	if s.scrollOffset < maxItems-s.pageSize {
		s.scrollOffset++
	}
}

func (s *State) PageUp() {
	s.scrollOffset -= s.pageSize
	if s.scrollOffset < 0 {
		s.scrollOffset = 0
	}
}

func (s *State) PageDown(maxItems int) {
	s.scrollOffset += s.pageSize
	if s.scrollOffset > maxItems-s.pageSize {
		s.scrollOffset = maxItems - s.pageSize
	}
	if s.scrollOffset < 0 {
		s.scrollOffset = 0
	}
}

// Search methods

// EnterSearchMode enters search mode
func (s *State) EnterSearchMode() {
	s.searchMode = true
	s.searchQuery = ""
	s.searchResult = nil
}

// ExitSearchMode exits search mode but keeps search results
func (s *State) ExitSearchMode() {
	s.searchMode = false
	// Keep searchQuery, searchRegex, useRegex, and searchResult for n/N navigation
}

// ClearSearch clears all search state
func (s *State) ClearSearch() {
	s.searchMode = false
	s.searchQuery = ""
	s.searchRegex = nil
	s.useRegex = false
	s.searchResult = nil
}

// IsSearchMode returns if in search mode
func (s *State) IsSearchMode() bool {
	return s.searchMode
}

// SearchQuery returns current search query
func (s *State) SearchQuery() string {
	return s.searchQuery
}

// SetSearchQuery sets search query and performs search
func (s *State) SetSearchQuery(query string) {
	s.searchQuery = query
	s.performSearch()
}

// AppendSearchQuery appends character to search query
func (s *State) AppendSearchQuery(ch rune) {
	s.searchQuery += string(ch)
	s.performSearch()
}

// BackspaceSearchQuery removes last character from search query
func (s *State) BackspaceSearchQuery() {
	if len(s.searchQuery) > 0 {
		s.searchQuery = s.searchQuery[:len(s.searchQuery)-1]
		s.performSearch()
	}
}

// SearchResult returns search result indices
func (s *State) SearchResult() []int {
	return s.searchResult
}

func (s *State) performSearch() {
	s.searchResult = nil
	if s.searchQuery == "" {
		return
	}

	// Try to compile regex if pattern contains regex characters
	s.useRegex = false
	if strings.ContainsAny(s.searchQuery, ".*+?^${}[]|()\\") {
		regex, err := regexp.Compile("(?i)" + s.searchQuery)
		if err == nil {
			s.useRegex = true
			s.searchRegex = regex
		}
	}

	query := s.searchQuery
	switch s.mode {
	case ViewModeTable, ViewModeAccountTables:
		// Search by table ID in filtered tables
		filteredTables := s.FilteredTables()
		for i, tbl := range filteredTables {
			if s.matchTableID(tbl.TableID, query) {
				s.searchResult = append(s.searchResult, i)
			}
		}
	case ViewModeAccount:
		// Search by account ID
		for i, acc := range s.accounts {
			if s.matchAccountID(acc.AccountID, query) {
				s.searchResult = append(s.searchResult, i)
			}
		}
	}
}

func (s *State) matchTableID(tableID uint64, query string) bool {
	idStr := fmt.Sprintf("%d", tableID)
	if s.useRegex && s.searchRegex != nil {
		return s.searchRegex.MatchString(idStr)
	}
	return strings.Contains(idStr, query)
}

func (s *State) matchAccountID(accountID uint32, query string) bool {
	idStr := fmt.Sprintf("%d", accountID)
	if s.useRegex && s.searchRegex != nil {
		return s.searchRegex.MatchString(idStr)
	}
	return strings.Contains(idStr, query)
}

// Breadcrumb returns navigation path
func (s *State) Breadcrumb() string {
	switch s.mode {
	case ViewModeList:
		return "📍 Checkpoints"
	case ViewModeEntry:
		if s.selectedEntry >= 0 && s.selectedEntry < len(s.entries) {
			entry := s.entries[s.selectedEntry]
			typ := "Global"
			if entry.IsIncremental() {
				typ = "Incremental"
			}
			return fmt.Sprintf("📍 ckp #%d (%s)", s.selectedEntry+1, typ)
		}
		return "📍 Checkpoint"
	case ViewModeAccount:
		return fmt.Sprintf("📍 ckp #%d > Accounts", s.selectedEntry+1)
	case ViewModeAccountTables:
		return fmt.Sprintf("📍 ckp #%d > Account: %d", s.selectedEntry+1, s.selectedAccount)
	case ViewModeTable:
		return fmt.Sprintf("📍 ckp #%d > Tables", s.selectedEntry+1)
	case ViewModeTableDetail:
		return fmt.Sprintf("📍 ckp #%d > Table: %d", s.selectedEntry+1, s.selectedTable)
	case ViewModeLogical:
		return "📍 Logical View"
	default:
		return "📍"
	}
}

// IsRegexSearch returns if using regex search
func (s *State) IsRegexSearch() bool {
	return s.useRegex
}
