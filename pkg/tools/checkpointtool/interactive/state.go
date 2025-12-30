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
	ViewModeTableDetail                   // Table detail
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

	// Cached data
	entries  []*checkpoint.CheckpointEntry
	accounts []*checkpointtool.AccountInfo
	tables   []*checkpointtool.TableInfo
	info     *checkpointtool.CheckpointInfo
}

// NewState creates a new state
func NewState(reader *checkpointtool.CheckpointReader) *State {
	s := &State{
		reader:   reader,
		mode:     ViewModeList,
		pageSize: 20,
		entries:  reader.Entries(),
		info:     reader.Info(),
	}
	return s
}

// Mode returns current view mode
func (s *State) Mode() ViewMode { return s.mode }

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
	return nil
}

// SelectTable selects a table for detail view
func (s *State) SelectTable(tableID uint64) error {
	s.pushHistory()
	s.selectedTable = tableID
	s.mode = ViewModeTableDetail
	s.scrollOffset = 0
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

// GetSelectedTable returns the currently selected table info
func (s *State) GetSelectedTable() *checkpointtool.TableInfo {
	for _, t := range s.tables {
		if t.TableID == s.selectedTable {
			return t
		}
	}
	return nil
}
