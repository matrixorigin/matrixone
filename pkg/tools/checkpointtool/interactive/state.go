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
	ViewModeList  ViewMode = iota // Checkpoint list
	ViewModeTable                 // Tables list
)

// State manages checkpoint viewer state
type State struct {
	reader *checkpointtool.CheckpointReader

	// View state
	mode ViewMode

	// Selection state
	selectedEntry int
	selectedTable uint64

	// Filter state
	filterAccountID int64 // -1 means no filter

	// Cached data
	entries     []*checkpoint.CheckpointEntry
	tables      []*checkpointtool.TableInfo
	info        *checkpointtool.CheckpointInfo
	dataEntries []*checkpointtool.ObjectEntryInfo
	tombEntries []*checkpointtool.ObjectEntryInfo
}

// NewState creates a new state
func NewState(reader *checkpointtool.CheckpointReader) *State {
	return &State{
		reader:          reader,
		mode:            ViewModeList,
		entries:         reader.Entries(),
		info:            reader.Info(),
		filterAccountID: -1,
	}
}

// === Getters ===

func (s *State) Mode() ViewMode                                 { return s.mode }
func (s *State) Entries() []*checkpoint.CheckpointEntry         { return s.entries }
func (s *State) DataEntries() []*checkpointtool.ObjectEntryInfo { return s.dataEntries }
func (s *State) TombEntries() []*checkpointtool.ObjectEntryInfo { return s.tombEntries }
func (s *State) HasAccountFilter() bool                         { return s.filterAccountID >= 0 }
func (s *State) GetAccountFilter() int64                        { return s.filterAccountID }

// GetSelectedTable returns the currently selected table
func (s *State) GetSelectedTable() *checkpointtool.TableInfo {
	for _, tbl := range s.tables {
		if tbl.TableID == s.selectedTable {
			return tbl
		}
	}
	return nil
}

// FilteredTables returns tables filtered by account id
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

// Accounts returns unique accounts from tables
func (s *State) Accounts() []*checkpointtool.AccountInfo {
	accountMap := make(map[uint32]*checkpointtool.AccountInfo)
	for _, tbl := range s.tables {
		if acc, ok := accountMap[tbl.AccountID]; ok {
			acc.TableCount++
			acc.DataRanges += len(tbl.DataRanges)
			acc.TombRanges += len(tbl.TombRanges)
		} else {
			accountMap[tbl.AccountID] = &checkpointtool.AccountInfo{
				AccountID:  tbl.AccountID,
				TableCount: 1,
				DataRanges: len(tbl.DataRanges),
				TombRanges: len(tbl.TombRanges),
			}
		}
	}
	accounts := make([]*checkpointtool.AccountInfo, 0, len(accountMap))
	for _, acc := range accountMap {
		accounts = append(accounts, acc)
	}
	return accounts
}

// === Navigation ===

// SwitchToTables loads tables for selected entry
func (s *State) SwitchToTables() error {
	if s.selectedEntry < 0 || s.selectedEntry >= len(s.entries) {
		return nil
	}
	tables, err := s.reader.GetTables(s.entries[s.selectedEntry])
	if err != nil {
		return err
	}
	s.tables = tables
	s.mode = ViewModeTable
	return nil
}

// SelectTable loads object entries for selected table
func (s *State) SelectTable(tableID uint64) error {
	s.selectedTable = tableID
	dataEntries, tombEntries, err := s.reader.GetObjectEntries(s.entries[s.selectedEntry], tableID)
	if err != nil {
		return err
	}
	s.dataEntries = dataEntries
	s.tombEntries = tombEntries
	return nil
}

// SetAccountFilter sets account filter
func (s *State) SetAccountFilter(accountID int64) {
	s.filterAccountID = accountID
}

// ClearAccountFilter clears account filter
func (s *State) ClearAccountFilter() {
	s.filterAccountID = -1
}
