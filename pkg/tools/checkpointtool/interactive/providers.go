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

	tea "github.com/charmbracelet/bubbletea"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
)

// === Checkpoint List Provider ===

type CheckpointListProvider struct {
	state *State
}

func (p *CheckpointListProvider) GetRows() [][]string {
	entries := p.state.entries
	rows := make([][]string, len(entries))
	for i, e := range entries {
		typeStr := "G"
		if e.IsIncremental() {
			typeStr = "I"
		}
		rows[i] = []string{
			typeStr,
			fmt.Sprintf("%d", e.LSN()),
			formatTS(e.GetStart()),
			formatTS(e.GetEnd()),
			stateStr(e.GetState()),
			fmt.Sprintf("%d", e.GetVersion()),
		}
	}
	return rows
}

func (p *CheckpointListProvider) GetRowNums() []string {
	return nil // Use default row numbers
}

func (p *CheckpointListProvider) GetOverview() string {
	info := p.state.info
	return fmt.Sprintf("Dir: %s │ Total: %d (G:%d I:%d C:%d)",
		info.Dir, info.TotalEntries, info.GlobalCount, info.IncrCount, info.CompactCount)
}

// === Checkpoint List Handler ===

type checkpointListHandler struct{}

func (h *checkpointListHandler) OnSelect(rowIdx int) tea.Cmd {
	return func() tea.Msg { return selectCheckpointMsg{idx: rowIdx} }
}

func (h *checkpointListHandler) OnBack() tea.Cmd { return nil }

func (h *checkpointListHandler) OnCustomKey(key string) tea.Cmd { return nil }

func (h *checkpointListHandler) MatchRow(row []string, query string) bool {
	for _, cell := range row {
		if strings.Contains(strings.ToLower(cell), strings.ToLower(query)) {
			return true
		}
	}
	return false
}

func (h *checkpointListHandler) FilterRow(row []string, filter string) bool {
	return h.MatchRow(row, filter)
}

// === Tables List Provider ===

type TablesListProvider struct {
	state *State
}

func (p *TablesListProvider) GetRows() [][]string {
	tables := p.state.FilteredTables()
	rows := make([][]string, len(tables))
	for i, tbl := range tables {
		rows[i] = []string{
			fmt.Sprintf("%d", tbl.AccountID),
			fmt.Sprintf("%d", tbl.TableID),
			fmt.Sprintf("%d", len(tbl.DataRanges)),
			fmt.Sprintf("%d", len(tbl.TombRanges)),
		}
	}
	return rows
}

func (p *TablesListProvider) GetRowNums() []string {
	return nil
}

func (p *TablesListProvider) GetOverview() string {
	tables := p.state.FilteredTables()
	totalData, totalTomb := 0, 0
	for _, tbl := range tables {
		totalData += len(tbl.DataRanges)
		totalTomb += len(tbl.TombRanges)
	}
	overview := fmt.Sprintf("📊 %d tables │ %d data objects │ %d tombstone objects",
		len(tables), totalData, totalTomb)
	if p.state.HasAccountFilter() {
		overview += fmt.Sprintf(" │ Filter: Account=%d", p.state.GetAccountFilter())
	}
	return overview
}

// === Tables List Handler ===

type tablesListHandler struct {
	state *State
}

func (h *tablesListHandler) OnSelect(rowIdx int) tea.Cmd {
	tables := h.state.FilteredTables()
	if rowIdx >= 0 && rowIdx < len(tables) {
		return func() tea.Msg { return selectTableMsg{tableID: tables[rowIdx].TableID} }
	}
	return nil
}

func (h *tablesListHandler) OnBack() tea.Cmd {
	return func() tea.Msg { return goBackMsg{} }
}

func (h *tablesListHandler) OnCustomKey(key string) tea.Cmd { return nil }

func (h *tablesListHandler) MatchRow(row []string, query string) bool {
	for _, cell := range row {
		if strings.Contains(cell, query) {
			return true
		}
	}
	return false
}

func (h *tablesListHandler) FilterRow(row []string, filter string) bool {
	if len(row) > 0 {
		return row[0] == filter
	}
	return false
}

// === Table Detail Provider ===

type TableDetailProvider struct {
	state *State
}

func (p *TableDetailProvider) GetRows() [][]string {
	dataEntries := p.state.DataEntries()
	tombEntries := p.state.TombEntries()

	rows := make([][]string, 0, len(dataEntries)+len(tombEntries))

	for _, entry := range dataEntries {
		objName := entry.Range.ObjectStats.ObjectName().String()
		rangeStr := fmt.Sprintf("%d-%d~%d-%d",
			entry.Range.Start.GetBlockOffset(), entry.Range.Start.GetRowOffset(),
			entry.Range.End.GetBlockOffset(), entry.Range.End.GetRowOffset())
		rangeRows := entry.Range.End.GetRowOffset() - entry.Range.Start.GetRowOffset() + 1

		rows = append(rows, []string{
			"Data",
			objName,
			rangeStr,
			fmt.Sprintf("%d", rangeRows),
			formatSize(entry.Range.ObjectStats.Size()),
			formatTSShort(entry.CreateTime),
		})
	}

	for _, entry := range tombEntries {
		objName := entry.Range.ObjectStats.ObjectName().String()
		rangeStr := fmt.Sprintf("%d-%d~%d-%d",
			entry.Range.Start.GetBlockOffset(), entry.Range.Start.GetRowOffset(),
			entry.Range.End.GetBlockOffset(), entry.Range.End.GetRowOffset())
		rangeRows := entry.Range.End.GetRowOffset() - entry.Range.Start.GetRowOffset() + 1

		rows = append(rows, []string{
			"Tomb",
			objName,
			rangeStr,
			fmt.Sprintf("%d", rangeRows),
			formatSize(entry.Range.ObjectStats.Size()),
			formatTSShort(entry.CreateTime),
		})
	}
	return rows
}

func (p *TableDetailProvider) GetRowNums() []string {
	return nil
}

func (p *TableDetailProvider) GetOverview() string {
	tbl := p.state.GetSelectedTable()
	if tbl == nil {
		return ""
	}
	dataEntries := p.state.DataEntries()
	tombEntries := p.state.TombEntries()
	return fmt.Sprintf("Table %d (Account: %d) │ %d data │ %d tomb",
		tbl.TableID, tbl.AccountID, len(dataEntries), len(tombEntries))
}

// === Table Detail Handler ===

type tableDetailHandler struct {
	state *State
}

func (h *tableDetailHandler) OnSelect(rowIdx int) tea.Cmd {
	dataEntries := h.state.DataEntries()
	tombEntries := h.state.TombEntries()

	var entry *checkpointtool.ObjectEntryInfo
	if rowIdx < len(dataEntries) {
		entry = dataEntries[rowIdx]
	} else if rowIdx < len(dataEntries)+len(tombEntries) {
		entry = tombEntries[rowIdx-len(dataEntries)]
	}

	if entry != nil {
		return func() tea.Msg { return openObjectMsg{path: entry.Range.ObjectStats.ObjectName().String()} }
	}
	return nil
}

func (h *tableDetailHandler) OnBack() tea.Cmd {
	return func() tea.Msg { return goBackMsg{} }
}

func (h *tableDetailHandler) OnCustomKey(key string) tea.Cmd { return nil }

func (h *tableDetailHandler) MatchRow(row []string, query string) bool {
	for _, cell := range row {
		if strings.Contains(strings.ToLower(cell), strings.ToLower(query)) {
			return true
		}
	}
	return false
}

func (h *tableDetailHandler) FilterRow(row []string, filter string) bool {
	if len(row) > 0 {
		return strings.EqualFold(row[0], filter)
	}
	return false
}

// === Account List Provider ===

type AccountListProvider struct {
	state *State
}

func (p *AccountListProvider) GetRows() [][]string {
	accounts := p.state.Accounts()
	rows := make([][]string, len(accounts))
	for i, acc := range accounts {
		rows[i] = []string{
			fmt.Sprintf("%d", acc.AccountID),
			fmt.Sprintf("%d", acc.TableCount),
			fmt.Sprintf("%d", acc.DataRanges),
			fmt.Sprintf("%d", acc.TombRanges),
		}
	}
	return rows
}

func (p *AccountListProvider) GetRowNums() []string {
	return nil
}

func (p *AccountListProvider) GetOverview() string {
	accounts := p.state.Accounts()
	return fmt.Sprintf("📊 %d accounts", len(accounts))
}

// === Account List Handler ===

type accountListHandler struct {
	state *State
}

func (h *accountListHandler) OnSelect(rowIdx int) tea.Cmd {
	accounts := h.state.Accounts()
	if rowIdx >= 0 && rowIdx < len(accounts) {
		// TODO: implement account selection
	}
	return nil
}

func (h *accountListHandler) OnBack() tea.Cmd {
	return func() tea.Msg { return goBackMsg{} }
}

func (h *accountListHandler) OnCustomKey(key string) tea.Cmd { return nil }

func (h *accountListHandler) MatchRow(row []string, query string) bool {
	if len(row) > 0 {
		return strings.Contains(row[0], query)
	}
	return false
}

func (h *accountListHandler) FilterRow(row []string, filter string) bool {
	return h.MatchRow(row, filter)
}
