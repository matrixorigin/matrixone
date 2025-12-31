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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	"github.com/matrixorigin/matrixone/pkg/tools/interactive"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
)

// EntryListItem wraps CheckpointEntry to implement interactive.ListItem
type EntryListItem struct {
	entry *checkpoint.CheckpointEntry
	index int
}

func (e *EntryListItem) GetColumns() []string {
	return []string{
		fmt.Sprintf("%4d", e.index),
		e.entry.GetType().String(),
		formatTS(e.entry.GetStart()),
		formatTS(e.entry.GetEnd()),
		stateStr(e.entry.GetState()),
	}
}

func (e *EntryListItem) IsSelectable() bool {
	return true
}

// TableItem wraps TableInfo to implement interactive.Item
type TableItem struct {
	info *checkpointtool.TableInfo
}

func (t *TableItem) GetFields() []string {
	return []string{
		fmt.Sprintf("%d", t.info.TableID),
		fmt.Sprintf("%d", t.info.AccountID),
		fmt.Sprintf("%d", len(t.info.DataRanges)),
		fmt.Sprintf("%d", len(t.info.TombRanges)),
	}
}

func (t *TableItem) GetSearchableText() string {
	return fmt.Sprintf("%d %d", t.info.TableID, t.info.AccountID)
}

func (t *TableItem) IsSelectable() bool {
	return true
}

// TableDataSource implements interactive.DataSource for tables
type TableDataSource struct {
	state *State
}

func NewTableDataSource(state *State) *TableDataSource {
	return &TableDataSource{state: state}
}

func (ds *TableDataSource) GetItems() []interactive.Item {
	items := make([]interactive.Item, len(ds.state.tables))
	for i, tbl := range ds.state.tables {
		items[i] = &TableItem{info: tbl}
	}
	return items
}

func (ds *TableDataSource) GetFilteredItems() []interactive.Item {
	tables := ds.state.FilteredTables()
	items := make([]interactive.Item, len(tables))
	for i, tbl := range tables {
		items[i] = &TableItem{info: tbl}
	}
	return items
}

func (ds *TableDataSource) GetItemCount() int {
	return len(ds.state.FilteredTables())
}

func (ds *TableDataSource) HasFilter() bool {
	return ds.state.HasAccountFilter()
}

func (ds *TableDataSource) GetFilterInfo() string {
	if !ds.state.HasAccountFilter() {
		return ""
	}
	return fmt.Sprintf("🔍 Filtered by Account ID: %d", ds.state.GetAccountFilter())
}

// Helper functions for formatting
func formatTS(ts types.TS) string {
	raw := ts.ToString()
	physical := ts.Physical()
	if physical == 0 {
		return raw
	}
	t := time.Unix(0, physical)
	return fmt.Sprintf("%s(%s)", raw, t.Format("2006/01/02 15:04:05.000000"))
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
