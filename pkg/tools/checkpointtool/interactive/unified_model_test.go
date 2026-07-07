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
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnifiedModel(t *testing.T) {
	// Create empty reader for testing
	reader := &checkpointtool.CheckpointReader{}
	model := NewUnifiedModel(reader)
	assert.Nil(t, model.Init())
	assert.Equal(t, ViewModeList, model.state.Mode())
	assert.Nil(t, model.state.Entries())

	// Test initial view
	view := model.View()
	t.Logf("Initial view:\n%s", view)

	if !strings.Contains(view, "Checkpoint List") {
		t.Error("Should show checkpoint list title")
	}

	// Test navigation
	model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'j'}})
	model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'k'}})

	// View should still render
	view = model.View()
	if view == "" {
		t.Error("View should not be empty after navigation")
	}
}

func TestUnifiedModelPageStack(t *testing.T) {
	reader := &checkpointtool.CheckpointReader{}
	model := NewUnifiedModel(reader)

	// Initially no pages in stack
	if len(model.pageStack) != 0 {
		t.Errorf("Initial stack should be empty, got %d", len(model.pageStack))
	}

	// After selecting checkpoint, stack should have 1 page
	// (This would require mock data to work properly)
}

func TestUnifiedModelOpenObjectMessageTracksRange(t *testing.T) {
	reader := &checkpointtool.CheckpointReader{}
	model := NewUnifiedModel(reader)
	stats := testObjectStats(t, 7, 10, 1, 100, 200)
	rng := ckputil.TableRange{
		TableID:     42,
		ObjectType:  ckputil.ObjectType_Data,
		ObjectStats: stats,
	}
	model.state.dataEntries = []*checkpointtool.ObjectEntryInfo{{
		ObjectStats: stats,
		Range:       rng,
	}}

	updated, cmd := model.Update(openObjectMsg{path: "obj"})
	require.Same(t, model, updated)
	require.NotNil(t, cmd)
	assert.Equal(t, "obj", model.GetObjectToOpen())
	require.NotNil(t, model.GetRangeToOpen())
	assert.Equal(t, uint64(42), model.GetRangeToOpen().TableID)

	model.ClearObjectToOpen()
	assert.Empty(t, model.GetObjectToOpen())
	assert.Nil(t, model.GetRangeToOpen())
}

func TestUnifiedModelCreatesPages(t *testing.T) {
	model := NewUnifiedModel(&checkpointtool.CheckpointReader{})
	model.state.logicalView = &checkpointtool.LogicalTableView{
		Headers: []string{"object", "block", "row", "id"},
	}

	assert.NotNil(t, model.createTablesListPage())
	assert.NotNil(t, model.createTableDetailPage())
	assert.NotNil(t, model.createLogicalTablePage())
	model.state.logicalView = nil
	assert.NotNil(t, model.createLogicalTablePage())
}

func testObjectStats(t *testing.T, idByte byte, rows, blocks, size, originSize uint32) objectio.ObjectStats {
	t.Helper()
	stats := objectio.ObjectStats{}
	objName := objectio.BuildObjectName(&types.Uuid{idByte}, uint16(idByte))
	require.NoError(t, objectio.SetObjectStatsObjectName(&stats, objName))
	require.NoError(t, objectio.SetObjectStatsRowCnt(&stats, rows))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(&stats, blocks))
	require.NoError(t, objectio.SetObjectStatsSize(&stats, size))
	require.NoError(t, objectio.SetObjectStatsOriginSize(&stats, originSize))
	return stats
}

// TestExpandObjectStats tests the ObjectStats expansion function
func TestExpandObjectStats(t *testing.T) {
	t.Run("valid_objectstats", func(t *testing.T) {
		// Create a valid ObjectStats
		var stats objectio.ObjectStats
		var objId objectio.ObjectId
		objectio.SetObjectStatsObjectName(&stats, objectio.BuildObjectNameWithObjectID(&objId))
		objectio.SetObjectStatsRowCnt(&stats, 100)
		objectio.SetObjectStatsSize(&stats, 2048)
		objectio.SetObjectStatsOriginSize(&stats, 4096)

		data := stats[:]
		result := expandObjectStats(data)

		assert.Equal(t, 5, len(result))
		// object_name should not be empty
		assert.NotEmpty(t, result[0])
		// flags should be "-" (no flags set)
		assert.Equal(t, "-", result[1])
		// rows
		assert.Equal(t, uint32(100), result[2])
		// osize
		assert.Equal(t, "4.0KB", result[3])
		// csize
		assert.Equal(t, "2.0KB", result[4])
	})

	t.Run("objectstats_with_flags", func(t *testing.T) {
		var stats objectio.ObjectStats
		var objId objectio.ObjectId
		objectio.SetObjectStatsObjectName(&stats, objectio.BuildObjectNameWithObjectID(&objId))
		objectio.WithSorted()(&stats)
		objectio.WithCNCreated()(&stats)

		data := stats[:]
		result := expandObjectStats(data)

		// flags should contain S and C
		flags := result[1].(string)
		assert.Contains(t, flags, "S")
		assert.Contains(t, flags, "C")
	})

	t.Run("objectstats_appendable", func(t *testing.T) {
		var stats objectio.ObjectStats
		var objId objectio.ObjectId
		objectio.SetObjectStatsObjectName(&stats, objectio.BuildObjectNameWithObjectID(&objId))
		objectio.WithAppendable()(&stats)

		data := stats[:]
		result := expandObjectStats(data)

		flags := result[1].(string)
		assert.Contains(t, flags, "A")
	})

	t.Run("invalid_data_wrong_length", func(t *testing.T) {
		data := []byte{1, 2, 3} // Wrong length
		result := expandObjectStats(data)

		// Should return empty values
		assert.Equal(t, 5, len(result))
		assert.Equal(t, "", result[0])
		assert.Equal(t, "", result[1])
	})

	t.Run("invalid_data_wrong_type", func(t *testing.T) {
		result := expandObjectStats("not bytes")

		assert.Equal(t, 5, len(result))
		assert.Equal(t, "", result[0])
	})

	t.Run("nil_value", func(t *testing.T) {
		result := expandObjectStats(nil)

		assert.Equal(t, 5, len(result))
		assert.Equal(t, "", result[0])
	})
}

// TestFormatSize tests the size formatting function
func TestFormatSize(t *testing.T) {
	tests := []struct {
		bytes    uint32
		expected string
	}{
		{0, "0B"},
		{100, "100B"},
		{1023, "1023B"},
		{1024, "1.0KB"},
		{1536, "1.5KB"},
		{2048, "2.0KB"},
		{1048576, "1.0MB"},
		{1572864, "1.5MB"},
		{1073741824, "1.0GB"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := formatSize(tt.bytes)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestCheckpointListProvider tests the checkpoint list provider
func TestCheckpointListProvider(t *testing.T) {
	t.Run("empty_state", func(t *testing.T) {
		state := &State{}
		provider := &CheckpointListProvider{state: state}

		rows := provider.GetRows()
		assert.Empty(t, rows)
	})

	t.Run("get_row_nums_returns_nil", func(t *testing.T) {
		state := &State{}
		provider := &CheckpointListProvider{state: state}

		rowNums := provider.GetRowNums()
		assert.Nil(t, rowNums)
	})
}

func TestStateAndProvidersWithData(t *testing.T) {
	dataStats := testObjectStats(t, 1, 100, 3, 2048, 4096)
	tombStats := testObjectStats(t, 2, 5, 1, 256, 512)
	otherStats := testObjectStats(t, 3, 7, 1, 128, 256)

	state := &State{
		filterAccountID: -1,
		selectedTable:   100,
		info: &checkpointtool.CheckpointInfo{
			Dir:          "/tmp/ckp",
			TotalEntries: 3,
			GlobalCount:  1,
			IncrCount:    2,
		},
		tables: []*checkpointtool.TableInfo{
			{
				AccountID: 1,
				TableID:   100,
				DataRanges: []ckputil.TableRange{{
					TableID:     100,
					ObjectType:  ckputil.ObjectType_Data,
					ObjectStats: dataStats,
				}},
				TombRanges: []ckputil.TableRange{{
					TableID:     100,
					ObjectType:  ckputil.ObjectType_Tombstone,
					ObjectStats: tombStats,
				}},
			},
			{
				AccountID: 2,
				TableID:   200,
				DataRanges: []ckputil.TableRange{{
					TableID:     200,
					ObjectType:  ckputil.ObjectType_Data,
					ObjectStats: otherStats,
				}},
			},
		},
		dataEntries: []*checkpointtool.ObjectEntryInfo{{ObjectStats: dataStats}},
		tombEntries: []*checkpointtool.ObjectEntryInfo{{ObjectStats: tombStats}},
		logicalView: &checkpointtool.LogicalTableView{
			Headers:      []string{"object", "block", "row", "id"},
			Rows:         [][]string{{"obj", "0", "1", "42"}},
			VisibleRows:  1,
			DeletedRows:  2,
			PhysicalRows: 3,
		},
	}

	t.Run("state_filters_accounts_and_selection", func(t *testing.T) {
		require.Same(t, state.tables[0], state.GetSelectedTable())
		require.Len(t, state.FilteredTables(), 2)
		state.SetAccountFilter(1)
		require.True(t, state.HasAccountFilter())
		assert.Equal(t, int64(1), state.GetAccountFilter())
		filtered := state.FilteredTables()
		require.Len(t, filtered, 1)
		assert.Equal(t, uint64(100), filtered[0].TableID)
		state.ClearAccountFilter()
		require.False(t, state.HasAccountFilter())

		accounts := state.Accounts()
		require.Len(t, accounts, 2)
		assert.ElementsMatch(t, []uint32{1, 2}, []uint32{accounts[0].AccountID, accounts[1].AccountID})
	})

	t.Run("providers_render_rows_and_overviews", func(t *testing.T) {
		checkpoints := (&CheckpointListProvider{state: state}).GetOverview()
		assert.Contains(t, checkpoints, "/tmp/ckp")
		assert.Contains(t, checkpoints, "Total: 3")

		tables := &TablesListProvider{state: state}
		tableRows := tables.GetRows()
		require.Len(t, tableRows, 2)
		assert.Equal(t, []string{"1", "100", "1", "1"}, tableRows[0])
		assert.Nil(t, tables.GetRowNums())
		assert.Contains(t, tables.GetOverview(), "2 tables")

		details := &TableDetailProvider{state: state}
		detailRows := details.GetRows()
		require.Len(t, detailRows, 2)
		assert.Equal(t, "Data", detailRows[0][0])
		assert.Equal(t, "0..2", detailRows[0][2])
		assert.Equal(t, "Tomb", detailRows[1][0])
		assert.Nil(t, details.GetRowNums())
		assert.Contains(t, details.GetOverview(), "1 data objs")
		assert.Contains(t, details.GetOverview(), "1 tomb objs")

		logical := &LogicalTableProvider{state: state}
		assert.Equal(t, state.logicalView.Rows, logical.GetRows())
		assert.Nil(t, logical.GetRowNums())
		assert.Contains(t, logical.GetOverview(), "visible: 1")

		accounts := &AccountListProvider{state: state}
		accountRows := accounts.GetRows()
		require.Len(t, accountRows, 2)
		assert.Nil(t, accounts.GetRowNums())
		assert.Contains(t, accounts.GetOverview(), "2 accounts")
	})

	t.Run("providers_empty_overviews", func(t *testing.T) {
		empty := &State{}
		assert.Empty(t, (&TableDetailProvider{state: empty}).GetOverview())
		assert.Nil(t, (&LogicalTableProvider{state: empty}).GetRows())
		assert.Empty(t, (&LogicalTableProvider{state: empty}).GetOverview())
	})
}

// TestCheckpointListHandler tests the handler behavior
func TestCheckpointListHandler(t *testing.T) {
	t.Run("match_row", func(t *testing.T) {
		handler := &checkpointListHandler{}

		row := []string{"G", "123456", "2024/01/01", "2024/01/02", "Finished", "1"}

		assert.True(t, handler.MatchRow(row, "123456"))
		assert.True(t, handler.MatchRow(row, "Finished"))
		assert.True(t, handler.MatchRow(row, "2024"))
		assert.False(t, handler.MatchRow(row, "notfound"))
	})

	t.Run("on_back_returns_nil", func(t *testing.T) {
		handler := &checkpointListHandler{}

		cmd := handler.OnBack()
		// OnBack returns nil for checkpoint list (top level)
		assert.Nil(t, cmd)
	})
}

func TestTableHandlersCommands(t *testing.T) {
	stats := testObjectStats(t, 9, 10, 1, 100, 200)
	state := &State{
		tables:      []*checkpointtool.TableInfo{{TableID: 42}},
		dataEntries: []*checkpointtool.ObjectEntryInfo{{ObjectStats: stats}},
		tombEntries: []*checkpointtool.ObjectEntryInfo{{ObjectStats: stats}},
	}

	t.Run("tables_select_back_and_filter", func(t *testing.T) {
		handler := &tablesListHandler{state: state}
		cmd := handler.OnSelect(0)
		require.NotNil(t, cmd)
		msg, ok := cmd().(selectTableMsg)
		require.True(t, ok)
		assert.Equal(t, uint64(42), msg.tableID)
		assert.Nil(t, handler.OnSelect(99))
		assert.Nil(t, handler.OnCustomKey("x"))
		assert.True(t, handler.FilterRow([]string{"1"}, "1"))
		assert.False(t, handler.FilterRow(nil, "1"))
		_, ok = handler.OnBack()().(goBackMsg)
		assert.True(t, ok)
	})

	t.Run("table_detail_select_back_filter_and_custom", func(t *testing.T) {
		handler := &tableDetailHandler{state: state}
		cmd := handler.OnSelect(0)
		require.NotNil(t, cmd)
		msg, ok := cmd().(openObjectMsg)
		require.True(t, ok)
		assert.Equal(t, stats.ObjectName().String(), msg.path)
		require.NotNil(t, handler.OnSelect(1))
		assert.Nil(t, handler.OnSelect(99))
		assert.True(t, handler.FilterRow([]string{"Data"}, "data"))
		assert.False(t, handler.FilterRow(nil, "data"))
		assert.Nil(t, handler.OnCustomKey("x"))
		_, ok = handler.OnBack()().(goBackMsg)
		assert.True(t, ok)
	})

	t.Run("logical_back", func(t *testing.T) {
		_, ok := (&logicalTableHandler{}).OnBack()().(goBackMsg)
		assert.True(t, ok)
	})
}

// TestTableDetailHandler tests table detail handler
func TestTableDetailHandler(t *testing.T) {
	t.Run("match_row", func(t *testing.T) {
		handler := &tableDetailHandler{}

		row := []string{"Data", "object_name_123", "0-10", "100", "1.5KB", "01-01 12:00"}

		assert.True(t, handler.MatchRow(row, "Data"))
		assert.True(t, handler.MatchRow(row, "object"))
		assert.True(t, handler.MatchRow(row, "123"))
		assert.False(t, handler.MatchRow(row, "notfound"))
	})

	t.Run("custom_key_logical_view", func(t *testing.T) {
		handler := &tableDetailHandler{}
		cmd := handler.OnCustomKey("L")
		if assert.NotNil(t, cmd) {
			msg := cmd()
			_, ok := msg.(openLogicalTableMsg)
			assert.True(t, ok)
		}
	})
}

// TestTSFormatting tests timestamp formatting
func TestTSFormatting(t *testing.T) {
	t.Run("format_ts_zero", func(t *testing.T) {
		var ts types.TS
		result := formatTS(ts)
		assert.Equal(t, "0-0", result)
	})

	t.Run("format_ts_short_empty", func(t *testing.T) {
		var ts types.TS
		result := formatTSShort(ts)
		assert.Equal(t, "-", result)
	})
}

// TestParseSize tests the size parsing function
func TestParseSize(t *testing.T) {
	tests := []struct {
		input    string
		expected uint32
	}{
		{"0B", 0},
		{"100B", 100},
		{"1.0KB", 1024},
		{"1.5KB", 1536},
		{"2.0KB", 2048},
		{"1.0MB", 1048576},
		{"1.5MB", 1572864},
		{"279.6KB", 286310},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseSize(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestCkpDataOverview tests the checkpoint data overview function
func TestCkpDataOverview(t *testing.T) {
	t.Run("empty_rows", func(t *testing.T) {
		result := ckpDataOverview(nil)
		assert.Equal(t, "No data", result)

		result = ckpDataOverview([][]string{})
		assert.Equal(t, "No data", result)
	})

	t.Run("with_data_objects", func(t *testing.T) {
		rows := [][]string{
			// account_id, db_id, table_id, object_type, object_name, flags, rows, osize, csize
			{"0", "1", "100", "1", "obj1", "-", "50", "2.0KB", "1.0KB"},
			{"0", "1", "100", "1", "obj1", "-", "50", "2.0KB", "1.0KB"}, // duplicate
			{"0", "1", "100", "1", "obj2", "-", "30", "1.0KB", "512B"},
		}
		result := ckpDataOverview(rows)

		assert.Contains(t, result, "3 ranges")
		assert.Contains(t, result, "2 data objs")
		assert.Contains(t, result, "0 tomb objs")
		assert.Contains(t, result, "80 rows")
		assert.Contains(t, result, "osize: 3.0KB")
		assert.Contains(t, result, "csize: 1.5KB")
		assert.Contains(t, result, "ratio: 50.0%")
	})

	t.Run("with_tombstone_objects", func(t *testing.T) {
		rows := [][]string{
			{"0", "1", "100", "1", "data1", "-", "100", "4.0KB", "2.0KB"},
			{"0", "1", "100", "2", "tomb1", "-", "10", "1.0KB", "512B"},
		}
		result := ckpDataOverview(rows)

		assert.Contains(t, result, "1 data objs")
		assert.Contains(t, result, "1 tomb objs")
		assert.Contains(t, result, "100 rows")
		assert.Contains(t, result, "10 deletes")
	})

	t.Run("short_row_skipped", func(t *testing.T) {
		rows := [][]string{
			{"0", "1", "100"}, // too short, should be skipped
			{"0", "1", "100", "1", "obj1", "-", "50", "2.0KB", "1.0KB"},
		}
		result := ckpDataOverview(rows)

		assert.Contains(t, result, "1 data objs")
	})
}
