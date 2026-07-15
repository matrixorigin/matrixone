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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStateFiltersAccountsAndSelectedTable(t *testing.T) {
	state := &State{
		filterAccountID: -1,
		tables: []*checkpointtool.TableInfo{
			{AccountID: 1, TableID: 10, DataRanges: make([]ckputil.TableRange, 2)},
			{AccountID: 2, TableID: 20, TombRanges: make([]ckputil.TableRange, 1)},
			{AccountID: 1, TableID: 11, DataRanges: make([]ckputil.TableRange, 1), TombRanges: make([]ckputil.TableRange, 3)},
		},
	}

	require.False(t, state.HasAccountFilter())
	require.Equal(t, state.tables, state.FilteredTables())
	require.Nil(t, state.GetSelectedTable())

	state.selectedTable = 20
	require.Equal(t, uint64(20), state.GetSelectedTable().TableID)

	state.SetAccountFilter(1)
	require.True(t, state.HasAccountFilter())
	require.Equal(t, int64(1), state.GetAccountFilter())
	filtered := state.FilteredTables()
	require.Len(t, filtered, 2)
	assert.Equal(t, uint64(10), filtered[0].TableID)
	assert.Equal(t, uint64(11), filtered[1].TableID)

	state.ClearAccountFilter()
	require.False(t, state.HasAccountFilter())
}

func TestStateAccountsAggregatesRanges(t *testing.T) {
	state := &State{
		tables: []*checkpointtool.TableInfo{
			{AccountID: 2, DataRanges: make([]ckputil.TableRange, 1)},
			{AccountID: 1, DataRanges: make([]ckputil.TableRange, 2), TombRanges: make([]ckputil.TableRange, 1)},
			{AccountID: 1, TombRanges: make([]ckputil.TableRange, 3)},
		},
	}

	accounts := state.Accounts()
	require.Len(t, accounts, 2)
	byID := make(map[uint32]*checkpointtool.AccountInfo)
	for _, account := range accounts {
		byID[account.AccountID] = account
	}

	require.Equal(t, 2, byID[1].TableCount)
	require.Equal(t, 2, byID[1].DataRanges)
	require.Equal(t, 4, byID[1].TombRanges)
	require.Equal(t, 1, byID[2].TableCount)
	require.Equal(t, 1, byID[2].DataRanges)
	require.Equal(t, 0, byID[2].TombRanges)
}

func TestStateNoopNavigationForInvalidSelection(t *testing.T) {
	state := &State{selectedEntry: -1}
	require.NoError(t, state.SwitchToTables())
	require.Equal(t, ViewModeList, state.Mode())

	state.selectedEntry = 1
	state.entries = nil
	require.NoError(t, state.SwitchToTables())
	require.Equal(t, ViewModeList, state.Mode())
}

func TestStateNavigationWithEmptyCheckpointEntry(t *testing.T) {
	entry := checkpoint.NewCheckpointEntry("", types.BuildTS(1, 0), types.BuildTS(2, 0), checkpoint.ET_Global)
	state := &State{
		reader:        &checkpointtool.CheckpointReader{},
		entries:       []*checkpoint.CheckpointEntry{entry},
		selectedEntry: 0,
	}

	require.NoError(t, state.SwitchToTables())
	require.Equal(t, ViewModeTable, state.Mode())
	require.Nil(t, state.LogicalView())

	require.NoError(t, state.SelectTable(42))
	require.Equal(t, uint64(42), state.selectedTable)
	require.Nil(t, state.DataEntries())
	require.Nil(t, state.TombEntries())

	require.NoError(t, state.LoadLogicalView())
	require.NotNil(t, state.LogicalView())
	require.Equal(t, []string{"object", "block", "row"}, state.LogicalView().Headers)

	require.NoError(t, state.LoadLogicalView())
}
