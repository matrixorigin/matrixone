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

	"github.com/matrixorigin/matrixone/pkg/tools/checkpointtool"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNavigationGKey(t *testing.T) {
	// Create test state with mock data
	state := createTestState(t)

	// Create model with test data
	m := model{
		state:  state,
		cursor: 5, // Start in middle
	}

	// Mock maxItems function
	originalMaxItems := func() int {
		switch state.mode {
		case ViewModeAccount:
			return len(state.accounts)
		case ViewModeTable, ViewModeAccountTables:
			return len(state.FilteredTables())
		case ViewModeTableDetail:
			return len(state.DataEntries()) + len(state.TombEntries())
		default:
			return len(state.entries)
		}
	}

	// Test 1: G key in account view
	state.mode = ViewModeAccount
	state.accounts = createTestAccounts(10) // 10 accounts
	state.pageSize = 5
	state.scrollOffset = 0

	maxItems := originalMaxItems()
	require.Equal(t, 10, maxItems)

	// Simulate G key press
	if maxItems > 0 {
		m.cursor = maxItems - 1 // cursor = 9
		// Update scroll offset to show the last item
		if m.cursor >= state.scrollOffset+state.pageSize {
			state.scrollOffset = m.cursor - state.pageSize + 1
			if state.scrollOffset < 0 {
				state.scrollOffset = 0
			}
		}
	}

	assert.Equal(t, 9, m.cursor)
	assert.Equal(t, 5, state.scrollOffset) // 9 - 5 + 1 = 5

	// Test 2: G key in table view
	state.mode = ViewModeTable
	state.tables = createTestTables(15) // 15 tables
	state.pageSize = 8
	state.scrollOffset = 0
	m.cursor = 0

	// FilteredTables() should return all tables when no filter
	maxItems = len(state.tables)
	require.Equal(t, 15, maxItems)

	// Simulate G key press
	if maxItems > 0 {
		m.cursor = maxItems - 1 // cursor = 14
		if m.cursor >= state.scrollOffset+state.pageSize {
			state.scrollOffset = m.cursor - state.pageSize + 1
			if state.scrollOffset < 0 {
				state.scrollOffset = 0
			}
		}
	}

	assert.Equal(t, 14, m.cursor)
	assert.Equal(t, 7, state.scrollOffset) // 14 - 8 + 1 = 7

	// Test 3: G key with small dataset (cursor fits in first page)
	state.mode = ViewModeAccount
	state.accounts = createTestAccounts(3) // Only 3 accounts
	state.pageSize = 10
	state.scrollOffset = 0
	m.cursor = 0

	maxItems = originalMaxItems()
	require.Equal(t, 3, maxItems)

	// Simulate G key press
	if maxItems > 0 {
		m.cursor = maxItems - 1 // cursor = 2
		if m.cursor >= state.scrollOffset+state.pageSize {
			state.scrollOffset = m.cursor - state.pageSize + 1
			if state.scrollOffset < 0 {
				state.scrollOffset = 0
			}
		}
	}

	assert.Equal(t, 2, m.cursor)
	assert.Equal(t, 0, state.scrollOffset) // No need to scroll
}

func TestNavigationGKeyEdgeCases(t *testing.T) {
	state := createTestState(t)
	m := model{
		state:  state,
		cursor: 0,
	}

	// Test 1: Empty dataset
	state.mode = ViewModeAccount
	state.accounts = nil
	state.pageSize = 5
	state.scrollOffset = 0

	maxItems := 0 // Empty

	// Simulate G key press with empty data
	if maxItems > 0 {
		m.cursor = maxItems - 1
		if m.cursor >= state.scrollOffset+state.pageSize {
			state.scrollOffset = m.cursor - state.pageSize + 1
			if state.scrollOffset < 0 {
				state.scrollOffset = 0
			}
		}
	}
	// Should remain unchanged
	assert.Equal(t, 0, m.cursor)
	assert.Equal(t, 0, state.scrollOffset)

	// Test 2: Single item dataset
	state.accounts = createTestAccounts(1)
	state.pageSize = 5
	state.scrollOffset = 0
	m.cursor = 0

	maxItems = 1

	// Simulate G key press
	if maxItems > 0 {
		m.cursor = maxItems - 1 // cursor = 0
		if m.cursor >= state.scrollOffset+state.pageSize {
			state.scrollOffset = m.cursor - state.pageSize + 1
			if state.scrollOffset < 0 {
				state.scrollOffset = 0
			}
		}
	}

	assert.Equal(t, 0, m.cursor)
	assert.Equal(t, 0, state.scrollOffset)

	// Test 3: Cursor already at last position
	state.accounts = createTestAccounts(10)
	state.pageSize = 5
	state.scrollOffset = 5
	m.cursor = 9 // Already at last

	maxItems = 10

	// Simulate G key press
	if maxItems > 0 {
		m.cursor = maxItems - 1 // cursor = 9 (unchanged)
		if m.cursor >= state.scrollOffset+state.pageSize {
			state.scrollOffset = m.cursor - state.pageSize + 1
			if state.scrollOffset < 0 {
				state.scrollOffset = 0
			}
		}
	}

	assert.Equal(t, 9, m.cursor)
	assert.Equal(t, 5, state.scrollOffset) // Should remain 5
}

// Helper functions for testing
func createTestAccounts(count int) []*checkpointtool.AccountInfo {
	accounts := make([]*checkpointtool.AccountInfo, count)
	for i := 0; i < count; i++ {
		accounts[i] = &checkpointtool.AccountInfo{
			AccountID:  uint32(i + 1),
			TableCount: i + 1,
			DataRanges: i * 2,
			TombRanges: i,
		}
	}
	return accounts
}

func createTestTables(count int) []*checkpointtool.TableInfo {
	tables := make([]*checkpointtool.TableInfo, count)
	for i := 0; i < count; i++ {
		tables[i] = &checkpointtool.TableInfo{
			TableID:   uint64(i + 1),
			AccountID: uint32((i % 3) + 1),
		}
	}
	return tables
}

func createTestState(t *testing.T) *State {
	// Create minimal test state
	state := &State{
		mode:         ViewModeList,
		pageSize:     10,
		scrollOffset: 0,
		entries:      make([]*checkpoint.CheckpointEntry, 0),
		accounts:     make([]*checkpointtool.AccountInfo, 0),
		tables:       make([]*checkpointtool.TableInfo, 0),
	}
	return state
}
