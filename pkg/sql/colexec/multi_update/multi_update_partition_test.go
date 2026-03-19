// Copyright 2021-2024 Matrix Origin
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

package multi_update

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionMultiUpdateString(t *testing.T) {
	op := &PartitionMultiUpdate{}
	buf := new(bytes.Buffer)
	op.String(buf)
	require.Equal(t, "MultiUpdate: partition_multi_update", buf.String())
}

func TestNewPartitionMultiUpdateFrom(t *testing.T) {
	ps := &PartitionMultiUpdate{
		raw:     &MultiUpdate{},
		tableID: 1,
	}
	op := NewPartitionMultiUpdateFrom(ps)
	require.Equal(t, ps.raw.MultiUpdateCtx, op.(*PartitionMultiUpdate).raw.MultiUpdateCtx)
	require.Equal(t, ps.raw.Action, op.(*PartitionMultiUpdate).raw.Action)
	require.Equal(t, ps.raw.IsOnduplicateKeyUpdate, op.(*PartitionMultiUpdate).raw.IsOnduplicateKeyUpdate)
	require.Equal(t, ps.raw.Engine, op.(*PartitionMultiUpdate).raw.Engine)
	require.Equal(t, ps.tableID, op.(*PartitionMultiUpdate).tableID)
}

func TestAddInsertAffectRows(t *testing.T) {
	tests := []struct {
		name       string
		action     actionType
		tableType  UpdateTableType
		rowCount   uint64
		expectRows uint64
	}{
		{
			name:       "actionInsert with main table",
			action:     actionInsert,
			tableType:  UpdateMainTable,
			rowCount:   5,
			expectRows: 5,
		},
		{
			name:       "actionUpdate with main table (REPLACE INTO)",
			action:     actionUpdate,
			tableType:  UpdateMainTable,
			rowCount:   3,
			expectRows: 3,
		},
		{
			name:       "actionInsert with unique index table (should not count)",
			action:     actionInsert,
			tableType:  UpdateUniqueIndexTable,
			rowCount:   5,
			expectRows: 0,
		},
		{
			name:       "actionUpdate with unique index table (should not count)",
			action:     actionUpdate,
			tableType:  UpdateUniqueIndexTable,
			rowCount:   3,
			expectRows: 0,
		},
		{
			name:       "actionDelete with main table (should not count)",
			action:     actionDelete,
			tableType:  UpdateMainTable,
			rowCount:   2,
			expectRows: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			update := &MultiUpdate{
				ctr: container{
					action:       tt.action,
					affectedRows: 0,
				},
			}
			update.addAffectedRowsFunc = update.doAddAffectedRows

			update.addInsertAffectRows(tt.tableType, tt.rowCount)

			require.Equal(t, tt.expectRows, update.ctr.affectedRows, "affected rows should match expected value")
		})
	}
}

func TestAddDeleteAffectRows(t *testing.T) {
	tests := []struct {
		name       string
		action     actionType
		tableType  UpdateTableType
		rowCount   uint64
		expectRows uint64
	}{
		{
			name:       "actionDelete with main table",
			action:     actionDelete,
			tableType:  UpdateMainTable,
			rowCount:   5,
			expectRows: 5,
		},
		{
			name:       "actionUpdate with main table (should not count for REPLACE INTO)",
			action:     actionUpdate,
			tableType:  UpdateMainTable,
			rowCount:   3,
			expectRows: 0,
		},
		{
			name:       "actionDelete with unique index table (should not count)",
			action:     actionDelete,
			tableType:  UpdateUniqueIndexTable,
			rowCount:   5,
			expectRows: 0,
		},
		{
			name:       "actionUpdate with unique index table (should not count)",
			action:     actionUpdate,
			tableType:  UpdateUniqueIndexTable,
			rowCount:   3,
			expectRows: 0,
		},
		{
			name:       "actionInsert with main table (should not count)",
			action:     actionInsert,
			tableType:  UpdateMainTable,
			rowCount:   2,
			expectRows: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			update := &MultiUpdate{
				ctr: container{
					action:       tt.action,
					affectedRows: 0,
				},
			}
			update.addAffectedRowsFunc = update.doAddAffectedRows

			update.addDeleteAffectRows(tt.tableType, tt.rowCount)

			require.Equal(t, tt.expectRows, update.ctr.affectedRows, "affected rows should match expected value")
		})
	}
}

func TestReplaceIntoAffectedRows(t *testing.T) {
	// Test REPLACE INTO scenario: should only count INSERT rows, not DELETE rows
	update := &MultiUpdate{
		ctr: container{
			action:       actionUpdate, // REPLACE INTO uses actionUpdate
			affectedRows: 0,
		},
	}
	update.addAffectedRowsFunc = update.doAddAffectedRows

	// Simulate REPLACE INTO: DELETE 2 rows, INSERT 2 rows
	// Should only count INSERT rows (2), not DELETE rows
	update.addDeleteAffectRows(UpdateMainTable, 2) // Should not count
	require.Equal(t, uint64(0), update.ctr.affectedRows, "DELETE rows should not be counted for REPLACE INTO")

	update.addInsertAffectRows(UpdateMainTable, 2) // Should count
	require.Equal(t, uint64(2), update.ctr.affectedRows, "INSERT rows should be counted for REPLACE INTO")
}

func TestUpdateAffectedRows(t *testing.T) {
	// Test UPDATE scenario: should only count INSERT rows (updated rows), not DELETE rows
	update := &MultiUpdate{
		ctr: container{
			action:       actionUpdate, // UPDATE uses actionUpdate
			affectedRows: 0,
		},
	}
	update.addAffectedRowsFunc = update.doAddAffectedRows

	// Simulate UPDATE: DELETE 3 rows, INSERT 3 rows
	// Should only count INSERT rows (3), not DELETE rows
	update.addDeleteAffectRows(UpdateMainTable, 3) // Should not count
	require.Equal(t, uint64(0), update.ctr.affectedRows, "DELETE rows should not be counted for UPDATE")

	update.addInsertAffectRows(UpdateMainTable, 3) // Should count
	require.Equal(t, uint64(3), update.ctr.affectedRows, "INSERT rows should be counted for UPDATE")
}

func TestInsertAffectedRows(t *testing.T) {
	// Test INSERT scenario: should count INSERT rows
	update := &MultiUpdate{
		ctr: container{
			action:       actionInsert,
			affectedRows: 0,
		},
	}
	update.addAffectedRowsFunc = update.doAddAffectedRows

	update.addInsertAffectRows(UpdateMainTable, 5)
	require.Equal(t, uint64(5), update.ctr.affectedRows, "INSERT rows should be counted")
}

func TestDeleteAffectedRows(t *testing.T) {
	// Test DELETE scenario: should count DELETE rows
	update := &MultiUpdate{
		ctr: container{
			action:       actionDelete,
			affectedRows: 0,
		},
	}
	update.addAffectedRowsFunc = update.doAddAffectedRows

	update.addDeleteAffectRows(UpdateMainTable, 4)
	require.Equal(t, uint64(4), update.ctr.affectedRows, "DELETE rows should be counted")
}
