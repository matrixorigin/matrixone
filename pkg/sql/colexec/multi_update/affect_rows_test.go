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
	"testing"

	"github.com/stretchr/testify/require"
)

// newAffectRowsTestOp builds a bare MultiUpdate wired with the real
// affected-rows accumulator so the counting helpers can be exercised directly.
func newAffectRowsTestOp(action actionType, countDelete bool) *MultiUpdate {
	op := &MultiUpdate{}
	op.ctr.action = action
	op.CountDeleteAffectRows = countDelete
	op.addAffectedRowsFunc = op.doAddAffectedRows
	return op
}

// TestUpsertAffectRowsAccounting pins the MySQL-compatible affected-rows
// accounting for the main table: a plain UPDATE counts the matched row once
// (INSERT side only), while an upsert (REPLACE / INSERT ... ON DUPLICATE KEY
// UPDATE) counts both the conflicting-row DELETE and the INSERT, yielding 2 for
// a replaced/updated row and 1 for a newly inserted row.
func TestUpsertAffectRowsAccounting(t *testing.T) {
	t.Run("plain update counts matched row once", func(t *testing.T) {
		op := newAffectRowsTestOp(actionUpdate, false)
		op.addDeleteAffectRows(UpdateMainTable, 1) // not counted for plain UPDATE
		op.addInsertAffectRows(UpdateMainTable, 1)
		require.EqualValues(t, 1, op.GetAffectedRows())
	})

	t.Run("upsert updates an existing row -> 2", func(t *testing.T) {
		op := newAffectRowsTestOp(actionUpdate, true)
		op.addDeleteAffectRows(UpdateMainTable, 1)
		op.addInsertAffectRows(UpdateMainTable, 1)
		require.EqualValues(t, 2, op.GetAffectedRows())
	})

	t.Run("upsert inserts a brand new row -> 1", func(t *testing.T) {
		// A new row has no conflicting DELETE, only the INSERT is counted.
		op := newAffectRowsTestOp(actionUpdate, true)
		op.addInsertAffectRows(UpdateMainTable, 1)
		require.EqualValues(t, 1, op.GetAffectedRows())
	})

	t.Run("pure delete counts deleted rows", func(t *testing.T) {
		op := newAffectRowsTestOp(actionDelete, false)
		op.addDeleteAffectRows(UpdateMainTable, 3)
		require.EqualValues(t, 3, op.GetAffectedRows())
	})

	t.Run("pure insert counts inserted rows", func(t *testing.T) {
		op := newAffectRowsTestOp(actionInsert, false)
		op.addInsertAffectRows(UpdateMainTable, 5)
		require.EqualValues(t, 5, op.GetAffectedRows())
	})

	t.Run("index tables never affect rows", func(t *testing.T) {
		op := newAffectRowsTestOp(actionUpdate, true)
		op.addInsertAffectRows(UpdateUniqueIndexTable, 4)
		op.addDeleteAffectRows(UpdateUniqueIndexTable, 4)
		op.addInsertAffectRows(UpdateSecondaryIndexTable, 7)
		op.addDeleteAffectRows(UpdateSecondaryIndexTable, 7)
		require.EqualValues(t, 0, op.GetAffectedRows())
	})

	t.Run("batch upsert: new + updated rows", func(t *testing.T) {
		// 2 brand new rows (INSERT only) + 3 conflicting rows (DELETE + INSERT):
		// inserts cover all 5 rows, deletes cover the 3 conflicts => 5 + 3 = 8.
		op := newAffectRowsTestOp(actionUpdate, true)
		op.addInsertAffectRows(UpdateMainTable, 5)
		op.addDeleteAffectRows(UpdateMainTable, 3)
		require.EqualValues(t, 8, op.GetAffectedRows())
	})
}
