// Copyright 2025 Matrix Origin
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

package logtailreplay

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// TestFilterBatchCase2InsertThenDelete tests the fix for Case 2.2:
// When the first operation is an insert and the last operation is a delete,
// the delete should be preserved (not discarded).
//
// This is critical for CDC restart scenarios where:
// 1. Before restart: INSERT was sent to downstream, but DELETE was not sent yet
// 2. After restart: Both INSERT and DELETE are in the query range
// 3. OLD behavior: Both INSERT and DELETE were discarded (BUG! downstream has stale data)
// 4. NEW behavior: DELETE is kept and sent to downstream (FIXED! downstream is consistent)
func TestFilterBatchCase2InsertThenDelete(t *testing.T) {
	mp := mpool.MustNewZero()

	// Test scenario: For the same primary key:
	// 1. Insert at ts=100 (first operation)
	// 2. Delete at ts=200 (last operation)
	//
	// Expected behavior after fix:
	// - Insert should be removed (it's not the last operation)
	// - Delete should be KEPT (to ensure downstream consistency in CDC restart)

	pkValue := int32(999)
	primarySeqnum := 0

	// Create data batch with 1 insert operation
	dataBatch := batch.New([]string{"pk", "col1", "ts"})
	dataBatch.Vecs = make([]*vector.Vector, 3)

	// Primary key column
	pkVec := vector.NewVec(types.T_int32.ToType())
	vector.AppendFixed(pkVec, pkValue, false, mp) // Insert at ts=100
	dataBatch.Vecs[0] = pkVec

	// Data column
	col1Vec := vector.NewVec(types.T_int64.ToType())
	vector.AppendFixed(col1Vec, int64(100), false, mp)
	dataBatch.Vecs[1] = col1Vec

	// Timestamp column (last column)
	tsVec := vector.NewVec(types.T_TS.ToType())
	vector.AppendFixed(tsVec, types.BuildTS(100, 0), false, mp) // Insert at ts=100
	dataBatch.Vecs[2] = tsVec
	dataBatch.SetRowCount(1)

	// Create tombstone batch with 1 delete operation
	tombstoneBatch := batch.New([]string{"pk", "ts"})
	tombstoneBatch.Vecs = make([]*vector.Vector, 2)

	// Primary key column
	tombstonePkVec := vector.NewVec(types.T_int32.ToType())
	vector.AppendFixed(tombstonePkVec, pkValue, false, mp) // Delete at ts=200
	tombstoneBatch.Vecs[0] = tombstonePkVec

	// Timestamp column
	tombstoneTsVec := vector.NewVec(types.T_TS.ToType())
	vector.AppendFixed(tombstoneTsVec, types.BuildTS(200, 0), false, mp) // Delete at ts=200 (last)
	tombstoneBatch.Vecs[1] = tombstoneTsVec
	tombstoneBatch.SetRowCount(1)

	// Call filterBatch with skipDeletes=true (CDC scenario)
	err := filterBatch(dataBatch, tombstoneBatch, primarySeqnum, true)
	require.NoError(t, err)

	// Verify the fix:
	// OLD behavior (BUG): Both data and tombstone were empty (data=0, tombstone=0)
	// NEW behavior (FIX): Delete is preserved (data=0, tombstone=1)

	// Data batch should be empty (insert is removed because delete is the last operation)
	assert.Equal(t, 0, dataBatch.Vecs[0].Length(),
		"Data batch should be empty because the delete is the last operation")

	// Tombstone batch should contain the delete (THIS IS THE FIX)
	assert.Equal(t, 1, tombstoneBatch.Vecs[0].Length(),
		"Tombstone batch should contain the delete to ensure downstream consistency in CDC restart")

	// Verify the remaining delete has the correct timestamp
	if tombstoneBatch.Vecs[0].Length() > 0 {
		remainingDeleteTs := vector.MustFixedColWithTypeCheck[types.TS](tombstoneBatch.Vecs[1])
		assert.Equal(t, types.BuildTS(200, 0), remainingDeleteTs[0],
			"The remaining delete should be at ts=200")

		remainingDeletePk := vector.MustFixedColWithTypeCheck[int32](tombstoneBatch.Vecs[0])
		assert.Equal(t, pkValue, remainingDeletePk[0],
			"The remaining delete should have the correct primary key")
	}
}

// TestFilterBatchCase2InsertThenDeleteMultiple tests Case 2.2 with multiple operations:
// Insert -> Insert -> Delete -> Delete
// Only the last delete should be preserved.
func TestFilterBatchCase2InsertThenDeleteMultiple(t *testing.T) {
	mp := mpool.MustNewZero()

	// Test scenario: For the same primary key:
	// 1. Insert at ts=100 (first operation)
	// 2. Insert at ts=200 (middle operation)
	// 3. Delete at ts=300 (middle operation)
	// 4. Delete at ts=400 (last operation)
	//
	// Expected behavior after fix:
	// - All inserts should be removed
	// - Only the last delete (ts=400) should be kept

	pkValue := int32(888)
	primarySeqnum := 0

	// Create data batch with 2 insert operations
	dataBatch := batch.New([]string{"pk", "col1", "ts"})
	dataBatch.Vecs = make([]*vector.Vector, 3)

	pkVec := vector.NewVec(types.T_int32.ToType())
	vector.AppendFixed(pkVec, pkValue, false, mp) // Insert at ts=100
	vector.AppendFixed(pkVec, pkValue, false, mp) // Insert at ts=200
	dataBatch.Vecs[0] = pkVec

	col1Vec := vector.NewVec(types.T_int64.ToType())
	vector.AppendFixed(col1Vec, int64(100), false, mp)
	vector.AppendFixed(col1Vec, int64(200), false, mp)
	dataBatch.Vecs[1] = col1Vec

	tsVec := vector.NewVec(types.T_TS.ToType())
	vector.AppendFixed(tsVec, types.BuildTS(100, 0), false, mp)
	vector.AppendFixed(tsVec, types.BuildTS(200, 0), false, mp)
	dataBatch.Vecs[2] = tsVec
	dataBatch.SetRowCount(2)

	// Create tombstone batch with 2 delete operations
	tombstoneBatch := batch.New([]string{"pk", "ts"})
	tombstoneBatch.Vecs = make([]*vector.Vector, 2)

	tombstonePkVec := vector.NewVec(types.T_int32.ToType())
	vector.AppendFixed(tombstonePkVec, pkValue, false, mp) // Delete at ts=300
	vector.AppendFixed(tombstonePkVec, pkValue, false, mp) // Delete at ts=400
	tombstoneBatch.Vecs[0] = tombstonePkVec

	tombstoneTsVec := vector.NewVec(types.T_TS.ToType())
	vector.AppendFixed(tombstoneTsVec, types.BuildTS(300, 0), false, mp)
	vector.AppendFixed(tombstoneTsVec, types.BuildTS(400, 0), false, mp) // Last operation
	tombstoneBatch.Vecs[1] = tombstoneTsVec
	tombstoneBatch.SetRowCount(2)

	// Call filterBatch with skipDeletes=true
	err := filterBatch(dataBatch, tombstoneBatch, primarySeqnum, true)
	require.NoError(t, err)

	// Verify: data should be empty, tombstone should have 1 row (last delete)
	assert.Equal(t, 0, dataBatch.Vecs[0].Length(),
		"Data batch should be empty")
	assert.Equal(t, 1, tombstoneBatch.Vecs[0].Length(),
		"Tombstone batch should contain only the last delete")

	// Verify the remaining delete is the last one (ts=400)
	if tombstoneBatch.Vecs[0].Length() > 0 {
		remainingDeleteTs := vector.MustFixedColWithTypeCheck[types.TS](tombstoneBatch.Vecs[1])
		assert.Equal(t, types.BuildTS(400, 0), remainingDeleteTs[0],
			"The remaining delete should be at ts=400 (the last one)")
	}
}
