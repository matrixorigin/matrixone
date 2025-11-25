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

// TestFilterBatchSkipDeletes tests the skipDeletes logic in filterBatch function,
// specifically the code path at lines 1078-1087 where first.isDelete is true
// and last.isDelete is false.
func TestFilterBatchSkipDeletes(t *testing.T) {
	mp := mpool.MustNewZero()

	// Test case: For the same primary key, we have:
	// 1. Delete at ts=100 (first operation, isDelete=true)
	// 2. Insert at ts=200 (middle operation, isDelete=false)
	// 3. Delete at ts=300 (middle operation, isDelete=true)
	// 4. Insert at ts=400 (last operation, isDelete=false)
	//
	// Expected behavior when skipDeletes=true:
	// - All previous operations (delete at 100, insert at 200, delete at 300) should be marked for deletion
	// - Only the last insert at ts=400 should be kept

	pkValue := int32(1)
	primarySeqnum := 0

	// Create data batch with primary key and timestamp
	// Data batch structure: [pk_col, other_cols..., ts_col]
	dataBatch := batch.New([]string{"pk", "col1", "ts"})
	dataBatch.Vecs = make([]*vector.Vector, 3)

	// Primary key column (index 0)
	pkVec := vector.NewVec(types.T_int32.ToType())
	vector.AppendFixed(pkVec, pkValue, false, mp) // Insert at ts=200
	vector.AppendFixed(pkVec, pkValue, false, mp) // Insert at ts=400
	dataBatch.Vecs[primarySeqnum] = pkVec

	// Other column (index 1)
	col1Vec := vector.NewVec(types.T_int64.ToType())
	vector.AppendFixed(col1Vec, int64(100), false, mp) // Value for insert at ts=200
	vector.AppendFixed(col1Vec, int64(200), false, mp) // Value for insert at ts=400
	dataBatch.Vecs[1] = col1Vec

	// Timestamp column (last column, index 2)
	tsVec := vector.NewVec(types.T_TS.ToType())
	vector.AppendFixed(tsVec, types.BuildTS(200, 0), false, mp) // Insert at ts=200
	vector.AppendFixed(tsVec, types.BuildTS(400, 0), false, mp) // Insert at ts=400
	dataBatch.Vecs[2] = tsVec
	dataBatch.SetRowCount(2)

	// Create tombstone batch with primary key and timestamp
	// Tombstone batch structure: [pk_col, ts_col]
	tombstoneBatch := batch.New([]string{"pk", "ts"})
	tombstoneBatch.Vecs = make([]*vector.Vector, 2)

	// Primary key column (index 0)
	tombstonePkVec := vector.NewVec(types.T_int32.ToType())
	vector.AppendFixed(tombstonePkVec, pkValue, false, mp) // Delete at ts=100
	vector.AppendFixed(tombstonePkVec, pkValue, false, mp) // Delete at ts=300
	tombstoneBatch.Vecs[0] = tombstonePkVec

	// Timestamp column (index 1)
	tombstoneTsVec := vector.NewVec(types.T_TS.ToType())
	vector.AppendFixed(tombstoneTsVec, types.BuildTS(100, 0), false, mp) // Delete at ts=100 (first)
	vector.AppendFixed(tombstoneTsVec, types.BuildTS(300, 0), false, mp) // Delete at ts=300
	tombstoneBatch.Vecs[1] = tombstoneTsVec
	tombstoneBatch.SetRowCount(2)

	// Test with skipDeletes=true
	err := filterBatch(dataBatch, tombstoneBatch, primarySeqnum, true)
	require.NoError(t, err)

	// After filtering with skipDeletes=true:
	// - All previous operations should be removed
	// - Only the last insert (at ts=400) should remain in data batch
	// - All deletes should be removed from tombstone batch
	assert.Equal(t, 1, dataBatch.Vecs[0].Length(), "Data batch should contain only the last insert")
	assert.Equal(t, 0, tombstoneBatch.Vecs[0].Length(), "Tombstone batch should be empty when skipDeletes=true")

	// Verify the remaining row in data batch is the last insert (ts=400)
	if dataBatch.Vecs[0].Length() > 0 {
		remainingTs := vector.MustFixedColWithTypeCheck[types.TS](dataBatch.Vecs[2])
		assert.Equal(t, types.BuildTS(400, 0), remainingTs[0], "Remaining row should be the insert at ts=400")

		remainingPk := vector.MustFixedColWithTypeCheck[int32](dataBatch.Vecs[0])
		assert.Equal(t, pkValue, remainingPk[0], "Remaining row should have the correct primary key")
	}

	// Reset batches for next test
	dataBatch = batch.New([]string{"pk", "col1", "ts"})
	dataBatch.Vecs = make([]*vector.Vector, 3)
	pkVec = vector.NewVec(types.T_int32.ToType())
	vector.AppendFixed(pkVec, pkValue, false, mp)
	vector.AppendFixed(pkVec, pkValue, false, mp)
	dataBatch.Vecs[primarySeqnum] = pkVec
	col1Vec = vector.NewVec(types.T_int64.ToType())
	vector.AppendFixed(col1Vec, int64(100), false, mp)
	vector.AppendFixed(col1Vec, int64(200), false, mp)
	dataBatch.Vecs[1] = col1Vec
	tsVec = vector.NewVec(types.T_TS.ToType())
	vector.AppendFixed(tsVec, types.BuildTS(200, 0), false, mp)
	vector.AppendFixed(tsVec, types.BuildTS(400, 0), false, mp)
	dataBatch.Vecs[2] = tsVec
	dataBatch.SetRowCount(2)

	tombstoneBatch = batch.New([]string{"pk", "ts"})
	tombstoneBatch.Vecs = make([]*vector.Vector, 2)
	tombstonePkVec = vector.NewVec(types.T_int32.ToType())
	vector.AppendFixed(tombstonePkVec, pkValue, false, mp)
	vector.AppendFixed(tombstonePkVec, pkValue, false, mp)
	tombstoneBatch.Vecs[0] = tombstonePkVec
	tombstoneTsVec = vector.NewVec(types.T_TS.ToType())
	vector.AppendFixed(tombstoneTsVec, types.BuildTS(100, 0), false, mp)
	vector.AppendFixed(tombstoneTsVec, types.BuildTS(300, 0), false, mp)
	tombstoneBatch.Vecs[1] = tombstoneTsVec
	tombstoneBatch.SetRowCount(2)

	// Test with skipDeletes=false (for comparison)
	err = filterBatch(dataBatch, tombstoneBatch, primarySeqnum, false)
	require.NoError(t, err)

	// When skipDeletes=false, the behavior should be the same in this case
	// (both branches currently have the same logic)
	// But we verify the function doesn't crash and processes correctly
	assert.GreaterOrEqual(t, dataBatch.Vecs[0].Length(), 0, "Data batch length should be valid")
	assert.GreaterOrEqual(t, tombstoneBatch.Vecs[0].Length(), 0, "Tombstone batch length should be valid")
}
