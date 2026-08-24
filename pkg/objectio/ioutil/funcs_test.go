// Copyright 2026 Matrix Origin
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

package ioutil

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

func TestEvalDeleteMaskFromDNCreatedTombstonesAbortColumnCompatibility(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	blockID := types.Blockid{}
	rowIDs := vector.NewVec(objectio.RowidType)
	commitTS := vector.NewVec(objectio.TSType)
	for _, offset := range []uint32{1, 2, 3} {
		require.NoError(t, vector.AppendFixed(
			rowIDs, types.NewRowid(&blockID, offset), false, mp,
		))
		require.NoError(t, vector.AppendFixed(
			commitTS, types.BuildTS(1, 0), false, mp,
		))
	}
	defer rowIDs.Free(mp)
	defer commitTS.Free(mp)
	snapshot := types.BuildTS(2, 0)

	t.Run("legacy const-null abort", func(t *testing.T) {
		abortColumn := vector.NewConstNull(types.T_bool.ToType(), 3, mp)
		defer abortColumn.Free(mp)

		rows, err := EvalDeleteMaskFromDNCreatedTombstones(
			rowIDs, commitTS, abortColumn, objectio.BlockObject{}, &snapshot, &blockID,
		)
		require.NoError(t, err)
		defer rows.Release()
		require.True(t, rows.Contains(1))
		require.True(t, rows.Contains(2))
		require.True(t, rows.Contains(3))
	})

	t.Run("current abort metadata", func(t *testing.T) {
		abortColumn := vector.NewVec(types.T_bool.ToType())
		for _, aborted := range []bool{false, true, false} {
			require.NoError(t, vector.AppendFixed(abortColumn, aborted, false, mp))
		}
		defer abortColumn.Free(mp)

		rows, err := EvalDeleteMaskFromDNCreatedTombstones(
			rowIDs, commitTS, abortColumn, objectio.BlockObject{}, &snapshot, &blockID,
		)
		require.NoError(t, err)
		defer rows.Release()
		require.True(t, rows.Contains(1))
		require.False(t, rows.Contains(2))
		require.True(t, rows.Contains(3))
	})

	t.Run("untyped placeholder is malformed", func(t *testing.T) {
		abortColumn := vector.Vector{}
		rows, err := EvalDeleteMaskFromDNCreatedTombstones(
			rowIDs, commitTS, &abortColumn, objectio.BlockObject{}, &snapshot, &blockID,
		)
		require.ErrorContains(t, err, "expected BOOL")
		require.False(t, rows.IsValid())
	})

	t.Run("missing commit timestamp is malformed", func(t *testing.T) {
		missingCommitTS := vector.NewConstNull(types.T_TS.ToType(), 3, mp)
		abortColumn := vector.NewConstNull(types.T_bool.ToType(), 3, mp)
		defer missingCommitTS.Free(mp)
		defer abortColumn.Free(mp)

		rows, err := EvalDeleteMaskFromDNCreatedTombstones(
			rowIDs, missingCommitTS, abortColumn, objectio.BlockObject{}, &snapshot, &blockID,
		)
		require.ErrorContains(t, err, "commit-ts column is unavailable")
		require.False(t, rows.IsValid())
	})

	t.Run("uncommitted sentinel stays invisible at max snapshot", func(t *testing.T) {
		uncommittedRows := vector.NewVec(objectio.RowidType)
		require.NoError(t, vector.AppendFixed(
			uncommittedRows, types.NewRowid(&blockID, 4), false, mp,
		))
		defer uncommittedRows.Free(mp)
		uncommittedTS, err := vector.NewConstFixed(
			types.T_TS.ToType(), types.MaxTs(), 1, mp,
		)
		require.NoError(t, err)
		defer uncommittedTS.Free(mp)
		abortColumn := vector.NewConstNull(types.T_bool.ToType(), 1, mp)
		defer abortColumn.Free(mp)
		maxSnapshot := types.MaxTs()

		rows, err := EvalDeleteMaskFromDNCreatedTombstones(
			uncommittedRows, uncommittedTS, abortColumn,
			objectio.BlockObject{}, &maxSnapshot, &blockID,
		)
		require.NoError(t, err)
		defer rows.Release()
		require.False(t, rows.Contains(4))
	})
}

func TestTombstoneCommitTSColumnAtDoesNotAllocate(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	const rowCount = 1024
	commitTS := types.BuildTS(42, 0)
	vec, err := vector.NewConstFixed(types.T_TS.ToType(), commitTS, rowCount, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	column, err := ValidateTombstoneCommitTSColumn(rowCount, vec)
	require.NoError(t, err)
	var got types.TS
	allocs := testing.AllocsPerRun(100, func() {
		for row := 0; row < rowCount; row++ {
			got = column.At(row)
		}
	})
	require.Equal(t, commitTS, got)
	require.Zero(t, allocs)
}

func TestValidateTombstoneAbortColumnAcceptsEmptyBatch(t *testing.T) {
	vec := vector.NewVec(types.T_bool.ToType())
	defer vec.Free(nil)

	column, err := ValidateTombstoneAbortColumn(0, vec)
	require.NoError(t, err)
	require.True(t, column.IsPresent())
}

func TestValidateTombstoneRowIDColumnRejectsBroadcastAndMalformedData(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	blockID := types.Blockid{}
	rowID := types.NewRowid(&blockID, 1)
	constRowIDs, err := vector.NewConstFixed(objectio.RowidType, rowID, 3, mp)
	require.NoError(t, err)
	defer constRowIDs.Free(mp)
	_, err = ValidateTombstoneRowIDColumn(3, constRowIDs)
	require.ErrorContains(t, err, "rowid column is unavailable")

	dense := vector.NewVec(objectio.RowidType)
	defer dense.Free(mp)
	require.NoError(t, vector.AppendFixed(dense, rowID, false, mp))
	_, err = ValidateTombstoneRowIDColumn(2, dense)
	require.ErrorContains(t, err, "rowid column is unavailable")
}

func TestCheckTombstoneFileRejectsMalformedInputsWithoutCallbacks(t *testing.T) {
	getCalled := false
	_, _, _, err := CheckTombstoneFile(
		context.Background(),
		[]byte{1},
		func() (*objectio.ObjectStats, error) {
			getCalled = true
			return nil, nil
		},
		func(*objectio.ObjectStats, int) (bool, error) {
			t.Fatal("block callback must not run")
			return false, nil
		},
		nil,
	)
	require.ErrorContains(t, err, "invalid tombstone rowid prefix length")
	require.False(t, getCalled)

	stats := new(objectio.ObjectStats)
	returned := false
	_, _, _, err = CheckTombstoneFile(
		context.Background(),
		make([]byte, types.BlockidSize),
		func() (*objectio.ObjectStats, error) {
			if returned {
				return nil, nil
			}
			returned = true
			return stats, nil
		},
		func(*objectio.ObjectStats, int) (bool, error) {
			t.Fatal("block callback must not run")
			return false, nil
		},
		nil,
	)
	require.ErrorContains(t, err, "invalid rowid zone map")
}

func TestCheckTombstoneFileChecksCancellationBeforeIterator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	getCalled := false
	_, _, _, err := CheckTombstoneFile(
		ctx,
		make([]byte, types.BlockidSize),
		func() (*objectio.ObjectStats, error) {
			getCalled = true
			return nil, nil
		},
		func(*objectio.ObjectStats, int) (bool, error) { return false, nil },
		nil,
	)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, getCalled)
}
