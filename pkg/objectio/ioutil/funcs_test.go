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

package ioutil

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

func TestEvalDeleteMaskFromDNCreatedTombstonesAbortColumnCompatibility(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	blockID := types.Blockid{}
	rowIDs := vector.NewVec(objectio.RowidType)
	commitTS := vector.NewVec(objectio.TSType)
	for _, offset := range []uint32{1, 2, 3} {
		require.NoError(t, vector.AppendFixed(rowIDs, types.NewRowid(&blockID, offset), false, mp))
		require.NoError(t, vector.AppendFixed(commitTS, types.BuildTS(1, 0), false, mp))
	}
	defer rowIDs.Free(mp)
	defer commitTS.Free(mp)

	t.Run("legacy checkpoint without abort column", func(t *testing.T) {
		abortColumn := vector.NewConstNull(types.T_bool.ToType(), 3, mp)
		defer abortColumn.Free(mp)

		rows, err := EvalDeleteMaskFromDNCreatedTombstones(
			rowIDs, commitTS, abortColumn, objectio.BlockObject{}, types.BuildTSForTest(2, 0), &blockID,
		)
		require.NoError(t, err)
		require.True(t, rows.IsValid())
		require.True(t, rows.Contains(1))
		require.True(t, rows.Contains(2))
		require.True(t, rows.Contains(3))
		rows.Release()
	})

	t.Run("current checkpoint with abort column", func(t *testing.T) {
		abortColumn := vector.NewVec(types.T_bool.ToType())
		for _, aborted := range []bool{false, true, false} {
			require.NoError(t, vector.AppendFixed(abortColumn, aborted, false, mp))
		}
		defer abortColumn.Free(mp)

		rows, err := EvalDeleteMaskFromDNCreatedTombstones(
			rowIDs, commitTS, abortColumn, objectio.BlockObject{}, types.BuildTSForTest(2, 0), &blockID,
		)
		require.NoError(t, err)
		require.True(t, rows.IsValid())
		require.True(t, rows.Contains(1))
		require.False(t, rows.Contains(2))
		require.True(t, rows.Contains(3))
		rows.Release()
	})

	t.Run("missing commit timestamp returns an error", func(t *testing.T) {
		missingCommitTS := vector.NewConstNull(types.T_TS.ToType(), 3, mp)
		abortColumn := vector.NewConstNull(types.T_bool.ToType(), 3, mp)
		defer missingCommitTS.Free(mp)
		defer abortColumn.Free(mp)

		rows, err := EvalDeleteMaskFromDNCreatedTombstones(
			rowIDs, missingCommitTS, abortColumn, objectio.BlockObject{}, types.BuildTSForTest(2, 0), &blockID,
		)
		require.ErrorContains(t, err, "commit-ts column is unavailable")
		require.False(t, rows.IsValid())
	})
}

func TestValidateTombstoneCommitTSColumn(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	valid := vector.NewVec(types.T_TS.ToType())
	for i := int64(1); i <= 3; i++ {
		require.NoError(t, vector.AppendFixed(valid, types.BuildTS(i, 0), false, mp))
	}
	column, err := ValidateTombstoneCommitTSColumn(3, valid)
	require.NoError(t, err)
	require.True(t, column.IsPresent())
	require.Equal(t, types.BuildTS(1, 0), column.At(0))
	require.Equal(t, types.BuildTS(3, 0), column.At(2))
	valid.Free(mp)

	missing := vector.NewConstNull(types.T_TS.ToType(), 3, mp)
	_, err = ValidateTombstoneCommitTSColumn(3, missing)
	require.ErrorContains(t, err, "unavailable")
	missing.Free(mp)

	partial := vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(partial, types.BuildTS(1, 0), false, mp))
	_, err = ValidateTombstoneCommitTSColumn(3, partial)
	require.ErrorContains(t, err, "1 rows, expected 3")
	partial.Free(mp)

	shortBacking := vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(shortBacking, types.BuildTS(1, 0), false, mp))
	shortBacking.SetLength(3)
	_, err = ValidateTombstoneCommitTSColumn(3, shortBacking)
	require.ErrorContains(t, err, "backing bytes")
	shortBacking.Free(mp)

	nullCommitTS := vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(nullCommitTS, types.BuildTS(1, 0), true, mp))
	_, err = ValidateTombstoneCommitTSColumn(1, nullCommitTS)
	require.ErrorContains(t, err, "contains null rows")
	nullCommitTS.Free(mp)

	wrongType := vector.NewVec(types.T_int64.ToType())
	_, err = ValidateTombstoneCommitTSColumn(0, wrongType)
	require.ErrorContains(t, err, "expected TS")
	wrongType.Free(mp)

	constant, err := vector.NewConstFixed(types.T_TS.ToType(), types.BuildTS(1, 0), 3, mp)
	require.NoError(t, err)
	column, err = ValidateTombstoneCommitTSColumn(3, constant)
	require.NoError(t, err)
	require.True(t, column.IsPresent())
	require.Equal(t, types.BuildTS(1, 0), column.At(0))
	require.Equal(t, types.BuildTS(1, 0), column.At(2))
	constant.Free(mp)
}

func TestValidateTombstoneAbortColumn(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	legacy := vector.NewConstNull(types.T_bool.ToType(), 3, mp)
	column, err := ValidateTombstoneAbortColumn(3, legacy)
	require.NoError(t, err)
	require.False(t, column.IsPresent())
	legacy.Free(mp)

	partial := vector.NewVec(types.T_bool.ToType())
	require.NoError(t, vector.AppendFixed(partial, true, false, mp))
	_, err = ValidateTombstoneAbortColumn(3, partial)
	require.Error(t, err)
	partial.Free(mp)

	wrongType := vector.NewVec(types.T_int8.ToType())
	_, err = ValidateTombstoneAbortColumn(3, wrongType)
	require.Error(t, err)
	wrongType.Free(mp)

	nullAbort := vector.NewVec(types.T_bool.ToType())
	require.NoError(t, vector.AppendFixed(nullAbort, false, true, mp))
	_, err = ValidateTombstoneAbortColumn(1, nullAbort)
	require.Error(t, err)
	nullAbort.Free(mp)

	constAbort, err := vector.NewConstFixed(types.T_bool.ToType(), true, 3, mp)
	require.NoError(t, err)
	column, err = ValidateTombstoneAbortColumn(3, constAbort)
	require.NoError(t, err)
	require.True(t, column.IsPresent())
	require.True(t, column.IsAborted(0))
	require.True(t, column.IsAborted(2))
	constAbort.Free(mp)

	constNull := vector.NewConstNull(types.T_bool.ToType(), 3, mp)
	column, err = ValidateTombstoneAbortColumn(3, constNull)
	require.NoError(t, err)
	require.False(t, column.IsPresent())
	constNull.Free(mp)

	empty := vector.NewVec(types.T_bool.ToType())
	_, err = ValidateTombstoneAbortColumn(3, empty)
	require.Error(t, err)
	empty.Free(mp)
}
