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
		abortColumn := vector.NewVec(types.T_bool.ToType())
		defer abortColumn.Free(mp)

		rows := EvalDeleteMaskFromDNCreatedTombstones(
			rowIDs, commitTS, abortColumn, objectio.BlockObject{}, types.BuildTSForTest(2, 0), &blockID,
		)
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

		rows := EvalDeleteMaskFromDNCreatedTombstones(
			rowIDs, commitTS, abortColumn, objectio.BlockObject{}, types.BuildTSForTest(2, 0), &blockID,
		)
		require.True(t, rows.IsValid())
		require.True(t, rows.Contains(1))
		require.False(t, rows.Contains(2))
		require.True(t, rows.Contains(3))
		rows.Release()
	})
}
