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

package blockio

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestCoarseFilterTombstoneObject(t *testing.T) {
	mp := mpool.MustNewZero()

	fs := testutil.NewSharedFS()

	rowCnt := 100
	ssCnt := 2

	rowids := make([]types.Rowid, rowCnt)
	oss := make([]objectio.ObjectStats, ssCnt)
	for i := 0; i < ssCnt; i++ {
		writer := ConstructTombstoneWriter(false, fs)
		writer.SetDataType(objectio.SchemaTombstone)
		assert.NotNil(t, writer)

		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())

		for i := 0; i < rowCnt; i++ {
			row := types.RandomRowid()
			rowids[i] = row
			pk := rand.Int()

			err := vector.AppendFixed[types.Rowid](bat.Vecs[0], row, false, mp)
			require.NoError(t, err)

			err = vector.AppendFixed[int32](bat.Vecs[1], int32(pk), false, mp)
			require.NoError(t, err)
		}

		_, err := writer.WriteBatch(bat)
		require.NoError(t, err)

		_, _, err = writer.Sync(context.Background())
		require.NoError(t, err)

		ss := writer.GetObjectStats()
		require.Equal(t, rowCnt, int(ss.Rows()))
		oss[i] = ss
	}

	// test coarse filter
	var cur int
	next_obj := func() (id *objectio.ObjectId) {
		if cur >= len(rowids) {
			return nil
		}
		id = rowids[cur].BorrowObjectID()
		cur++
		return
	}

	filtered, err := CoarseFilterTombstoneObject(context.Background(), next_obj, oss, fs)
	require.NoError(t, err)
	require.Equal(t, 1, len(filtered))
	require.Equal(t, oss[ssCnt-1], filtered[0])
}
