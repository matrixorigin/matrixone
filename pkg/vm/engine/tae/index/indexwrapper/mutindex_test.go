// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package indexwrapper

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

func TestContainsSkipsDuplicateAbortedOffsets(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	idx := NewMutIndex(types.T_int32.ToType())

	insert := func(offset int) {
		vec := vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixed(vec, int32(7), false, mp))
		require.NoError(t, idx.BatchUpsert(vec, offset))
		vec.Free(mp)
	}
	insert(0)
	insert(1)

	check := func(skipFn func(uint32) error) bool {
		keys := vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixed(keys, int32(7), false, mp))
		err := idx.Contains(
			context.Background(),
			keys,
			index.NewZM(types.T_int32, 0),
			&types.Blockid{},
			skipFn,
			mp,
		)
		require.NoError(t, err)
		deleted := keys.IsNull(0)
		keys.Free(mp)
		return deleted
	}

	require.True(t, check(func(row uint32) error {
		if row == 0 {
			return index.ErrNotFound
		}
		return nil
	}), "a live retry offset must still classify the key as deleted")
	require.False(t, check(func(uint32) error {
		return index.ErrNotFound
	}), "all-aborted offsets must leave the key visible")
}

func TestGetDuplicatedRowsWindow(t *testing.T) {
	for _, tc := range []struct {
		name     string
		offsets  []int
		min, max int32
		aborted  map[uint32]bool
		conflict bool
		want     int
	}{
		{name: "row zero", offsets: []int{0}, min: 0, max: 1, want: 0},
		{name: "inclusive lower", offsets: []int{0, 1}, min: 1, max: 2, want: 1},
		{name: "exclusive upper", offsets: []int{1}, min: 0, max: 1, want: -1},
		{name: "empty window", offsets: []int{1}, min: 1, max: 1, want: -1},
		{name: "aborted latest", offsets: []int{0, 1}, min: 0, max: 2, aborted: map[uint32]bool{1: true}, want: 0},
		{name: "all aborted", offsets: []int{0, 1}, min: 0, max: 2, aborted: map[uint32]bool{0: true, 1: true}, want: -1},
		{name: "conflict outside window", offsets: []int{1}, min: 0, max: 1, conflict: true, want: -1},
		{name: "conflict in empty window", offsets: []int{1}, min: 1, max: 1, conflict: true, want: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			idx := NewMutIndex(types.T_int32.ToType())
			defer idx.Close()
			keys := vector.NewVec(types.T_int32.ToType())
			defer keys.Free(mp)
			require.NoError(t, vector.AppendFixed(keys, int32(7), false, mp))
			for _, offset := range tc.offsets {
				require.NoError(t, idx.BatchUpsert(keys, offset))
			}
			rows := vector.NewVec(types.T_Rowid.ToType())
			defer rows.Free(mp)
			require.NoError(t, vector.AppendFixed(rows, types.Rowid{}, true, mp))
			err := idx.GetDuplicatedRows(context.Background(), keys, index.NewZM(types.T_int32, 0), &types.Blockid{}, rows,
				func() (int32, int32, error) { return tc.min, tc.max, nil },
				func(row uint32) error {
					if tc.aborted[row] {
						return index.ErrNotFound
					}
					if tc.conflict {
						return moerr.NewTxnWWConflictNoCtx(0, "")
					}
					return nil
				}, mp)
			if tc.conflict {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want < 0, rows.IsNull(0))
			if tc.want >= 0 {
				require.Equal(t, uint32(tc.want), vector.MustFixedColNoTypeCheck[types.Rowid](rows)[0].GetRowOffset())
			}
		})
	}
}
