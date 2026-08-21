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

package indexwrapper

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestMutIndexDeleteAtPreservesOtherPositions(t *testing.T) {
	idx := NewMutIndex(types.T_int32.ToType())
	keys := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	defer keys.Close()
	keys.Append(int32(7), false)
	keys.Append(int32(7), false)
	require.NoError(t, idx.BatchUpsert(keys.GetDownstreamVector(), 0))

	require.NoError(t, idx.DeleteAt(int32(7), 0))
	rows, err := idx.GetActiveRow(int32(7))
	require.NoError(t, err)
	require.Equal(t, []uint32{1}, rows)
}

func TestGetDuplicatedRowsSkipsIneligibleCandidates(t *testing.T) {
	idx := NewMutIndex(types.T_int32.ToType())
	indexedKeys := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	defer indexedKeys.Close()
	for i := 0; i < 3; i++ {
		indexedKeys.Append(int32(7), false)
	}
	require.NoError(t, idx.BatchUpsert(indexedKeys.GetDownstreamVector(), 0))

	query := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
	defer query.Close()
	query.Append(int32(7), false)
	rowIDs := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
	defer rowIDs.Close()
	rowIDs.Append(nil, true)

	objID := types.NewObjectid()
	blkID := types.NewBlockidWithObjectID(&objID, 0)
	err := idx.GetDuplicatedRows(
		context.Background(),
		query.GetDownstreamVector(),
		nil,
		&blkID,
		rowIDs.GetDownstreamVector(),
		func() (index.RowSelection, error) {
			return index.RowSelection{MinRow: 0, MaxRow: 3}, nil
		},
		func(row uint32) error {
			if row > 0 {
				return index.ErrNotFound
			}
			return nil
		},
		common.DefaultAllocator,
	)
	require.NoError(t, err)
	require.False(t, rowIDs.IsNull(0))
	rid := rowIDs.Get(0).(types.Rowid)
	require.Equal(t, uint32(0), rid.GetRowOffset())

	skippedRowIDs := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
	defer skippedRowIDs.Close()
	skippedRowIDs.Append(nil, true)
	err = idx.GetDuplicatedRows(
		context.Background(),
		query.GetDownstreamVector(),
		nil,
		&blkID,
		skippedRowIDs.GetDownstreamVector(),
		func() (index.RowSelection, error) {
			return index.RowSelection{MinRow: 0, MaxRow: 3}, nil
		},
		func(uint32) error { return index.ErrNotFound },
		common.DefaultAllocator,
	)
	require.NoError(t, err)
	require.True(t, skippedRowIDs.IsNull(0))
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
