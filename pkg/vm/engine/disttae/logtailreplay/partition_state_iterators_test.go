// Copyright 2023 Matrix Origin
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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/stretchr/testify/require"
)

func TestPartitionStateRowsIter(t *testing.T) {
	state := NewPartitionState(false)
	ctx := context.Background()
	pool := mpool.MustNewZero()
	packer := types.NewPacker(pool)
	defer packer.FreeMem()

	{
		// empty rows
		iter := state.NewRowsIter(types.BuildTS(0, 0), nil, false)
		n := 0
		for iter.Next() {
			n++
		}
		require.Equal(t, 0, n)
		require.Nil(t, iter.Close())
	}

	{
		iter := state.NewRowsIter(types.BuildTS(0, 0), nil, false)
		n := 0
		for iter.Next() {
			n++
		}
		require.Equal(t, 0, n)
		require.Nil(t, iter.Close())
	}

	const num = 128

	sid := objectio.NewSegmentid()
	buildRowID := func(i int) types.Rowid {
		blk := objectio.NewBlockid(&sid, uint16(i), 0)
		return objectio.NewRowid(&blk, uint32(0))
	}

	{
		// insert
		rowIDVec := vector.NewVec(types.T_Rowid.ToType())
		tsVec := vector.NewVec(types.T_TS.ToType())
		vec1 := vector.NewVec(types.T_int64.ToType())
		for i := 0; i < num; i++ {
			vector.AppendFixed(rowIDVec, buildRowID(i+1), false, pool)
			vector.AppendFixed(tsVec, types.BuildTS(int64(i), 0), false, pool)
			vector.AppendFixed(vec1, int64(i), false, pool)
		}
		state.HandleRowsInsert(ctx, &api.Batch{
			Attrs: []string{"rowid", "time", "a"},
			Vecs: []*api.Vector{
				mustVectorToProto(rowIDVec),
				mustVectorToProto(tsVec),
				mustVectorToProto(vec1),
			},
		}, 0, packer)
	}

	for i := 0; i < num; i++ {
		ts := types.BuildTS(int64(i), 0)
		iter := state.NewRowsIter(ts, nil, false)
		n := 0
		rowIDs := make(map[types.Rowid]bool)
		for iter.Next() {
			n++
			entry := iter.Entry()
			rowIDs[entry.RowID] = true

			// RowExists
			require.True(t, state.RowExists(entry.RowID, ts))

		}
		require.Equal(t, i+1, n)
		require.Equal(t, i+1, len(rowIDs))
		require.Nil(t, iter.Close())
	}

	// primary key iter
	for i := 0; i < num; i++ {
		ts := types.BuildTS(int64(i), 0)
		bs := EncodePrimaryKey(int64(i), packer)
		iter := state.NewPrimaryKeyIter(ts, bs)
		n := 0
		for iter.Next() {
			n++
		}
		require.Equal(t, 1, n)
		require.Nil(t, iter.Close())
	}

	{
		// duplicated rows
		rowIDVec := vector.NewVec(types.T_Rowid.ToType())
		tsVec := vector.NewVec(types.T_TS.ToType())
		vec1 := vector.NewVec(types.T_int64.ToType())
		for i := 0; i < num; i++ {
			vector.AppendFixed(rowIDVec, buildRowID(i+1), false, pool)
			vector.AppendFixed(tsVec, types.BuildTS(int64(i), 1), false, pool)
			vector.AppendFixed(vec1, int64(i), false, pool)
		}
		state.HandleRowsInsert(ctx, &api.Batch{
			Attrs: []string{"rowid", "time", "a"},
			Vecs: []*api.Vector{
				mustVectorToProto(rowIDVec),
				mustVectorToProto(tsVec),
				mustVectorToProto(vec1),
			},
		}, 0, packer)
	}

	for i := 0; i < num; i++ {
		iter := state.NewRowsIter(types.BuildTS(int64(i), 0), nil, false)
		n := 0
		rowIDs := make(map[types.Rowid]bool)
		for iter.Next() {
			n++
			entry := iter.Entry()
			rowIDs[entry.RowID] = true
		}
		require.Equal(t, i+1, n)
		require.Equal(t, i+1, len(rowIDs))
		require.Nil(t, iter.Close())
	}

	deleteAt := 1000
	{
		// delete
		rowIDVec := vector.NewVec(types.T_Rowid.ToType())
		tsVec := vector.NewVec(types.T_TS.ToType())
		for i := 0; i < num; i++ {
			vector.AppendFixed(rowIDVec, buildRowID(i+1), false, pool)
			vector.AppendFixed(tsVec, types.BuildTS(int64(deleteAt+i), 1), false, pool)
		}
		state.HandleRowsDelete(ctx, &api.Batch{
			Attrs: []string{"rowid", "time", "a"},
			Vecs: []*api.Vector{
				mustVectorToProto(rowIDVec),
				mustVectorToProto(tsVec),
			},
		})
	}

	for i := 0; i < num; i++ {
		{
			iter := state.NewRowsIter(types.BuildTS(int64(deleteAt+i), 0), nil, false)
			rowIDs := make(map[types.Rowid]bool)
			n := 0
			for iter.Next() {
				n++
				entry := iter.Entry()
				rowIDs[entry.RowID] = true
			}
			require.Equal(t, num-i, n)
			require.Equal(t, num-i, len(rowIDs))
			require.Nil(t, iter.Close())
		}

		{
			blockID, _ := buildRowID(i + 1).Decode()
			iter := state.NewRowsIter(types.BuildTS(int64(deleteAt+i+1), 0), &blockID, true)
			rowIDs := make(map[types.Rowid]bool)
			n := 0
			for iter.Next() {
				n++
				entry := iter.Entry()
				rowIDs[entry.RowID] = true
			}
			require.Equal(t, 1, n, "num is %d", i)
			require.Equal(t, 1, len(rowIDs))
			require.Nil(t, iter.Close())
		}
	}

	deleteAt = 2000
	{
		// duplicate delete
		rowIDVec := vector.NewVec(types.T_Rowid.ToType())
		tsVec := vector.NewVec(types.T_TS.ToType())
		for i := 0; i < num; i++ {
			vector.AppendFixed(rowIDVec, buildRowID(i+1), false, pool)
			vector.AppendFixed(tsVec, types.BuildTS(int64(deleteAt+i), 1), false, pool)
		}
		state.HandleRowsDelete(ctx, &api.Batch{
			Attrs: []string{"rowid", "time", "a"},
			Vecs: []*api.Vector{
				mustVectorToProto(rowIDVec),
				mustVectorToProto(tsVec),
			},
		})
	}

	for i := 0; i < num; i++ {
		{
			blockID, _ := buildRowID(i + 1).Decode()
			iter := state.NewRowsIter(types.BuildTS(int64(deleteAt+i), 0), &blockID, true)
			rowIDs := make(map[types.Rowid]bool)
			n := 0
			for iter.Next() {
				n++
				entry := iter.Entry()
				rowIDs[entry.RowID] = true
			}
			require.Equal(t, 1, n)
			require.Equal(t, 1, len(rowIDs))
			require.Nil(t, iter.Close())
		}
	}

}
