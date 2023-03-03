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

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/stretchr/testify/require"
)

func TestPartitionStateRowsIter(t *testing.T) {
	state := NewPartitionState()
	ctx := context.Background()
	pool := mpool.MustNewZero()

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

	buildRowID := func(i int) types.Rowid {
		return types.Rowid{
			byte(i), 0, 0, 0, 0, 0,
			byte(i), 0, 0, 0, 0, 0, // block id
		}
	}

	{
		// insert
		rowIDVec := vector.New(types.T_Rowid.ToType())
		tsVec := vector.New(types.T_TS.ToType())
		vec1 := vector.New(types.T_int64.ToType())
		for i := 0; i < num; i++ {
			rowIDVec.Append(buildRowID(i+1), false, pool)
			tsVec.Append(types.BuildTS(int64(i), 0), false, pool)
			vec1.Append(int64(i), false, pool)
		}
		state.HandleRowsInsert(ctx, &api.Batch{
			Attrs: []string{"rowid", "time", "a"},
			Vecs: []*api.Vector{
				mustVectorToProto(rowIDVec),
				mustVectorToProto(tsVec),
				mustVectorToProto(vec1),
			},
		}, -1)
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
			require.True(t, state.RowExists(entry.RowID, ts))
		}
		require.Equal(t, i+1, n)
		require.Equal(t, i+1, len(rowIDs))
		require.Nil(t, iter.Close())
	}

	{
		// duplicated rows
		rowIDVec := vector.New(types.T_Rowid.ToType())
		tsVec := vector.New(types.T_TS.ToType())
		vec1 := vector.New(types.T_int64.ToType())
		for i := 0; i < num; i++ {
			rowIDVec.Append(buildRowID(i+1), false, pool)
			tsVec.Append(types.BuildTS(int64(i), 1), false, pool)
			vec1.Append(int64(i), false, pool)
		}
		state.HandleRowsInsert(ctx, &api.Batch{
			Attrs: []string{"rowid", "time", "a"},
			Vecs: []*api.Vector{
				mustVectorToProto(rowIDVec),
				mustVectorToProto(tsVec),
				mustVectorToProto(vec1),
			},
		}, -1)
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
		rowIDVec := vector.New(types.T_Rowid.ToType())
		tsVec := vector.New(types.T_TS.ToType())
		for i := 0; i < num; i++ {
			rowIDVec.Append(buildRowID(i+1), false, pool)
			tsVec.Append(types.BuildTS(int64(deleteAt+i), 1), false, pool)
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
			blockID, _ := catalog.DecodeRowid(buildRowID(i + 1))
			iter := state.NewRowsIter(types.BuildTS(int64(deleteAt+i+1), 0), &blockID, true)
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

	deleteAt = 2000
	{
		// duplicate delete
		rowIDVec := vector.New(types.T_Rowid.ToType())
		tsVec := vector.New(types.T_TS.ToType())
		for i := 0; i < num; i++ {
			rowIDVec.Append(buildRowID(i+1), false, pool)
			tsVec.Append(types.BuildTS(int64(deleteAt+i), 1), false, pool)
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
			blockID, _ := catalog.DecodeRowid(buildRowID(i + 1))
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
