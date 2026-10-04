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

package mergesort

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/stretchr/testify/require"
)

func TestTransferMappingDigestCoversCreatedOrderAndMapping(t *testing.T) {
	table, err := NewLifecycleTransferTable(api.TransferMaps{
		{
			{ObjIdx: 0, BlkIdx: 0, RowIdx: 0},
			{ObjIdx: api.NoTransfer},
		},
	})
	require.NoError(t, err)
	t.Cleanup(table.Release)
	base := TransferMappingDigest([][]byte{[]byte("a"), []byte("b")}, table)
	require.Equal(
		t,
		base,
		TransferMappingDigest([][]byte{[]byte("a"), []byte("b")}, table),
	)
	require.NotEqual(
		t,
		base,
		TransferMappingDigest([][]byte{[]byte("b"), []byte("a")}, table),
	)
	table.GetBlockMap(0)[0].RowIdx = 1
	require.NotEqual(
		t,
		base,
		TransferMappingDigest([][]byte{[]byte("a"), []byte("b")}, table),
	)
}

func TestNewLifecycleTransferTable(t *testing.T) {
	t.Run("copies active and fully deleted blocks", func(t *testing.T) {
		maps := api.TransferMaps{
			nil,
			{{ObjIdx: 0, RowIdx: 1}, {ObjIdx: api.NoTransfer}},
		}
		table, err := NewLifecycleTransferTable(maps)
		require.NoError(t, err)
		t.Cleanup(table.Release)
		require.Equal(t, 2, table.Len())
		require.Equal(t, 2, table.Stride)
		require.Nil(t, table.GetBlockMap(0))
		require.Equal(t, maps[1], table.GetBlockMap(1))
		maps[1][0].RowIdx = 2
		require.Equal(t, uint32(1), table.GetBlockMap(1)[0].RowIdx)
		table.Release()
		require.Nil(t, table.Slab)
		require.Nil(t, table.BlockActive)
	})
	t.Run("all blocks deleted", func(t *testing.T) {
		table, err := NewLifecycleTransferTable(api.TransferMaps{nil, {}})
		require.NoError(t, err)
		t.Cleanup(table.Release)
		require.Equal(t, 2, table.Len())
		require.Nil(t, table.Slab)
		require.Nil(t, table.GetBlockMap(0))
		require.Nil(t, table.GetBlockMap(1))
	})
	t.Run("rejects inconsistent producer stride", func(t *testing.T) {
		table, err := NewLifecycleTransferTable(api.TransferMaps{
			{{ObjIdx: 0}},
			{{ObjIdx: 0}, {ObjIdx: api.NoTransfer}},
		})
		require.ErrorContains(t, err, "block strides differ")
		require.Nil(t, table)
	})
	t.Run("allocation failure", func(t *testing.T) {
		useCapacityLimitedTransferSlabMPool(t)
		table, err := NewLifecycleTransferTable(api.TransferMaps{{{ObjIdx: 0}}})
		require.Error(t, err)
		require.Nil(t, table)
	})
}
