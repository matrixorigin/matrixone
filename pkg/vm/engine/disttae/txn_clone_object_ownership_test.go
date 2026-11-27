// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestDeletedBlocksIsolateSharedBlockByTable(t *testing.T) {
	const (
		tableA uint64 = 100
		tableB uint64 = 200
	)
	rowID := types.RandomRowid()
	blockID, _ := rowID.Decode()
	deletes := &deletedBlocks{offsets: map[uint64]map[types.Blockid][]int64{}}
	deletes.addDeletedBlocks(tableA, blockID, []int64{1})
	deletes.addDeletedBlocks(tableB, blockID, []int64{2, 3})

	require.Len(t, deletes.offsets, 2)
	require.Equal(t, 2, deletes.size())
	readAll := func(tableID uint64) []int64 {
		var offsets []int64
		deletes.getDeletedRowIDs(tableID, func(row types.Rowid) {
			_, offset := row.Decode()
			offsets = append(offsets, int64(offset))
		})
		return offsets
	}
	require.ElementsMatch(t, []int64{1}, readAll(tableA))
	require.ElementsMatch(t, []int64{2, 3}, readAll(tableB))

	deletes.clean()
	require.Zero(t, deletes.size())
	require.Empty(t, deletes.offsets)
}
