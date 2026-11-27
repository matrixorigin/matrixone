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

package ckputil

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

func TestExportToTableRangesByFilter(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	bat := NewMetaBatch(mp)
	defer bat.Clean(mp)
	var stats objectio.ObjectStats
	var want []TableRange
	for i, tc := range []struct {
		table uint64
		kind  int8
	}{
		{1, ObjectType_Data},
		{2, ObjectType_Tombstone},
		{2, ObjectType_Data},
		{2, ObjectType_Tombstone},
		{2, ObjectType_Data},
		{3, ObjectType_Data},
	} {
		stats[0] = byte(i)
		r := TableRange{TableID: tc.table, ObjectType: tc.kind,
			Start: types.BuildTestRowid(int64(i+1), 0), End: types.BuildTestRowid(int64(i+1), 1), ObjectStats: stats}
		require.NoError(t, r.AppendTo(bat, mp))
		if tc.table == 2 && tc.kind == ObjectType_Tombstone {
			want = append(want, r)
		}
	}
	got := ExportToTableRangesByFilter(bat, 2, ObjectType_Tombstone)
	require.Equal(t, want, got)
	require.Nil(t, ExportToTableRangesByFilter(bat, 1, ObjectType_Tombstone))
	require.Nil(t, ExportToTableRangesByFilter(bat, 4, ObjectType_Data))
	got[0].ObjectStats[0] ^= 0xff
	require.Equal(t, want, ExportToTableRangesByFilter(bat, 2, ObjectType_Tombstone))
	// Only logical batch rows may be exported, even when vectors retain more.
	bat.SetRowCount(3)
	require.Equal(t, want[:1], ExportToTableRangesByFilter(bat, 2, ObjectType_Tombstone))
	require.Nil(t, ExportToTableRangesByFilter(bat, 3, ObjectType_Data))
}
