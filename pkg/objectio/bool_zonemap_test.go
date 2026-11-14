// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectio

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestBoolZoneMapPersistence(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewFileService(ctx, fileservice.Config{
		Name: defines.LocalFileServiceName, Backend: "DISK", DataDir: t.TempDir(), Cache: fileservice.DisabledCacheConfig,
	}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	mp := mpool.MustNewZero()
	t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
	for _, tc := range []struct {
		name           string
		values         []bool
		legacy, rawMax bool
	}{
		{"legacy_mixed", []bool{false, true}, true, false},
		{"mixed", []bool{false, true}, false, true},
		{"all_false", []bool{false, false}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bat := batch.NewWithSize(2)
			t.Cleanup(func() { bat.Clean(mp) })
			bat.Vecs[0] = vector.NewVec(types.T_bool.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_int8.ToType())
			require.NoError(t, vector.AppendFixedList(bat.Vecs[0], tc.values, nil, mp))
			require.NoError(t, vector.AppendFixedList(bat.Vecs[1], []int8{0, 1}, nil, mp))
			bat.SetRowCount(2)
			name := tc.name + ".blk"
			writer, err := NewObjectWriterSpecial(WriterNormal, name, fs)
			require.NoError(t, err)
			writer.SetSortKeySeqnum(0)
			block, err := writer.Write(bat)
			require.NoError(t, err)
			zm := index.NewZM(types.T_bool, 0)
			if tc.legacy {
				index.UpdateZM(zm, []byte{0})
			} else {
				require.NoError(t, index.BatchUpdateZM(zm, bat.Vecs[0]))
			}
			require.Equal(t, tc.rawMax, types.DecodeBool(zm.GetMaxBuf()))
			block.ColumnMeta(0).SetZoneMap(zm)
			writer.WriteObjectMeta(ctx, 2, []ColumnMeta{block.ColumnMeta(0), block.ColumnMeta(1)})
			_, err = writer.WriteEnd(ctx, WriteOptions{Type: WriteTS, Val: time.Now()})
			require.NoError(t, err)
			stats := writer.GetObjectStats()
			rawStats := ZoneMap(stats[zoneMapOffset : zoneMapOffset+zoneMapLen])
			require.Equal(t, tc.rawMax, types.DecodeBool(rawStats.GetMaxBuf()))
			require.True(t, types.DecodeBool(stats.SortKeyZoneMap().GetMaxBuf()))
			reader, err := NewObjectReaderWithStr(name, fs)
			require.NoError(t, err)
			loaded, err := reader.ReadAllMeta(ctx, mp)
			require.NoError(t, err)
			meta, ok := loaded.DataMeta()
			require.True(t, ok)
			for _, col := range []ColumnMeta{meta.MustGetColumn(0), meta.GetBlockMeta(0).MustGetColumn(0)} {
				raw := ZoneMap(col[zoneMapOff : zoneMapOff+zoneMapLen])
				require.Equal(t, tc.rawMax, types.DecodeBool(raw.GetMaxBuf()), "真实序列化边界不能被兼容读视图污染")
				require.True(t, types.DecodeBool(col.ZoneMap().GetMaxBuf()))
			}
			raw, err := reader.ReadExtent(ctx, meta.BlockHeader().ZoneMapArea())
			require.NoError(t, err)
			view := ZoneMapArea(raw).GetZoneMap(0, 0)
			require.Len(t, view, 2*ZoneMapSize)
			require.True(t, types.DecodeBool(view.GetMaxBuf()))
			offset, _ := BlockIndex(raw).BlockMetaPos(0)
			require.Equal(t, tc.rawMax, types.DecodeBool(ZoneMap(raw[offset:offset+ZoneMapSize]).GetMaxBuf()))
		})
	}
}
