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

package deletion

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

// collectBatchInfo groups deleted rows by block and copies each row's key into
// the block's batch. The copy must keep the key's exact value and type for
// every key type, including variable-length keys stored out of line and types
// the old boxed copy (getNonNullValue + AppendAny) did not handle.
func TestCollectBatchInfoCopiesKeys(t *testing.T) {
	long := strings.Repeat("k", 100) // longer than an inline varlena
	for _, tc := range []struct {
		name   string
		typ    types.Type
		append func(v *vector.Vector, i int, mp *mpool.MPool) error
		get    func(v *vector.Vector, i int) any
	}{
		{"int64", types.T_int64.ToType(),
			func(v *vector.Vector, i int, mp *mpool.MPool) error {
				return vector.AppendFixed(v, int64(100+i), false, mp)
			},
			func(v *vector.Vector, i int) any { return vector.GetFixedAtNoTypeCheck[int64](v, i) }},
		{"varchar", types.T_varchar.ToType(),
			func(v *vector.Vector, i int, mp *mpool.MPool) error {
				return vector.AppendBytes(v, []byte(long+string(rune('a'+i))), false, mp)
			},
			func(v *vector.Vector, i int) any { return string(v.GetBytesAt(i)) }},
		{"decimal128", types.New(types.T_decimal128, 20, 2),
			func(v *vector.Vector, i int, mp *mpool.MPool) error {
				return vector.AppendFixed(v, types.Decimal128{B0_63: uint64(7 + i)}, false, mp)
			},
			func(v *vector.Vector, i int) any { return vector.GetFixedAtNoTypeCheck[types.Decimal128](v, i) }},
		{"float16", types.T_float16.ToType(),
			func(v *vector.Vector, i int, mp *mpool.MPool) error {
				return vector.AppendFixed(v, types.Float16(0x3c00+i), false, mp)
			},
			func(v *vector.Vector, i int) any { return vector.GetFixedAtNoTypeCheck[types.Float16](v, i) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			defer proc.Free()
			mp := proc.Mp()
			before := mp.CurrNB()

			blk1 := types.BuildTestBlockid(1, 1)
			blk2 := types.BuildTestBlockid(1, 2)
			// rows 0 and 2 delete from blk1, row 1 from blk2, row 3 repeats row 0
			rowIDs := []types.Rowid{
				types.NewRowid(&blk1, 0),
				types.NewRowid(&blk2, 0),
				types.NewRowid(&blk1, 5),
				types.NewRowid(&blk1, 0),
			}
			src := batch.NewWithSize(2)
			src.Vecs[0] = vector.NewVec(types.T_Rowid.ToType(), mp)
			src.Vecs[1] = vector.NewVec(tc.typ, mp)
			for i, rid := range rowIDs {
				require.NoError(t, vector.AppendFixed(src.Vecs[0], rid, false, mp))
				require.NoError(t, tc.append(src.Vecs[1], i, mp))
			}
			src.SetRowCount(len(rowIDs))

			del := &Deletion{Nbucket: 1}
			del.ctr.blockId_type = make(map[types.Blockid]int8)
			del.ctr.blockId_bitmap = make(map[types.Blockid]*nulls.Nulls)
			del.ctr.pool = &BatchPool{}
			del.ctr.partitionId_blockId_rowIdBatch = make(map[int]map[types.Blockid]*batch.Batch)

			require.NoError(t, collectBatchInfo(proc, del, src, 0, 0, 1))

			blocks := del.ctr.partitionId_blockId_rowIdBatch[0]
			require.Len(t, blocks, 2)
			for blk, wantRows := range map[types.Blockid][]int{blk1: {0, 2}, blk2: {1}} {
				bat := blocks[blk]
				require.NotNil(t, bat)
				keys := bat.Vecs[1]
				require.Equal(t, tc.typ, *keys.GetType())
				require.Equal(t, len(wantRows), keys.Length())
				for j, row := range wantRows {
					require.Equal(t, rowIDs[row], vector.GetFixedAtNoTypeCheck[types.Rowid](bat.Vecs[0], j))
					require.False(t, keys.IsNull(uint64(j)))
					require.Equal(t, tc.get(src.Vecs[1], row), tc.get(keys, j))
				}
			}
			require.Equal(t, uint32(3), del.ctr.deleted_length)

			for _, bat := range blocks {
				bat.Clean(mp)
			}
			del.ctr.freeBlockBitmaps()
			src.Clean(mp)
			require.Equal(t, before, mp.CurrNB())
		})
	}
}
