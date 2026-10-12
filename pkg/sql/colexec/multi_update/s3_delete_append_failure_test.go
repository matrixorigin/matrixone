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

package multi_update

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestPrepareDeleteBatchesAppendFailure(t *testing.T) {
	for _, tc := range []struct {
		name           string
		preallocateRow bool
		varlenPK       bool
	}{
		{name: "rowid append"},
		{name: "pk union", preallocateRow: true},
		{name: "pk varlen area", preallocateRow: true, varlenPK: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp, err := mpool.NewMPool("delete-append-cap", 1<<20, mpool.NoFixed)
			require.NoError(t, err)
			proc := process.NewTopProcess(context.Background(), mp, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			defer proc.Free()
			defer mpool.DeleteMPool(mp)

			rowID := types.BuildTestRowid(1, 1)
			src := batch.NewOffHeap([]string{"rowid", "pk"})
			src.Vecs[RowIDIdx] = vector.NewOffHeapVecWithType(types.T_Rowid.ToType())
			if tc.varlenPK {
				src.Vecs[PkIdx] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
			} else {
				src.Vecs[PkIdx] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
			}
			require.NoError(t, vector.AppendFixed(src.Vecs[RowIDIdx], rowID, false, mp))
			if tc.varlenPK {
				require.NoError(t, vector.AppendBytes(src.Vecs[PkIdx], []byte("long primary key beyond inline storage"), false, mp))
			} else {
				require.NoError(t, vector.AppendFixed(src.Vecs[PkIdx], int64(42), false, mp))
			}
			src.SetRowCount(1)
			defer src.Clean(mp)

			writer := &s3WriterDelegate{deleteBlockMap: make([]map[types.Blockid]*deleteBlockData, 1)}
			blockID := rowID.CloneBlockID()
			if tc.preallocateRow {
				block := newDeleteBlockData(src, 1)
				require.NoError(t, block.bat.Vecs[RowIDIdx].PreExtend(1, mp))
				if tc.varlenPK {
					require.NoError(t, block.bat.Vecs[PkIdx].PreExtend(1, mp))
				}
				writer.deleteBlockMap[0] = map[types.Blockid]*deleteBlockData{blockID: block}
			}
			defer func() {
				for _, block := range writer.deleteBlockMap[0] {
					block.bat.Clean(mp)
				}
			}()

			remaining := mp.Cap() - mp.CurrNB() - 1
			require.Positive(t, remaining)
			pressure, err := mp.Alloc(int(remaining), true)
			require.NoError(t, err)
			defer func() {
				if pressure != nil {
					mp.Free(pressure)
				}
			}()

			bats, err := writer.prepareDeleteBatches(proc, 0, []*batch.Batch{src}, false)
			for _, bat := range bats {
				bat.Clean(mp)
			}
			require.Error(t, err, "an omitted tombstone must abort the delete")
			require.Nil(t, bats)
			block := writer.deleteBlockMap[0][blockID]
			require.NotNil(t, block)
			require.False(t, block.bitmap.Contains(uint64(rowID.GetRowOffset())))
			require.Equal(t, 0, block.bat.RowCount())
			require.Equal(t, 0, block.bat.Vecs[RowIDIdx].Length())
			require.Equal(t, 0, block.bat.Vecs[PkIdx].Length())

			mp.Free(pressure)
			pressure = nil
			bats, err = writer.prepareDeleteBatches(proc, 0, []*batch.Batch{src, src}, false)
			require.NoError(t, err)
			require.Len(t, bats, 1)
			require.Equal(t, 1, bats[0].RowCount(), "retry must append once and deduplicate")
			require.Equal(t, 1, bats[0].Vecs[RowIDIdx].Length())
			require.Equal(t, 1, bats[0].Vecs[PkIdx].Length())
			bats[0].Clean(mp)
		})
	}
}
