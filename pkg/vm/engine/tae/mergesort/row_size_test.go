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

package mergesort

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestMergeFlushesOversizedRows(t *testing.T) {
	for _, sorted := range []bool{true, false} {
		name := "reshape"
		if sorted {
			name = "sorted merge"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			mp := mpool.MustNewZero()
			fs := testutil.NewSharedFS()
			host := &sizeLimitMergeHost{
				jsonReadbackMergeHost: jsonReadbackMergeHost{mp: mp, loaded: make([]bool, 2)},
				fs:                    fs,
				sortKey:               -1,
				deletes:               []*nulls.Nulls{{}, {}},
			}
			if sorted {
				host.sortKey = 0
			}
			t.Cleanup(func() {
				if host.transfer != nil {
					host.transfer.Release()
				}
				for _, run := range host.runs {
					run.Clean(mp)
				}
				fs.Close(ctx)
				require.Zero(t, mp.CurrNB())
				mpool.DeleteMPool(mp)
			})
			// Use a scaled byte budget: even a single large row may exceed it.
			payloadSizes := []int{1, 1, 300, 1, 1, 80, 80, 1}
			for run := range 2 {
				bat := batch.NewWithSize(2)
				bat.Attrs = []string{"key", "payload"}
				bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
				bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
				host.runs = append(host.runs, bat)
				for row := range 4 {
					key := row*2 + run
					require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(key), false, mp))
					require.NoError(t, vector.AppendBytes(bat.Vecs[1], bytes.Repeat([]byte{'x'}, payloadSizes[key]), false, mp))
				}
				bat.SetRowCount(4)
			}
			host.deletes[0].Add(2) // key 4 must retain the NoTransfer sentinel.

			if sorted {
				require.NoError(t, mergeObjs(ctx, host, 0))
			} else {
				require.NoError(t, reshape(ctx, host))
			}
			require.NotNil(t, host.transfer)
			require.Equal(t, 8, host.transfer.Stride, "source block stride must not shrink with output blocks")
			require.Len(t, host.commitEntry.CreatedObjs, 3, "flush before the row limit and before 100 rows")

			var actual []int64
			output := make([][][]int64, 0, len(host.commitEntry.CreatedObjs)) // object -> block -> keys
			for _, raw := range host.commitEntry.CreatedObjs {
				stat := objectio.ObjectStats(raw)
				reader, err := ioutil.NewFileReaderNoCache(fs, stat.ObjectName().String())
				require.NoError(t, err)
				bats, release, err := reader.LoadAllColumns(ctx, []uint16{0, 1}, mp)
				require.NoError(t, err)
				var blocks [][]int64
				for _, bat := range bats {
					keys := append([]int64(nil), vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])...)
					blocks = append(blocks, keys)
					actual = append(actual, keys...)
					// All rows before the last fit the budget: only one row can
					// overshoot it, including when that row alone is oversized.
					prefixSize := 0
					for row, key := range keys {
						require.LessOrEqual(t, prefixSize, 192)
						value := bat.Vecs[1].GetBytesAt(row)
						require.Equal(t, bytes.Repeat([]byte{'x'}, payloadSizes[key]), value)
						prefixSize += 8 + types.VarlenaSize
						if len(value) > types.VarlenaInlineSize {
							prefixSize += len(value)
						}
					}
				}
				output = append(output, blocks)
				release()
			}
			expected := []int64{0, 2, 6, 1, 3, 5, 7}
			if sorted {
				expected = []int64{0, 1, 2, 3, 5, 6, 7}
			}
			require.Equal(t, expected, actual)
			for run := range 2 {
				mapping := host.transfer.GetBlockMap(run)
				for row := range 4 {
					key := int64(row*2 + run)
					dest := mapping[row]
					if key == 4 {
						require.Equal(t, api.NoTransfer, dest.ObjIdx)
						continue
					}
					require.Less(t, int(dest.ObjIdx), len(output))
					blocks := output[dest.ObjIdx]
					require.Less(t, int(dest.BlkIdx), len(blocks))
					keys := blocks[dest.BlkIdx]
					require.Less(t, int(dest.RowIdx), len(keys))
					require.Equal(t, key, keys[dest.RowIdx])
				}
			}
		})
	}
}

type sizeLimitMergeHost struct {
	jsonReadbackMergeHost
	fs       fileservice.FileService
	sortKey  int
	deletes  []*nulls.Nulls
	transfer *TransferTable
}

func (h *sizeLimitMergeHost) DoTransfer() bool                  { return true }
func (h *sizeLimitMergeHost) SetTransferTable(t *TransferTable) { h.transfer = t }
func (h *sizeLimitMergeHost) GetBlockMaxRows() uint32           { return 8 }
func (h *sizeLimitMergeHost) GetTargetObjSize() uint32          { return 128 }
func (h *sizeLimitMergeHost) GetSortKeyType() types.Type        { return types.T_int64.ToType() }
func (h *sizeLimitMergeHost) PrepareNewWriter() *ioutil.BlockWriter {
	return ioutil.ConstructWriter(0, []uint16{0, 1}, h.sortKey, h.sortKey >= 0, false, h.fs)
}
func (h *sizeLimitMergeHost) GetTotalSize() uint64 {
	return uint64(h.runs[0].Size() + h.runs[1].Size())
}
func (h *sizeLimitMergeHost) GetTotalRowCnt() uint32 { return 8 }
func (h *sizeLimitMergeHost) LoadNextBatch(ctx context.Context, obj uint32, reuse *batch.Batch) (*batch.Batch, *nulls.Nulls, func(), error) {
	bat, _, release, err := h.jsonReadbackMergeHost.LoadNextBatch(ctx, obj, reuse)
	return bat, h.deletes[obj], release, err
}
