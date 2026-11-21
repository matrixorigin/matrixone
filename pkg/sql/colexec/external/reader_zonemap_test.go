// Copyright 2024 Matrix Origin
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

package external

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Filtered projections over a zonemap (RESULT_SCAN) file must map filter
// columns to physical file indexes, not prune-affected output positions.
// Regression: review of the #30571 fix showed `where filtered_col ...`
// pruned blocks by the wrong column's zone map/null count.
func Test_translateZonemapFilterColumnMap(t *testing.T) {
	t.Run("pruned single column maps to its physical index", func(t *testing.T) {
		// Saved schema [a, b]; the query projects only b: output position 0,
		// physical file position 1. A filter on b initially resolved to the
		// pruned schema (0), which must become the physical index (1).
		param := &ExternalParam{
			Attrs: []plan.ExternAttr{
				{ColName: "b", ColIndex: 0, ColFieldIndex: 1},
			},
			Filter: &FilterParam{
				columnMap: map[int]int{0: 0},
			},
		}
		translateZonemapFilterColumnMap(param)
		require.Equal(t, map[int]int{0: 1}, param.Filter.columnMap)
	})

	t.Run("full projection stays identity", func(t *testing.T) {
		param := &ExternalParam{
			Attrs: []plan.ExternAttr{
				{ColName: "a", ColIndex: 0, ColFieldIndex: 0},
				{ColName: "b", ColIndex: 1, ColFieldIndex: 1},
			},
			Filter: &FilterParam{
				columnMap: map[int]int{1: 1},
			},
		}
		translateZonemapFilterColumnMap(param)
		require.Equal(t, map[int]int{1: 1}, param.Filter.columnMap)
	})

	t.Run("nil filter and unmapped keys are untouched", func(t *testing.T) {
		// No Filter param at all.
		translateZonemapFilterColumnMap(&ExternalParam{})
		// No Filter.columnMap at all.
		translateZonemapFilterColumnMap(&ExternalParam{Filter: &FilterParam{}})
		// Keys that do not reference a projected attr are kept as-is.
		param := &ExternalParam{
			Attrs:  []plan.ExternAttr{{ColName: "b", ColIndex: 0, ColFieldIndex: 1}},
			Filter: &FilterParam{columnMap: map[int]int{5: 5}},
		}
		translateZonemapFilterColumnMap(param)
		require.Equal(t, map[int]int{5: 5}, param.Filter.columnMap)
	})
}

func TestZonemapSavedResultProjection(t *testing.T) {
	ctx := t.Context()
	fs, err := fileservice.NewMemoryFS("result-scan-projection", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	path := objectio.BuildObjectName(objectio.NewSegmentid(), 0).String()
	writer, err := ioutil.NewBlockWriter(fs, path)
	require.NoError(t, err)
	stored := strings.Repeat("saved value ", 4)
	pool := mpool.MustNewZero()
	defer mpool.DeleteMPool(pool)
	for _, rows := range []int{5, 2} {
		func() {
			data := batch.NewWithSize(2)
			defer data.Clean(pool)
			data.Vecs[0] = vector.NewVec(types.T_int32.ToType())
			data.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
			for row := range rows {
				require.NoError(t, vector.AppendFixed(data.Vecs[0], int32(row), false, pool))
				require.NoError(t, vector.AppendBytes(data.Vecs[1], []byte(stored), row == 1, pool))
			}
			data.SetRowCount(rows)
			_, err := writer.WriteBatch(data)
			require.NoError(t, err)
		}()
	}
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)

	for _, tc := range []struct {
		name          string
		fields        []int32
		failAppend    bool
		corruptDecode bool
	}{
		{name: "mixed", fields: []int32{-1, 1, 0}},
		{name: "all-missing", fields: []int32{2}},
		{name: "no-columns"},
		{name: "allocation-failure", fields: []int32{0, 1}, failAppend: true},
		{name: "decode-failure", fields: []int32{0, 1}, corruptDecode: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			scanFS := &resultScanReadTracker{FileService: fs, corruptDecode: tc.corruptDecode}
			proc := testutil.NewProcess(t, testutil.WithMPool(mp), testutil.WithFileService(scanFS))
			param := &ExternalParam{
				ExParamConst: ExParamConst{Ctx: ctx, Extern: &tree.ExternParam{
					ExParam: tree.ExParam{FileService: scanFS, Ctx: ctx},
				}},
				ExParam: ExParam{Fileparam: &ExFileparam{Filepath: path}, Filter: &FilterParam{}},
			}
			buf := batch.NewWithSize(len(tc.fields))
			defer buf.Clean(mp)
			for col, field := range tc.fields {
				name, typ := "missing", types.T_int32
				switch field {
				case 0:
					name = "v"
				case 1:
					name, typ = catalog.ExternalFilePath, types.T_varchar
				}
				param.Attrs = append(param.Attrs, plan.ExternAttr{ColName: name, ColIndex: int32(col), ColFieldIndex: field})
				param.Cols = append(param.Cols, &plan.ColDef{Name: name, Typ: plan.Type{Id: int32(typ)}})
				buf.Vecs[col] = vector.NewOffHeapVecWithType(typ.ToType())
			}
			reader := NewZonemapReader(param, proc)
			_, err := reader.Open(param, proc)
			require.NoError(t, err)
			defer reader.Close()
			bytes, objects := mp.OnHeapOutstanding()
			offHeap := mp.CurrNB()
			analyzer := process.NewAnalyzer(0, false, false, "saved-result")
			if tc.failAppend {
				registry, err := mpool.NewAllocationAccountRegistry(1, 8)
				require.NoError(t, err)
				account, err := registry.Open(1)
				require.NoError(t, err)
				defer func() {
					require.Zero(t, account.Seal().Used)
					_, err := registry.Finalize(account)
					require.NoError(t, err)
				}()
				selection, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerExternal, 1, 2)
				require.NoError(t, err)
				buf.Vecs[1].Free(mp)
				buf.Vecs[1], err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_varchar.ToType(), selection)
				require.NoError(t, err)
				defer buf.Clean(mp)
			}
			for block, rows := range []int{5, 2} {
				buf.CleanOnlyData()
				if block == 1 && len(tc.fields) == 0 {
					canceled, cancel := context.WithCancel(ctx)
					cancel()
					// The first block read populated metadata for both blocks.
					require.Len(t, reader.zoneparam.bs, 2)
					require.ErrorIs(t, reader.getBatchFromZonemapFile(canceled, proc, buf), context.Canceled)
					require.Zero(t, buf.RowCount())
					require.Equal(t, block, reader.zoneparam.offset)
				}
				finished, err := reader.ReadBatch(ctx, buf, proc, analyzer)
				if tc.failAppend || tc.corruptDecode {
					if tc.corruptDecode {
						require.ErrorIs(t, err, io.ErrUnexpectedEOF)
					} else {
						require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
						require.Equal(t, rows, buf.Vecs[0].Length())
					}
					require.Len(t, scanFS.columns, 2)
					require.Zero(t, buf.RowCount())
					require.Zero(t, reader.zoneparam.offset)
					break
				}
				require.NoError(t, err)
				require.Equal(t, block == 1, finished)
				require.Equal(t, rows, buf.RowCount())
				for col, field := range tc.fields {
					vec := buf.Vecs[col]
					require.Equal(t, rows, vec.Length())
					for row := range rows {
						switch field {
						case 0:
							require.Equal(t, int32(row), vector.GetFixedAtWithTypeCheck[int32](vec, row))
						case 1:
							require.Equal(t, row == 1, vec.GetNulls().Contains(uint64(row)))
							if row != 1 {
								require.Equal(t, stored, string(vec.GetBytesAt(row)))
							}
						default:
							require.True(t, vec.GetNulls().Contains(uint64(row)))
						}
					}
				}
			}
			if tc.name == "all-missing" || tc.name == "no-columns" {
				require.Empty(t, scanFS.columns)
			}
			for _, column := range scanFS.columns {
				require.Equal(t, 1, column.releases)
			}
			buf.Clean(mp)
			gotBytes, gotObjects := mp.OnHeapOutstanding()
			require.Equal(t, bytes, gotBytes)
			require.Equal(t, objects, gotObjects)
			require.Equal(t, offHeap, mp.CurrNB())
		})
	}
}

type resultScanReadTracker struct {
	fileservice.FileService
	columns       []*resultScanTrackedColumn
	corruptDecode bool
}

func (fs *resultScanReadTracker) Read(ctx context.Context, vec *fileservice.IOVector) error {
	if err := fs.FileService.Read(ctx, vec); err != nil {
		return err
	}
	for i := range vec.Entries {
		entry := &vec.Entries[i]
		if entry.ValidateCacheData == nil {
			continue
		}
		data := &resultScanTrackedColumn{
			Data:      entry.CachedData,
			truncated: fs.corruptDecode && i == 1,
		}
		entry.CachedData = data
		fs.columns = append(fs.columns, data)
	}
	return nil
}

type resultScanTrackedColumn struct {
	fscache.Data
	truncated bool
	releases  int
}

func (c *resultScanTrackedColumn) Bytes() []byte {
	if c.truncated {
		return nil
	}
	return c.Data.Bytes()
}

func (c *resultScanTrackedColumn) Release() {
	c.releases++
	if c.releases == 1 {
		c.Data.Release()
	}
}
