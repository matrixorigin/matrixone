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

package disttae

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestColumnSummaryZoneMap(t *testing.T) {
	for _, tc := range []struct {
		name     string
		blocks   [][]*bool
		legacy   bool
		min, max any
	}{
		{"false", [][]*bool{{boolPointer(false), boolPointer(false)}}, false, false, false},
		{"false null", [][]*bool{{boolPointer(false), nil}}, false, false, false},
		{"multiblock false", [][]*bool{{boolPointer(false), nil}, {boolPointer(false), nil}}, false, false, false},
		{"mixed", [][]*bool{{boolPointer(false), boolPointer(true)}}, false, false, true},
		{"legacy mixed", [][]*bool{{boolPointer(false), boolPointer(true)}}, true, false, true},
		{"legacy multiblock", [][]*bool{{boolPointer(false), nil}, {boolPointer(true), nil}}, true, false, true},
		{"all true", [][]*bool{{boolPointer(true), boolPointer(true)}}, false, true, true},
		{"all null", [][]*bool{{nil, nil}}, false, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fs, err := fileservice.NewLocalFS(ctx, defines.SharedFileServiceName, t.TempDir(), fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			t.Cleanup(func() { fs.Close(ctx) })
			mp := mpool.MustNewZero()
			t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
			writer := ioutil.ConstructWriter(0, []uint16{0}, 0, false, false, fs)
			for _, rows := range tc.blocks {
				func() {
					b := batch.NewWithSize(1)
					defer b.Clean(mp)
					b.Vecs[0] = vector.NewVec(types.T_bool.ToType())
					for _, v := range rows {
						value := false
						if v != nil {
							value = *v
						}
						require.NoError(t, vector.AppendFixed(b.Vecs[0], value, v == nil, mp))
					}
					b.SetRowCount(len(rows))
					_, err := writer.WriteBatch(b)
					require.NoError(t, err)
				}()
			}
			_, _, err = writer.Sync(ctx)
			require.NoError(t, err)
			stats := writer.GetObjectStats()
			loc := stats.ObjectLocation()
			original, err := objectio.FastLoadObjectMeta(ctx, &loc, false, fs)
			require.NoError(t, err)
			meta := append(objectio.ObjectMeta(nil), original...)
			data := meta.MustDataMeta()
			if tc.legacy {
				data.MustGetColumn(0).SetZoneMap(index.BuildZM(types.T_bool, []byte{0}))
				for i := uint32(0); i < data.BlockCount(); i++ {
					data.GetBlockMeta(i).MustGetColumn(0).SetZoneMap(index.BuildZM(types.T_bool, []byte{0}))
				}
			}
			before := bytes.Clone(meta)
			originalBefore := bytes.Clone(original)
			if !tc.legacy {
				table := newColumnSummaryScanTable(t, ctx, fs, mp, stats)
				infos, err := table.GetColumMetadataScanInfo(ctx, "b", false)
				require.NoError(t, err)
				require.Len(t, infos, 1)
				require.Equal(t, "b", infos[0].ColName)
				require.Equal(t, int64(data.BlockHeader().Rows()), infos[0].RowCnt)
				require.Equal(t, int64(data.MustGetColumn(0).NullCnt()), infos[0].NullCnt)
				require.Equal(t, tc.min, objectio.ZoneMap(infos[0].ZoneMap).GetMin())
				require.Equal(t, tc.max, objectio.ZoneMap(infos[0].ZoneMap).GetMax())
				// 统计快捷路径仍然不返回列边界。
				all, err := table.GetColumMetadataScanInfo(ctx, AllColumns, false)
				require.NoError(t, err)
				require.Len(t, all, 1)
				require.False(t, objectio.ZoneMap(all[0].ZoneMap).IsInited())
				if tc.name == "false" {
					canceled, cancel := context.WithCancel(ctx)
					cancel()
					infos, err := table.GetColumMetadataScanInfo(canceled, "b", false)
					require.Error(t, err)
					require.Nil(t, infos)
				}
			}
			cols := []*plan.ColDef{{Seqnum: 0, Typ: plan.Type{Id: int32(types.T_bool)}}}
			zms := make([]objectio.ZoneMap, 1)
			tableTypes := make([]uint8, 1)
			for repeat := 0; repeat < 2; repeat++ {
				require.NoError(t, updateColumnSummaries(ctx, data, loc, cols, zms, tableTypes, fs, mp))
			}
			exact := zms[0]
			require.Equal(t, uint8(types.T_bool), tableTypes[0])
			require.Equal(t, tc.min, exact.GetMin())
			require.Equal(t, tc.max, exact.GetMax())
			// 返回值将被跨对象合并，绝不能别名到文件服务共享元数据。
			if exact.IsInited() {
				exact.GetMinBuf()[0] ^= 1
			}
			require.Equal(t, before, []byte(meta))
			require.Equal(t, originalBefore, []byte(original))
			if tc.name == "false" {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				require.Error(t, updateColumnSummaries(canceled, data, loc, cols, zms, tableTypes, fs, mp))
			}
		})
	}
}

// 复用真实对象及轻量事务，仅注入已可见对象，避免启动完整集群。
func newColumnSummaryScanTable(t *testing.T, ctx context.Context, fs fileservice.FileService, mp *mpool.MPool,
	stats objectio.ObjectStats,
) *txnTable {
	t.Helper()
	table := newTxnTableForTest(t)
	table.tableId = 10001
	table.remoteWorkspace = true
	table.createdInTxn = true
	table.tableDef = &plan.TableDef{Cols: []*plan.ColDef{{Name: "b", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_bool)}}}}
	eng := table.eng.(*Engine)
	eng.partitions = make(map[[2]uint64]*logtailreplay.Partition)
	proc := process.NewTopProcess(ctx, mp, nil, table.db.op, fs, nil, nil, nil, nil, nil, nil)
	t.Cleanup(proc.Free)
	table.getTxn().proc = proc
	part := eng.GetOrCreateLatestPart(ctx, 0, table.db.databaseId, table.tableId)
	state, done := part.MutateState()
	require.NoError(t, state.HandleObjectEntry(ctx, fs, objectio.ObjectEntry{ObjectStats: stats}, false))
	done()
	return table
}

func boolPointer(v bool) *bool { return &v }

func TestColumnSummaryZoneMapMetadataOnly(t *testing.T) {
	for _, tc := range []struct {
		typ   types.T
		value any
	}{
		{types.T_bool, true}, {types.T_int32, int32(7)},
	} {
		data := objectio.BuildMetaData(1, 1)
		data.MustGetColumn(0).SetZoneMap(index.BuildZM(tc.typ, types.EncodeValue(tc.value, tc.typ)))
		before := bytes.Clone(data)
		// 无文件服务也能完成：精确全 true 和非 BOOL 不应读列数据。
		exact, err := columnSummaryZoneMap(context.Background(), data, nil, 0, nil, nil)
		require.NoError(t, err)
		require.Equal(t, tc.value, exact.GetMax())
		exact.GetMaxBuf()[0] ^= 1
		require.Equal(t, before, []byte(data))
	}
}
