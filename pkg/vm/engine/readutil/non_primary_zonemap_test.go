// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package readutil

import (
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestFilterObjectsNonPrimaryZoneMaps(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewLocalFS2(ctx, defines.SharedFileServiceName, t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	tableDef := &plan.TableDef{
		Name:          "non_primary_zonemap",
		Name2ColIndex: map[string]int32{"id": 0, "k": 1},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
		Cols: []*plan.ColDef{
			{Name: "id", Seqnum: 0, Primary: true, Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "k", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_int32)}},
		},
	}
	filterExpr := func(column int32, op string, lower, upper int32) *plan.Expr {
		args := []*plan.Expr{MakeColExprForTest(column, types.T_int32, tableDef.Cols[column].Name)}
		for _, bound := range []int32{lower, upper} {
			args = append(args, &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_int32)},
				Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: types.EncodeInt32(&bound)}},
			})
		}
		if op == "in_range" {
			args = append(args, &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_uint8)},
				Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: []byte{0}}},
			})
		}
		return foldedFunction(op, args)
	}

	for _, layout := range []struct {
		name   string
		blocks [][2]int32
		want   []uint16
	}{
		{name: "clustered", blocks: [][2]int32{{0, 1}, {100, 150}, {200, 201}}, want: []uint16{1}},
		{name: "interleaved", blocks: [][2]int32{{0, 100}, {1, 200}, {150, 201}}, want: []uint16{0, 1, 2}},
		{name: "matching_block_after_high_block", blocks: [][2]int32{{200, 201}, {0, 1}, {100, 150}}, want: []uint16{2}},
	} {
		t.Run(layout.name, func(t *testing.T) {
			writer := ioutil.ConstructWriter(0, []uint16{0, 1}, 0, true, false, fs)
			for block, values := range layout.blocks {
				func() {
					input := batch.NewWithSize(2)
					defer input.Clean(mp)
					input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
					input.Vecs[1] = vector.NewVec(types.T_int32.ToType())
					// 块 location 的行数计算要求非末块填满；每块仅重复两个值。
					for row := 0; row < objectio.BlockMaxRows; row++ {
						require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(block*objectio.BlockMaxRows+row+1), false, mp))
						require.NoError(t, vector.AppendFixed(input.Vecs[1], values[row%2], false, mp))
					}
					input.SetRowCount(objectio.BlockMaxRows)
					_, writeErr := writer.WriteBatch(input)
					require.NoError(t, writeErr)
				}()
			}
			_, _, err := writer.Sync(ctx)
			require.NoError(t, err)
			stats := writer.GetObjectStats(objectio.WithSorted())
			require.Equal(t, uint32(3), stats.BlkCnt())
			require.Equal(t, uint32(3*objectio.BlockMaxRows), stats.Rows())
			location := stats.ObjectLocation()
			meta, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
			require.NoError(t, err)
			dataMeta := meta.MustDataMeta()
			objectZM := dataMeta.MustGetColumn(1).ZoneMap()
			require.True(t, objectZM.IsInited())
			require.Equal(t, int32(0), objectZM.GetMin())
			require.Equal(t, int32(201), objectZM.GetMax())
			for block, values := range layout.blocks {
				zm := dataMeta.GetBlockMeta(uint32(block)).MustGetColumn(1).ZoneMap()
				require.True(t, zm.IsInited())
				require.Equal(t, values[0], zm.GetMin())
				require.Equal(t, values[1], zm.GetMax())
			}

			var unfiltered objectio.BlockInfoSlice
			_, loads, _, _, _, _, _, _, err := FilterObjects(
				ctx, engine.RangesParam{}, nil, nil, nil, nil, nil,
				nil, nil, []objectio.ObjectStats{stats}, &unfiltered, false, nil, fs,
			)
			require.NoError(t, err)
			require.Zero(t, loads)
			require.Equal(t, 3, unfiltered.Len())

			for _, query := range []struct {
				name   string
				op     string
				column int32
				lower  int32
				upper  int32
				want   []uint16
			}{
				{name: "between", op: "between", column: 1, lower: 100, upper: 150, want: layout.want},
				{name: "in_range", op: "in_range", column: 1, lower: 100, upper: 150, want: layout.want},
				{name: "outside_object", op: "between", column: 1, lower: 300, upper: 400},
				{name: "primary_key_control", op: "between", lower: objectio.BlockMaxRows + 1, upper: 2 * objectio.BlockMaxRows, want: []uint16{1}},
			} {
				t.Run(query.name, func(t *testing.T) {
					fast, load, object, block, seek, ok, hint := CompileFilterExprs(
						[]*plan.Expr{filterExpr(query.column, query.op, query.lower, query.upper)}, tableDef, fs,
					)
					require.True(t, ok)
					if query.column == 1 {
						require.Nil(t, fast, "普通列不能使用主键摘要")
						require.Nil(t, seek, "普通列不能假定块有序")
					}
					var selected objectio.BlockInfoSlice
					total, loads, objects, objectHits, blocks, blockHits, _, _, err := FilterObjects(
						ctx, engine.RangesParam{}, fast, load, object, block, seek,
						nil, nil, []objectio.ObjectStats{stats}, &selected, hint, nil, fs,
					)
					require.NoError(t, err)
					require.Equal(t, 3, total)
					require.Equal(t, 1, loads, "每个对象只加载一次元数据，不随块数重复加载")
					wantIDs := make([]types.Blockid, 0, len(query.want))
					gotIDs := make([]types.Blockid, 0, selected.Len())
					for _, id := range query.want {
						wantIDs = append(wantIDs, stats.ConstructBlockId(id))
					}
					for i := 0; i < selected.Len(); i++ {
						gotIDs = append(gotIDs, selected.Get(i).BlockID)
					}
					require.Equal(t, wantIDs, gotIDs)
					if query.column == 1 {
						require.Equal(t, 1, objects)
						if query.want == nil {
							require.Equal(t, 1, objectHits)
							require.Zero(t, blocks, "对象裁剪应先于逐块检查")
						} else {
							require.Zero(t, objectHits)
							require.Equal(t, 3, blocks, "普通列必须检查后续块")
							require.Equal(t, 3-len(query.want), blockHits)
						}
					}
				})
			}
		})
	}

	t.Run("unknown_block_statistics", func(t *testing.T) {
		meta := objectio.BuildMetaData(1, 2).GetBlockMeta(0)
		for _, op := range []string{"between", "in_range"} {
			t.Run(op, func(t *testing.T) {
				_, _, _, block, _, ok, _ := CompileFilterExpr(filterExpr(1, op, 100, 150), tableDef, nil)
				require.True(t, ok)
				meta.MustGetColumn(1).SetZoneMap(index.NewZM(types.T_int32, 0))
				stop, keep, err := block(0, meta, nil)
				require.NoError(t, err)
				require.False(t, stop)
				require.True(t, keep, "未知统计不能证明块不含匹配行")
			})
		}
	})
}
