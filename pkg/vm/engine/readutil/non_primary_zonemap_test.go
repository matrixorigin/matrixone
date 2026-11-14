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
	"fmt"
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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

// 同一列在常量左右两侧时，块/对象比较与有序 seek 都必须保留原 SQL 语义。
func TestCompileFilterExprComparisonOrientation(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	pkTable := &plan.TableDef{
		Name2ColIndex: map[string]int32{"k": 0},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"k"}, PkeyColName: "k"},
		Cols:          []*plan.ColDef{{Name: "k", Seqnum: 0, Primary: true, Typ: plan.Type{Id: int32(types.T_int32)}}},
	}
	pkOperators := map[string][2]int{
		"<":  {function.LESS_THAN, function.GREAT_THAN},
		"<=": {function.LESS_EQUAL, function.GREAT_EQUAL},
		">":  {function.GREAT_THAN, function.LESS_THAN},
		">=": {function.GREAT_EQUAL, function.LESS_EQUAL},
		"=":  {function.EQUAL, function.EQUAL},
	}
	for _, sorted := range []bool{false, true} {
		name := "ordinary"
		if sorted {
			name = "sorted"
		}
		t.Run(name, func(t *testing.T) {
			tableDef := &plan.TableDef{
				Name2ColIndex: map[string]int32{"k": 0},
				Cols:          []*plan.ColDef{{Name: "k", Seqnum: 0, ClusterBy: sorted, Typ: plan.Type{Id: int32(types.T_int32)}}},
			}
			// 每块只有一个边界值，不经过文件服务或真实进程。
			meta := objectio.BuildMetaData(3, 1)
			for i, value := range []int32{99, 100, 101} {
				zm := index.NewZM(types.T_int32, 0)
				index.UpdateZM(zm, types.EncodeInt32(&value))
				meta.GetBlockMeta(uint32(i)).MustGetColumn(0).SetZoneMap(zm)
			}
			for _, tc := range []struct {
				op          string
				left, right [3]bool
			}{
				{"<", [3]bool{true, false, false}, [3]bool{false, false, true}},
				{"<=", [3]bool{true, true, false}, [3]bool{false, true, true}},
				{">", [3]bool{false, false, true}, [3]bool{true, false, false}},
				{">=", [3]bool{false, true, true}, [3]bool{true, true, false}},
				{"=", [3]bool{false, true, false}, [3]bool{false, true, false}},
			} {
				for _, columnLeft := range []bool{true, false} {
					label, want := "column_left", tc.left
					if !columnLeft {
						label, want = "column_right", tc.right
					}
					t.Run(tc.op+"/"+label, func(t *testing.T) {
						bound := int32(100)
						value := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Fold{
							Fold: &plan.FoldVal{IsConst: true, Data: types.EncodeInt32(&bound)},
						}}
						args := []*plan.Expr{MakeColExprForTest(0, types.T_int32, "k"), value}
						if !columnLeft {
							args[0], args[1] = args[1], args[0]
						}
						expr := MakeFunctionExprForTest(tc.op, args)
						before, err := expr.Marshal()
						require.NoError(t, err)
						for repeat := 0; repeat < 2; repeat++ {
							pk, err := ConstructBasePKFilter(expr, pkTable, mp)
							require.NoError(t, err)
							t.Cleanup(pk.Cleanup)
							require.True(t, pk.Valid)
							wantOp := pkOperators[tc.op][0]
							if !columnLeft {
								wantOp = pkOperators[tc.op][1]
							}
							require.Equal(t, wantOp, pk.Op, "主键读取与块过滤必须使用相同的比较方向")
							require.Equal(t, int32(100), types.DecodeInt32(pk.LB))
							fast, _, _, block, seek, can, _ := CompileFilterExpr(expr, tableDef, nil)
							require.True(t, can)
							after, err := expr.Marshal()
							require.NoError(t, err)
							require.Equal(t, before, after, "函数 ID、操作数及参数不能被过滤编译修改")
							for i := range want {
								blk := meta.GetBlockMeta(uint32(i))
								stop, keep, err := block(i, blk, nil)
								require.NoError(t, err)
								require.Equal(t, want[i], keep, "block %d", i)
								if stop {
									require.True(t, sorted)
									for _, remaining := range want[i:] {
										require.False(t, remaining, "不能提前终止而漏掉匹配块")
									}
								}
								if fast != nil {
									var stats objectio.ObjectStats
									require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(&stats, blk.MustGetColumn(0).ZoneMap()))
									keep, err := fast(&stats)
									require.NoError(t, err)
									require.Equal(t, want[i], keep, "object %d", i)
								}
							}
							unknown := objectio.BuildMetaData(1, 1)
							stop, keep, err := block(0, unknown.GetBlockMeta(0), nil)
							require.NoError(t, err)
							require.False(t, stop)
							require.True(t, keep, "缺失元数据不是不匹配的证据")
							if seek != nil {
								pos := seek(meta)
								for _, skipped := range want[:pos] {
									require.False(t, skipped, "seek 不能跳过匹配块")
								}
								require.Zero(t, seek(unknown), "未知统计不能用于跳过块")
							}
						}
						// NULL 在执行阶段折叠为 nil，必须退化而非复用此前的边界。
						value.GetFold().Data = nil
						_, _, _, _, _, can, _ := CompileFilterExpr(expr, tableDef, nil)
						require.False(t, can)
					})
				}
			}
		})
	}
}

func TestCompileFilterExprLegacyBoolZoneMap(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewLocalFS2(ctx, defines.SharedFileServiceName, t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	// 复用真实对象布局，只注入摘要，避免手工构造不可能到达的元数据。
	writer := ioutil.ConstructWriter(0, []uint16{0}, 0, false, false, fs)
	func() {
		input := batch.NewWithSize(1)
		defer input.Clean(mp)
		input.Vecs[0] = vector.NewVec(types.T_bool.ToType())
		require.NoError(t, vector.AppendFixedList(input.Vecs[0], []bool{false, true}, nil, mp))
		input.SetRowCount(2)
		_, err := writer.WriteBatch(input)
		require.NoError(t, err)
	}()
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	location := stats.ObjectLocation()
	original, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
	require.NoError(t, err)

	for _, sorted := range []bool{false, true} {
		for _, storedMax := range []bool{false, true} {
			for _, bound := range []bool{false, true} {
				t.Run(fmt.Sprintf("sorted=%v/stored=%v/bound=%v", sorted, storedMax, bound), func(t *testing.T) {
					meta := append(objectio.ObjectMeta(nil), original...)
					data := meta.MustDataMeta()
					// false/false 可能来自旧的混合块；true/true 则可精确排除 false。
					zm := index.BuildZM(types.T_bool, types.EncodeBool(&storedMax))
					data.MustGetColumn(0).SetZoneMap(zm)
					data.GetBlockMeta(0).MustGetColumn(0).SetZoneMap(zm)
					var objectStats objectio.ObjectStats
					require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(&objectStats, zm))
					table := &plan.TableDef{
						Name2ColIndex: map[string]int32{"v": 0},
						Cols:          []*plan.ColDef{{Name: "v", Seqnum: 0, ClusterBy: sorted, Typ: plan.Type{Id: int32(types.T_bool)}}},
					}
					expr := MakeFunctionExprForTest("=", []*plan.Expr{
						MakeColExprForTest(0, types.T_bool, "v"),
						{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Fold{
							Fold: &plan.FoldVal{IsConst: true, Data: types.EncodeBool(&bound)},
						}},
					})
					fast, _, object, block, seek, can, _ := CompileFilterExpr(expr, table, nil)
					require.True(t, can)
					want := !storedMax || bound
					keep, err := object(meta, nil)
					require.NoError(t, err)
					require.Equal(t, sorted || want, keep, "对象摘要")
					stop, keep, err := block(0, data.GetBlockMeta(0), nil)
					require.NoError(t, err)
					require.Equal(t, want, keep, "块摘要")
					if want {
						require.False(t, stop)
					}
					if fast != nil {
						keep, err := fast(&objectStats)
						require.NoError(t, err)
						require.Equal(t, want, keep, "排序键摘要")
					}
					if seek != nil && want {
						require.Zero(t, seek(data), "不能跳过可能含 true 的旧块")
					}
				})
			}
		}
	}
}

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
		bounds := []int32{lower}
		if op == "between" || op == "in_range" {
			bounds = append(bounds, upper)
		}
		for _, bound := range bounds {
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
		return MakeFunctionExprForTest(op, args)
	}

	for _, layout := range []struct {
		name        string
		blocks      [][2]int32
		want        []uint16
		comparisons [5][]uint16 // =、<、<=、>、>= 100 的块集合，允许范围重叠产生假阳性。
	}{
		{name: "clustered", blocks: [][2]int32{{0, 1}, {100, 150}, {200, 201}}, want: []uint16{1},
			comparisons: [5][]uint16{{1}, {0}, {0, 1}, {1, 2}, {1, 2}}},
		{name: "interleaved", blocks: [][2]int32{{0, 100}, {1, 200}, {150, 201}}, want: []uint16{0, 1, 2},
			comparisons: [5][]uint16{{0, 1}, {0, 1}, {0, 1}, {1, 2}, {0, 1, 2}}},
		{name: "matching_block_after_high_block", blocks: [][2]int32{{200, 201}, {0, 1}, {100, 150}}, want: []uint16{2},
			comparisons: [5][]uint16{{2}, {1}, {1, 2}, {0, 2}, {0, 2}}},
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
				{name: "equal", op: "=", column: 1, lower: 100, want: layout.comparisons[0]},
				{name: "less", op: "<", column: 1, lower: 100, want: layout.comparisons[1]},
				{name: "less_equal", op: "<=", column: 1, lower: 100, want: layout.comparisons[2]},
				{name: "greater", op: ">", column: 1, lower: 100, want: layout.comparisons[3]},
				{name: "greater_equal", op: ">=", column: 1, lower: 100, want: layout.comparisons[4]},
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
						unknown := append(objectio.ObjectMeta(nil), meta...)
						unknown.MustDataMeta().MustGetColumn(1).SetZoneMap(index.NewZM(types.T_int32, 0))
						keep, err := object(unknown, nil)
						require.NoError(t, err)
						require.True(t, keep, "缺失对象统计不能排除对象")
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
		for _, sorted := range []bool{false, true} {
			table, col := *tableDef, *tableDef.Cols[1]
			col.ClusterBy = sorted
			table.Cols = append([]*plan.ColDef(nil), tableDef.Cols...)
			table.Cols[1] = &col
			for _, op := range []string{"between", "in_range"} {
				hints := []uint8{0}
				if op == "in_range" {
					hints = []uint8{0, 1, 2, 3}
				}
				for _, hint := range hints {
					t.Run(fmt.Sprintf("sorted=%v/%s/hint=%d", sorted, op, hint), func(t *testing.T) {
						expr := filterExpr(1, op, 100, 150)
						if op == "in_range" {
							expr.GetF().Args[3].GetFold().Data[0] = hint
						}
						fast, _, _, block, seek, ok, _ := CompileFilterExpr(expr, &table, nil)
						require.True(t, ok)
						meta := objectio.BuildMetaData(1, 2)
						meta.GetBlockMeta(0).MustGetColumn(1).SetZoneMap(index.NewZM(types.T_int32, 0))
						stop, keep, err := block(0, meta.GetBlockMeta(0), nil)
						require.NoError(t, err)
						require.False(t, stop)
						require.True(t, keep, "未知统计不能证明块不含匹配行")
						if seek != nil {
							require.Zero(t, seek(meta))
						}
						if fast != nil {
							keep, err := fast(&objectio.ObjectStats{})
							require.NoError(t, err)
							require.True(t, keep)
						}
					})
				}
			}
		}
	})
}
