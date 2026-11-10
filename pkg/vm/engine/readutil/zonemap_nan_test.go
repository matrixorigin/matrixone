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

package readutil

import (
	"context"
	"fmt"
	"math"
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestCompileFilterExprLegacyNaNZoneMap(t *testing.T) {
	t.Run("float32", func(t *testing.T) { testLegacyNaNZoneMap(t, types.T_float32, float32(math.NaN())) })
	t.Run("float64", func(t *testing.T) { testLegacyNaNZoneMap(t, types.T_float64, math.NaN()) })
}

// Storage's raw floating-point sort does not order rows across NaNs. Even
// valid per-block summaries therefore need not be ordered between blocks.
func TestFloatSortedKeyDoesNotSkipUnorderedBlocks(t *testing.T) {
	t.Run("float32", func(t *testing.T) { testFloatSortedKeyUnordered(t, types.T_float32, float32(math.NaN())) })
	t.Run("float64", func(t *testing.T) { testFloatSortedKeyUnordered(t, types.T_float64, math.NaN()) })
}

func TestFloatPrimaryKeyFiltersFailOpenSafely(t *testing.T) {
	t.Run("float32", func(t *testing.T) { testFloatPrimaryKeyFilters(t, types.T_float32, float32(math.Copysign(0, -1))) })
	t.Run("float64", func(t *testing.T) { testFloatPrimaryKeyFilters(t, types.T_float64, math.Copysign(0, -1)) })
}

func testFloatPrimaryKeyFilters[T ~float32 | ~float64](t *testing.T, oid types.T, negativeZero T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	col := MakeColExprForTest(0, oid, "v")
	table := &plan.TableDef{
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "v"},
		Name2ColIndex: map[string]int32{"v": 0},
		Cols:          []*plan.ColDef{{Name: "v", Seqnum: 0, Primary: true, Typ: plan.Type{Id: int32(oid)}}},
	}
	zero := T(0)
	bound := &plan.Expr{Typ: plan.Type{Id: int32(oid)}, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: types.EncodeValue(zero, oid)}}}
	equal := MakeFunctionExprForTest("=", []*plan.Expr{col, bound})
	for _, expr := range []*plan.Expr{equal, MakeInExprForTest(col, []T{zero}, oid, mp)} {
		base, err := ConstructBasePKFilter(expr, table, mp)
		require.NoError(t, err)
		require.False(t, base.Valid, "float PK row filters must leave the residual predicate in control")
		base.Cleanup()

		fast, _, _, block, seek, can, _ := CompileFilterExpr(expr, table, nil)
		require.True(t, can)
		require.Nil(t, seek)
		meta := objectio.BuildMetaData(1, 1)
		meta.GetBlockMeta(0).MustGetColumn(0).SetZoneMap(index.BuildZM(oid, types.EncodeValue(negativeZero, oid)))
		stop, keep, err := block(0, meta.GetBlockMeta(0), nil)
		require.NoError(t, err)
		require.False(t, stop)
		require.True(t, keep, "SQL-equal signed zero must not be rejected by a raw-byte Bloom filter")

		meta.GetBlockMeta(0).MustGetColumn(0).SetZoneMap(index.NewZM(oid, 0))
		stop, keep, err = block(0, meta.GetBlockMeta(0), nil)
		require.NoError(t, err)
		require.False(t, stop)
		require.True(t, keep, "uninitialized block summaries cannot reject the block")
		var stats objectio.ObjectStats
		require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(&stats, index.NewZM(oid, 0)))
		keep, err = fast(&stats)
		require.NoError(t, err)
		require.True(t, keep, "a typed but uninitialized object summary is unknown")
	}
}

func testFloatSortedKeyUnordered[T ~float32 | ~float64](t *testing.T, oid types.T, nan T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			meta := objectio.BuildMetaData(4, 1)
			bounds := [][2]T{{4, 4}, {5, 100}, {2, 2}, {1, 3}}
			for i, bound := range bounds {
				if legacy && (i == 0 || i == 2) {
					bound = [2]T{nan, nan}
				}
				zm := index.BuildZM(oid, types.EncodeValue(bound[0], oid))
				copy(zm.GetMaxBuf(), types.EncodeValue(bound[1], oid))
				meta.GetBlockMeta(uint32(i)).MustGetColumn(0).SetZoneMap(zm)
			}
			for i, bound := range bounds {
				if legacy && (i == 0 || i == 2) {
					continue
				}
				zm := meta.GetBlockMeta(uint32(i)).MustGetColumn(0).ZoneMap()
				require.Equal(t, types.EncodeValue(bound[0], oid), zm.GetMinBuf())
				require.Equal(t, types.EncodeValue(bound[1], oid), zm.GetMaxBuf())
			}
			table := &plan.TableDef{Name2ColIndex: map[string]int32{"v": 0}, Cols: []*plan.ColDef{
				{Name: "v", Seqnum: 0, ClusterBy: true, Typ: plan.Type{Id: int32(oid)}},
			}}
			for _, q := range []struct {
				op     string
				lo, hi T
				match  int
			}{
				{"=", 100, 0, 1}, {">", 99, 0, 1}, {">=", 100, 0, 1},
				{"<", 1.5, 0, 3}, {"<=", 1.5, 0, 3},
				{"between", 99, 101, 1}, {"in_range", 99, 101, 1}, {"in", 100, 0, 1},
			} {
				t.Run(q.op, func(t *testing.T) {
					bound := func(v T) *plan.Expr {
						return &plan.Expr{Typ: plan.Type{Id: int32(oid)}, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: types.EncodeValue(v, oid)}}}
					}
					args := []*plan.Expr{MakeColExprForTest(0, oid, "v"), bound(q.lo)}
					if q.op == "between" || q.op == "in_range" {
						args = append(args, bound(q.hi))
					}
					if q.op == "in_range" {
						args = append(args, &plan.Expr{Typ: plan.Type{Id: int32(types.T_uint8)}, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: []byte{0}}}})
					}
					var expr *plan.Expr
					if q.op == "in" {
						expr = MakeInExprForTest(args[0], []T{q.lo}, oid, mp)
					} else {
						expr = MakeFunctionExprForTest(q.op, args)
					}
					_, _, _, block, seek, can, _ := CompileFilterExpr(expr, table, nil)
					require.True(t, can)
					start := 0
					if seek != nil {
						start = seek(meta)
					}
					found := false
					for i := start; i < 4; i++ {
						stop, keep, err := block(i, meta.GetBlockMeta(uint32(i)), nil)
						require.NoError(t, err)
						if stop {
							break
						}
						if i == q.match && keep {
							found = true
						}
					}
					require.True(t, found, "seek or early stop skipped a matching block")
				})
			}
		})
	}
}

func testLegacyNaNZoneMap[T ~float32 | ~float64](t *testing.T, oid types.T, nan T) {
	t.Helper()
	ctx := context.Background()
	fs, err := fileservice.NewLocalFS2(ctx, defines.SharedFileServiceName, t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	mp := mpool.MustNewZero()
	t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
	// 仅用一行建立真实对象元数据布局，再注入历史摘要字节。
	// 当前 writer 已忽略 NaN，不能把新 writer 生成的统计冒充旧格式反例。
	writer := ioutil.ConstructWriter(0, []uint16{0}, 0, false, false, fs)
	func() {
		input := batch.NewWithSize(1)
		defer input.Clean(mp)
		input.Vecs[0] = vector.NewVec(oid.ToType())
		require.NoError(t, vector.AppendFixed(input.Vecs[0], T(7), false, mp))
		input.SetRowCount(1)
		_, err := writer.WriteBatch(input)
		require.NoError(t, err)
	}()
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	location := stats.ObjectLocation()
	original, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
	require.NoError(t, err)
	for _, shape := range []struct {
		name    string
		lo, hi  T
		unknown bool
	}{
		{"legacy_both_nan", nan, nan, true},
		{"legacy_min_nan", nan, 7, true},
		{"legacy_max_nan", 7, nan, true},
		{"ordinary_control", 7, 7, false},
	} {
		for _, sorted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/sorted=%v", shape.name, sorted), func(t *testing.T) {
				meta := append(objectio.ObjectMeta(nil), original...)
				data := meta.MustDataMeta()
				zm := index.BuildZM(oid, types.EncodeValue(T(7), oid))
				copy(zm.GetMinBuf(), types.EncodeValue(shape.lo, oid))
				copy(zm.GetMaxBuf(), types.EncodeValue(shape.hi, oid))
				require.True(t, zm.IsInited())
				data.MustGetColumn(0).SetZoneMap(zm)
				data.GetBlockMeta(0).MustGetColumn(0).SetZoneMap(zm)
				var objectStats objectio.ObjectStats
				require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(&objectStats, zm))
				table := &plan.TableDef{Name2ColIndex: map[string]int32{"v": 0}, Cols: []*plan.ColDef{
					{Name: "v", Seqnum: 0, ClusterBy: sorted, Typ: plan.Type{Id: int32(oid)}},
				}}
				for _, op := range []string{"=", "<", "<=", ">", ">=", "between", "in_range"} {
					for _, matches := range []bool{false, true} {
						hints := []uint8{0}
						if op == "in_range" {
							hints = []uint8{0, 1, 2, 3}
						}
						for _, hint := range hints {
							t.Run(fmt.Sprintf("%s/matches=%v/hint=%d", op, matches, hint), func(t *testing.T) {
								lo, hi := T(8), T(9)
								switch op {
								case "=":
									if matches {
										lo = 7
									}
								case "<", "<=":
									lo = 6
									if matches {
										lo = 8
									}
								case ">", ">=":
									if matches {
										lo = 0
									}
								default:
									if matches {
										lo, hi = 6, 8
									}
								}
								bound := func(v T) *plan.Expr {
									return &plan.Expr{Typ: plan.Type{Id: int32(oid)}, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: types.EncodeValue(v, oid)}}}
								}
								args := []*plan.Expr{MakeColExprForTest(0, oid, "v"), bound(lo)}
								if op == "between" || op == "in_range" {
									args = append(args, bound(hi))
								}
								if op == "in_range" {
									args = append(args, &plan.Expr{Typ: plan.Type{Id: int32(types.T_uint8)}, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: []byte{hint}}}})
								}
								fast, _, object, block, seek, can, _ := CompileFilterExpr(MakeFunctionExprForTest(op, args), table, nil)
								require.True(t, can)
								want := matches || shape.unknown
								keep, err := object(meta, nil)
								require.NoError(t, err)
								require.Equal(t, want || sorted, keep, "对象过滤")
								stop, keep, err := block(0, data.GetBlockMeta(0), nil)
								require.NoError(t, err)
								require.Equal(t, want, keep, "块过滤")
								if want {
									require.False(t, stop, "不能用 NaN 摘要提前停止")
								}
								if fast != nil {
									keep, err := fast(&objectStats)
									require.NoError(t, err)
									require.Equal(t, want, keep, "排序键摘要")
								}
								if seek != nil && want {
									require.Zero(t, seek(data), "不能跳过潜在匹配块")
								}
							})
						}
					}
				}
			})
		}
	}
}
