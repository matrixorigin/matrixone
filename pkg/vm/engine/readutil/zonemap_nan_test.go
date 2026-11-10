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

func testLegacyNaNZoneMap[T ~float32 | ~float64](t *testing.T, oid types.T, nan T) {
	t.Helper()
	ctx := context.Background()
	fs, err := fileservice.NewLocalFS(ctx, defines.SharedFileServiceName, t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	mp := mpool.MustNewZero()
	t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
	// 实际持久化 [NaN,7]，验证当前分支 writer 产生的摘要和读取消费者。
	// 另注入历史双/单端 NaN 字节，不依赖未来 writer 仍产生旧摘要。
	writer := ioutil.ConstructWriter(0, []uint16{0}, 0, false, false, fs)
	func() {
		input := batch.NewWithSize(1)
		defer input.Clean(mp)
		input.Vecs[0] = vector.NewVec(oid.ToType())
		require.NoError(t, vector.AppendFixedList(input.Vecs[0], []T{nan, 7}, nil, mp))
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
	writerZM := original.MustDataMeta().MustGetColumn(0).ZoneMap()
	require.True(t, writerZM.IsInited())
	t.Logf("writer 摘要：min=%v max=%v，数据包含普通值 7", writerZM.GetMin(), writerZM.GetMax())
	for _, shape := range []struct {
		name       string
		lo, hi     T
		unknown    bool
		fromWriter bool
	}{
		{name: "writer_summary", fromWriter: true},
		{name: "legacy_both_nan", lo: nan, hi: nan, unknown: true},
		{name: "legacy_min_nan", lo: nan, hi: 7, unknown: true},
		{name: "legacy_max_nan", lo: 7, hi: nan, unknown: true},
		{name: "ordinary_control", lo: 7, hi: 7},
	} {
		for _, sorted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/sorted=%v", shape.name, sorted), func(t *testing.T) {
				meta := append(objectio.ObjectMeta(nil), original...)
				data := meta.MustDataMeta()
				if !shape.fromWriter {
					zm := index.BuildZM(oid, types.EncodeValue(T(7), oid))
					copy(zm.GetMinBuf(), types.EncodeValue(shape.lo, oid))
					copy(zm.GetMaxBuf(), types.EncodeValue(shape.hi, oid))
					require.True(t, zm.IsInited())
					data.MustGetColumn(0).SetZoneMap(zm)
					data.GetBlockMeta(0).MustGetColumn(0).SetZoneMap(zm)
				}
				var objectStats objectio.ObjectStats
				require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(&objectStats, data.MustGetColumn(0).ZoneMap()))
				table := &plan.TableDef{Name2ColIndex: map[string]int32{"v": 0}, Cols: []*plan.ColDef{
					{Name: "v", Seqnum: 0, ClusterBy: sorted, Typ: plan.Type{Id: int32(oid)}},
				}}
				matchCases := []bool{false, true}
				if shape.fromWriter {
					// 唯一不依赖 writer 实现的 oracle：包含 7 的块不能排除命中 7 的查询。
					matchCases = []bool{true}
				}
				for _, op := range []string{"=", "<", "<=", ">", ">=", "between", "in_range"} {
					for _, matches := range matchCases {
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
