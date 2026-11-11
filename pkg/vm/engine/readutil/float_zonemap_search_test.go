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
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestFloatSortedKeyDoesNotSkipUnorderedBlocks(t *testing.T) {
	t.Run("float32", func(t *testing.T) {
		testFloatSearch(t, types.T_float32, float32(math.NaN()), float32(math.Copysign(0, -1)))
	})
	t.Run("float64", func(t *testing.T) { testFloatSearch(t, types.T_float64, math.NaN(), math.Copysign(0, -1)) })
}

func testFloatSearch[T ~float32 | ~float64](t *testing.T, oid types.T, nan, negativeZero T) {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	bound := func(value T) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(oid)}, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: types.EncodeValue(value, oid)}}}
	}
	col := MakeColExprForTest(0, oid, "v")
	table := &plan.TableDef{Pkey: &plan.PrimaryKeyDef{PkeyColName: "v"}, Name2ColIndex: map[string]int32{"v": 0},
		Cols: []*plan.ColDef{{Name: "v", Primary: true, Typ: plan.Type{Id: int32(oid)}}}}
	for _, expr := range []*plan.Expr{MakeFunctionExprForTest("=", []*plan.Expr{col, bound(0)}), MakeInExprForTest(col, []T{0}, oid, mp)} {
		base, err := ConstructBasePKFilter(expr, table, mp)
		require.NoError(t, err)
		require.False(t, base.Valid)
		fast, _, _, block, seek, can, _ := CompileFilterExpr(expr, table, nil)
		require.True(t, can)
		require.Nil(t, seek)
		meta := objectio.BuildMetaData(1, 1)
		meta.GetBlockMeta(0).MustGetColumn(0).SetZoneMap(index.BuildZM(oid, types.EncodeValue(negativeZero, oid)))
		stop, keep, err := block(0, meta.GetBlockMeta(0), nil)
		require.NoError(t, err)
		require.False(t, stop)
		require.True(t, keep, "SQL-equal signed zero cannot be rejected by raw-byte Bloom filters")
		meta.GetBlockMeta(0).MustGetColumn(0).SetZoneMap(index.NewZM(oid, 0))
		stop, keep, err = block(0, meta.GetBlockMeta(0), nil)
		require.NoError(t, err)
		require.False(t, stop)
		require.True(t, keep)
		var stats objectio.ObjectStats
		require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(&stats, index.NewZM(oid, 0)))
		keep, err = fast(&stats)
		require.NoError(t, err)
		require.True(t, keep)
	}
	// NaNs disrupt the physical sort, even when the current writer omits NaNs
	// from otherwise valid per-block summaries. Exercise both stored formats.
	table.Cols[0].Primary = false
	table.Cols[0].ClusterBy = true
	for _, legacy := range []bool{false, true} {
		meta := objectio.BuildMetaData(4, 1)
		for i, values := range [][2]T{{4, 4}, {5, 100}, {2, 2}, {1, 3}} {
			if legacy && (i == 0 || i == 2) {
				values = [2]T{nan, nan}
			}
			zm := index.BuildZM(oid, types.EncodeValue(values[0], oid))
			copy(zm.GetMaxBuf(), types.EncodeValue(values[1], oid))
			meta.GetBlockMeta(uint32(i)).MustGetColumn(0).SetZoneMap(zm)
		}
		for _, q := range []struct {
			op     string
			lo, hi T
			match  int
		}{
			{"=", 100, 0, 1}, {">", 99, 0, 1}, {">=", 100, 0, 1}, {"<", 1.5, 0, 3}, {"<=", 1.5, 0, 3},
			{"between", 99, 101, 1}, {"in_range", 99, 101, 1}, {"in", 100, 0, 1},
		} {
			t.Run(fmt.Sprintf("legacy=%v/%s", legacy, q.op), func(t *testing.T) {
				args := []*plan.Expr{col, bound(q.lo)}
				if q.op == "between" || q.op == "in_range" {
					args = append(args, bound(q.hi))
				}
				if q.op == "in_range" {
					args = append(args, &plan.Expr{Typ: plan.Type{Id: int32(types.T_uint8)}, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: []byte{0}}}})
				}
				expr := MakeFunctionExprForTest(q.op, args)
				if q.op == "in" {
					expr = MakeInExprForTest(col, []T{q.lo}, oid, mp)
				}
				_, _, _, block, seek, can, _ := CompileFilterExpr(expr, table, nil)
				require.True(t, can)
				require.Nil(t, seek)
				for i := 0; i <= q.match; i++ {
					stop, keep, err := block(i, meta.GetBlockMeta(uint32(i)), nil)
					require.NoError(t, err)
					require.False(t, stop, "an unordered earlier block cannot justify early termination")
					if i == q.match {
						require.True(t, keep)
					}
				}
			})
		}
	}
}
