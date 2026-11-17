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

package readutil

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func inSeekTable(primary bool) *plan.TableDef {
	return &plan.TableDef{
		Name: "in_seek", Name2ColIndex: map[string]int32{"k": 0},
		Cols: []*plan.ColDef{{Name: "k", Seqnum: 0, Primary: primary, ClusterBy: !primary,
			Typ: plan.Type{Id: int32(types.T_int32)}}},
	}
}

// 每块一个已知值；unknown=-1 表示摘要完整，不需要磁盘或服务。
func inSeekMeta(count int, unknown int) objectio.ObjectDataMeta {
	meta := objectio.BuildMetaData(uint16(count), 1)
	for i := range count {
		zm := index.NewZM(types.T_int32, 0)
		if i != unknown {
			value := int32(i * 10)
			index.UpdateZM(zm, types.EncodeInt32(&value))
		}
		meta.GetBlockMeta(uint32(i)).MustGetColumn(0).SetZoneMap(zm)
	}
	return meta
}

func TestInSeekRetainsUnknownBlocks(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	for _, primary := range []bool{false, true} {
		for _, tc := range []struct {
			name           string
			count, unknown int
			value          int32
			want           int
		}{
			{"empty", 0, -1, 10, 0},
			{"single_unknown", 1, 0, 10, 0},
			{"known_first_match", 4, -1, 0, 0},
			{"known_late_match", 4, -1, 30, 3},
			{"known_no_match", 4, -1, 40, 4},
			{"unknown_first_before_match", 4, 0, 30, 0},
			{"unknown_middle_before_match", 4, 1, 30, 1},
			{"unknown_middle_before_low_blocks", 4, 1, 40, 1},
			{"unknown_last", 4, 3, 40, 3},
		} {
			t.Run(fmt.Sprintf("primary=%v/%s", primary, tc.name), func(t *testing.T) {
				expr := MakeInExprForTest(MakeColExprForTest(0, types.T_int32, "k"), []int32{tc.value}, types.T_int32, mp)
				_, _, _, block, seek, ok, _ := CompileFilterExpr(expr, inSeekTable(primary), nil)
				require.True(t, ok)
				require.NotNil(t, seek)
				meta := inSeekMeta(tc.count, tc.unknown)
				require.Equal(t, tc.want, seek(meta), "只能跳过已知且不匹配的连续前缀")
				if tc.unknown >= 0 {
					stop, keep, err := block(tc.unknown, meta.GetBlockMeta(uint32(tc.unknown)), nil)
					require.NoError(t, err)
					require.False(t, stop)
					require.True(t, keep)
				}
			})
		}
	}
}

func TestInSeekDisabledForUnorderedOrNullableInputs(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	for _, tc := range []struct {
		name            string
		unsorted, empty bool
		null            bool
	}{
		{name: "unsorted", unsorted: true},
		{name: "empty", empty: true},
		{name: "nullable", null: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := inSeekTable(false)
			table.Cols[0].ClusterBy = !tc.unsorted
			values := []int32{10}
			if tc.empty {
				values = nil
			}
			expr := MakeInExprForTest(MakeColExprForTest(0, types.T_int32, "k"), values, types.T_int32, mp)
			if tc.null {
				v := vector.NewVec(types.T_int32.ToType())
				defer v.Free(mp)
				require.NoError(t, vector.AppendFixed(v, int32(10), false, mp))
				require.NoError(t, vector.AppendFixed(v, int32(0), true, mp))
				data, err := v.MarshalBinary()
				require.NoError(t, err)
				expr.GetF().Args[1].GetVec().Data = data
				expr.GetF().Args[1].GetVec().Len = 2
			}
			_, _, _, _, seek, ok, _ := CompileFilterExpr(expr, table, nil)
			require.True(t, ok)
			require.Nil(t, seek)
		})
	}
}

func BenchmarkInSeek(b *testing.B) {
	mp := mpool.MustNewZero()
	b.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(b, mp.CurrNB())
	})
	for _, count := range []int{1, 128, 4096} {
		for _, scenario := range []string{"early", "late", "none", "unknown_middle"} {
			b.Run(fmt.Sprintf("blocks=%d/%s", count, scenario), func(b *testing.B) {
				value, unknown := int32(0), -1
				if scenario != "early" {
					value = int32((count - 1) * 10)
				}
				if scenario == "none" || scenario == "unknown_middle" {
					value = int32(count * 10)
				}
				if scenario == "unknown_middle" {
					unknown = count / 2
				}
				expr := MakeInExprForTest(MakeColExprForTest(0, types.T_int32, "k"), []int32{value}, types.T_int32, mp)
				_, _, _, _, seek, ok, _ := CompileFilterExpr(expr, inSeekTable(false), nil)
				require.True(b, ok)
				meta := inSeekMeta(count, unknown)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					_ = seek(meta)
				}
			})
		}
	}
}
