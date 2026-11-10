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

package vector

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDictionaryFunctionParameters(t *testing.T) {
	for _, strings := range []bool{false, true} {
		name := "fixed"
		if strings {
			name = "strings"
		}
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			typ := types.T_int64.ToType()
			if strings {
				typ = types.T_varchar.ToType()
			}
			source := NewVec(typ)
			t.Cleanup(func() { source.Free(mp) })
			if strings {
				for i, v := range []string{"a", "a string longer than the inline descriptor", "unused"} {
					require.NoError(t, AppendBytes(source, []byte(v), i == 2, mp))
				}
			} else {
				require.NoError(t, AppendFixedList(source, []int64{10, 20, 0}, []bool{false, false, true}, mp))
			}
			base, err := FreezeVector(source, mp, nil)
			require.NoError(t, err)
			t.Cleanup(base.Release)
			rows, err := NewRowSelection([]int64{1, 0, 1, 2}, 3, mp, nil)
			require.NoError(t, err)
			t.Cleanup(rows.Release)
			dict, err := NewDictionaryVector(base, rows, mp, nil)
			require.NoError(t, err)
			t.Cleanup(func() { dict.Free(mp) })
			gathered := NewVec(typ)
			t.Cleanup(func() { gathered.Free(mp) })
			require.NoError(t, gathered.Union(dict, []int64{2, 1, 3, 0}, mp))
			require.Equal(t, 4, gathered.Length())
			bulk := NewVec(typ)
			t.Cleanup(func() { bulk.Free(mp) })
			require.NoError(t, bulk.UnionBatch(dict, 0, 4, []uint8{1, 0, 1, 1}, mp))
			require.Equal(t, 3, bulk.Length())
			require.True(t, bulk.IsNull(2))
			if strings {
				require.Equal(t, "a string longer than the inline descriptor", string(bulk.GetBytesAt(1)))
			} else {
				require.Equal(t, int64(20), GetFixedAtNoTypeCheck[int64](bulk, 1))
			}
			require.True(t, gathered.IsNull(2))
			copied := NewVec(typ)
			t.Cleanup(func() { copied.Free(mp) })
			require.NoError(t, copied.UnionMulti(dict, 1, 2, mp))
			require.NoError(t, copied.Copy(dict, 0, 0, mp))
			if strings {
				require.Equal(t, "a string longer than the inline descriptor", string(gathered.GetBytesAt(0)))
				require.Equal(t, "a", string(gathered.GetBytesAt(1)))
				require.Equal(t, "a string longer than the inline descriptor", string(copied.GetBytesAt(0)))
			} else {
				require.Equal(t, []int64{20, 10, 0, 20}, MustFixedColNoTypeCheck[int64](gathered))
				require.Equal(t, []int64{20, 10}, MustFixedColNoTypeCheck[int64](copied))
			}

			if strings {
				p := GenerateFunctionStrParameter(dict)
				for i, want := range []string{"a string longer than the inline descriptor", "a", "a string longer than the inline descriptor", ""} {
					got, isNull := p.GetStrValue(uint64(i))
					require.Equal(t, i == 3, isNull)
					require.Equal(t, want, string(got))
				}
				require.Same(t, dict, p.GetSourceVector())
				require.True(t, ReuseFunctionStrParameter(dict, p))
				require.False(t, ReuseFunctionStrParameter(source, p))
				require.False(t, ReuseFunctionStrParameter(dict, GenerateFunctionStrParameter(source)))
				require.Panics(t, func() { p.UnSafeGetAllValue() })
			} else {
				p := GenerateFunctionFixedTypeParameter[int64](dict)
				for i, want := range []int64{20, 10, 20, 0} {
					got, isNull := p.GetValue(uint64(i))
					require.Equal(t, i == 3, isNull)
					require.Equal(t, want, got)
				}
				require.Same(t, dict, p.GetSourceVector())
				require.True(t, p.WithAnyNullValue())
				require.True(t, ReuseFunctionFixedTypeParameter(dict, p))
				require.False(t, ReuseFunctionFixedTypeParameter(source, p))
				require.False(t, ReuseFunctionFixedTypeParameter(dict, GenerateFunctionFixedTypeParameter[int64](source)))
				require.Panics(t, func() { p.UnSafeGetAllValue() })
			}
		})
	}
}

func TestDictionaryFunctionParameterConstantBacking(t *testing.T) {
	for _, allNull := range []bool{false, true} {
		t.Run(map[bool]string{false: "value", true: "null"}[allNull], func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			source, err := NewConstFixed(types.T_int64.ToType(), int64(7), 3, mp)
			require.NoError(t, err)
			if allNull {
				source.Free(mp)
				source = NewConstNull(types.T_int64.ToType(), 3, mp)
			}
			t.Cleanup(func() { source.Free(mp) })
			base, err := FreezeVector(source, mp, nil)
			require.NoError(t, err)
			t.Cleanup(base.Release)
			rows, err := NewRowSelection([]int64{2, 0, 2}, 3, mp, nil)
			require.NoError(t, err)
			t.Cleanup(rows.Release)
			dict, err := NewDictionaryVector(base, rows, mp, nil)
			require.NoError(t, err)
			t.Cleanup(func() { dict.Free(mp) })
			parameter := GenerateFunctionFixedTypeParameter[int64](dict)
			for row := uint64(0); row < 3; row++ {
				value, isNull := parameter.GetValue(row)
				require.Equal(t, allNull, isNull)
				if !allNull {
					require.Equal(t, int64(7), value)
				}
			}
		})
	}
}
