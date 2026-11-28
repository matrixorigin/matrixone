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

package hashmap

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// nullableKeyVector returns a UnitLimit-row key vector of oid with every
// tenth row NULL, so encoding takes fillNullableStr's per-row loop.
func nullableKeyVector(tb testing.TB, oid types.T, mp *mpool.MPool) *vector.Vector {
	v := vector.NewVec(oid.ToType(), mp)
	for row := 0; row < UnitLimit; row++ {
		null := row%10 == 3
		var err error
		switch oid {
		case types.T_int32:
			err = vector.AppendFixed(v, int32(row), null, mp)
		case types.T_float64:
			err = vector.AppendFixed(v, float64(row)/3, null, mp)
		case types.T_float32:
			err = vector.AppendFixed(v, float32(row)/3, null, mp)
		case types.T_decimal128:
			err = vector.AppendFixed(v, types.Decimal128{B0_63: uint64(row)}, null, mp)
		case types.T_varchar:
			err = vector.AppendBytes(v, []byte(fmt.Sprintf("key-%06d", row)), null, mp)
		}
		if err != nil {
			tb.Fatal(err)
		}
	}
	return v
}

// BenchmarkFillNullableStr measures the per-row loop of nullable key
// encoding (vectors with NULLs, so the no-NULL fast paths do not apply).
func BenchmarkFillNullableStr(b *testing.B) {
	for _, oid := range []types.T{types.T_int32, types.T_float32, types.T_float64, types.T_decimal128, types.T_varchar} {
		b.Run(oid.OidString(), func(b *testing.B) {
			mp := mpool.MustNewZero()
			hashMap, err := NewStrHashMap(true, mp)
			if err != nil {
				b.Fatal(err)
			}
			defer hashMap.Free()
			vec := nullableKeyVector(b, oid, mp)
			defer vec.Free(mp)
			itr := hashMap.NewIterator().(*strHashmapIterator)
			if err := itr.prepareHashKeys([]*vector.Vector{vec}, 0, UnitLimit); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for k := range itr.keys[:UnitLimit] {
					itr.keys[k] = itr.keys[k][:0]
				}
				fillNullableStr(itr, vec, UnitLimit, 0)
			}
		})
	}
}

// TestFillNullableStrMatchesLegacy requires the typed loops to encode the same
// key bytes, and mark the same rows as not inserted, as the per-row switch
// they replace: for vectors with NULLs, const and const-NULL vectors, maps that
// keep NULL keys and maps that do not, and a start offset.
func TestFillNullableStrMatchesLegacy(t *testing.T) {
	mp := mpool.MustNewZero()
	oids := []types.T{
		types.T_bool, types.T_int8, types.T_int32, types.T_int64, types.T_uint16,
		types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128,
		types.T_date, types.T_datetime, types.T_uuid, types.T_varchar, types.T_char, types.T_text,
	}
	for _, hasNull := range []bool{true, false} {
		for _, oid := range oids {
			typ := oid.ToType()
			vecs := map[string]*vector.Vector{}
			plain := vector.NewVec(typ, mp)
			for row := 0; row < UnitLimit; row++ {
				null := row%10 == 3
				var err error
				if typ.IsVarlen() {
					err = vector.AppendBytes(plain, []byte(fmt.Sprintf("key-%06d", row)), null, mp)
				} else {
					raw := make([]byte, typ.TypeSize())
					for k := range raw {
						raw[k] = byte(row*7 + k)
					}
					if oid == types.T_bool {
						raw[0] &= 1
					}
					err = vector.MakeAppendBytesFunc(plain)(raw, null, mp)
				}
				require.NoError(t, err)
			}
			vecs["plain"] = plain
			c := vector.NewVec(typ, mp)
			require.NoError(t, vector.GetConstSetFunction(typ, mp)(c, plain, 0, UnitLimit))
			vecs["const"] = c
			vecs["const null"] = vector.NewConstNull(typ, UnitLimit, mp)

			for name, vec := range vecs {
				for _, start := range []int{0, 5} {
					n := UnitLimit - start
					run := func(fill func(*strHashmapIterator, *vector.Vector, int, int)) ([][]byte, []int64) {
						hm, err := NewStrHashMap(hasNull, mp)
						require.NoError(t, err)
						defer hm.Free()
						itr := hm.NewIterator().(*strHashmapIterator)
						require.NoError(t, itr.prepareHashKeys([]*vector.Vector{vec}, start, n))
						for k := range itr.keys[:n] {
							itr.keys[k] = itr.keys[k][:0]
							itr.zValues[k] = 1
						}
						fill(itr, vec, n, start)
						keys := make([][]byte, n)
						for k := range keys {
							keys[k] = append([]byte(nil), itr.keys[k]...)
						}
						return keys, append([]int64(nil), itr.zValues[:n]...)
					}
					wantKeys, wantZ := run(legacyFillNullableStr)
					gotKeys, gotZ := run(fillNullableStr)
					msg := fmt.Sprintf("%s %s hasNull=%v start=%d", oid.OidString(), name, hasNull, start)
					require.Equal(t, wantKeys, gotKeys, msg)
					require.Equal(t, wantZ, gotZ, msg)
				}
			}
			for _, vec := range vecs {
				vec.Free(mp)
			}
		}
	}
	require.Zero(t, mp.CurrNB())
}
