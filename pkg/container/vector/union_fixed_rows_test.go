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

package vector

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestFixedSourceRowsMatchUnionOne(t *testing.T) {
	for _, oid := range []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_decimal128, types.T_decimal256, types.T_char, types.T_varchar} {
		varlen := oid == types.T_char || oid == types.T_varchar
		prefixes := []int{0, 1, 2}
		if varlen {
			prefixes = []int{1}
		}
		for _, prefix := range prefixes {
			for _, onlyNulls := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/prefix%d/all_null%t", oid, prefix, onlyNulls), func(t *testing.T) {
					mp := mpool.MustNewZero()
					a, b, got, want := NewVec(oid.ToType()), NewVec(oid.ToType()), NewVec(oid.ToType()), NewVec(oid.ToType())
					t.Cleanup(func() {
						a.Free(mp)
						b.Free(mp)
						got.Free(mp)
						want.Free(mp)
						require.Zero(t, mp.CurrNB())
					})
					for k, v := range []*Vector{a, b} {
						if varlen {
							for row := range 20 {
								require.NoError(t, AppendBytes(v, []byte(fmt.Sprintf("\xff\x00%d-%d", k, row)), false, mp))
							}
						} else {
							require.NoError(t, v.PreExtend(20, mp))
							v.SetLength(20)
							for i := 0; i < 20*oid.TypeLen(); i++ {
								v.data[i] = byte(i*3 + k*17)
							}
						}
						v.SetNull(2)
						v.SetNull(4)
					}
					if prefix != 0 {
						if varlen && prefix == 1 {
							for _, dst := range []*Vector{got, want} {
								require.NoError(t, AppendBytes(dst, []byte("an existing out-of-line prefix"), false, mp))
							}
						} else {
							sel := int64(0)
							if prefix == 2 {
								sel = 2
							}
							require.NoError(t, got.UnionOne(a, sel, mp))
							require.NoError(t, want.UnionOne(a, sel, mp))
						}
					}
					rows := []FixedSourceRow{{a, 8}, {b, 2}, {a, 8}, {b, 0}, {a, 4}, {b, 19}}
					if onlyNulls {
						rows = []FixedSourceRow{{a, 2}, {b, 4}, {a, 2}}
					}
					used, err := got.TryUnionFlatFixedRows(rows, mp)
					require.True(t, used)
					require.NoError(t, err)
					for _, row := range rows {
						require.NoError(t, want.UnionOne(row.Vec, row.Row, mp))
					}
					require.Equal(t, want.length, got.length)
					require.Equal(t, want.prepareParamKind, got.prepareParamKind)
					require.Equal(t, want.prepareParamKindSeen, got.prepareParamKindSeen)
					require.Equal(t, want.stringSource, got.stringSource)
					width := oid.TypeLen()
					for i := 0; i < got.length; i++ {
						require.Equal(t, want.IsNull(uint64(i)), got.IsNull(uint64(i)))
						require.Equal(t, want.gsp.Contains(uint64(i)), got.gsp.Contains(uint64(i)))
						if !want.IsNull(uint64(i)) {
							if varlen {
								require.Equal(t, want.GetBytesAt(i), got.GetBytesAt(i))
							} else {
								require.Equal(t, want.data[i*width:(i+1)*width], got.data[i*width:(i+1)*width])
							}
						}
					}
				})
			}
		}
	}
}

func TestFixedSourceRowsFallbackAndAdmission(t *testing.T) {
	for _, shape := range []string{"const", "grouping", "parameter", "binary", "borrowed", "alias", "type", "negative", "past_end", "singleton", "unicode", "out_of_line"} {
		t.Run(shape, func(t *testing.T) {
			mp := mpool.MustNewZero()
			typ := types.T_int64.ToType()
			if shape == "unicode" || shape == "out_of_line" {
				typ = types.New(types.T_varchar, 64, 0)
				if shape == "unicode" {
					typ.Charset = types.CharsetUTF8MB4UnicodeCI
				}
			}
			a, b, dst := NewVec(typ), NewVec(typ), NewVec(typ)
			t.Cleanup(func() { b.cantFreeData = false; a.Free(mp); b.Free(mp); dst.Free(mp); require.Zero(t, mp.CurrNB()) })
			for _, v := range []*Vector{a, b} {
				if typ.IsVarlen() {
					require.NoError(t, AppendBytes(v, []byte("inline"), false, mp))
					require.NoError(t, AppendBytes(v, []byte("a string longer than inline storage"), false, mp))
				} else {
					require.NoError(t, AppendFixed(v, int64(10), false, mp))
					require.NoError(t, AppendFixed(v, int64(20), false, mp))
				}
			}
			require.NoError(t, dst.UnionOne(a, 0, mp))
			rows := []FixedSourceRow{{a, 0}, {b, 1}}
			switch shape {
			case "const":
				b.class = CONSTANT
			case "grouping":
				b.gsp.Add(1)
			case "parameter":
				b.SetPrepareParamKind(PrepareParamInteger)
			case "binary":
				b.SetIsBinaryString(true)
			case "borrowed":
				b.cantFreeData = true
			case "alias":
				rows[1] = FixedSourceRow{dst, 0}
			case "type":
				b.typ = types.T_int32.ToType()
			case "negative":
				rows[1].Row = -1
			case "past_end":
				rows[1].Row = 2
			case "singleton":
				rows = rows[:1]
			}
			before := append([]byte(nil), dst.data...)
			used, err := dst.TryUnionFlatFixedRows(rows, mp)
			require.False(t, used)
			require.NoError(t, err)
			require.Equal(t, 1, dst.length)
			require.Equal(t, before, dst.data)
		})
	}
	for _, nullable := range []bool{false, true} {
		t.Run(fmt.Sprintf("admission/nullable%t", nullable), func(t *testing.T) {
			state := newTestVectorAllocationAccount(t, 1, 16)
			mp := mpool.MustNewZero()
			src := NewVec(types.T_int64.ToType())
			dst := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
			t.Cleanup(func() {
				src.Free(mp)
				dst.Free(mp)
				require.Zero(t, mp.CurrNB())
				finalizeTestVectorAllocationAccount(t, state)
			})
			require.NoError(t, AppendFixed(src, int64(10), nullable, mp))
			require.NoError(t, AppendFixed(src, int64(20), false, mp))
			used, err := dst.TryUnionFlatFixedRows([]FixedSourceRow{{src, 0}, {src, 1}}, mp)
			require.True(t, used)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			require.Zero(t, dst.length)
			require.Zero(t, state.account.Snapshot().Used)
		})
	}
}
