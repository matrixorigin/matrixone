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
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestUnionPlainFixedSelectionReuse(t *testing.T) {
	for _, oid := range []types.T{types.T_uint32, types.T_uint64, types.T_float32, types.T_float64} {
		for _, backing := range []string{"owned", "window", "external"} {
			for _, index32 := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/int32=%t", oid, backing, index32), func(t *testing.T) {
					mp := mpool.MustNewZero()
					t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
					src, dst := NewVec(oid.ToType(), mp), NewVec(oid.ToType(), mp)
					t.Cleanup(func() { src.Free(mp); dst.Free(mp) })
					require.NoError(t, src.PreExtend(4, mp))
					src.SetLength(4)
					width := src.typ.TypeSize()
					for row := range 4 {
						if width == 4 {
							binary.LittleEndian.PutUint32(src.data[row*width:], []uint32{0x80000000, 0x7fc01234, 0xff800000, 0xffffffff}[row])
						} else {
							binary.LittleEndian.PutUint64(src.data[row*width:], []uint64{0x8000000000000000, 0x7ff8000000001234, 0xfff0000000000000, 0xffffffffffffffff}[row])
						}
					}
					input := src
					if backing != "owned" {
						var err error
						if backing == "window" {
							input, err = src.Window(0, 4)
						} else {
							data, allocErr := mp.Alloc(4*width, true)
							require.NoError(t, allocErr)
							copy(data, src.data[:4*width])
							input = NewVecWithData(oid.ToType(), 4, data, nil, nil)
							// A read-only vector over memory the test owns and frees after input.
							t.Cleanup(func() { mp.Free(data) })
						}
						require.NoError(t, err)
						t.Cleanup(func() { input.Free(mp) })
					}
					require.NoError(t, dst.PreExtend(8, mp))
					for range 2 {
						dst.CleanOnlyData()
						before := mp.Stats().NumAlloc.Load()
						if index32 {
							require.NoError(t, dst.UnionInt32(input, []int32{3}, mp))
							require.NoError(t, dst.UnionInt32(input, []int32{2, 0, 1, 1}, mp))
						} else {
							require.NoError(t, dst.Union(input, []int64{3}, mp))
							require.NoError(t, dst.Union(input, []int64{2, 0, 1, 1}, mp))
						}
						require.Equal(t, before, mp.Stats().NumAlloc.Load())
						require.Equal(t, 5, dst.Length())
						for row, sourceRow := range []int{3, 2, 0, 1, 1} {
							require.Equal(t, src.data[sourceRow*width:(sourceRow+1)*width], dst.data[row*width:(row+1)*width])
							require.False(t, dst.IsNull(uint64(row)))
						}
					}
				})
			}
		}
	}
}

func TestUnionFixedSelection(t *testing.T) {
	for _, oid := range []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint32, types.T_uint64, types.T_float32, types.T_float64, types.T_date, types.T_datetime,
		types.T_time, types.T_timestamp, types.T_decimal64,
		types.T_TS, types.T_decimal128, types.T_uuid, types.T_Blockid, types.T_Rowid, types.T_decimal256} {
		for _, index32 := range []bool{false, true} {
			for _, nullable := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/int32=%t/nullable=%t", oid, index32, nullable), func(t *testing.T) {
					mp := mpool.MustNewZero()
					t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
					src, dst := NewVec(oid.ToType(), mp), NewVec(oid.ToType(), mp)
					t.Cleanup(func() { src.Free(mp); dst.Free(mp) })
					require.NoError(t, src.PreExtend(4, mp))
					src.SetLength(4)
					width := src.typ.TypeSize()
					expected := make([][]byte, 4)
					for row := range expected {
						expected[row] = make([]byte, width)
						for column := range width {
							expected[row][column] = byte(row*32 + column + 1)
						}
						// Include signed zero and NaN payloads: selection copies
						// physical bits, without numeric conversion.
						if oid == types.T_float32 {
							binary.LittleEndian.PutUint32(expected[row], []uint32{0x80000000, 0x7fc01234, 0xff800000, 0xffffffff}[row])
						} else if oid == types.T_float64 {
							binary.LittleEndian.PutUint64(expected[row], []uint64{0x8000000000000000, 0x7ff8000000001234, 0xfff0000000000000, 0xffffffffffffffff}[row])
						}
						copy(src.data[row*width:(row+1)*width], expected[row])
					}
					if nullable {
						src.GetNulls().Add(1)
					}
					view, err := src.Window(1, 4)
					require.NoError(t, err)
					t.Cleanup(func() { view.Free(mp) })
					for range 2 {
						dst.CleanOnlyData()
						// Preserve an existing prefix, then select from a read-only
						// source with repetitions, a tail row and reverse order.
						if index32 {
							require.NoError(t, dst.UnionInt32(src, []int32{0}, mp))
							require.NoError(t, dst.UnionInt32(view, nil, mp))
							require.Equal(t, 1, dst.Length())
							require.NoError(t, dst.UnionInt32(view, []int32{2, 1, 1, 0}, mp))
						} else {
							require.NoError(t, dst.Union(src, []int64{0}, mp))
							require.NoError(t, dst.Union(view, nil, mp))
							require.Equal(t, 1, dst.Length())
							require.NoError(t, dst.Union(view, []int64{2, 1, 1, 0}, mp))
						}
						require.Equal(t, 5, dst.Length())
						for row, sourceRow := range []int{0, 3, 2, 2, 1} {
							isNull := nullable && sourceRow == 1
							require.Equal(t, isNull, dst.IsNull(uint64(row)))
							if !isNull {
								require.Equal(t, expected[sourceRow], dst.data[row*width:(row+1)*width])
							}
						}
					}
					// Self-append must read the original prefix after growth has
					// replaced the backing allocation.
					oldLength := dst.Length()
					copies := cap(dst.data)/width + 1
					if index32 {
						rows := make([]int32, copies)
						for i := range rows {
							rows[i] = 1
						}
						require.NoError(t, dst.UnionInt32(dst, rows, mp))
					} else {
						rows := make([]int64, copies)
						for i := range rows {
							rows[i] = 1
						}
						require.NoError(t, dst.Union(dst, rows, mp))
					}
					require.Equal(t, oldLength+copies, dst.Length())
					for row := oldLength; row < dst.Length(); row++ {
						require.Equal(t, expected[3], dst.data[row*width:(row+1)*width])
						require.False(t, dst.IsNull(uint64(row)))
					}
					// Results own their copied bytes and cannot change with the source.
					clear(src.data)
					require.Equal(t, expected[3], dst.data[width:2*width])
				})
			}
		}
	}
}

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
					a, b, got, want := NewVec(oid.ToType(), mp), NewVec(oid.ToType(), mp), NewVec(oid.ToType(), mp), NewVec(oid.ToType(), mp)
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
					width := oid.TypeLen()
					for i := 0; i < got.length; i++ {
						require.Equal(t, want.IsNull(uint64(i)), got.IsNull(uint64(i)))
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
	for _, shape := range []string{"const", "alias", "type", "negative", "past_end", "singleton", "unicode", "out_of_line"} {
		t.Run(shape, func(t *testing.T) {
			mp := mpool.MustNewZero()
			typ := types.T_int64.ToType()
			if shape == "unicode" || shape == "out_of_line" {
				typ = types.New(types.T_varchar, 64, 0)
				if shape == "unicode" {
					typ.Charset = types.CharsetUTF8MB4UnicodeCI
				}
			}
			a, b, dst := NewVec(typ, mp), NewVec(typ, mp), NewVec(typ, mp)
			t.Cleanup(func() { a.Free(mp); b.Free(mp); dst.Free(mp); require.Zero(t, mp.CurrNB()) })
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
			src := NewVec(types.T_int64.ToType(), mp)
			dmp := state.view(t, mp)
			dst := NewVec(types.T_int64.ToType(), dmp)
			t.Cleanup(func() {
				src.Free(mp)
				dst.Free(dmp)
				require.Zero(t, mp.CurrNB())
				finalizeTestVectorAllocationAccount(t, state)
			})
			require.NoError(t, AppendFixed(src, int64(10), nullable, mp))
			require.NoError(t, AppendFixed(src, int64(20), false, mp))
			used, err := dst.TryUnionFlatFixedRows([]FixedSourceRow{{src, 0}, {src, 1}}, dmp)
			require.True(t, used)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			require.Zero(t, dst.length)
			require.Zero(t, state.account.Snapshot().Used)
		})
	}
}

func TestUnionInlineSelection(t *testing.T) {
	for _, shape := range []string{"inline", "zero-area-noninline", "zero-area-noninline-empty-dest", "nullable", "mixed"} {
		for _, index32 := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/int32=%t", shape, index32), func(t *testing.T) {
				mp := mpool.MustNewZero()
				t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
				src, dst := NewVec(types.T_varchar.ToType(), mp), NewVec(types.T_varchar.ToType(), mp)
				t.Cleanup(func() { src.Free(mp); dst.Free(mp) })
				values := []string{"a", "b", strings.Repeat("c", types.VarlenaInlineSize), "d", "e", "f", "g", "h"}
				emptyNonInline := strings.HasPrefix(shape, "zero-area-noninline")
				if emptyNonInline {
					values[1] = ""
				} else if shape == "mixed" {
					values[1] = strings.Repeat("b", types.VarlenaInlineSize+1)
				}
				for row, value := range values {
					require.NoError(t, AppendBytes(src, []byte(value), shape == "nullable" && row == 4, mp))
				}
				if emptyNonInline {
					// Zero area bytes alone do not prove inline descriptors.
					MustFixedColNoTypeCheck[types.Varlena](src)[1].SetOffsetLen(0, 0)
					require.Empty(t, src.area)
				}
				// Consecutive runs, gaps, repetition and reversal share one
				// append. The expected rows do not depend on the copy helper.
				rows := []int64{1, 2, 3, 6, 6, 4, 0, 1, 2}
				prefix := "prefix"
				if shape == "zero-area-noninline" {
					prefix = strings.Repeat("p", 64)
				}
				for range 2 {
					dst.CleanOnlyData()
					require.NoError(t, AppendBytes(dst, []byte(prefix), false, mp))
					if index32 {
						sels := make([]int32, len(rows))
						for i, row := range rows {
							sels[i] = int32(row)
						}
						require.NoError(t, dst.UnionInt32(src, sels, mp))
					} else {
						require.NoError(t, dst.Union(src, rows, mp))
					}
					require.Equal(t, prefix, dst.GetStringAt(0))
					require.Equal(t, len(rows)+1, dst.Length())
					for i, row := range rows {
						isNull := shape == "nullable" && row == 4
						require.Equal(t, isNull, dst.IsNull(uint64(i+1)))
						if !isNull {
							require.Equal(t, values[row], dst.GetStringAt(i+1))
							if emptyNonInline && row == 1 {
								// The ordinary copy rebases even an empty non-inline
								// payload after the destination's existing area.
								offset, length := MustFixedColNoTypeCheck[types.Varlena](dst)[i+1].OffsetLen()
								expectedOffset := uint32(0)
								if shape == "zero-area-noninline" {
									expectedOffset = uint32(len(prefix))
								}
								require.Equal(t, expectedOffset, offset)
								require.Zero(t, length)
							}
						}
					}
				}
			})
		}
	}
}

// TestUnionBatchVarlenContiguousRangeFastPath covers the partial-range shape
// used when a wide loop-join result is split into bounded output batches. The
// source payload is contiguous, but offset != 0 and cnt < source length, so it
// cannot use the whole-vector fast path.
func TestUnionBatchVarlenContiguousRangeFastPath(t *testing.T) {
	mp := mpool.MustNewZero()
	const (
		rows  = 300
		dims  = 768
		start = 41
		count = 173
	)

	src := NewVec(types.T_array_float32.ToType(), mp)
	for row := 0; row < rows; row++ {
		isNull := row%29 == 0
		value := make([]float32, dims)
		if !isNull {
			for col := range value {
				value[col] = float32(row*1000 + col)
			}
		}
		require.NoError(t, AppendArray(src, value, isNull, mp))
	}

	dst := NewVec(types.T_array_float32.ToType(), mp)
	require.NoError(t, AppendArray(dst, []float32{1, 2, 3}, false, mp))
	require.NoError(t, dst.UnionBatch(src, start, count, nil, mp))

	ref := NewVec(types.T_array_float32.ToType(), mp)
	require.NoError(t, AppendArray(ref, []float32{1, 2, 3}, false, mp))
	for row := start; row < start+count; row++ {
		require.NoError(t, ref.UnionOne(src, int64(row), mp))
	}

	require.Equal(t, ref.Length(), dst.Length())
	for row := 0; row < dst.Length(); row++ {
		isNull := ref.IsNull(uint64(row))
		require.Equalf(t, isNull, dst.IsNull(uint64(row)), "null row %d", row)
		if !isNull {
			require.Equalf(t,
				GetArrayAt[float32](ref, row),
				GetArrayAt[float32](dst, row),
				"value row %d", row)
		}
	}

	src.Free(mp)
	dst.Free(mp)
	ref.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestUnionBatchVarlenWindowCopiesOnlyLiveArea is the regression guard for
// read-only windows retaining their parent's complete area. UnionBatch receives
// the window as a whole vector (offset 0, cnt == length), but must copy only the
// payload referenced by the window's live descriptors.
func TestUnionBatchVarlenWindowCopiesOnlyLiveArea(t *testing.T) {
	mp := mpool.MustNewZero()
	const (
		rows  = 300
		dims  = 64
		start = 101
		count = 37
	)
	src := NewVec(types.T_array_float32.ToType(), mp)
	value := make([]float32, dims)
	for row := 0; row < rows; row++ {
		value[0] = float32(row)
		require.NoError(t, AppendArray(src, value, false, mp))
	}
	// A cloned window copies only its rows' payload; a window is a read-only
	// reference that retains the whole shared area.
	owned, err := src.CloneWindow(start, start+count, mp)
	require.NoError(t, err)
	require.Len(t, owned.GetArea(), count*dims*4)
	owned.Free(mp)
	readonly := NewVecWithData(src.typ, src.Length(), src.data, src.area, nil)
	window, err := readonly.Window(start, start+count)
	require.NoError(t, err)
	require.True(t, window.IsReadOnly())
	require.Len(t, window.GetArea(), rows*dims*4)

	dst := NewVec(types.T_array_float32.ToType(), mp)
	require.NoError(t, dst.UnionBatch(window, 0, window.Length(), nil, mp))
	require.Len(t, dst.GetArea(), count*dims*4)
	for row := 0; row < count; row++ {
		require.Equal(t,
			GetArrayAt[float32](src, start+row),
			GetArrayAt[float32](dst, row))
	}

	window.Free(mp)
	src.Free(mp)
	dst.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

// TestUnionBatchVarlenRangeFallsBackForReorderedArea verifies that a valid
// vector whose row headers do not follow area order still uses the general
// per-row path and preserves its logical values.
func TestUnionBatchVarlenRangeFallsBackForReorderedArea(t *testing.T) {
	mp := mpool.MustNewZero()
	src := NewVec(types.T_varchar.ToType(), mp)
	for i := 0; i < 8; i++ {
		require.NoError(t, AppendBytes(src,
			[]byte(fmt.Sprintf("row-%d-", i)+string(make([]byte, 128))),
			false, mp))
	}
	var headers []types.Varlena
	ToSliceNoTypeCheck(src, &headers)
	headers[3], headers[4] = headers[4], headers[3]

	dst := NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, dst.UnionBatch(src, 2, 4, nil, mp))
	require.Equal(t, 4, dst.Length())
	for i := 0; i < 4; i++ {
		require.Equal(t, src.GetBytesAt(2+i), dst.GetBytesAt(i))
	}

	src.Free(mp)
	dst.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestUnionBatchVarlenPartialSelfAppendForcedGrowth(t *testing.T) {
	mp := mpool.MustNewZero()
	vec, values, isNull := newForcedGrowthAliasVector(t, mp)
	const offset = 1
	cnt := vec.Length() - offset
	requireUnionBatchAliasGrowth(t, vec, values, isNull, offset, cnt)

	oldLength := vec.Length()
	require.NoError(t, vec.UnionBatch(vec, offset, cnt, nil, mp))
	assertUnionBatchAliasResult(t, vec, values, isNull, oldLength, offset, cnt)

	vec.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestUnionBatchVarlenWindowAliasForcedGrowth(t *testing.T) {
	mp := mpool.MustNewZero()
	vec, values, isNull := newForcedGrowthAliasVector(t, mp)
	const offset = 1
	cnt := vec.Length() - offset
	requireUnionBatchAliasGrowth(t, vec, values, isNull, offset, cnt)

	window, err := vec.Window(offset, offset+cnt)
	require.NoError(t, err)
	oldLength := vec.Length()
	require.NoError(t, vec.UnionBatch(window, 0, window.Length(), nil, mp))
	assertUnionBatchAliasResult(t, vec, values, isNull, oldLength, offset, cnt)

	window.Free(mp)
	vec.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func newForcedGrowthAliasVector(
	t *testing.T,
	mp *mpool.MPool,
) (*Vector, []string, []bool) {
	t.Helper()
	vec := NewVec(types.T_varchar.ToType(), mp)
	values := make([]string, 0, 128)
	isNull := make([]bool, 0, 128)
	for row := 0; row < 128; row++ {
		null := row%7 == 3
		value := fmt.Sprintf("row-%03d-", row) + string(make([]byte, 256+row%13))
		require.NoError(t, AppendBytes(vec, []byte(value), null, mp))
		values = append(values, value)
		isNull = append(isNull, null)
		if row < 7 {
			continue
		}
		payloadBytes := 0
		for i := 1; i < len(values); i++ {
			if !isNull[i] {
				payloadBytes += len(values[i])
			}
		}
		selectedRows := len(values) - 1
		if len(vec.area)+payloadBytes > cap(vec.area) &&
			len(vec.data)+selectedRows*vec.typ.TypeSize() > cap(vec.data) {
			return vec, values, isNull
		}
	}
	vec.Free(mp)
	t.Fatal("failed to construct a vector that forces data and area growth")
	return nil, nil, nil
}

func requireUnionBatchAliasGrowth(
	t *testing.T,
	vec *Vector,
	values []string,
	isNull []bool,
	offset int,
	cnt int,
) {
	t.Helper()
	payloadBytes := 0
	for row := offset; row < offset+cnt; row++ {
		if !isNull[row] {
			payloadBytes += len(values[row])
		}
	}
	require.Greater(t, len(vec.area)+payloadBytes, cap(vec.area))
	require.Greater(t, len(vec.data)+cnt*vec.typ.TypeSize(), cap(vec.data))
}

func assertUnionBatchAliasResult(
	t *testing.T,
	vec *Vector,
	values []string,
	isNull []bool,
	oldLength int,
	offset int,
	cnt int,
) {
	t.Helper()
	require.Equal(t, oldLength+cnt, vec.Length())
	for i := 0; i < cnt; i++ {
		sourceRow := offset + i
		targetRow := oldLength + i
		require.Equalf(t, isNull[sourceRow], vec.IsNull(uint64(targetRow)),
			"null row %d", targetRow)
		if !isNull[sourceRow] {
			require.Equalf(t, values[sourceRow], string(vec.GetBytesAt(targetRow)),
				"value row %d", targetRow)
		}
	}
}

func BenchmarkUnionBatchVarlenWindow768(b *testing.B) {
	mp := mpool.MustNewZero()
	const (
		rows  = 16384
		dims  = 768
		start = 4096
		count = 8192
	)
	src := NewVec(types.T_array_float32.ToType(), mp)
	value := make([]float32, dims)
	for row := 0; row < rows; row++ {
		value[0] = float32(row)
		if err := AppendArray(src, value, false, mp); err != nil {
			b.Fatal(err)
		}
	}
	window, err := src.Window(start, start+count)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(count * dims * 4)
	dst := NewVec(types.T_array_float32.ToType(), mp)
	if err := dst.UnionBatch(window, 0, count, nil, mp); err != nil {
		b.Fatal(err)
	}
	dst.CleanOnlyData()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := dst.UnionBatch(window, 0, count, nil, mp); err != nil {
			b.Fatal(err)
		}
		dst.CleanOnlyData()
	}
	b.StopTimer()
	dst.Free(mp)
	window.Free(mp)
	src.Free(mp)
}

// TestUnionBroadcastAndPregrow covers the const-broadcast doubling fill and the
// sels-gather area pre-grow (UnionMulti / Union / UnionBatch / AppendMultiFixed),
// with mixed inline+non-inline values and nulls, cross-checked against per-row
// UnionOne (the known-correct path) and direct expectations.
func TestUnionVarlenGatherAndBroadcast(t *testing.T) {
	mp := mpool.MustNewZero()
	const n = 400
	vals := make([]string, n)
	isNull := make([]bool, n)
	for i := 0; i < n; i++ {
		switch i % 4 {
		case 0:
			vals[i] = fmt.Sprintf("s%d", i) // inline
		case 1:
			vals[i] = "L" + fmt.Sprintf("%d", i) + string(make([]byte, 700)) // non-inline
		case 2:
			isNull[i] = true
		default:
			vals[i] = fmt.Sprintf("m%d-%s", i, string(make([]byte, 40))) // non-inline, mid
		}
	}
	src := NewVec(types.T_varchar.ToType(), mp)
	for i := 0; i < n; i++ {
		require.NoError(t, AppendBytes(src, []byte(vals[i]), isNull[i], mp))
	}
	get := func(v *Vector, i int) (string, bool) {
		if v.GetNulls().Contains(uint64(i)) {
			return "", true
		}
		return string(v.GetBytesAt(i)), false
	}

	// 1) Union (unionT sels-gather, pre-grow path) into a seeded target, reverse order.
	{
		v := NewVec(types.T_varchar.ToType(), mp)
		require.NoError(t, AppendBytes(v, []byte("seed"+string(make([]byte, 900))), false, mp))
		sels := make([]int64, n)
		for i := range sels {
			sels[i] = int64(n - 1 - i)
		}
		require.NoError(t, v.Union(src, sels, mp))
		require.Equal(t, 1+n, v.Length())
		s, nu := get(v, 0)
		require.False(t, nu)
		require.Equal(t, "seed"+string(make([]byte, 900)), s)
		for i, sel := range sels {
			gs, gn := get(v, 1+i)
			require.Equalf(t, isNull[sel], gn, "Union null row %d", i)
			if !gn {
				require.Equalf(t, vals[sel], gs, "Union row %d", i)
			}
		}
	}

	// 2) UnionMulti broadcast (varlen) of one non-inline row, large cnt.
	{
		v := NewVec(types.T_varchar.ToType(), mp)
		require.NoError(t, v.UnionMulti(src, 1, 333, mp)) // src[1] is non-inline
		require.Equal(t, 333, v.Length())
		for i := 0; i < 333; i++ {
			require.Equal(t, vals[1], string(v.GetBytesAt(i)))
		}
	}

	src.Free(mp)
}

func BenchmarkConstBroadcastFill(b *testing.B) {
	const cnt = 8192
	var va types.Varlena
	va.SetOffsetLen(12345, 678)
	dst := make([]types.Varlena, cnt)
	b.Run("scalar", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < cnt; j++ {
				dst[j] = va
			}
		}
	})
	b.Run("doubling", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			fillSlice(dst, 0, cnt, va)
		}
	})
}

func TestUnionBatchFixedNullableRange(t *testing.T) {
	for _, test := range []struct {
		name  string
		nulls []uint64
	}{
		{name: "sparse", nulls: []uint64{0, 2, 6}},
		{name: "all", nulls: []uint64{0, 1, 2, 3, 4, 6}},
		{name: "outside-range", nulls: []uint64{0, 6}},
	} {
		for _, preflighted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/preflighted-%t", test.name, preflighted), func(t *testing.T) {
				mp := mpool.MustNewZero()
				t.Cleanup(func() {
					defer mpool.DeleteMPool(mp)
					require.Zero(t, mp.CurrNB())
				})
				source := NewVec(types.T_int64.ToType(), mp)
				defer source.Free(mp)
				require.NoError(t, AppendFixedList(source, []int64{10, 11, 12, 13, 14, 15, 16}, nil, mp))
				source.GetNulls().Add(test.nulls...)
				source.SetLength(5) // Retain stale bits outside the logical range.
				destination := NewVec(types.T_int64.ToType(), mp)
				defer destination.Free(mp)
				require.NoError(t, AppendFixed(destination, int64(99), false, mp))
				if preflighted {
					// The preflight API requires explicit flags, so this also
					// checks the unchanged flagged-copy control.
					flags := []uint8{1, 1, 1}
					preflight, err := destination.PreExtendSelectedBatch(source, 2, 3, flags, 4, mp)
					require.NoError(t, err)
					require.NoError(t, destination.UnionBatchPreflighted(source, 2, 3, flags, &preflight, mp))
				} else {
					require.NoError(t, destination.UnionBatch(source, 2, 3, nil, mp))
				}
				require.Equal(t, 4, destination.Length())
				require.Equal(t, int64(99), GetFixedAtNoTypeCheck[int64](destination, 0))
				require.False(t, destination.IsNull(0))
				for row := 1; row < 4; row++ {
					isNull := test.name == "all" || test.name == "sparse" && row == 1
					require.Equal(t, isNull, destination.IsNull(uint64(row)))
					if !isNull {
						require.Equal(t, int64(11+row), GetFixedAtNoTypeCheck[int64](destination, row))
					}
				}
				require.False(t, destination.GetNulls().Contains(5))
			})
		}
	}
}

// TestUnionBatchNullFastPath exercises the UnionBatch full-append fast path with
// nulls, appended into a non-empty target (baseOff != 0 so the
// offset-rebase and null-header-clear interact), cross-checked against per-row
// UnionOne (which handles nulls correctly).
func TestUnionBatchNullFastPath(t *testing.T) {
	mp := mpool.MustNewZero()
	const n = 300
	build := func() *Vector {
		w := NewVec(types.T_varchar.ToType(), mp)
		for i := 0; i < n; i++ {
			var b []byte
			null := false
			switch i % 5 {
			case 0:
				b = []byte(fmt.Sprintf("s%d", i)) // inline
			case 1:
				b = append([]byte(fmt.Sprintf("L%d-", i)), make([]byte, 600)...) // non-inline
			case 2:
				null = true // null
			default:
				b = []byte(fmt.Sprintf("m%d", i))
			}
			require.NoError(t, AppendBytes(w, b, null, mp))
		}
		return w
	}
	w := build()

	// fast path: append all of w into a seeded (non-empty) target.
	v := NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, AppendBytes(v, append([]byte("seed-"), make([]byte, 500)...), false, mp))
	require.NoError(t, v.UnionBatch(w, 0, w.Length(), nil, mp))

	// reference via per-row UnionOne.
	ref := NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, AppendBytes(ref, append([]byte("seed-"), make([]byte, 500)...), false, mp))
	for i := 0; i < w.Length(); i++ {
		require.NoError(t, ref.UnionOne(w, int64(i), mp))
	}

	require.Equal(t, ref.Length(), v.Length())
	for i := 0; i < v.Length(); i++ {
		rn := ref.GetNulls().Contains(uint64(i))
		vn := v.GetNulls().Contains(uint64(i))
		require.Equalf(t, rn, vn, "nsp row %d", i)
		if !rn {
			require.Equalf(t, string(ref.GetBytesAt(i)), string(v.GetBytesAt(i)), "value row %d", i)
		}
	}

	// edge: all-null source.
	{
		aw := NewVec(types.T_varchar.ToType(), mp)
		for i := 0; i < 50; i++ {
			require.NoError(t, AppendBytes(aw, nil, true, mp))
		}
		av := NewVec(types.T_varchar.ToType(), mp)
		require.NoError(t, av.UnionBatch(aw, 0, aw.Length(), nil, mp))
		require.Equal(t, 50, av.Length())
		for i := 0; i < 50; i++ {
			require.True(t, av.GetNulls().Contains(uint64(i)))
		}
		aw.Free(mp)
		av.Free(mp)
	}
	w.Free(mp)
	v.Free(mp)
	ref.Free(mp)
}

// TestUnionBatchFastPathStaleBitmapBits is the regression guard for the fast-path
// bug where w carried nsp bits at index >= w.length — a normal reused state,
// since SetLength shrinks length without clearing the bitmaps. The buggy code
// propagated bits via Foreach (which walks every set bit), so a stale nsp bit
// panicked on the header clear (vCol[oldLen+i] out of range). The fix bounds propagation to [0,cnt), matching
// the per-row UnionOne path which only consults [0,cnt). Pre-fix this test panics
// at UnionBatch; post-fix it matches UnionOne with no phantom bits.
func TestUnionBatchFastPathStaleBitmapBits(t *testing.T) {
	mp := mpool.MustNewZero()
	build := func() *Vector {
		w := NewVec(types.T_varchar.ToType(), mp)
		for i := 0; i < 20; i++ {
			require.NoError(t, AppendBytes(w, []byte(fmt.Sprintf("r%d", i)), false, mp))
		}
		// in-range bits (must propagate) plus stale bits >= the shrunk length
		// (must be ignored, exactly as the per-row path ignores them).
		w.nsp.Add(3, 15, 18)
		w.SetLength(10) // length 10; bits 15,18 are now stale (>= length)
		return w
	}
	w := build()
	require.Equal(t, 10, w.Length())

	// fast path into a non-empty target (baseOff != 0 so the rebase runs too).
	v := NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, AppendBytes(v, []byte("seed"), false, mp))
	require.NoError(t, v.UnionBatch(w, 0, w.Length(), nil, mp)) // panics pre-fix

	// reference: per-row UnionOne, which only consults [0,cnt).
	ref := NewVec(types.T_varchar.ToType(), mp)
	require.NoError(t, AppendBytes(ref, []byte("seed"), false, mp))
	for i := 0; i < w.Length(); i++ {
		require.NoError(t, ref.UnionOne(w, int64(i), mp))
	}

	require.Equal(t, ref.Length(), v.Length())
	for i := 0; i < v.Length(); i++ {
		rn := ref.GetNulls().Contains(uint64(i))
		require.Equalf(t, rn, v.GetNulls().Contains(uint64(i)), "nsp row %d", i)
		if !rn {
			require.Equalf(t, string(ref.GetBytesAt(i)), string(v.GetBytesAt(i)), "value row %d", i)
		}
	}

	// stale source bits must not leak as phantom bits past the appended range
	// (oldLen == 1 for the single seed row).
	for _, stale := range []uint64{15, 18} {
		require.Falsef(t, v.GetNulls().Contains(1+stale), "phantom nsp bit at %d", 1+stale)
	}

	w.Free(mp)
	v.Free(mp)
	ref.Free(mp)
}

// TestUnionPregrowSkipsNullRows guards the varlena area pre-grow against counting
// null rows. A reused varlen vector can retain a stale non-inline header in a null
// slot (a null append does not overwrite the slot). The union skips null rows, so
// the pre-grow must too — otherwise it reserves area for dead payload, triggering a
// large needless mp.Grow (or an alloc failure) on null-heavy inputs the union path
// would mostly skip. Output is identical with/without the fix, so this asserts on
// the reservation: the only copied (non-null) rows are inline, so v.area must stay
// tiny rather than be grown to fit the stale null-slot headers.
func TestUnionPregrowSkipsNullRows(t *testing.T) {
	mp := mpool.MustNewZero()
	const n = 32
	w := NewVec(types.T_varchar.ToType(), mp)
	for i := 0; i < n; i++ {
		require.NoError(t, AppendBytes(w, []byte("x"), false, mp)) // inline: uses no area
	}
	// Plant a stale BIG header claiming a large length in each null slot (as a
	// reused vector would), then mark the row null without clearing the header.
	var wCol []types.Varlena
	ToSliceNoTypeCheck(w, &wCol)
	const staleLen = 1 << 20 // 1 MiB of dead payload per null row
	for i := 1; i < n; i += 2 {
		wCol[i].SetOffsetLen(0, staleLen)
		w.nsp.Add(uint64(i))
	}

	// Union all rows (sels-gather -> the pre-grow path). Pre-fix this reserves
	// ~(n/2)*staleLen of area for the dead null-slot payload.
	v := NewVec(types.T_varchar.ToType(), mp)
	sels := make([]int64, n)
	for i := range sels {
		sels[i] = int64(i)
	}
	require.NoError(t, v.Union(w, sels, mp))

	require.Lessf(t, cap(v.area), staleLen,
		"pre-grow over-reserved area (cap=%d) from stale null-row headers", cap(v.area))

	// correctness is unchanged: odd rows null, even rows carry "x".
	require.Equal(t, n, v.Length())
	for i := 0; i < n; i++ {
		if i%2 == 1 {
			require.Truef(t, v.GetNulls().Contains(uint64(i)), "row %d should be null", i)
		} else {
			require.Falsef(t, v.GetNulls().Contains(uint64(i)), "row %d should not be null", i)
			require.Equalf(t, "x", string(v.GetBytesAt(i)), "row %d value", i)
		}
	}
	w.Free(mp)
	v.Free(mp)
}

func TestPreflightedSelectedVarlenRanges(t *testing.T) {
	for _, kind := range []string{"all", "mixed", "offset", "window", "reordered", "null-dead-payload", "empty"} {
		t.Run(kind, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			src := NewVec(types.T_varchar.ToType(), mp)
			t.Cleanup(func() { src.Free(mp) })
			for i := 0; i < 8; i++ {
				size := 64 + i
				if i == 1 {
					size = types.VarlenaInlineSize
				}
				if i == 2 {
					size = types.VarlenaInlineSize + 1
				}
				if i == 3 {
					size = 0
				}
				if kind == "null-dead-payload" && i == 0 {
					size = 128 * 1024
				}
				value := make([]byte, size)
				for j := range value {
					value[j] = byte('a' + i)
				}
				require.NoError(t, AppendBytes(src, value, i == 6, mp))
			}
			if kind == "null-dead-payload" {
				src.nsp.Add(0)
			}
			if kind == "reordered" {
				col := MustFixedColNoTypeCheck[types.Varlena](src)
				col[2], col[4] = col[4], col[2]
			}
			input := src
			offset, count := 0, 8
			if kind == "offset" || kind == "reordered" {
				offset, count = 1, 5
			}
			if kind == "window" {
				var err error
				input, err = src.Window(2, 6)
				require.NoError(t, err)
				count = 4
				t.Cleanup(func() { input.Free(mp) })
			}
			if kind == "empty" {
				count = 0
			}
			flags := make([]uint8, count)
			for i := range flags {
				flags[i] = 1
			}
			if kind == "mixed" {
				flags[1] = 0
			}
			state := newTestVectorAllocationAccount(t, 64*1024, 32)
			t.Cleanup(func() { finalizeTestVectorAllocationAccount(t, state) })
			dmp := state.view(t, mp)
			dst := NewVec(types.T_varchar.ToType(), dmp)
			t.Cleanup(func() { dst.Free(dmp) })
			require.NoError(t, AppendBytes(dst, []byte("prefix"), false, dmp))
			selected := 0
			liveBytes := 0
			for i, f := range flags {
				if f == 0 {
					continue
				}
				selected++
				if !input.IsNull(uint64(offset+i)) && len(input.GetBytesAt(offset+i)) > types.VarlenaInlineSize {
					liveBytes += len(input.GetBytesAt(offset + i))
				}
			}
			preflight, err := dst.PreExtendSelectedBatch(input, offset, count, flags, 1+selected, dmp)
			require.NoError(t, err)
			admitted := state.account.Snapshot().Used
			require.NoError(t, dst.UnionBatchPreflighted(input, int64(offset), count, flags, &preflight, dmp))
			require.Equal(t, admitted, state.account.Snapshot().Used, "publication cannot allocate")
			require.Len(t, dst.GetArea(), liveBytes, "only admitted live bytes may be copied")
			next := 1
			for i, f := range flags {
				if f == 0 {
					continue
				}
				row := offset + i
				require.Equal(t, input.IsNull(uint64(row)), dst.IsNull(uint64(next)))
				if !input.IsNull(uint64(row)) {
					require.Equal(t, input.GetBytesAt(row), dst.GetBytesAt(next))
				}
				next++
			}
		})
	}
}

func BenchmarkPreflightedVarlenSelection(b *testing.B) {
	for _, stride := range []int{1, 2} {
		b.Run(fmt.Sprintf("select-every-%d", stride), func(b *testing.B) {
			mp := mpool.MustNewZero()
			src := NewVec(types.T_varchar.ToType(), mp)
			dst := NewVec(types.T_varchar.ToType(), mp)
			const rows = 256
			flags := make([]uint8, rows)
			selected := 0
			for i := 0; i < rows; i++ {
				require.NoError(b, AppendBytes(src, []byte(fmt.Sprintf("%0128d", i)), false, mp))
				if i%stride == 0 {
					flags[i] = 1
					selected++
				}
			}
			b.SetBytes(int64(selected * 128))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				preflight, err := dst.PreExtendSelectedBatch(src, 0, rows, flags, selected, mp)
				if err != nil {
					b.Fatal(err)
				}
				if err := dst.UnionBatchPreflighted(src, 0, rows, flags, &preflight, mp); err != nil {
					b.Fatal(err)
				}
				dst.CleanOnlyData()
			}
			b.StopTimer()
			dst.Free(mp)
			src.Free(mp)
			require.Zero(b, mp.CurrNB())
		})
	}
}

func TestConstNullCleanAndUnionBatch(t *testing.T) {
	for _, oid := range []types.T{types.T_any, types.T_int32, types.T_float64, types.T_text} {
		t.Run(oid.String(), func(t *testing.T) {
			mp := mpool.MustNewZero()
			src := NewConstNull(oid.ToType(), 3, mp)
			dst := NewConstNull(oid.ToType(), 3, mp)
			defer func() { src.Free(mp); dst.Free(mp); require.Zero(t, mp.CurrNB()) }()
			for _, length := range []int{3, 1, 0, 4} {
				t.Run(fmt.Sprintf("length_%d", length), func(t *testing.T) {
					src.SetLength(length)
					dst.CleanOnlyData()
					require.Zero(t, dst.Length(), "clean must remove logical rows even without a payload")
					require.NoError(t, dst.UnionBatch(src, 0, length, nil, mp))
					require.Equal(t, length, dst.Length())
					for row := 0; row < dst.Length(); row++ {
						require.True(t, dst.IsNull(uint64(row)))
					}
				})
			}
		})
	}
}

func TestUnionOneVarlenaAllocationFailureFailsClosed(t *testing.T) {
	sourceMP := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, sourceMP.CurrNB()) })
	source := NewVec(types.T_varchar.ToType(), sourceMP)
	t.Cleanup(func() { source.Free(sourceMP) })
	require.NoError(t, AppendBytes(source, make([]byte, 2<<20), false, sourceMP))
	mp, err := mpool.NewMPool(t.Name(), 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	destination := NewVec(types.T_varchar.ToType(), mp)
	t.Cleanup(func() { destination.Free(mp) })
	require.NoError(t, AppendBytes(destination, []byte("prefix"), false, mp))
	require.True(t, destination.VarlenaAreaIsDisjoint())
	require.Error(t, destination.UnionOne(source, 0, mp))
	require.Equal(t, "prefix", destination.GetStringAt(0))
}

func BenchmarkUnionFixedSelection(b *testing.B) {
	for _, oid := range []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_TS, types.T_decimal128, types.T_Blockid, types.T_Rowid, types.T_decimal256} {
		for _, rows := range []int{1, 256} {
			for _, shape := range []string{"flat", "nullable", "constant"} {
				b.Run(fmt.Sprintf("%s/rows=%d/%s", oid, rows, shape), func(b *testing.B) {
					mp := mpool.MustNewZero()
					src, dst := NewVec(oid.ToType(), mp), NewVec(oid.ToType(), mp)
					b.Cleanup(func() { src.Free(mp); dst.Free(mp) })
					require.NoError(b, src.PreExtend(1024, mp))
					src.SetLength(1024)
					for i := range src.data {
						src.data[i] = byte(i)
					}
					sels := make([]int64, rows)
					sels32 := make([]int32, rows)
					for i := range sels {
						sels[i] = int64((i * 17) % 1024)
						sels32[i] = int32(sels[i])
					}
					if shape == "nullable" {
						src.GetNulls().Add(17)
					} else if shape == "constant" {
						src.SetClass(CONSTANT)
					}
					require.NoError(b, dst.Union(src, sels, mp))

					for _, index32 := range []bool{false, true} {
						b.Run(fmt.Sprintf("int32=%t", index32), func(b *testing.B) {
							b.ReportAllocs()
							for range b.N {
								dst.CleanOnlyData()
								var err error
								if index32 {
									err = dst.UnionInt32(src, sels32, mp)
								} else {
									err = dst.Union(src, sels, mp)
								}
								if err != nil {
									b.Fatal(err)
								}
							}
						})
					}
				})
			}
		}
	}
}

func BenchmarkUnionFixedSelectionDensity(b *testing.B) {
	for _, width := range []int{4, 8} {
		for _, dense := range []bool{false, true} {
			shape := "sparse"
			if dense {
				shape = "dense"
			}
			b.Run(fmt.Sprintf("width%d/%s", width, shape), func(b *testing.B) {
				mp := mpool.MustNewZero()
				b.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(b, mp.CurrNB()) })
				typ := types.T_uint32.ToType()
				if width == 8 {
					typ = types.T_uint64.ToType()
				}
				src, dst := NewVec(typ, mp), NewVec(typ, mp)
				b.Cleanup(func() { src.Free(mp); dst.Free(mp) })
				require.NoError(b, src.PreExtend(8192, mp))
				src.SetLength(8192)
				for i := range src.data {
					src.data[i] = byte(i)
				}
				rows := make([]int64, 0, 8192)
				for row := range 8192 {
					if dense && row%20 != 0 || !dense && row%20 == 0 {
						rows = append(rows, int64(row))
					}
				}
				require.NoError(b, dst.Union(src, rows, mp))
				b.SetBytes(int64(len(rows) * width))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					dst.CleanOnlyData()
					if err := dst.Union(src, rows, mp); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
			})
		}
	}
}

func TestUnionAllFixedSizeTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	vec5 := NewVec(types.T_int8.ToType(), mp)
	defer vec5.Free(mp)
	vec6 := NewConstNull(types.T_int8.ToType(), 5, mp)
	defer vec6.Free(mp)

	err := AppendFixed(vec5, int8(1), false, mp)
	require.NoError(t, err)
	err = vec5.UnionMulti(vec6, 1, 2, mp)
	require.NoError(t, err)
	require.False(t, vec5.GetNulls().Contains(0))
	require.True(t, vec5.GetNulls().Contains(1))
	require.True(t, vec5.GetNulls().Contains(2))
	require.False(t, vec5.GetNulls().Contains(3))

	vec7 := NewVec(types.T_char.ToType(), mp)
	defer vec7.Free(mp)
	err = AppendMultiBytes(vec7, nil, true, 2, mp)
	require.NoError(t, err)
	require.True(t, vec7.GetNulls().Contains(0))
	require.True(t, vec7.GetNulls().Contains(1))
	require.False(t, vec7.GetNulls().Contains(2))
	require.Equal(t, 2, vec7.Length())

	fixSizedTypes := []types.Type{
		types.T_int8.ToType(),
		types.T_int16.ToType(),
		types.T_int32.ToType(),
		types.T_int64.ToType(),
		types.T_uint8.ToType(),
		types.T_uint16.ToType(),
		types.T_uint32.ToType(),
		types.T_uint64.ToType(),
		types.T_float32.ToType(),
		types.T_float64.ToType(),
		types.T_bool.ToType(),
		types.T_bit.ToType(),
		types.T_Rowid.ToType(),
		types.T_TS.ToType(),
		types.T_uuid.ToType(),
		types.T_datetime.ToType(),
		types.T_timestamp.ToType(),
	}
	for _, fType := range fixSizedTypes {
		v1 := NewVec(fType, mp)
		v2 := NewVec(fType, mp)
		defer v1.Free(mp)
		defer v2.Free(mp)
		switch fType.Oid {
		case types.T_int8:
			vals := make([]int8, 2)
			err = AppendFixedList[int8](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]int8, 3)
			err = AppendFixedList[int8](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_int16:
			vals := make([]int16, 2)
			err = AppendFixedList[int16](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]int16, 3)
			err = AppendFixedList[int16](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_int32:
			vals := make([]int32, 2)
			err = AppendFixedList[int32](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]int32, 3)
			err = AppendFixedList[int32](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_int64:
			vals := make([]int64, 2)
			err = AppendFixedList[int64](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]int64, 3)
			err = AppendFixedList[int64](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_uint8:
			vals := make([]uint8, 2)
			err = AppendFixedList[uint8](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]uint8, 3)
			err = AppendFixedList[uint8](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_uint16:
			vals := make([]uint16, 2)
			err = AppendFixedList[uint16](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]uint16, 3)
			err = AppendFixedList[uint16](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_uint32:
			vals := make([]uint32, 2)
			err = AppendFixedList[uint32](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]uint32, 3)
			err = AppendFixedList[uint32](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_uint64:
			vals := make([]uint64, 2)
			err = AppendFixedList[uint64](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]uint64, 3)
			err = AppendFixedList[uint64](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_float32:
			vals := make([]float32, 2)
			err = AppendFixedList[float32](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]float32, 3)
			err = AppendFixedList[float32](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_float64:
			vals := make([]float64, 2)
			err = AppendFixedList[float64](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]float64, 3)
			err = AppendFixedList[float64](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_bool:
			vals := make([]bool, 2)
			err = AppendFixedList[bool](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]bool, 3)
			err = AppendFixedList[bool](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_bit:
			vals := make([]uint64, 2)
			err = AppendFixedList[uint64](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]uint64, 3)
			err = AppendFixedList[uint64](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_Rowid:
			vals := make([]types.Rowid, 2)
			err = AppendFixedList[types.Rowid](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]types.Rowid, 3)
			err = AppendFixedList[types.Rowid](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_TS:
			vals := make([]types.TS, 2)
			err = AppendFixedList[types.TS](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]types.TS, 3)
			err = AppendFixedList[types.TS](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_uuid:
			vals := make([]types.Uuid, 2)
			err = AppendFixedList[types.Uuid](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]types.Uuid, 3)
			err = AppendFixedList[types.Uuid](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_datetime:
			vals := make([]types.Datetime, 2)
			err = AppendFixedList[types.Datetime](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]types.Datetime, 3)
			err = AppendFixedList[types.Datetime](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		case types.T_timestamp:
			vals := make([]types.Timestamp, 2)
			err = AppendFixedList[types.Timestamp](v1, vals, []bool{true, true}, mp)
			require.NoError(t, err)
			vals = make([]types.Timestamp, 3)
			err = AppendFixedList[types.Timestamp](v2, vals, []bool{true, true, true}, mp)
			require.NoError(t, err)
		}
		union := GetUnionAllFunction(fType, mp)
		err = union(v1, v2)
		require.NoError(t, err)
		require.Equal(t, 5, v1.Length())
		require.Equal(t, 5, v1.GetNulls().Count())
	}
}

func TestSelectedBatchPreflightProtocol(t *testing.T) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varchar.ToType(), mp)
	destination := NewVec(types.T_varchar.ToType(), mp)
	defer func() {
		destination.Free(mp)
		source.Free(mp)
		require.Zero(t, mp.CurrNB())
	}()

	for _, value := range []string{
		strings.Repeat("a", 64),
		strings.Repeat("b", 80),
		strings.Repeat("c", 96),
	} {
		require.NoError(t, AppendBytes(source, []byte(value), false, mp))
	}
	flags := []uint8{1, 0, 1}
	var preflight SelectedBatchPreflight
	require.ErrorIs(t,
		destination.UnionBatchPreflighted(source, 0, len(flags), flags, &preflight, mp),
		mpool.ErrAllocationAccountInvariant)
	require.ErrorIs(t,
		destination.UnionBatchPreflighted(source, 0, len(flags), flags, nil, mp),
		mpool.ErrAllocationAccountInvariant)

	preflight, err := destination.PreExtendSelectedBatch(
		source, 0, len(flags), flags, 2, mp)
	require.NoError(t, err)
	require.True(t, preflight.Ready())
	admitted := mp.CurrNB()
	require.NoError(t,
		destination.UnionBatchPreflighted(source, 0, len(flags), flags, &preflight, mp))
	require.Equal(t, admitted, mp.CurrNB(),
		"publication must use only capacity admitted by preflight")
	require.Equal(t, 2, destination.Length())
	require.Equal(t, source.GetBytesAt(0), destination.GetBytesAt(0))
	require.Equal(t, source.GetBytesAt(2), destination.GetBytesAt(1))
	require.False(t, preflight.Ready())
	require.ErrorIs(t,
		destination.UnionBatchPreflighted(source, 0, len(flags), flags, &preflight, mp),
		mpool.ErrAllocationAccountInvariant,
		"the preflight is consumed exactly once")

	preflight, err = destination.PreExtendSelectedBatch(
		source, 0, len(flags), []uint8{1, 2, 0}, 2, mp)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	require.False(t, preflight.Ready(),
		"a failed preflight must not return a publishable result")

	destination.ResetWithSameType()
	preflight, err = destination.PreExtendSelectedBatchValidated(
		source, 0, len(flags), flags, 2, mp)
	require.NoError(t, err)
	require.NoError(t,
		destination.UnionBatchPreflighted(source, 0, len(flags), flags, &preflight, mp))
	require.Equal(t, 2, destination.Length())

	constant, err := NewConstBytes(
		types.T_varchar.ToType(), []byte(strings.Repeat("constant", 8)),
		len(flags)+4, mp)
	require.NoError(t, err)
	defer constant.Free(mp)
	destination.ResetWithSameType()
	preflight, err = destination.PreExtendSelectedBatchValidated(
		constant, 2, len(flags), flags, 2, mp)
	require.NoError(t, err)
	require.NoError(t,
		destination.UnionBatchPreflighted(constant, 2, len(flags), flags, &preflight, mp))
	require.Equal(t, 2, destination.Length())
	require.Equal(t, constant.GetBytesAt(0), destination.GetBytesAt(0))
	require.Equal(t, constant.GetBytesAt(0), destination.GetBytesAt(1))

	preflight, err = destination.PreExtendSelectedBatch(
		destination, 0, 2, []uint8{1, 0}, 3, mp)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid,
		"a preflight cannot retain an aliased source for publication")
	require.False(t, preflight.Ready(),
		"a rejected aliased preflight must not return a publishable result")
}

func TestUnionOneAndMultiMixedSources(t *testing.T) {
	for _, copies := range []int{1, 3} {
		t.Run(fmt.Sprintf("copies=%d", copies), func(t *testing.T) {
			mp := mpool.MustNewZero()
			source := NewVec(types.T_varchar.ToType(), mp)
			destination := NewVec(types.T_varchar.ToType(), mp)
			var changed, constant *Vector
			t.Cleanup(func() {
				source.Free(mp)
				changed.Free(mp)
				constant.Free(mp)
				destination.Free(mp)
				require.Zero(t, mp.CurrNB())
			})
			require.NoError(t, AppendBytesList(source, [][]byte{
				[]byte("value"), nil, []byte(strings.Repeat("long", 16)),
			}, []bool{false, true, false}, mp))
			changed, err := source.Dup(mp)
			require.NoError(t, err)
			changed.GetNulls().Add(0)
			constant, err = NewConstBytes(types.T_varchar.ToType(), []byte("constant"), 4, mp)
			require.NoError(t, err)
			rows := []struct {
				vec *Vector
				row int
			}{{source, 1}, {constant, 3}, {source, 2}, {changed, 1},
				{changed, 0}, {changed, 2}, {source, 0}, {source, 1}}
			for range 2 { // Reuse must not retain a previous mixed representation.
				destination.ResetWithSameType()
				for end, input := range rows {
					if copies == 1 {
						require.NoError(t, destination.UnionOne(input.vec, int64(input.row), mp))
					} else {
						require.NoError(t, destination.UnionMulti(input.vec, int64(input.row), copies, mp))
					}
					require.Equal(t, (end+1)*copies, destination.Length())
					for row := range destination.Length() {
						expected := rows[row/copies]
						isNull := expected.vec.IsNull(uint64(expected.row))
						require.Equal(t, isNull, destination.IsNull(uint64(row)))
						if !isNull {
							require.Equal(t, expected.vec.GetBytesAt(expected.row), destination.GetBytesAt(row))
						}
					}
				}
			}
		})
	}
}

func BenchmarkUnionMulti(b *testing.B) {
	for _, oid := range []types.T{types.T_int64, types.T_varchar} {
		for _, copies := range []int{1, 8, 64} {
			b.Run(fmt.Sprintf("%s/copies=%d", oid, copies), func(b *testing.B) {
				mp := mpool.MustNewZero()
				source := NewVec(oid.ToType(), mp)
				defer source.Free(mp)
				destination := NewVec(oid.ToType(), mp)
				defer destination.Free(mp)
				if oid == types.T_int64 {
					require.NoError(b, AppendFixed(source, int64(42), false, mp))
				} else {
					require.NoError(b, AppendBytes(source, []byte("ordinary column value"), false, mp))
				}
				require.NoError(b, destination.PreExtend(copies, mp))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					destination.ResetWithSameType()
					if err := destination.UnionMulti(source, 0, copies, mp); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func TestUnionOneSourceShapes(t *testing.T) {
	for _, scenario := range []string{"source_null", "source_all_null", "null_prefix", "value_prefix", "source_const", "source_const_null", "source_const_null_with_data", "destination_const", "nil_mpool"} {
		t.Run(scenario, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			source := NewVec(types.T_int64.ToType(), mp)
			t.Cleanup(func() { source.Free(mp) })
			destination := NewVec(types.T_int64.ToType(), mp)
			t.Cleanup(func() { destination.Free(mp) })
			require.NoError(t, AppendFixedList(source, []int64{11, 22, 33}, nil, mp))
			require.NoError(t, destination.PreExtend(4, mp))
			offset := 0
			appendMP := mp
			switch scenario {
			case "source_null":
				source.GetNulls().Set(1)
			case "source_all_null":
				for row := range source.Length() {
					source.GetNulls().Set(uint64(row))
				}
			case "null_prefix":
				require.NoError(t, AppendFixed(destination, int64(0), true, mp))
				offset = 1
			case "value_prefix":
				require.NoError(t, AppendFixed(destination, int64(44), false, mp))
				offset = 1
			case "source_const_null":
				source.Free(mp)
				source = NewConstNull(types.T_int64.ToType(), 3, mp)
			case "source_const", "source_const_null_with_data":
				source.Free(mp)
				var err error
				source, err = NewConstFixed(types.T_int64.ToType(), int64(77), 3, mp)
				require.NoError(t, err)
				if scenario == "source_const_null_with_data" {
					source.GetNulls().Set(0)
				}
			case "destination_const":
				destination.SetClass(CONSTANT)
			case "nil_mpool":
				appendMP = nil
			}
			if appendMP == nil {
				// A call must pass the vector's own pool.
				require.Error(t, destination.UnionOne(source, 0, appendMP))
				return
			}
			for row, sel := range []int64{0, 1, 2} {
				require.NoError(t, destination.UnionOne(source, sel, appendMP))
				require.Equal(t, source.IsNull(uint64(sel)), destination.IsNull(uint64(row+offset)))
				if !source.IsNull(uint64(sel)) {
					// Assert physical row bytes as well as constant-class retention.
					physicalSourceRow := int(sel)
					if source.IsConst() {
						physicalSourceRow = 0
					}
					require.Equal(t, source.data[physicalSourceRow*8:(physicalSourceRow+1)*8], destination.data[(row+offset)*8:(row+offset+1)*8])
				}
			}
			require.Equal(t, 3+offset, destination.Length())
			if scenario == "null_prefix" {
				require.True(t, destination.IsNull(0))
			}
			if scenario == "value_prefix" {
				require.False(t, destination.IsNull(0))
				require.Equal(t, int64(44), GetFixedAtWithTypeCheck[int64](destination, 0))
			}
			if scenario == "destination_const" {
				require.True(t, destination.IsConst())
				require.Equal(t, int64(11), GetFixedAtWithTypeCheck[int64](destination, 2))
			}
		})
	}
}

func TestUnionOneFixedGrowthFailureKeepsPrefix(t *testing.T) {
	const poolCap = int64(1 << 20)
	mp, err := mpool.NewMPool(t.Name(), poolCap, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	destination := NewVec(types.T_int64.ToType(), mp)
	t.Cleanup(func() { destination.Free(mp) })
	require.NoError(t, destination.PreExtend(1, mp))
	rows := cap(destination.data) / 8
	for range rows {
		require.NoError(t, AppendFixed(destination, int64(7), false, mp))
	}
	sourceMP := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, sourceMP.CurrNB()) })
	source := NewVec(types.T_int64.ToType(), sourceMP)
	t.Cleanup(func() { source.Free(sourceMP) })
	require.NoError(t, AppendFixed(source, int64(9), false, sourceMP))
	fill, err := mp.Alloc(int(poolCap-mp.CurrNB()), true)
	require.NoError(t, err)
	t.Cleanup(func() { mp.Free(fill) })
	require.Error(t, destination.UnionOne(source, 0, mp))
	require.Equal(t, rows, destination.Length())
	for _, value := range MustFixedColNoTypeCheck[int64](destination) {
		require.Equal(t, int64(7), value)
	}
	mp.Free(fill)
	fill = nil
	require.NoError(t, destination.UnionOne(source, 0, mp))
	require.Equal(t, rows+1, destination.Length())
	require.Equal(t, int64(9), GetFixedAtWithTypeCheck[int64](destination, rows))
}

func BenchmarkUnionOne(b *testing.B) {
	for _, oid := range []types.T{types.T_int64, types.T_varchar} {
		b.Run(oid.String(), func(b *testing.B) {
			const rows = 1024
			mp := mpool.MustNewZero()
			source := NewVec(oid.ToType(), mp)
			destination := NewVec(oid.ToType(), mp)
			defer source.Free(mp)
			defer destination.Free(mp)
			if oid == types.T_int64 {
				require.NoError(b, AppendFixedList(source, make([]int64, rows), nil, mp))
			} else {
				for range rows {
					require.NoError(b, AppendBytes(source, []byte("ordinary column value"), false, mp))
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				destination.ResetWithSameType()
				for row := range rows {
					if err := destination.UnionOne(source, int64(row), mp); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func TestUnionBatchCapacityFailurePreservesDestinationArea(t *testing.T) {
	oldCapLimit := mpool.CapLimit
	mpool.CapLimit = 64 << 10
	t.Cleanup(func() {
		mpool.CapLimit = oldCapLimit
	})

	mp := mpool.MustNewZeroNoFixed()
	destination := NewVec(types.New(types.T_array_float32, 1024, 0), mp)
	source := NewVec(types.New(types.T_array_float32, 1024, 0), mp)
	defer func() {
		destination.Free(mp)
		source.Free(mp)
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()

	value := make([]float32, 1024)
	for range 15 {
		require.NoError(t, AppendArray(destination, value, false, mp))
	}
	require.NoError(t, AppendArray(source, value, false, mp))
	beforeLength := destination.Length()
	beforeArea := bytes.Clone(destination.GetArea())

	err := destination.UnionBatch(source, 0, 1, nil, mp)
	require.ErrorIs(t, err, mpool.ErrAllocationAllocatorLimit)
	require.Equal(t, beforeLength, destination.Length())
	require.Equal(t, beforeArea, destination.GetArea())
}

func BenchmarkUnionBatchFixedRange(b *testing.B) {
	for _, test := range []struct {
		name      string
		nullEvery int
	}{
		{name: "no-null"},
		{name: "sparse", nullEvery: 100},
		{name: "dense", nullEvery: 2},
		{name: "all-null", nullEvery: 1},
	} {
		b.Run(test.name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			source := NewVec(types.T_int64.ToType(), mp)
			defer source.Free(mp)
			for i := 0; i < 1024; i++ {
				isNull := test.nullEvery != 0 && i%test.nullEvery == 0
				require.NoError(b, AppendFixed(source, int64(i), isNull, mp))
			}
			destination := NewVec(types.T_int64.ToType(), mp)
			defer destination.Free(mp)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				destination.ResetWithSameType()
				if err := destination.UnionBatch(source, 0, source.Length(), nil, mp); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnionBatchVarlenInline(b *testing.B) {
	mp := mpool.MustNewZero()
	source := NewVec(types.T_varbinary.ToType(), mp)
	defer source.Free(mp)
	for i := 0; i < 1024; i++ {
		require.NoError(b, AppendBytes(source, []byte("value"), false, mp))
	}
	destination := NewVec(types.T_varbinary.ToType(), mp)
	defer destination.Free(mp)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		destination.ResetWithSameType()
		if err := destination.UnionBatch(source, 0, source.Length(), nil, mp); err != nil {
			b.Fatal(err)
		}
	}
}
