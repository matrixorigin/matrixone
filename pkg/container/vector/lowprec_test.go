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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// Low-precision float columns share storage width with uint8/uint16 but must
// be read through their own element types; the type-checked accessors reject
// the raw integer view. Const-set and union of a constant source exercise both
// paths.
func TestLowPrecisionConstSetAndUnion(t *testing.T) {
	for _, oid := range []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4, types.T_uint8, types.T_uint16} {
		t.Run(oid.String(), func(t *testing.T) {
			mp := mpool.MustNewZero()
			typ := oid.ToType()
			src := NewVec(typ, mp)
			raw := make([]byte, typ.TypeSize()*2)
			raw[0], raw[typ.TypeSize()] = 0x3f, 0x40
			require.NoError(t, src.PreExtend(2, mp))
			src.SetLength(2)
			copy(src.GetData(), raw)
			src.SetNull(0)

			dst := NewVec(typ, mp)
			set := GetConstSetFunction(typ, mp)
			require.NoError(t, set(dst, src, 1, 3))
			require.True(t, dst.IsConst())
			require.Equal(t, 3, dst.Length())
			require.Equal(t, src.GetRawBytesAt(1), dst.GetRawBytesAt(0))
			require.NoError(t, set(dst, src, 0, 2))
			require.True(t, dst.IsConstNull())

			value := NewVec(typ, mp)
			require.NoError(t, set(value, src, 1, 2))
			union := NewVec(typ, mp)
			require.NoError(t, GetUnionAllFunction(typ, mp)(union, value))
			require.Equal(t, 2, union.Length())
			require.Equal(t, src.GetRawBytesAt(1), union.GetRawBytesAt(1))

			for _, v := range []*Vector{src, dst, value, union} {
				v.Free(mp)
			}
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestGetLowPrecisionFloatAt(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, oid := range []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4} {
		vec := NewVec(oid.ToType(), mp)
		var err error
		switch oid {
		case types.T_bf16:
			err = AppendFixed(vec, types.BF16FromFloat32(-0.5), false, mp)
		case types.T_float16:
			err = AppendFixed(vec, types.Float16FromFloat32(-0.5), false, mp)
		case types.T_float8:
			err = AppendFixed(vec, types.Float8FromFloat32(-0.5), false, mp)
		case types.T_float4:
			err = AppendFixed(vec, types.Float4FromFloat32(-0.5), false, mp)
		}
		require.NoError(t, err)
		f, ok := GetLowPrecisionFloatAt(vec, 0)
		require.True(t, ok)
		require.Equal(t, float32(-0.5), f, oid.String())
		vec.Free(mp)
	}
	vec := NewVec(types.T_float32.ToType(), mp)
	require.NoError(t, AppendFixed(vec, float32(1), false, mp))
	_, ok := GetLowPrecisionFloatAt(vec, 0)
	require.False(t, ok)
	vec.Free(mp)
	require.Zero(t, mp.CurrNB())
}

// TestLowPrecisionFloatMarshalRoundTrip checks that bf16, float16, float8 and float4
// vectors decode after MarshalBinary; the decoder's type allowlist gates WAL replay and
// batches sent between services.
func TestLowPrecisionFloatMarshalRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, oid := range []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4} {
		vec := NewVec(oid.ToType(), mp)
		for i, v := range []float32{1.5, -0.5, 0} {
			var err error
			switch oid {
			case types.T_bf16:
				err = AppendFixed(vec, types.BF16FromFloat32(v), i == 2, mp)
			case types.T_float16:
				err = AppendFixed(vec, types.Float16FromFloat32(v), i == 2, mp)
			case types.T_float8:
				err = AppendFixed(vec, types.Float8FromFloat32(v), i == 2, mp)
			case types.T_float4:
				err = AppendFixed(vec, types.Float4FromFloat32(v), i == 2, mp)
			}
			require.NoError(t, err)
		}
		data, err := vec.MarshalBinary()
		require.NoError(t, err)
		got := NewVec(oid.ToType(), mp)
		require.NoError(t, got.UnmarshalBinary(data), oid.String())
		require.Equal(t, 3, got.Length())
		f, _ := GetLowPrecisionFloatAt(got, 1)
		require.Equal(t, float32(-0.5), f, oid.String())
		require.True(t, got.IsNull(2))
		got.Free(mp)
		vec.Free(mp)
	}
	require.Zero(t, mp.CurrNB())
}

func testLowPrecisionFloatRowStrings[T types.LowPrecFloat](t *testing.T, oid types.T, stored []T, want []float32) {
	t.Helper()
	t.Run(oid.String(), func(t *testing.T) {
		mp := mpool.MustNewZero()
		t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
		vec := NewVec(oid.ToType(), mp)
		t.Cleanup(func() { vec.Free(mp) })
		require.NoError(t, AppendFixedList(vec, stored, nil, mp))
		var zero T
		require.NoError(t, AppendFixed(vec, zero, true, mp))
		for i, value := range stored {
			text := fmt.Sprintf("%v", want[i])
			require.Equal(t, text, vec.RowToString(i))
			constant, err := NewConstFixed(oid.ToType(), value, 10, mp)
			require.NoError(t, err)
			t.Cleanup(func() { constant.Free(mp) })
			require.Equal(t, text, constant.RowToString(7))
		}
		require.Equal(t, "null", vec.RowToString(len(stored)))
		null := NewConstNull(oid.ToType(), 10, mp)
		t.Cleanup(func() { null.Free(mp) })
		require.Equal(t, "null", null.RowToString(7))
	})
}

// lowPrecValueOrderingCase exercises the container ordering/aggregation paths for one
// low-precision float type, asserting they order/aggregate by FLOAT VALUE, not raw bits.
func lowPrecValueOrderingCase[T types.LowPrecFloat](
	t *testing.T, oid types.T, from func(float32) T,
) {
	mp := mpool.MustNew(t.Name())

	// Values exactly representable in ALL four formats (float4 magnitudes are
	// {0,.5,1,1.5,2,3,4,6}). Negatives are the discriminator: a negative float's raw
	// uint bits exceed a positive's, so a bit-wise sort would reverse them.
	vals := []float32{2.0, -1.0, -3.0, 0.5}
	wantSorted := []float32{-3.0, -1.0, 0.5, 2.0}

	build := func(fs []float32) *Vector {
		v := NewVec(oid.ToType(), mp)
		for _, f := range fs {
			require.NoError(t, AppendFixed(v, from(f), false, mp))
		}
		return v
	}
	toF := func(v *Vector) []float32 {
		col := MustFixedColNoTypeCheck[T](v)
		out := make([]float32, len(col))
		for i := range col {
			out[i] = col[i].ToFloat32()
		}
		return out
	}

	// InplaceSort: value order (negatives first), no compaction.
	v := build(vals)
	require.True(t, supportsInplaceSort(oid))
	require.NoError(t, v.InplaceSort(mp))
	require.Equal(t, wantSorted, toF(v), "InplaceSort")
	require.True(t, v.GetSorted())
	v.Free(mp)

	// compareVectorRows: -3.0 (row 2) < 2.0 (row 0); a raw-bit compare would say the
	// negative is larger.
	v = build(vals)
	require.Less(t, compareVectorRows(v, 2, 0), 0, "compareVectorRows neg<pos")
	require.Greater(t, compareVectorRows(v, 0, 1), 0, "compareVectorRows 2.0>-1.0")
	v.Free(mp)

	// InplaceSortAndCompact: dedup by value (+0/-0 collapse; here plain dups).
	v = build([]float32{1.0, -1.0, 1.0, -1.0})
	require.NoError(t, v.InplaceSortAndCompact(mp))
	require.Equal(t, []float32{-1.0, 1.0}, toF(v), "InplaceSortAndCompact")
	require.True(t, v.GetSorted())
	v.Free(mp)

	// GetSumValue: widened float64 sum = 2-1-3+0.5 = -1.5.
	v = build(vals)
	ok, sumBytes := v.GetSumValue()
	require.True(t, ok)
	require.InDelta(t, -1.5, types.DecodeFloat64(sumBytes), 1e-6, "GetSumValue")
	v.Free(mp)

	// GetMinMaxValue: by value, min=-3.0 max=2.0.
	v = build(vals)
	ok, minBytes, maxBytes := v.GetMinMaxValue()
	require.True(t, ok)
	require.Equal(t, float32(-3.0), types.DecodeFixed[T](minBytes).ToFloat32(), "GetMinMaxValue min")
	require.Equal(t, float32(2.0), types.DecodeFixed[T](maxBytes).ToFloat32(), "GetMinMaxValue max")
	v.Free(mp)
}

// TestLowPrecFloatContainerValueOrdering covers the Layer-4 container paths for the
// scalar low-precision float types (#20567): they must order/aggregate by float value
// (via ToFloat32), never by the raw uint bits, whose sign bit inverts float order.
func TestLowPrecFloatContainerValueOrdering(t *testing.T) {
	t.Run("bf16", func(t *testing.T) {
		lowPrecValueOrderingCase(t, types.T_bf16, types.BF16FromFloat32)
	})
	t.Run("float16", func(t *testing.T) {
		lowPrecValueOrderingCase(t, types.T_float16, types.Float16FromFloat32)
	})
	t.Run("float8", func(t *testing.T) {
		lowPrecValueOrderingCase(t, types.T_float8, types.Float8FromFloat32)
	})
	t.Run("float4", func(t *testing.T) {
		lowPrecValueOrderingCase(t, types.T_float4, types.Float4FromFloat32)
	})
}

// TestLowPrecFloatGetMinMaxSkipsNaN verifies NaN never poisons the zonemap bounds
// (bf16/float16/float8 have a NaN encoding; float4 has none).
func TestLowPrecFloatGetMinMaxSkipsNaN(t *testing.T) {
	mp := mpool.MustNew(t.Name())
	nan := float32(math.NaN())

	// bf16 with a NaN mixed in: min/max come from the finite values only.
	v := NewVec(types.T_bf16.ToType(), mp)
	for _, f := range []float32{nan, -2.0, 4.0, nan} {
		require.NoError(t, AppendFixed(v, types.BF16FromFloat32(f), false, mp))
	}
	ok, minB, maxB := v.GetMinMaxValue()
	require.True(t, ok)
	require.Equal(t, float32(-2.0), types.DecodeFixed[types.BF16](minB).ToFloat32())
	require.Equal(t, float32(4.0), types.DecodeFixed[types.BF16](maxB).ToFloat32())
	v.Free(mp)

	// All-NaN bf16: no comparable value → ok=false, zonemap left uninitialized.
	v = NewVec(types.T_bf16.ToType(), mp)
	for i := 0; i < 3; i++ {
		require.NoError(t, AppendFixed(v, types.BF16FromFloat32(nan), false, mp))
	}
	ok, _, _ = v.GetMinMaxValue()
	require.False(t, ok)
	v.Free(mp)
}

// TestLowPrecFloatAccessMovement exercises the access/movement/string switches for the
// scalar low-precision float types (bf16/float16/float8/float4) so each per-type arm is
// covered: GetAny/AppendAny round-trip, ShrinkByRows, ShrinkByMask, Shuffle, ShuffleWithBuf,
// String, RowToString, and NewFunctionResultWrapper (#20567).
func TestLowPrecFloatAccessMovement(t *testing.T) {
	mp := mpool.MustNew(t.Name())

	cases := []struct {
		oid types.T
		mk  func(float32) any
	}{
		{types.T_bf16, func(f float32) any { return types.BF16FromFloat32(f) }},
		{types.T_float16, func(f float32) any { return types.Float16FromFloat32(f) }},
		{types.T_float8, func(f float32) any { return types.Float8FromFloat32(f) }},
		{types.T_float4, func(f float32) any { return types.Float4FromFloat32(f) }},
	}

	for _, c := range cases {
		t.Run(c.oid.String(), func(t *testing.T) {
			// AppendAny builds the column; GetAny reads it back.
			v := NewVec(c.oid.ToType(), mp)
			for _, f := range []float32{1.0, -2.0, 0.5, 4.0} {
				require.NoError(t, AppendAny(v, c.mk(f), false, mp))
			}
			require.Equal(t, c.mk(1.0), GetAny(v, 0, true))
			require.Equal(t, c.mk(-2.0), GetAny(v, 1, true))

			// String and RowToString must not panic and render the row.
			_ = v.String()
			require.NotEmpty(t, v.RowToString(0))

			// NewFunctionResultWrapper builds a result vector for the type.
			rw := NewFunctionResultWrapper(c.oid.ToType(), mp)
			require.NotNil(t, rw)
			rw.Free()

			// Shuffle reorders in place.
			require.NoError(t, v.Shuffle([]int64{3, 2, 1, 0}, mp))
			require.Equal(t, c.mk(4.0), GetAny(v, 0, true))
			v.Free(mp)

			// ShuffleWithBuf.
			v2 := NewVec(c.oid.ToType(), mp)
			for _, f := range []float32{1.0, 2.0, 3.0} {
				require.NoError(t, AppendAny(v2, c.mk(f), false, mp))
			}
			var buf []byte
			require.NoError(t, v2.ShuffleWithBuf([]int64{2, 1, 0}, mp, &buf))
			require.Equal(t, c.mk(3.0), GetAny(v2, 0, true))
			v2.Free(mp)

			// ShrinkByRows keeps selected rows.
			v3 := NewVec(c.oid.ToType(), mp)
			for _, f := range []float32{1.0, 2.0, 3.0, 4.0} {
				require.NoError(t, AppendAny(v3, c.mk(f), false, mp))
			}
			require.NoError(t, v3.ShrinkByRows([]int64{0, 2}, mp))
			require.Equal(t, 2, v3.Length())
			require.Equal(t, c.mk(1.0), GetAny(v3, 0, true))
			require.Equal(t, c.mk(3.0), GetAny(v3, 1, true))
			v3.Free(mp)

			// ShrinkByMask.
			v4 := NewVec(c.oid.ToType(), mp)
			for _, f := range []float32{1.0, 2.0, 3.0, 4.0} {
				require.NoError(t, AppendAny(v4, c.mk(f), false, mp))
			}
			var bm bitmap.Bitmap
			require.NoError(t, bm.InitWithSize(4, mpool.MustNewZero()))
			bm.Add(1)
			bm.Add(3)
			require.NoError(t, v4.ShrinkByMask(&bm, false, 0, mp))
			require.Equal(t, 2, v4.Length())
			require.Equal(t, c.mk(2.0), GetAny(v4, 0, true))
			v4.Free(mp)
		})
	}
}
