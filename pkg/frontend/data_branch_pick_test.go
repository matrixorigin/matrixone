// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	tree "github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestSegmentBuilder_SingleValue(t *testing.T) {
	pkType := types.T_int32.ToType()
	sb := newSegmentBuilder(pkType)

	v := int32(42)
	sb.observe(types.EncodeInt32(&v))

	segments := sb.finalize()
	require.Len(t, segments, 1)

	zm := index.ZM(segments[0])
	require.True(t, zm.IsInited())
	require.Equal(t, types.T_int32, zm.GetType())
}

func TestSegmentBuilder_ConsecutiveValues_SingleSegment(t *testing.T) {
	// 1, 2, 3, ..., 10 — all close together, should be one segment.
	pkType := types.T_int64.ToType()
	sb := newSegmentBuilder(pkType)

	for i := int64(1); i <= 10; i++ {
		sb.observe(types.EncodeInt64(&i))
	}

	segments := sb.finalize()
	require.Len(t, segments, 1)

	zm := index.ZM(segments[0])
	min := types.DecodeInt64(zm.GetMinBuf())
	max := types.DecodeInt64(zm.GetMaxBuf())
	require.Equal(t, int64(1), min)
	require.Equal(t, int64(10), max)
}

func TestSegmentBuilder_LargeGap_SplitsSegment(t *testing.T) {
	// Values: 1, 2, 3, 4, 5, 1000000, 1000001, 1000002, 1000003, 1000004
	// The huge gap (5 → 1000000) should trigger a split after enough samples.
	pkType := types.T_int64.ToType()
	sb := newSegmentBuilder(pkType)

	// First cluster: 1-5
	for i := int64(1); i <= 5; i++ {
		sb.observe(types.EncodeInt64(&i))
	}
	// Second cluster: 1000000-1000004
	for i := int64(1000000); i <= 1000004; i++ {
		sb.observe(types.EncodeInt64(&i))
	}

	segments := sb.finalize()
	require.Equal(t, 2, len(segments), "expected 2 segments for 2 clusters")

	// Segment 0: [1, 5]
	zm0 := index.ZM(segments[0])
	require.Equal(t, int64(1), types.DecodeInt64(zm0.GetMinBuf()))
	require.Equal(t, int64(5), types.DecodeInt64(zm0.GetMaxBuf()))

	// Segment 1: [1000000, 1000004]
	zm1 := index.ZM(segments[1])
	require.Equal(t, int64(1000000), types.DecodeInt64(zm1.GetMinBuf()))
	require.Equal(t, int64(1000004), types.DecodeInt64(zm1.GetMaxBuf()))
}

func TestSegmentBuilder_StringType_CountBased(t *testing.T) {
	// String types use count-based splitting (maxSegmentSize cap).
	// With only a few values, should be one segment.
	pkType := types.T_varchar.ToType()
	sb := newSegmentBuilder(pkType)

	sb.observe([]byte("apple"))
	sb.observe([]byte("banana"))
	sb.observe([]byte("cherry"))
	sb.observe([]byte("date"))

	segments := sb.finalize()
	require.Len(t, segments, 1)

	zm := index.ZM(segments[0])
	require.Equal(t, types.T_varchar, zm.GetType())
}

func TestBuildSegmentsFromSortedVec_Int32(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	pkType := types.T_int32.ToType()
	vec := vector.NewVec(pkType)
	defer vec.Free(mp)

	// Add values: 10, 20, 30 (already sorted, close together).
	for _, v := range []int32{10, 20, 30} {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}

	segments := buildSegmentsFromSortedVec(vec, pkType)
	require.NotEmpty(t, segments)

	// All values are close → single segment.
	// (minGapSample=4 means gap-based splitting needs at least 4 gaps,
	// but we only have 2 gaps, so no splitting occurs.)
	require.Len(t, segments, 1)
}

func TestBuildSegmentsFromSortedVec_Empty(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	pkType := types.T_int64.ToType()
	vec := vector.NewVec(pkType)
	defer vec.Free(mp)

	segments := buildSegmentsFromSortedVec(vec, pkType)
	require.Nil(t, segments)
}

func TestBuildPKFilterFromVec_ProducesValidFilter(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	pkType := types.T_int64.ToType()
	vec := vector.NewVec(pkType)

	// Two clusters: [1..5] and [1000..1005]
	for i := int64(1); i <= 5; i++ {
		require.NoError(t, vector.AppendFixed(vec, i, false, mp))
	}
	for i := int64(1000); i <= 1005; i++ {
		require.NoError(t, vector.AppendFixed(vec, i, false, mp))
	}

	pkFilter := buildPKFilterFromVec(vec, pkType, 0)
	// vec is NOT freed inside buildPKFilterFromVec — caller is responsible.
	vec.Free(mp)

	require.NotNil(t, pkFilter)
	require.NotEmpty(t, pkFilter.Segments)
	require.Equal(t, 0, pkFilter.PrimarySeqnum)
}

func TestBuildPKFilterFromVec_NilVec(t *testing.T) {
	pkFilter := buildPKFilterFromVec(nil, types.T_int32.ToType(), 0)
	require.Nil(t, pkFilter)
}

func TestNumericGap_Int64(t *testing.T) {
	pkType := types.T_int64.ToType()
	a := int64(10)
	b := int64(50)
	gap := numericGap(types.EncodeInt64(&a), types.EncodeInt64(&b), pkType)
	require.InDelta(t, 40.0, gap, 0.001)
}

func TestNumericGap_Uint32(t *testing.T) {
	pkType := types.T_uint32.ToType()
	a := uint32(100)
	b := uint32(300)
	gap := numericGap(types.EncodeUint32(&a), types.EncodeUint32(&b), pkType)
	require.InDelta(t, 200.0, gap, 0.001)
}

func TestNumericGap_AllTypes(t *testing.T) {
	tests := []struct {
		name   string
		oid    types.T
		encode func() ([]byte, []byte)
		expect float64
	}{
		{"int8", types.T_int8, func() ([]byte, []byte) {
			a, b := int8(10), int8(30)
			return types.EncodeInt8(&a), types.EncodeInt8(&b)
		}, 20.0},
		{"int16", types.T_int16, func() ([]byte, []byte) {
			a, b := int16(100), int16(500)
			return types.EncodeInt16(&a), types.EncodeInt16(&b)
		}, 400.0},
		{"int32", types.T_int32, func() ([]byte, []byte) {
			a, b := int32(10), int32(60)
			return types.EncodeInt32(&a), types.EncodeInt32(&b)
		}, 50.0},
		{"uint8", types.T_uint8, func() ([]byte, []byte) {
			a, b := uint8(5), uint8(55)
			return types.EncodeUint8(&a), types.EncodeUint8(&b)
		}, 50.0},
		{"uint8_reverse", types.T_uint8, func() ([]byte, []byte) {
			a, b := uint8(55), uint8(5)
			return types.EncodeUint8(&a), types.EncodeUint8(&b)
		}, 50.0},
		{"uint16", types.T_uint16, func() ([]byte, []byte) {
			a, b := uint16(100), uint16(600)
			return types.EncodeUint16(&a), types.EncodeUint16(&b)
		}, 500.0},
		{"uint16_reverse", types.T_uint16, func() ([]byte, []byte) {
			a, b := uint16(600), uint16(100)
			return types.EncodeUint16(&a), types.EncodeUint16(&b)
		}, 500.0},
		{"uint64", types.T_uint64, func() ([]byte, []byte) {
			a, b := uint64(1000), uint64(2000)
			return types.EncodeUint64(&a), types.EncodeUint64(&b)
		}, 1000.0},
		{"uint64_reverse", types.T_uint64, func() ([]byte, []byte) {
			a, b := uint64(2000), uint64(1000)
			return types.EncodeUint64(&a), types.EncodeUint64(&b)
		}, 1000.0},
		{"float32", types.T_float32, func() ([]byte, []byte) {
			a, b := float32(1.5), float32(3.5)
			return types.EncodeFloat32(&a), types.EncodeFloat32(&b)
		}, 2.0},
		{"float64", types.T_float64, func() ([]byte, []byte) {
			a, b := float64(10.0), float64(25.0)
			return types.EncodeFloat64(&a), types.EncodeFloat64(&b)
		}, 15.0},
		{"varchar_returns_zero", types.T_varchar, func() ([]byte, []byte) {
			return []byte("aaa"), []byte("zzz")
		}, 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pkType := tt.oid.ToType()
			a, b := tt.encode()
			gap := numericGap(a, b, pkType)
			require.InDelta(t, tt.expect, gap, 0.01)
		})
	}
}

func TestIsNumericType(t *testing.T) {
	numeric := []types.T{
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
	}
	for _, oid := range numeric {
		require.True(t, isNumericType(oid), "expected numeric: %s", oid)
	}
	nonNumeric := []types.T{types.T_varchar, types.T_char, types.T_date, types.T_bool}
	for _, oid := range nonNumeric {
		require.False(t, isNumericType(oid), "expected non-numeric: %s", oid)
	}
}

func TestUnescapeMySQLString(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect string
	}{
		{"no_escapes", "hello world", "hello world"},
		{"backslash_n", `hello\nworld`, "hello\nworld"},
		{"backslash_t", `col\tval`, "col\tval"},
		{"backslash_r", `line\rend`, "line\rend"},
		{"backslash_0", `null\0byte`, "null\x00byte"},
		{"backslash_b", `back\bspace`, "back\bspace"},
		{"backslash_Z", `ctrl\Zchar`, "ctrl\x1achar"},
		{"backslash_backslash", `path\\dir`, `path\dir`},
		{"backslash_quote", `it\'s`, `it's`},
		{"double_single_quote", "it''s", "it's"},
		{"mixed", `a\nb''c\\d`, "a\nb'c\\d"},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expect, unescapeMySQLString(tt.input))
		})
	}
}

func TestAppendNumericStringToVec_AllTypes(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	tests := []struct {
		name   string
		oid    types.T
		input  string
		verify func(*vector.Vector)
	}{
		{"int8", types.T_int8, "42", func(v *vector.Vector) {
			require.Equal(t, int8(42), vector.GetFixedAtNoTypeCheck[int8](v, 0))
		}},
		{"int16", types.T_int16, "-100", func(v *vector.Vector) {
			require.Equal(t, int16(-100), vector.GetFixedAtNoTypeCheck[int16](v, 0))
		}},
		{"int32", types.T_int32, "999", func(v *vector.Vector) {
			require.Equal(t, int32(999), vector.GetFixedAtNoTypeCheck[int32](v, 0))
		}},
		{"int64", types.T_int64, "123456789", func(v *vector.Vector) {
			require.Equal(t, int64(123456789), vector.GetFixedAtNoTypeCheck[int64](v, 0))
		}},
		{"uint8", types.T_uint8, "200", func(v *vector.Vector) {
			require.Equal(t, uint8(200), vector.GetFixedAtNoTypeCheck[uint8](v, 0))
		}},
		{"uint16", types.T_uint16, "60000", func(v *vector.Vector) {
			require.Equal(t, uint16(60000), vector.GetFixedAtNoTypeCheck[uint16](v, 0))
		}},
		{"uint32", types.T_uint32, "4000000", func(v *vector.Vector) {
			require.Equal(t, uint32(4000000), vector.GetFixedAtNoTypeCheck[uint32](v, 0))
		}},
		{"uint64", types.T_uint64, "18000000000", func(v *vector.Vector) {
			require.Equal(t, uint64(18000000000), vector.GetFixedAtNoTypeCheck[uint64](v, 0))
		}},
		{"float32", types.T_float32, "3.14", func(v *vector.Vector) {
			require.InDelta(t, float32(3.14), vector.GetFixedAtNoTypeCheck[float32](v, 0), 0.001)
		}},
		{"float64", types.T_float64, "2.71828", func(v *vector.Vector) {
			require.InDelta(t, 2.71828, vector.GetFixedAtNoTypeCheck[float64](v, 0), 0.00001)
		}},
		{"varchar", types.T_varchar, "hello", func(v *vector.Vector) {
			require.Equal(t, []byte("hello"), v.GetBytesAt(0))
		}},
		{"char", types.T_char, "world", func(v *vector.Vector) {
			require.Equal(t, []byte("world"), v.GetBytesAt(0))
		}},
		{"text", types.T_text, "long text", func(v *vector.Vector) {
			require.Equal(t, []byte("long text"), v.GetBytesAt(0))
		}},
		{"blob", types.T_blob, "binary", func(v *vector.Vector) {
			require.Equal(t, []byte("binary"), v.GetBytesAt(0))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pkType := tt.oid.ToType()
			vec := vector.NewVec(pkType)
			defer vec.Free(mp)

			err := appendNumericStringToVec(vec, tt.input, pkType, mp)
			require.NoError(t, err)
			require.Equal(t, 1, vec.Length())
			tt.verify(vec)
		})
	}
}

func TestAppendNumericStringToVec_UnsupportedType(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	pkType := types.T_decimal128.ToType()
	vec := vector.NewVec(pkType)
	defer vec.Free(mp)

	err = appendNumericStringToVec(vec, "123", pkType, mp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported PK type")
}

func TestAppendNumericStringToVec_ParseError(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	pkType := types.T_int32.ToType()
	vec := vector.NewVec(pkType)
	defer vec.Free(mp)

	err = appendNumericStringToVec(vec, "not_a_number", pkType, mp)
	require.Error(t, err)
}

func TestAppendExprToVec_StrVal(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	pkType := types.T_varchar.ToType()
	vec := vector.NewVec(pkType)
	defer vec.Free(mp)

	expr := tree.NewStrVal("test_value")
	err = appendExprToVec(vec, expr, pkType, mp)
	require.NoError(t, err)
	require.Equal(t, 1, vec.Length())
	require.Equal(t, []byte("test_value"), vec.GetBytesAt(0))
}

func TestAppendExprToVec_UnaryMinus(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	pkType := types.T_int32.ToType()
	vec := vector.NewVec(pkType)
	defer vec.Free(mp)

	num := tree.NewNumVal[int64](42, "42", false, tree.P_int64)
	expr := tree.NewUnaryExpr(tree.UNARY_MINUS, num)
	err = appendExprToVec(vec, expr, pkType, mp)
	require.NoError(t, err)
	require.Equal(t, 1, vec.Length())
	require.Equal(t, int32(-42), vector.GetFixedAtNoTypeCheck[int32](vec, 0))
}

func TestAppendExprToVec_UnaryPlus(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	pkType := types.T_int64.ToType()
	vec := vector.NewVec(pkType)
	defer vec.Free(mp)

	num := tree.NewNumVal[int64](99, "99", false, tree.P_int64)
	expr := tree.NewUnaryExpr(tree.UNARY_PLUS, num)
	err = appendExprToVec(vec, expr, pkType, mp)
	require.NoError(t, err)
	require.Equal(t, int64(99), vector.GetFixedAtNoTypeCheck[int64](vec, 0))
}

func TestAppendExprToVec_UnsupportedExpr(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	pkType := types.T_int32.ToType()
	vec := vector.NewVec(pkType)
	defer vec.Free(mp)

	expr := &tree.UnresolvedName{}
	err = appendExprToVec(vec, expr, pkType, mp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported expression type")
}

func TestSegmentBuilder_HardCapSplit(t *testing.T) {
	// Verify that maxSegmentSize (8192) triggers a split even for
	// consecutive values.
	pkType := types.T_int64.ToType()
	sb := newSegmentBuilder(pkType)

	for i := int64(0); i < int64(maxSegmentSize+100); i++ {
		sb.observe(types.EncodeInt64(&i))
	}

	segments := sb.finalize()
	require.Equal(t, 2, len(segments), "expected 2 segments after exceeding maxSegmentSize")

	zm0 := index.ZM(segments[0])
	require.Equal(t, int64(0), types.DecodeInt64(zm0.GetMinBuf()))
	require.Equal(t, int64(maxSegmentSize-1), types.DecodeInt64(zm0.GetMaxBuf()))

	zm1 := index.ZM(segments[1])
	require.Equal(t, int64(maxSegmentSize), types.DecodeInt64(zm1.GetMinBuf()))
}

func TestSegmentBuilder_FlushEmpty(t *testing.T) {
	pkType := types.T_int32.ToType()
	sb := newSegmentBuilder(pkType)
	// Finalize without any observe calls.
	segments := sb.finalize()
	require.Nil(t, segments)
}
