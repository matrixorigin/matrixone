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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	tree "github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
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
		pkType types.Type // if Oid==0, uses oid.ToType()
		encode func() ([]byte, []byte)
		expect float64
	}{
		{"int8", types.T_int8, types.Type{}, func() ([]byte, []byte) {
			a, b := int8(10), int8(30)
			return types.EncodeInt8(&a), types.EncodeInt8(&b)
		}, 20.0},
		{"int16", types.T_int16, types.Type{}, func() ([]byte, []byte) {
			a, b := int16(100), int16(500)
			return types.EncodeInt16(&a), types.EncodeInt16(&b)
		}, 400.0},
		{"int32", types.T_int32, types.Type{}, func() ([]byte, []byte) {
			a, b := int32(10), int32(60)
			return types.EncodeInt32(&a), types.EncodeInt32(&b)
		}, 50.0},
		{"uint8", types.T_uint8, types.Type{}, func() ([]byte, []byte) {
			a, b := uint8(5), uint8(55)
			return types.EncodeUint8(&a), types.EncodeUint8(&b)
		}, 50.0},
		{"uint8_reverse", types.T_uint8, types.Type{}, func() ([]byte, []byte) {
			a, b := uint8(55), uint8(5)
			return types.EncodeUint8(&a), types.EncodeUint8(&b)
		}, 50.0},
		{"uint16", types.T_uint16, types.Type{}, func() ([]byte, []byte) {
			a, b := uint16(100), uint16(600)
			return types.EncodeUint16(&a), types.EncodeUint16(&b)
		}, 500.0},
		{"uint16_reverse", types.T_uint16, types.Type{}, func() ([]byte, []byte) {
			a, b := uint16(600), uint16(100)
			return types.EncodeUint16(&a), types.EncodeUint16(&b)
		}, 500.0},
		{"uint64", types.T_uint64, types.Type{}, func() ([]byte, []byte) {
			a, b := uint64(1000), uint64(2000)
			return types.EncodeUint64(&a), types.EncodeUint64(&b)
		}, 1000.0},
		{"uint64_reverse", types.T_uint64, types.Type{}, func() ([]byte, []byte) {
			a, b := uint64(2000), uint64(1000)
			return types.EncodeUint64(&a), types.EncodeUint64(&b)
		}, 1000.0},
		{"float32", types.T_float32, types.Type{}, func() ([]byte, []byte) {
			a, b := float32(1.5), float32(3.5)
			return types.EncodeFloat32(&a), types.EncodeFloat32(&b)
		}, 2.0},
		{"float64", types.T_float64, types.Type{}, func() ([]byte, []byte) {
			a, b := float64(10.0), float64(25.0)
			return types.EncodeFloat64(&a), types.EncodeFloat64(&b)
		}, 15.0},
		{"decimal64", types.T_decimal64, types.New(types.T_decimal64, 10, 2), func() ([]byte, []byte) {
			a, _ := types.ParseDecimal64("10.50", 10, 2)
			b, _ := types.ParseDecimal64("20.50", 10, 2)
			return types.EncodeDecimal64(&a), types.EncodeDecimal64(&b)
		}, 10.0},
		{"decimal128", types.T_decimal128, types.New(types.T_decimal128, 20, 2), func() ([]byte, []byte) {
			a, _ := types.ParseDecimal128("100.25", 20, 2)
			b, _ := types.ParseDecimal128("200.75", 20, 2)
			return types.EncodeDecimal128(&a), types.EncodeDecimal128(&b)
		}, 100.50},
		{"varchar_returns_zero", types.T_varchar, types.Type{}, func() ([]byte, []byte) {
			return []byte("aaa"), []byte("zzz")
		}, 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pkType := tt.pkType
			if pkType.Oid == 0 {
				pkType = tt.oid.ToType()
			}
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
		types.T_decimal64, types.T_decimal128,
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
		name    string
		pkType  types.Type // explicit type (if Oid==0, uses oid.ToType())
		oid     types.T
		input   string
		verify  func(*vector.Vector)
	}{
		{"int8", types.Type{}, types.T_int8, "42", func(v *vector.Vector) {
			require.Equal(t, int8(42), vector.GetFixedAtNoTypeCheck[int8](v, 0))
		}},
		{"int16", types.Type{}, types.T_int16, "-100", func(v *vector.Vector) {
			require.Equal(t, int16(-100), vector.GetFixedAtNoTypeCheck[int16](v, 0))
		}},
		{"int32", types.Type{}, types.T_int32, "999", func(v *vector.Vector) {
			require.Equal(t, int32(999), vector.GetFixedAtNoTypeCheck[int32](v, 0))
		}},
		{"int64", types.Type{}, types.T_int64, "123456789", func(v *vector.Vector) {
			require.Equal(t, int64(123456789), vector.GetFixedAtNoTypeCheck[int64](v, 0))
		}},
		{"uint8", types.Type{}, types.T_uint8, "200", func(v *vector.Vector) {
			require.Equal(t, uint8(200), vector.GetFixedAtNoTypeCheck[uint8](v, 0))
		}},
		{"uint16", types.Type{}, types.T_uint16, "60000", func(v *vector.Vector) {
			require.Equal(t, uint16(60000), vector.GetFixedAtNoTypeCheck[uint16](v, 0))
		}},
		{"uint32", types.Type{}, types.T_uint32, "4000000", func(v *vector.Vector) {
			require.Equal(t, uint32(4000000), vector.GetFixedAtNoTypeCheck[uint32](v, 0))
		}},
		{"uint64", types.Type{}, types.T_uint64, "18000000000", func(v *vector.Vector) {
			require.Equal(t, uint64(18000000000), vector.GetFixedAtNoTypeCheck[uint64](v, 0))
		}},
		{"float32", types.Type{}, types.T_float32, "3.14", func(v *vector.Vector) {
			require.InDelta(t, float32(3.14), vector.GetFixedAtNoTypeCheck[float32](v, 0), 0.001)
		}},
		{"float64", types.Type{}, types.T_float64, "2.71828", func(v *vector.Vector) {
			require.InDelta(t, 2.71828, vector.GetFixedAtNoTypeCheck[float64](v, 0), 0.00001)
		}},
		{"decimal64", types.New(types.T_decimal64, 10, 2), types.T_decimal64, "12.34", func(v *vector.Vector) {
			expected, _ := types.ParseDecimal64("12.34", 10, 2)
			require.Equal(t, expected, vector.MustFixedColNoTypeCheck[types.Decimal64](v)[0])
		}},
		{"decimal128", types.New(types.T_decimal128, 20, 2), types.T_decimal128, "56789.12", func(v *vector.Vector) {
			expected, _ := types.ParseDecimal128("56789.12", 20, 2)
			require.Equal(t, expected, vector.MustFixedColNoTypeCheck[types.Decimal128](v)[0])
		}},
		{"varchar", types.Type{}, types.T_varchar, "hello", func(v *vector.Vector) {
			require.Equal(t, []byte("hello"), v.GetBytesAt(0))
		}},
		{"char", types.Type{}, types.T_char, "world", func(v *vector.Vector) {
			require.Equal(t, []byte("world"), v.GetBytesAt(0))
		}},
		{"text", types.Type{}, types.T_text, "long text", func(v *vector.Vector) {
			require.Equal(t, []byte("long text"), v.GetBytesAt(0))
		}},
		{"blob", types.Type{}, types.T_blob, "binary", func(v *vector.Vector) {
			require.Equal(t, []byte("binary"), v.GetBytesAt(0))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pkType := tt.pkType
			if pkType.Oid == 0 {
				pkType = tt.oid.ToType()
			}
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

	pkType := types.T_uuid.ToType()
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

// ---------- End-to-end PK filter pruning tests ----------

// makeBlockZM builds a ZoneMap for the given int32 range [min, max].
func makeBlockZM(min, max int32) index.ZM {
	zm := index.NewZM(types.T_int32, 0)
	index.UpdateZM(zm, types.EncodeInt32(&min))
	index.UpdateZM(zm, types.EncodeInt32(&max))
	return zm
}

func makeBlockZMVarchar(min, max string) index.ZM {
	zm := index.NewZM(types.T_varchar, 0)
	index.UpdateZM(zm, []byte(min))
	index.UpdateZM(zm, []byte(max))
	return zm
}

// TestPKFilterPruning_SinglePK_Int32 verifies that buildPKFilterFromVec
// produces segments that correctly prune block ZoneMaps for single-column
// int32 primary keys.
func TestPKFilterPruning_SinglePK_Int32(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	pkType := types.T_int32.ToType()
	pkSeqnum := 0

	// Simulate KEYS (10, 20, 30, 50, 51, 52).
	vec := vector.NewVec(pkType)
	for _, v := range []int32{10, 20, 30, 50, 51, 52} {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	vec.InplaceSort()

	pkFilter := buildPKFilterFromVec(vec, pkType, pkSeqnum)
	vec.Free(mp)

	require.NotNil(t, pkFilter)
	require.Equal(t, pkSeqnum, pkFilter.PrimarySeqnum)
	require.Greater(t, len(pkFilter.Segments), 0)

	// Block [5, 15] — contains key 10 → should match
	require.True(t, index.AnySegmentOverlaps(makeBlockZM(5, 15), pkFilter.Segments))

	// Block [25, 35] — contains key 30 → should match
	require.True(t, index.AnySegmentOverlaps(makeBlockZM(25, 35), pkFilter.Segments))

	// Block [48, 55] — contains keys 50,51,52 → should match
	require.True(t, index.AnySegmentOverlaps(makeBlockZM(48, 55), pkFilter.Segments))

	// Block [10, 52] — spans all keys → should match
	require.True(t, index.AnySegmentOverlaps(makeBlockZM(10, 52), pkFilter.Segments))

	// Block [100, 200] — entirely above all keys → should NOT match
	require.False(t, index.AnySegmentOverlaps(makeBlockZM(100, 200), pkFilter.Segments))

	// Block [0, 5] — entirely below all keys → should NOT match
	require.False(t, index.AnySegmentOverlaps(makeBlockZM(0, 5), pkFilter.Segments))
}

// TestPKFilterPruning_SinglePK_Varchar verifies pruning for varchar PK.
func TestPKFilterPruning_SinglePK_Varchar(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	pkType := types.T_varchar.ToType()
	pkSeqnum := 2 // simulate column at seqnum 2

	// Simulate KEYS ('apple', 'banana', 'mango').
	vec := vector.NewVec(pkType)
	for _, s := range []string{"apple", "banana", "mango"} {
		require.NoError(t, vector.AppendBytes(vec, []byte(s), false, mp))
	}
	vec.InplaceSort()

	pkFilter := buildPKFilterFromVec(vec, pkType, pkSeqnum)
	vec.Free(mp)

	require.NotNil(t, pkFilter)
	require.Equal(t, pkSeqnum, pkFilter.PrimarySeqnum)

	// Block ["alpha", "avocado"] — overlaps segment containing "apple" → match
	require.True(t, index.AnySegmentOverlaps(makeBlockZMVarchar("alpha", "avocado"), pkFilter.Segments))

	// Block ["lemon", "nectarine"] — contains "mango" → match
	require.True(t, index.AnySegmentOverlaps(makeBlockZMVarchar("lemon", "nectarine"), pkFilter.Segments))

	// Block ["peach", "plum"] — above all keys → no match
	require.False(t, index.AnySegmentOverlaps(makeBlockZMVarchar("peach", "plum"), pkFilter.Segments))

	// Block ["aaa", "ant"] — below all keys → no match
	require.False(t, index.AnySegmentOverlaps(makeBlockZMVarchar("aaa", "ant"), pkFilter.Segments))
}

// TestPKFilterPruning_Decimal64 verifies pruning for decimal64 PK.
func TestPKFilterPruning_Decimal64(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	pkType := types.New(types.T_decimal64, 10, 2)
	pkSeqnum := 0

	// Simulate KEYS (10.50, 20.00, 30.75).
	vec := vector.NewVec(pkType)
	for _, s := range []string{"10.50", "20.00", "30.75"} {
		v, parseErr := types.ParseDecimal64(s, 10, 2)
		require.NoError(t, parseErr)
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	vec.InplaceSort()

	pkFilter := buildPKFilterFromVec(vec, pkType, pkSeqnum)
	vec.Free(mp)

	require.NotNil(t, pkFilter)
	require.Greater(t, len(pkFilter.Segments), 0)

	// Block containing 10.50 → match
	lo, _ := types.ParseDecimal64("5.00", 10, 2)
	hi, _ := types.ParseDecimal64("15.00", 10, 2)
	blockZM := index.NewZM(types.T_decimal64, 2)
	index.UpdateZM(blockZM, types.EncodeDecimal64(&lo))
	index.UpdateZM(blockZM, types.EncodeDecimal64(&hi))
	require.True(t, index.AnySegmentOverlaps(blockZM, pkFilter.Segments))

	// Block entirely above → no match
	lo2, _ := types.ParseDecimal64("50.00", 10, 2)
	hi2, _ := types.ParseDecimal64("60.00", 10, 2)
	blockZM2 := index.NewZM(types.T_decimal64, 2)
	index.UpdateZM(blockZM2, types.EncodeDecimal64(&lo2))
	index.UpdateZM(blockZM2, types.EncodeDecimal64(&hi2))
	require.False(t, index.AnySegmentOverlaps(blockZM2, pkFilter.Segments))
}

// TestPKFilterPruning_SeqnumPreserved verifies that pkSeqnum is correctly
// stored in PKFilter and not confused with column index.
func TestPKFilterPruning_SeqnumPreserved(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	pkType := types.T_int64.ToType()

	vec := vector.NewVec(pkType)
	require.NoError(t, vector.AppendFixed(vec, int64(100), false, mp))
	vec.InplaceSort()

	// Simulate a schema where PK is at column index 2 but seqnum 5
	// (e.g., after dropping columns).
	pkFilter := buildPKFilterFromVec(vec, pkType, 5)
	vec.Free(mp)

	require.NotNil(t, pkFilter)
	require.Equal(t, 5, pkFilter.PrimarySeqnum, "PrimarySeqnum should be the seqnum, not column index")
}

// TestPKFilterPruning_LargeKeySet verifies that a large number of keys
// produces multiple segments and all are used for pruning.
func TestPKFilterPruning_LargeKeySet(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	pkType := types.T_int64.ToType()

	// Insert 1000 keys: 0, 10, 20, ..., 9990
	vec := vector.NewVec(pkType)
	for i := int64(0); i < 1000; i++ {
		require.NoError(t, vector.AppendFixed(vec, i*10, false, mp))
	}
	vec.InplaceSort()

	pkFilter := buildPKFilterFromVec(vec, pkType, 0)
	vec.Free(mp)

	require.NotNil(t, pkFilter)
	// Should have at least 1 segment
	require.Greater(t, len(pkFilter.Segments), 0)

	// Block containing key 500 (value 5000) → should match
	blockZM := index.NewZM(types.T_int64, 0)
	v1, v2 := int64(4990), int64(5010)
	index.UpdateZM(blockZM, types.EncodeInt64(&v1))
	index.UpdateZM(blockZM, types.EncodeInt64(&v2))
	require.True(t, index.AnySegmentOverlaps(blockZM, pkFilter.Segments))

	// Block entirely outside key range → should not match
	blockZM2 := index.NewZM(types.T_int64, 0)
	v3, v4 := int64(10000), int64(20000)
	index.UpdateZM(blockZM2, types.EncodeInt64(&v3))
	index.UpdateZM(blockZM2, types.EncodeInt64(&v4))
	require.False(t, index.AnySegmentOverlaps(blockZM2, pkFilter.Segments))

	// Block before key range → should not match
	blockZM3 := index.NewZM(types.T_int64, 0)
	v5, v6 := int64(-100), int64(-1)
	index.UpdateZM(blockZM3, types.EncodeInt64(&v5))
	index.UpdateZM(blockZM3, types.EncodeInt64(&v6))
	require.False(t, index.AnySegmentOverlaps(blockZM3, pkFilter.Segments))
}

// encodeCompositePK encodes component vectors into a __mo_cpkey_col vector
// using the same serial() function that production code uses.
func encodeCompositePK(t *testing.T, compVecs []*vector.Vector) *vector.Vector {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	encodedVec, err := function.RunFunctionDirectly(
		proc, function.SerialFunctionEncodeID, compVecs, compVecs[0].Length())
	require.NoError(t, err)
	return encodedVec
}

// TestPKFilterPruning_CompositePK_IntVarchar verifies that composite PK
// (int32, varchar) encoded via serial() produces segments that correctly
// prune block ZoneMaps.
func TestPKFilterPruning_CompositePK_IntVarchar(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	// Simulate a table with PRIMARY KEY (id INT, name VARCHAR).
	// KEYS ((1, 'alice'), (2, 'bob'), (5, 'eve'))

	// Build component vectors.
	intVec := vector.NewVec(types.T_int32.ToType())
	varcharVec := vector.NewVec(types.T_varchar.ToType())
	for _, pair := range []struct {
		id   int32
		name string
	}{
		{1, "alice"}, {2, "bob"}, {5, "eve"},
	} {
		require.NoError(t, vector.AppendFixed(intVec, pair.id, false, mp))
		require.NoError(t, vector.AppendBytes(varcharVec, []byte(pair.name), false, mp))
	}

	// Encode into __mo_cpkey_col.
	encodedVec := encodeCompositePK(t, []*vector.Vector{intVec, varcharVec})
	defer encodedVec.Free(mp)
	intVec.Free(mp)
	varcharVec.Free(mp)

	require.Equal(t, 3, encodedVec.Length())

	// Sort (lexicographic order on serial-encoded bytes).
	encodedVec.InplaceSort()

	// The __mo_cpkey_col type in production is T_varchar.
	cpkType := types.T_varchar.ToType()
	pkSeqnum := 3 // simulate __mo_cpkey_col at seqnum 3

	pkFilter := buildPKFilterFromVec(encodedVec, cpkType, pkSeqnum)
	require.NotNil(t, pkFilter)
	require.Equal(t, pkSeqnum, pkFilter.PrimarySeqnum)
	require.Greater(t, len(pkFilter.Segments), 0)

	// Construct block ZoneMaps from encoded composite keys.
	// Block that spans (1,'alice') to (2,'bob') → should match.
	key1 := encodedVec.GetRawBytesAt(0) // smallest after sort
	key2 := encodedVec.GetRawBytesAt(1)
	blockZM1 := index.NewZM(types.T_varchar, 0)
	index.UpdateZM(blockZM1, key1)
	index.UpdateZM(blockZM1, key2)
	require.True(t, index.AnySegmentOverlaps(blockZM1, pkFilter.Segments))

	// Block that spans (5,'eve') only → should match.
	key3 := encodedVec.GetRawBytesAt(2)
	blockZM2 := index.NewZM(types.T_varchar, 0)
	index.UpdateZM(blockZM2, key3)
	index.UpdateZM(blockZM2, key3) // single-key range
	require.True(t, index.AnySegmentOverlaps(blockZM2, pkFilter.Segments))

	// Build an "outside" key that is lexicographically beyond all KEYS values.
	// Encode (100, 'zzz') — well above our key set.
	outsideIntVec := vector.NewVec(types.T_int32.ToType())
	outsideVarcharVec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendFixed(outsideIntVec, int32(100), false, mp))
	require.NoError(t, vector.AppendBytes(outsideVarcharVec, []byte("zzz"), false, mp))
	outsideEnc := encodeCompositePK(t, []*vector.Vector{outsideIntVec, outsideVarcharVec})
	defer outsideEnc.Free(mp)
	outsideIntVec.Free(mp)
	outsideVarcharVec.Free(mp)

	outsideKey := outsideEnc.GetRawBytesAt(0)
	blockZM3 := index.NewZM(types.T_varchar, 0)
	index.UpdateZM(blockZM3, outsideKey)
	index.UpdateZM(blockZM3, outsideKey)
	require.False(t, index.AnySegmentOverlaps(blockZM3, pkFilter.Segments))
}

// TestPKFilterPruning_CompositePK_IntInt verifies pruning for a
// (int32, int32) composite PK.
func TestPKFilterPruning_CompositePK_IntInt(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	// KEYS ((1, 10), (1, 20), (2, 10), (3, 30))
	col1 := vector.NewVec(types.T_int32.ToType())
	col2 := vector.NewVec(types.T_int32.ToType())
	for _, pair := range []struct{ a, b int32 }{
		{1, 10}, {1, 20}, {2, 10}, {3, 30},
	} {
		require.NoError(t, vector.AppendFixed(col1, pair.a, false, mp))
		require.NoError(t, vector.AppendFixed(col2, pair.b, false, mp))
	}

	encodedVec := encodeCompositePK(t, []*vector.Vector{col1, col2})
	defer encodedVec.Free(mp)
	col1.Free(mp)
	col2.Free(mp)

	encodedVec.InplaceSort()

	cpkType := types.T_varchar.ToType()
	pkFilter := buildPKFilterFromVec(encodedVec, cpkType, 0)
	require.NotNil(t, pkFilter)
	require.Greater(t, len(pkFilter.Segments), 0)

	// Block spanning first two keys → match
	blockZM1 := index.NewZM(types.T_varchar, 0)
	index.UpdateZM(blockZM1, encodedVec.GetRawBytesAt(0))
	index.UpdateZM(blockZM1, encodedVec.GetRawBytesAt(1))
	require.True(t, index.AnySegmentOverlaps(blockZM1, pkFilter.Segments))

	// Block spanning all keys → match
	blockZM2 := index.NewZM(types.T_varchar, 0)
	index.UpdateZM(blockZM2, encodedVec.GetRawBytesAt(0))
	index.UpdateZM(blockZM2, encodedVec.GetRawBytesAt(3))
	require.True(t, index.AnySegmentOverlaps(blockZM2, pkFilter.Segments))

	// Block below all keys: encode (0, 0) and (0, 5)
	belowCol1 := vector.NewVec(types.T_int32.ToType())
	belowCol2 := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed(belowCol1, int32(0), false, mp))
	require.NoError(t, vector.AppendFixed(belowCol1, int32(0), false, mp))
	require.NoError(t, vector.AppendFixed(belowCol2, int32(0), false, mp))
	require.NoError(t, vector.AppendFixed(belowCol2, int32(5), false, mp))
	belowEnc := encodeCompositePK(t, []*vector.Vector{belowCol1, belowCol2})
	defer belowEnc.Free(mp)
	belowCol1.Free(mp)
	belowCol2.Free(mp)

	blockZM3 := index.NewZM(types.T_varchar, 0)
	index.UpdateZM(blockZM3, belowEnc.GetRawBytesAt(0))
	index.UpdateZM(blockZM3, belowEnc.GetRawBytesAt(1))
	require.False(t, index.AnySegmentOverlaps(blockZM3, pkFilter.Segments))

	// Block above all keys: encode (100, 100) and (200, 200)
	aboveCol1 := vector.NewVec(types.T_int32.ToType())
	aboveCol2 := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed(aboveCol1, int32(100), false, mp))
	require.NoError(t, vector.AppendFixed(aboveCol1, int32(200), false, mp))
	require.NoError(t, vector.AppendFixed(aboveCol2, int32(100), false, mp))
	require.NoError(t, vector.AppendFixed(aboveCol2, int32(200), false, mp))
	aboveEnc := encodeCompositePK(t, []*vector.Vector{aboveCol1, aboveCol2})
	defer aboveEnc.Free(mp)
	aboveCol1.Free(mp)
	aboveCol2.Free(mp)

	blockZM4 := index.NewZM(types.T_varchar, 0)
	index.UpdateZM(blockZM4, aboveEnc.GetRawBytesAt(0))
	index.UpdateZM(blockZM4, aboveEnc.GetRawBytesAt(1))
	require.False(t, index.AnySegmentOverlaps(blockZM4, pkFilter.Segments))
}
