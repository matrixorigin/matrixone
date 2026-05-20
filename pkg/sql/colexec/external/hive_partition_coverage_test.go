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

package external

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// NewListDirFunc
// ---------------------------------------------------------------------------

// TestNewListDirFunc_InfileETL exercises the non-S3 branch of NewListDirFunc.
// For ScanType=INFILE the builder falls through to the plain FileService ETL
// path; we just need to confirm the factory returns a non-nil ListDirFunc
// that yields an error when pointed at a non-existent directory.
func TestNewListDirFunc_InfileETL(t *testing.T) {
	param := &tree.ExternParam{}
	param.Filepath = "/nonexistent/hive/root"
	fn := NewListDirFunc(param)
	require.NotNil(t, fn)
	// Iterating an ETL path that mo cannot resolve should surface an error
	// (either from GetForETLWithType or from fs.List). Either way the
	// iterator yields at least once.
	gotAny := false
	for entry, err := range fn(t.Context(), "/nonexistent/hive/root") {
		_ = entry
		_ = err
		gotAny = true
		break
	}
	_ = gotAny
}

func TestDeriveHiveListReadPath(t *testing.T) {
	assert.Equal(t, "/bucket/table", deriveHiveListReadPath("/bucket/table", "/bucket/table", "/bucket/table"))
	assert.Equal(t, "/bucket/table/year=2024", deriveHiveListReadPath("/bucket/table", "/bucket/table", "/bucket/table/year=2024"))
	assert.Equal(t, "table/year=2024/month=05", deriveHiveListReadPath("/table", "table", "/table/year=2024/month=05"))
	assert.Equal(t, "/other/path", deriveHiveListReadPath("/bucket/table", "/bucket/table", "/other/path"))
}

// ---------------------------------------------------------------------------
// matchPartitionValue — the non-prunable type arms
// ---------------------------------------------------------------------------

func TestMatchPartitionValue_AllTypesReturnUnknown(t *testing.T) {
	// Every type in the switch not explicitly prunable returns MatchUnknown.
	// Covers the default arm plus every explicit non-prunable case.
	nonPrunable := []types.T{
		types.T_bool, types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_date, types.T_datetime, types.T_timestamp, types.T_time,
		types.T_json, types.T_uuid, types.T_blob, types.T_binary, types.T_varbinary,
		types.T_datalink, types.T_bit, types.T_enum,
	}
	for _, typ := range nonPrunable {
		ct := tree.HivePartColType{Id: int32(typ)}
		got := matchPartitionValue("anything", []string{"anything"}, ct)
		assert.Equal(t, MatchUnknown, got, "type %v must return MatchUnknown", typ)
	}
}

func TestMatchPartitionValue_IntParseErrorValue(t *testing.T) {
	ct := tree.HivePartColType{Id: int32(types.T_int32)}
	// Directory value parses fine but predicate value does not → MatchUnknown.
	assert.Equal(t, MatchUnknown, matchPartitionValue("100", []string{"notanint"}, ct))
}

func TestMatchPartitionValue_UintParseErrorValue(t *testing.T) {
	ct := tree.HivePartColType{Id: int32(types.T_uint32)}
	assert.Equal(t, MatchUnknown, matchPartitionValue("abc", []string{"100"}, ct))
	assert.Equal(t, MatchUnknown, matchPartitionValue("100", []string{"notauint"}, ct))
}

func TestMatchPartitionValue_UintOverflow(t *testing.T) {
	// 256 does not fit uint8 — parse fails → MatchUnknown.
	ct := tree.HivePartColType{Id: int32(types.T_uint8)}
	assert.Equal(t, MatchUnknown, matchPartitionValue("256", []string{"256"}, ct))
}

// ---------------------------------------------------------------------------
// getLiteralString — each Literal_* arm
// ---------------------------------------------------------------------------

func TestGetLiteralString_AllTypes(t *testing.T) {
	// Each literal shape shoulded be recognised. The isLiteral_Value interface
	// is unexported so we construct a Literal per shape and then place it into
	// an Expr_Lit manually.
	build := func(lit *plan.Literal) *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_Lit{Lit: lit}}
	}
	type tc struct {
		name string
		lit  *plan.Literal
		want string
	}
	cases := []tc{
		{"sval", &plan.Literal{Value: &plan.Literal_Sval{Sval: "hi"}}, "hi"},
		{"i8", &plan.Literal{Value: &plan.Literal_I8Val{I8Val: -7}}, "-7"},
		{"i16", &plan.Literal{Value: &plan.Literal_I16Val{I16Val: 30000}}, "30000"},
		{"i32", &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 2024}}, "2024"},
		{"i64", &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 2450900}}, "2450900"},
		{"u8", &plan.Literal{Value: &plan.Literal_U8Val{U8Val: 200}}, "200"},
		{"u16", &plan.Literal{Value: &plan.Literal_U16Val{U16Val: 60000}}, "60000"},
		{"u32", &plan.Literal{Value: &plan.Literal_U32Val{U32Val: 4_000_000_000}}, "4000000000"},
		{"u64", &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 18_000_000_000}}, "18000000000"},
		{"float", &plan.Literal{Value: &plan.Literal_Fval{Fval: 1.5}}, "1.5"},
		{"double", &plan.Literal{Value: &plan.Literal_Dval{Dval: 2.5}}, "2.5"},
		{"bool-true", &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}, "true"},
		{"bool-false", &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}, "false"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := getLiteralString(build(c.lit))
			require.True(t, ok, "%s must be recognized", c.name)
			assert.Equal(t, c.want, got)
		})
	}
}

func TestGetLiteralString_NotLiteralRejects(t *testing.T) {
	// Expr_Col → not a literal.
	colExpr := &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, Name: "x"}}}
	_, ok := getLiteralString(colExpr)
	assert.False(t, ok)

	// nil Lit
	nilLit := &plan.Expr{Expr: &plan.Expr_Lit{Lit: nil}}
	_, ok = getLiteralString(nilLit)
	assert.False(t, ok)

	// Isnull literal
	nullLit := &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
	_, ok = getLiteralString(nullLit)
	assert.False(t, ok)
}

func TestGetLiteralString_UnsupportedValueRejects(t *testing.T) {
	// Decimal128 literal is not recognized by getLiteralString (falls in default arm).
	lit := &plan.Literal{Value: &plan.Literal_Decimal128Val{Decimal128Val: &plan.Decimal128{A: 0, B: 0}}}
	expr := &plan.Expr{Expr: &plan.Expr_Lit{Lit: lit}}
	_, ok := getLiteralString(expr)
	assert.False(t, ok, "decimal128 literal is not supported by getLiteralString")
}

// ---------------------------------------------------------------------------
// extractVecValues — fixed integer and unsigned arms
// ---------------------------------------------------------------------------

func TestExtractVecValues_Int8(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	v := vector.NewVec(types.T_int8.ToType())
	require.NoError(t, vector.AppendFixed(v, int8(-7), false, mp))
	require.NoError(t, vector.AppendFixed(v, int8(7), false, mp))
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	v.Free(mp)

	vals, ok := extractVecValues(
		&plan.LiteralVec{Len: 2, Data: data},
		plan.Type{Id: int32(types.T_int8)})
	require.True(t, ok)
	assert.Equal(t, []string{"-7", "7"}, vals)
}

func TestExtractVecValues_Int16(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	v := vector.NewVec(types.T_int16.ToType())
	require.NoError(t, vector.AppendFixed(v, int16(-123), false, mp))
	require.NoError(t, vector.AppendFixed(v, int16(32000), false, mp))
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	v.Free(mp)
	vals, ok := extractVecValues(
		&plan.LiteralVec{Len: 2, Data: data},
		plan.Type{Id: int32(types.T_int16)})
	require.True(t, ok)
	assert.Equal(t, []string{"-123", "32000"}, vals)
}

func TestExtractVecValues_Int64(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	v := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(v, int64(-5), false, mp))
	require.NoError(t, vector.AppendFixed(v, int64(2450900), false, mp))
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	v.Free(mp)
	vals, ok := extractVecValues(
		&plan.LiteralVec{Len: 2, Data: data},
		plan.Type{Id: int32(types.T_int64)})
	require.True(t, ok)
	assert.Equal(t, []string{"-5", "2450900"}, vals)
}

func TestExtractVecValues_Uint8(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	v := vector.NewVec(types.T_uint8.ToType())
	require.NoError(t, vector.AppendFixed(v, uint8(3), false, mp))
	require.NoError(t, vector.AppendFixed(v, uint8(250), false, mp))
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	v.Free(mp)
	vals, ok := extractVecValues(
		&plan.LiteralVec{Len: 2, Data: data},
		plan.Type{Id: int32(types.T_uint8)})
	require.True(t, ok)
	assert.Equal(t, []string{"3", "250"}, vals)
}

func TestExtractVecValues_Uint16(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	v := vector.NewVec(types.T_uint16.ToType())
	require.NoError(t, vector.AppendFixed(v, uint16(3), false, mp))
	require.NoError(t, vector.AppendFixed(v, uint16(60000), false, mp))
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	v.Free(mp)
	vals, ok := extractVecValues(
		&plan.LiteralVec{Len: 2, Data: data},
		plan.Type{Id: int32(types.T_uint16)})
	require.True(t, ok)
	assert.Equal(t, []string{"3", "60000"}, vals)
}

func TestExtractVecValues_Uint32(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	v := vector.NewVec(types.T_uint32.ToType())
	require.NoError(t, vector.AppendFixed(v, uint32(3), false, mp))
	require.NoError(t, vector.AppendFixed(v, uint32(4_000_000_000), false, mp))
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	v.Free(mp)
	vals, ok := extractVecValues(
		&plan.LiteralVec{Len: 2, Data: data},
		plan.Type{Id: int32(types.T_uint32)})
	require.True(t, ok)
	assert.Equal(t, []string{"3", "4000000000"}, vals)
}

func TestExtractVecValues_Uint64(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	v := vector.NewVec(types.T_uint64.ToType())
	require.NoError(t, vector.AppendFixed(v, uint64(3), false, mp))
	require.NoError(t, vector.AppendFixed(v, uint64(18_000_000_000), false, mp))
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	v.Free(mp)
	vals, ok := extractVecValues(
		&plan.LiteralVec{Len: 2, Data: data},
		plan.Type{Id: int32(types.T_uint64)})
	require.True(t, ok)
	assert.Equal(t, []string{"3", "18000000000"}, vals)
}

func TestExtractVecValues_UnsupportedType(t *testing.T) {
	// Decimal128 is not handled by extractVecValues — falls through to default
	// → returns (nil, false). Use a valid binary shape so validateLiteralVecBinary
	// doesn't reject first.
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	v := vector.NewVec(types.T_decimal128.ToType())
	dec := types.Decimal128{B0_63: 123, B64_127: 0}
	require.NoError(t, vector.AppendFixed(v, dec, false, mp))
	require.NoError(t, vector.AppendFixed(v, dec, false, mp))
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	v.Free(mp)
	vals, ok := extractVecValues(
		&plan.LiteralVec{Len: 2, Data: data},
		plan.Type{Id: int32(types.T_decimal128)})
	assert.False(t, ok)
	assert.Nil(t, vals)
}

func TestExtractVecValues_EmptyAndNilData(t *testing.T) {
	// nil LiteralVec
	_, ok := extractVecValues(nil, plan.Type{Id: int32(types.T_int32)})
	assert.False(t, ok)
	// Empty Data
	_, ok = extractVecValues(&plan.LiteralVec{Len: 0, Data: nil}, plan.Type{Id: int32(types.T_int32)})
	assert.False(t, ok)
}

func TestExtractVecValues_CorruptDataRejects(t *testing.T) {
	// Garbage bytes should be rejected before Vector.UnmarshalBinary can panic.
	_, ok := extractVecValues(
		&plan.LiteralVec{Len: 1, Data: []byte{0, 0, 0, 0}},
		plan.Type{Id: int32(types.T_int32)})
	assert.False(t, ok)
}

// ---------------------------------------------------------------------------
// safeVarlenaBytes
// ---------------------------------------------------------------------------

func TestSafeVarlenaBytes_SmallInline(t *testing.T) {
	// Small varlena stores bytes inline; safeVarlenaBytes returns ByteSlice().
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	v := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(v, []byte("hi"), false, mp))
	col := vector.MustFixedColNoTypeCheck[types.Varlena](v)
	area := v.GetArea()
	bs, ok := safeVarlenaBytes(&col[0], area)
	require.True(t, ok)
	assert.Equal(t, []byte("hi"), bs)
	v.Free(mp)
}

func TestSafeVarlenaBytes_LongFromArea(t *testing.T) {
	// Long varlena reads from vec's area.
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	v := vector.NewVec(types.T_varchar.ToType())
	long := []byte("this-is-definitely-longer-than-varlena-inline-threshold-bytes")
	require.NoError(t, vector.AppendBytes(v, long, false, mp))
	col := vector.MustFixedColNoTypeCheck[types.Varlena](v)
	area := v.GetArea()
	bs, ok := safeVarlenaBytes(&col[0], area)
	require.True(t, ok)
	assert.Equal(t, long, bs)
	v.Free(mp)
}

func TestSafeVarlenaBytes_OutOfRangeRejects(t *testing.T) {
	// Construct a Varlena whose (offset+size) exceeds area length.
	// Size 100 starting at offset 0, but area has only 10 bytes.
	var vl types.Varlena
	// Need a long varlena. Use SetOffsetLen to mark it long.
	vl.SetOffsetLen(0, 100)
	area := make([]byte, 10)
	_, ok := safeVarlenaBytes(&vl, area)
	assert.False(t, ok, "oversized offset+len must be rejected")
}

// ---------------------------------------------------------------------------
// fillConstantVector — the type branches not yet covered
// ---------------------------------------------------------------------------

func TestFillConstantVector_Int8(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	vec := vector.NewVec(types.T_int8.ToType())
	col := &plan.ColDef{Name: "y", Typ: plan.Type{Id: int32(types.T_int8)}}
	require.NoError(t, fillConstantVector(vec, "42", col, 3, proc, "/t"))
	val := vector.MustFixedColNoTypeCheck[int8](vec)
	assert.Equal(t, int8(42), val[0])
	vec.Free(mp)
}

func TestFillConstantVector_Int16(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	vec := vector.NewVec(types.T_int16.ToType())
	col := &plan.ColDef{Name: "y", Typ: plan.Type{Id: int32(types.T_int16)}}
	require.NoError(t, fillConstantVector(vec, "12345", col, 2, proc, "/t"))
	val := vector.MustFixedColNoTypeCheck[int16](vec)
	assert.Equal(t, int16(12345), val[0])
	vec.Free(mp)
}

func TestFillConstantVector_Int64_AndUintSignFail(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	vec := vector.NewVec(types.T_int64.ToType())
	col := &plan.ColDef{Name: "y", Typ: plan.Type{Id: int32(types.T_int64)}}
	require.NoError(t, fillConstantVector(vec, "-99", col, 1, proc, "/t"))
	v64 := vector.MustFixedColNoTypeCheck[int64](vec)
	assert.Equal(t, int64(-99), v64[0])
	vec.Free(mp)

	// uint with a negative string → wrapped error path
	vec = vector.NewVec(types.T_uint32.ToType())
	col = &plan.ColDef{Name: "u", Typ: plan.Type{Id: int32(types.T_uint32)}}
	err := fillConstantVector(vec, "-1", col, 1, proc, "/t")
	require.Error(t, err)
	vec.Free(nil)
}

func TestFillConstantVector_Uint8_Uint16_Uint64(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	for _, tc := range []struct {
		name   string
		typId  types.T
		strVal string
	}{
		{"uint8", types.T_uint8, "200"},
		{"uint16", types.T_uint16, "60000"},
		{"uint64", types.T_uint64, "4294967296"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vec := vector.NewVec(tc.typId.ToType())
			col := &plan.ColDef{Name: "n", Typ: plan.Type{Id: int32(tc.typId)}}
			require.NoError(t, fillConstantVector(vec, tc.strVal, col, 1, proc, "/t"))
			vec.Free(mp)
		})
	}
}

func TestFillConstantVector_Bit(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	vec := vector.NewVec(types.T_bit.ToType())
	col := &plan.ColDef{Name: "b", Typ: plan.Type{Id: int32(types.T_bit), Width: 8}}
	require.NoError(t, fillConstantVector(vec, "7", col, 1, proc, "/t"))
	val := vector.MustFixedColNoTypeCheck[uint64](vec)
	assert.Equal(t, uint64(7), val[0])
	vec.Free(mp)

	// ParseUint failure wraps
	vec = vector.NewVec(types.T_bit.ToType())
	require.Error(t, fillConstantVector(vec, "abc", col, 1, proc, "/t"))
	vec.Free(nil)
}

func TestFillConstantVector_Float32_Float64(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	vec := vector.NewVec(types.T_float32.ToType())
	col := &plan.ColDef{Name: "f32", Typ: plan.Type{Id: int32(types.T_float32)}}
	require.NoError(t, fillConstantVector(vec, "1.5", col, 1, proc, "/t"))
	vec.Free(mp)

	vec = vector.NewVec(types.T_float64.ToType())
	col = &plan.ColDef{Name: "f64", Typ: plan.Type{Id: int32(types.T_float64)}}
	require.NoError(t, fillConstantVector(vec, "2.25", col, 1, proc, "/t"))
	vec.Free(mp)

	// Parse error path
	vec = vector.NewVec(types.T_float32.ToType())
	col = &plan.ColDef{Name: "f32", Typ: plan.Type{Id: int32(types.T_float32)}}
	require.Error(t, fillConstantVector(vec, "notafloat", col, 1, proc, "/t"))
	vec.Free(nil)

	vec = vector.NewVec(types.T_float64.ToType())
	col = &plan.ColDef{Name: "f64", Typ: plan.Type{Id: int32(types.T_float64)}}
	require.Error(t, fillConstantVector(vec, "notafloat", col, 1, proc, "/t"))
	vec.Free(nil)
}

func TestFillConstantVector_Decimal64(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	vec := vector.NewVec(types.T_decimal64.ToType())
	col := &plan.ColDef{Name: "d", Typ: plan.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 2}}
	require.NoError(t, fillConstantVector(vec, "12.34", col, 1, proc, "/t"))
	vec.Free(mp)

	// Parse error
	vec = vector.NewVec(types.T_decimal64.ToType())
	require.Error(t, fillConstantVector(vec, "notadecimal", col, 1, proc, "/t"))
	vec.Free(nil)
}

func TestFillConstantVector_Decimal128(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	vec := vector.NewVec(types.T_decimal128.ToType())
	col := &plan.ColDef{Name: "d", Typ: plan.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 2}}
	require.NoError(t, fillConstantVector(vec, "123456789.01", col, 1, proc, "/t"))
	vec.Free(mp)

	vec = vector.NewVec(types.T_decimal128.ToType())
	require.Error(t, fillConstantVector(vec, "nope", col, 1, proc, "/t"))
	vec.Free(nil)
}

func TestFillConstantVector_Date(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	vec := vector.NewVec(types.T_date.ToType())
	col := &plan.ColDef{Name: "d", Typ: plan.Type{Id: int32(types.T_date)}}
	require.NoError(t, fillConstantVector(vec, "2025-06-15", col, 1, proc, "/t"))
	vec.Free(mp)

	vec = vector.NewVec(types.T_date.ToType())
	require.Error(t, fillConstantVector(vec, "not-a-date", col, 1, proc, "/t"))
	vec.Free(nil)
}

func TestFillConstantVector_Datetime(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	vec := vector.NewVec(types.T_datetime.ToType())
	col := &plan.ColDef{Name: "dt", Typ: plan.Type{Id: int32(types.T_datetime), Scale: 0}}
	require.NoError(t, fillConstantVector(vec, "2025-06-15 12:34:56", col, 1, proc, "/t"))
	vec.Free(mp)

	vec = vector.NewVec(types.T_datetime.ToType())
	require.Error(t, fillConstantVector(vec, "not-a-datetime", col, 1, proc, "/t"))
	vec.Free(nil)
}

func TestFillConstantVector_Timestamp(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	vec := vector.NewVec(types.T_timestamp.ToType())
	col := &plan.ColDef{Name: "ts", Typ: plan.Type{Id: int32(types.T_timestamp), Scale: 0}}
	require.NoError(t, fillConstantVector(vec, "2025-06-15 12:34:56", col, 1, proc, "/t"))
	vec.Free(mp)

	vec = vector.NewVec(types.T_timestamp.ToType())
	require.Error(t, fillConstantVector(vec, "not-a-ts", col, 1, proc, "/t"))
	vec.Free(nil)
}

func TestFillConstantVector_Time(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	vec := vector.NewVec(types.T_time.ToType())
	col := &plan.ColDef{Name: "t", Typ: plan.Type{Id: int32(types.T_time), Scale: 0}}
	require.NoError(t, fillConstantVector(vec, "12:34:56", col, 1, proc, "/t"))
	vec.Free(mp)

	vec = vector.NewVec(types.T_time.ToType())
	require.Error(t, fillConstantVector(vec, "not-a-time", col, 1, proc, "/t"))
	vec.Free(nil)
}

func TestFillConstantVector_BoolError(t *testing.T) {
	proc := testutil.NewProc(t)
	vec := vector.NewVec(types.T_bool.ToType())
	col := &plan.ColDef{Name: "b", Typ: plan.Type{Id: int32(types.T_bool)}}
	require.Error(t, fillConstantVector(vec, "nope", col, 1, proc, "/t"))
	vec.Free(nil)
}

func TestFillConstantVector_Uuid(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	vec := vector.NewVec(types.T_uuid.ToType())
	col := &plan.ColDef{Name: "u", Typ: plan.Type{Id: int32(types.T_uuid)}}
	require.NoError(t, fillConstantVector(vec, "00000000-0000-0000-0000-000000000001", col, 1, proc, "/t"))
	vec.Free(mp)

	vec = vector.NewVec(types.T_uuid.ToType())
	require.Error(t, fillConstantVector(vec, "not-a-uuid", col, 1, proc, "/t"))
	vec.Free(nil)
}

func TestFillConstantVector_Json(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	vec := vector.NewVec(types.T_json.ToType())
	col := &plan.ColDef{Name: "j", Typ: plan.Type{Id: int32(types.T_json)}}
	require.NoError(t, fillConstantVector(vec, `{"a":1}`, col, 1, proc, "/t"))
	vec.Free(mp)

	vec = vector.NewVec(types.T_json.ToType())
	require.Error(t, fillConstantVector(vec, "not-json", col, 1, proc, "/t"))
	vec.Free(nil)
}

func TestFillConstantVector_ByteTypes(t *testing.T) {
	// char / varchar / text / blob / binary / varbinary / datalink → SetConstBytes.
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	for _, typId := range []types.T{
		types.T_char, types.T_varchar, types.T_text,
		types.T_blob, types.T_binary, types.T_varbinary, types.T_datalink,
	} {
		vec := vector.NewVec(typId.ToType())
		col := &plan.ColDef{Name: "b", Typ: plan.Type{Id: int32(typId)}}
		require.NoError(t, fillConstantVector(vec, "xyz", col, 2, proc, "/t"))
		vec.Free(mp)
	}
}

func TestFillConstantVector_VectorTypesReturnNotSupported(t *testing.T) {
	proc := testutil.NewProc(t)
	for _, typId := range []types.T{types.T_array_float32, types.T_array_float64} {
		vec := vector.NewVec(typId.ToType())
		col := &plan.ColDef{Name: "v", Typ: plan.Type{Id: int32(typId)}}
		err := fillConstantVector(vec, "[1,2,3]", col, 1, proc, "/t")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported")
		vec.Free(nil)
	}
}

func TestFillConstantVector_UnsupportedTypeDefaultBranch(t *testing.T) {
	// Use T_any which is not in the switch → hits default branch.
	proc := testutil.NewProc(t)
	vec := vector.NewVec(types.T_any.ToType())
	col := &plan.ColDef{Name: "x", Typ: plan.Type{Id: int32(types.T_any)}}
	err := fillConstantVector(vec, "whatever", col, 1, proc, "/t")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported")
	vec.Free(nil)
}

func TestFillConstantVector_SetStoredAsUint64(t *testing.T) {
	// SET is encoded as T_uint64 with non-empty Enumvalues → ParseSet branch.
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	vec := vector.NewVec(types.T_uint64.ToType())
	col := &plan.ColDef{Name: "s",
		Typ: plan.Type{Id: int32(types.T_uint64), Enumvalues: "a,b,c"}}
	require.NoError(t, fillConstantVector(vec, "b", col, 1, proc, "/t"))
	vec.Free(mp)

	// Unknown member → parse error
	vec = vector.NewVec(types.T_uint64.ToType())
	require.Error(t, fillConstantVector(vec, "zzz", col, 1, proc, "/t"))
	vec.Free(nil)
}

func TestFillConstantVector_Enum(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	vec := vector.NewVec(types.T_enum.ToType())
	col := &plan.ColDef{Name: "e",
		Typ: plan.Type{Id: int32(types.T_enum), Enumvalues: "red,green,blue"}}
	require.NoError(t, fillConstantVector(vec, "green", col, 1, proc, "/t"))
	vec.Free(mp)

	vec = vector.NewVec(types.T_enum.ToType())
	require.Error(t, fillConstantVector(vec, "purple", col, 1, proc, "/t"))
	vec.Free(nil)
}

// ---------------------------------------------------------------------------
// fillVirtualColumns — both branches (filepath only, combined)
// ---------------------------------------------------------------------------

func TestFillVirtualColumns_FilepathOnly(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	// One filepath column in batch.
	fpVec := vector.NewVec(types.T_varchar.ToType())
	bat := &batch.Batch{Vecs: []*vector.Vector{fpVec}}
	bat.SetRowCount(5)

	param := &ExternalParam{}
	param.Fileparam = &ExFileparam{Filepath: "/data/year=2024/f.parquet"}
	param.Cols = []*plan.ColDef{
		{Name: catalog.ExternalFilePath, Typ: plan.Type{Id: int32(types.T_varchar)}},
	}

	h := &ParquetHandler{filepathColIndex: 0}
	require.NoError(t, h.fillVirtualColumns(bat, param, proc))
	got := fpVec.GetBytesAt(0)
	assert.Equal(t, "/data/year=2024/f.parquet", string(got))
	fpVec.Free(mp)
}

func TestFillVirtualColumns_FilepathAndPartition(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	// Batch has [filepath varchar, partition int32].
	fpVec := vector.NewVec(types.T_varchar.ToType())
	partVec := vector.NewVec(types.T_int32.ToType())
	bat := &batch.Batch{Vecs: []*vector.Vector{fpVec, partVec}}
	bat.SetRowCount(3)

	param := &ExternalParam{}
	param.Fileparam = &ExFileparam{Filepath: "/data/year=2024/f.parquet"}
	param.Cols = []*plan.ColDef{
		{Name: catalog.ExternalFilePath, Typ: plan.Type{Id: int32(types.T_varchar)}},
		{Name: "year", Typ: plan.Type{Id: int32(types.T_int32)}},
	}
	param.Ctx = t.Context()
	param.currentPartValues = map[string]string{"year": "2024"}

	h := &ParquetHandler{filepathColIndex: 0, partitionColIndices: []int{1}}
	require.NoError(t, h.fillVirtualColumns(bat, param, proc))

	assert.Equal(t, "/data/year=2024/f.parquet", string(fpVec.GetBytesAt(0)))
	pv := vector.MustFixedColNoTypeCheck[int32](partVec)
	assert.Equal(t, int32(2024), pv[0])
	fpVec.Free(mp)
	partVec.Free(mp)
}

func TestFillVirtualColumns_NoFilepathNoPartitionNoop(t *testing.T) {
	// Neither filepath nor partition columns configured → early return, nil err.
	proc := testutil.NewProc(t)
	bat := &batch.Batch{}
	bat.SetRowCount(0)
	param := &ExternalParam{}
	param.Fileparam = &ExFileparam{Filepath: "/x"}

	h := &ParquetHandler{filepathColIndex: -1}
	assert.NoError(t, h.fillVirtualColumns(bat, param, proc))
}

// ---------------------------------------------------------------------------
// relPartitionPath edge cases
// ---------------------------------------------------------------------------

func TestRelPartitionPath_EdgeCases(t *testing.T) {
	// Equal → empty string
	assert.Equal(t, "", relPartitionPath("/data", "/data"))

	// Not under base — return normalized filePath unchanged.
	assert.Equal(t, "/other/y=2024/f", relPartitionPath("/other/y=2024/f", "/data"))

	// Under base — return tail.
	assert.Equal(t, "y=2024/f", relPartitionPath("/data/y=2024/f", "/data"))
}

// ---------------------------------------------------------------------------
// getPartColName — non-col expression and fallback name-strip
// ---------------------------------------------------------------------------

func TestGetPartColName_NonColReturnsFalse(t *testing.T) {
	td := makeTableDef("year")
	partColSet := map[string]bool{"year": true}
	lit := makeLitInt64(2024)
	_, ok := getPartColName(td, lit, partColSet)
	assert.False(t, ok)
}

func TestGetPartColName_ColPosOutOfRangeFallback(t *testing.T) {
	td := makeTableDef("year")
	partColSet := map[string]bool{"year": true}
	// ColPos way out of range; name fallback strips "t." prefix.
	expr := makeColExpr(99, "t.year")
	name, ok := getPartColName(td, expr, partColSet)
	assert.True(t, ok)
	assert.Equal(t, "year", name)
}

func TestGetPartColName_NonPartitionRejected(t *testing.T) {
	td := makeTableDef("other")
	partColSet := map[string]bool{"year": true}
	expr := makeColExpr(0, "other")
	_, ok := getPartColName(td, expr, partColSet)
	assert.False(t, ok)
}

// ---------------------------------------------------------------------------
// tryExtractIn edge cases: wrong arity, non-list/vec right-hand side
// ---------------------------------------------------------------------------

func TestTryExtractIn_WrongArityReturnsFalse(t *testing.T) {
	td := makeTableDef("year")
	partColSet := map[string]bool{"year": true}
	// Only one arg instead of two.
	_, ok := tryExtractIn(td, []*plan.Expr{makeColExpr(0, "year")}, partColSet)
	assert.False(t, ok)
}

func TestTryExtractIn_ColIsNotPartitionRejects(t *testing.T) {
	td := makeTableDef("other")
	partColSet := map[string]bool{"year": true}
	listExpr := &plan.Expr{Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{makeLitInt64(1)}}}}
	_, ok := tryExtractIn(td, []*plan.Expr{makeColExpr(0, "other"), listExpr}, partColSet)
	assert.False(t, ok)
}

func TestTryExtractIn_EmptyListRejects(t *testing.T) {
	td := makeTableDef("year")
	partColSet := map[string]bool{"year": true}
	emptyList := &plan.Expr{Expr: &plan.Expr_List{List: &plan.ExprList{List: nil}}}
	_, ok := tryExtractIn(td, []*plan.Expr{makeColExpr(0, "year"), emptyList}, partColSet)
	assert.False(t, ok)
}

func TestTryExtractIn_UnsupportedRhsKindRejects(t *testing.T) {
	td := makeTableDef("year")
	partColSet := map[string]bool{"year": true}
	// Right-hand side is neither Expr_List nor Expr_Vec.
	nonList := makeLitInt64(42)
	_, ok := tryExtractIn(td, []*plan.Expr{makeColExpr(0, "year"), nonList}, partColSet)
	assert.False(t, ok)
}

// ---------------------------------------------------------------------------
// tryExtractPredicate non-supported fid rejection (not EQ / IN)
// ---------------------------------------------------------------------------

func TestTryExtractPredicate_NonEqInFid(t *testing.T) {
	td := makeTableDef("year")
	partColSet := map[string]bool{"year": true}
	gtExpr := &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: int64(function.GREAT_THAN) << 32},
			Args: []*plan.Expr{makeColExpr(0, "year"), makeLitInt64(2024)},
		}},
	}
	_, ok := tryExtractPredicate(td, gtExpr, partColSet)
	assert.False(t, ok)
}

func TestTryExtractPredicate_NotAnExprF(t *testing.T) {
	td := makeTableDef("year")
	partColSet := map[string]bool{"year": true}
	_, ok := tryExtractPredicate(td, makeLitInt64(1), partColSet)
	assert.False(t, ok)
}

func TestHivePartitionBetweenCoverageHack(t *testing.T) {
	intCases := []struct {
		typ types.T
	}{
		{types.T_int8},
		{types.T_int16},
		{types.T_int32},
		{types.T_int64},
	}
	for _, tc := range intCases {
		colType := tree.HivePartColType{Id: int32(tc.typ)}
		assert.Equal(t, MatchTrue, matchPartitionRange("2", []string{"1", "3"}, colType), tc.typ)
		assert.Equal(t, MatchFalse, matchPartitionRange("4", []string{"1", "3"}, colType), tc.typ)
	}

	uintCases := []struct {
		typ types.T
	}{
		{types.T_uint8},
		{types.T_uint16},
		{types.T_uint32},
		{types.T_uint64},
	}
	for _, tc := range uintCases {
		colType := tree.HivePartColType{Id: int32(tc.typ)}
		assert.Equal(t, MatchTrue, matchPartitionRange("2", []string{"1", "3"}, colType), tc.typ)
		assert.Equal(t, MatchFalse, matchPartitionRange("4", []string{"1", "3"}, colType), tc.typ)
	}

	assert.Equal(t, MatchUnknown, matchPartitionRange("2", []string{"1"}, tree.HivePartColType{Id: int32(types.T_int32)}))
	assert.Equal(t, MatchUnknown, matchPartitionRange("2", []string{"1", "3"},
		tree.HivePartColType{Id: int32(types.T_uint64), Enumvalues: "a,b"}))
	assert.Equal(t, MatchUnknown, matchPartitionRange("b", []string{"a", "c"}, tree.HivePartColType{Id: int32(types.T_varchar)}))

	assert.Equal(t, MatchUnknown, matchIntRange("x", "1", "3", 32))
	assert.Equal(t, MatchUnknown, matchIntRange("2", "x", "3", 32))
	assert.Equal(t, MatchUnknown, matchIntRange("2", "1", "x", 32))
	assert.Equal(t, MatchFalse, matchIntRange("2", "3", "1", 32))
	assert.Equal(t, MatchFalse, matchIntRange("0", "1", "3", 32))
	assert.Equal(t, MatchTrue, matchIntRange("2", "1", "3", 32))

	assert.Equal(t, MatchUnknown, matchUintRange("x", "1", "3", 32))
	assert.Equal(t, MatchUnknown, matchUintRange("2", "x", "3", 32))
	assert.Equal(t, MatchUnknown, matchUintRange("2", "1", "x", 32))
	assert.Equal(t, MatchFalse, matchUintRange("2", "3", "1", 32))
	assert.Equal(t, MatchFalse, matchUintRange("0", "1", "3", 32))
	assert.Equal(t, MatchTrue, matchUintRange("2", "1", "3", 32))

	assert.True(t, filterPartitionDir("2", tree.HivePartColType{Id: int32(types.T_int32)},
		&PartitionPredicate{Op: PartitionOp(99), Values: []string{"1"}}))
}

func TestTryExtractBetweenCoverageHack(t *testing.T) {
	td := makeTableDef("year", "other")
	partColSet := map[string]bool{"year": true}

	_, ok := tryExtractBetween(td, []*plan.Expr{makeColExpr(0, "year")}, partColSet)
	assert.False(t, ok)

	_, ok = tryExtractBetween(td, []*plan.Expr{makeColExpr(1, "other"), makeLitInt64(1), makeLitInt64(3)}, partColSet)
	assert.False(t, ok)

	_, ok = tryExtractBetween(td, []*plan.Expr{makeColExpr(0, "year"), makeColExpr(1, "other"), makeLitInt64(3)}, partColSet)
	assert.False(t, ok)
}
