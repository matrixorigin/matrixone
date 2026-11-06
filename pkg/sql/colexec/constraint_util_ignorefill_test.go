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

package colexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type notNullWarning struct {
	code uint16
	msg  string
}

type notNullWarningSession struct {
	total    uint64
	warnings []notNullWarning
}

func (*notNullWarningSession) GetTempTable(string, string) (string, bool) { return "", false }
func (*notNullWarningSession) AddTempTable(string, string, string)        {}
func (*notNullWarningSession) RemoveTempTable(string, string)             {}
func (*notNullWarningSession) RemoveTempTableByRealName(string)           {}
func (*notNullWarningSession) GetSqlModeNoAutoValueOnZero() (bool, bool)  { return false, false }
func (s *notNullWarningSession) AppendWarningBatch(total uint64, codes []uint16, messages []string) {
	s.total += total
	for i := 0; i < len(codes) && i < len(messages); i++ {
		s.warnings = append(s.warnings, notNullWarning{code: codes[i], msg: messages[i]})
	}
}

func newIgnoreFillProc(t *testing.T) (*process.Process, *notNullWarningSession) {
	proc := testutil.NewProc(t)
	session := &notNullWarningSession{}
	proc.Session = session
	proc.SetStmtProfile(&process.StmtProfile{})
	proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", true)
	return proc, session
}

func notNullCol(name string, oid types.T) *plan.ColDef {
	return &plan.ColDef{
		Name: name,
		Typ:  plan.Type{Id: int32(oid)},
		Default: &plan.Default{
			NullAbility: false,
		},
	}
}

func nullableCol(name string, oid types.T) *plan.ColDef {
	return &plan.ColDef{
		Name: name,
		Typ:  plan.Type{Id: int32(oid)},
		Default: &plan.Default{
			NullAbility: true,
		},
	}
}

// TestBatchDataNotNullFillForIgnoreAdjustsEveryFillableType pins the implicit
// default written for each fillable column type. Every adjusted cell produces
// one ER_BAD_NULL_ERROR warning carrying the MySQL message.
func TestBatchDataNotNullFillForIgnoreAdjustsEveryFillableType(t *testing.T) {
	proc, session := newIgnoreFillProc(t)
	defer proc.Free()
	mp := proc.Mp()

	// Every type gets one kept value at slot 0 and one NULL cell at slot 1,
	// so a single call proves both the implicit default and value retention.
	// Uuid has no cheap non-null zero allocation here, so both of its cells
	// are NULL and only the adjustment is asserted.
	intVec := vector.NewVec(types.T_int32.ToType())
	defer intVec.Free(mp)
	require.NoError(t, vector.AppendFixed(intVec, int32(7), false, mp))
	require.NoError(t, vector.AppendAny(intVec, nil, true, mp))

	// fixedPair builds the (kept value, NULL cell) pair. The created vectors
	// are registered for one test-scoped release; freeing inside the helper
	// would release each vector as soon as it is built.
	var pairVecs []*vector.Vector
	fixedPair := func(vec *vector.Vector, value any) {
		pairVecs = append(pairVecs, vec)
		require.NoError(t, vector.AppendAny(vec, value, false, mp))
		require.NoError(t, vector.AppendAny(vec, nil, true, mp))
	}
	t.Cleanup(func() {
		for _, vec := range pairVecs {
			vec.Free(mp)
		}
	})

	floatVec := vector.NewVec(types.T_float64.ToType())
	fixedPair(floatVec, float64(3))
	// The low-precision float family stores +0 as all-zero bits, so the
	// implicit default of each is the zero of its Go type; kept values stay
	// exactly representable per format (bf16 2, float16 3, float8 4,
	// float4 6 is the largest finite float4 magnitude).
	bf16Vec := vector.NewVec(types.T_bf16.ToType())
	fixedPair(bf16Vec, types.BF16FromFloat32(2))
	f16Vec := vector.NewVec(types.T_float16.ToType())
	fixedPair(f16Vec, types.Float16FromFloat32(3))
	f8Vec := vector.NewVec(types.T_float8.ToType())
	fixedPair(f8Vec, types.Float8FromFloat32(4))
	f4Vec := vector.NewVec(types.T_float4.ToType())
	fixedPair(f4Vec, types.Float4FromFloat32(6))
	decVec := vector.NewVec(types.T_decimal64.ToType())
	fixedPair(decVec, types.Decimal64(3))
	boolVec := vector.NewVec(types.T_bool.ToType())
	fixedPair(boolVec, true)
	bitVec := vector.NewVec(types.T_bit.ToType())
	fixedPair(bitVec, uint64(5))
	dateVec := vector.NewVec(types.T_date.ToType())
	fixedPair(dateVec, types.Date(2500))
	datetimeVec := vector.NewVec(types.T_datetime.ToType())
	fixedPair(datetimeVec, types.Datetime(10))
	timeVec := vector.NewVec(types.T_time.ToType())
	fixedPair(timeVec, types.Time(30))
	timestampVec := vector.NewVec(types.T_timestamp.ToType())
	fixedPair(timestampVec, types.Timestamp(30))
	yearVec := vector.NewVec(types.T_year.ToType())
	fixedPair(yearVec, types.MoYear(2025))
	enumVec := vector.NewVec(types.New(types.T_enum, 0, 0))
	fixedPair(enumVec, types.Enum(2))

	uuidVec := vector.NewVec(types.T_uuid.ToType())
	defer uuidVec.Free(mp)
	require.NoError(t, vector.AppendAny(uuidVec, nil, true, mp))
	require.NoError(t, vector.AppendAny(uuidVec, nil, true, mp))

	varcharVec := vector.NewVec(types.T_varchar.ToType())
	defer varcharVec.Free(mp)
	require.NoError(t, vector.AppendBytes(varcharVec, []byte("me"), false, mp))
	require.NoError(t, vector.AppendAny(varcharVec, nil, true, mp))

	// The byte-string family pins the BINARY(n) divergence: a plain
	// assignment right-pads any shorter value with 0x00 to the declared
	// width, so BINARY(2)'s implicit default is two zero bytes, while
	// VARBINARY keeps the empty payload.
	binVec := vector.NewVec(types.New(types.T_binary, 2, 0))
	defer binVec.Free(mp)
	require.NoError(t, vector.AppendBytes(binVec, []byte{0x11, 0x22}, false, mp))
	require.NoError(t, vector.AppendAny(binVec, nil, true, mp))

	varbinVec := vector.NewVec(types.New(types.T_varbinary, 4, 0))
	defer varbinVec.Free(mp)
	require.NoError(t, vector.AppendBytes(varbinVec, []byte("ab"), false, mp))
	require.NoError(t, vector.AppendAny(varbinVec, nil, true, mp))

	jsonVec := vector.NewVec(types.T_json.ToType())
	defer jsonVec.Free(mp)
	jsonSeed, err := bytejson.ParseFromByteSlice([]byte(`{"k":1}`))
	require.NoError(t, err)
	jsonSeedBytes, err := types.EncodeJson(jsonSeed)
	require.NoError(t, err)
	require.NoError(t, vector.AppendBytes(jsonVec, jsonSeedBytes, false, mp))
	require.NoError(t, vector.AppendAny(jsonVec, nil, true, mp))

	vecs := []*vector.Vector{
		intVec, floatVec, bf16Vec, f16Vec, f8Vec, f4Vec,
		decVec, boolVec, bitVec,
		dateVec, datetimeVec, timeVec, timestampVec, yearVec,
		enumVec, uuidVec, varcharVec, binVec, varbinVec, jsonVec,
	}
	attrs := []string{
		"i", "f", "bf", "f16", "f8", "f4",
		"de", "b", "bit",
		"d", "dt", "tm", "ts", "y",
		"e", "u", "s", "bn", "vb", "j",
	}
	cols := []*plan.ColDef{
		notNullCol("i", types.T_int32),
		notNullCol("f", types.T_float64),
		notNullCol("bf", types.T_bf16),
		notNullCol("f16", types.T_float16),
		notNullCol("f8", types.T_float8),
		notNullCol("f4", types.T_float4),
		notNullCol("de", types.T_decimal64),
		notNullCol("b", types.T_bool),
		notNullCol("bit", types.T_bit),
		notNullCol("d", types.T_date),
		notNullCol("dt", types.T_datetime),
		notNullCol("tm", types.T_time),
		notNullCol("ts", types.T_timestamp),
		notNullCol("y", types.T_year),
		notNullCol("e", types.T_enum),
		notNullCol("u", types.T_uuid),
		notNullCol("s", types.T_varchar),
		{
			Name: "bn",
			Typ:  plan.Type{Id: int32(types.T_binary), Width: 2},
			Default: &plan.Default{
				NullAbility: false,
			},
		},
		{
			Name: "vb",
			Typ:  plan.Type{Id: int32(types.T_varbinary), Width: 4},
			Default: &plan.Default{
				NullAbility: false,
			},
		},
		notNullCol("j", types.T_json),
	}

	filled, err := BatchDataNotNullFillForIgnore(vecs, attrs, cols, proc)
	require.NoError(t, err)
	require.Len(t, filled, 20)
	for _, vec := range vecs {
		require.False(t, vec.GetNulls().Contains(0) || vec.GetNulls().Contains(1),
			"adjusted vector still has NULLs")
	}

	require.Equal(t, int32(7), vector.GetFixedAtNoTypeCheck[int32](intVec, 0),
		"non-null cells keep their values")
	require.Equal(t, int32(0), vector.GetFixedAtNoTypeCheck[int32](intVec, 1),
		"the NULL cell was adjusted to the implicit default")
	require.Equal(t, float64(3), vector.GetFixedAtNoTypeCheck[float64](floatVec, 0))
	require.Equal(t, float64(0), vector.GetFixedAtNoTypeCheck[float64](floatVec, 1))
	require.Equal(t, types.BF16FromFloat32(2), vector.GetFixedAtNoTypeCheck[types.BF16](bf16Vec, 0))
	require.Equal(t, types.BF16(0), vector.GetFixedAtNoTypeCheck[types.BF16](bf16Vec, 1))
	require.Equal(t, types.Float16FromFloat32(3), vector.GetFixedAtNoTypeCheck[types.Float16](f16Vec, 0))
	require.Equal(t, types.Float16(0), vector.GetFixedAtNoTypeCheck[types.Float16](f16Vec, 1))
	require.Equal(t, types.Float8FromFloat32(4), vector.GetFixedAtNoTypeCheck[types.Float8](f8Vec, 0))
	require.Equal(t, types.Float8(0), vector.GetFixedAtNoTypeCheck[types.Float8](f8Vec, 1))
	require.Equal(t, types.Float4FromFloat32(6), vector.GetFixedAtNoTypeCheck[types.Float4](f4Vec, 0))
	require.Equal(t, types.Float4(0), vector.GetFixedAtNoTypeCheck[types.Float4](f4Vec, 1))
	require.Equal(t, types.Decimal64(3), vector.GetFixedAtNoTypeCheck[types.Decimal64](decVec, 0))
	require.Equal(t, types.Decimal64(0), vector.GetFixedAtNoTypeCheck[types.Decimal64](decVec, 1))
	require.Equal(t, true, vector.GetFixedAtNoTypeCheck[bool](boolVec, 0))
	require.Equal(t, false, vector.GetFixedAtNoTypeCheck[bool](boolVec, 1))
	require.Equal(t, uint64(5), vector.GetFixedAtNoTypeCheck[uint64](bitVec, 0))
	require.Equal(t, uint64(0), vector.GetFixedAtNoTypeCheck[uint64](bitVec, 1))
	require.Equal(t, types.Date(2500), vector.GetFixedAtNoTypeCheck[types.Date](dateVec, 0))
	require.Equal(t, types.ZeroDate, vector.GetFixedAtNoTypeCheck[types.Date](dateVec, 1))
	require.Equal(t, types.Datetime(10), vector.GetFixedAtNoTypeCheck[types.Datetime](datetimeVec, 0))
	require.Equal(t, types.ZeroDatetime, vector.GetFixedAtNoTypeCheck[types.Datetime](datetimeVec, 1))
	require.Equal(t, types.Time(30), vector.GetFixedAtNoTypeCheck[types.Time](timeVec, 0))
	require.Equal(t, types.Time(0), vector.GetFixedAtNoTypeCheck[types.Time](timeVec, 1))
	require.Equal(t, types.Timestamp(30), vector.GetFixedAtNoTypeCheck[types.Timestamp](timestampVec, 0))
	require.Equal(t, types.ZeroTimestamp, vector.GetFixedAtNoTypeCheck[types.Timestamp](timestampVec, 1))
	require.Equal(t, types.MoYear(2025), vector.GetFixedAtNoTypeCheck[types.MoYear](yearVec, 0))
	require.Equal(t, types.MoYear(0), vector.GetFixedAtNoTypeCheck[types.MoYear](yearVec, 1))
	require.Equal(t, types.Enum(2), vector.GetFixedAtNoTypeCheck[types.Enum](enumVec, 0))
	require.Equal(t, types.Enum(0), vector.GetFixedAtNoTypeCheck[types.Enum](enumVec, 1))
	require.Equal(t, types.Uuid{}, vector.GetFixedAtNoTypeCheck[types.Uuid](uuidVec, 0))
	require.Equal(t, types.Uuid{}, vector.GetFixedAtNoTypeCheck[types.Uuid](uuidVec, 1))
	require.Equal(t, "me", varcharVec.GetStringAt(0), "non-null cells keep their values")
	require.Equal(t, "", varcharVec.GetStringAt(1))
	require.Equal(t, []byte{0x11, 0x22}, binVec.GetBytesAt(0), "non-null cells keep their values")
	require.Equal(t, []byte{0, 0}, binVec.GetBytesAt(1),
		"BINARY(2) fills with the padded empty-string assignment (two 0x00 bytes)")
	require.Equal(t, []byte("ab"), varbinVec.GetBytesAt(0), "non-null cells keep their values")
	require.Empty(t, varbinVec.GetBytesAt(1),
		"VARBINARY keeps the empty payload: padding is the BINARY(n) rule")
	require.Equal(t, jsonSeed, types.DecodeJson(jsonVec.GetBytesAt(0)),
		"non-null cells keep their values")
	require.Equal(t, bytejson.Null, types.DecodeJson(jsonVec.GetBytesAt(1)),
		"the NULL cell was adjusted to the JSON null literal")

	// One warning per adjusted cell, with the MySQL ER_BAD_NULL_ERROR message.
	// The uuid vector contributes two NULL cells, so the batch total is 21
	// adjusted cells across the 20 columns.
	require.Equal(t, uint64(21), session.total)
	require.Len(t, session.warnings, 21)
	for _, warn := range session.warnings {
		require.Equal(t, moerr.ER_BAD_NULL_ERROR, warn.code)
	}
	require.Equal(t, notNullWarning{code: moerr.ER_BAD_NULL_ERROR, msg: "Column 'i' cannot be null"},
		session.warnings[0])
}

// TestBatchDataNotNullFillForIgnorePreservesNullableColumns proves the
// counterexample: a nullable column's NULLs are untouched, and no NULL row
// was silently dropped or reclassified.
func TestBatchDataNotNullFillForIgnorePreservesNullableColumns(t *testing.T) {
	proc, session := newIgnoreFillProc(t)
	defer proc.Free()

	nnVec := vector.NewVec(types.T_int64.ToType())
	defer nnVec.Free(proc.Mp())
	require.NoError(t, vector.AppendFixedList(nnVec, []int64{7, 7, 7}, nil, proc.Mp()))
	nnVec.GetNulls().Add(1)

	nullVec := vector.NewVec(types.T_int64.ToType())
	defer nullVec.Free(proc.Mp())
	require.NoError(t, vector.AppendFixedList(nullVec, []int64{1, 2, 3}, nil, proc.Mp()))
	nullVec.GetNulls().Add(0)
	nullVec.GetNulls().Add(2)

	vecs := []*vector.Vector{nnVec, nullVec}
	attrs := []string{"nn", "n"}
	cols := []*plan.ColDef{notNullCol("nn", types.T_int64), nullableCol("n", types.T_int64)}

	filled, err := BatchDataNotNullFillForIgnore(vecs, attrs, cols, proc)
	require.NoError(t, err)
	require.Equal(t, []int{0}, filled)
	require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](nnVec, 0))
	require.Equal(t, int64(0), vector.GetFixedAtNoTypeCheck[int64](nnVec, 1),
		"the NOT NULL NULL cell was adjusted")
	require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](nnVec, 2))
	require.True(t, nullVec.GetNulls().Contains(0))
	require.False(t, nullVec.GetNulls().Contains(1))
	require.True(t, nullVec.GetNulls().Contains(2))
	require.Equal(t, uint64(1), session.total)
}

// TestBatchDataNotNullFillForIgnoreUnsupportedTypeKeepsViolation keeps the
// strict-mode failure for a NOT NULL column whose type has no implicit
// default, so IGNORE cannot trade the report for corrupted rows.
func TestBatchDataNotNullFillForIgnoreUnsupportedTypeKeepsViolation(t *testing.T) {
	proc, session := newIgnoreFillProc(t)
	defer proc.Free()

	vecTyp := types.New(types.T_array_float32, 0, 0)
	arrVec := vector.NewVec(vecTyp)
	defer arrVec.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(arrVec, []byte{0, 0, 0, 0, 0, 0, 0, 0}, true, proc.Mp()))

	vecs := []*vector.Vector{arrVec}
	cols := []*plan.ColDef{notNullCol("v", types.T_array_float32)}
	_, err := BatchDataNotNullFillForIgnore(vecs, []string{"v"}, cols, proc)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation))
	require.Zero(t, session.total, "an aborted adjustment publishes no warnings")
	require.True(t, arrVec.IsNull(0), "the unsupported NULL cell was left untouched")
}

// TestBatchDataNotNullFillForIgnoreDatalinkKeepsViolation pins the datalink
// scoping of the IGNORE adjustment: a datalink value is a validated URL that
// load_file can resolve, and the empty byte string the shared string-family
// fill would store has no parsable scheme, so DATALINK keeps the geometry
// contract - no implicit default at all. Under statement IGNORE the NULL cell
// keeps the constraint violation instead of persisting a URL the datalink
// functions cannot read back.
func TestBatchDataNotNullFillForIgnoreDatalinkKeepsViolation(t *testing.T) {
	proc, session := newIgnoreFillProc(t)
	defer proc.Free()

	dlVec := vector.NewVec(types.T_datalink.ToType())
	defer dlVec.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(dlVec, nil, true, proc.Mp()))

	vecs := []*vector.Vector{dlVec}
	cols := []*plan.ColDef{notNullCol("d", types.T_datalink)}
	_, err := BatchDataNotNullFillForIgnore(vecs, []string{"d"}, cols, proc)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation))
	require.Zero(t, session.total, "an aborted adjustment publishes no warnings")
	require.True(t, dlVec.IsNull(0), "the datalink NULL cell was left untouched")
}

// TestBatchDataNotNullFillForIgnoreGeometryKeepsViolation records the geometry
// finding: the empty byte string is not a valid geometry payload, so a
// GEOMETRY NOT NULL column has no implicit default. Under statement IGNORE
// the NULL cell keeps the constraint violation instead of persisting a value
// the geometry functions cannot read back (MySQL 8.0.44 rejects the row with
// error 3673).
func TestBatchDataNotNullFillForIgnoreGeometryKeepsViolation(t *testing.T) {
	proc, session := newIgnoreFillProc(t)
	defer proc.Free()

	geoVec := vector.NewVec(types.T_geometry.ToType())
	defer geoVec.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(geoVec, nil, true, proc.Mp()))

	vecs := []*vector.Vector{geoVec}
	cols := []*plan.ColDef{notNullCol("g", types.T_geometry)}
	_, err := BatchDataNotNullFillForIgnore(vecs, []string{"g"}, cols, proc)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation))
	require.Zero(t, session.total, "an aborted adjustment publishes no warnings")
	require.True(t, geoVec.IsNull(0), "the geometry NULL cell was left untouched")
}

// TestBatchDataNotNullFillForIgnoreReplacesConstNullBroadcasts pins the
// constant-NULL contract of the LOAD legs: a NOT NULL column whose folded
// default projects as NewConstNull carries no backing storage, so the fill
// replaces the broadcast with an equivalent constant of the implicit default
// (the sanctioned NewConstFixed shape), keeps every logical row and reports
// one ER_BAD_NULL_ERROR warning per adjusted row.
func TestBatchDataNotNullFillForIgnoreReplacesConstNullBroadcasts(t *testing.T) {
	proc, session := newIgnoreFillProc(t)
	defer proc.Free()
	mp := proc.Mp()

	cn := vector.NewConstNull(types.T_int64.ToType(), 3, mp)
	// A nullable column's constant NULL stays untouched alongside the fill.
	nbCn := vector.NewConstNull(types.T_int64.ToType(), 3, mp)
	keepVec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(5), 3, mp)
	require.NoError(t, err)
	// The BINARY(2) broadcast proves the padded value through the same
	// shared rule the in-place fill uses.
	binCn := vector.NewConstNull(types.New(types.T_binary, 2, 0), 2, mp)
	// Every NOT NULL broadcast above gains a replacement the cleanup frees.
	replacements := make([]*vector.Vector, 0, 2)
	t.Cleanup(func() {
		for _, vec := range replacements {
			vec.Free(mp)
		}
	})

	vecs := []*vector.Vector{cn, nbCn, keepVec, binCn}
	attrs := []string{"a", "n", "k", "bn"}
	cols := []*plan.ColDef{
		notNullCol("a", types.T_int64),
		nullableCol("n", types.T_int64),
		notNullCol("k", types.T_int64),
		{
			Name: "bn",
			Typ:  plan.Type{Id: int32(types.T_binary), Width: 2},
			Default: &plan.Default{
				NullAbility: false,
			},
		},
	}

	filled, err := BatchDataNotNullFillForIgnore(vecs, attrs, cols, proc)
	require.NoError(t, err)
	require.Equal(t, []int{0, 3}, filled,
		"the constant NULL broadcasts of NOT NULL columns were adjusted")
	require.Same(t, nbCn, vecs[1], "nullable constant NULL is untouched")
	require.Same(t, keepVec, vecs[2], "non-null constant is untouched")

	for _, idx := range []int{0, 3} {
		got := vecs[idx]
		require.True(t, got.IsConst(),
			"the replacement keeps the constant broadcast of the original")
		replacements = append(replacements, got)
	}
	require.NotSame(t, cn, vecs[0], "the constant NULL was replaced")
	require.NotSame(t, binCn, vecs[3], "the constant NULL was replaced")
	require.Equal(t, 3, vecs[0].Length(), "logical length is preserved")
	require.False(t, vecs[0].IsConstNull())
	require.False(t, vecs[0].GetNulls().Contains(0))
	require.Equal(t, int64(0), vector.GetFixedAtNoTypeCheck[int64](vecs[0], 0),
		"the broadcast cell carries the implicit default")

	require.Equal(t, 2, vecs[3].Length())
	require.Equal(t, []byte{0, 0}, vecs[3].GetBytesAt(0),
		"BINARY(2) broadcast fills with the padded empty-string assignment")

	require.Equal(t, uint64(5), session.total,
		"one warning per adjusted logical row")
	require.Len(t, session.warnings, 5)
	for i, warn := range session.warnings {
		require.Equal(t, moerr.ER_BAD_NULL_ERROR, warn.code)
		if i < 3 {
			require.Equal(t, "Column 'a' cannot be null", warn.msg)
		} else {
			require.Equal(t, "Column 'bn' cannot be null", warn.msg)
		}
	}
}

// TestBatchDataNotNullFillForIgnoreConstNullWithoutDefaultKeepsViolation keeps
// the strict-mode failure for a constant NULL of a type with no implicit
// default: IGNORE cannot trade the report for corrupted rows.
func TestBatchDataNotNullFillForIgnoreConstNullWithoutDefaultKeepsViolation(t *testing.T) {
	proc, session := newIgnoreFillProc(t)
	defer proc.Free()

	geoCn := vector.NewConstNull(types.T_geometry.ToType(), 2, proc.Mp())
	vecs := []*vector.Vector{geoCn}
	cols := []*plan.ColDef{notNullCol("g", types.T_geometry)}
	_, err := BatchDataNotNullFillForIgnore(vecs, []string{"g"}, cols, proc)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation))
	require.Zero(t, session.total, "an aborted adjustment publishes no warnings")
}

// TestBatchDataNotNullCheckStrictStillRejects is the strict-mode red leg for
// the same vectors: without the IGNORE profile the NULL cell stays fatal.
func TestBatchDataNotNullCheckStrictStillRejects(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	nnVec := vector.NewVec(types.T_int64.ToType())
	defer nnVec.Free(proc.Mp())
	require.NoError(t, vector.AppendFixedList(nnVec, []int64{7, 7}, nil, proc.Mp()))
	nnVec.GetNulls().Add(0)

	tableDef := &plan.TableDef{Cols: []*plan.ColDef{notNullCol("nn", types.T_int64)}}
	err := BatchDataNotNullCheck([]*vector.Vector{nnVec}, []string{"nn"}, tableDef, proc.Ctx)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation))
}
