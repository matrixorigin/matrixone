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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func unsignedDecimalAdmissionProc(t *testing.T, sqlMode string) (*process.Process, *numericWarningSession) {
	t.Helper()
	proc := testutil.NewProcess(t)
	session := &numericWarningSession{}
	proc.Session = session
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return sqlMode, nil
	})
	return proc, session
}

// runUnsignedDecimalAdmission invokes the executor entry directly: the target
// vector is a type carrier only, never read row-wise, and the third parameter
// is the constant destination-column name the 1264 diagnostic renders.
func runUnsignedDecimalAdmission(
	proc *process.Process,
	from *vector.Vector,
	target types.Type,
	resultType types.Type,
	length int,
	entry unsignedDecimalAdmissionEntry,
	column string,
) (vector.FunctionResultWrapper, error) {
	result := vector.NewFunctionResultWrapper(resultType, proc.Mp())
	targetCarrier := vector.NewVec(target)
	nameCarrier, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(column), length, proc.Mp())
	if err != nil {
		return result, err
	}
	// The evaluation framework pre-extends the result before calling the
	// executor; mirror that contract here or Append writes into empty cols.
	if err := result.PreExtendAndReset(length); err != nil {
		return result, err
	}
	err = newUnsignedDecimalAdmission(
		[]*vector.Vector{from, targetCarrier, nameCarrier}, result, proc, length, nil, entry)
	return result, err
}

// d64 parses a signed decimal64 literal for test vectors (Decimal64 is an
// unsigned bit representation, so negative constants cannot convert directly).
func d64(v string) types.Decimal64 {
	d, err := types.ParseDecimal64(v, 10, 2)
	if err != nil {
		panic(err)
	}
	return d
}

func decimalResult[T types.Decimal64 | types.Decimal128 | types.Decimal256](
	t *testing.T, result vector.FunctionResultWrapper,
) (*vector.Vector, func(int) bool) {
	vec := vector.MustFunctionResult[T](result).GetResultVector()
	return vec, func(i int) bool { return vec.GetNulls().Contains(uint64(i)) }
}

// requireOutOfRangeForColumn pins the 1264/22003 wire contract of the strict
// assignment diagnostic: the internal family, the numeric MySQL code, the
// SQLSTATE, and the destination column plus 1-based statement row.
func requireOutOfRangeForColumn(t *testing.T, err error, column string, row uint64) {
	t.Helper()
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRangeValueForColumn),
		"strict assignment reject must be the column-aware family, got %v", err)
	var moErr *moerr.Error
	require.ErrorAs(t, err, &moErr)
	require.Equal(t, moerr.ER_WARN_DATA_OUT_OF_RANGE, moErr.MySQLCode(),
		"the strict assignment reject maps to MySQL 1264, not the cast family 1690")
	require.Equal(t, "22003", moErr.SqlState())
	require.Contains(t, moErr.Error(), "Out of range value for column '"+column+"' at row ")
	require.Contains(t, moErr.Error(), "at row "+itoa64(row))
}

func itoa64(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

// requireAdmissionWarning pins the lenient twin of the strict diagnostic:
// ER_WARN_DATA_OUT_OF_RANGE with the same column/row context.
func requireAdmissionWarning(t *testing.T, w numericWarning, column string, row uint64) {
	t.Helper()
	require.Equal(t, moerr.ER_WARN_DATA_OUT_OF_RANGE, w.code)
	require.Equal(t, "Out of range value for column '"+column+"' at row "+itoa64(row), w.msg)
}

func TestUnsignedDecimalAdmissionFixedStrict(t *testing.T) {
	proc, session := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
	typ := types.New(types.T_decimal64, 10, 2)

	from := testutil.NewVector(2, typ, proc.Mp(), false,
		[]types.Decimal64{types.Decimal64(150), d64("-1.50")})
	_, err := runUnsignedDecimalAdmission(proc, from, typ, typ, 2, unsignedDecimalAdmissionStrict, "amt")
	requireOutOfRangeForColumn(t, err, "amt", 2)
	require.Empty(t, session.warnings, "the strict reject owns no warning")

	// Non-negative values pass through unchanged under the same entry.
	from = testutil.NewVector(2, typ, proc.Mp(), false,
		[]types.Decimal64{types.Decimal64(150), types.Decimal64(0)})
	result, err := runUnsignedDecimalAdmission(proc, from, typ, typ, 2, unsignedDecimalAdmissionStrict, "amt")
	require.NoError(t, err)
	vec, isNull := decimalResult[types.Decimal64](t, result)
	require.Equal(t, "1.50", vector.GetFixedAtWithTypeCheck[types.Decimal64](vec, 0).Format(2))
	require.Equal(t, "0.00", vector.GetFixedAtWithTypeCheck[types.Decimal64](vec, 1).Format(2))
	require.False(t, isNull(0) || isNull(1))
	require.Empty(t, session.warnings)

	// A NULL row passes as NULL with no diagnostics and no strict reject.
	from = testutil.NewVectorWithNulls(2, typ, proc.Mp(), false, []bool{true, false},
		[]types.Decimal64{types.Decimal64(0), types.Decimal64(0)})
	result, err = runUnsignedDecimalAdmission(proc, from, typ, typ, 2, unsignedDecimalAdmissionStrict, "amt")
	require.NoError(t, err)
	_, isNull = decimalResult[types.Decimal64](t, result)
	require.True(t, isNull(0))
	require.False(t, isNull(1))
	require.Empty(t, session.warnings)
}

// TestUnsignedDecimalAdmissionStrict1264RowContract walks the 1-based row
// numbering across violation positions: the first violating row owns the
// error and its message names that row.
func TestUnsignedDecimalAdmissionStrict1264RowContract(t *testing.T) {
	proc, session := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
	typ := types.New(types.T_decimal64, 10, 2)

	from := testutil.NewVector(3, typ, proc.Mp(), false,
		[]types.Decimal64{types.Decimal64(100), d64("-2.25"), d64("-0.01")})
	_, err := runUnsignedDecimalAdmission(proc, from, typ, typ, 3, unsignedDecimalAdmissionMode, "ud_col")
	requireOutOfRangeForColumn(t, err, "ud_col", 2)
	require.Empty(t, session.warnings)
}

func TestUnsignedDecimalAdmissionLenientClipAndWarnings(t *testing.T) {
	proc, session := unsignedDecimalAdmissionProc(t, "ONLY_FULL_GROUP_BY")
	typ := types.New(types.T_decimal64, 10, 2)

	from := testutil.NewVector(3, typ, proc.Mp(), false,
		[]types.Decimal64{d64("-1.50"), types.Decimal64(725), types.Decimal64(0)})
	result, err := runUnsignedDecimalAdmission(proc, from, typ, typ, 3, unsignedDecimalAdmissionMode, "amt")
	require.NoError(t, err)
	vec, isNull := decimalResult[types.Decimal64](t, result)
	require.Equal(t, "0.00", vector.GetFixedAtWithTypeCheck[types.Decimal64](vec, 0).Format(2),
		"lenient entries substitute the additive identity")
	require.Equal(t, "7.25", vector.GetFixedAtWithTypeCheck[types.Decimal64](vec, 1).Format(2))
	require.Equal(t, "0.00", vector.GetFixedAtWithTypeCheck[types.Decimal64](vec, 2).Format(2))
	require.False(t, isNull(0) || isNull(1) || isNull(2))
	require.Len(t, session.warnings, 1, "exactly one admission warning for the single adjusted row")
	requireAdmissionWarning(t, session.warnings[0], "amt", 1)

	// The IGNORE entry clips identically without consulting sql_mode.
	proc2, session2 := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
	from = testutil.NewVector(1, typ, proc2.Mp(), false, []types.Decimal64{d64("-0.01")})
	result, err = runUnsignedDecimalAdmission(proc2, from, typ, typ, 1, unsignedDecimalAdmissionIgnore, "ign_col")
	require.NoError(t, err)
	vec, _ = decimalResult[types.Decimal64](t, result)
	require.Equal(t, "0.00", vector.GetFixedAtWithTypeCheck[types.Decimal64](vec, 0).Format(2))
	require.Len(t, session2.warnings, 1)
	requireAdmissionWarning(t, session2.warnings[0], "ign_col", 1)

	// Width families: decimal128 and decimal256 clip at their own width.
	w128 := types.New(types.T_decimal128, 20, 2)
	from128 := testutil.NewVector(1, w128, proc.Mp(), false,
		[]types.Decimal128{types.Decimal128FromInt64(-150)})
	session.warnings = nil
	result, err = runUnsignedDecimalAdmission(proc, from128, w128, w128, 1, unsignedDecimalAdmissionMode, "amt")
	require.NoError(t, err)
	vec128 := vector.MustFunctionResult[types.Decimal128](result).GetResultVector()
	require.Equal(t, "0.00", vector.GetFixedAtWithTypeCheck[types.Decimal128](vec128, 0).Format(2))
	requireAdmissionWarning(t, session.warnings[0], "amt", 1)

	session.warnings = nil
	w256 := types.New(types.T_decimal256, 65, 6)
	neg256 := types.Decimal256FromInt64(-1)
	from256 := vector.NewVec(w256)
	require.NoError(t, vector.AppendFixed(from256, neg256, false, proc.Mp()))
	result, err = runUnsignedDecimalAdmission(proc, from256, w256, w256, 1, unsignedDecimalAdmissionMode, "amt")
	require.NoError(t, err)
	vec256 := vector.MustFunctionResult[types.Decimal256](result).GetResultVector()
	require.Equal(t, "0.000000", vector.GetFixedAtWithTypeCheck[types.Decimal256](vec256, 0).Format(6))
	requireAdmissionWarning(t, session.warnings[0], "amt", 1)

	// Negative float zero admits (MySQL sign check on a zero decimal).
	session.warnings = nil
	f64 := types.T_float64.ToType()
	fromF := testutil.NewVector(2, f64, proc.Mp(), false, []float64{-0.0, -2.5})
	result, err = runUnsignedDecimalAdmission(proc, fromF, typ, f64, 2, unsignedDecimalAdmissionMode, "fcol")
	require.NoError(t, err)
	vecF := vector.MustFunctionResult[float64](result).GetResultVector()
	require.Equal(t, float64(0), vector.GetFixedAtWithTypeCheck[float64](vecF, 0))
	require.Equal(t, float64(0), vector.GetFixedAtWithTypeCheck[float64](vecF, 1),
		"-2.5 clips to the additive identity")
	require.Len(t, session.warnings, 1)
	requireAdmissionWarning(t, session.warnings[0], "fcol", 2)

	// Unsigned and non-negative integer sources need no diagnostics; signed
	// negatives clip with the same column/row warning.
	session.warnings = nil
	i64 := types.T_int64.ToType()
	fromI := testutil.NewVector(2, i64, proc.Mp(), false, []int64{5, -2})
	result, err = runUnsignedDecimalAdmission(proc, fromI, typ, i64, 2, unsignedDecimalAdmissionMode, "icol")
	require.NoError(t, err)
	vecI := vector.MustFunctionResult[int64](result).GetResultVector()
	require.Equal(t, int64(5), vector.GetFixedAtWithTypeCheck[int64](vecI, 0))
	require.Equal(t, int64(0), vector.GetFixedAtWithTypeCheck[int64](vecI, 1))
	require.Len(t, session.warnings, 1)
	requireAdmissionWarning(t, session.warnings[0], "icol", 2)
}

// TestUnsignedDecimalAdmissionTimeSource pins the signed-TIME source family:
// types.Time carries an explicit sign, so a negative TIME violates the
// unsigned domain exactly like a negative decimal, while zero, positive, and
// NULL rows pass without diagnostics.
func TestUnsignedDecimalAdmissionTimeSource(t *testing.T) {
	typ := types.New(types.T_decimal64, 10, 2)
	timeT := types.T_time.ToType()
	newTimeVec := func(proc *process.Process, vals []types.Time) *vector.Vector {
		vec := vector.NewVec(timeT)
		for _, v := range vals {
			if err := vector.AppendFixed(vec, v, false, proc.Mp()); err != nil {
				t.Fatal(err)
			}
		}
		return vec
	}

	// Strict modes reject the negative row with the 1264 column diagnostic.
	proc, session := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
	from := newTimeVec(proc, []types.Time{types.Time(100), types.Time(-100)})
	_, err := runUnsignedDecimalAdmission(proc, from, typ, timeT, 2, unsignedDecimalAdmissionMode, "tcol")
	requireOutOfRangeForColumn(t, err, "tcol", 2)
	require.Empty(t, session.warnings)

	// Lenient: the negative clips to the additive identity of the TIME source;
	// zero and positive rows pass through with their sign facts intact.
	proc, session = unsignedDecimalAdmissionProc(t, "ONLY_FULL_GROUP_BY")
	from = newTimeVec(proc, []types.Time{types.Time(-100), types.Time(0), types.Time(250)})
	result, err := runUnsignedDecimalAdmission(proc, from, typ, timeT, 3, unsignedDecimalAdmissionMode, "tcol")
	require.NoError(t, err)
	vec := vector.MustFunctionResult[types.Time](result).GetResultVector()
	require.Equal(t, types.Time(0), vector.GetFixedAtNoTypeCheck[types.Time](vec, 0))
	require.Equal(t, types.Time(0), vector.GetFixedAtNoTypeCheck[types.Time](vec, 1))
	require.Equal(t, types.Time(250), vector.GetFixedAtNoTypeCheck[types.Time](vec, 2))
	require.Len(t, session.warnings, 1)
	requireAdmissionWarning(t, session.warnings[0], "tcol", 1)

	// NULL rows pass as NULL with no diagnostics.
	fromNull := vector.NewVec(timeT)
	require.NoError(t, vector.AppendFixed(fromNull, types.Time(0), true, proc.Mp()))
	result, err = runUnsignedDecimalAdmission(proc, fromNull, typ, timeT, 1, unsignedDecimalAdmissionMode, "tcol")
	require.NoError(t, err)
	vec = vector.MustFunctionResult[types.Time](result).GetResultVector()
	require.True(t, vec.GetNulls().Contains(0))
}

// TestUnsignedDecimalAdmissionYearSource pins the YEAR identity: engine YEAR
// values are 0 or 1901..2155 by construction (ValidateMoYear), so admission
// is a plain pass over the physical MoYear representation with no diagnostics
// under every policy.
func TestUnsignedDecimalAdmissionYearSource(t *testing.T) {
	typ := types.New(types.T_decimal64, 10, 2)
	yearT := types.T_year.ToType()

	proc, session := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
	from := vector.NewVec(yearT)
	for _, v := range []types.MoYear{0, 2025, 2155} {
		require.NoError(t, vector.AppendFixed(from, v, false, proc.Mp()))
	}
	result, err := runUnsignedDecimalAdmission(proc, from, typ, yearT, 3, unsignedDecimalAdmissionMode, "ycol")
	require.NoError(t, err)
	vec := vector.MustFunctionResult[types.MoYear](result).GetResultVector()
	require.Equal(t, types.MoYear(0), vector.GetFixedAtNoTypeCheck[types.MoYear](vec, 0))
	require.Equal(t, types.MoYear(2025), vector.GetFixedAtNoTypeCheck[types.MoYear](vec, 1))
	require.Equal(t, types.MoYear(2155), vector.GetFixedAtNoTypeCheck[types.MoYear](vec, 2))
	require.Empty(t, session.warnings)
}

// TestUnsignedDecimalAdmissionJSONSource pins the decoded-JSON family:
// encoded numeric scalars classify by their decoded value (never by a text
// grammar), violations clip to an encoded JSON numeric zero, and non-numeric
// scalars the outer cast owns pass through untouched.
func TestUnsignedDecimalAdmissionJSONSource(t *testing.T) {
	typ := types.New(types.T_decimal64, 10, 2)
	jsonT := types.T_json.ToType()
	encode := func(s string) string {
		bj, err := types.ParseStringToByteJson(s)
		require.NoError(t, err)
		enc, err := types.EncodeJson(bj)
		require.NoError(t, err)
		return string(enc)
	}
	decodeResult := func(result vector.FunctionResultWrapper, i int) bytejson.ByteJson {
		vec := vector.MustFunctionResult[types.Varlena](result).GetResultVector()
		return types.DecodeJson(vec.GetBytesAt(i))
	}
	newJSONVec := func(proc *process.Process, rows ...string) *vector.Vector {
		vec := vector.NewVec(jsonT)
		for _, row := range rows {
			require.NoError(t, vector.AppendBytes(vec, []byte(row), false, proc.Mp()))
		}
		return vec
	}

	// Lenient: negative int64 and string-decimal scalars clip to encoded zero;
	// positive, unsigned-range, literal and array encodings pass byte-identical.
	// A JSON string whose TEXT does not parse under the outer cast's own
	// conversion ("  -3" has leading blanks, which strconv.ParseFloat rejects)
	// is a conversion failure, not a negative: it passes through untouched so
	// the outer cast reports its own error instead of being silently rescued
	// to zero.
	proc, session := unsignedDecimalAdmissionProc(t, "ONLY_FULL_GROUP_BY")
	rows := []string{
		encode("-1"),                   // Int64 negative -> clip at row 1
		encode("18446744073709551615"), // beyond int64: non-negative numeric
		encode("\"-7\""),               // JSON string negative -> clip at row 3
		encode("true"),                 // literal: owned by the outer cast
		encode("12.5"),                 // positive numeric
		encode("  \"  -3\"  "),         // conversion failure: pass-through, cast owns the error
		encode("[-1]"),                 // array: the outer cast rejects it
		encode("\"15\""),               // positive string
	}
	from := newJSONVec(proc, rows...)
	result, err := runUnsignedDecimalAdmission(proc, from, typ, jsonT, 8, unsignedDecimalAdmissionMode, "j")
	require.NoError(t, err)
	for _, i := range []int{0, 2} {
		bj := decodeResult(result, i)
		require.Equal(t, bytejson.TpCodeInt64, bj.Type, "clipped rows decode as numeric zero (row %d)", i)
		require.Zero(t, bj.GetInt64())
	}
	vec := vector.MustFunctionResult[types.Varlena](result).GetResultVector()
	for _, i := range []int{1, 3, 4, 5, 6, 7} {
		require.Equal(t, rows[i], vec.GetStringAt(i), "admitted rows pass byte-identical (row %d)", i)
	}
	require.Len(t, session.warnings, 2)
	requireAdmissionWarning(t, session.warnings[0], "j", 1)
	requireAdmissionWarning(t, session.warnings[1], "j", 3)

	// Strict: a decoded negative rejects with the 1264 column diagnostic.
	proc, session = unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
	from = newJSONVec(proc, encode("1"), encode("-0.5"))
	_, err = runUnsignedDecimalAdmission(proc, from, typ, jsonT, 2, unsignedDecimalAdmissionMode, "j")
	requireOutOfRangeForColumn(t, err, "j", 2)
	require.Empty(t, session.warnings)

	// SQL NULL rows pass as NULL with no diagnostics.
	fromNull := vector.NewVec(jsonT)
	require.NoError(t, vector.AppendBytes(fromNull, nil, true, proc.Mp()))
	result, err = runUnsignedDecimalAdmission(proc, fromNull, typ, jsonT, 1, unsignedDecimalAdmissionMode, "j")
	require.NoError(t, err)
	vec = vector.MustFunctionResult[types.Varlena](result).GetResultVector()
	require.True(t, vec.GetNulls().Contains(0))
}

// TestUnsignedDecimalAdmissionJSONSignedZeroMalformedAndNegative composes
// admission with the REAL outer json->decimal cast over the admission output
// (the executor's exact composition) for the classification corner cases the
// value-based classifier must own:
//   - JSON signed zeros convert to -0.0 and store 0.00 with zero diagnostics
//     under strict, non-strict and IGNORE policies;
//   - malformed JSON strings are conversion failures owned by the cast:
//     admission passes them byte-identical with no 1264 diagnostic under every
//     policy, and the composed cast reports its own conversion error;
//   - genuine negatives reject with 1264 strict-side and clip+warnings
//     lenient-side, the composed cast then storing 0.00.
func TestUnsignedDecimalAdmissionJSONSignedZeroMalformedAndNegative(t *testing.T) {
	typ := types.New(types.T_decimal64, 10, 2)
	jsonT := types.T_json.ToType()
	encode := func(s string) string {
		bj, err := types.ParseStringToByteJson(s)
		require.NoError(t, err)
		enc, err := types.EncodeJson(bj)
		require.NoError(t, err)
		return string(enc)
	}
	newJSONVec := func(proc *process.Process, rows ...string) *vector.Vector {
		vec := vector.NewVec(jsonT)
		for _, row := range rows {
			require.NoError(t, vector.AppendBytes(vec, []byte(row), false, proc.Mp()))
		}
		return vec
	}
	// composedCast runs the real outer json->decimal cast on the admission
	// result for row i, exactly as the executor composes the two functions.
	composedCast := func(t *testing.T, proc *process.Process, result vector.FunctionResultWrapper, i int) (error, string) {
		t.Helper()
		vec := vector.MustFunctionResult[types.Varlena](result).GetResultVector()
		single := vector.NewVec(jsonT)
		require.NoError(t, vector.AppendBytes(single, append([]byte(nil), vec.GetBytesAt(i)...), false, proc.Mp()))
		castResult := vector.NewFunctionResultWrapper(typ, proc.Mp())
		require.NoError(t, castResult.PreExtendAndReset(1))
		err := jsonToNumeric(proc.Ctx, vector.GenerateFunctionStrParameter(single), castResult, 1, typ)
		if err != nil {
			return err, ""
		}
		out := vector.MustFunctionResult[types.Decimal64](castResult).GetResultVector()
		return nil, vector.GetFixedAtWithTypeCheck[types.Decimal64](out, 0).Format(2)
	}
	requireCastConversionError := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)
		require.False(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRangeValueForColumn),
			"conversion failures must stay with the cast, never the 1264 admission family: %v", err)
		require.Contains(t, err.Error(), "operator cast")
	}

	zeroNumber := encode("-0.00")        // JSON number: decodes to -0.0
	zeroString := encode("\"-0.00\"")    // JSON string: parses to -0.0
	malformed := encode("\"-abc\"")      // unconvertible text
	malformedBlank := encode("\"  -3\"") // leading blanks: ParseFloat rejects
	negativeNumber := encode("-2.5")
	negativeString := encode("\"-2.5\"")

	t.Run("strict", func(t *testing.T) {
		proc, session := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
		rows := []string{zeroNumber, zeroString, malformed, malformedBlank, negativeNumber}
		_, err := runUnsignedDecimalAdmission(proc, newJSONVec(proc, rows...), typ, jsonT, 5, unsignedDecimalAdmissionMode, "j")
		requireOutOfRangeForColumn(t, err, "j", 5)
		require.Empty(t, session.warnings)

		// Without the negative row, admission is clean and the composed cast
		// stores 0.00 for both signed-zero spellings.
		session.warnings = nil
		result, err := runUnsignedDecimalAdmission(proc, newJSONVec(proc, zeroNumber, zeroString), typ, jsonT, 2, unsignedDecimalAdmissionMode, "j")
		require.NoError(t, err)
		require.Empty(t, session.warnings, "signed zeros must not raise a spurious 1264")
		for i := 0; i < 2; i++ {
			castErr, stored := composedCast(t, proc, result, i)
			require.NoError(t, castErr)
			require.Equal(t, "0.00", stored, "signed zero stores the additive identity")
		}

		// Malformed strings pass admission untouched; the real cast owns the
		// conversion error and it is NOT the 1264 admission family.
		session.warnings = nil
		result, err = runUnsignedDecimalAdmission(proc, newJSONVec(proc, malformed, malformedBlank), typ, jsonT, 2, unsignedDecimalAdmissionMode, "j")
		require.NoError(t, err)
		require.Empty(t, session.warnings, "conversion failures raise no 1264 admission warning")
		for i := 0; i < 2; i++ {
			castErr, _ := composedCast(t, proc, result, i)
			requireCastConversionError(t, castErr)
		}
	})

	t.Run("non_strict", func(t *testing.T) {
		proc, session := unsignedDecimalAdmissionProc(t, "ONLY_FULL_GROUP_BY")
		rows := []string{zeroNumber, zeroString, malformed, negativeNumber, negativeString}
		result, err := runUnsignedDecimalAdmission(proc, newJSONVec(proc, rows...), typ, jsonT, 5, unsignedDecimalAdmissionMode, "j")
		require.NoError(t, err)
		require.Len(t, session.warnings, 2, "exactly the two genuine negatives warn")
		requireAdmissionWarning(t, session.warnings[0], "j", 4)
		requireAdmissionWarning(t, session.warnings[1], "j", 5)
		for i, want := range []string{"0.00", "0.00", "", "0.00", "0.00"} {
			castErr, stored := composedCast(t, proc, result, i)
			if i == 2 {
				requireCastConversionError(t, castErr)
				continue
			}
			require.NoError(t, castErr)
			require.Equal(t, want, stored)
		}
	})

	t.Run("ignore", func(t *testing.T) {
		proc, session := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
		rows := []string{zeroString, malformed, negativeString}
		result, err := runUnsignedDecimalAdmission(proc, newJSONVec(proc, rows...), typ, jsonT, 3, unsignedDecimalAdmissionIgnore, "j")
		require.NoError(t, err)
		require.Len(t, session.warnings, 1, "IGNORE warns only for the genuine negative")
		requireAdmissionWarning(t, session.warnings[0], "j", 3)
		castErr, stored := composedCast(t, proc, result, 0)
		require.NoError(t, castErr)
		require.Equal(t, "0.00", stored)
		castErr, _ = composedCast(t, proc, result, 1)
		requireCastConversionError(t, castErr)
		castErr, stored = composedCast(t, proc, result, 2)
		require.NoError(t, castErr)
		require.Equal(t, "0.00", stored, "the clipped negative converts as the stored zero")
	})
}

// TestUnsignedDecimalAdmissionBinarySource pins the IsBin family: payload
// bytes decode through the exact entry the outer assignment cast dispatches
// (ParseDecimal64FromByte + column scale), classification follows the decoded
// sign, and admitted rows re-render to canonical text that re-parses to the
// identical value the direct cast stores. The executor reset clears IsBin, so
// only the rendered text can carry the conversion contract forward.
func TestUnsignedDecimalAdmissionBinarySource(t *testing.T) {
	typ := types.New(types.T_decimal64, 10, 2)
	binT := types.T_varbinary.ToType()

	newBinVec := func(proc *process.Process, payloads ...[]byte) *vector.Vector {
		strs := make([]string, len(payloads))
		for i, p := range payloads {
			strs[i] = string(p)
		}
		vec := testutil.NewVector(len(payloads), binT, proc.Mp(), false, strs)
		vec.SetIsBin(true)
		return vec
	}
	// reparseProof replays the outer cast on the admission output and compares
	// against a direct binary decode: admission must not change the value the
	// cast stores.
	reparseProof := func(t *testing.T, result vector.FunctionResultWrapper, payload []byte, i int) {
		vec := vector.MustFunctionResult[types.Varlena](result).GetResultVector()
		rendered := vec.GetStringAt(i)
		reparsed, err := parseDecimal64CastString(rendered, typ.Width, typ.Scale)
		require.NoError(t, err, "admission output %q must re-parse at the target", rendered)
		direct, err := types.ParseDecimal64FromByte(string(payload), typ.Width, typ.Scale)
		require.NoError(t, err)
		require.Equal(t, direct, reparseProof0(reparsed))
	}

	p150 := []byte{0x00, 0x00, 0x00, 0x96}                         // 150 -> 1.50
	pZero := []byte{0x00}                                          // 0
	pDigits := []byte("123")                                       // raw bytes, not text digits
	pNineZeros := make([]byte, 9)                                  // zero-filled payloads decode to 0 exactly like the direct cast
	pMinus1Bytes := []byte("-1")                                   // the reviewer's payload: 0x2D31
	pNeg := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE, 0xCD} // two's-complement negative

	// Lenient pass-through and clip.
	proc, session := unsignedDecimalAdmissionProc(t, "ONLY_FULL_GROUP_BY")
	from := newBinVec(proc, p150, pZero, pDigits, pNineZeros, pMinus1Bytes, pNeg)
	result, err := runUnsignedDecimalAdmission(proc, from, typ, binT, 6, unsignedDecimalAdmissionMode, "b")
	require.NoError(t, err)
	reparseProof(t, result, p150, 0)
	reparseProof(t, result, pZero, 1)
	reparseProof(t, result, pDigits, 2)
	reparseProof(t, result, pNineZeros, 3)
	reparseProof(t, result, pMinus1Bytes, 4)
	vec := vector.MustFunctionResult[types.Varlena](result).GetResultVector()
	require.Equal(t, "0", vec.GetStringAt(5), "the decoded negative clips to additive identity")
	require.Len(t, session.warnings, 1)
	requireAdmissionWarning(t, session.warnings[0], "b", 6)

	// Strict reject of the same decoded negative names the column and row.
	proc, session = unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
	from = newBinVec(proc, pNeg)
	_, err = runUnsignedDecimalAdmission(proc, from, typ, binT, 1, unsignedDecimalAdmissionMode, "b")
	requireOutOfRangeForColumn(t, err, "b", 1)
	require.Empty(t, session.warnings)

	// Decode failures propagate exactly like the cast's own failure: the
	// empty payload is the only byte shape the FromByte decoders reject, so
	// it must surface here exactly as the direct cast surfaces it.
	from = newBinVec(proc, []byte{})
	_, err = runUnsignedDecimalAdmission(proc, from, typ, binT, 1, unsignedDecimalAdmissionMode, "b")
	require.Error(t, err, "the empty payload must fail just like the direct cast decode")
	require.False(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRangeValueForColumn),
		"decode failures are the cast's diagnostics, never a misclassified 1264")

	// NULL rows pass as NULL.
	fromNull := vector.NewVec(binT)
	fromNull.SetIsBin(true)
	require.NoError(t, vector.AppendBytes(fromNull, nil, true, proc.Mp()))
	result, err = runUnsignedDecimalAdmission(proc, fromNull, typ, binT, 1, unsignedDecimalAdmissionMode, "b")
	require.NoError(t, err)
	vec = vector.MustFunctionResult[types.Varlena](result).GetResultVector()
	require.True(t, vec.GetNulls().Contains(0))
}

func reparseProof0(d types.Decimal64) types.Decimal64 { return d }

func TestUnsignedDecimalAdmissionVarlenaBoundaries(t *testing.T) {
	typ := types.New(types.T_decimal64, 10, 2)
	strT := types.T_varchar.ToType()

	// Mode entry (non-IGNORE): lexical responsibility stays with the cast;
	// only complete-consumption negatives are diagnosed here.
	proc, session := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
	from := testutil.NewVector(2, strT, proc.Mp(), false, []string{"-1.5", "-1junk"})
	_, err := runUnsignedDecimalAdmission(proc, from, typ, strT, 2, unsignedDecimalAdmissionMode, "v")
	requireOutOfRangeForColumn(t, err, "v", 1)

	from = testutil.NewVector(2, strT, proc.Mp(), false, []string{"-1junk", "abc"})
	result, err := runUnsignedDecimalAdmission(proc, from, typ, strT, 2, unsignedDecimalAdmissionMode, "v")
	require.NoError(t, err)
	vec := vector.MustFunctionResult[types.Varlena](result).GetResultVector()
	require.Equal(t, "-1junk", vec.GetStringAt(0),
		"malformed values pass through for the cast's lexical diagnostic (P3-3)")
	require.Equal(t, "abc", vec.GetStringAt(1))
	require.Empty(t, session.warnings)

	// IGNORE entry: retained negative prefix yields the truncation diagnostic
	// ordered before exactly one 1264 admission warning.
	procI, sessionI := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
	from = testutil.NewVector(4, strT, procI.Mp(), false,
		[]string{"-1junk", "-1.5", "abc", "-0junk"})
	result, err = runUnsignedDecimalAdmission(procI, from, typ, strT, 4, unsignedDecimalAdmissionIgnore, "v")
	require.NoError(t, err)
	vec = vector.MustFunctionResult[types.Varlena](result).GetResultVector()
	require.Equal(t, "0", vec.GetStringAt(0))
	require.Equal(t, "0", vec.GetStringAt(1))
	require.Equal(t, "abc", vec.GetStringAt(2), "no numeric prefix: cast keeps its invalid diagnostic")
	require.Equal(t, "-0junk", vec.GetStringAt(3), "negative zero retained prefix passes admission")
	require.Len(t, sessionI.warnings, 3)
	require.Equal(t, moerr.WARN_DATA_TRUNCATED, sessionI.warnings[0].code,
		"the retained-prefix truncation diagnostic precedes the 1264 admission warning")
	requireAdmissionWarning(t, sessionI.warnings[1], "v", 1)
	requireAdmissionWarning(t, sessionI.warnings[2], "v", 2)
}

func TestUnsignedDecimalAdmissionScalarNullAndValidation(t *testing.T) {
	proc, session := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
	typ := types.New(types.T_decimal64, 10, 2)
	strT := types.T_varchar.ToType()

	// Scalar NULL source (ANY): identity nulls, no domain diagnostics.
	anyT := types.T_any.ToType()
	from := vector.NewVec(anyT)
	result, err := runUnsignedDecimalAdmission(proc, from, typ, strT, 3, unsignedDecimalAdmissionStrict, "s")
	require.NoError(t, err)
	vec := vector.MustFunctionResult[types.Varlena](result).GetResultVector()
	require.Equal(t, 3, vec.Length())
	for i := 0; i < 3; i++ {
		require.True(t, vec.GetNulls().Contains(uint64(i)))
	}
	require.Empty(t, session.warnings)

	// Parameter shape validation: the admission takes exactly three
	// parameters (value, target type carrier, column name carrier).
	err = newUnsignedDecimalAdmission(
		[]*vector.Vector{from}, nil, proc, 1, nil, unsignedDecimalAdmissionMode)
	require.Error(t, err)

	intCarrier := vector.NewVec(types.T_int64.ToType())
	nameCarrier, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("s"), 1, proc.Mp())
	require.NoError(t, err)
	result = vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
	err = newUnsignedDecimalAdmission(
		[]*vector.Vector{from, intCarrier, nameCarrier}, result, proc, 1, nil, unsignedDecimalAdmissionMode)
	require.Error(t, err, "a non-decimal target carrier is invalid")
}

// TestUnsignedDecimalAdmissionColumnLabelFallback pins the generic-replay
// contract: a composition that cannot name its destination carries an empty
// constant, and the diagnostic falls back to the legacy type label.
func TestUnsignedDecimalAdmissionColumnLabelFallback(t *testing.T) {
	proc, session := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
	typ := types.New(types.T_decimal64, 10, 2)

	from := testutil.NewVector(1, typ, proc.Mp(), false, []types.Decimal64{d64("-1")})
	_, err := runUnsignedDecimalAdmission(proc, from, typ, typ, 1, unsignedDecimalAdmissionStrict, "")
	requireOutOfRangeForColumn(t, err, "decimal", 1)
	require.Empty(t, session.warnings)

	// Non-empty names are used verbatim, including dotted qualified names.
	from = testutil.NewVector(1, typ, proc.Mp(), false, []types.Decimal64{d64("-1")})
	_, err = runUnsignedDecimalAdmission(proc, from, typ, typ, 1, unsignedDecimalAdmissionStrict, "orders.total")
	requireOutOfRangeForColumn(t, err, "orders.total", 1)
}

// TestUnsignedDecimalAdmissionFixedSourceMatrix walks the remaining
// fixed-size source kinds through admission: non-negative values pass
// through unchanged, and a negative row is the strict 1264 reject naming
// its column and 1-based statement row.
func TestUnsignedDecimalAdmissionFixedSourceMatrix(t *testing.T) {
	target := types.New(types.T_decimal64, 10, 2)

	// Integer widths narrower than int64 and float32 own their own negative
	// classification; a negative row rejects strictly.
	t.Run("signed kinds reject negative rows", func(t *testing.T) {
		proc, session := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")

		from := testutil.NewVector(2, types.T_float32.ToType(), proc.Mp(), false,
			[]float32{0.5, -0.5})
		_, err := runUnsignedDecimalAdmission(proc, from, target, types.T_float32.ToType(), 2,
			unsignedDecimalAdmissionStrict, "m")
		requireOutOfRangeForColumn(t, err, "m", 2)

		from = testutil.NewVector(2, types.T_int8.ToType(), proc.Mp(), false,
			[]int8{1, -8})
		_, err = runUnsignedDecimalAdmission(proc, from, target, types.T_int8.ToType(), 2,
			unsignedDecimalAdmissionStrict, "m")
		requireOutOfRangeForColumn(t, err, "m", 2)

		from = testutil.NewVector(2, types.T_int16.ToType(), proc.Mp(), false,
			[]int16{1, -1600})
		_, err = runUnsignedDecimalAdmission(proc, from, target, types.T_int16.ToType(), 2,
			unsignedDecimalAdmissionStrict, "m")
		requireOutOfRangeForColumn(t, err, "m", 2)

		from = testutil.NewVector(2, types.T_int32.ToType(), proc.Mp(), false,
			[]int32{1, -320000})
		_, err = runUnsignedDecimalAdmission(proc, from, target, types.T_int32.ToType(), 2,
			unsignedDecimalAdmissionStrict, "m")
		requireOutOfRangeForColumn(t, err, "m", 2)
		require.Empty(t, session.warnings, "strict rejects own no warnings")
	})

	// Non-negative values of every remaining fixed kind pass through
	// unchanged with no diagnostics.
	t.Run("non-negative rows pass through", func(t *testing.T) {
		proc, session := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")

		from := testutil.NewVector(2, types.T_float32.ToType(), proc.Mp(), false,
			[]float32{0.5, 2.5})
		result, err := runUnsignedDecimalAdmission(proc, from, target, types.T_float32.ToType(), 2,
			unsignedDecimalAdmissionStrict, "m")
		require.NoError(t, err)
		vec := vector.MustFunctionResult[float32](result).GetResultVector()
		require.Equal(t, float32(0.5), vector.GetFixedAtWithTypeCheck[float32](vec, 0))
		require.Equal(t, float32(2.5), vector.GetFixedAtWithTypeCheck[float32](vec, 1))

		from = testutil.NewVector(2, types.T_int8.ToType(), proc.Mp(), false,
			[]int8{0, 8})
		result, err = runUnsignedDecimalAdmission(proc, from, target, types.T_int8.ToType(), 2,
			unsignedDecimalAdmissionStrict, "m")
		require.NoError(t, err)
		vecI8 := vector.MustFunctionResult[int8](result).GetResultVector()
		require.Equal(t, int8(8), vector.GetFixedAtWithTypeCheck[int8](vecI8, 1))

		from = testutil.NewVector(2, types.T_int16.ToType(), proc.Mp(), false,
			[]int16{0, 1600})
		result, err = runUnsignedDecimalAdmission(proc, from, target, types.T_int16.ToType(), 2,
			unsignedDecimalAdmissionStrict, "m")
		require.NoError(t, err)
		vecI16 := vector.MustFunctionResult[int16](result).GetResultVector()
		require.Equal(t, int16(1600), vector.GetFixedAtWithTypeCheck[int16](vecI16, 1))

		from = testutil.NewVector(2, types.T_int32.ToType(), proc.Mp(), false,
			[]int32{0, 320000})
		result, err = runUnsignedDecimalAdmission(proc, from, target, types.T_int32.ToType(), 2,
			unsignedDecimalAdmissionStrict, "m")
		require.NoError(t, err)
		vecI32 := vector.MustFunctionResult[int32](result).GetResultVector()
		require.Equal(t, int32(320000), vector.GetFixedAtWithTypeCheck[int32](vecI32, 1))

		// Unsigned kinds classify non-negative by construction.
		from = testutil.NewVector(2, types.T_uint8.ToType(), proc.Mp(), false,
			[]uint8{0, 8})
		result, err = runUnsignedDecimalAdmission(proc, from, target, types.T_uint8.ToType(), 2,
			unsignedDecimalAdmissionStrict, "m")
		require.NoError(t, err)
		vecU8 := vector.MustFunctionResult[uint8](result).GetResultVector()
		require.Equal(t, uint8(8), vector.GetFixedAtWithTypeCheck[uint8](vecU8, 1))

		from = testutil.NewVector(2, types.T_uint16.ToType(), proc.Mp(), false,
			[]uint16{0, 1600})
		result, err = runUnsignedDecimalAdmission(proc, from, target, types.T_uint16.ToType(), 2,
			unsignedDecimalAdmissionStrict, "m")
		require.NoError(t, err)
		vecU16 := vector.MustFunctionResult[uint16](result).GetResultVector()
		require.Equal(t, uint16(1600), vector.GetFixedAtWithTypeCheck[uint16](vecU16, 1))

		from = testutil.NewVector(2, types.T_uint32.ToType(), proc.Mp(), false,
			[]uint32{0, 320000})
		result, err = runUnsignedDecimalAdmission(proc, from, target, types.T_uint32.ToType(), 2,
			unsignedDecimalAdmissionStrict, "m")
		require.NoError(t, err)
		vecU32 := vector.MustFunctionResult[uint32](result).GetResultVector()
		require.Equal(t, uint32(320000), vector.GetFixedAtWithTypeCheck[uint32](vecU32, 1))

		from = testutil.NewVector(2, types.T_uint64.ToType(), proc.Mp(), false,
			[]uint64{0, 6400000000})
		result, err = runUnsignedDecimalAdmission(proc, from, target, types.T_uint64.ToType(), 2,
			unsignedDecimalAdmissionStrict, "m")
		require.NoError(t, err)
		vecU64 := vector.MustFunctionResult[uint64](result).GetResultVector()
		require.Equal(t, uint64(6400000000), vector.GetFixedAtWithTypeCheck[uint64](vecU64, 1))

		from = testutil.NewVector(2, types.T_bool.ToType(), proc.Mp(), false,
			[]bool{false, true})
		result, err = runUnsignedDecimalAdmission(proc, from, target, types.T_bool.ToType(), 2,
			unsignedDecimalAdmissionStrict, "m")
		require.NoError(t, err)
		vecB := vector.MustFunctionResult[bool](result).GetResultVector()
		require.True(t, vector.GetFixedAtWithTypeCheck[bool](vecB, 1))

		// BIT keeps its physical uint64 representation through admission.
		from = testutil.NewVector(2, types.T_bit.ToType(), proc.Mp(), false,
			[]uint64{0, 5})
		result, err = runUnsignedDecimalAdmission(proc, from, target, types.T_bit.ToType(), 2,
			unsignedDecimalAdmissionStrict, "m")
		require.NoError(t, err)
		vecBit := vector.MustFunctionResult[uint64](result).GetResultVector()
		require.Equal(t, uint64(5), vector.GetFixedAtWithTypeCheck[uint64](vecBit, 1))

		require.Empty(t, session.warnings)
	})
}

// TestUnsignedDecimalAdmissionTemporalEnumAndUnsupportedSources walks the
// identity admission group (date, datetime, timestamp, enum ordinals are
// non-negative by construction here) and the loud rejection of truly
// unsupported source kinds.
func TestUnsignedDecimalAdmissionTemporalEnumAndUnsupportedSources(t *testing.T) {
	target := types.New(types.T_decimal64, 10, 2)
	proc, session := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")

	from := testutil.NewVector(1, types.T_date.ToType(), proc.Mp(), false,
		[]string{"2024-01-01"})
	result, err := runUnsignedDecimalAdmission(proc, from, target, types.T_date.ToType(), 1,
		unsignedDecimalAdmissionStrict, "d")
	require.NoError(t, err)
	vecD := vector.MustFunctionResult[types.Date](result).GetResultVector()
	require.False(t, vecD.GetNulls().Contains(0))

	from = testutil.NewVector(1, types.T_datetime.ToType(), proc.Mp(), false,
		[]string{"2024-01-01 01:02:03"})
	result, err = runUnsignedDecimalAdmission(proc, from, target, types.T_datetime.ToType(), 1,
		unsignedDecimalAdmissionStrict, "d")
	require.NoError(t, err)
	vecDT := vector.MustFunctionResult[types.Datetime](result).GetResultVector()
	require.False(t, vecDT.GetNulls().Contains(0))

	from = testutil.NewVector(1, types.T_timestamp.ToType(), proc.Mp(), false,
		[]string{"2024-01-01 01:02:03"})
	result, err = runUnsignedDecimalAdmission(proc, from, target, types.T_timestamp.ToType(), 1,
		unsignedDecimalAdmissionStrict, "d")
	require.NoError(t, err)
	vecTS := vector.MustFunctionResult[types.Timestamp](result).GetResultVector()
	require.False(t, vecTS.GetNulls().Contains(0))

	// Enum ordinals are non-negative by construction.
	from = testutil.NewVector(1, types.T_enum.ToType(), proc.Mp(), false,
		[]uint16{7})
	result, err = runUnsignedDecimalAdmission(proc, from, target, types.T_enum.ToType(), 1,
		unsignedDecimalAdmissionStrict, "d")
	require.NoError(t, err)
	vecE := vector.MustFunctionResult[types.Enum](result).GetResultVector()
	require.Equal(t, types.Enum(7), vector.GetFixedAtWithTypeCheck[types.Enum](vecE, 0))

	require.Empty(t, session.warnings)

	// Kinds outside every admission group are rejected loudly.
	from = testutil.NewVector(1, types.T_uuid.ToType(), proc.Mp(), false,
		[]types.Uuid{{}})
	_, err = runUnsignedDecimalAdmission(proc, from, target, types.T_uuid.ToType(), 1,
		unsignedDecimalAdmissionStrict, "d")
	require.ErrorContains(t, err, "is not supported")
}

// TestUnsignedDecimalAdmissionSelectListSkipsRows checks skipped rows
// materialize as NULL with no classification or diagnostics in every
// admission shape.
func TestUnsignedDecimalAdmissionSelectListSkipsRows(t *testing.T) {
	// SelectList[i] == false marks row i skipped (Contains negates it).
	skip0 := &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}}
	run := func(from *vector.Vector, resultType types.Type, proc *process.Process, entry unsignedDecimalAdmissionEntry) (vector.FunctionResultWrapper, error) {
		targetCarrier := vector.NewVec(types.New(types.T_decimal64, 10, 2))
		nameCarrier, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("s"), 2, proc.Mp())
		require.NoError(t, err)
		result := vector.NewFunctionResultWrapper(resultType, proc.Mp())
		require.NoError(t, result.PreExtendAndReset(2))
		err = newUnsignedDecimalAdmission(
			[]*vector.Vector{from, targetCarrier, nameCarrier}, result, proc, 2, skip0, entry)
		return result, err
	}

	// Fixed-size shape: the skipped row carries a negative that must NOT
	// reject, and the evaluated row passes through.
	t.Run("fixed", func(t *testing.T) {
		proc, session := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
		typ := types.T_int8.ToType()
		from := testutil.NewVector(2, typ, proc.Mp(), false, []int8{-1, 5})
		result, err := run(from, typ, proc, unsignedDecimalAdmissionStrict)
		require.NoError(t, err)
		vec := vector.MustFunctionResult[int8](result).GetResultVector()
		require.True(t, vec.GetNulls().Contains(0), "skipped row materializes as NULL")
		require.Equal(t, int8(5), vector.GetFixedAtWithTypeCheck[int8](vec, 1))
		require.Empty(t, session.warnings)
	})

	// JSON shape.
	t.Run("json", func(t *testing.T) {
		proc, session := unsignedDecimalAdmissionProc(t, "ONLY_FULL_GROUP_BY")
		jsonT := types.T_json.ToType()
		encode := func(s string) []byte {
			bj, err := types.ParseStringToByteJson(s)
			require.NoError(t, err)
			enc, err := types.EncodeJson(bj)
			require.NoError(t, err)
			return enc
		}
		from := vector.NewVec(jsonT)
		require.NoError(t, vector.AppendBytes(from, encode("-1"), false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(from, encode("2"), false, proc.Mp()))
		result, err := run(from, jsonT, proc, unsignedDecimalAdmissionIgnore)
		require.NoError(t, err)
		vec := vector.MustFunctionResult[types.Varlena](result).GetResultVector()
		require.True(t, vec.GetNulls().Contains(0))
		require.Empty(t, session.warnings, "the skipped negative earns no clip warning")
	})

	// Binary shape.
	t.Run("binary", func(t *testing.T) {
		proc, session := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
		binT := types.T_varbinary.ToType()
		from := testutil.NewVector(2, binT, proc.Mp(), false,
			[]string{string([]byte{0x00, 0x96}), string([]byte{0x00, 0x96})})
		from.SetIsBin(true)
		result, err := run(from, binT, proc, unsignedDecimalAdmissionStrict)
		require.NoError(t, err)
		vec := vector.MustFunctionResult[types.Varlena](result).GetResultVector()
		require.True(t, vec.GetNulls().Contains(0))
		require.Equal(t, "1.50", vec.GetStringAt(1), "150 renders 1.50 at scale 2")
		require.Empty(t, session.warnings)
	})

	// Text varlena shape.
	t.Run("varlena", func(t *testing.T) {
		proc, session := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
		strT := types.T_varchar.ToType()
		from := testutil.NewVector(2, strT, proc.Mp(), false, []string{"-1.5", "2.5"})
		result, err := run(from, strT, proc, unsignedDecimalAdmissionStrict)
		require.NoError(t, err)
		vec := vector.MustFunctionResult[types.Varlena](result).GetResultVector()
		require.True(t, vec.GetNulls().Contains(0))
		require.Equal(t, "2.5", vec.GetStringAt(1))
		require.Empty(t, session.warnings)
	})
}

// TestUnsignedDecimalAdmissionBinaryDecimal128And256 walks the binary decode
// and re-render through the DECIMAL128 and DECIMAL256 FromByte branches,
// including the decoder's own failure surfacing unchanged.
func TestUnsignedDecimalAdmissionBinaryDecimal128And256(t *testing.T) {
	binT := types.T_varbinary.ToType()
	p150 := []byte{0x00, 0x00, 0x00, 0x96} // 150 -> 1.50 at scale 2

	newBinVec := func(proc *process.Process, payloads ...[]byte) *vector.Vector {
		strs := make([]string, len(payloads))
		for i, p := range payloads {
			strs[i] = string(p)
		}
		vec := testutil.NewVector(len(payloads), binT, proc.Mp(), false, strs)
		vec.SetIsBin(true)
		return vec
	}

	// DECIMAL(20,2) decodes through ParseDecimal128FromByte.
	proc, session := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
	typ128 := types.New(types.T_decimal128, 20, 2)
	from := newBinVec(proc, p150)
	result, err := runUnsignedDecimalAdmission(proc, from, typ128, binT, 1,
		unsignedDecimalAdmissionStrict, "b")
	require.NoError(t, err)
	vec := vector.MustFunctionResult[types.Varlena](result).GetResultVector()
	require.Equal(t, "1.50", vec.GetStringAt(0))

	from = newBinVec(proc, []byte{})
	_, err = runUnsignedDecimalAdmission(proc, from, typ128, binT, 1,
		unsignedDecimalAdmissionStrict, "b")
	require.Error(t, err, "the DECIMAL128 decoder's own failure propagates unchanged")

	// DECIMAL(38,2) decodes through ParseDecimal256FromByte.
	typ256 := types.New(types.T_decimal256, 38, 2)
	from = newBinVec(proc, p150)
	result, err = runUnsignedDecimalAdmission(proc, from, typ256, binT, 1,
		unsignedDecimalAdmissionStrict, "b")
	require.NoError(t, err)
	vec = vector.MustFunctionResult[types.Varlena](result).GetResultVector()
	require.Equal(t, "1.50", vec.GetStringAt(0))

	from = newBinVec(proc, []byte{})
	_, err = runUnsignedDecimalAdmission(proc, from, typ256, binT, 1,
		unsignedDecimalAdmissionStrict, "b")
	require.Error(t, err, "the DECIMAL256 decoder's own failure propagates unchanged")
	require.Empty(t, session.warnings)
}

// TestUnsignedDecimalAdmissionMySQLPrefixCharsetSource checks the private
// numeric-marker charset binds the MySQL decimal prefix grammar for plain
// varlena sources: the leading prefix's sign owns the domain decision.
func TestUnsignedDecimalAdmissionMySQLPrefixCharsetSource(t *testing.T) {
	typ := types.New(types.T_decimal128, 10, 2)
	typ.Charset = 255
	strT := types.T_varchar.ToType()

	proc, session := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
	from := testutil.NewVector(2, strT, proc.Mp(), false, []string{"1.5", "2.50"})
	result, err := runUnsignedDecimalAdmission(proc, from, typ, strT, 2,
		unsignedDecimalAdmissionStrict, "v")
	require.NoError(t, err)
	vec := vector.MustFunctionResult[types.Varlena](result).GetResultVector()
	require.Equal(t, "1.5", vec.GetStringAt(0))
	require.Equal(t, "2.50", vec.GetStringAt(1))

	from = testutil.NewVector(1, strT, proc.Mp(), false, []string{"-1.5"})
	_, err = runUnsignedDecimalAdmission(proc, from, typ, strT, 1,
		unsignedDecimalAdmissionStrict, "v")
	requireOutOfRangeForColumn(t, err, "v", 1)
	require.Empty(t, session.warnings)
}

// TestUnsignedDecimalAdmissionStringOutcomeBranches walks the text
// classifier's remaining outcomes: clean non-negative pass, the IGNORE
// retained-prefix clip at DECIMAL128/DECIMAL256 destinations, and the
// abstain for an overflowing retained prefix.
func TestUnsignedDecimalAdmissionStringOutcomeBranches(t *testing.T) {
	strT := types.T_varchar.ToType()

	// Clean non-negative tokens pass with no diagnostics.
	proc, session := unsignedDecimalAdmissionProc(t, "ONLY_FULL_GROUP_BY")
	typ := types.New(types.T_decimal64, 10, 2)
	from := testutil.NewVector(2, strT, proc.Mp(), false, []string{"1.5", "2.00"})
	result, err := runUnsignedDecimalAdmission(proc, from, typ, strT, 2,
		unsignedDecimalAdmissionIgnore, "v")
	require.NoError(t, err)
	vec := vector.MustFunctionResult[types.Varlena](result).GetResultVector()
	require.Equal(t, "1.5", vec.GetStringAt(0))
	require.Equal(t, "2.00", vec.GetStringAt(1))
	require.Empty(t, session.warnings)

	// IGNORE retained-prefix clip at a DECIMAL128 destination: truncation
	// diagnostic first, exactly one 1264 admission warning second.
	proc128, session128 := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
	typ128 := types.New(types.T_decimal128, 10, 2)
	from = testutil.NewVector(1, strT, proc128.Mp(), false, []string{"-1junk"})
	result, err = runUnsignedDecimalAdmission(proc128, from, typ128, strT, 1,
		unsignedDecimalAdmissionIgnore, "v")
	require.NoError(t, err)
	vec = vector.MustFunctionResult[types.Varlena](result).GetResultVector()
	require.Equal(t, "0", vec.GetStringAt(0))
	require.Len(t, session128.warnings, 2)
	require.Equal(t, moerr.WARN_DATA_TRUNCATED, session128.warnings[0].code)
	requireAdmissionWarning(t, session128.warnings[1], "v", 1)

	// The same clip at a DECIMAL256 destination.
	proc256, session256 := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
	typ256 := types.New(types.T_decimal256, 38, 2)
	from = testutil.NewVector(1, strT, proc256.Mp(), false, []string{"-1junk"})
	result, err = runUnsignedDecimalAdmission(proc256, from, typ256, strT, 1,
		unsignedDecimalAdmissionIgnore, "v")
	require.NoError(t, err)
	vec = vector.MustFunctionResult[types.Varlena](result).GetResultVector()
	require.Equal(t, "0", vec.GetStringAt(0))
	require.Len(t, session256.warnings, 2)
	requireAdmissionWarning(t, session256.warnings[1], "v", 1)

	// An overflowing retained prefix keeps the cast's own diagnostics:
	// admission abstains and passes the row through unchanged.
	procNarrow, sessionNarrow := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
	narrow := types.New(types.T_decimal64, 4, 0)
	from = testutil.NewVector(1, strT, procNarrow.Mp(), false, []string{"-99999junk"})
	result, err = runUnsignedDecimalAdmission(procNarrow, from, narrow, strT, 1,
		unsignedDecimalAdmissionIgnore, "v")
	require.NoError(t, err)
	vec = vector.MustFunctionResult[types.Varlena](result).GetResultVector()
	require.Equal(t, "-99999junk", vec.GetStringAt(0),
		"the cast owns the overflow diagnostic for an unretainable prefix")
	require.Empty(t, sessionNarrow.warnings, "abstention owns no admission diagnostics")
}

// TestUnsignedDecimalAdmissionVarlenaNullRows checks NULL text rows pass as
// NULL without classification in the varlena admission loop.
func TestUnsignedDecimalAdmissionVarlenaNullRows(t *testing.T) {
	proc, session := unsignedDecimalAdmissionProc(t, "STRICT_TRANS_TABLES")
	typ := types.New(types.T_decimal64, 10, 2)
	strT := types.T_varchar.ToType()
	from := vector.NewVec(strT)
	require.NoError(t, vector.AppendBytes(from, []byte("1.5"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(from, nil, true, proc.Mp()))

	result, err := runUnsignedDecimalAdmission(proc, from, typ, strT, 2,
		unsignedDecimalAdmissionStrict, "v")
	require.NoError(t, err)
	vec := vector.MustFunctionResult[types.Varlena](result).GetResultVector()
	require.Equal(t, "1.5", vec.GetStringAt(0))
	require.True(t, vec.GetNulls().Contains(1))
	require.Empty(t, session.warnings)
}

// TestAppendUnsignedDecimalOutOfRangeWarningNilProcess pins the lenient
// warning helper's nil-process guard: a composition without a session is a
// pure no-op rather than a panic.
func TestAppendUnsignedDecimalOutOfRangeWarningNilProcess(t *testing.T) {
	require.NotPanics(t, func() {
		appendUnsignedDecimalOutOfRangeWarning(nil, "v", 0)
	})
}
