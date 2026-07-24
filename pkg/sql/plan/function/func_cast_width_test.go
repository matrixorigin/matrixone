// Copyright 2021 Matrix Origin
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
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type castWarningTestSession struct {
	codes []uint16
	msgs  []string
}

func (s *castWarningTestSession) GetTempTable(string, string) (string, bool) { return "", false }
func (s *castWarningTestSession) AddTempTable(string, string, string)        {}
func (s *castWarningTestSession) RemoveTempTable(string, string)             {}
func (s *castWarningTestSession) RemoveTempTableByRealName(string)           {}
func (s *castWarningTestSession) GetSqlModeNoAutoValueOnZero() (bool, bool)  { return false, false }
func (s *castWarningTestSession) AddWarning(code uint16, msg string) {
	s.codes = append(s.codes, code)
	s.msgs = append(s.msgs, msg)
}

func runStrToStrWidth(t *testing.T, mp *mpool.MPool, proc *process.Process, input string, toType types.Type, strict, allowTrim bool) (string, bool, error) {
	t.Helper()
	src := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(src, []byte(input), false, mp))
	defer src.Free(mp)

	from := vector.GenerateFunctionStrParameter(src)
	rsw := vector.NewFunctionResultWrapper(toType, mp)
	to := rsw.(*vector.FunctionResult[types.Varlena])
	defer to.Free()
	require.NoError(t, to.PreExtendAndReset(1))

	if err := strToStr(context.Background(), proc, from, to, 1, toType, strict, allowTrim, allowTrim); err != nil {
		return "", false, err
	}
	got, null := vector.GenerateFunctionStrParameter(to.GetResultVector()).GetStrValue(0)
	return string(got), null, nil
}

// TestStrToStrWidthEnforcement covers the CHAR/VARCHAR over-length matrix:
// strict vs non-strict, and the trailing-space exemption (allowTrailingSpaceTrim).
func TestStrToStrWidthEnforcement(t *testing.T) {
	mp := mpool.MustNewZero()
	vc3 := types.New(types.T_varchar, 3, 0)

	cases := []struct {
		name      string
		input     string
		strict    bool
		allowTrim bool
		want      string
		wantErr   bool
	}{
		{"fits", "abc", true, true, "abc", false},
		{"strict_overlen_reject", "abcd", true, false, "", true},
		{"strict_overlen_reject_even_with_trimflag", "abcd", true, true, "", true},
		{"nonstrict_truncate", "abcd", false, true, "abc", false},
		{"nonstrict_truncate_no_trimflag", "abcd", false, false, "abc", false},
		{"trailing_space_exempt_under_strict", "abc   ", true, true, "abc", false},
		{"trailing_space_not_exempt_for_ddl", "abc   ", true, false, "", true},
		{"trailing_space_exempt_multibyte", "你好世   ", true, true, "你好世", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			got, _, err := runStrToStrWidth(t, mp, proc, c.input, vc3, c.strict, c.allowTrim)
			if c.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}
}

func TestZeroWidthCharVarcharAssignment(t *testing.T) {
	type castFunc func(
		[]*vector.Vector,
		vector.FunctionResultWrapper,
		*process.Process,
		int,
		*FunctionSelectList,
	) error

	run := func(
		t *testing.T,
		sourceType types.T,
		targetType types.T,
		sqlMode string,
		input string,
		cast castFunc,
	) (string, error) {
		t.Helper()
		proc := testutil.NewProcess(t)
		proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
			require.Equal(t, "sql_mode", name)
			return sqlMode, nil
		})

		var src *vector.Vector
		switch sourceType {
		case types.T_text:
			src = vector.NewVec(types.T_text.ToType())
			require.NoError(t, vector.AppendBytes(src, []byte(input), false, proc.Mp()))
		case types.T_int64:
			src = vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixed(src, int64(1), false, proc.Mp()))
		case types.T_bool:
			src = vector.NewVec(types.T_bool.ToType())
			require.NoError(t, vector.AppendFixed(src, true, false, proc.Mp()))
		default:
			t.Fatalf("unsupported zero-width test source %s", sourceType)
		}
		defer src.Free(proc.Mp())

		target := types.New(targetType, 0, 0)
		dst := vector.NewVec(target)
		defer dst.Free(proc.Mp())
		result := vector.NewFunctionResultWrapper(target, proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(1))

		if err := cast([]*vector.Vector{src, dst}, result, proc, 1, nil); err != nil {
			return "", err
		}
		got, null := vector.GenerateFunctionStrParameter(result.GetResultVector()).GetStrValue(0)
		require.False(t, null)
		return string(got), nil
	}

	for _, targetType := range []types.T{types.T_char, types.T_varchar} {
		for _, source := range []struct {
			name  string
			typ   types.T
			input string
		}{
			{name: "string", typ: types.T_text, input: "x"},
			{name: "integer", typ: types.T_int64},
			{name: "boolean", typ: types.T_bool},
		} {
			t.Run(targetType.String()+"/"+source.name+"/strict", func(t *testing.T) {
				_, err := run(
					t, source.typ, targetType, "STRICT_TRANS_TABLES", source.input, NewAssignCast)
				require.Error(t, err)
				moErr := err.(*moerr.Error)
				require.Equal(t, moerr.ErrCastWidthExceeded, moErr.ErrorCode())
				require.Equal(t, uint16(moerr.ER_DATA_TOO_LONG), moErr.MySQLCode())
			})
			t.Run(targetType.String()+"/"+source.name+"/nonstrict", func(t *testing.T) {
				got, err := run(t, source.typ, targetType, "", source.input, NewAssignCast)
				require.NoError(t, err)
				require.Empty(t, got)
			})
			t.Run(targetType.String()+"/"+source.name+"/ignore", func(t *testing.T) {
				got, err := run(
					t, source.typ, targetType, "STRICT_TRANS_TABLES", source.input, NewAssignIgnoreCast)
				require.NoError(t, err)
				require.Empty(t, got)
			})
		}

		t.Run(targetType.String()+"/empty_string/strict", func(t *testing.T) {
			got, err := run(t, types.T_text, targetType, "STRICT_TRANS_TABLES", "", NewAssignCast)
			require.NoError(t, err)
			require.Empty(t, got)
		})
	}
}

func TestTruncateCastBytesResultWidthBounds(t *testing.T) {
	cases := []struct {
		name   string
		input  string
		target types.Type
		strict bool
		want   string
	}{
		{name: "char_zero", input: "x", target: types.New(types.T_char, 0, 0), want: ""},
		{name: "varchar_zero", input: "x", target: types.New(types.T_varchar, 0, 0), want: ""},
		{name: "zero_empty", target: types.New(types.T_varchar, 0, 0), want: ""},
		{name: "strict_defers_error", input: "x", target: types.New(types.T_varchar, 0, 0), strict: true, want: "x"},
		{name: "negative_is_unbounded", input: "x", target: types.New(types.T_varchar, -1, 0), want: "x"},
		{name: "text_is_unbounded", input: "x", target: types.T_text.ToType(), want: "x"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := truncateCastBytesResult(
				context.Background(), []byte(c.input), c.target, c.strict)
			require.Equal(t, c.want, string(got))
		})
	}
}

func TestOverLenIsAllTrailingSpaces(t *testing.T) {
	require.True(t, overLenIsAllTrailingSpaces("abc ", 3))
	require.True(t, overLenIsAllTrailingSpaces("abc   ", 3))
	require.True(t, overLenIsAllTrailingSpaces("你好世 ", 3))
	require.False(t, overLenIsAllTrailingSpaces("abcd", 3))
	require.False(t, overLenIsAllTrailingSpaces("ab cd", 3)) // excess runes 'c','d' are not spaces
	require.False(t, overLenIsAllTrailingSpaces("abc", 3))   // not over-length
	require.False(t, overLenIsAllTrailingSpaces("ab", 3))    // shorter
}

func TestIsStrictSqlMode(t *testing.T) {
	require.True(t, isStrictSqlMode(nil))

	proc := testutil.NewProcess(t)
	set := func(v interface{}, err error) {
		proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return v, err })
	}

	set("STRICT_TRANS_TABLES", nil)
	require.True(t, isStrictSqlMode(proc))
	set("STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION", nil)
	require.True(t, isStrictSqlMode(proc))
	set("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES", nil)
	require.True(t, isStrictSqlMode(proc))
	set("TRADITIONAL", nil)
	require.True(t, isStrictSqlMode(proc))
	set("ansi, traditional ", nil)
	require.True(t, isStrictSqlMode(proc))

	set("", nil)
	require.False(t, isStrictSqlMode(proc))
	set("NO_ENGINE_SUBSTITUTION", nil)
	require.False(t, isStrictSqlMode(proc))
	set("NOT_STRICT_TRANS_TABLES", nil)
	require.False(t, isStrictSqlMode(proc))

	// error / nil / non-string default to strict (safe fallback)
	set(nil, moerr.NewInternalErrorNoCtx("boom"))
	require.True(t, isStrictSqlMode(proc))
	set(nil, nil)
	require.True(t, isStrictSqlMode(proc))
	set(123, nil)
	require.True(t, isStrictSqlMode(proc))
}

func castTextToVarchar3(t *testing.T, proc *process.Process, input string) (string, error) {
	t.Helper()
	vc3 := types.New(types.T_varchar, 3, 0)
	src := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(src, []byte(input), false, proc.Mp()))
	dst := vector.NewVec(vc3)
	rs := vector.NewFunctionResultWrapper(vc3, proc.Mp())
	defer rs.Free()
	require.NoError(t, rs.PreExtendAndReset(1))
	if err := NewAssignCast([]*vector.Vector{src, dst}, rs, proc, 1, nil); err != nil {
		return "", err
	}
	got, _ := vector.GenerateFunctionStrParameter(rs.GetResultVector()).GetStrValue(0)
	return string(got), nil
}

// TestNewAssignCastHonorsSqlMode verifies the DML assignment cast (cast_assign)
// gates the width check on sql_mode at runtime, applies the trailing-space
// exemption under strict mode, and that the DDL cast (cast_strict) does not.
func TestNewAssignCastHonorsSqlMode(t *testing.T) {
	newProc := func(sqlMode string) *process.Process {
		proc := testutil.NewProcess(t)
		proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
			if name == "sql_mode" {
				return sqlMode, nil
			}
			return nil, moerr.NewInternalError(proc.Ctx, "unexpected variable "+name)
		})
		return proc
	}

	// strict mode: over-length non-space value is rejected.
	_, err := castTextToVarchar3(t, newProc("STRICT_TRANS_TABLES"), "abcd")
	require.Error(t, err)

	// non-strict mode: over-length value is truncated, no error.
	got, err := castTextToVarchar3(t, newProc(""), "abcd")
	require.NoError(t, err)
	require.Equal(t, "abc", got)

	// strict mode: trailing-space-only overflow is accepted (truncated).
	got, err = castTextToVarchar3(t, newProc("STRICT_TRANS_TABLES"), "abc   ")
	require.NoError(t, err)
	require.Equal(t, "abc", got)

	// ALTER COPY executes an internal INSERT, but keeps its legacy internal
	// error contract instead of exposing the DML-only 1406 mapping.
	alterProc := newProc("STRICT_TRANS_TABLES")
	alterProc.Ctx = context.WithValue(alterProc.Ctx, defines.AlterCopyOpt{}, &plan.AlterCopyOpt{})
	_, err = castTextToVarchar3(t, alterProc, "abcd")
	require.Error(t, err)
	require.Equal(t, moerr.ErrInternal, err.(*moerr.Error).ErrorCode())

	// DDL cast (NewStrictCast) never applies the trailing-space exemption.
	proc := testutil.NewProcess(t)
	src := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(src, []byte("abc   "), false, proc.Mp()))
	dst := vector.NewVec(types.New(types.T_varchar, 3, 0))
	rs := vector.NewFunctionResultWrapper(types.New(types.T_varchar, 3, 0), proc.Mp())
	defer rs.Free()
	require.NoError(t, rs.PreExtendAndReset(1))
	require.Error(t, NewStrictCast([]*vector.Vector{src, dst}, rs, proc, 1, nil))
}

func TestAssignmentCastTypedSourceAndWarnings(t *testing.T) {
	ignoreFunction, err := GetFunctionByName(
		context.Background(),
		"cast_ignore",
		[]types.Type{types.T_int64.ToType(), types.New(types.T_varchar, 3, 0)},
	)
	require.NoError(t, err)
	functionID, _ := DecodeOverloadID(ignoreFunction.GetEncodedOverloadID())
	require.Equal(t, int32(CAST_IGNORE), functionID)

	newProc := func(sqlMode string) (*process.Process, *castWarningTestSession) {
		proc := testutil.NewProcess(t)
		session := &castWarningTestSession{}
		proc.Session = session
		proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
			require.Equal(t, "sql_mode", name)
			return sqlMode, nil
		})
		return proc, session
	}
	run := func(
		proc *process.Process,
		cast func([]*vector.Vector, vector.FunctionResultWrapper, *process.Process, int, *FunctionSelectList) error,
	) (string, error) {
		src := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(src, int64(12345), false, proc.Mp()))
		defer src.Free(proc.Mp())
		target := types.New(types.T_varchar, 3, 0)
		dst := vector.NewVec(target)
		defer dst.Free(proc.Mp())
		result := vector.NewFunctionResultWrapper(target, proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(1))
		if err := cast([]*vector.Vector{src, dst}, result, proc, 1, nil); err != nil {
			return "", err
		}
		got, _ := vector.GenerateFunctionStrParameter(result.GetResultVector()).GetStrValue(0)
		return string(got), nil
	}

	strictProc, strictWarnings := newProc("STRICT_TRANS_TABLES")
	_, err = run(strictProc, NewAssignCast)
	require.Error(t, err)
	moErr := err.(*moerr.Error)
	require.Equal(t, moerr.ErrCastWidthExceeded, moErr.ErrorCode())
	require.Equal(t, uint16(moerr.ER_DATA_TOO_LONG), moErr.MySQLCode())
	require.Equal(t, "22001", moErr.SqlState())
	require.Empty(t, strictWarnings.codes)

	nonStrictProc, nonStrictWarnings := newProc("")
	got, err := run(nonStrictProc, NewAssignCast)
	require.NoError(t, err)
	require.Equal(t, "123", got)
	require.Equal(t, []uint16{moerr.WARN_DATA_TRUNCATED}, nonStrictWarnings.codes)

	ignoreProc, ignoreWarnings := newProc("STRICT_TRANS_TABLES")
	got, err = run(ignoreProc, NewAssignIgnoreCast)
	require.NoError(t, err)
	require.Equal(t, "123", got)
	require.Equal(t, []uint16{moerr.WARN_DATA_TRUNCATED}, ignoreWarnings.codes)

	explicitProc, explicitWarnings := newProc("STRICT_TRANS_TABLES")
	got, err = run(explicitProc, NewExplicitCast)
	require.NoError(t, err)
	require.Equal(t, "123", got)
	require.Equal(t, []uint16{moerr.ER_TRUNCATED_WRONG_VALUE}, explicitWarnings.codes)
}

func TestMultibyteAssignmentErrorUsesCharacterLength(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return "STRICT_TRANS_TABLES", nil
	})
	_, err := castTextToVarchar3(t, proc, "你好世界")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "Src length 4"))
	require.False(t, strings.Contains(err.Error(), "Src length 12"))
}

func castGeometryToVarchar(
	t *testing.T,
	proc *process.Process,
	input string,
	width int32,
	cast func(
		[]*vector.Vector,
		vector.FunctionResultWrapper,
		*process.Process,
		int,
		*FunctionSelectList,
	) error,
) (string, error) {
	t.Helper()
	toType := types.New(types.T_varchar, width, 0)
	src := vector.NewVec(types.T_geometry.ToType())
	require.NoError(t, vector.AppendBytes(src, encodeGeometryPayload(input, 0, false), false, proc.Mp()))
	defer src.Free(proc.Mp())
	dst := vector.NewVec(toType)
	defer dst.Free(proc.Mp())
	rs := vector.NewFunctionResultWrapper(toType, proc.Mp())
	defer rs.Free()
	require.NoError(t, rs.PreExtendAndReset(1))
	if err := cast([]*vector.Vector{src, dst}, rs, proc, 1, nil); err != nil {
		return "", err
	}
	got, _ := vector.GenerateFunctionStrParameter(rs.GetResultVector()).GetStrValue(0)
	return string(got), nil
}

func TestGeometryToVarcharWidthEnforcement(t *testing.T) {
	newProc := func(sqlMode string) *process.Process {
		proc := testutil.NewProcess(t)
		proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
			require.Equal(t, "sql_mode", name)
			return sqlMode, nil
		})
		return proc
	}

	_, err := castGeometryToVarchar(
		t,
		newProc("STRICT_TRANS_TABLES"),
		"POINT(1 2)",
		3,
		NewAssignCast,
	)
	require.Error(t, err)
	require.Equal(t, moerr.ErrCastWidthExceeded, err.(*moerr.Error).ErrorCode())

	got, err := castGeometryToVarchar(t, newProc(""), "POINT(1 2)", 3, NewAssignCast)
	require.NoError(t, err)
	require.Equal(t, "POI", got)

	got, err = castGeometryToVarchar(
		t,
		testutil.NewProcess(t),
		"POINT(1 2)",
		3,
		NewExplicitCast,
	)
	require.NoError(t, err)
	require.Equal(t, "POI", got)
}

func runJSONToStrWidth(t *testing.T, mp *mpool.MPool, jsonText string, toType types.Type, strict, allowTrim bool) (string, error) {
	t.Helper()
	encoded := makeJSONEncodedFromText(t, []string{jsonText}, nil)[0]
	src := vector.NewVec(types.T_json.ToType())
	require.NoError(t, vector.AppendBytes(src, []byte(encoded), false, mp))
	defer src.Free(mp)

	from := vector.GenerateFunctionStrParameter(src)
	rsw := vector.NewFunctionResultWrapper(toType, mp)
	to := rsw.(*vector.FunctionResult[types.Varlena])
	defer to.Free()
	require.NoError(t, to.PreExtendAndReset(1))

	if err := jsonToStr(context.Background(), from, to, 1, nil, strict, allowTrim, allowTrim); err != nil {
		return "", err
	}
	got, _ := vector.GenerateFunctionStrParameter(to.GetResultVector()).GetStrValue(0)
	return string(got), nil
}

// TestJSONToStrWidthEnforcement mirrors TestStrToStrWidthEnforcement for the
// JSON->CHAR/VARCHAR path: the trailing-space exemption, sql_mode gating, and
// the DML(1406)/DDL(1067) split must apply to JSON sources too.
func TestJSONToStrWidthEnforcement(t *testing.T) {
	mp := mpool.MustNewZero()
	vc3 := types.New(types.T_varchar, 3, 0)

	cases := []struct {
		name      string
		jsonText  string
		strict    bool
		allowTrim bool
		want      string
		wantErr   bool
		errCode   uint16 // 0 = not asserted
	}{
		{"fits", `"abc"`, true, true, "abc", false, 0},
		{"trailing_space_exempt_strict_dml", `"abc   "`, true, true, "abc", false, 0},
		{"real_overlen_dml_reject", `"abcd"`, true, true, "", true, moerr.ErrCastWidthExceeded},
		{"real_overlen_ddl_reject", `"abcd"`, true, false, "", true, moerr.ErrInvalidDefault},
		{"nonstrict_truncate", `"abcd"`, false, true, "abc", false, 0},
		{"trailing_space_not_exempt_ddl", `"abc   "`, true, false, "", true, moerr.ErrInvalidDefault},
		{"multibyte_trailing_space_exempt", `"你好世   "`, true, true, "你好世", false, 0},
		{"multibyte_fits_strict", `"你好"`, true, true, "你好", false, 0},
		{"multibyte_fits_nonstrict", `"你好"`, false, true, "你好", false, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := runJSONToStrWidth(t, mp, c.jsonText, vc3, c.strict, c.allowTrim)
			if c.wantErr {
				require.Error(t, err)
				if c.errCode != 0 {
					require.Equal(t, c.errCode, err.(*moerr.Error).ErrorCode())
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}
}
