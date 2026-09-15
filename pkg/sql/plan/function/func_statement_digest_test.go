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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestStatementDigestTextVectorAndNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{NewFunctionTestInput(
			types.T_varchar.ToType(),
			[]string{"SELECT 1", "SELECT 2 /* comment */ WHERE 10=20", ""},
			[]bool{false, false, true},
		)},
		NewFunctionTestResult(
			types.T_text.ToType(), false,
			[]string{"SELECT ?", "SELECT ? WHERE ? = ?", ""},
			[]bool{false, false, true},
		),
		StatementDigestText,
	)
	ok, info := testCase.Run()
	require.True(t, ok, info)
}

func TestStatementDigestTextErrors(t *testing.T) {
	proc := testutil.NewProcess(t)

	constInput, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("SELECT ?"), 1, proc.Mp())
	require.NoError(t, err)
	defer constInput.Free(proc.Mp())
	require.NoError(t, constInput.SetStringSource(types.StringSourceLiteral))
	fn, err := GetFunctionByName(proc.Ctx, "statement_digest_text", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)
	_, err = RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{constInput}, 1)
	require.Error(t, err)
	require.Equal(t, uint16(moerr.ER_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode())

	proc.SetPrepareParams(constInput)
	_, err = RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{constInput}, 1)
	require.Error(t, err)
	require.Equal(t, uint16(moerr.ER_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode())
	proc.SetPrepareParams(nil)

	dynamicInput := vector.NewVec(types.T_varchar.ToType())
	defer dynamicInput.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(dynamicInput, []byte("SELECT ?"), false, proc.Mp()))
	_, err = RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{dynamicInput}, 1)
	require.Error(t, err)
	require.Equal(t, uint16(moerr.ER_UNDISCLOSED_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode())
}

func TestStatementDigestTextIsRuntimeDependent(t *testing.T) {
	proc := testutil.NewProcess(t)
	fn, err := GetFunctionByName(proc.Ctx, "statement_digest_text", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)
	ov, err := GetFunctionById(proc.Ctx, fn.GetEncodedOverloadID())
	require.NoError(t, err)
	require.True(t, ov.CannotFold())
	require.True(t, ov.IsRealTimeRelated())
}

func TestStatementDigestTextBinaryCompatibility(t *testing.T) {
	proc := testutil.NewProcess(t)
	nullInput := vector.NewConstNull(types.T_blob.ToType(), 1, proc.Mp())
	fn, err := GetFunctionByName(proc.Ctx, "statement_digest_text", []types.Type{types.T_blob.ToType()})
	require.NoError(t, err)
	nullResult, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{nullInput}, 1)
	nullInput.Free(proc.Mp())
	require.NoError(t, err)
	require.True(t, nullResult.IsNull(0))
	nullResult.Free(proc.Mp())

	for _, oid := range []types.T{types.T_binary, types.T_varbinary, types.T_blob} {
		t.Run(oid.String(), func(t *testing.T) {
			fn, err := GetFunctionByName(proc.Ctx, "statement_digest_text", []types.Type{oid.ToType()})
			require.NoError(t, err)
			valid, err := vector.NewConstBytes(oid.ToType(), []byte("SELECT 1"), 1, proc.Mp())
			require.NoError(t, err)
			result, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{valid}, 1)
			valid.Free(proc.Mp())
			require.NoError(t, err)
			require.Equal(t, "SELECT ?", result.GetStringAt(0))
			result.Free(proc.Mp())

			invalid, err := vector.NewConstBytes(oid.ToType(), []byte{0xff, 0x00, 0xc3, 0x28}, 1, proc.Mp())
			require.NoError(t, err)
			_, err = RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{invalid}, 1)
			invalid.Free(proc.Mp())
			require.Error(t, err)
			require.Equal(t, uint16(moerr.ER_UNDISCLOSED_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode())
		})
	}

	for _, oid := range []types.T{types.T_geometry, types.T_geometry32} {
		t.Run(oid.String(), func(t *testing.T) {
			nullInput := vector.NewConstNull(oid.ToType(), 1, proc.Mp())
			fn, err := GetFunctionByName(proc.Ctx, "statement_digest_text", []types.Type{oid.ToType()})
			require.NoError(t, err)
			nullResult, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{nullInput}, 1)
			nullInput.Free(proc.Mp())
			require.NoError(t, err)
			require.True(t, nullResult.IsNull(0))
			nullResult.Free(proc.Mp())

			input, err := vector.NewConstBytes(oid.ToType(), []byte("SELECT 1"), 1, proc.Mp())
			require.NoError(t, err)
			_, err = RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input}, 1)
			input.Free(proc.Mp())
			require.Error(t, err)
			require.Equal(t, uint16(moerr.ER_UNDISCLOSED_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode())
		})
	}

	textInput, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("SELECT 1"), 1, proc.Mp())
	require.NoError(t, err)
	defer textInput.Free(proc.Mp())
	textInput.SetIsBinaryString(true)
	binaryFn, err := GetFunctionByName(proc.Ctx, "statement_digest_text", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)
	textResult, err := RunFunctionDirectly(proc, binaryFn.GetEncodedOverloadID(), []*vector.Vector{textInput}, 1)
	require.NoError(t, err)
	require.Equal(t, "SELECT ?", textResult.GetStringAt(0))
	textResult.Free(proc.Mp())
}

func TestStatementDigestTextMalformedUTF8AlwaysUndisclosed(t *testing.T) {
	proc := testutil.NewProcess(t)
	fn, err := GetFunctionByName(proc.Ctx, "statement_digest_text", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)

	for _, source := range []types.StringSource{
		types.StringSourceLiteral,
		types.StringSourceExpression,
		types.StringSourceSQLPrepare,
	} {
		t.Run(fmt.Sprintf("source-%d", source), func(t *testing.T) {
			input, err := vector.NewConstBytes(
				types.T_varchar.ToType(), []byte{'S', 'E', 'L', 'E', 'C', 'T', ' ', 0xff, 0xc3, 0x28}, 1, proc.Mp(),
			)
			require.NoError(t, err)
			defer input.Free(proc.Mp())
			require.NoError(t, input.SetStringSource(source))

			_, err = RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input}, 1)
			require.Error(t, err)
			require.Equal(
				t, uint16(moerr.ER_UNDISCLOSED_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode(),
			)
		})
	}

	// Valid UTF-8 malformed SQL remains disclosed for a direct literal; the
	// encoding guard must not broaden suppression to ordinary parser errors.
	control, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("SELECT ?"), 1, proc.Mp())
	require.NoError(t, err)
	defer control.Free(proc.Mp())
	require.NoError(t, control.SetStringSource(types.StringSourceLiteral))
	_, err = RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{control}, 1)
	require.Error(t, err)
	require.Equal(t, uint16(moerr.ER_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode())
}

func TestStatementDigestTextPreparedParamProvenance(t *testing.T) {
	proc := testutil.NewProcess(t)
	fn, err := GetFunctionByName(proc.Ctx, "statement_digest_text", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)

	literal, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("SELECT ?"), 1, proc.Mp())
	require.NoError(t, err)
	defer literal.Free(proc.Mp())
	require.NoError(t, literal.SetStringSource(types.StringSourceLiteral))
	// A separate marker in the prepared statement must not hide diagnostics
	// for this literal digest argument.
	proc.SetPrepareParams(literal)
	_, err = RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{literal}, 1)
	require.Error(t, err)
	require.Equal(t, uint16(moerr.ER_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode())
	proc.SetPrepareParams(nil)

	marker, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("SELECT ?"), 1, proc.Mp())
	require.NoError(t, err)
	defer marker.Free(proc.Mp())
	require.NoError(t, marker.SetStringSource(types.StringSourceSQLPrepare))
	_, err = RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{marker}, 1)
	require.Error(t, err)
	require.Equal(t, uint16(moerr.ER_UNDISCLOSED_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode())
}

func TestStatementDigestTextReadsSettingsAtExecution(t *testing.T) {
	proc := testutil.NewProcess(t)
	limit := int64(1024)
	mode := ""
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		switch name {
		case "sql_mode":
			require.True(t, system)
			require.False(t, global)
			return mode, nil
		case "max_digest_length":
			require.True(t, system)
			require.True(t, global)
			return limit, nil
		default:
			return nil, fmt.Errorf("unexpected variable %s", name)
		}
	})
	input, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("SELECT a,b,c,d,e,f"), 1, proc.Mp())
	require.NoError(t, err)
	defer input.Free(proc.Mp())
	require.NoError(t, input.SetStringSource(types.StringSourceLiteral))
	fn, err := GetFunctionByName(proc.Ctx, "statement_digest_text", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)
	first, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input}, 1)
	require.NoError(t, err)
	limit = 8
	proc.ResetMaxDigestLengthSnapshot()
	second, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input}, 1)
	require.NoError(t, err)
	firstBytes := first.GetBytesAt(0)
	secondBytes := second.GetBytesAt(0)
	require.NotEqual(t, string(firstBytes), string(secondBytes))
	first.Free(proc.Mp())
	second.Free(proc.Mp())

	limit = 1024
	proc.ResetMaxDigestLengthSnapshot()
	modeInput, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(`SELECT "column" FROM t`), 1, proc.Mp())
	require.NoError(t, err)
	defer modeInput.Free(proc.Mp())
	require.NoError(t, modeInput.SetStringSource(types.StringSourceLiteral))

	mode = ""
	stringModeResult, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{modeInput}, 1)
	require.NoError(t, err)
	require.Equal(t, "SELECT ? FROM `t`", stringModeResult.GetStringAt(0))
	stringModeResult.Free(proc.Mp())

	mode = "ANSI_QUOTES"
	identifierModeResult, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{modeInput}, 1)
	require.NoError(t, err)
	require.Equal(t, "SELECT `column` FROM `t`", identifierModeResult.GetStringAt(0))
	identifierModeResult.Free(proc.Mp())
}

func TestStatementDigestTextUsesRemoteLengthSnapshot(t *testing.T) {
	inputSQL := []byte("SELECT a,b,c,d,e,f")
	for _, test := range []struct {
		name   string
		length int64
		want   string
	}{
		{name: "explicit zero", length: 0, want: ""},
		{name: "custom", length: 8, want: "SELECT `a`"},
		{name: "default", length: process.DefaultMaxDigestLength, want: "SELECT `a` , `b` , `c` , `d` , `e` , `f`"},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.SetResolveVariableFunc(nil) // remote CNs have no session resolver
			proc.GetSessionInfo().MaxDigestLength = test.length
			proc.GetSessionInfo().MaxDigestLengthSet = true

			input, err := vector.NewConstBytes(types.T_varchar.ToType(), inputSQL, 1, proc.Mp())
			require.NoError(t, err)
			defer input.Free(proc.Mp())
			require.NoError(t, input.SetStringSource(types.StringSourceLiteral))
			fn, err := GetFunctionByName(proc.Ctx, "statement_digest_text", []types.Type{types.T_varchar.ToType()})
			require.NoError(t, err)
			result, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input}, 1)
			require.NoError(t, err)
			defer result.Free(proc.Mp())
			require.Equal(t, test.want, result.GetStringAt(0))
		})
	}
}

func TestStatementDigestTextDoesNotDiscloseFoldedExpression(t *testing.T) {
	proc := testutil.NewProcess(t)
	input, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("SELECT ?"), 1, proc.Mp())
	require.NoError(t, err)
	defer input.Free(proc.Mp())
	require.NoError(t, input.SetStringSource(types.StringSourceExpression))
	fn, err := GetFunctionByName(proc.Ctx, "statement_digest_text", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)
	_, err = RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input}, 1)
	require.Error(t, err)
	require.Equal(t, uint16(moerr.ER_UNDISCLOSED_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode())
}

func TestStatementDigestTextNestedCallIsUndisclosed(t *testing.T) {
	proc := testutil.NewProcess(t)
	fn, err := GetFunctionByName(proc.Ctx, "statement_digest_text", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)
	innerInput, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("SELECT 1, 2, 3"), 1, proc.Mp())
	require.NoError(t, err)
	defer innerInput.Free(proc.Mp())
	require.NoError(t, innerInput.SetStringSource(types.StringSourceLiteral))
	inner, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{innerInput}, 1)
	require.NoError(t, err)
	_, err = RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{inner}, 1)
	inner.Free(proc.Mp())
	require.Error(t, err)
	require.Equal(t, uint16(moerr.ER_UNDISCLOSED_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode())
}

func TestStatementDigestTextMixedSourcesStayUndisclosed(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := vector.NewVec(types.T_varchar.ToType())
	defer input.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(input, []byte("SELECT ?"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(input, []byte("SELECT"), false, proc.Mp()))
	require.NoError(t, input.SetStringSourcesWithMP([]types.StringSource{
		types.StringSourceLiteral,
		types.StringSourceExpression,
	}, proc.Mp()))
	fn, err := GetFunctionByName(proc.Ctx, "statement_digest_text", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)
	_, err = RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input}, 2)
	require.Error(t, err)
	require.Equal(t, uint16(moerr.ER_UNDISCLOSED_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode())
}

func TestStatementDigestTextSelectListSkipsMaskedBinaryRow(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := vector.NewVec(types.T_varchar.ToType())
	defer input.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(input, []byte("SELECT 1"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(input, []byte{0xff, 0x00}, false, proc.Mp()))
	require.NoError(t, input.SetStringSourcesWithMP([]types.StringSource{
		types.StringSourceLiteral,
		types.StringSourceExpression,
	}, proc.Mp()))
	require.NoError(t, input.SetIsBinaryStringAt(1, true, proc.Mp()))

	result := vector.NewFunctionResultWrapper(types.T_text.ToType(), proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(2))
	require.NoError(t, StatementDigestText(
		[]*vector.Vector{input}, result, proc, 2,
		&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}},
	))

	got := result.GetResultVector()
	require.False(t, got.IsNull(0))
	require.Equal(t, "SELECT ?", got.GetStringAt(0))
	require.True(t, got.IsNull(1))
}

func TestStatementDigestTextOverloadsAndCharset(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, oid := range []types.T{
		types.T_varchar, types.T_char, types.T_text,
		types.T_binary, types.T_varbinary, types.T_blob,
		types.T_geometry, types.T_geometry32,
	} {
		t.Run(oid.String(), func(t *testing.T) {
			argType := oid.ToType()
			argType.Charset = uint8(7)
			fn, err := GetFunctionByName(proc.Ctx, "statement_digest_text", []types.Type{argType})
			require.NoError(t, err)
			_, shouldCast := fn.ShouldDoImplicitTypeCast()
			require.False(t, shouldCast)
			require.Equal(t, types.T_text, fn.GetReturnType().Oid)
			require.Equal(t, argType.Charset, fn.GetReturnType().Charset)
		})
	}

	fn, err := GetFunctionByName(proc.Ctx, "statement_digest_text", []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)
	targets, shouldCast := fn.ShouldDoImplicitTypeCast()
	require.True(t, shouldCast)
	require.Len(t, targets, 1)
	require.Equal(t, types.T_varchar, targets[0].Oid)
	require.Equal(t, types.T_text, fn.GetReturnType().Oid)
}

func TestStatementDigestSettings(t *testing.T) {
	require.Equal(t, "", statementDigestSQLMode(nil))
	require.Equal(t, 1024, statementDigestMaxLength(nil))

	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().SqlMode = "NO_BACKSLASH_ESCAPES"
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		switch name {
		case "sql_mode":
			require.True(t, system)
			require.False(t, global)
			return "ANSI_QUOTES", nil
		case "max_digest_length":
			require.True(t, system)
			require.True(t, global)
			return int64(2048), nil
		default:
			return nil, fmt.Errorf("unexpected variable %s", name)
		}
	})
	require.Equal(t, "ANSI_QUOTES", statementDigestSQLMode(proc))
	require.Equal(t, 2048, statementDigestMaxLength(proc))

	for _, test := range []struct {
		name  string
		value interface{}
		want  int
	}{
		{name: "zero", value: uint64(0), want: 0},
		{name: "maximum", value: 1 << 20, want: 1 << 20},
		{name: "negative", value: int64(-1), want: 1024},
		{name: "too large", value: uint64(1 << 21), want: 1024},
		{name: "wrong type", value: "1024", want: 1024},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := testutil.NewProcess(t)
			p.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
				return test.value, nil
			})
			require.Equal(t, test.want, statementDigestMaxLength(p))
		})
	}

	fallback := testutil.NewProcess(t)
	fallback.GetSessionInfo().SqlMode = "NO_BACKSLASH_ESCAPES"
	fallback.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return nil, fmt.Errorf("resolver unavailable")
	})
	require.Equal(t, "NO_BACKSLASH_ESCAPES", statementDigestSQLMode(fallback))
	require.Equal(t, 1024, statementDigestMaxLength(fallback))
	fallback.GetSessionInfo().SqlMode = process.EmptySqlModeSentinel
	require.Equal(t, "", statementDigestSQLMode(fallback))
}

func TestStatementDigestTextPreservesBackgroundSQLModeSnapshot(t *testing.T) {
	inputSQL := []byte(`SELECT "column" FROM t`)
	for _, test := range []struct {
		name       string
		isFrontend bool
		want       string
	}{
		{
			name:       "background empty resolver keeps captured ANSI quotes",
			isFrontend: false,
			want:       "SELECT `column` FROM `t`",
		},
		{
			name:       "frontend empty resolver explicitly clears mode",
			isFrontend: true,
			want:       "SELECT ? FROM `t`",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.Base.IsFrontend = test.isFrontend
			proc.GetSessionInfo().SqlMode = "ANSI_QUOTES"
			proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
				switch name {
				case "sql_mode":
					require.True(t, system)
					require.False(t, global)
					return "", nil
				case "max_digest_length":
					require.True(t, system)
					require.True(t, global)
					return int64(process.DefaultMaxDigestLength), nil
				default:
					return nil, fmt.Errorf("unexpected variable %s", name)
				}
			})

			input, err := vector.NewConstBytes(types.T_varchar.ToType(), inputSQL, 1, proc.Mp())
			require.NoError(t, err)
			defer input.Free(proc.Mp())
			require.NoError(t, input.SetStringSource(types.StringSourceLiteral))
			fn, err := GetFunctionByName(proc.Ctx, "statement_digest_text", []types.Type{types.T_varchar.ToType()})
			require.NoError(t, err)
			result, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input}, 1)
			require.NoError(t, err)
			defer result.Free(proc.Mp())
			require.Equal(t, test.want, result.GetStringAt(0))
		})
	}
}

func TestStatementDigestSQLModeResolutionBoundaries(t *testing.T) {
	for _, test := range []struct {
		name       string
		frontend   bool
		snapshot   string
		value      interface{}
		err        error
		noResolver bool
		want       string
	}{
		{name: "remote snapshot", snapshot: "ANSI_QUOTES", noResolver: true, want: "ANSI_QUOTES"},
		{name: "background backslash mode", snapshot: "NO_BACKSLASH_ESCAPES", value: "", want: "NO_BACKSLASH_ESCAPES"},
		{name: "combined modes", snapshot: "ANSI_QUOTES,NO_BACKSLASH_ESCAPES", value: "", want: "ANSI_QUOTES,NO_BACKSLASH_ESCAPES"},
		{name: "captured explicit empty", snapshot: process.EmptySqlModeSentinel, value: "", want: ""},
		{name: "resolver explicit empty", snapshot: "ANSI_QUOTES", value: process.EmptySqlModeSentinel, want: ""},
		{name: "nonempty resolver wins", snapshot: "ANSI_QUOTES", value: "NO_BACKSLASH_ESCAPES", want: "NO_BACKSLASH_ESCAPES"},
		{name: "resolver error", snapshot: "ANSI_QUOTES", err: fmt.Errorf("unavailable"), want: "ANSI_QUOTES"},
		{name: "wrong resolver type", snapshot: "ANSI_QUOTES", value: 42, want: "ANSI_QUOTES"},
		{name: "nil resolver value", snapshot: "ANSI_QUOTES", want: "ANSI_QUOTES"},
		{name: "empty snapshot", value: "", want: ""},
		{name: "frontend clear", frontend: true, snapshot: "ANSI_QUOTES", value: "", want: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.Base.IsFrontend = test.frontend
			proc.GetSessionInfo().SqlMode = test.snapshot
			proc.SetResolveVariableFunc(nil)
			if !test.noResolver {
				proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
					require.Equal(t, "sql_mode", name)
					require.True(t, system)
					require.False(t, global)
					return test.value, test.err
				})
			}
			require.Equal(t, test.want, statementDigestSQLMode(proc))
		})
	}
}

const (
	statementDigestSelectLiteralHash = "d1b44b0c19af710b5a679907e284acd2ddc285201794bc69a2389d77baedddae"
	statementDigestSelectInHash      = "cba65b0398663b18b471aba08f14a530e0aef745b1b031c40a61d22ad271dc26"
	statementDigestEmptyHash         = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

func statementDigestHashResultType() types.Type {
	typ := types.T_varchar.ToType()
	typ.Width = 64
	return typ
}

func TestStatementDigestHashTypeResolution(t *testing.T) {
	proc := testutil.NewProcess(t)
	require.True(t, GetFunctionIsVolatileOrRealTimeRelatedByName("statement_digest"))
	fn, err := GetFunctionByName(proc.Ctx, "statement_digest", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)
	ov, err := GetFunctionById(proc.Ctx, fn.GetEncodedOverloadID())
	require.NoError(t, err)
	require.True(t, ov.CannotFold())
	require.True(t, ov.IsRealTimeRelated())
	for _, oid := range []types.T{
		types.T_varchar, types.T_text, types.T_blob,
		types.T_char, types.T_binary, types.T_varbinary,
	} {
		fn, err := GetFunctionByName(proc.Ctx, "statement_digest", []types.Type{oid.ToType()})
		require.NoError(t, err, oid.String())
		_, shouldCast := fn.ShouldDoImplicitTypeCast()
		require.False(t, shouldCast, oid.String())
		require.Equal(t, types.T_varchar, fn.GetReturnType().Oid)
		require.Equal(t, int32(64), fn.GetReturnType().Width)
	}

	fn, err = GetFunctionByName(proc.Ctx, "statement_digest", []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)
	castTypes, shouldCast := fn.ShouldDoImplicitTypeCast()
	require.True(t, shouldCast)
	require.Equal(t, []types.Type{formattedScalarStringType(types.T_int64.ToType())}, castTypes)

	_, err = GetFunctionByName(proc.Ctx, "statement_digest", nil)
	require.Error(t, err)
	_, err = GetFunctionByName(proc.Ctx, "statement_digest", []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()})
	require.Error(t, err)
}

func TestStatementDigestHashCompatibility(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []string{
		"SELECT 1",
		"SELECT $$/*!80000$$",
		"SELECT $q$/*!80000$q$",
		"SELECT /*!80000 $q$*/$q$ + 1 */",
		"SELECT $$a$$$$/*!80000$$",
		"SELECT @@global.$$ /*!80401 + 1 */",
		"SELECT /*!80000 */ /*+ MAX_EXECUTION_TIME(1000) */ 1",
		"select 2",
		"SELECT * FROM t WHERE id IN (1,2,3)",
		"SELECT 1;",
		"-- comment only\n",
		"/* comment only */",
		"/*!80000 */",
		"SELECT _utf8 X'41'",
		"SELECT _binary B'1'",
		"SELECT _latin1 0x41",
		"SELECT _ascii 0b1",
		"SELECT _utf8 /*!80401 1 */ 'x'",
		"SELECT _utf8 /*!80000 'x' */",
		"SELECT 1 /*!80401 ; SELECT 2 */",
		"SELECT 1 # /*!99999\n + 2 /* ordinary */",
		"SELECT /*!80401 */ /*+ BKA(t) */ 1",
		"SELECT /*+ SET_VAR(sort_buffer_size=16M) */ 1",
	}
	wanted := []string{
		statementDigestSelectLiteralHash,
		statementDigestSelectLiteralHash,
		statementDigestSelectLiteralHash,
		"18c24a99168954090331d4686d78ade5498aa5cfc6b125c260cd8183fa150bd5",
		"330b0454d6423fbb864f3707655da47d815faa8923bdaa068e8e9365e6c97a55",
		"58c4eba416a474a48989a9393e40f45f9f60b37c65fc991dd9b0eeda5979c042",
		statementDigestSelectLiteralHash,
		statementDigestSelectLiteralHash,
		statementDigestSelectInHash,
		"4b46f54bd8065b8dc5777a0cc14bcefc2be16c230edaa024ddd28ebf988a865c",
		statementDigestEmptyHash,
		statementDigestEmptyHash,
		statementDigestEmptyHash,
		"04144c90cfef7b8973c07fe5b12181df7996a6db61471dfe8b365d188cea8e19",
		"04144c90cfef7b8973c07fe5b12181df7996a6db61471dfe8b365d188cea8e19",
		"04144c90cfef7b8973c07fe5b12181df7996a6db61471dfe8b365d188cea8e19",
		"04144c90cfef7b8973c07fe5b12181df7996a6db61471dfe8b365d188cea8e19",
		"04144c90cfef7b8973c07fe5b12181df7996a6db61471dfe8b365d188cea8e19",
		"04144c90cfef7b8973c07fe5b12181df7996a6db61471dfe8b365d188cea8e19",
		statementDigestSelectLiteralHash,
		"18c24a99168954090331d4686d78ade5498aa5cfc6b125c260cd8183fa150bd5",
		statementDigestSelectLiteralHash,
		"c76de720d78ecfc18e2cf4e87e894bc134f7596270461ad343b3e9d069af2d31",
	}
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), inputs, nil)},
		NewFunctionTestResult(statementDigestHashResultType(), false, wanted, nil),
		StatementDigest,
	)
	succeed, info := testCase.Run()
	require.True(t, succeed, info)
}

func TestStatementDigestHashNullAndMaskedRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	nullCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{NewFunctionTestInput(
			types.T_varchar.ToType(),
			[]string{"SELECT 1", "", "select 2"},
			[]bool{false, true, false},
		)},
		NewFunctionTestResult(
			statementDigestHashResultType(), false,
			[]string{statementDigestSelectLiteralHash, "", statementDigestSelectLiteralHash},
			[]bool{false, true, false},
		),
		StatementDigest,
	)
	succeed, info := nullCase.Run()
	require.True(t, succeed, info)

	input := vector.NewVec(types.T_varchar.ToType())
	defer input.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(input, []byte("SELECT 1"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(input, []byte{0xff, 0x00}, false, proc.Mp()))
	require.NoError(t, input.SetStringSourcesWithMP([]types.StringSource{
		types.StringSourceLiteral,
		types.StringSourceExpression,
	}, proc.Mp()))
	require.NoError(t, input.SetIsBinaryStringAt(1, true, proc.Mp()))

	result := vector.NewFunctionResultWrapper(statementDigestHashResultType(), proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(2))
	require.NoError(t, StatementDigest(
		[]*vector.Vector{input}, result, proc, 2,
		&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}},
	))
	got := result.GetResultVector()
	require.False(t, got.IsNull(0))
	require.Equal(t, statementDigestSelectLiteralHash, got.GetStringAt(0))
	require.True(t, got.IsNull(1))
}

func TestStatementDigestHashErrorDisclosureAndEncoding(t *testing.T) {
	proc := testutil.NewProcess(t)
	fn, err := GetFunctionByName(proc.Ctx, "statement_digest", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)

	run := func(input *vector.Vector) error {
		result, runErr := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input}, 1)
		if result != nil {
			result.Free(proc.Mp())
		}
		return runErr
	}

	literal, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("SELECT ?"), 1, proc.Mp())
	require.NoError(t, err)
	require.NoError(t, literal.SetStringSource(types.StringSourceLiteral))
	err = run(literal)
	literal.Free(proc.Mp())
	require.Error(t, err)
	require.Equal(t, uint16(moerr.ER_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode())

	expression, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("SELECT ?"), 1, proc.Mp())
	require.NoError(t, err)
	require.NoError(t, expression.SetStringSource(types.StringSourceExpression))
	err = run(expression)
	expression.Free(proc.Mp())
	require.Error(t, err)
	require.Equal(t, uint16(moerr.ER_UNDISCLOSED_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode())

	malformed, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte{0xff, 0x00, 0xc3, 0x28}, 1, proc.Mp())
	require.NoError(t, err)
	require.NoError(t, malformed.SetStringSource(types.StringSourceLiteral))
	err = run(malformed)
	malformed.Free(proc.Mp())
	require.Error(t, err)
	require.Equal(t, uint16(moerr.ER_UNDISCLOSED_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode())

	geometry, err := vector.NewConstBytes(types.T_geometry.ToType(), []byte("SELECT 1"), 1, proc.Mp())
	require.NoError(t, err)
	err = run(geometry)
	geometry.Free(proc.Mp())
	require.Error(t, err)
	require.Equal(t, uint16(moerr.ER_UNDISCLOSED_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode())

	validBinary, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("SELECT 1"), 1, proc.Mp())
	require.NoError(t, err)
	validBinary.SetIsBinaryString(true)
	err = run(validBinary)
	validBinary.Free(proc.Mp())
	require.NoError(t, err)
}

func TestStatementDigestHashSharesTextCharsetAdmission(t *testing.T) {
	newLiteral := func(proc *process.Process, sql string) *vector.Vector {
		input, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(sql), 1, proc.Mp())
		require.NoError(t, err)
		require.NoError(t, input.SetStringSource(types.StringSourceLiteral))
		return input
	}
	run := func(proc *process.Process, input *vector.Vector) error {
		fn, err := GetFunctionByName(proc.Ctx, "statement_digest", []types.Type{types.T_varchar.ToType()})
		require.NoError(t, err)
		result, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input}, 1)
		if result != nil {
			result.Free(proc.Mp())
		}
		return err
	}

	for _, sql := range []string{"SELECT _utf8", "SELECT 1, _latin1"} {
		proc := testutil.NewProcess(t)
		input := newLiteral(proc, sql)
		err := run(proc, input)
		input.Free(proc.Mp())
		require.Error(t, err, sql)
		require.Equal(t, uint16(moerr.ER_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode(), sql)
		require.Contains(t, err.Error(), "character set introducer", sql)
	}

	for _, tc := range []struct {
		sql  string
		want string
	}{
		{sql: "   ", want: `Could not parse argument to digest function: "SQL parser error: syntax error, or too many sql to parse".`},
		{sql: "SELECT FROM", want: `Could not parse argument to digest function: "You have an error in your SQL syntax; check the manual that corresponds to your MatrixOne server version for the right syntax to use. syntax error at line 1 column 11 near " FROM";".`},
		{sql: "SELECT 1; SELECT 2", want: `Could not parse argument to digest function: "SQL parser error: syntax error, or too many sql to parse".`},
	} {
		proc := testutil.NewProcess(t)
		input := newLiteral(proc, tc.sql)
		err := run(proc, input)
		input.Free(proc.Mp())
		require.Equal(t, tc.want, err.Error(), tc.sql)
	}

	zero := testutil.NewProcess(t)
	zero.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
		if name == "sql_mode" {
			return "", nil
		}
		return int64(0), nil
	})
	zeroInput := newLiteral(zero, "SELECT _utf8")
	err := run(zero, zeroInput)
	zeroInput.Free(zero.Mp())
	require.Error(t, err)
	require.Equal(t, uint16(moerr.ER_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode())
}

func TestStatementDigestHashCharsetModesAndVersionComments(t *testing.T) {
	newLiteral := func(proc *process.Process, sql string) *vector.Vector {
		input, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(sql), 1, proc.Mp())
		require.NoError(t, err)
		require.NoError(t, input.SetStringSource(types.StringSourceLiteral))
		return input
	}
	run := func(proc *process.Process, input *vector.Vector) (string, error) {
		fn, err := GetFunctionByName(proc.Ctx, "statement_digest", []types.Type{types.T_varchar.ToType()})
		require.NoError(t, err)
		result, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input}, 1)
		if result != nil {
			defer result.Free(proc.Mp())
		}
		if err != nil {
			return "", err
		}
		return result.GetStringAt(0), nil
	}

	noBackslash := testutil.NewProcess(t)
	noBackslash.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
		if name == "sql_mode" {
			return "NO_BACKSLASH_ESCAPES", nil
		}
		return int64(process.DefaultMaxDigestLength), nil
	})
	input := newLiteral(noBackslash, `SELECT _utf8'abc\'`)
	got, err := run(noBackslash, input)
	input.Free(noBackslash.Mp())
	require.NoError(t, err)
	require.Equal(t, "04144c90cfef7b8973c07fe5b12181df7996a6db61471dfe8b365d188cea8e19", got)

	zeroLength := testutil.NewProcess(t)
	zeroLength.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
		if name == "sql_mode" {
			return "NO_BACKSLASH_ESCAPES", nil
		}
		return int64(0), nil
	})
	zeroInput := newLiteral(zeroLength, `SELECT _utf8'abc\'`)
	got, err = run(zeroLength, zeroInput)
	zeroInput.Free(zeroLength.Mp())
	require.NoError(t, err)
	require.Equal(t, statementDigestEmptyHash, got)

	for _, sql := range []string{
		"SELECT _utf8 /*!80000 1 */ 'x'",
		"SELECT _utf8 /*!80000 1; SELECT 2 */ 'x'",
	} {
		proc := testutil.NewProcess(t)
		input := newLiteral(proc, sql)
		_, err := run(proc, input)
		input.Free(proc.Mp())
		require.Error(t, err, sql)
		require.Equal(t, uint16(moerr.ER_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode(), sql)
	}
}

func TestStatementDigestHashSettingsAndRemoteSnapshot(t *testing.T) {
	makeInput := func(proc *process.Process) *vector.Vector {
		input, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(`SELECT "column" FROM t`), 1, proc.Mp())
		require.NoError(t, err)
		require.NoError(t, input.SetStringSource(types.StringSourceLiteral))
		return input
	}
	run := func(proc *process.Process, input *vector.Vector) (string, error) {
		fn, err := GetFunctionByName(proc.Ctx, "statement_digest", []types.Type{types.T_varchar.ToType()})
		require.NoError(t, err)
		result, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input}, 1)
		if err != nil {
			return "", err
		}
		defer result.Free(proc.Mp())
		return result.GetStringAt(0), nil
	}

	emptyMode := testutil.NewProcess(t)
	emptyMode.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
		if name == "sql_mode" {
			return "", nil
		}
		return int64(process.DefaultMaxDigestLength), nil
	})
	emptyInput := makeInput(emptyMode)
	emptyHash, err := run(emptyMode, emptyInput)
	emptyInput.Free(emptyMode.Mp())
	require.NoError(t, err)

	ansiMode := testutil.NewProcess(t)
	ansiMode.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
		if name == "sql_mode" {
			return "ANSI_QUOTES", nil
		}
		return int64(process.DefaultMaxDigestLength), nil
	})
	ansiInput := makeInput(ansiMode)
	ansiHash, err := run(ansiMode, ansiInput)
	ansiInput.Free(ansiMode.Mp())
	require.NoError(t, err)
	require.NotEqual(t, emptyHash, ansiHash)
	require.Equal(t, "5d8be60a36c3d08bd5c7f5ad059da01a5883b2610a0c07f7a31cbb2c1a76ca9c", ansiHash)

	zeroLength := testutil.NewProcess(t)
	zeroLength.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
		if name == "sql_mode" {
			return "", nil
		}
		return int64(0), nil
	})
	zeroInput := makeInput(zeroLength)
	zeroHash, err := run(zeroLength, zeroInput)
	zeroInput.Free(zeroLength.Mp())
	require.NoError(t, err)
	require.Equal(t, statementDigestEmptyHash, zeroHash)

	remote := testutil.NewProcess(t)
	remote.Base.IsFrontend = false
	remote.Base.SessionInfo.SqlMode = "ANSI_QUOTES"
	remote.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
		if name == "sql_mode" {
			return "", nil
		}
		return int64(process.DefaultMaxDigestLength), nil
	})
	remoteInput := makeInput(remote)
	remoteHash, err := run(remote, remoteInput)
	remoteInput.Free(remote.Mp())
	require.NoError(t, err)
	require.Equal(t, ansiHash, remoteHash)
}
