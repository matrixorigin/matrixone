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
	fn, err := GetFunctionByName(proc.Ctx, "statement_digest_text", []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)
	_, err = RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{constInput}, 1)
	require.Error(t, err)
	require.Equal(t, uint16(moerr.ER_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode())

	proc.SetPrepareParams(constInput)
	_, err = RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{constInput}, 1)
	require.Error(t, err)
	require.Equal(t, uint16(moerr.ER_UNDISCLOSED_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode())
	proc.SetPrepareParams(nil)

	dynamicInput := vector.NewVec(types.T_varchar.ToType())
	defer dynamicInput.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(dynamicInput, []byte("SELECT ?"), false, proc.Mp()))
	_, err = RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{dynamicInput}, 1)
	require.Error(t, err)
	require.Equal(t, uint16(moerr.ER_UNDISCLOSED_PARSE_ERROR_IN_DIGEST_FN), moerr.DowncastError(err).MySQLCode())
}

func TestStatementDigestTextOverloadsAndCharset(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, oid := range []types.T{
		types.T_varchar, types.T_char, types.T_text,
		types.T_binary, types.T_varbinary, types.T_blob,
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
