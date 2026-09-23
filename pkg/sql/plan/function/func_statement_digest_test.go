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

func statementDigestTextInput(t *testing.T, proc *process.Process, sql string, source types.StringSource) *vector.Vector {
	t.Helper()
	input, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(sql), 1, proc.Mp())
	require.NoError(t, err)
	require.NoError(t, input.SetStringSource(source))
	return input
}

func runStatementDigestText(t *testing.T, proc *process.Process, input *vector.Vector) (*vector.Vector, error) {
	t.Helper()
	fn, err := GetFunctionByName(proc.Ctx, "statement_digest_text", []types.Type{input.GetType()})
	require.NoError(t, err)
	return RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input}, input.Length())
}

func TestStatementDigestTextNormalizesValuesAndPreservesNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := vector.NewVec(types.T_varchar.ToType())
	defer input.Free(proc.Mp())
	for _, row := range []struct {
		sql  string
		null bool
	}{
		{sql: "SELECT 1"},
		{sql: "SELECT 2 /* comment */ WHERE 10=20"},
		{null: true},
	} {
		require.NoError(t, vector.AppendBytes(input, []byte(row.sql), row.null, proc.Mp()))
	}
	result, err := runStatementDigestText(t, proc, input)
	require.NoError(t, err)
	defer result.Free(proc.Mp())
	require.Equal(t, "SELECT ?", result.GetStringAt(0))
	require.Equal(t, "SELECT ? WHERE ? = ?", result.GetStringAt(1))
	require.True(t, result.IsNull(2))
}

func TestStatementDigestTextErrorsDoNotLeakExpressionInput(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, test := range []struct {
		name   string
		sql    string
		source types.StringSource
		code   uint16
	}{
		{"literal syntax", "SELECT ?", types.StringSourceLiteral, moerr.ER_PARSE_ERROR_IN_DIGEST_FN},
		{"expression syntax", "SELECT ?", types.StringSourceExpression, moerr.ER_UNDISCLOSED_PARSE_ERROR_IN_DIGEST_FN},
		{"prepared syntax", "SELECT ?", types.StringSourceSQLPrepare, moerr.ER_UNDISCLOSED_PARSE_ERROR_IN_DIGEST_FN},
		{"malformed utf8", "SELECT \xff", types.StringSourceLiteral, moerr.ER_UNDISCLOSED_PARSE_ERROR_IN_DIGEST_FN},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := statementDigestTextInput(t, proc, test.sql, test.source)
			defer input.Free(proc.Mp())
			_, err := runStatementDigestText(t, proc, input)
			require.Error(t, err)
			require.Equal(t, test.code, moerr.DowncastError(err).MySQLCode())
		})
	}
}

func TestStatementDigestTextUsesCurrentSettings(t *testing.T) {
	proc := testutil.NewProcess(t)
	length := int64(process.DefaultMaxDigestLength)
	mode := ""
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
		switch name {
		case "sql_mode":
			return mode, nil
		case "max_digest_length":
			return length, nil
		default:
			return nil, fmt.Errorf("unexpected variable %s", name)
		}
	})
	input := statementDigestTextInput(t, proc, `SELECT "column" FROM t WHERE id IN (1,2,3)`, types.StringSourceLiteral)
	defer input.Free(proc.Mp())

	result, err := runStatementDigestText(t, proc, input)
	require.NoError(t, err)
	require.Equal(t, "SELECT ? FROM `t` WHERE `id` IN (?)", result.GetStringAt(0))
	result.Free(proc.Mp())

	mode = "ANSI_QUOTES"
	proc.ResetMaxDigestLengthSnapshot()
	result, err = runStatementDigestText(t, proc, input)
	require.NoError(t, err)
	require.Equal(t, "SELECT `column` FROM `t` WHERE `id` IN (?)", result.GetStringAt(0))
	result.Free(proc.Mp())

	length = 0
	proc.ResetMaxDigestLengthSnapshot()
	result, err = runStatementDigestText(t, proc, input)
	require.NoError(t, err)
	require.Empty(t, result.GetStringAt(0))
	result.Free(proc.Mp())
}

func TestStatementDigestTextReturnsTypeAndDoesNotFold(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, oid := range []types.T{types.T_varchar, types.T_text, types.T_blob, types.T_binary, types.T_varbinary} {
		fn, err := GetFunctionByName(proc.Ctx, "statement_digest_text", []types.Type{oid.ToType()})
		require.NoError(t, err, oid.String())
		require.Equal(t, types.T_text, fn.GetReturnType().Oid)
		ov, err := GetFunctionById(proc.Ctx, fn.GetEncodedOverloadID())
		require.NoError(t, err)
		require.True(t, ov.CannotFold())
	}
}

func TestStatementDigestTextUsesRemoteSettingSnapshot(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(nil)
	proc.GetSessionInfo().MaxDigestLength = 8
	proc.GetSessionInfo().MaxDigestLengthSet = true
	input := statementDigestTextInput(t, proc, "SELECT a,b,c", types.StringSourceLiteral)
	defer input.Free(proc.Mp())
	result, err := runStatementDigestText(t, proc, input)
	require.NoError(t, err)
	defer result.Free(proc.Mp())
	require.Equal(t, "SELECT `a`", result.GetStringAt(0))
}
