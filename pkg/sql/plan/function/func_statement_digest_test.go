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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	digestSelectLiteral = "d1b44b0c19af710b5a679907e284acd2ddc285201794bc69a2389d77baedddae"
	digestSelectIn      = "cba65b0398663b18b471aba08f14a530e0aef745b1b031c40a61d22ad271dc26"
)

func statementDigestResultType() types.Type {
	typ := types.T_varchar.ToType()
	typ.Width = 64
	return typ
}

func TestStatementDigestTypeResolution(t *testing.T) {
	proc := testutil.NewProcess(t)
	require.True(t, GetFunctionIsVolatileOrRealTimeRelatedByName("statement_digest"))
	directTypes := []types.T{
		types.T_varchar,
		types.T_text,
		types.T_blob,
		types.T_char,
		types.T_binary,
		types.T_varbinary,
	}
	for _, oid := range directTypes {
		fn, err := GetFunctionByName(proc.Ctx, "statement_digest", []types.Type{oid.ToType()})
		require.NoError(t, err, oid.String())
		_, shouldCast := fn.ShouldDoImplicitTypeCast()
		require.False(t, shouldCast, oid.String())
		require.Equal(t, types.T_varchar, fn.GetReturnType().Oid)
		require.Equal(t, int32(64), fn.GetReturnType().Width)
	}

	fn, err := GetFunctionByName(proc.Ctx, "statement_digest", []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)
	castTypes, shouldCast := fn.ShouldDoImplicitTypeCast()
	require.True(t, shouldCast)
	require.Equal(t, []types.Type{types.T_varchar.ToType()}, castTypes)

	_, err = GetFunctionByName(proc.Ctx, "statement_digest", nil)
	require.Error(t, err)
	_, err = GetFunctionByName(proc.Ctx, "statement_digest", []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()})
	require.Error(t, err)
}

func TestStatementDigestMySQLCompatibility(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []string{
		"SELECT 1",
		"select 2",
		"SELECT '中文'",
		"SELECT * FROM mytable WHERE cola = 10 AND colb = 20",
		"SELECT * FROM t WHERE id IN (1,2,3)",
		"  select /* c */ 1 + 'x' as A",
		"SELECT `MiXeD` FROM `Db`.`TaBlE`",
		"SELECT * FROM t WHERE b=-42",
		"SELECT CASE WHEN a THEN -1 ELSE +2 END",
		"SELECT _utf8mb4'hello'",
		"SELECT 1;",
		"-- comment only\n",
		"/* comment only */",
		"/*+ */",
		"/*!80000 */",
		"/*!80000 SELECT 1 */",
		"SELECT /*+ SET_VAR(sort_buffer_size=16M) */ 1",
		"SELECT a FROM t GROUP BY a WITH ROLLUP",
		"CREATE TABLE t(a INT NULL, b INT NOT NULL, c INT DEFAULT NULL, d INT NULL DEFAULT NULL)",
	}
	wanted := []string{
		digestSelectLiteral,
		digestSelectLiteral,
		digestSelectLiteral,
		"3bb95eeade896657c4526e74ff2a2862039d0a0fe8a9e7155b5fe492cbd78387",
		digestSelectIn,
		"9bbd87fc802fc989dbb46cd2585c8231e01a881fe548d1bfa871cc503c69e240",
		"65f40f94e799e258bb02185d82064aea136521ba1f53de6cc92abbc54e640cdb",
		"4474030ab69df2ff0b2834202712d7114446f7e1429d610030b17018e09db1f0",
		"b13883eec7cbb158f27ca4e1fda9be138c02d89a1407b7cfe3bfb53027c27958",
		"04144c90cfef7b8973c07fe5b12181df7996a6db61471dfe8b365d188cea8e19",
		"4b46f54bd8065b8dc5777a0cc14bcefc2be16c230edaa024ddd28ebf988a865c",
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		digestSelectLiteral,
		"c76de720d78ecfc18e2cf4e87e894bc134f7596270461ad343b3e9d069af2d31",
		"8b73b88a9ea6da4ce94aaed3dc371d92791c705aa173693ee25e141acc4f02df",
		"e9b16bb26e6f0a1adede7aaf756bf40913eae757496c8bc35e0207573b3452c9",
	}
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), inputs, nil)},
		NewFunctionTestResult(statementDigestResultType(), false, wanted, nil),
		StatementDigest,
	)
	succeed, info := testCase.Run()
	require.True(t, succeed, info)
}

func TestStatementDigestNullAndConstantVectors(t *testing.T) {
	proc := testutil.NewProcess(t)

	nullCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{NewFunctionTestInput(
			types.T_varchar.ToType(),
			[]string{"SELECT 1", "", "select 2"},
			[]bool{false, true, false},
		)},
		NewFunctionTestResult(
			statementDigestResultType(),
			false,
			[]string{digestSelectLiteral, "", digestSelectLiteral},
			[]bool{false, true, false},
		),
		StatementDigest,
	)
	succeed, info := nullCase.Run()
	require.True(t, succeed, info)

	constCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{NewFunctionTestConstInput(
			types.T_text.ToType(),
			[]string{"SELECT 1", "unused", "unused"},
			nil,
		)},
		NewFunctionTestResult(
			statementDigestResultType(),
			false,
			[]string{digestSelectLiteral, digestSelectLiteral, digestSelectLiteral},
			nil,
		),
		StatementDigest,
	)
	succeed, info = constCase.Run()
	require.True(t, succeed, info)
}

func TestStatementDigestRejectsInvalidSQL(t *testing.T) {
	proc := testutil.NewProcess(t)
	invalidInputs := []string{
		"",
		"   ",
		"SELECT FROM",
		"SELECT 1; SELECT 2",
		"/*!80000 SELECT FROM */",
		string([]byte{0xff, 0xfe}),
	}
	for _, input := range invalidInputs {
		testCase := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{input}, nil)},
			NewFunctionTestResult(statementDigestResultType(), true, []string{""}, nil),
			StatementDigest,
		)
		succeed, info := testCase.Run()
		require.True(t, succeed, "%q: %s", input, info)
	}
}

func TestStatementDigestHonorsParserSQLMode(t *testing.T) {
	parserMode, digestMode, err := statementDigestSQLMode(nil)
	require.NoError(t, err)
	require.Empty(t, parserMode)
	require.Zero(t, digestMode)

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.SqlMode = "NO_BACKSLASH_ESCAPES"
	proc.SetResolveVariableFunc(func(name string, system, global bool) (any, error) {
		require.True(t, system)
		switch name {
		case "sql_mode":
			require.False(t, global)
			return "ANSI_QUOTES,NO_BACKSLASH_ESCAPES", nil
		case "max_digest_length":
			require.True(t, global)
			return int64(defaultMaxDigestLength), nil
		default:
			t.Fatalf("unexpected variable %q", name)
			return nil, nil
		}
	})
	parserMode, digestMode, err = statementDigestSQLMode(proc)
	require.NoError(t, err)
	require.Equal(t, "ANSI_QUOTES,NO_BACKSLASH_ESCAPES", parserMode)
	require.NotZero(t, digestMode)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{NewFunctionTestInput(
			types.T_varchar.ToType(),
			[]string{`SELECT "col" FROM "t"`},
			nil,
		)},
		NewFunctionTestResult(
			statementDigestResultType(),
			false,
			[]string{"db603811932de66d66fe3e297cd45d8aa36de4ab3587b3fe46a459237f9522dc"},
			nil,
		),
		StatementDigest,
	)
	succeed, info := testCase.Run()
	require.True(t, succeed, info)

	proc.Base.SessionInfo.SqlMode = process.EmptySqlModeSentinel
	proc.SetResolveVariableFunc(nil)
	parserMode, digestMode, err = statementDigestSQLMode(proc)
	require.NoError(t, err)
	require.Empty(t, parserMode)
	require.Zero(t, digestMode)

	proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
		return nil, errors.New("resolve sql_mode")
	})
	_, _, err = statementDigestSQLMode(proc)
	require.EqualError(t, err, "resolve sql_mode")
	resolveErrorCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{"SELECT 1"}, nil)},
		NewFunctionTestResult(statementDigestResultType(), true, []string{""}, nil),
		StatementDigest,
	)
	succeed, info = resolveErrorCase.Run()
	require.True(t, succeed, info)
}

func TestStatementDigestHonorsMaxDigestLength(t *testing.T) {
	maxLength, err := statementDigestMaxLength(nil)
	require.NoError(t, err)
	require.Equal(t, defaultMaxDigestLength, maxLength)

	proc := testutil.NewProcess(t)
	resolvedMaxLength := int64(16)
	proc.SetResolveVariableFunc(func(name string, system, global bool) (any, error) {
		require.True(t, system)
		switch name {
		case "sql_mode":
			require.False(t, global)
			return "", nil
		case "max_digest_length":
			require.True(t, global)
			return resolvedMaxLength, nil
		default:
			t.Fatalf("unexpected variable %q", name)
			return nil, nil
		}
	})

	run := func(want string) {
		testCase := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{NewFunctionTestInput(
				types.T_varchar.ToType(),
				[]string{"SELECT * FROM mytable WHERE cola = 10 AND colb = 20"},
				nil,
			)},
			NewFunctionTestResult(statementDigestResultType(), false, []string{want}, nil),
			StatementDigest,
		)
		succeed, info := testCase.Run()
		require.True(t, succeed, info)
	}

	run("9642da7ea9b8e3a69d62fcb050b8ac7c794e3dc4e4696c8dca7a8589cc9e0160")
	resolvedMaxLength = 0
	run("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

	proc.Base.IsFrontend = false
	proc.Base.SessionInfo.MaxDigestLength = 16
	proc.Base.SessionInfo.MaxDigestLengthSet = true
	resolvedMaxLength = defaultMaxDigestLength
	run("9642da7ea9b8e3a69d62fcb050b8ac7c794e3dc4e4696c8dca7a8589cc9e0160")
	proc.Base.IsFrontend = true
	proc.Base.SessionInfo.MaxDigestLengthSet = false

	proc.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
		if name == "sql_mode" {
			return "", nil
		}
		return "1024", nil
	})
	_, err = statementDigestMaxLength(proc)
	require.EqualError(t, err, "internal error: unexpected max_digest_length type string")

	proc.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
		if name == "sql_mode" {
			return "", nil
		}
		return int64(1048577), nil
	})
	_, err = statementDigestMaxLength(proc)
	require.EqualError(t, err, "internal error: max_digest_length is out of range: 1048577")

	proc.SetResolveVariableFunc(func(name string, _, _ bool) (any, error) {
		if name == "sql_mode" {
			return "", nil
		}
		return nil, errors.New("resolve max_digest_length")
	})
	_, err = statementDigestMaxLength(proc)
	require.EqualError(t, err, "resolve max_digest_length")
}

func TestStatementDigestSkipsMaskedRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{NewFunctionTestInput(
			types.T_varchar.ToType(),
			[]string{"SELECT FROM", "SELECT 1"},
			nil,
		)},
		NewFunctionTestResult(
			statementDigestResultType(),
			false,
			[]string{"", digestSelectLiteral},
			[]bool{true, false},
		),
		StatementDigest,
	).WithSelectList(&FunctionSelectList{
		AnyNull:    true,
		SelectList: []bool{false, true},
	})
	succeed, info := testCase.Run()
	require.True(t, succeed, info)
}
