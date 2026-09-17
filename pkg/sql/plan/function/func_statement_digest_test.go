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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func statementDigestResultType() types.Type {
	typ := types.T_varchar.ToType()
	typ.Width = 64
	return typ
}

func statementDigestHashForTest(formatted string) string {
	hash := sha256.Sum256([]byte(formatted))
	return hex.EncodeToString(hash[:])
}

func statementDigestFormatForTest(t *testing.T, sql, sqlMode string) string {
	t.Helper()
	statements, err := parsers.ParseWithSQLMode(t.Context(), dialect.MYSQL, sql, 0, sqlMode)
	require.NoError(t, err)
	defer func() {
		for _, stmt := range statements {
			if stmt != nil {
				stmt.Free()
			}
		}
	}()
	require.Len(t, statements, 1)
	formatted, err := statementDigestDeparse(statements[0])
	require.NoError(t, err)
	return formatted
}

func TestStatementDigestTypeResolution(t *testing.T) {
	proc := testutil.NewProcess(t)
	require.True(t, GetFunctionIsVolatileOrRealTimeRelatedByName("statement_digest"))
	for _, oid := range []types.T{
		types.T_varchar,
		types.T_text,
		types.T_blob,
		types.T_char,
		types.T_binary,
		types.T_varbinary,
	} {
		fn, err := GetFunctionByName(proc.Ctx, "statement_digest", []types.Type{oid.ToType()})
		require.NoError(t, err, oid.String())
		_, shouldCast := fn.ShouldDoImplicitTypeCast()
		require.False(t, shouldCast, oid.String())
		require.Equal(t, types.T_varchar, fn.GetReturnType().Oid)
		require.Equal(t, int32(64), fn.GetReturnType().Width)
		overload, ok := GetFunctionByIdWithoutError(fn.GetEncodedOverloadID())
		require.True(t, ok, oid.String())
		require.NotNil(t, overload.newOp, oid.String())
		require.NotNil(t, overload.newOp(), oid.String())
	}

	fn, err := GetFunctionByName(proc.Ctx, "statement_digest", []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)
	castTypes, shouldCast := fn.ShouldDoImplicitTypeCast()
	require.True(t, shouldCast)
	require.Equal(t, []types.Type{formattedScalarStringType(types.T_int64.ToType())}, castTypes)

	_, err = GetFunctionByName(proc.Ctx, "statement_digest", nil)
	require.Error(t, err)
	_, err = GetFunctionByName(proc.Ctx, "statement_digest", []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()})
	require.Error(t, err)
}

func TestStatementDigestUsesMatrixOneASTDeparse(t *testing.T) {
	cases := []struct {
		sql       string
		formatted string
	}{
		{sql: "SELECT 1", formatted: "select 1"},
		{sql: "  SELECT /* ignored */ 1 ; ", formatted: "select 1"},
		{sql: "SELECT 2", formatted: "select 2"},
		{sql: "SELECT 'a'", formatted: "select 'a'"},
		{sql: "SELECT a", formatted: "select `a`"},
		{sql: "SELECT @name", formatted: "select @`name`"},
		{sql: "SELECT ?", formatted: "select ?"},
		{sql: "SELECT 1 + 2", formatted: "select 1 + 2"},
	}

	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			formatted := statementDigestFormatForTest(t, tc.sql, "")
			require.Equal(t, tc.formatted, formatted)

			got, err := statementDigestValue(t.Context(), tc.sql, "")
			require.NoError(t, err)
			require.Equal(t, statementDigestHashForTest(tc.formatted), string(got))
		})
	}

	// Hash equality follows canonical formatting, not the spelling or layout
	// of the original SQL. Literal values and identifier names remain distinct.
	one, err := statementDigestValue(t.Context(), "SELECT 1", "")
	require.NoError(t, err)
	commented, err := statementDigestValue(t.Context(), " select /* note */ 1; ", "")
	require.NoError(t, err)
	two, err := statementDigestValue(t.Context(), "SELECT 2", "")
	require.NoError(t, err)
	quotedIdentifier, err := statementDigestValue(t.Context(), "SELECT a", "")
	require.NoError(t, err)
	stringLiteral, err := statementDigestValue(t.Context(), "SELECT 'a'", "")
	require.NoError(t, err)
	require.Equal(t, string(one), string(commented))
	require.NotEqual(t, string(one), string(two))
	require.NotEqual(t, string(quotedIdentifier), string(stringLiteral))
}

func TestStatementDigestSQLModeAndVariables(t *testing.T) {
	plain, err := statementDigestValue(t.Context(), `SELECT "name"`, "")
	require.NoError(t, err)
	ansi, err := statementDigestValue(t.Context(), `SELECT "name"`, "ANSI_QUOTES")
	require.NoError(t, err)
	require.NotEqual(t, string(plain), string(ansi))

	param1, err := statementDigestValue(t.Context(), "SELECT ?", "")
	require.NoError(t, err)
	param2, err := statementDigestValue(t.Context(), "select ?", "")
	require.NoError(t, err)
	require.Equal(t, string(param1), string(param2))

	userVar, err := statementDigestValue(t.Context(), "SELECT @name", "")
	require.NoError(t, err)
	otherUserVar, err := statementDigestValue(t.Context(), "SELECT @other", "")
	require.NoError(t, err)
	require.NotEqual(t, string(userVar), string(otherUserVar))

	caseVariants := []string{"SELECT @MiXeD", "SELECT @mixed", "SELECT @`MIXED`"}
	var canonical string
	for i, sql := range caseVariants {
		got, err := statementDigestValue(t.Context(), sql, "")
		require.NoError(t, err, sql)
		if i == 0 {
			canonical = string(got)
		} else {
			require.Equal(t, canonical, string(got), sql)
		}
	}

	unicodeUpper, err := statementDigestValue(t.Context(), "SELECT @Ä", "")
	require.NoError(t, err)
	unicodeLower, err := statementDigestValue(t.Context(), "SELECT @ä", "")
	require.NoError(t, err)
	require.Equal(t, string(unicodeUpper), string(unicodeLower))

	globalSystem, err := statementDigestValue(t.Context(), "SELECT @@global.TIME_ZONE", "")
	require.NoError(t, err)
	otherGlobalSystem, err := statementDigestValue(t.Context(), "SELECT @@GLOBAL.time_zone", "")
	require.NoError(t, err)
	sessionSystem, err := statementDigestValue(t.Context(), "SELECT @@session.time_zone", "")
	require.NoError(t, err)
	require.Equal(t, string(globalSystem), string(otherGlobalSystem))
	require.NotEqual(t, string(globalSystem), string(sessionSystem))
}

func TestStatementDigestFormatsNestedAndSpecialStatements(t *testing.T) {
	for _, sql := range []string{
		"SELECT TRIM(BOTH 'x' FROM 'xxx')",
		"SELECT (SELECT 1)",
		"PREPARE p FROM 'SELECT 1'",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := statementDigestValue(t.Context(), sql, "")
			require.NoError(t, err)
		})
	}
}

func TestStatementDigestSQLModeSnapshotAndErrors(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Base.IsFrontend = false
	proc.Base.SessionInfo.SqlMode = "ANSI_QUOTES"
	proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
		return nil, errors.New("receiving CN resolver must not be consulted")
	})
	sqlMode, err := statementDigestSQLMode(proc)
	require.NoError(t, err)
	require.Equal(t, "ANSI_QUOTES", sqlMode)

	proc.Base.IsFrontend = true
	proc.SetResolveVariableFunc(func(name string, system, global bool) (any, error) {
		require.Equal(t, "sql_mode", name)
		require.True(t, system)
		require.False(t, global)
		return int64(123), nil
	})
	_, err = statementDigestSQLMode(proc)
	require.EqualError(t, err, "internal error: unexpected sql_mode type int64")

	proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
		return nil, errors.New("resolve sql_mode")
	})
	_, err = statementDigestSQLMode(proc)
	require.EqualError(t, err, "resolve sql_mode")

	proc.Base.SessionInfo.SqlMode = process.EmptySqlModeSentinel
	proc.Base.IsFrontend = false
	proc.SetResolveVariableFunc(nil)
	sqlMode, err = statementDigestSQLMode(proc)
	require.NoError(t, err)
	require.Empty(t, sqlMode)
}

func TestStatementDigestRejectsInvalidAndMultipleStatements(t *testing.T) {
	for _, input := range []string{
		"",
		"   ",
		"/* comment only */",
		";;;",
		"SELECT FROM",
		"SELECT 1 trailing garbage",
		"SELECT 1; SELECT 2",
		"/*!80000 SELECT FROM */",
		// MatrixOne currently rejects this MySQL-supported JSON_TABLE form.
		// The MatrixOne-native hash must return that parser error rather than
		// manufacture a digest for a statement MatrixOne cannot parse.
		"SELECT * FROM JSON_TABLE('[1]', '$[*]' COLUMNS(x INT PATH '$' NULL ON EMPTY ERROR ON ERROR)) AS jt",
		string([]byte{0xff, 0xfe}),
	} {
		t.Run(input, func(t *testing.T) {
			_, err := statementDigestValue(t.Context(), input, "")
			require.Error(t, err)
		})
	}

	// Parser errors must not leave reusable parser state that affects the next
	// invocation.
	_, err := statementDigestValue(t.Context(), "SELECT FROM", "")
	require.Error(t, err)
	got, err := statementDigestValue(t.Context(), "SELECT 1", "")
	require.NoError(t, err)
	require.Equal(t, statementDigestHashForTest("select 1"), string(got))
}

func TestStatementDigestReturnsParseErrorsThroughFunction(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	for _, sql := range []string{
		"SELECT FROM",
		"SELECT 1; SELECT 2",
		"",
		"/* comment only */",
		"SELECT * FROM JSON_TABLE('[1]', '$[*]' COLUMNS(x INT PATH '$' NULL ON EMPTY ERROR ON ERROR)) AS jt",
	} {
		t.Run(sql, func(t *testing.T) {
			caseRun := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{NewFunctionTestInput(
					types.T_varchar.ToType(),
					[]string{sql},
					nil,
				)},
				NewFunctionTestResult(statementDigestResultType(), true, nil, nil),
				StatementDigest,
			)
			succeed, info := caseRun.Run()
			require.True(t, succeed, info)
			require.Zero(t, caseRun.GetResultVectorDirectly().Length(), "failed input must not produce a digest")
		})
	}
}

func TestStatementDigestFormatFailureIsAnError(t *testing.T) {
	_, err := statementDigestDeparse(nil)
	require.EqualError(t, err, "not supported: cannot format a nil statement for STATEMENT_DIGEST")

	_, err = statementDigestDeparse(testStatementDigestFormatter{panicOnFormat: true})
	require.EqualError(t, err, "internal error: AST formatter failed during STATEMENT_DIGEST")

	_, err = statementDigestDeparse(testStatementDigestFormatter{})
	require.EqualError(t, err, "not supported: AST formatter produced empty output for STATEMENT_DIGEST")
}

type testStatementDigestFormatter struct {
	panicOnFormat bool
}

func (s testStatementDigestFormatter) String() string { return "test" }
func (s testStatementDigestFormatter) Format(*tree.FmtCtx) {
	if s.panicOnFormat {
		panic("test formatter panic")
	}
}
func (testStatementDigestFormatter) GetStatementType() string { return "test" }
func (testStatementDigestFormatter) GetQueryType() string     { return "test" }
func (testStatementDigestFormatter) StmtKind() tree.StmtKind  { return 0 }
func (testStatementDigestFormatter) Free()                    {}

func TestStatementDigestCancelledContextDoesNotParse(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := statementDigestValue(ctx, "SELECT 1", "")
	require.ErrorIs(t, err, context.Canceled)
}

func TestStatementDigestNullConstantAndMaskedRows(t *testing.T) {
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
			[]string{statementDigestHashForTest("select 1"), "", statementDigestHashForTest("select 2")},
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
			[]string{statementDigestHashForTest("select 1"), statementDigestHashForTest("select 1"), statementDigestHashForTest("select 1")},
			nil,
		),
		StatementDigest,
	)
	succeed, info = constCase.Run()
	require.True(t, succeed, info)

	maskedCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{NewFunctionTestInput(
			types.T_varchar.ToType(),
			[]string{"SELECT FROM", "SELECT 1"},
			nil,
		)},
		NewFunctionTestResult(
			statementDigestResultType(),
			false,
			[]string{"", statementDigestHashForTest("select 1")},
			[]bool{true, false},
		),
		StatementDigest,
	).WithSelectList(&FunctionSelectList{
		AnyNull:    true,
		SelectList: []bool{false, true},
	})
	succeed, info = maskedCase.Run()
	require.True(t, succeed, info)
}

func TestStatementDigestDoesNotResolveSQLModeForUnevaluatedRows(t *testing.T) {
	tests := []struct {
		name       string
		input      FunctionTestInput
		length     int
		selectList *FunctionSelectList
	}{
		{
			name: "zero-row constant batch",
			input: NewFunctionTestConstInput(
				types.T_varchar.ToType(), []string{"SELECT FROM"}, nil,
			),
			length: 0,
		},
		{
			name: "all rows ignored",
			input: NewFunctionTestInput(
				types.T_varchar.ToType(), []string{"SELECT FROM", "SELECT FROM"}, nil,
			),
			length:     2,
			selectList: &FunctionSelectList{AllNull: true},
		},
		{
			name: "all rows masked",
			input: NewFunctionTestInput(
				types.T_varchar.ToType(), []string{"SELECT FROM", "SELECT FROM"}, nil,
			),
			length:     2,
			selectList: &FunctionSelectList{AnyNull: true, SelectList: []bool{false, false}},
		},
		{
			name: "all input rows NULL",
			input: NewFunctionTestInput(
				types.T_varchar.ToType(), []string{"SELECT FROM", "SELECT FROM"}, []bool{true, true},
			),
			length: 2,
		},
		{
			name: "constant NULL",
			input: NewFunctionTestConstInput(
				types.T_varchar.ToType(), []string{"SELECT FROM"}, []bool{true},
			),
			length: 1,
		},
		{
			name: "only non-NULL input is masked",
			input: NewFunctionTestInput(
				types.T_varchar.ToType(), []string{"SELECT FROM", "SELECT 1"}, []bool{false, true},
			),
			length:     2,
			selectList: &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.Base.IsFrontend = true
			resolverCalls := 0
			resolverErr := errors.New("sql_mode must not be resolved for unevaluated rows")
			proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
				resolverCalls++
				return nil, resolverErr
			})

			wanted := make([]string, tc.length)
			nulls := make([]bool, tc.length)
			for i := range nulls {
				nulls[i] = true
			}
			caseRun := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{tc.input},
				NewFunctionTestResult(statementDigestResultType(), false, wanted, nulls),
				StatementDigest,
			).WithSelectList(tc.selectList)
			// A non-empty constant vector is used for the zero-row case to ensure
			// the function does not enter the generic helper's constant fast path.
			if tc.length == 0 {
				caseRun.fnLength = 0
			}

			succeed, info := caseRun.Run()
			require.True(t, succeed, info)
			require.Zero(t, resolverCalls)
		})
	}
}

func TestStatementDigestResolvesSQLModeOnlyForActiveRows(t *testing.T) {
	t.Run("resolve once when at least one row is evaluated", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		proc.Base.IsFrontend = true
		resolverCalls := 0
		proc.SetResolveVariableFunc(func(name string, system, global bool) (any, error) {
			resolverCalls++
			require.Equal(t, "sql_mode", name)
			require.True(t, system)
			require.False(t, global)
			return "ANSI_QUOTES", nil
		})

		inputs := []string{`SELECT "one"`, `SELECT "ignored"`, `SELECT "two"`}
		caseRun := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{NewFunctionTestInput(
				types.T_varchar.ToType(), inputs, []bool{false, true, false},
			)},
			NewFunctionTestResult(statementDigestResultType(), false,
				[]string{
					statementDigestHashForTest(statementDigestFormatForTest(t, inputs[0], "ANSI_QUOTES")),
					"",
					statementDigestHashForTest(statementDigestFormatForTest(t, inputs[2], "ANSI_QUOTES")),
				},
				[]bool{false, true, false}),
			StatementDigest,
		)
		succeed, info := caseRun.Run()
		require.True(t, succeed, info)
		require.Equal(t, 1, resolverCalls, "one call per vector invocation, not per row")
	})

	for _, tc := range []struct {
		name    string
		resolve func() (any, error)
		message string
	}{
		{
			name: "resolver error",
			resolve: func() (any, error) {
				return nil, errors.New("resolve sql_mode")
			},
			message: "resolve sql_mode",
		},
		{
			name: "invalid resolver type",
			resolve: func() (any, error) {
				return int64(123), nil
			},
			message: "unexpected sql_mode type int64",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.Base.IsFrontend = true
			resolverCalls := 0
			proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
				resolverCalls++
				return tc.resolve()
			})

			caseRun := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{NewFunctionTestInput(
					types.T_varchar.ToType(), []string{"SELECT 1"}, nil,
				)},
				NewFunctionTestResult(statementDigestResultType(), false, nil, nil),
				StatementDigest,
			)
			require.NoError(t, caseRun.result.PreExtendAndReset(caseRun.fnLength))
			err := StatementDigest(caseRun.parameters, caseRun.result, proc, caseRun.fnLength, nil)
			require.ErrorContains(t, err, tc.message)
			require.Equal(t, 1, resolverCalls)
			require.Zero(t, caseRun.GetResultVectorDirectly().Length(), "failed mode resolution must not emit a digest")
		})
	}
}
