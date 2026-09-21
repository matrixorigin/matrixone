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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/version"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func statementHashResultType() types.Type {
	typ := types.T_varchar.ToType()
	typ.Width = 64
	return typ
}

func statementHashHashForTest(formatted string) string {
	hash := sha256.Sum256([]byte(formatted))
	return hex.EncodeToString(hash[:])
}

func statementHashFormatForTest(t *testing.T, sql, sqlMode string) string {
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
	formatted, err := statementHashDeparse(statements[0])
	require.NoError(t, err)
	return formatted
}

func TestStatementHashTypeResolution(t *testing.T) {
	proc := testutil.NewProcess(t)
	require.True(t, GetFunctionIsVolatileOrRealTimeRelatedByName("mo_statement_hash"))
	for _, oid := range []types.T{
		types.T_varchar,
		types.T_text,
		types.T_blob,
		types.T_char,
		types.T_binary,
		types.T_varbinary,
	} {
		fn, err := GetFunctionByName(proc.Ctx, "mo_statement_hash", []types.Type{oid.ToType()})
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

	fn, err := GetFunctionByName(proc.Ctx, "mo_statement_hash", []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)
	castTypes, shouldCast := fn.ShouldDoImplicitTypeCast()
	require.True(t, shouldCast)
	require.Equal(t, []types.Type{formattedScalarStringType(types.T_int64.ToType())}, castTypes)

	_, err = GetFunctionByName(proc.Ctx, "mo_statement_hash", nil)
	require.Error(t, err)
	_, err = GetFunctionByName(proc.Ctx, "mo_statement_hash", []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()})
	require.Error(t, err)
}

func TestStatementHashUsesMatrixOneASTDeparse(t *testing.T) {
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
			formatted := statementHashFormatForTest(t, tc.sql, "")
			require.Equal(t, tc.formatted, formatted)

			got, err := statementHashValue(t.Context(), tc.sql, "")
			require.NoError(t, err)
			require.Equal(t, statementHashHashForTest(tc.formatted), string(got))
		})
	}

	// Hash equality follows canonical formatting, not the spelling or layout
	// of the original SQL. Literal values and identifier names remain distinct.
	one, err := statementHashValue(t.Context(), "SELECT 1", "")
	require.NoError(t, err)
	commented, err := statementHashValue(t.Context(), " select /* note */ 1; ", "")
	require.NoError(t, err)
	two, err := statementHashValue(t.Context(), "SELECT 2", "")
	require.NoError(t, err)
	quotedIdentifier, err := statementHashValue(t.Context(), "SELECT a", "")
	require.NoError(t, err)
	stringLiteral, err := statementHashValue(t.Context(), "SELECT 'a'", "")
	require.NoError(t, err)
	require.Equal(t, string(one), string(commented))
	require.NotEqual(t, string(one), string(two))
	require.NotEqual(t, string(quotedIdentifier), string(stringLiteral))
}

func TestStatementHashSQLModeAndVariables(t *testing.T) {
	plain, err := statementHashValue(t.Context(), `SELECT "name"`, "")
	require.NoError(t, err)
	ansi, err := statementHashValue(t.Context(), `SELECT "name"`, "ANSI_QUOTES")
	require.NoError(t, err)
	require.NotEqual(t, string(plain), string(ansi))

	param1, err := statementHashValue(t.Context(), "SELECT ?", "")
	require.NoError(t, err)
	param2, err := statementHashValue(t.Context(), "select ?", "")
	require.NoError(t, err)
	require.Equal(t, string(param1), string(param2))

	userVar, err := statementHashValue(t.Context(), "SELECT @name", "")
	require.NoError(t, err)
	otherUserVar, err := statementHashValue(t.Context(), "SELECT @other", "")
	require.NoError(t, err)
	require.NotEqual(t, string(userVar), string(otherUserVar))

	caseVariants := []string{"SELECT @MiXeD", "SELECT @mixed", "SELECT @`MIXED`"}
	var canonical string
	for i, sql := range caseVariants {
		got, err := statementHashValue(t.Context(), sql, "")
		require.NoError(t, err, sql)
		if i == 0 {
			canonical = string(got)
		} else {
			require.Equal(t, canonical, string(got), sql)
		}
	}

	unicodeUpper, err := statementHashValue(t.Context(), "SELECT @Ä", "")
	require.NoError(t, err)
	unicodeLower, err := statementHashValue(t.Context(), "SELECT @ä", "")
	require.NoError(t, err)
	require.Equal(t, string(unicodeUpper), string(unicodeLower))

	globalSystem, err := statementHashValue(t.Context(), "SELECT @@global.TIME_ZONE", "")
	require.NoError(t, err)
	otherGlobalSystem, err := statementHashValue(t.Context(), "SELECT @@GLOBAL.time_zone", "")
	require.NoError(t, err)
	sessionSystem, err := statementHashValue(t.Context(), "SELECT @@session.time_zone", "")
	require.NoError(t, err)
	require.Equal(t, string(globalSystem), string(otherGlobalSystem))
	require.NotEqual(t, string(globalSystem), string(sessionSystem))
}

func TestStatementHashFormatsNestedAndSpecialStatements(t *testing.T) {
	for _, sql := range []string{
		"SELECT TRIM(BOTH 'x' FROM 'xxx')",
		"SELECT (SELECT 1)",
		"PREPARE p FROM 'SELECT 1'",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := statementHashValue(t.Context(), sql, "")
			require.NoError(t, err)
		})
	}
}

func TestStatementHashSQLModeSnapshotAndErrors(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Base.IsFrontend = false
	proc.Base.SessionInfo.SqlMode = "ANSI_QUOTES"
	proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
		return nil, errors.New("receiving CN resolver must not be consulted")
	})
	sqlMode, err := statementHashSQLMode(proc)
	require.NoError(t, err)
	require.Equal(t, "ANSI_QUOTES", sqlMode)

	proc.Base.IsFrontend = true
	proc.SetResolveVariableFunc(func(name string, system, global bool) (any, error) {
		require.Equal(t, "sql_mode", name)
		require.True(t, system)
		require.False(t, global)
		return int64(123), nil
	})
	_, err = statementHashSQLMode(proc)
	require.EqualError(t, err, "internal error: unexpected sql_mode type int64")

	proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
		return nil, errors.New("resolve sql_mode")
	})
	_, err = statementHashSQLMode(proc)
	require.EqualError(t, err, "resolve sql_mode")

	proc.Base.SessionInfo.SqlMode = process.EmptySqlModeSentinel
	proc.Base.IsFrontend = false
	proc.SetResolveVariableFunc(nil)
	sqlMode, err = statementHashSQLMode(proc)
	require.NoError(t, err)
	require.Empty(t, sqlMode)
}

func TestStatementHashRejectsInvalidAndMultipleStatements(t *testing.T) {
	for _, input := range []string{
		"",
		"   ",
		"/* comment only */",
		";;;",
		"SELECT FROM",
		"SELECT 1 trailing garbage",
		"SELECT 1; SELECT 2",
		"/*!80000 SELECT FROM */",
		string([]byte{0xff, 0xfe}),
	} {
		t.Run(input, func(t *testing.T) {
			_, err := statementHashValue(t.Context(), input, "")
			require.Error(t, err)
		})
	}

	// Parser errors must not leave reusable parser state that affects the next
	// invocation.
	_, err := statementHashValue(t.Context(), "SELECT FROM", "")
	require.Error(t, err)
	got, err := statementHashValue(t.Context(), "SELECT 1", "")
	require.NoError(t, err)
	require.Equal(t, statementHashHashForTest("select 1"), string(got))
}

func TestStatementHashReturnsParserErrorForUnsupportedMySQLJSONTable(t *testing.T) {
	// This is valid MySQL JSON_TABLE syntax, but MatrixOne's parser does not yet
	// implement JSON_TABLE. The native hash follows MatrixOne's parser contract:
	// return its syntax error instead of hashing a rejected statement.
	sql := "SELECT * FROM JSON_TABLE('[1]', '$[*]' COLUMNS(x INT PATH '$' NULL ON EMPTY ERROR ON ERROR)) AS jt"
	statements, parseErr := parsers.ParseWithSQLMode(t.Context(), dialect.MYSQL, sql, 0, "")
	for _, stmt := range statements {
		if stmt != nil {
			stmt.Free()
		}
	}
	t.Logf("MatrixOne parser error for JSON_TABLE reproducer: %v", parseErr)
	require.ErrorContains(t, parseErr, "syntax error at line 1 column 46",
		"MatrixOne itself rejects the unsupported JSON_TABLE grammar at COLUMNS")

	hash, hashErr := statementHashValue(t.Context(), sql, "")
	require.Nil(t, hash, "a rejected statement must not produce a statement hash")
	require.EqualError(t, hashErr, parseErr.Error(), "MO_STATEMENT_HASH must propagate the parser's error unchanged")
}

func TestStatementHashInputSizeLimit(t *testing.T) {
	input := "SELECT 1" + strings.Repeat(" ", maxStatementHashInputBytes-len("SELECT 1"))
	got, err := statementHashValue(t.Context(), input, "")
	require.NoError(t, err, "the exact input limit is accepted")
	require.Equal(t, statementHashHashForTest("select 1"), string(got))

	_, err = statementHashValue(t.Context(), strings.Repeat(" ", maxStatementHashInputBytes+1), "")
	require.ErrorContains(t, err, "maximum is")
}

func TestStatementHashInputComplexityLimits(t *testing.T) {
	t.Run("nesting limit is accepted", func(t *testing.T) {
		input := "SELECT " + strings.Repeat("(", maxStatementHashNesting) + "1" +
			strings.Repeat(")", maxStatementHashNesting)
		_, err := statementHashValue(t.Context(), input, "")
		require.NoError(t, err)
	})

	t.Run("nesting above limit is rejected before parsing", func(t *testing.T) {
		input := "SELECT " + strings.Repeat("(", maxStatementHashNesting+1) + "1" +
			strings.Repeat(")", maxStatementHashNesting+1)
		_, err := statementHashValue(t.Context(), input, "")
		require.ErrorContains(t, err,
			"maximum nesting depth of 512")
	})

	t.Run("token limit boundary", func(t *testing.T) {
		atLimit := strings.Repeat(";", maxStatementHashTokenCount)
		require.NoError(t, validateStatementHashInputComplexity(t.Context(), atLimit, ""))

		overLimit := atLimit + ";"
		err := validateStatementHashInputComplexity(t.Context(), overLimit, "")
		require.ErrorContains(t, err, "maximum of 16384 SQL tokens")
	})

	t.Run("delimiters inside strings and comments do not count", func(t *testing.T) {
		require.NoError(t, validateStatementHashInputComplexity(
			t.Context(), "SELECT '(' /* ((( */", ""))
	})
}

func TestStatementHashRejectsOversizedInputThroughFunction(t *testing.T) {
	proc := testutil.NewProcess(t)
	resolverCalls := 0
	proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
		resolverCalls++
		return nil, errors.New("resolver must not run for an oversized statement")
	})
	input := strings.Repeat(" ", maxStatementHashInputBytes+1)
	caseRun := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{input}, nil)},
		NewFunctionTestResult(statementHashResultType(), true, nil, nil),
		StatementHash,
	)
	succeed, info := caseRun.Run()
	require.True(t, succeed, info)
	require.Zero(t, caseRun.GetResultVectorDirectly().Length(), "oversized input must not produce a hash")
	require.Zero(t, resolverCalls, "reject oversized input before resolving session state")
}

func TestStatementHashReturnsParseErrorsThroughFunction(t *testing.T) {
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
				NewFunctionTestResult(statementHashResultType(), true, nil, nil),
				StatementHash,
			)
			succeed, info := caseRun.Run()
			require.True(t, succeed, info)
			require.Zero(t, caseRun.GetResultVectorDirectly().Length(), "failed input must not produce a hash")
		})
	}
}

func TestStatementHashFormatFailureIsAnError(t *testing.T) {
	_, err := statementHashDeparse(nil)
	require.EqualError(t, err, "not supported: cannot format a nil statement for MO_STATEMENT_HASH")

	_, err = statementHashDeparse(testStatementHashFormatter{panicOnFormat: true})
	require.EqualError(t, err, "internal error: AST formatter failed during MO_STATEMENT_HASH")

	_, err = statementHashDeparse(testStatementHashFormatter{})
	require.EqualError(t, err, "not supported: AST formatter produced empty output for MO_STATEMENT_HASH")

	formattedLen := 0
	_, err = statementHashDeparse(testStatementHashFormatter{
		formatted:    strings.Repeat("x", maxStatementHashFormattedBytes+1),
		formattedLen: &formattedLen,
	})
	require.EqualError(t, err,
		"invalid input: MO_STATEMENT_HASH formatted statement exceeds the maximum of 4194304 bytes")
	require.Equal(t, maxStatementHashFormattedBytes, formattedLen,
		"the formatter buffer must stop at the configured bound")
}

type testStatementHashFormatter struct {
	panicOnFormat bool
	formatted     string
	formattedLen  *int
}

func (s testStatementHashFormatter) String() string { return "test" }
func (s testStatementHashFormatter) Format(ctx *tree.FmtCtx) {
	if s.panicOnFormat {
		panic("test formatter panic")
	}
	ctx.WriteString(s.formatted)
	if s.formattedLen != nil {
		*s.formattedLen = ctx.Len()
	}
}
func (testStatementHashFormatter) GetStatementType() string { return "test" }
func (testStatementHashFormatter) GetQueryType() string     { return "test" }
func (testStatementHashFormatter) StmtKind() tree.StmtKind  { return 0 }
func (testStatementHashFormatter) Free()                    {}

func TestStatementHashCancelledContextDoesNotParse(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := statementHashValue(ctx, "SELECT 1", "")
	require.ErrorIs(t, err, context.Canceled)
}

func TestStatementHashNullConstantAndMaskedRows(t *testing.T) {
	proc := testutil.NewProcess(t)

	nullCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{NewFunctionTestInput(
			types.T_varchar.ToType(),
			[]string{"SELECT 1", "", "select 2"},
			[]bool{false, true, false},
		)},
		NewFunctionTestResult(
			statementHashResultType(),
			false,
			[]string{statementHashHashForTest("select 1"), "", statementHashHashForTest("select 2")},
			[]bool{false, true, false},
		),
		StatementHash,
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
			statementHashResultType(),
			false,
			[]string{statementHashHashForTest("select 1"), statementHashHashForTest("select 1"), statementHashHashForTest("select 1")},
			nil,
		),
		StatementHash,
	)
	succeed, info = constCase.Run()
	require.True(t, succeed, info)

	maskedConstCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{NewFunctionTestConstInput(
			types.T_varchar.ToType(), []string{"SELECT 1", "unused"}, nil,
		)},
		NewFunctionTestResult(
			statementHashResultType(),
			false,
			[]string{"", statementHashHashForTest("select 1")},
			[]bool{true, false},
		),
		StatementHash,
	).WithSelectList(&FunctionSelectList{
		AnyNull:    true,
		SelectList: []bool{false, true},
	})
	succeed, info = maskedConstCase.Run()
	require.True(t, succeed, info)

	// A constant invalid statement must not be parsed when the selection mask
	// has no active rows. This is distinct from AllNull: short-circuit masks
	// can represent the same empty evaluation with AnyNull=true and all rows
	// ignored.
	allMaskedConstCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{NewFunctionTestConstInput(
			types.T_varchar.ToType(), []string{"SELECT FROM", "unused"}, nil,
		)},
		NewFunctionTestResult(
			statementHashResultType(), false,
			[]string{"", ""}, []bool{true, true},
		),
		StatementHash,
	).WithSelectList(&FunctionSelectList{
		AnyNull:    true,
		SelectList: []bool{false, false},
	})
	succeed, info = allMaskedConstCase.Run()
	require.True(t, succeed, info)

	maskedCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{NewFunctionTestInput(
			types.T_varchar.ToType(),
			[]string{"SELECT FROM", "SELECT 1"},
			nil,
		)},
		NewFunctionTestResult(
			statementHashResultType(),
			false,
			[]string{"", statementHashHashForTest("select 1")},
			[]bool{true, false},
		),
		StatementHash,
	).WithSelectList(&FunctionSelectList{
		AnyNull:    true,
		SelectList: []bool{false, true},
	})
	succeed, info = maskedCase.Run()
	require.True(t, succeed, info)
}

func TestStatementHashDoesNotResolveSQLModeForUnevaluatedRows(t *testing.T) {
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
		{
			name: "oversized input is masked",
			input: NewFunctionTestInput(
				types.T_varchar.ToType(),
				[]string{"SELECT 1", strings.Repeat("x", maxStatementHashInputBytes+1)},
				nil,
			),
			length:     2,
			selectList: &FunctionSelectList{AnyNull: true, SelectList: []bool{false, false}},
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
				NewFunctionTestResult(statementHashResultType(), false, wanted, nulls),
				StatementHash,
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

func TestStatementHashSkipsMaskedOversizedInputWhileEvaluatingActiveRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Base.IsFrontend = true
	resolverCalls := 0
	proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
		resolverCalls++
		return "STRICT_TRANS_TABLES", nil
	})
	caseRun := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{NewFunctionTestInput(
			types.T_varchar.ToType(),
			[]string{"SELECT 1", strings.Repeat("x", maxStatementHashInputBytes+1)},
			nil,
		)},
		NewFunctionTestResult(
			statementHashResultType(),
			false,
			[]string{statementHashHashForTest("select 1"), ""},
			[]bool{false, true},
		),
		StatementHash,
	).WithSelectList(&FunctionSelectList{
		AnyNull:    true,
		SelectList: []bool{true, false},
	})

	succeed, info := caseRun.Run()
	require.True(t, succeed, info)
	require.Equal(t, 1, resolverCalls)
}

func TestStatementHashRejectsRemoteWorkerBuildMismatch(t *testing.T) {
	oldBuildCommitID := version.BuildCommitID
	version.BuildCommitID = strings.Repeat("a", 40)
	t.Cleanup(func() { version.BuildCommitID = oldBuildCommitID })

	proc := testutil.NewProcess(t)
	proc.Base.IsFrontend = false
	remoteSession, err := process.ConvertToProcessSessionInfo(pipeline.SessionInfo{
		SqlMode:                            process.EmptySqlModeSentinel,
		StatementHashExpectedBuildCommitId: version.BuildCommitID,
	})
	require.NoError(t, err)
	proc.Base.SessionInfo = remoteSession
	matchingBuild := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{"SELECT 1"}, nil)},
		NewFunctionTestResult(statementHashResultType(), false,
			[]string{statementHashHashForTest("select 1")}, nil),
		StatementHash,
	)
	succeed, info := matchingBuild.Run()
	require.True(t, succeed, info)

	// A receiver that matched during dispatch can be replaced before function
	// execution; the active-row fence must still reject the new build.
	version.BuildCommitID = strings.Repeat("b", 40)
	for _, tc := range []struct {
		name       string
		input      FunctionTestInput
		length     int
		selectList *FunctionSelectList
	}{
		{
			name:   "zero-row batch",
			input:  NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"SELECT 1"}, nil),
			length: 0,
		},
		{
			name: "all-null batch",
			input: NewFunctionTestInput(
				types.T_varchar.ToType(), []string{"SELECT 1", "SELECT 2"}, []bool{true, true},
			),
			length: 2,
		},
		{
			name: "all-masked batch",
			input: NewFunctionTestInput(
				types.T_varchar.ToType(), []string{"SELECT 1", "SELECT 2"}, nil,
			),
			length: 2,
			selectList: &FunctionSelectList{
				AnyNull: true, SelectList: []bool{false, false},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caseRun := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{tc.input},
				NewFunctionTestResult(statementHashResultType(), true, nil, nil),
				StatementHash,
			).WithSelectList(tc.selectList)
			caseRun.fnLength = tc.length
			succeed, info := caseRun.Run()
			require.True(t, succeed, info)
		})
	}

	caseRun := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{"SELECT 1"}, nil)},
		NewFunctionTestResult(statementHashResultType(), true, nil, nil),
		StatementHash,
	)
	succeed, info = caseRun.Run()
	require.True(t, succeed, info)
	require.Zero(t, caseRun.GetResultVectorDirectly().Length(),
		"a worker with a different formatter build must not produce a hash")
}

func TestStatementHashDefersRemoteSQLModeErrorsUntilActiveRows(t *testing.T) {
	resolverErr := moerr.NewInternalErrorNoCtx("captured resolver failure")
	resolverErr.SetDetail("origin detail")
	encodedResolverErr, err := resolverErr.MarshalBinary()
	require.NoError(t, err)

	for _, tc := range []struct {
		name       string
		input      FunctionTestInput
		length     int
		selectList *FunctionSelectList
		wantError  bool
	}{
		{
			name: "zero-row remote batch",
			input: NewFunctionTestInput(
				types.T_varchar.ToType(), []string{"SELECT 1"}, nil,
			),
			length: 0,
		},
		{
			name: "all rows masked remotely",
			input: NewFunctionTestInput(
				types.T_varchar.ToType(), []string{"SELECT 1", "SELECT 2"}, nil,
			),
			length: 2,
			selectList: &FunctionSelectList{
				AnyNull: true, SelectList: []bool{false, false},
			},
		},
		{
			name: "all input rows null remotely",
			input: NewFunctionTestInput(
				types.T_varchar.ToType(), []string{"SELECT 1", "SELECT 2"}, []bool{true, true},
			),
			length: 2,
		},
		{
			name: "active remote row returns captured error",
			input: NewFunctionTestInput(
				types.T_varchar.ToType(), []string{"SELECT 1"}, nil,
			),
			length:    1,
			wantError: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.Base.IsFrontend = false
			proc.Base.SessionInfo.SqlMode = process.EmptySqlModeSentinel
			proc.Base.SessionInfo.StatementHashSQLModeError = append([]byte(nil), encodedResolverErr...)
			proc.Base.SessionInfo.StatementHashSQLModeErrorDetail = resolverErr.Detail()
			resolverCalls := 0
			proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) {
				resolverCalls++
				return nil, errors.New("worker resolver must not replace the captured error")
			})

			var values []string
			var nulls []bool
			if tc.length > 0 && !tc.wantError {
				values = make([]string, tc.length)
				nulls = make([]bool, tc.length)
				for i := range nulls {
					nulls[i] = true
				}
			}
			caseRun := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{tc.input},
				NewFunctionTestResult(statementHashResultType(), tc.wantError, values, nulls),
				StatementHash,
			).WithSelectList(tc.selectList)
			if tc.length == 0 {
				caseRun.fnLength = 0
			}

			succeed, info := caseRun.Run()
			require.True(t, succeed, info)
			require.Zero(t, resolverCalls)
			if tc.wantError {
				require.Zero(t, caseRun.GetResultVectorDirectly().Length(),
					"active resolver failure must not produce a hash")
			}
		})
	}
}

func TestStatementHashResolvesSQLModeOnlyForActiveRows(t *testing.T) {
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
			NewFunctionTestResult(statementHashResultType(), false,
				[]string{
					statementHashHashForTest(statementHashFormatForTest(t, inputs[0], "ANSI_QUOTES")),
					"",
					statementHashHashForTest(statementHashFormatForTest(t, inputs[2], "ANSI_QUOTES")),
				},
				[]bool{false, true, false}),
			StatementHash,
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
				NewFunctionTestResult(statementHashResultType(), false, nil, nil),
				StatementHash,
			)
			require.NoError(t, caseRun.result.PreExtendAndReset(caseRun.fnLength))
			err := StatementHash(caseRun.parameters, caseRun.result, proc, caseRun.fnLength, nil)
			require.ErrorContains(t, err, tc.message)
			require.Equal(t, 1, resolverCalls)
			require.Zero(t, caseRun.GetResultVectorDirectly().Length(), "failed mode resolution must not emit a hash")
		})
	}
}
