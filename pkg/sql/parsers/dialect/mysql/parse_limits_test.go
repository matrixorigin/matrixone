// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestParseLimitsExactBoundariesAndOrdinaryParity(t *testing.T) {
	for _, sql := range []string{"select 1", "create view v as select a from t", "select 'a\\nb' /* comment */ as x", "select id from t for update nowait", "select id from t for share nowait", "select id from t for share skip locked"} {
		t.Run(sql, func(t *testing.T) {
			lexer := NewLexer(dialect.MYSQL, sql, 1)
			control := &parseControl{ctx: t.Context(), limits: ParseLimits{Input: len(sql), Tokens: 10000, Work: 10000}}
			lexer.scanner.parseControl = control
			require.Zero(t, yyParse(lexer))
			for _, stmt := range lexer.stmts {
				stmt.Free()
			}
			PutScanner(lexer.scanner)
			limits := ParseLimits{Input: len(sql), Tokens: control.tokens, Work: control.work}
			statements, err := ParseWithSQLMode(WithParseLimits(t.Context(), limits), sql, 1, "")
			require.NoError(t, err)
			for _, stmt := range statements {
				stmt.Free()
			}
			for _, less := range []ParseLimits{{Input: limits.Input - 1, Tokens: limits.Tokens, Work: limits.Work}, {Input: limits.Input, Tokens: limits.Tokens - 1, Work: limits.Work}, {Input: limits.Input, Tokens: limits.Tokens, Work: limits.Work - 1}} {
				_, err = ParseWithSQLMode(WithParseLimits(t.Context(), less), sql, 1, "")
				require.ErrorIs(t, err, ErrParseLimit)
			}
			ordinary, err := Parse(t.Context(), sql, 1)
			require.NoError(t, err)
			for _, stmt := range ordinary {
				stmt.Free()
			}
		})
	}
}
func TestParseLimitsCancellationAndLongScannerChains(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	cause := errors.New("stop schema parse")
	cancel(cause)
	_, err := ParseWithSQLMode(WithParseLimits(ctx, ParseLimits{Input: 1024, Tokens: 1024, Work: 1024}), "select 1", 1, "")
	require.ErrorIs(t, err, cause)
	sql := strings.Repeat("/* comment */", 8192) + "select 1"
	statements, err := ParseWithSQLMode(WithParseLimits(t.Context(), ParseLimits{Input: len(sql), Tokens: 8, Work: len(sql) * 4}), sql, 1, "")
	require.NoError(t, err)
	for _, stmt := range statements {
		stmt.Free()
	}
	scanner := NewScanner(dialect.MYSQL, strings.Repeat("begin ", 8192)+";")
	defer PutScanner(scanner)
	token, _ := scanner.Scan()
	require.Equal(t, SPBEGIN, token)
}
func TestIntegralGrammarReturnsErrorsInsteadOfPanicking(t *testing.T) {
	for _, sql := range []string{
		"create view v as select cast(1 as char(18446744073709551615))",
		"create view v as select sample(*, 18446744073709551615 rows) from t",
		"create table t(a decimal(18446744073709551615, 2))",
		"create table t(a timestamp(18446744073709551615))",
		"create table t(a float(18446744073709551615, 2))",
	} {
		t.Run(sql, func(t *testing.T) {
			require.NotPanics(t, func() {
				statements, err := Parse(t.Context(), sql, 1)
				for _, stmt := range statements {
					stmt.Free()
				}
				require.Error(t, err)
			})
		})
	}
}

func TestTypeLengthIntegralOverflow(t *testing.T) {
	// Exercise each grammar parameter position, including scale independently
	// of width and shared consumers outside CREATE TABLE.
	formats := []string{
		"create table t(a varchar(%s))", "create table t(a char(%s))",
		"create table t(a binary(%s))", "create table t(a varbinary(%s))",
		"create table t(a bit(%s))", "create table t(a int(%s))",
		"create table t(a year(%s))", "create table t(a text(%s))",
		"create table t(a blob(%s))", "create table t(a vecf32(%s))",
		"create table t(a timestamp(%s))", "create table t(a datetime(%s))",
		"create table t(a time(%s))", "create table t(a float(%s))",
		"create table t(a double(%s,0))", "create table t(a float(10,%s))",
		"create table t(a double precision(10,%s))", "create table t(a real(10,%s))",
		"create table t(a decimal(%s))", "create table t(a decimal(%s,0))",
		"create table t(a decimal(10,%s))", "create table t(a numeric(10,%s))",
		"alter table t add column a varchar(%s)", "select cast('a' as char(%s))",
		"create index i on t(a(%s))",
	}
	for _, format := range formats {
		for _, literal := range []string{
			"2147483648", "4294967295", "4294967296", "4294967297",
			"6442450944", "9223372036854775807", "18446744073709551615",
		} {
			sql := fmt.Sprintf(format, literal)
			t.Run(sql, func(t *testing.T) {
				statements, err := Parse(t.Context(), sql, 1)
				t.Cleanup(func() {
					for _, stmt := range statements {
						stmt.Free()
					}
				})
				expected := "value is out of range int32"
				if literal == "18446744073709551615" {
					expected = "value is out of range int64"
				}
				require.ErrorContains(t, err, expected)
				require.Empty(t, statements)
			})
		}
	}
}

func TestTypeLengthIntegralCompatibility(t *testing.T) {
	for _, tc := range []struct {
		typ          string
		width, scale int32
	}{
		{"varchar(10)", 10, 0}, {"varchar(0)", 0, 0}, {"varchar", -1, 0},
		{"varchar(2147483647)", 2147483647, 0}, {"varchar(70000)", 70000, 0},
		{"int(0)", -1, 0}, {"int", 0, 0}, {"int(2147483647)", 2147483647, 0},
		{"float", 0, -1}, {"float(0)", -1, -1}, {"float(24)", 24, -1},
		{"double(255,30)", 255, 30}, {"decimal", 10, 0}, {"decimal(65,30)", 65, 30},
		{"timestamp", 0, 0}, {"timestamp(0)", 0, 0}, {"timestamp(6)", 6, 6},
	} {
		t.Run(tc.typ, func(t *testing.T) {
			statements, err := Parse(t.Context(), "create table t(a "+tc.typ+")", 1)
			t.Cleanup(func() {
				for _, stmt := range statements {
					stmt.Free()
				}
			})
			require.NoError(t, err)
			require.Len(t, statements, 1)
			col := statements[0].(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef)
			it := col.Type.(*tree.T).InternalType
			require.Equal(t, tc.width, it.DisplayWith)
			require.Equal(t, tc.scale, it.Scale)
		})
	}
	for _, typ := range []string{"varchar(-1)", "float(256)", "double(10,31)", "decimal(66)", "decimal(2,3)"} {
		t.Run(typ, func(t *testing.T) {
			statements, err := Parse(t.Context(), "create table t(a "+typ+")", 1)
			t.Cleanup(func() {
				for _, stmt := range statements {
					stmt.Free()
				}
			})
			require.Error(t, err)
			require.Empty(t, statements)
		})
	}
	// The shared int64 conversion must still accept non-type arguments above
	// MaxInt32; export file size is not an int32 AST field.
	statements, err := Parse(t.Context(), "select 1 into outfile 'x' max_file_size 4294967297", 1)
	t.Cleanup(func() {
		for _, stmt := range statements {
			stmt.Free()
		}
	})
	require.NoError(t, err)
	require.Len(t, statements, 1)
	require.Equal(t, uint64(4294967297)*1024, statements[0].(*tree.Select).Ep.MaxFileSize)
}

func TestTypeLengthIntegralParserReuse(t *testing.T) {
	parser := &MySQLParser{}
	invalidStatements, err := parser.Parse(t.Context(), "select 1; create table t(a varchar(4294967297))", 1)
	t.Cleanup(func() {
		for _, stmt := range invalidStatements {
			stmt.Free()
		}
	})
	require.ErrorContains(t, err, "value is out of range int32")
	require.Empty(t, invalidStatements)
	statements, err := parser.Parse(t.Context(), "create table t(a varchar(10))", 1)
	t.Cleanup(func() {
		for _, stmt := range statements {
			stmt.Free()
		}
	})
	require.NoError(t, err)
	require.Len(t, statements, 1)
	col := statements[0].(*tree.CreateTable).Defs[0].(*tree.ColumnTableDef)
	require.Equal(t, int32(10), col.Type.(*tree.T).InternalType.DisplayWith)
}

// The real parser reaches the cancellation checkpoint after work has begun;
// the context controls only scheduling, not tokenization or parser results.
type cancelDuringParseContext struct {
	context.Context
	cancel context.CancelFunc
	checks int
}

func (c *cancelDuringParseContext) Value(any) any { return nil }
func (c *cancelDuringParseContext) Err() error {
	c.checks++
	if c.checks == 4 {
		c.cancel()
	}
	return c.Context.Err()
}
func TestParseLimitsCancelDuringScannerAndRecoverForNextParse(t *testing.T) {
	for _, sql := range []string{"select 1,2,3,4,5,6", "select /*" + strings.Repeat("x", 8192) + "*/ 1"} {
		base, cancel := context.WithCancel(t.Context())
		ctx := &cancelDuringParseContext{Context: base, cancel: cancel}
		statements, err := ParseWithSQLMode(WithParseLimits(ctx, ParseLimits{Input: len(sql), Tokens: 100, Work: len(sql) * 4}), sql, 1, "")
		cancel()
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, statements)
		require.GreaterOrEqual(t, ctx.checks, 4)
		statements, err = Parse(t.Context(), "select 1", 1)
		require.NoError(t, err)
		for _, stmt := range statements {
			stmt.Free()
		}
	}
}

// Seven lexical tokens plus EOF must still count when NOWAIT shares the mode's
// parser token; lexical folding cannot bypass the public token budget.
func TestSelectLockModifierTokenBudget(t *testing.T) {
	for _, mode := range []string{"update", "share"} {
		sql := "select id from t for " + mode + " nowait"
		for _, budget := range []int{7, 8} {
			limits := ParseLimits{Input: len(sql), Tokens: budget, Work: 10000}
			stmts, err := ParseWithSQLMode(WithParseLimits(t.Context(), limits), sql, 1, "")
			for _, stmt := range stmts {
				stmt.Free()
			}
			if budget == 7 {
				require.ErrorIs(t, err, ErrParseLimit)
			} else {
				require.NoError(t, err)
			}
		}
	}
}

func TestSelectSkipLockedTokenBudget(t *testing.T) {
	sql := "select id from t for share skip locked"
	for _, budget := range []int{8, 9} {
		limits := ParseLimits{Input: len(sql), Tokens: budget, Work: 10000}
		stmts, err := ParseWithSQLMode(WithParseLimits(t.Context(), limits), sql, 1, "")
		for _, stmt := range stmts {
			stmt.Free()
		}
		if budget == 8 {
			require.ErrorIs(t, err, ErrParseLimit)
		} else {
			require.NoError(t, err)
		}
	}
}
