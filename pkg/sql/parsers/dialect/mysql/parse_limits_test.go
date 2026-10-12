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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestParseLimitsExactBoundariesAndOrdinaryParity(t *testing.T) {
	for _, sql := range []string{"select 1", "create view v as select a from t", "select 'a\\nb' /* comment */ as x"} {
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
