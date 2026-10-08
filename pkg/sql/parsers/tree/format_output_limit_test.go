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

package tree

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

type fmtSequence []NodeFormatter

func (nodes fmtSequence) Format(ctx *FmtCtx) {
	for _, node := range nodes {
		node.Format(ctx)
	}
}

type fmtText string

func (text fmtText) Format(ctx *FmtCtx) { ctx.WriteString(string(text)) }

type fmtSQLValue struct {
	typ   P_TYPE
	value string
}

func (value fmtSQLValue) Format(ctx *FmtCtx) {
	_, _ = ctx.WriteValue(value.typ, value.value)
}

type fmtIdentifier string

func (identifier fmtIdentifier) Format(ctx *FmtCtx) {
	ctx.WriteIdentifier(Identifier(identifier))
}

type fmtVisit struct{ visited *bool }

func (node fmtVisit) Format(ctx *FmtCtx) {
	*node.visited = true
	ctx.WriteString("visited")
}

func TestFmtCtxOutputLimitAbortsTraversalAtFirstOverflow(t *testing.T) {
	visited := false
	ctx := NewFmtCtx(dialect.MYSQL, WithMaxOutputBytes(3))
	complete := ctx.FormatNode(fmtSequence{
		fmtText("ab"),
		fmtText("cd"),
		fmtVisit{visited: &visited},
	})

	require.False(t, complete)
	require.True(t, ctx.OutputLimitExceeded())
	require.Equal(t, "ab", ctx.String())
	require.False(t, visited, "formatting must stop instead of visiting nodes after overflow")
}

func TestFmtCtxOutputLimitPreservesNormalSQLQuoting(t *testing.T) {
	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteIdentifier(), WithSingleQuoteString())
	ctx.WriteIdentifier(Identifier("a`b"))
	ctx.WriteByte(' ')
	_, err := ctx.WriteValue(P_char, "c'd")
	require.NoError(t, err)
	ctx.WriteByte(' ')
	_, err = ctx.WriteValue(P_ScoreBinary, "e'f")
	require.NoError(t, err)
	require.Equal(t, "`a``b` 'c''d' _binary 'e''f'", ctx.String())

	bounded := NewFmtCtx(dialect.MYSQL, WithSingleQuoteString(), WithMaxOutputBytes(8))
	require.True(t, bounded.FormatNode(fmtSQLValue{typ: P_char, value: "a'bcd"}))
	require.Equal(t, "'a''bcd'", bounded.String(), "exactly-at-limit output remains complete")

	overflow := NewFmtCtx(dialect.MYSQL, WithSingleQuoteString(), WithMaxOutputBytes(7))
	require.False(t, overflow.FormatNode(fmtSQLValue{typ: P_char, value: "a'bcd"}))
	require.True(t, overflow.OutputLimitExceeded())
}

func TestFmtCtxOutputLimitStreamsBackslashEscaping(t *testing.T) {
	value := "line\nquote' nul\x00 return\r tab\t backspace\b ctrl-z\x1a slash\\ and wildcard\\%"
	for _, tc := range []struct {
		name string
		typ  P_TYPE
	}{{name: "char", typ: P_char}, {name: "binary", typ: P_ScoreBinary}} {
		t.Run(tc.name, func(t *testing.T) {
			stmt := NewNumVal(value, value, false, tc.typ)
			wantCtx := NewFmtCtx(dialect.MYSQL, WithSingleQuoteString())
			wantCtx.WriteValue(tc.typ, FormatString(value))

			gotCtx := NewFmtCtx(dialect.MYSQL, WithSingleQuoteString(), WithMaxOutputBytes(wantCtx.Len()))
			require.True(t, gotCtx.FormatNode(stmt))
			require.Equal(t, wantCtx.String(), gotCtx.String())
		})
	}

	positions := make([]StringLiteralPosition, 0, 1)
	unbounded := NewFmtCtx(dialect.MYSQL, WithStringLiteralPositions(&positions))
	require.True(t, unbounded.FormatNode(NewNumVal(value, value, false, P_char)))
	boundedPositions := make([]StringLiteralPosition, 0, 1)
	bounded := NewFmtCtx(
		dialect.MYSQL,
		WithStringLiteralPositions(&boundedPositions),
		WithMaxOutputBytes(unbounded.Len()),
	)
	require.True(t, bounded.FormatNode(NewNumVal(value, value, false, P_char)))
	require.Equal(t, unbounded.String(), bounded.String())
	require.Equal(t, positions, boundedPositions, "bounded formatting must preserve unquoted literal positions")

	limited := NewFmtCtx(dialect.MYSQL, WithSingleQuoteString(), WithMaxOutputBytes(16))
	largeEscapes := strings.Repeat("\\", 1<<20)
	require.False(t, limited.FormatNode(NewNumVal(largeEscapes, largeEscapes, false, P_char)))
	require.True(t, limited.OutputLimitExceeded())
	require.LessOrEqual(t, limited.Len(), 16)

	largeQuotes := strings.Repeat("'", 1<<20)
	visited := false
	quoteLimited := NewFmtCtx(dialect.MYSQL, WithSingleQuoteString(), WithMaxOutputBytes(16))
	require.False(t, quoteLimited.FormatNode(fmtSequence{
		fmtSQLValue{typ: P_char, value: largeQuotes},
		fmtVisit{visited: &visited},
	}))
	require.True(t, quoteLimited.OutputLimitExceeded())
	require.LessOrEqual(t, quoteLimited.Len(), 16)
	require.False(t, visited, "quote expansion overflow must abort later AST visits")

	largeBackticks := strings.Repeat("`", 1<<20)
	identifierLimited := NewFmtCtx(dialect.MYSQL, WithQuoteIdentifier(), WithMaxOutputBytes(16))
	require.False(t, identifierLimited.FormatNode(fmtSequence{
		fmtIdentifier(largeBackticks),
		fmtVisit{visited: &visited},
	}))
	require.True(t, identifierLimited.OutputLimitExceeded())
	require.LessOrEqual(t, identifierLimited.Len(), 16)
	require.False(t, visited, "identifier expansion overflow must abort later AST visits")
}
