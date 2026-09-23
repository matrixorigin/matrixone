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

package frontend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type fingerprintTestStatement struct {
	text       string
	panicOnFmt bool
	formatted  bool
	cancel     context.CancelFunc
}

func (s *fingerprintTestStatement) String() string           { return s.text }
func (s *fingerprintTestStatement) GetStatementType() string { return "test" }
func (s *fingerprintTestStatement) GetQueryType() string     { return "test" }
func (s *fingerprintTestStatement) StmtKind() tree.StmtKind  { return 0 }
func (s *fingerprintTestStatement) Free()                    {}
func (s *fingerprintTestStatement) Format(ctx *tree.FmtCtx) {
	s.formatted = true
	if s.panicOnFmt {
		panic("formatter failure")
	}
	_, _ = ctx.WriteString(s.text)
	if s.cancel != nil {
		s.cancel()
	}
}

func TestFormatStatementFingerprintBoundsAndFailsOpen(t *testing.T) {
	t.Run("exact limit is complete and hashable", func(t *testing.T) {
		stmt := &fingerprintTestStatement{text: strings.Repeat("x", maxStatementFingerprintFormattedBytes)}
		got, attempted := formatStatementFingerprint(context.Background(), stmt)
		sum := sha256.Sum256([]byte(stmt.text))
		require.True(t, attempted)
		require.Equal(t, hex.EncodeToString(sum[:]), got)
	})

	t.Run("over limit is absent, never a hash of a truncated prefix", func(t *testing.T) {
		stmt := &fingerprintTestStatement{text: strings.Repeat("x", maxStatementFingerprintFormattedBytes+1)}
		got, attempted := formatStatementFingerprint(context.Background(), stmt)
		require.True(t, attempted)
		require.Empty(t, got)
	})

	t.Run("empty formatter output is absent", func(t *testing.T) {
		got, attempted := formatStatementFingerprint(context.Background(), &fingerprintTestStatement{})
		require.True(t, attempted)
		require.Empty(t, got)
	})

	t.Run("formatter panic is telemetry absence, not a query error", func(t *testing.T) {
		stmt := &fingerprintTestStatement{text: "select 1", panicOnFmt: true}
		got, attempted := formatStatementFingerprint(context.Background(), stmt)
		require.True(t, attempted)
		require.Empty(t, got)
	})

	t.Run("cancellation skips formatting", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		stmt := &fingerprintTestStatement{text: "select 1"}
		got, attempted := formatStatementFingerprint(ctx, stmt)
		require.True(t, attempted)
		require.Empty(t, got)
		require.False(t, stmt.formatted)
	})

	t.Run("cancellation observed after formatting omits the value", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stmt := &fingerprintTestStatement{text: "select 1", cancel: cancel}
		got, attempted := formatStatementFingerprint(ctx, stmt)
		require.True(t, attempted)
		require.Empty(t, got)
		require.True(t, stmt.formatted)
	})
}

func TestFormatStatementFingerprintDoesNotMutateAST(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select `MiXeD` from `SomeTable` where id = 'value' and @MixedName = 1", 1)
	require.NoError(t, err)
	defer stmt.Free()

	render := func() string {
		ctx := tree.NewFmtCtx(
			dialect.MYSQL,
			tree.WithQuoteIdentifier(),
			tree.WithSingleQuoteString(),
			tree.WithCanonicalUserVariableNames(),
			tree.WithMaxOutputBytes(maxStatementFingerprintFormattedBytes),
		)
		stmt.Format(ctx)
		require.False(t, ctx.OutputLimitExceeded())
		return ctx.String()
	}
	before := render()
	fingerprint, attempted := formatStatementFingerprint(context.Background(), stmt)
	after := render()

	require.True(t, attempted)
	require.NotEmpty(t, fingerprint)
	require.Equal(t, before, after, "fingerprinting must not mutate the parsed AST")
}

func TestFormatStatementFingerprintCanonicalizesUserVariableCase(t *testing.T) {
	parse := func(sql string) tree.Statement {
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		return stmt
	}
	upper := parse("select @MixedName")
	lower := parse("select @mixedname")
	defer upper.Free()
	defer lower.Free()

	upperFingerprint, upperAttempted := formatStatementFingerprint(context.Background(), upper)
	lowerFingerprint, lowerAttempted := formatStatementFingerprint(context.Background(), lower)
	require.True(t, upperAttempted)
	require.True(t, lowerAttempted)
	require.NotEmpty(t, upperFingerprint)
	require.Equal(t, upperFingerprint, lowerFingerprint)
}

func TestFormatStatementFingerprintTracksASTAndIgnoresLexicalNoise(t *testing.T) {
	parse := func(sql string) tree.Statement {
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		return stmt
	}
	fingerprint := func(stmt tree.Statement) string {
		got, attempted := formatStatementFingerprint(context.Background(), stmt)
		require.True(t, attempted)
		require.NotEmpty(t, got)
		return got
	}

	first := parse("select 1 + 2")
	second := parse(" SELECT /* ordinary comment */ 1+2 -- trailing comment\n")
	differentLiteral := parse("select 1 + 3")
	identifier := parse("select a from first_table")
	differentIdentifier := parse("select a from second_table")
	defer first.Free()
	defer second.Free()
	defer differentLiteral.Free()
	defer identifier.Free()
	defer differentIdentifier.Free()

	require.Equal(t, fingerprint(first), fingerprint(second),
		"whitespace and ordinary comments that do not change the parsed AST are excluded")
	require.NotEqual(t, fingerprint(first), fingerprint(differentLiteral),
		"different literal values produce different AST fingerprints")
	require.NotEqual(t, fingerprint(identifier), fingerprint(differentIdentifier),
		"different identifiers produce different AST fingerprints")
}

func TestFormatStatementFingerprintUsesTheAdmittedSQLModeAST(t *testing.T) {
	ctx := context.Background()
	stringAST, err := parsers.ParseOneWithSQLMode(ctx, dialect.MYSQL, `select "value"`, 1, "")
	require.NoError(t, err)
	identifierAST, err := parsers.ParseOneWithSQLMode(ctx, dialect.MYSQL,
		`select "value"`, 1, "ANSI_QUOTES")
	require.NoError(t, err)
	defer stringAST.Free()
	defer identifierAST.Free()

	stringFingerprint, stringAttempted := formatStatementFingerprint(ctx, stringAST)
	identifierFingerprint, identifierAttempted := formatStatementFingerprint(ctx, identifierAST)
	require.True(t, stringAttempted)
	require.True(t, identifierAttempted)
	require.NotEmpty(t, stringFingerprint)
	require.NotEmpty(t, identifierFingerprint)
	require.NotEqual(t, stringFingerprint, identifierFingerprint,
		"the fingerprint follows the AST admitted under the session SQL mode")
}
