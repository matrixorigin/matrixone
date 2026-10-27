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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestStatementDigestMySQL84(t *testing.T) {
	data, err := os.ReadFile("testdata/statement_digest_mysql84.json")
	require.NoError(t, err)
	var cases []struct {
		SQLMode   string `json:"sql_mode"`
		SQL       string `json:"sql"`
		Text      string `json:"text"`
		Digest    string `json:"digest"`
		ErrorCode int    `json:"error_code"`
	}
	require.NoError(t, json.Unmarshal(data, &cases))
	require.NotEmpty(t, cases)
	for i, tc := range cases {
		t.Run(fmt.Sprintf("%03d", i), func(t *testing.T) {
			text, hash, err := StatementDigest(context.Background(), tc.SQL, tc.SQLMode)
			if tc.ErrorCode != 0 {
				require.Error(t, err, "SQL=%q mode=%q", tc.SQL, tc.SQLMode)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrDigestParse), "%v", err)
				return
			}
			require.NoError(t, err, "SQL=%q mode=%q", tc.SQL, tc.SQLMode)
			require.Equal(t, tc.Text, text, "SQL=%q mode=%q", tc.SQL, tc.SQLMode)
			require.Equal(t, tc.Digest, hash, "SQL=%q mode=%q", tc.SQL, tc.SQLMode)
		})
	}
}

func TestStatementDigestParseLimitsAndScannerReuse(t *testing.T) {
	for _, limits := range []ParseLimits{
		{Input: 7, Tokens: 100, Work: 1000},
		{Input: 100, Tokens: 1, Work: 1000},
		{Input: 100, Tokens: 100, Work: 1},
	} {
		_, _, err := StatementDigest(WithParseLimits(t.Context(), limits), "SELECT 1", "")
		require.ErrorIs(t, err, ErrParseLimit)
		text, _, err := StatementDigest(t.Context(), "SELECT 1", "")
		require.NoError(t, err)
		require.Equal(t, "SELECT ?", text)
	}
	// Hint content is skipped by syntax parsing but tokenized by the second
	// pass. Force that pass's own abort/release path, not a first-pass failure.
	hintSQL := "SELECT /*+ MAX_EXECUTION_TIME(1000) */ 1"
	hintContext := WithParseLimits(t.Context(), ParseLimits{Input: 1000, Tokens: 3, Work: 1000})
	statements, err := ParseWithSQLMode(hintContext, hintSQL, 1, "")
	require.NoError(t, err)
	for _, statement := range statements {
		statement.Free()
	}
	_, _, err = StatementDigest(hintContext, hintSQL, "")
	require.ErrorIs(t, err, ErrParseLimit)
	_, _, err = StatementDigest(t.Context(), strings.Repeat(" ", 1<<20+1), "")
	require.ErrorIs(t, err, ErrParseLimit)

	ctx, cancel := context.WithCancelCause(t.Context())
	cause := errors.New("digest invocation canceled")
	cancel(cause)
	_, _, err = StatementDigest(ctx, "SELECT 1", "")
	require.ErrorIs(t, err, cause)

	for _, sql := range []string{"SELECT ROW(1)", "SELECT 'unterminated", "SELECT /*!99999 1+ */ 2"} {
		_, _, _ = StatementDigest(t.Context(), sql, "ANSI_QUOTES")
		func() {
			scanner := NewScanner(dialect.MYSQL, "SELECT 1")
			defer PutScanner(scanner)
			require.False(t, scanner.digestMode)
			require.Nil(t, scanner.parseControl)
		}()
	}
	// Digest version filtering must not leak into MO's pre-existing executable
	// comment policy on a subsequent ordinary parse.
	statement, err := ParseOne(t.Context(), "SELECT /*!99999 1+ */ 2", 1)
	require.NoError(t, err)
	defer statement.Free()
	require.Equal(t, "select 1 + 2", tree.String(statement, dialect.MYSQL))
}

func BenchmarkStatementDigest(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_, _, err := StatementDigest(b.Context(), "SELECT a FROM t WHERE id IN (1,2,3)", "")
		if err != nil {
			b.Fatal(err)
		}
	}
}
