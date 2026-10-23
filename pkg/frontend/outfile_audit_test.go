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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestTopLevelOutfileReceiptAndCredentialRedaction(t *testing.T) {
	sql := "SELECT 'stage://audit/out.csv?token=out_secret' AS marker INTO OUTFILE 'stage://audit/out.csv?token=out_secret'"
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	receipt, ok := makeOutfileReceipt(stmt)
	require.True(t, ok)
	require.Equal(t, "stage://audit/out.csv", receipt.Target)
	require.Equal(t, "csv", receipt.Format)
	require.True(t, receipt.TargetRedacted)
	require.False(t, receipt.TargetInvalid)

	redacted := redactStatementTextForLogging(stmt, sql)
	require.NotContains(t, redacted, "out_secret")
	require.NotContains(t, redacted, "token")
	require.Contains(t, redacted, receipt.Target)
	require.Equal(t, 2, strings.Count(redacted, receipt.Target), "both the projected literal and OUTFILE target must be redacted")
}

func TestOutfileTargetRetentionIsBoundedAndOrdinarySelectIsUntouched(t *testing.T) {
	for _, target := range []string{
		"stage://audit/out_%d.csv",
		"stage://audit/out_%05d.csv",
		"stage://audit/out_%25d_%d.csv",
		"stage://audit/__matrixone_outfile_format_d__/out_%d_%05d_%d.csv",
		"stage://audit/" + strings.Repeat("part_%d_", 100) + "end.csv",
	} {
		t.Run(target, func(t *testing.T) {
			safe := sanitizeOutfileTarget(target)
			require.Equal(t, target, safe.target)
			require.False(t, safe.invalid)
		})
	}
	malformed := sanitizeOutfileTarget("stage://audit/bad%zz.csv")
	require.True(t, malformed.invalid)
	require.Empty(t, malformed.target)

	longTarget := "SHARED:/" + strings.Repeat("x", 5000) + ".csv"
	safe := sanitizeOutfileTarget(longTarget)
	require.LessOrEqual(t, len(safe.target), outfileAuditTargetLimit)
	require.True(t, safe.truncated)
	require.False(t, safe.invalid)

	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "SELECT 1", 1)
	require.NoError(t, err)
	_, ok := makeOutfileReceipt(stmt)
	require.False(t, ok)
	require.Equal(t, "SELECT 1", redactOutfileStatementText(stmt, "SELECT 1"))
}

func TestTopLevelOutfileAdmissionIgnoresNonOutfileStatements(t *testing.T) {
	for _, sql := range []string{
		"SELECT 1",
		"SELECT 1 INTO @value",
		"EXPLAIN SELECT 1 INTO OUTFILE '/tmp/ignored.csv'",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			_, ok := makeOutfileReceipt(stmt)
			require.False(t, ok)
		})
	}
}
