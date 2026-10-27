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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestTopLevelOutfileReceiptAndCredentialRedaction(t *testing.T) {
	sql := "SELECT 'stage://audit/report@2026.csv?token=out_secret' AS marker INTO OUTFILE 'stage://audit/report@2026.csv?token=out_secret'"
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	receipt, ok := makeOutfileReceipt(stmt)
	require.True(t, ok)
	require.Equal(t, "stage://audit/report@2026.csv", receipt.Target)
	require.Equal(t, "csv", receipt.Format)
	require.True(t, receipt.TargetRedacted)
	require.False(t, receipt.TargetInvalid)

	redacted := redactStatementTextForLogging(stmt, sql)
	require.NotContains(t, redacted, "out_secret")
	require.NotContains(t, redacted, "token")
	require.Contains(t, redacted, receipt.Target)
	require.Equal(t, 2, strings.Count(redacted, receipt.Target), "both the projected literal and OUTFILE target must be redacted")

	ses := &Session{prepareStmts: map[string]*PrepareStmt{"audit": {Sql: sql, PrepareStmt: stmt}}}
	const env = "execute audit"
	for _, limit := range []int{0, 4, len(env), len(env) + 3, len(env) + 4, len(env) + 8, -1} {
		got := makeExecuteSql(t.Context(), ses, &tree.Select{}, true, "audit", env, limit)
		require.NotContains(t, got, "out_secret", "limit=%d", limit)
		if limit == -1 {
			require.Contains(t, got, receipt.Target)
		}
	}
	require.Equal(t, sql, ses.prepareStmts["audit"].Sql, "diagnostics do not mutate execution SQL")
}

func TestOutfileTargetRetainsFileServiceAcceptedNames(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	fs, err := fileservice.NewLocalFS(ctx, "outfile-audit-test", root, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)

	targets := []string{
		"stage://stage/report@2026.csv",
		"SHARED:/name@2.csv",
		"stage://stage/data%s.csv",
		"stage://stage/report%e.csv",
		"stage://stage/report%2b.csv",
		"stage://stage/报告.csv",
		"stage://stage/%E6%8A%A5%E5%91%8A.csv",
	}
	for _, target := range targets {
		t.Run(target, func(t *testing.T) {
			parsed, err := fileservice.ParsePath(target)
			require.NoError(t, err, "fileservice must accept the target before comparing receipt identity")
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "SELECT 1 INTO OUTFILE '"+target+"'", 1)
			require.NoError(t, err)
			receipt, ok := makeOutfileReceipt(stmt)
			require.True(t, ok)
			require.Equal(t, target, receipt.Target)
			require.False(t, receipt.TargetInvalid)

			content := []byte("sink-accepted")
			fullPath := filepath.Join(root, parsed.File)
			require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), 0o700))
			require.NoError(t, fs.Write(ctx, fileservice.IOVector{
				FilePath: parsed.File,
				Entries:  []fileservice.IOEntry{{Size: int64(len(content)), Data: content}},
			}))
			info, err := os.Stat(fullPath)
			require.NoError(t, err)
			require.Positive(t, info.Size(), "the local file-service sink must write the parsed target")

			safe := sanitizeOutfileTarget(target)
			require.Equal(t, target, safe.target)
			require.False(t, safe.invalid)
			require.False(t, safe.redacted)
		})
	}
}

func TestOutfileTargetRedactionPreservesCredentialBoundaries(t *testing.T) {
	for _, path := range []string{"报告.csv", "%E6%8A%A5%E5%91%8A.csv"} {
		safe := sanitizeOutfileTarget("stage://user:secret@stage/" + path + "?token=secret#private")
		require.Equal(t, "stage://stage/"+path, safe.target)
		require.True(t, safe.redacted)
		require.False(t, safe.invalid)
	}
	userinfo := sanitizeOutfileTarget("stage://user:secret@stage/private.csv")
	require.Equal(t, "stage://stage/private.csv", userinfo.target)
	require.True(t, userinfo.redacted)
	require.False(t, userinfo.invalid)

	serviceUserinfo := sanitizeOutfileTarget("user@host:/private.csv")
	require.Equal(t, "redacted", serviceUserinfo.target)
	require.True(t, serviceUserinfo.redacted)
	require.False(t, serviceUserinfo.invalid)

	namedCredential := sanitizeOutfileTarget("SHARED:/report@token=secret.csv")
	require.Equal(t, "redacted", namedCredential.target)
	require.True(t, namedCredential.redacted)
	require.False(t, namedCredential.invalid)
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
	require.False(t, malformed.invalid, "literal percent is legal in a fileservice path")
	require.Equal(t, "stage://audit/bad%zz.csv", malformed.target)

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
