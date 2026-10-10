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

package dml

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

// Capture the actual terminal exporter on wire SQL, then pass its bytes to the
// SQL history reader. Install hooks only while the fixture is stopped, and
// close it before restoring them; no concurrent config mutation is permitted.
func TestStatementDiagnosticsSQL(t *testing.T) {
	require.NoError(t, embed.CloseBaseClusterTests())
	oldEnable := motrace.GetTracerProvider().IsEnable()
	originalReport := motrace.ReportStatement
	records := make(chan []byte, 8)
	hooks := gostub.Stub(&motrace.UseCompactStatementDiagnostics, func() bool { return true })
	hooks.Stub(&motrace.ReportStatement, func(ctx context.Context, s *motrace.StatementInfo) error {
		text := string(s.Statement)
		analysisProbe := strings.Contains(text, "issue23386_analysis_probe")
		if analysisProbe && s.Status == motrace.StatementStatusRunning {
			// Control only admission through the synchronous existing report seam.
			// SQL, prepared execution and analysis publication remain real.
			s.ResponseAt, s.Duration = time.Now(), 5*time.Second
		}
		if analysisProbe && s.Status == motrace.StatementStatusSuccess ||
			(strings.Contains(text, "missing_issue23386_probe") || strings.Contains(text, "issue23386_duplicate.t")) && s.Status == motrace.StatementStatusFailed {
			raw := append([]byte(nil), s.ExecPlan2Json(ctx)...)
			select {
			case records <- raw:
			default:
			}
		}
		return originalReport(ctx, s)
	})
	defer hooks.Reset()
	defer motrace.GetTracerProvider().SetEnable(oldEnable)
	defer func() { require.NoError(t, embed.CloseBaseClusterTests()) }()
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		db := openRetestSQLDB(t, c)
		defer db.Close()
		motrace.GetTracerProvider().SetEnable(true)
		readRecord := func(t *testing.T, skipPreparation bool) ([]byte, *models.StatementDiagnostics) {
			t.Helper()
			for {
				var raw []byte
				// Wire results may precede EndStatement; wait for terminal publication.
				select {
				case raw = <-records:
				case <-ctx.Done():
					t.Fatal("terminal diagnostic not published: ", ctx.Err())
				}
				var plan models.ExplainData
				require.NoError(t, json.Unmarshal(raw, &plan))
				d := plan.StatementDiagnostics
				require.NotNil(t, d)
				if skipPreparation && d.Detail.LogicalTotal == 0 {
					continue
				}
				require.NotNil(t, d.Summary)
				require.LessOrEqual(t, len(raw), models.DiagnosticsL3Budget)
				return raw, d
			}
		}
		checkReader := func(t *testing.T, raw []byte, outcome, capture string) {
			t.Helper()
			for _, mode := range []string{"normal", "verbose", "analyze"} {
				var rendered string
				require.NoError(t, db.QueryRowContext(ctx, "select mo_explain_phy(?,?)", string(raw), mode).Scan(&rendered))
				require.Contains(t, rendered, "Statement diagnostics L")
				require.Contains(t, rendered, outcome)
				if capture != "" {
					require.Contains(t, rendered, "detail="+capture)
				}
				if capture == "analysis_unavailable" {
					require.NotContains(t, rendered, "node[")
					require.NotContains(t, rendered, "instance[")
				}
			}
		}
		checkPrepared := func(t *testing.T, analyzed bool) {
			t.Helper()
			raw, d := readRecord(t, true)
			require.Equal(t, "success", d.Outcome)
			require.Equal(t, 2, d.CapturedLevel)
			capture := "analysis_unavailable"
			if analyzed {
				capture = "complete"
				require.Equal(t, uint64(1), d.Summary.Attempts)
				require.NotEmpty(t, d.Logical)
				for _, n := range d.Logical {
					require.True(t, n.AnalyzeAvailable)
				}
			} else {
				require.Zero(t, d.Summary.Attempts)
				require.Empty(t, d.Logical)
				require.Empty(t, d.Physical)
				require.Equal(t, d.Detail.LogicalTotal, d.Detail.LogicalOmitted)
			}
			require.Equal(t, capture, d.Detail.Capture)
			checkReader(t, raw, "success", capture)
		}
		var v int
		require.NoError(t, db.QueryRowContext(ctx, "select 7 as issue23386_probe").Scan(&v))
		require.Equal(t, 7, v)
		_, err := db.ExecContext(ctx, "create database issue23386_duplicate")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, err := db.ExecContext(cleanupCtx, "drop database issue23386_duplicate")
			require.NoError(t, err)
			var remaining int
			require.NoError(t, db.QueryRowContext(cleanupCtx, "select count(*) from mo_catalog.mo_database where datname='issue23386_duplicate'").Scan(&remaining))
			require.Zero(t, remaining)
		}()
		for _, sql := range []string{"create table issue23386_duplicate.t(id int primary key)", "insert into issue23386_duplicate.t values(1)"} {
			_, err = db.ExecContext(ctx, sql)
			require.NoError(t, err)
		}
		for _, tc := range []struct{ name, sql string }{
			{"compile_failure", "select * from missing_issue23386_probe"},
			{"run_failure", "insert into issue23386_duplicate.t values(1)"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				sql := tc.sql
				_, err := db.ExecContext(ctx, sql)
				require.Error(t, err)
				if strings.HasPrefix(sql, "insert") {
					var wireErr *mysql.MySQLError
					require.ErrorAs(t, err, &wireErr)
					require.Equal(t, uint16(1062), wireErr.Number)
				}
				raw, d := readRecord(t, false)
				require.Equal(t, "failed", d.Outcome)
				require.GreaterOrEqual(t, d.Level, 2)
				capture := ""
				if strings.HasPrefix(sql, "insert") {
					capture = "execution_failed_before_analysis"
					require.Equal(t, "execution_failed_before_analysis", d.Detail.Capture)
					require.Empty(t, d.Logical)
					require.Empty(t, d.Physical)
				}
				checkReader(t, raw, "failed", capture)
			})
		}
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from issue23386_duplicate.t").Scan(&v))
		require.Equal(t, 1, v, "failed insert must leave the original row intact")
		// Live EXPLAIN PHY remains the full physical presentation contract.
		explained, err := db.QueryContext(ctx, "explain phyplan select 7")
		require.NoError(t, err)
		defer explained.Close()
		var lines []string
		for explained.Next() {
			var line string
			require.NoError(t, explained.Scan(&line))
			lines = append(lines, line)
		}
		require.NoError(t, explained.Err())
		require.NoError(t, explained.Close())
		rendered := strings.Join(lines, "\n")
		require.Contains(t, rendered, "Scope")
		require.NotContains(t, rendered, "Statement diagnostics")
		// Exercise the real prepared/reset boundary and check execution results.
		prepared, err := db.PrepareContext(ctx, "select ? as issue23386_analysis_probe")
		require.NoError(t, err)
		defer prepared.Close()
		for _, want := range []int{7, 12} {
			require.NoError(t, prepared.QueryRowContext(ctx, want).Scan(&v))
			require.Equal(t, want, v)
			checkPrepared(t, true)
		}
		require.NoError(t, prepared.Close())
		for _, tc := range []struct {
			name, sql string
			empty     bool
		}{
			{"prepared_zero_rows", "select ? as issue23386_analysis_probe where false", true},
			{"prepared_explain", "explain select ? as issue23386_analysis_probe", false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				stmt, err := db.PrepareContext(ctx, tc.sql)
				require.NoError(t, err)
				defer stmt.Close()
				rows, err := stmt.QueryContext(ctx, 7)
				require.NoError(t, err)
				defer rows.Close()
				if tc.empty {
					require.False(t, rows.Next(), "zero-row SQL must remain empty")
				} else {
					var lines []string
					for rows.Next() {
						var line string
						require.NoError(t, rows.Scan(&line))
						lines = append(lines, line)
					}
					require.Contains(t, strings.Join(lines, "\n"), "Project")
				}
				require.NoError(t, rows.Err())
				require.NoError(t, rows.Close())
				checkPrepared(t, tc.empty)
				require.NoError(t, stmt.Close())
			})
		}
	})
}
