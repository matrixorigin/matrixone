// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
package dml

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

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
	failedRecord := make(chan []byte, 1)
	hooks := gostub.Stub(&motrace.UseCompactStatementDiagnostics, func() bool { return true })
	hooks.Stub(&motrace.ReportStatement, func(ctx context.Context, s *motrace.StatementInfo) error {
		text := string(s.Statement)
		if strings.Contains(text, "missing_issue23386_probe") && s.Status == motrace.StatementStatusFailed {
			raw := append([]byte(nil), s.ExecPlan2Json(ctx)...)
			select {
			case failedRecord <- raw:
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
		var v int
		require.NoError(t, db.QueryRowContext(ctx, "select 7 as issue23386_probe").Scan(&v))
		require.Equal(t, 7, v)
		_, err := db.ExecContext(ctx, "select * from missing_issue23386_probe")
		require.Error(t, err)
		var raw []byte
		// The wire ERR packet can reach the client before EndStatement runs.
		// Synchronize with the terminal producer, rather than assume timing.
		select {
		case raw = <-failedRecord:
		case <-ctx.Done():
			t.Fatal("terminal diagnostic not published: ", ctx.Err())
		}
		require.NotEmpty(t, raw, "failed wire SQL must reach terminal diagnostics")
		var plan models.ExplainData
		require.NoError(t, json.Unmarshal(raw, &plan))
		d := plan.StatementDiagnostics
		require.NotNil(t, d)
		require.Equal(t, "failed", d.Outcome)
		require.GreaterOrEqual(t, d.Level, 2)
		require.NotNil(t, d.Summary)
		require.LessOrEqual(t, len(raw), models.DiagnosticsL3Budget)
		for _, mode := range []string{"normal", "verbose", "analyze"} {
			var rendered string
			require.NoError(t, db.QueryRowContext(ctx, "select mo_explain_phy(?,?)", string(raw), mode).Scan(&rendered))
			require.Contains(t, rendered, "Statement diagnostics L")
			require.Contains(t, rendered, "failed")
		}
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
		prepared, err := db.PrepareContext(ctx, "select ? as issue23386_probe")
		require.NoError(t, err)
		defer prepared.Close()
		for _, want := range []int{11, 12} {
			require.NoError(t, prepared.QueryRowContext(ctx, want).Scan(&v))
			require.Equal(t, want, v)
		}
	})
}
