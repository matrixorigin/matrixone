// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
package motrace

import (
	"context"
	"encoding/json"
	"github.com/matrixorigin/matrixone/pkg/config"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestTerminalDiagnosticsEarlyErrorAndAggregation(t *testing.T) {
	old := GetTracerProvider()
	SetTracerProvider(newMOTracerProvider(EnableTracer(true), WithStatementDiagnosticsFormat("compact-v1"), WithLongQueryTime(1), WithSelectThreshold(time.Second)))
	defer SetTracerProvider(old)
	reports := 0
	defer gostub.Stub(&ReportStatement, func(context.Context, *StatementInfo) error { reports++; return nil }).Reset()
	for _, tc := range []struct {
		name   string
		err    error
		delta  resource.Delta
		retain bool
	}{{name: "ordinary"}, {name: "parse cancellation", err: context.Canceled, retain: true}, {name: "terminal IO", delta: resource.Delta{Usage: resource.Usage{S3ReadBytes: 1 << 30}}, retain: true}} {
		t.Run(tc.name, func(t *testing.T) {
			s := &StatementInfo{ResponseAt: time.Now(), Duration: time.Millisecond, StatementType: "Select", SqlSourceType: "external_sql"}
			root := resource.NewRoot(resource.ConnExternal)
			root.AddLocal(tc.delta)
			s.SetResourceRoot(root)
			s.EndStatement(context.Background(), tc.err, 1, 10, 1)
			require.Equal(t, tc.retain, s.disableAgg)
			require.Equal(t, !tc.retain, StatementInfoFilter(s))
			if tc.retain {
				var payload models.ExplainData
				require.NoError(t, json.Unmarshal(s.ExecPlan2Json(context.Background()), &payload))
				d := payload.StatementDiagnostics
				require.NotNil(t, d)
				require.Equal(t, models.DiagnosticOutcome(tc.err), d.Outcome)
				require.Equal(t, tc.delta.Usage.S3ReadBytes, d.Summary.S3ReadBytes)
				require.False(t, d.Summary.WaitActiveKnown)
				require.Zero(t, d.CapturedLevel)
			} else {
				require.Equal(t, "{}", string(s.ExecPlan2Json(context.Background())))
			}
			before := reports
			s.EndStatement(context.Background(), tc.err, 1, 10, 1)
			require.Equal(t, before, reports)
			s.FreeExecPlan()
		})
	}
	require.Equal(t, 3, reports)
}

func TestStatementDiagnosticsFormatConfiguration(t *testing.T) {
	cfg := config.NewObservabilityParameters()
	require.Equal(t, "compact-v1", cfg.StatementDiagnosticsFormat)
	cfg.StatementDiagnosticsFormat = "legacy"
	cfg.SetDefaultValues("test")
	require.Equal(t, "legacy", cfg.StatementDiagnosticsFormat)
	require.False(t, newMOTracerProvider(WithStatementDiagnosticsFormat(cfg.StatementDiagnosticsFormat)).compactStatementDiagnostics)
	require.True(t, newMOTracerProvider(WithStatementDiagnosticsFormat("compact-v1")).compactStatementDiagnostics)
	cfg.StatementDiagnosticsFormat = "misspelled"
	before := GetTracerProvider()
	err, started := InitWithConfig(context.Background(), cfg)
	require.Error(t, err)
	require.False(t, started)
	require.Same(t, before, GetTracerProvider())
}
