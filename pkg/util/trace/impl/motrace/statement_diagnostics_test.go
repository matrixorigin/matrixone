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

package motrace

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
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

// Exercise the production aggregation and export path, rather than only its predicate.
func TestStatementDiagnosticsAggregationExport(t *testing.T) {
	old := GetTracerProvider()
	t.Cleanup(func() { SetTracerProvider(old) })
	stub := gostub.Stub(&ReportStatement, func(context.Context, *StatementInfo) error { return nil })
	t.Cleanup(stub.Reset)
	ctx := context.Background()
	fixed := time.Date(2026, time.October, 10, 12, 0, 1, 0, time.UTC)
	failure := errors.New("missing table")
	for _, tc := range []struct {
		name, format string
		err          error
		retain       bool
	}{
		{name: "compact success", format: "compact-v1"},
		{name: "legacy failure", format: "legacy", err: failure},
		{name: "compact failure", format: "compact-v1", err: failure, retain: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			SetTracerProvider(newMOTracerProvider(EnableTracer(true), WithStatementDiagnosticsFormat(tc.format), WithLongQueryTime(1), WithSelectThreshold(time.Second)))
			aggr := NewAggregator(ctx, 5*time.Second, StatementInfoNew, StatementInfoUpdate, StatementInfoFilter)
			t.Cleanup(aggr.Close)
			row := SingleStatementTable.GetRow(ctx)
			t.Cleanup(row.Free)
			var output []table.Item
			for i := 0; i < 2; i++ {
				s := &StatementInfo{Account: "test", User: "admin", Statement: []byte("select 1"), RequestAt: fixed.Add(-time.Millisecond), ResponseAt: fixed, Duration: time.Millisecond, StatementType: "Select", SqlSourceType: "cloud_nonuser_sql"}
				t.Cleanup(s.Free)
				s.EndStatement(ctx, tc.err, 0, 0, 0)
				returned, err := aggr.AddItem(s)
				if tc.retain {
					require.ErrorIs(t, err, ErrFilteredOut)
					require.Same(t, s, returned)
					output = append(output, returned)
				} else {
					require.NoError(t, err)
					require.Nil(t, returned)
				}
			}
			grouped := aggr.GetResults()
			if tc.retain {
				require.Empty(t, grouped)
				require.Len(t, output, 2)
			} else {
				require.Len(t, grouped, 1)
				output = grouped
			}
			for _, item := range output {
				item.(*StatementInfo).FillRow(ctx, row)
				values := statementCapacityValues(row.ToStrings())
				if tc.err != nil {
					require.Equal(t, "Failed", values["status"])
					require.Equal(t, failure.Error(), values["error"])
				} else {
					require.Equal(t, "Success", values["status"])
				}
				if tc.retain {
					require.Equal(t, "0", values["aggr_count"])
					var payload models.ExplainData
					require.NoError(t, json.Unmarshal([]byte(values["exec_plan"]), &payload))
					require.NotNil(t, payload.StatementDiagnostics)
					require.Equal(t, 2, payload.StatementDiagnostics.Level)
					require.Equal(t, "failed", payload.StatementDiagnostics.Outcome)
				} else {
					require.Equal(t, "2", values["aggr_count"])
					require.Equal(t, "{}", values["exec_plan"])
				}
			}
		})
	}
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
