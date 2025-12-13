// Copyright 2024 Matrix Origin
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

package dashboard

import (
	"context"

	"github.com/K-Phoen/grabana/axis"
	"github.com/K-Phoen/grabana/dashboard"
	"github.com/K-Phoen/grabana/timeseries"
	tsaxis "github.com/K-Phoen/grabana/timeseries/axis"
	"github.com/K-Phoen/sdk"
)

// floatPtr returns a pointer to the given float64 value
func floatPtr(f float64) *float64 {
	return &f
}

// thresholdStep represents a threshold step in Grafana
type thresholdStep struct {
	Value *float64
	Color string
}

// withThresholds adds threshold configuration to a timeseries panel
func (c *DashboardCreator) withThresholds(mode string, steps []thresholdStep) timeseries.Option {
	return func(ts *timeseries.TimeSeries) error {
		// Initialize Thresholds if not already set
		if ts.Builder.TimeseriesPanel.FieldConfig.Defaults.Thresholds.Mode == "" {
			ts.Builder.TimeseriesPanel.FieldConfig.Defaults.Thresholds.Mode = mode
		}
		ts.Builder.TimeseriesPanel.FieldConfig.Defaults.Thresholds.Steps = make([]sdk.ThresholdStep, 0, len(steps))

		for _, step := range steps {
			thresholdStep := sdk.ThresholdStep{
				Color: step.Color,
				Value: step.Value,
			}
			ts.Builder.TimeseriesPanel.FieldConfig.Defaults.Thresholds.Steps = append(
				ts.Builder.TimeseriesPanel.FieldConfig.Defaults.Thresholds.Steps,
				thresholdStep,
			)
		}
		return nil
	}
}

func (c *DashboardCreator) initCDCDashboard() error {
	folder, err := c.createFolder(c.folderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"CDC Metrics",
		c.withRowOptions(
			c.initCDCTaskRow(),
			c.initCDCWatermarkRow(),
			c.initCDCProcessingRow(),
			c.initCDCTableStreamRow(),
			c.initCDCSinkerRow(),
			c.initCDCHealthRow(),
		)...,
	)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initCDCTaskRow() dashboard.Option {
	return dashboard.Row(
		"CDC Task Overview",
		c.withTimeSeries(
			"Tasks by State",
			6,
			[]string{
				`sum(` + c.getMetricWithFilter("mo_cdc_task_total", "") + `) by (state)`,
			},
			[]string{
				"{{ state }}",
			},
			timeseries.Axis(tsaxis.Unit("short")),
			SpanNulls(true),
		),
		c.withTimeSeries(
			"State Changes /s",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_cdc_task_state_change_total", "") + `[$interval])) by (from_state, to_state)`,
			},
			[]string{
				"{{ from_state }} -> {{ to_state }}",
			},
			timeseries.Axis(tsaxis.Unit("short")),
		),
		c.withTimeSeries(
			"Task Errors /s",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_cdc_task_error_total", "") + `[$interval])) by (error_type)`,
			},
			[]string{
				"{{ error_type }}",
			},
			timeseries.Axis(tsaxis.Unit("short")),
			c.withThresholds("absolute", []thresholdStep{
				{Value: nil, Color: "green"},             // < 0.01/s: normal
				{Value: floatPtr(0.01), Color: "yellow"}, // 0.01-0.1/s: warning
				{Value: floatPtr(0.1), Color: "red"},     // > 0.1/s: critical
			}),
		),
	)
}

func (c *DashboardCreator) initCDCWatermarkRow() dashboard.Option {
	return dashboard.Row(
		"Watermark Health",
		c.withTimeSeries(
			"Top Watermark Lag (s)",
			6,
			[]string{
				`topk(10, ` + c.getMetricWithFilter("mo_cdc_watermark_lag_seconds", "") + `)`,
			},
			[]string{
				"{{ table }}",
			},
			timeseries.Axis(tsaxis.Unit("s")),
			SpanNulls(true),
		),
		c.withTimeSeries(
			"Top Watermark Lag Ratio",
			6,
			[]string{
				`topk(10, ` + c.getMetricWithFilter("mo_cdc_watermark_lag_ratio", "") + `)`,
			},
			[]string{
				"{{ table }}",
			},
			timeseries.Axis(tsaxis.Unit("short")),
			SpanNulls(true),
			c.withThresholds("absolute", []thresholdStep{
				{Value: nil, Color: "green"},            // < 2: normal (lag < 6s)
				{Value: floatPtr(2.0), Color: "yellow"}, // 2-5: warning (lag 6-15s)
				{Value: floatPtr(5.0), Color: "red"},    // > 5: critical (lag > 15s)
			}),
		),
		c.withTimeSeries(
			"Watermark Cache Size",
			6,
			[]string{
				`sum(` + c.getMetricWithFilter("mo_cdc_watermark_cache_size", `tier="uncommitted"`) + `)`,
				`sum(` + c.getMetricWithFilter("mo_cdc_watermark_cache_size", `tier="committing"`) + `)`,
				`sum(` + c.getMetricWithFilter("mo_cdc_watermark_cache_size", `tier="committed"`) + `)`,
			},
			[]string{
				"uncommitted (buffered in memory, not yet persisted to DB)",
				"committing (being persisted to database, transitional state)",
				"committed (persisted to database, synced with mo_cdc_watermark table)",
			},
			timeseries.Axis(tsaxis.Unit("short")),
			SpanNulls(true),
		),
		c.withTimeSeries(
			"Watermark Memory Updates /s",
			3,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_cdc_watermark_update_total", "") + `[$interval])) by (update_type)`,
			},
			[]string{
				"{{ update_type }} (memory cache)",
			},
			timeseries.Axis(tsaxis.Unit("short")),
		),
		c.withTimeSeries(
			"Watermark DB Commits /s",
			3,
			[]string{
				`rate(` + c.getMetricWithFilter("mo_cdc_watermark_commit_batch_total", "") + `[$interval])`,
			},
			[]string{
				"batch commits to database",
			},
			timeseries.Axis(tsaxis.Unit("short")),
		),
		c.getPercentHist(
			"Watermark DB Commit Duration (UPDATE SQL)",
			c.getMetricWithFilter("mo_cdc_watermark_commit_duration_seconds_bucket", ""),
			[]float64{0.50, 0.90, 0.99},
			timeseries.Axis(tsaxis.Unit("s")),
		),
	)
}

func (c *DashboardCreator) initCDCProcessingRow() dashboard.Option {
	return dashboard.Row(
		"Data Processing",
		c.withTimeSeries(
			"Rows Processed /s",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_cdc_rows_processed_total", "") + `[$interval])) by (operation)`,
			},
			[]string{
				"{{ operation }}",
			},
			timeseries.Axis(tsaxis.Unit("short")),
		),
		c.withTimeSeries(
			"Bytes Processed /s",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_cdc_bytes_processed_total", "") + `[$interval])) by (operation)`,
			},
			[]string{
				"{{ operation }}",
			},
			timeseries.Axis(tsaxis.Unit("bytes")),
		),
		c.getHistogramWithExtraBy(
			"Batch Size (rows)",
			c.getMetricWithFilter("mo_cdc_batch_size_rows_bucket", ""),
			[]float64{0.50, 0.90, 0.99},
			6,
			"type",
			axis.Unit("short"),
			axis.Min(0),
		),
		c.withTimeSeries(
			"Throughput (rows/s)",
			6,
			[]string{
				// Use Counter-based calculation for more accurate and continuous throughput
				// This shows the actual processing rate, not just the last round's throughput
				`sum(rate(` + c.getMetricWithFilter("mo_cdc_rows_processed_total", `operation="insert"`) + `[$interval])) by (table)`,
			},
			[]string{
				"{{ table }}",
			},
			timeseries.Axis(tsaxis.Unit("short")),
		),
		c.getHistogramWithExtraBy(
			"End-to-end Latency (s)",
			c.getMetricWithFilter("mo_cdc_latency_seconds_bucket", ""),
			[]float64{0.50, 0.90, 0.99},
			6,
			"table",
			axis.Unit("s"),
			axis.Min(0),
		),
	)
}

func (c *DashboardCreator) initCDCTableStreamRow() dashboard.Option {
	return dashboard.Row(
		"Table Stream Execution",
		c.withTimeSeries(
			"Streams by State",
			6,
			[]string{
				`sum(` + c.getMetricWithFilter("mo_cdc_table_stream_total", "") + `) by (state)`,
			},
			[]string{
				"{{ state }}",
			},
			timeseries.Axis(tsaxis.Unit("short")),
		),
		c.withTimeSeries(
			"Round Success /s",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_cdc_table_stream_round_total", `status="success"`) + `[$interval])) by (table)`,
			},
			[]string{
				"{{ table }}",
			},
			timeseries.Axis(tsaxis.Unit("short")),
		),
		c.withTimeSeries(
			"Round Failures /s",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_cdc_table_stream_round_total", `status!="success"`) + `[$interval])) by (table, status)`,
			},
			[]string{
				"{{ table }} ({{ status }})",
			},
			timeseries.Axis(tsaxis.Unit("short")),
		),
		c.getHistogramWithExtraBy(
			"Round Duration (s)",
			c.getMetricWithFilter("mo_cdc_table_stream_round_duration_seconds_bucket", ""),
			[]float64{0.50, 0.90, 0.99},
			6,
			"table",
			axis.Unit("s"),
			axis.Min(0),
		),
		c.withTimeSeries(
			"Retry Attempts /s",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_cdc_table_stream_retry_total", `outcome="attempted"`) + `[$interval])) by (table, error_type)`,
			},
			[]string{
				"{{ table }} ({{ error_type }})",
			},
			timeseries.Axis(tsaxis.Unit("short")),
			c.withThresholds("absolute", []thresholdStep{
				{Value: nil, Color: "green"},            // < 1/s: normal
				{Value: floatPtr(1.0), Color: "yellow"}, // 1-5/s: warning
				{Value: floatPtr(5.0), Color: "red"},    // > 5/s: critical
			}),
		),
		c.withTimeSeries(
			"Retry Outcomes /s",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_cdc_table_stream_retry_total", `outcome!="attempted"`) + `[$interval])) by (table, error_type, outcome)`,
			},
			[]string{
				"{{ table }} ({{ error_type }}, {{ outcome }})",
			},
			timeseries.Axis(tsaxis.Unit("short")),
		),
		c.getHistogramWithExtraBy(
			"Retry Backoff Delay (s)",
			c.getMetricWithFilter("mo_cdc_table_stream_retry_delay_seconds_bucket", ""),
			[]float64{0.50, 0.90, 0.99},
			6,
			"error_type",
			axis.Unit("s"),
			axis.Min(0),
		),
		c.withTimeSeries(
			"Auxiliary Errors /s",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_cdc_table_stream_auxiliary_error_total", "") + `[$interval])) by (table, auxiliary_error_type)`,
			},
			[]string{
				"{{ table }} ({{ auxiliary_error_type }})",
			},
			timeseries.Axis(tsaxis.Unit("short")),
		),
		c.withTimeSeries(
			"Original Error Preserved /s",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_cdc_table_stream_original_error_preserved_total", "") + `[$interval])) by (table, original_error_type)`,
			},
			[]string{
				"{{ table }} ({{ original_error_type }})",
			},
			timeseries.Axis(tsaxis.Unit("short")),
		),
	)
}

func (c *DashboardCreator) initCDCSinkerRow() dashboard.Option {
	return dashboard.Row(
		"Sinker Performance",
		c.withTimeSeries(
			"Transaction Rate /s",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_cdc_sinker_transaction_total", "") + `[$interval])) by (operation, status)`,
			},
			[]string{
				"{{ operation }} ({{ status }})",
			},
			timeseries.Axis(tsaxis.Unit("short")),
		),
		c.withTimeSeries(
			"SQL Executions /s",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_cdc_sinker_sql_total", "") + `[$interval])) by (sql_type, status)`,
			},
			[]string{
				"{{ sql_type }} ({{ status }})",
			},
			timeseries.Axis(tsaxis.Unit("short")),
		),
		c.getHistogramWithExtraBy(
			"SQL Duration (s)",
			c.getMetricWithFilter("mo_cdc_sinker_sql_duration_seconds_bucket", ""),
			[]float64{0.50, 0.90, 0.99},
			6,
			"sql_type",
			axis.Unit("s"),
			axis.Min(0),
		),
		c.withTimeSeries(
			"Retry Rate /s",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_cdc_sinker_retry_total", "") + `[$interval])) by (reason, result)`,
			},
			[]string{
				"{{ reason }} ({{ result }})",
			},
			timeseries.Axis(tsaxis.Unit("short")),
		),
	)
}

func (c *DashboardCreator) initCDCHealthRow() dashboard.Option {
	return dashboard.Row(
		"Heartbeat & Table Health",
		c.withTimeSeries(
			"Heartbeat Rate /s",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_cdc_heartbeat_total", "") + `[$interval])) by (table)`,
			},
			[]string{
				"{{ table }}",
			},
			timeseries.Axis(tsaxis.Unit("short")),
		),
		c.withTimeSeries(
			"Stuck Tables",
			6,
			[]string{
				`max(` + c.getMetricWithFilter("mo_cdc_table_stuck", "") + `) by (table)`,
			},
			[]string{
				"{{ table }}",
			},
			timeseries.Axis(tsaxis.Unit("short")),
		),
		c.withTimeSeries(
			"Last Activity (seconds ago)",
			6,
			[]string{
				`(time() - max(` + c.getMetricWithFilter("mo_cdc_table_last_activity_timestamp", "") + `) by (table))`,
			},
			[]string{
				"{{ table }}",
			},
			timeseries.Axis(tsaxis.Unit("s")),
		),
	)
}
