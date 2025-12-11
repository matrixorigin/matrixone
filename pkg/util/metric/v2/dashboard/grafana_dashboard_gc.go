// Copyright 2023 Matrix Origin
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
	"fmt"

	"github.com/K-Phoen/grabana/dashboard"
	"github.com/K-Phoen/grabana/timeseries"
)

func (c *DashboardCreator) initGCDashboard() error {
	folder, err := c.createFolder(c.folderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"GC Metrics",
		c.withRowOptions(
			c.initGCExecutionRow(),
			c.initGCFileDeletionRow(),
			c.initGCDurationMainRow(),
			c.initGCDurationMergeRow(),
			c.initGCCheckpointStatsRow(),
			c.initGCTimestampRow(),
		)...)

	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initGCExecutionRow() dashboard.Option {
	return dashboard.Row(
		"GC Execution Metrics",
		c.getTimeSeries(
			"GC Execution Success",
			[]string{fmt.Sprintf(
				"sum(increase(%s[$interval])) by (type)",
				c.getMetricWithFilter(`mo_gc_execution_total`, `status="success"`),
			)},
			[]string{"{{ type }}"},
			timeseries.Span(6),
		),
		c.getTimeSeries(
			"GC Execution Errors",
			[]string{fmt.Sprintf(
				"sum(increase(%s[$interval])) by (type)",
				c.getMetricWithFilter(`mo_gc_execution_total`, `status="error"`),
			)},
			[]string{"{{ type }}"},
			timeseries.Span(6),
		),
	)
}

func (c *DashboardCreator) initGCFileDeletionRow() dashboard.Option {
	return dashboard.Row(
		"GC File Deletion Metrics",
		c.getTimeSeries(
			"Files Deleted",
			[]string{fmt.Sprintf(
				"sum(increase(%s[$interval])) by (type)",
				c.getMetricWithFilter(`mo_gc_file_deletion_total`, ``),
			)},
			[]string{"{{ type }}"},
			timeseries.Span(12),
		),
	)
}

func (c *DashboardCreator) initGCDurationMainRow() dashboard.Option {
	return dashboard.Row(
		"GC Duration Metrics - Main Operations",
		c.getPercentHist(
			"GC Checkpoint Total Duration",
			c.getMetricWithFilter(`mo_gc_duration_seconds_bucket`, `type="checkpoint",phase="total"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(4),
		),
		c.getPercentHist(
			"GC Soft GC Duration (Filter)",
			c.getMetricWithFilter(`mo_gc_duration_seconds_bucket`, `type="checkpoint",phase="filter"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(4),
		),
		c.getPercentHist(
			"GC Snapshot Total Duration",
			c.getMetricWithFilter(`mo_gc_duration_seconds_bucket`, `type="snapshot",phase="total"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(4),
		),
	)
}

func (c *DashboardCreator) initGCDurationMergeRow() dashboard.Option {
	return dashboard.Row(
		"GC Duration Metrics - Merge Operations",
		c.getPercentHist(
			"GC Merge Checkpoint Duration",
			c.getMetricWithFilter(`mo_gc_duration_seconds_bucket`, `type="merge",phase="total"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(6),
		),
		c.getPercentHist(
			"GC Merge Table Duration",
			c.getMetricWithFilter(`mo_gc_duration_seconds_bucket`, `type="merge",phase="table"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(6),
		),
	)
}

func (c *DashboardCreator) initGCCheckpointStatsRow() dashboard.Option {
	return dashboard.Row(
		"GC Checkpoint Statistics",
		c.getTimeSeries(
			"Checkpoints Merged",
			[]string{fmt.Sprintf(
				"sum(increase(%s[$interval]))",
				c.getMetricWithFilter(`mo_gc_checkpoint_total`, `action="merged"`),
			)},
			[]string{"merged"},
			timeseries.Span(4),
		),
		c.getTimeSeries(
			"Checkpoints Deleted",
			[]string{fmt.Sprintf(
				"sum(increase(%s[$interval]))",
				c.getMetricWithFilter(`mo_gc_checkpoint_total`, `action="deleted"`),
			)},
			[]string{"deleted"},
			timeseries.Span(4),
		),
		c.getTimeSeries(
			"Checkpoint Rows Merged",
			[]string{fmt.Sprintf(
				"sum(increase(%s[$interval]))",
				c.getMetricWithFilter(`mo_gc_checkpoint_rows_total`, `type="merged"`),
			)},
			[]string{"rows-merged"},
			timeseries.Span(4),
		),
		c.getTimeSeries(
			"Checkpoint Rows Scanned",
			[]string{fmt.Sprintf(
				"sum(increase(%s[$interval]))",
				c.getMetricWithFilter(`mo_gc_checkpoint_rows_total`, `type="scanned"`),
			)},
			[]string{"rows-scanned"},
			timeseries.Span(4),
		),
	)
}

func (c *DashboardCreator) initGCTimestampRow() dashboard.Option {
	return dashboard.Row(
		"GC Timestamps",
		c.getTimeSeries(
			"GC Last Execution Time",
			[]string{c.getMetricWithFilter(`mo_gc_last_execution_timestamp`, ``)},
			[]string{"{{ type }}"},
			timeseries.Span(6),
		),
		c.getTimeSeries(
			"GC Last Deletion Time",
			[]string{c.getMetricWithFilter(`mo_gc_last_deletion_timestamp`, ``)},
			[]string{"{{ type }}"},
			timeseries.Span(6),
		),
	)
}
