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

	"github.com/K-Phoen/grabana/axis"
	"github.com/K-Phoen/grabana/dashboard"
	"github.com/K-Phoen/grabana/row"
	"github.com/K-Phoen/grabana/timeseries"
	tsaxis "github.com/K-Phoen/grabana/timeseries/axis"
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
			c.initGCDurationRow(),
			c.initGCAlertsRow(),
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
			timeseries.Span(6),
		),
		c.getTimeSeries(
			"File Size Deleted",
			[]string{fmt.Sprintf(
				"sum(increase(%s[$interval])) by (type)",
				c.getMetricWithFilter(`mo_gc_file_size_bytes_sum`, ``),
			)},
			[]string{"{{ type }}"},
			timeseries.Span(6),
			axis.Unit("decbytes"),
		),
	)
}

func (c *DashboardCreator) initGCDurationRow() dashboard.Option {
	return dashboard.Row(
		"GC Duration Metrics",
		c.getPercentHist(
			"GC Checkpoint Total Duration",
			c.getMetricWithFilter(`mo_gc_duration_bucket`, `type="checkpoint",phase="total"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(4),
		),
		c.getPercentHist(
			"GC Merge Total Duration",
			c.getMetricWithFilter(`mo_gc_duration_bucket`, `type="merge",phase="total"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(4),
		),
		c.getPercentHist(
			"GC Snapshot Total Duration",
			c.getMetricWithFilter(`mo_gc_duration_bucket`, `type="snapshot",phase="total"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			SpanNulls(true),
			timeseries.Span(4),
		),
	)
}

func (c *DashboardCreator) initGCAlertsRow() dashboard.Option {
	return dashboard.Row(
		"GC Alerts",
		c.getTimeSeries(
			"GC Last Execution Time",
			[]string{fmt.Sprintf(
				"%s",
				c.getMetricWithFilter(`mo_gc_last_execution_timestamp`, ``),
			)},
			[]string{"{{ type }}"},
			timeseries.Span(4),
		),
		c.getTimeSeries(
			"GC Last Deletion Time",
			[]string{fmt.Sprintf(
				"%s",
				c.getMetricWithFilter(`mo_gc_last_deletion_timestamp`, ``),
			)},
			[]string{"{{ type }}"},
			timeseries.Span(4),
		),
		c.getTimeSeries(
			"GC Alerts",
			[]string{fmt.Sprintf(
				"%s",
				c.getMetricWithFilter(`mo_gc_alert`, ``),
			)},
			[]string{"{{ type }}"},
			timeseries.Span(4),
		),
	)
}
