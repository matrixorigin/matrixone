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

	"github.com/K-Phoen/grabana/axis"
	"github.com/K-Phoen/grabana/dashboard"
	"github.com/K-Phoen/grabana/row"
	"github.com/K-Phoen/grabana/timeseries"
	tsaxis "github.com/K-Phoen/grabana/timeseries/axis"
	"github.com/K-Phoen/grabana/timeseries/fields"
)

func (c *DashboardCreator) initFileServiceDashboard() error {
	folder, err := c.createFolder(c.folderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"FileService Metrics",
		c.withRowOptions(
			c.initFSOverviewRow(),
			c.initFSCacheRow(),
			c.initFSObjectStorageRow(),
			c.initFSIOMergerDurationRow(),
			c.initFSReadWriteDurationRow(),
			c.initFSHTTPTraceRow(),
			c.initFSReadWriteBytesRow(),
			c.initFSS3ConnOverviewRow(),
			c.initFSS3ConnDurationRow(),
		)...)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initFSOverviewRow() dashboard.Option {

	cacheHitQuery := func(hitType, readType string) string {
		// example result:
		// sum(rate(mo_fs_read_total{type="hit-disk",instance=~"$instance"}[$interval])) / sum(rate(mo_fs_read_total{type="read-disk",instance=~"$instance"}[$interval]))
		hitMetricFilter := `type="` + hitType + `"`
		readMetricFilter := `type="` + readType + `"`
		return `sum(rate(` + c.getMetricWithFilter("mo_fs_read_total", hitMetricFilter) + `[$interval]))` +
			`/ sum(rate(` + c.getMetricWithFilter("mo_fs_read_total", readMetricFilter) + `[$interval]))`
	}

	return dashboard.Row(
		"FileService Overview",
		c.withMultiGraph(
			"S3 Read requests",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_fs_read_total", `type="s3"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_fs_read_total", `type="local"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_fs_read_total", `type="hit-mem"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_fs_read_total", `type="hit-disk"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_fs_read_total", `type="hit-remote"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_fs_read_total", `type="hit-meta"`) + `[$interval]))`,
			},
			[]string{
				"s3",
				"local",
				"hit-mem",
				"hit-disk",
				"hit-remote",
				"hit-meta",
			}),

		c.withMultiGraph(
			"Cache Hit",
			6,
			[]string{
				cacheHitQuery("hit-mem", "read-mem"),
				cacheHitQuery("hit-disk", "read-disk"),
				cacheHitQuery("hit-meta", "read-meta"),
			},
			[]string{
				"mem",
				"disk",
				"meta",
			},
			UnitPercent01,
		),
	)
}

func (c *DashboardCreator) initFSCacheRow() dashboard.Option {

	cacheUsingPercent := func(componentFilter string) string {
		// example result:
		// sum by(component) (mo_fs_cache_bytes{instance=~"$instance", type="inuse", component=~".*mem"}) / sum by(component) (mo_fs_cache_bytes{instance=~"$instance", type="cap", component=~".*mem"})
		inuseFilter := `type="inuse",` + componentFilter
		capilter := `type="cap",` + componentFilter
		return `sum by(component) (` + c.getMetricWithFilter("mo_fs_cache_bytes", inuseFilter) + `)` +
			` / sum by(component) (` + c.getMetricWithFilter("mo_fs_cache_bytes", capilter) + `)`
	}

	onePanel := func(title, componentFilter string) row.Option {
		return c.withTimeSeries(
			title,
			3,
			[]string{
				`sum by (component) (` + c.getMetricWithFilter("mo_fs_cache_bytes", `type="inuse", `+componentFilter) + `)`,
				`sum by (component) (` + c.getMetricWithFilter("mo_fs_cache_bytes", `type="cap", `+componentFilter) + `)`,
				cacheUsingPercent(componentFilter),
			},
			[]string{
				"{{component}} - inuse",
				"{{component}} - cap",
				"{{component}} - Usage",
			},
			timeseries.Axis(tsaxis.Unit("bytes")),
			/* like:
			"overrides": [
			{
			   "matcher": { "id": "byRegexp", "options": "/.*Usage/" },
			   "properties": [
			     { "id": "custom.axisPlacement", "value": "right" },
			     { "id": "unit", "value": "percentunit" }
			   ]
			 }
			]*/
			timeseries.FieldOverride( // override right-axis
				fields.ByRegex("/.*Usage/"),
				fields.AxisPlacement(tsaxis.Right),
				fields.Unit("percentunit"),
				fields.FillOpacity(0),
				ScaleDistributionLinear(),
			),
		)
	}

	return dashboard.Row(
		"FileService Cache",
		onePanel("Mem", `component=~".*mem"`),
		onePanel("Meta", `component=~".*meta"`),
		onePanel("Disk", `component=~".*disk"`),
	)
}

func (c *DashboardCreator) initFSReadWriteBytesRow() dashboard.Option {
	return dashboard.Row(
		"FileService read write bytes",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_fs_s3_io_bytes_bucket`, `type="read"`),
				c.getMetricWithFilter(`mo_fs_s3_io_bytes_bucket`, `type="write"`),
				c.getMetricWithFilter(`mo_fs_local_io_bytes_bucket`, `type="read"`),
				c.getMetricWithFilter(`mo_fs_local_io_bytes_bucket`, `type="write"`),
			},
			[]string{
				"s3-read",
				"s3-write",
				"local-read",
				"local-write",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("bytes"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initFSS3ConnOverviewRow() dashboard.Option {
	return dashboard.Row(
		"FileService S3 connection overview",
		c.withGraph(
			"Connect",
			6,
			`sum(rate(`+c.getMetricWithFilter("mo_fs_s3_conn_duration_seconds_count", `type="connect"`)+`[$interval]))`,
			""),
		c.withGraph(
			"DNS Resolve",
			6,
			`sum(rate(`+c.getMetricWithFilter("mo_fs_s3_conn_duration_seconds_count", `type="dns-resolve"`)+`[$interval]))`,
			""),
	)
}

func (c *DashboardCreator) initFSS3ConnDurationRow() dashboard.Option {
	return dashboard.Row(
		"FileService s3 connection duration",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_fs_s3_conn_duration_seconds_bucket`, `type="connect"`),
				c.getMetricWithFilter(`mo_fs_s3_conn_duration_seconds_bucket`, `type="get-conn"`),
				c.getMetricWithFilter(`mo_fs_s3_conn_duration_seconds_bucket`, `type="got-first-response"`),
				c.getMetricWithFilter(`mo_fs_s3_conn_duration_seconds_bucket`, `type="dns-resolve"`),
				c.getMetricWithFilter(`mo_fs_s3_conn_duration_seconds_bucket`, `type="tls-handshake"`),
			},
			[]string{
				"connect",
				"get-conn",
				"got-first-response",
				"dns-resolve",
				"tls-handshake",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initFSIOMergerDurationRow() dashboard.Option {
	return dashboard.Row(
		"FileService io merger duration",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_fs_io_merger_duration_seconds_bucket`, `type="wait"`),
				c.getMetricWithFilter(`mo_fs_io_merger_duration_seconds_bucket`, `type="initiate"`),
			},
			[]string{
				"wait",
				"initiate",
			},
			[]float64{0.90, 0.99, 1},
			[]float32{3, 3, 3},
			axis.Unit("seconds"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initFSReadWriteDurationRow() dashboard.Option {
	return dashboard.Row(
		"FileService read write duration",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_fs_read_write_duration_bucket`, `type="read-vector-cache"`),
				c.getMetricWithFilter(`mo_fs_read_write_duration_bucket`, `type="update-vector-cache"`),
				c.getMetricWithFilter(`mo_fs_read_write_duration_bucket`, `type="read-memory-cache"`),
				c.getMetricWithFilter(`mo_fs_read_write_duration_bucket`, `type="update-memory-cache"`),
				c.getMetricWithFilter(`mo_fs_read_write_duration_bucket`, `type="read-disk-cache"`),
				c.getMetricWithFilter(`mo_fs_read_write_duration_bucket`, `type="update-disk-cache"`),
				c.getMetricWithFilter(`mo_fs_read_write_duration_bucket`, `type="read-remote-cache"`),
				c.getMetricWithFilter(`mo_fs_read_write_duration_bucket`, `type="get-reader"`),
				c.getMetricWithFilter(`mo_fs_read_write_duration_bucket`, `type="get-content"`),
				c.getMetricWithFilter(`mo_fs_read_write_duration_bucket`, `type="get-entry-data "`),
				c.getMetricWithFilter(`mo_fs_read_write_duration_bucket`, `type="write-to-writer"`),
				c.getMetricWithFilter(`mo_fs_read_write_duration_bucket`, `type="set-cached-data"`),
				c.getMetricWithFilter(`mo_fs_read_write_duration_bucket`, `type="disk-cache-set-file"`),
				c.getMetricWithFilter(`mo_fs_read_write_duration_bucket`, `type="list"`),
				c.getMetricWithFilter(`mo_fs_read_write_duration_bucket`, `type="stat"`),
				c.getMetricWithFilter(`mo_fs_read_write_duration_bucket`, `type="write"`),
				c.getMetricWithFilter(`mo_fs_read_write_duration_bucket`, `type="io-read-all"`),
			},
			[]string{
				"read-vector-cache",
				"update-vector-cache",
				"read-memory-cache",
				"update-memory-cache",
				"read-disk-cache",
				"update-disk-cache",
				"read-remote-cache",
				"get-reader",
				"get-content",
				"get-entry-data ",
				"write-to-writer",
				"set-cached-data",
				"disk-cache-set-file",
				"list",
				"stat",
				"write",
				"io-read-all",
			},
			[]float64{0.90, 0.99, 1},
			[]float32{3, 3, 3},
			axis.Unit("seconds"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initFSObjectStorageRow() dashboard.Option {
	return dashboard.Row(
		"Object Storage",

		c.withMultiGraph(
			"s3 operations",
			3,
			[]string{
				c.getMetricWithFilter("mo_fs_object_storage_operations", `name="s3",op="read"`),
				c.getMetricWithFilter("mo_fs_object_storage_operations", `name="s3",op="active-read"`),
				c.getMetricWithFilter("mo_fs_object_storage_operations", `name="s3",op="write"`),
				c.getMetricWithFilter("mo_fs_object_storage_operations", `name="s3",op="delete"`),
				c.getMetricWithFilter("mo_fs_object_storage_operations", `name="s3",op="list"`),
				c.getMetricWithFilter("mo_fs_object_storage_operations", `name="s3",op="exists"`),
				c.getMetricWithFilter("mo_fs_object_storage_operations", `name="s3",op="stat"`),
			},
			[]string{
				"read",
				"active-read",
				"write",
				"delete",
				"list",
				"exists",
				"stat",
			},
		),

		c.withMultiGraph(
			"s3 operations rate",
			3,
			[]string{
				`rate(` + c.getMetricWithFilter("mo_fs_object_storage_operations", `name="s3",op="read"`) + `[$interval])`,
				`rate(` + c.getMetricWithFilter("mo_fs_object_storage_operations", `name="s3",op="active-read"`) + `[$interval])`,
				`rate(` + c.getMetricWithFilter("mo_fs_object_storage_operations", `name="s3",op="write"`) + `[$interval])`,
				`rate(` + c.getMetricWithFilter("mo_fs_object_storage_operations", `name="s3",op="delete"`) + `[$interval])`,
				`rate(` + c.getMetricWithFilter("mo_fs_object_storage_operations", `name="s3",op="list"`) + `[$interval])`,
				`rate(` + c.getMetricWithFilter("mo_fs_object_storage_operations", `name="s3",op="exists"`) + `[$interval])`,
				`rate(` + c.getMetricWithFilter("mo_fs_object_storage_operations", `name="s3",op="stat"`) + `[$interval])`,
			},
			[]string{
				"read",
				"active-read",
				"write",
				"delete",
				"list",
				"exists",
				"stat",
			},
		),
	)
}

func (c *DashboardCreator) initFSHTTPTraceRow() dashboard.Option {
	return dashboard.Row(
		"HTTP Trace",

		c.withMultiGraph(
			"trace",
			4,
			[]string{
				`sum(` + c.getMetricWithFilter("mo_fs_http_trace", `op="GetConn"`) + `)`,
				`sum(` + c.getMetricWithFilter("mo_fs_http_trace", `op="GotConn"`) + `)`,
				`sum(` + c.getMetricWithFilter("mo_fs_http_trace", `op="GotConnReused"`) + `)`,
				`sum(` + c.getMetricWithFilter("mo_fs_http_trace", `op="GotConnIdle"`) + `)`,
				`sum(` + c.getMetricWithFilter("mo_fs_http_trace", `op="DNSStart"`) + `)`,
				`sum(` + c.getMetricWithFilter("mo_fs_http_trace", `op="ConnectStart"`) + `)`,
				`sum(` + c.getMetricWithFilter("mo_fs_http_trace", `op="TSLHandshakeStart"`) + `)`,
			},
			[]string{
				"GetConn",
				"GotConn",
				"GotConnReused",
				"GotConnIdle",
				"DNSStart",
				"ConnectStart",
				"TLSHandshakeStart",
			},
		),
	)
}
