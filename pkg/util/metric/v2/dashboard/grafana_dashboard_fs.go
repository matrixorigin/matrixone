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
)

func (c *DashboardCreator) initFileServiceDashboard() error {
	folder, err := c.createFolder(moFolderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"FileService Metrics",
		c.withRowOptions(
			c.initFSOverviewRow(),
			c.initFSReadWriteDurationRow(),
			c.initFSReadWriteBytesRow(),
			c.initFSS3ConnOverviewRow(),
			c.initFSS3ConnDurationRow(),
			c.initFSIOMergerDurationRow(),
		)...)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initFSOverviewRow() dashboard.Option {
	return dashboard.Row(
		"FileService Overview",
		c.withMultiGraph(
			"S3 Read requests",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_fs_read_total", `type="s3"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_fs_read_total", `type="hit-mem"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_fs_read_total", `type="hit-disk"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_fs_read_total", `type="hit-remote"`) + `[$interval]))`,
			},
			[]string{
				"s3",
				"hit-mem",
				"hit-dist",
				"hit-remote",
			}),

		c.withMultiGraph(
			"S3 Write requests",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter("mo_fs_write_total", `type="s3"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter("mo_fs_write_total", `type="local"`) + `[$interval]))`,
			},
			[]string{
				"s3",
				"local",
			}),
	)
}

func (c *DashboardCreator) initFSReadWriteDurationRow() dashboard.Option {
	return dashboard.Row(
		"FileService read write duration",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_fs_s3_io_duration_seconds_bucket`, `type="read"`),
				c.getMetricWithFilter(`mo_fs_s3_io_duration_seconds_bucket`, `type="write"`),
				c.getMetricWithFilter(`mo_fs_local_io_duration_seconds_bucket`, `type="read"`),
				c.getMetricWithFilter(`mo_fs_local_io_duration_seconds_bucket`, `type="write"`),
				c.getMetricWithFilter(`mo_fs_s3_io_duration_seconds_bucket`, `type="list"`),
				c.getMetricWithFilter(`mo_fs_s3_io_duration_seconds_bucket`, `type="stat"`),
			},
			[]string{
				"s3-read",
				"s3-write",
				"local-read",
				"local-write",
				"s3-list",
				"s3-stat",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
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
				c.getMetricWithFilter(`mo_fs_s3_conn_duration_seconds_bucket`, `type="dns-resolve"`),
				c.getMetricWithFilter(`mo_fs_s3_conn_duration_seconds_bucket`, `type="tls-handshake"`),
			},
			[]string{
				"connect",
				"get-conn",
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
				c.getMetricWithFilter(`mo_fs_io_merger_duration`, `type="wait"`),
				c.getMetricWithFilter(`mo_fs_io_merger_duration`, `type="initiate"`),
			},
			[]string{
				"wait",
				"initiate",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("seconds"),
			axis.Min(0))...,
	)
}
