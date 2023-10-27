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
			c.initFSReadOverviewRow(),
			c.initFSWriteOverviewRow(),
			c.initFSS3ReadDurationRow(),
			c.initFSS3WriteDurationRow(),
			c.initFSLocalReadDurationRow(),
			c.initFSLocalWriteDurationRow(),
			c.initFSS3ReadBytesRow(),
			c.initFSS3WriteBytesRow(),
			c.initFSLocalReadBytesRow(),
			c.initFSLocalWriteBytesRow(),
			c.initFSS3ConnectRequestsRow(),
			c.initFSS3ConnectRow(),
			c.initFSS3GetConnRow(),
			c.initFSResolveS3DNSRow(),
			c.initFSS3TLSHandshakeRow(),
		)...)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initFSReadOverviewRow() dashboard.Option {
	return dashboard.Row(
		"FileService read overview",
		c.withGraph(
			"S3 Read requests",
			3,
			`sum(rate(`+c.getMetricWithFilter("mo_fs_read_total", `type="s3"`)+`[$interval])) by (`+c.by+`)`,
			"{{ "+c.by+" }}"),

		c.withGraph(
			"Mem Read requests",
			3,
			`sum(rate(`+c.getMetricWithFilter("mo_fs_read_total", `type="hit-mem"`)+`[$interval])) by (`+c.by+`)`,
			"{{ "+c.by+" }}"),

		c.withGraph(
			"Disk Read requests",
			3,
			`sum(rate(`+c.getMetricWithFilter("mo_fs_read_total", `type="hit-disk"`)+`[$interval])) by (`+c.by+`)`,
			"{{ "+c.by+" }}"),

		c.withGraph(
			"Remote Read requests",
			3,
			`sum(rate(`+c.getMetricWithFilter("mo_fs_read_total", `type="hit-remote"`)+`[$interval])) by (`+c.by+`)`,
			"{{ "+c.by+" }}"),
	)
}

func (c *DashboardCreator) initFSWriteOverviewRow() dashboard.Option {
	return dashboard.Row(
		"FileService write overview",
		c.withGraph(
			"S3 Write requests",
			6,
			`sum(rate(`+c.getMetricWithFilter("mo_fs_write_total", `type="s3"`)+`[$interval])) by (`+c.by+`)`,
			"{{ `+c.by+` }"),

		c.withGraph(
			"Local Write requests",
			6,
			`sum(rate(`+c.getMetricWithFilter("mo_fs_write_total", `type="local"`)+`[$interval])) by (`+c.by+`)`,
			"{{ "+c.by+" }}"),
	)
}

func (c *DashboardCreator) initFSS3ReadDurationRow() dashboard.Option {
	return dashboard.Row(
		"FileService S3 read duration",
		c.getHistogram(
			c.getMetricWithFilter(`mo_fs_s3_io_duration_seconds_bucket`, `type="read"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initFSS3WriteDurationRow() dashboard.Option {
	return dashboard.Row(
		"FileService S3 read duration",
		c.getHistogram(
			c.getMetricWithFilter(`mo_fs_s3_io_duration_seconds_bucket`, `type="write"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initFSLocalReadDurationRow() dashboard.Option {
	return dashboard.Row(
		"FileService local read duration",
		c.getHistogram(
			c.getMetricWithFilter(`mo_fs_local_io_duration_seconds_bucket`, `type="read"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initFSLocalWriteDurationRow() dashboard.Option {
	return dashboard.Row(
		"FileService local read duration",
		c.getHistogram(
			c.getMetricWithFilter(`mo_fs_local_io_duration_seconds_bucket`, `type="write"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initFSS3ReadBytesRow() dashboard.Option {
	return dashboard.Row(
		"FileService S3 read size",
		c.getBytesHistogram(
			c.getMetricWithFilter(`mo_fs_s3_io_bytes_bucket`, `type="read"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initFSS3WriteBytesRow() dashboard.Option {
	return dashboard.Row(
		"FileService S3 write size",
		c.getBytesHistogram(
			c.getMetricWithFilter(`mo_fs_s3_io_bytes_bucket`, `type="write"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initFSLocalReadBytesRow() dashboard.Option {
	return dashboard.Row(
		"FileService local read size",
		c.getBytesHistogram(
			c.getMetricWithFilter(`mo_fs_local_io_bytes_bucket`, `type="read"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initFSLocalWriteBytesRow() dashboard.Option {
	return dashboard.Row(
		"FileService local write size",
		c.getBytesHistogram(
			c.getMetricWithFilter(`mo_fs_local_io_bytes_bucket`, `type="write"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initFSS3ConnectRequestsRow() dashboard.Option {
	return dashboard.Row(
		"FileService S3 connection status",
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

func (c *DashboardCreator) initFSS3ConnectRow() dashboard.Option {
	return dashboard.Row(
		"FileService connect to S3",
		c.getHistogram(
			c.getMetricWithFilter(`mo_fs_s3_conn_duration_seconds_bucket`, `type="connect"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initFSS3GetConnRow() dashboard.Option {
	return dashboard.Row(
		"FileService get S3 connection",
		c.getHistogram(
			c.getMetricWithFilter(`mo_fs_s3_conn_duration_seconds_bucket`, `type="get-conn"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initFSResolveS3DNSRow() dashboard.Option {
	return dashboard.Row(
		"FileService resolve S3 dns",
		c.getHistogram(
			c.getMetricWithFilter(`mo_fs_s3_conn_duration_seconds_bucket`, `type="dns-resolve"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initFSS3TLSHandshakeRow() dashboard.Option {
	return dashboard.Row(
		"FileService S3 connection tls handshake",
		c.getHistogram(
			c.getMetricWithFilter(`mo_fs_s3_conn_duration_seconds_bucket`, `type="tls-handshake"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}
