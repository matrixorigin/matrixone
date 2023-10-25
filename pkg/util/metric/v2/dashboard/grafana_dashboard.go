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
	"net/http"

	"github.com/K-Phoen/grabana"
	"github.com/K-Phoen/grabana/axis"
	"github.com/K-Phoen/grabana/dashboard"
	"github.com/K-Phoen/grabana/graph"
	"github.com/K-Phoen/grabana/row"
	"github.com/K-Phoen/grabana/target/prometheus"
	"github.com/K-Phoen/grabana/variable/interval"
	"github.com/K-Phoen/grabana/variable/query"
)

var (
	txnFolderName     = "Txn"
	logtailFolderName = "LogTail"
	fsFolderName      = "FileService"
	taskFolderName    = "Tasks"
)

type DashboardCreator struct {
	cli        *grabana.Client
	dataSource string
}

func NewDashboardCreator(
	host,
	username,
	password,
	dataSource string) *DashboardCreator {
	return &DashboardCreator{
		cli:        grabana.NewClient(http.DefaultClient, host, grabana.WithBasicAuth(username, password)),
		dataSource: dataSource,
	}
}

func (c *DashboardCreator) Create() error {
	if err := c.initTxnDashboard(); err != nil {
		return err
	}

	if err := c.initLogTailDashboard(); err != nil {
		return err
	}

	if err := c.initTaskDashboard(); err != nil {
		return err
	}

	return c.initFileServiceDashboard()
}

func (c *DashboardCreator) createFolder(name string) (*grabana.Folder, error) {
	return c.cli.FindOrCreateFolder(context.Background(), name)
}

func (c *DashboardCreator) withGraph(
	title string,
	span float32,
	pql string,
	legend string,
	opts ...axis.Option) row.Option {
	return row.WithGraph(
		title,
		graph.Span(span),
		graph.Height("400px"),
		graph.DataSource(c.dataSource),
		graph.WithPrometheusTarget(
			pql,
			prometheus.Legend(legend),
		),
		graph.LeftYAxis(opts...),
	)
}

func (c *DashboardCreator) getHistogram(
	metric string,
	percents []float64,
	columns []float32) []row.Option {
	var options []row.Option

	for i := 0; i < len(percents); i++ {
		percent := percents[i]
		options = append(options, c.withGraph(
			fmt.Sprintf("P%f time", percent*100),
			columns[i],
			fmt.Sprintf("histogram_quantile(%f, sum(rate(%s[$interval])) by (le, pod))", percent, metric),
			"{{ pod }}",
			axis.Unit("s"),
			axis.Min(0)))
	}
	return options
}

func (c *DashboardCreator) getBytesHistogram(
	metric string,
	percents []float64,
	columns []float32) []row.Option {
	var options []row.Option

	for i := 0; i < len(percents); i++ {
		percent := percents[i]
		options = append(options, c.withGraph(
			fmt.Sprintf("P%f time", percent*100),
			columns[i],
			fmt.Sprintf("histogram_quantile(%f, sum(rate(%s[$interval])) by (le, pod))", percent, metric),
			"{{ pod }}",
			axis.Unit("bytes"),
			axis.Min(0)))
	}
	return options
}

func (c *DashboardCreator) withRowOptions(rows ...dashboard.Option) []dashboard.Option {
	return append(rows,
		dashboard.AutoRefresh("5s"),
		dashboard.VariableAsInterval(
			"interval",
			interval.Default("1m"),
			interval.Values([]string{"1m", "5m", "10m", "30m", "1h", "6h", "12h"}),
		),
		dashboard.VariableAsQuery(
			"physicalCluster",
			query.DataSource(c.dataSource),
			query.DefaultAll(),
			query.IncludeAll(),
			query.Multiple(),
			query.Label("matrixone_cloud_main_cluster"),
			query.Request("label_values(matrixone_cloud_main_cluster)"),
		),
		dashboard.VariableAsQuery(
			"cluster",
			query.DataSource(c.dataSource),
			query.DefaultAll(),
			query.IncludeAll(),
			query.Multiple(),
			query.Label("matrixone_cloud_cluster"),
			query.Request("label_values(matrixone_cloud_cluster)"),
		),
		dashboard.VariableAsQuery(
			"pod",
			query.DataSource(c.dataSource),
			query.DefaultAll(),
			query.IncludeAll(),
			query.Multiple(),
			query.Label("pod"),
			query.Request("label_values(pod)"),
		))
}
