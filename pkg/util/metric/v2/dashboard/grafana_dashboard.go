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
	"github.com/K-Phoen/grabana/graph"
	"github.com/K-Phoen/grabana/row"
	"github.com/K-Phoen/grabana/target/prometheus"
)

var (
	txnFolderName     = "Txn"
	logtailFolderName = "LogTail"
	fsFolderName      = "FileService"
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

	return c.initFileServiceDashboard()
}

func (c *DashboardCreator) createFolder(name string) (*grabana.Folder, error) {
	folder, err := c.cli.GetFolderByTitle(context.Background(), name)
	if err != nil && err != grabana.ErrFolderNotFound {
		return nil, err
	}

	if folder == nil {
		folder, err = c.cli.CreateFolder(context.Background(), name)
		if err != nil {
			return nil, err
		}
	}
	return folder, nil
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
			fmt.Sprintf("histogram_quantile(%f, sum(rate(%s[$interval])) by (le, instance))", percent, metric),
			"{{ instance }}",
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
			fmt.Sprintf("histogram_quantile(%f, sum(rate(%s[$interval])) by (le, instance))", percent, metric),
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)))
	}
	return options
}
