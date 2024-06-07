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
	"runtime/metrics"
	"strings"

	"github.com/K-Phoen/grabana/axis"
	"github.com/K-Phoen/grabana/dashboard"
	"github.com/K-Phoen/grabana/row"
)

func (c *DashboardCreator) initRuntimeDashboard() error {
	folder, err := c.createFolder(moFolderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"Go Runtime Metrics",
		c.withRowOptions(
			dashboard.Row("Memory", c.getRowOptions("/memory/")...),
			dashboard.Row("GC", c.getRowOptions("/gc/")...),
			dashboard.Row("CPU", c.getRowOptions("/cpu/")...),
			dashboard.Row("Schedule", c.getRowOptions("/sched/")...),
			dashboard.Row("Sync", c.getRowOptions("/sync/")...),
			dashboard.Row("CGO", c.getRowOptions("/cgo/")...),
		)...)
	if err != nil {
		return err
	}

	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) getRowOptions(prefixes ...string) []row.Option {
	var options []row.Option

	// read info from runtime/metrics
	allDescriptions := metrics.All()
	for _, desc := range allDescriptions {
		ok := false
		for _, prefix := range prefixes {
			if strings.HasPrefix(desc.Name, prefix) {
				ok = true
				break
			}
		}
		if !ok {
			continue
		}

		metricName := "go" + desc.Name
		metricName = strings.ReplaceAll(metricName, "/", "_")
		metricName = strings.ReplaceAll(metricName, ":", "_")
		metricName = strings.ReplaceAll(metricName, "-", "_")

		switch desc.Kind {

		case metrics.KindUint64, metrics.KindFloat64:
			// sum
			options = append(
				options,
				c.withGraph(
					desc.Name,
					3,
					`sum(`+c.getMetricWithFilter(metricName, "")+`) by (`+c.by+`)`,
					"{{ "+c.by+" }}"),
			)

			// rate
			if desc.Cumulative {
				_, unit, _ := strings.Cut(desc.Name, ":")
				options = append(
					options,
					c.withGraph(
						"rate: "+desc.Name+" per "+unit,
						3,
						`sum(rate(`+c.getMetricWithFilter(metricName, "")+`[$interval])) by (`+c.by+`)`,
						"{{ "+c.by+" }}",
						axis.Unit(unit),
						axis.Min(0)),
				)
			}

		case metrics.KindFloat64Histogram:
			// histogram
			_, unit, _ := strings.Cut(desc.Name, ":")
			options = append(
				options,
				c.getHistogram(
					desc.Name,
					c.getMetricWithFilter(metricName+"_bucket", ``),
					[]float64{0.8, 0.90, 0.95, 0.99},
					6,
					axis.Unit(unit),
					axis.Min(0)),
			)

		}
	}

	return options
}
