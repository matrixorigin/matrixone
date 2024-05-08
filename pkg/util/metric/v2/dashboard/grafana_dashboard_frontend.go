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

func (c *DashboardCreator) initFrontendDashboard() error {
	folder, err := c.createFolder(moFolderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"Frontend Metrics",
		c.withRowOptions(
			c.initFrontendAcceptConnectionDuration(),
			c.initFrontendRoutineAndRequestCount(),
		)...)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initFrontendAcceptConnectionDuration() dashboard.Option {
	return dashboard.Row(
		"Accept Connection Duration",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_frontend_accept_connection_duration_bucket`, `label="created"`),
				c.getMetricWithFilter(`mo_frontend_accept_connection_duration_bucket`, `label="establish"`),
				c.getMetricWithFilter(`mo_frontend_accept_connection_duration_bucket`, `label="upgradeTLS"`),
				c.getMetricWithFilter(`mo_frontend_accept_connection_duration_bucket`, `label="authenticate"`),
				c.getMetricWithFilter(`mo_frontend_accept_connection_duration_bucket`, `label="check-tenant"`),
				c.getMetricWithFilter(`mo_frontend_accept_connection_duration_bucket`, `label="check-user"`),
				c.getMetricWithFilter(`mo_frontend_accept_connection_duration_bucket`, `label="check-role"`),
				c.getMetricWithFilter(`mo_frontend_accept_connection_duration_bucket`, `label="check-dbname"`),
				c.getMetricWithFilter(`mo_frontend_accept_connection_duration_bucket`, `label="init-global-sys-var"`),
			},
			[]string{
				"created",
				"establish",
				"upgradeTLS",
				"authenticate",
				"check-tenant",
				"check-user",
				"check-role",
				"check-dbname",
				"init-global-sys-var",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initFrontendRoutineAndRequestCount() dashboard.Option {
	return dashboard.Row(
		"Routine and request count",
		c.withMultiGraph(
			"Routine Count",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter(`mo_frontend_routine_count`, `label="created"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter(`mo_frontend_routine_count`, `label="close"`) + `[$interval]))`,
			},
			[]string{
				"created",
				"close",
			}),
		c.withMultiGraph(
			"Request Count",
			6,
			[]string{
				`sum(rate(` + c.getMetricWithFilter(`mo_frontend_request_count`, `label="start-handle"`) + `[$interval]))`,
				`sum(rate(` + c.getMetricWithFilter(`mo_frontend_request_count`, `label="end-handle"`) + `[$interval]))`,
			},
			[]string{
				"start-handle",
				"end-handle",
			}),
	)
}
