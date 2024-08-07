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
			c.initFrontendResolveDuration(),
			c.initFrontendCreateAccount(),
			c.initFrontendPubSubDuration(),
			c.initFrontendSQLLength(),
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

func (c *DashboardCreator) initFrontendResolveDuration() dashboard.Option {
	return dashboard.Row(
		"Resolve Duration",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_frontend_resolve_duration_bucket`, `label="total-resolve"`),
				c.getMetricWithFilter(`mo_frontend_resolve_duration_bucket`, `label="ensure-database"`),
				c.getMetricWithFilter(`mo_frontend_resolve_duration_bucket`, `label="get-sub-meta"`),
				c.getMetricWithFilter(`mo_frontend_resolve_duration_bucket`, `label="check-sub-valid"`),
				c.getMetricWithFilter(`mo_frontend_resolve_duration_bucket`, `label="get-relation"`),
				c.getMetricWithFilter(`mo_frontend_resolve_duration_bucket`, `label="open-db"`),
				c.getMetricWithFilter(`mo_frontend_resolve_duration_bucket`, `label="open-table"`),
				c.getMetricWithFilter(`mo_frontend_resolve_duration_bucket`, `label="get-tmp-table"`),
			},
			[]string{
				"total-resolve",
				"ensure-database",
				"get-sub-meta",
				"check-sub-valid",
				"get-relation",
				"open-db",
				"open-table",
				"get-tmp-table",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initFrontendCreateAccount() dashboard.Option {
	return dashboard.Row(
		"Create account Duration",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_frontend_create_account_duration_bucket`, `label="total-create"`),
				c.getMetricWithFilter(`mo_frontend_create_account_duration_bucket`, `label="step1"`),
				c.getMetricWithFilter(`mo_frontend_create_account_duration_bucket`, `label="step2"`),
				c.getMetricWithFilter(`mo_frontend_create_account_duration_bucket`, `label="create-tables-in-mo-catalog"`),
				c.getMetricWithFilter(`mo_frontend_create_account_duration_bucket`, `label="exec-ddl1"`),
				c.getMetricWithFilter(`mo_frontend_create_account_duration_bucket`, `label="init-data1"`),
				c.getMetricWithFilter(`mo_frontend_create_account_duration_bucket`, `label="create-tables-in-system"`),
				c.getMetricWithFilter(`mo_frontend_create_account_duration_bucket`, `label="create-tables-in-info-schema"`),
			},
			[]string{
				"total-create",
				"step1",
				"step2",
				"create-tables-in-mo-catalog",
				"exec-ddl1",
				"init-data1",
				"create-tables-in-system",
				"create-tables-in-info-schema",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initFrontendPubSubDuration() dashboard.Option {
	return dashboard.Row(
		"Create account Duration",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_frontend_pub_sub_duration_bucket`, `label="create-pub"`),
				c.getMetricWithFilter(`mo_frontend_pub_sub_duration_bucket`, `label="alter-pub"`),
				c.getMetricWithFilter(`mo_frontend_pub_sub_duration_bucket`, `label="drop-pub"`),
				c.getMetricWithFilter(`mo_frontend_pub_sub_duration_bucket`, `label="show-pub"`),
				c.getMetricWithFilter(`mo_frontend_pub_sub_duration_bucket`, `label="show-sub"`),
			},
			[]string{
				"create-pub",
				"alter-pub",
				"drop-pub",
				"show-pub",
				"show-sub",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
	)
}

func (c *DashboardCreator) initFrontendSQLLength() dashboard.Option {
	return dashboard.Row(
		"Input SQL Length",
		c.getMultiHistogram(
			[]string{
				c.getMetricWithFilter(`mo_frontend_sql_length_bucket`, `label="total-sql-length"`),
				c.getMetricWithFilter(`mo_frontend_sql_length_bucket`, `label="load-data-inline-sql-length"`),
				c.getMetricWithFilter(`mo_frontend_sql_length_bucket`, `label="other-sql-length""`),
			},
			[]string{
				"total-sql-length",
				"load-data-inline-sql-length",
				"other-sql-length",
			},
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3},
			axis.Unit("s"),
			axis.Min(0))...,
	)
}
