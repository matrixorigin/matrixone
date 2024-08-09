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

func (c *DashboardCreator) initPipelineDashBoard() error {
	folder, err := c.createFolder(c.folderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"Pipeline Metrics",
		c.withRowOptions(
			c.initRemoteRunPipelineCountRow(),
		)...)
	if err != nil {
		return err
	}

	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initRemoteRunPipelineCountRow() dashboard.Option {
	return dashboard.Row(
		"remote run",
		c.withGraph(
			"message sender",
			6,
			`sum(`+c.getMetricWithFilter("mo_pipeline_stream_connection", `type="living"`)+`)`,
			"living message sender",
		),
	)
}
