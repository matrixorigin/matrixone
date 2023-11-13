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

func (c *DashboardCreator) initMemDashboard() error {
	folder, err := c.createFolder(moFolderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"Memory Metrics",
		c.withRowOptions(
			c.initMpoolAllocatorRow(),
		)...)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initMpoolAllocatorRow() dashboard.Option {
	return dashboard.Row(
		"",
		c.withMultiGraph(
			"TAE Mpool Allocator",
			6,
			[]string{
				`sum(` + c.getMetricWithFilter("mo_mem_mpool_allocated_size", `type="tae_default"`) + `)`,
				`sum(` + c.getMetricWithFilter("mo_mem_mpool_allocated_size", `type="tae_small"`) + `)`,
				`sum(` + c.getMetricWithFilter("mo_mem_mpool_allocated_size", `type="tae_mutable"`) + `)`,
				`sum(` + c.getMetricWithFilter("mo_mem_mpool_allocated_size", `type="vectorpool_default"`) + `)`,
			},
			[]string{
				"tae-defaulter-allocator",
				"tae-small-allocator",
				"tae-mutable-memory-allocator",
				"vectorPool-default-allocator",
			}),

		c.withGraph(
			"Cross Pool Free Counter",
			6,
			`increase(`+c.getMetricWithFilter("mo_mem_cross_pool_free_total", "")+`[$interval])`,
			""),
	)
}
