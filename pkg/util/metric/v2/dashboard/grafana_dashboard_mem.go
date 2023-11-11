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
			c.initTAEMpoolAllocatorRow(),
		)...)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initTAEMpoolAllocatorRow() dashboard.Option {
	return dashboard.Row(
		"TAE Mpool Allocator",
		c.withGraph(
			"TAE Defaulter Allocator",
			3,
			`sum(`+c.getMetricWithFilter("mo_mem_mpool_allocated_size", `type="tae_default"`)+`) by (name)`,
			"{{ name }}"),

		c.withGraph(
			"TAE Small Allocator",
			3,
			`sum(`+c.getMetricWithFilter("mo_mem_mpool_allocated_size", `type="tae_small"`)+`) by (name)`,
			"{{ name }}"),

		c.withGraph(
			"TAE Mutable Memory Allocator",
			3,
			`sum(`+c.getMetricWithFilter("mo_mem_mpool_allocated_size", `type="tae_mutable"`)+`) by (name)`,
			"{{ name }}"),

		c.withGraph(
			"VectorPool Default Allocator",
			3,
			`sum(`+c.getMetricWithFilter("mo_mem_mpool_allocated_size", `type="vectorpool_default"`)+`) by (name)`,
			"{{ name }}"),
	)
}
