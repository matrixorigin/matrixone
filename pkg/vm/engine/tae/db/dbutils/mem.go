// Copyright 2021 Matrix Origin
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

package dbutils

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/shirou/gopsutil/v3/mem"
)

func MakeDefaultMemtablePool(name string) *containers.VectorPool {
	var (
		limit            int
		memtableCapacity int
	)
	memStats, err := mem.VirtualMemory()
	if err != nil {
		panic(err)
	}
	if memStats.Total > mpool.GB*20 {
		limit = mpool.MB * 2
		memtableCapacity = 256
	} else if memStats.Total > mpool.GB*10 {
		limit = mpool.MB * 2
		memtableCapacity = 128
	} else if memStats.Total > mpool.GB*5 {
		limit = mpool.MB * 2
		memtableCapacity = 64
	} else {
		limit = mpool.MB * 1
		memtableCapacity = 64
	}

	return containers.NewVectorPool(
		name,
		memtableCapacity,
		containers.WithAllocationLimit(limit),
		containers.WithMPool(common.MutMemAllocator),
	)
}

func MakeDefaultTransientPool(name string) *containers.VectorPool {
	var (
		limit            int
		trasientCapacity int
	)
	memStats, err := mem.VirtualMemory()
	if err != nil {
		panic(err)
	}
	if memStats.Total > mpool.GB*20 {
		limit = mpool.MB
		trasientCapacity = 512
	} else if memStats.Total > mpool.GB*10 {
		limit = mpool.MB * 512
		trasientCapacity = 512
	} else if memStats.Total > mpool.GB*5 {
		limit = mpool.KB * 256
		trasientCapacity = 512
	} else {
		limit = mpool.KB * 256
		trasientCapacity = 256
	}

	return containers.NewVectorPool(
		name,
		trasientCapacity,
		containers.WithAllocationLimit(limit),
	)
}
