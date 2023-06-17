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

package model

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/shirou/gopsutil/v3/mem"
)

type RuntimeOption func(*Runtime)

func WithRuntimeMemtablePool(vp *containers.VectorPool) RuntimeOption {
	return func(r *Runtime) {
		r.VectorPool.Memtable = vp
	}
}

func WithRuntimeTransientPool(vp *containers.VectorPool) RuntimeOption {
	return func(r *Runtime) {
		r.VectorPool.Transient = vp
	}
}

func WithRuntimeFilterIndexCache(c LRUCache) RuntimeOption {
	return func(r *Runtime) {
		r.Cache.FilterIndex = c
	}
}

func WithRuntimeObjectFS(fs *objectio.ObjectFS) RuntimeOption {
	return func(r *Runtime) {
		r.Fs = fs
	}
}

func WithRuntimeTransferTable(tt *HashPageTable) RuntimeOption {
	return func(r *Runtime) {
		r.TransferTable = tt
	}
}

type Runtime struct {
	VectorPool struct {
		Memtable  *containers.VectorPool
		Transient *containers.VectorPool
	}

	Cache struct {
		FilterIndex LRUCache
	}

	Fs *objectio.ObjectFS

	TransferTable *HashPageTable
}

func NewRuntime(opts ...RuntimeOption) *Runtime {
	r := new(Runtime)
	for _, opt := range opts {
		opt(r)
	}
	r.fillDefaults()
	return r
}

func (r *Runtime) fillDefaults() {
	if r.VectorPool.Memtable == nil {
		r.VectorPool.Memtable = MakeDefaultMemtablePool("memtable-vector-pool")
	}
	if r.VectorPool.Transient == nil {
		r.VectorPool.Transient = MakeDefaultTransientPool("trasient-vector-pool")
	}
}

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
