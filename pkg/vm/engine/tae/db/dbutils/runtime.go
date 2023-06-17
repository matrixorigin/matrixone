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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
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

func WithRuntimeFilterIndexCache(c model.LRUCache) RuntimeOption {
	return func(r *Runtime) {
		r.Cache.FilterIndex = c
	}
}

func WithRuntimeObjectFS(fs *objectio.ObjectFS) RuntimeOption {
	return func(r *Runtime) {
		r.Fs = fs
	}
}

func WithRuntimeTransferTable(tt *model.HashPageTable) RuntimeOption {
	return func(r *Runtime) {
		r.TransferTable = tt
	}
}

func WithRuntimeScheduler(s tasks.TaskScheduler) RuntimeOption {
	return func(r *Runtime) {
		r.Scheduler = s
	}
}

type Runtime struct {
	VectorPool struct {
		Memtable  *containers.VectorPool
		Transient *containers.VectorPool
	}

	Cache struct {
		FilterIndex model.LRUCache
	}

	Fs *objectio.ObjectFS

	TransferTable *model.HashPageTable
	Scheduler     tasks.TaskScheduler
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
