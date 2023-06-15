package model

import (
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
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
	return r
}
