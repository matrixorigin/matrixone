// Copyright 2026 Matrix Origin
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

// Package bufferlease owns immutable external byte-buffer lifetimes shared by
// containers, decoders, and FileService adapters.
package bufferlease

import (
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// BufferLease is a ref-counted immutable byte backing. Retain must fail after
// the refcount reaches zero; every successful Retain requires one Release.
// Bytes is valid only while the caller owns a live reference.
type BufferLease interface {
	Retain() bool
	Release()
	Bytes() []byte
	AccountedBytes() int64
}

// RefCounted is the default BufferLease implementation. NewRefCounted returns
// one initial owner reference.
type RefCounted struct {
	refs       atomic.Int64
	accounted  int64
	mu         sync.RWMutex
	data       []byte
	releaseOne func()
}

func NewRefCounted(
	data []byte,
	accountedBytes int64,
	releaseOne func(),
) (*RefCounted, error) {
	if accountedBytes < 0 {
		return nil, moerr.NewInvalidInputNoCtx("negative buffer lease accounting")
	}
	lease := &RefCounted{
		data:       data,
		accounted:  accountedBytes,
		releaseOne: releaseOne,
	}
	lease.refs.Store(1)
	return lease, nil
}

func (l *RefCounted) Retain() bool {
	if l == nil {
		return false
	}
	for {
		refs := l.refs.Load()
		if refs <= 0 {
			return false
		}
		if l.refs.CompareAndSwap(refs, refs+1) {
			return true
		}
	}
}

func (l *RefCounted) Release() {
	if l == nil {
		panic("release nil buffer lease")
	}
	refs := l.refs.Add(-1)
	if refs < 0 {
		panic("buffer lease release underflow")
	}
	if refs != 0 {
		return
	}

	// The successful 1 -> 0 transition is the only backing cleanup owner.
	l.mu.Lock()
	l.data = nil
	releaseOne := l.releaseOne
	l.releaseOne = nil
	l.mu.Unlock()
	if releaseOne != nil {
		releaseOne()
	}
}

func (l *RefCounted) Bytes() []byte {
	if l == nil || l.refs.Load() <= 0 {
		return nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.data
}

func (l *RefCounted) AccountedBytes() int64 {
	if l == nil {
		return 0
	}
	return l.accounted
}
