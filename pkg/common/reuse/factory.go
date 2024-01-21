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

package reuse

import (
	"fmt"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

var (
	pools = map[string]any{}
)

var (
	SyncBased  = SPI(0)
	MpoolBased = SPI(1)

	defaultSPI    = SyncBased
	enableChecker atomic.Bool
	enableVerbose atomic.Bool
)

// SPI choose pool implementation
type SPI int

func use(spi SPI) {
	defaultSPI = spi
}

// DefaultOptions default options
func DefaultOptions[T ReusableObject]() *Options[T] {
	return &Options[T]{}
}

// WithReleaseFunc with specified release function. The release function is used to
// release resources before gc.
func (opts *Options[T]) WithReleaseFunc(release func(*T)) *Options[T] {
	opts.release = release
	return opts
}

// WithEnableChecker enable check double free, leak free.
func (opts *Options[T]) WithEnableChecker() *Options[T] {
	opts.enableChecker = true
	return opts
}

func (opts *Options[T]) withGCRecover(fn func()) *Options[T] {
	opts.gcRecover = fn
	return opts
}

func (opts *Options[T]) adjust() {
	if opts.release == nil {
		opts.release = func(*T) {}
	}
	if opts.memCapacity == 0 {
		opts.memCapacity = mpool.MB
	}
}

// CreatePool create pool instance.
func CreatePool[T ReusableObject](
	new func() *T,
	reset func(*T),
	opts *Options[T]) {
	var v T
	if p := get(v); p != nil {
		panic(fmt.Sprintf("%T pool already created", v))
	}

	switch defaultSPI {
	case SyncBased:
		pools[v.TypeName()] = newSyncPoolBased(new, reset, opts)
	case MpoolBased:
		pools[v.TypeName()] = newMpoolBased(mpool.MB*5, opts)
	}
}

// Alloc allocates a pooled object.
func Alloc[T ReusableObject](p Pool[T]) *T {
	if p == nil {
		var v T
		p = get(v)
		if p == nil {
			panic(fmt.Sprintf("%T pool not created", v))
		}
	}
	return p.Alloc()
}

// Free free a pooled object.
func Free[T ReusableObject](v *T, p Pool[T]) {
	if p == nil {
		var ev T
		p = get(ev)
	}
	if p == nil {
		panic(fmt.Sprintf("%T pool not created", v))
	}
	p.Free(v)
}

func get[T ReusableObject](v T) Pool[T] {
	if pool, ok := pools[v.TypeName()]; ok {
		return pool.(Pool[T])
	}
	return nil
}
