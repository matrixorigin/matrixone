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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

var (
	pools = map[unsafe.Pointer]any{}
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
func DefaultOptions[T any, P ReusableObject[T]]() *Options[T, P] {
	return &Options[T, P]{}
}

// WithReleaseFunc with specified release function. The release function is used to
// release resources before gc.
func (opts *Options[T, P]) WithReleaseFunc(release func(P)) *Options[T, P] {
	opts.release = release
	return opts
}

// WithEnableChecker enable check double free, leak free.
func (opts *Options[T, P]) WithEnableChecker() *Options[T, P] {
	opts.enableChecker = true
	return opts
}

func (opts *Options[T, P]) withGCRecover(fn func()) *Options[T, P] {
	opts.gcRecover = fn
	return opts
}

func (opts *Options[T, P]) adjust() {
	if opts.release == nil {
		opts.release = func(P) {}
	}
	if opts.memCapacity == 0 {
		opts.memCapacity = mpool.MB
	}
}

// CreatePool create pool instance.
func CreatePool[T any, P ReusableObject[T]](
	new func() P,
	reset func(P),
	opts *Options[T, P]) {
	if p := get[T, P](); p != nil {
		panic(fmt.Sprintf("%T pool already created", P(nil)))
	}

	tp := typeOf[T]()
	switch defaultSPI {
	case SyncBased:
		pools[tp] = newSyncPoolBased(new, reset, opts)
	case MpoolBased:
		pools[tp] = newMpoolBased(mpool.MB*5, opts)
	}
}

// Alloc allocates a pooled object.
func Alloc[T any, P ReusableObject[T]](p Pool[T, P]) P {
	if p == nil {
		var v T
		p = get[T, P]()
		if p == nil {
			panic(fmt.Sprintf("%T pool not created", v))
		}
	}
	return p.Alloc()
}

// Free free a pooled object.
func Free[T any, P ReusableObject[T]](v P, p Pool[T, P]) {
	if p == nil {
		p = get[T, P]()
	}
	if p == nil {
		panic(fmt.Sprintf("%T pool not created", v))
	}
	p.Free(v)
}

func get[T any, P ReusableObject[T]]() Pool[T, P] {
	if pool, ok := pools[typeOf[T]()]; ok {
		return pool.(Pool[T, P])
	}
	return nil
}

func typeOf[T any]() unsafe.Pointer {
	var v *T
	i := any(v)
	// any is a fat point and reflect.Type is a *abi.Type
	// type emptyInterface struct {
	// 	typ  *abi.Type
	// 	word unsafe.Pointer
	// }
	return *(*unsafe.Pointer)(unsafe.Pointer(&i))
}
