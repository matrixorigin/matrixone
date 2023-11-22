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
	"runtime"
	"sync"
)

var (
	pools = map[string]any{}
)

// DefaultOptions default options
func DefaultOptions[T ReusableObject]() *Options[T] {
	return &Options[T]{}
}

// WithReleaseFunc with specified release function. The release function is used to
// release resources before gc.
func (opts *Options[T]) WithReleaseFunc(release func(T)) *Options[T] {
	opts.release = release
	return opts
}

func (opts *Options[T]) adjust() {
	if opts.release == nil {
		opts.release = func(T) {}
	}
}

// CreatePool create pool instance.
func CreatePool[T ReusableObject](
	new func() T,
	reset func(T),
	opts *Options[T]) {
	v := new()
	if p := get(v); p != nil {
		panic(fmt.Sprintf("%T pool already created", v))
	}
	pools[v.Name()] = newSyncPoolBased(new, reset, opts)
}

// Alloc allocates a pooled object.
func Alloc[T ReusableObject]() T {
	var v T
	p := get(v)
	if p == nil {
		panic(fmt.Sprintf("%T pool not created", v))
	}
	return p.Alloc()
}

// Free free a pooled object.
func Free[T ReusableObject](v T) {
	p := get(v)
	if p == nil {
		panic(fmt.Sprintf("%T pool not created", v))
	}
	p.Free(v)
}

func get[T ReusableObject](v T) Pool[T] {
	if pool, ok := pools[v.Name()]; ok {
		return pool.(Pool[T])
	}
	return nil
}

func newSyncPoolBased[T ReusableObject](
	new func() T,
	reset func(T),
	opts *Options[T]) Pool[T] {
	opts.adjust()
	return &syncPoolBased[T]{
		pool: sync.Pool{
			New: func() any {
				v := new()
				runtime.SetFinalizer(
					v,
					func(v T) {
						opts.release(v)
					})
				return v
			},
		},
		reset: reset,
		opts:  opts,
	}
}
