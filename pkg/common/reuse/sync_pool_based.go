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
	"runtime"
	"sync"
)

type syncPoolBased[T ReusableObject] struct {
	pool  sync.Pool
	reset func(*T)
	opts  *Options[T]
	c     *checker[T]
}

func newSyncPoolBased[T ReusableObject](
	new func() *T,
	reset func(*T),
	opts *Options[T]) Pool[T] {
	opts.adjust()
	c := newChecker[T](opts.enableChecker)
	return &syncPoolBased[T]{
		pool: sync.Pool{
			New: func() any {
				v := new()

				if enableChecker.Load() && c.enable {
					c.created(v)
					runtime.SetFinalizer(
						v,
						func(v *T) {
							if opts.gcRecover != nil {
								defer opts.gcRecover()
							}
							c.gc(v)
							opts.release(v)
						})
				}

				return v
			},
		},
		reset: reset,
		opts:  opts,
		c:     c,
	}
}

func (p *syncPoolBased[T]) Alloc() *T {
	v := p.pool.Get().(*T)
	p.c.got(v)
	return v
}

func (p *syncPoolBased[T]) Free(v *T) {
	p.c.free(v)
	p.reset(v)
	p.pool.Put(v)
}
