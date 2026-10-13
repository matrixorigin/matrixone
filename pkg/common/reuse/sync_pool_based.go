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

type syncPoolBased[T any, P ReusableObject[T]] struct {
	pool  sync.Pool
	reset func(P)
	opts  *Options[T, P]
	c     *checker[T, P]
}

func newSyncPoolBased[T any, P ReusableObject[T]](
	new func() P,
	reset func(P),
	opts *Options[T, P]) Pool[T, P] {
	opts.adjust()
	c := newChecker[T, P](opts.enableChecker)
	return &syncPoolBased[T, P]{
		pool: sync.Pool{
			New: func() any {
				return new()
			},
		},
		reset: reset,
		opts:  opts,
		c:     c,
	}
}

func (p *syncPoolBased[T, P]) Alloc() P {
	v := p.pool.Get().(P)
	if p.c.enable {
		if epoch := checkerActive.current(); epoch != 0 &&
			p.c.gotActive(v, epoch, true) {
			p.installFinalizer(v)
		}
	}
	return v
}

func (p *syncPoolBased[T, P]) Free(v P) {
	if p.c.enable {
		if epoch := checkerActive.current(); epoch != 0 &&
			p.c.freeActive(v, epoch, true) {
			p.installFinalizer(v)
		}
	}
	p.reset(v)
	p.pool.Put(v)
}

func (p *syncPoolBased[T, P]) installFinalizer(v P) {
	c := p.c
	opts := p.opts
	runtime.SetFinalizer(
		v,
		func(v P) {
			defer opts.release(v)
			if opts.gcRecover != nil {
				defer opts.gcRecover()
			}
			c.gc(v)
		})
}
