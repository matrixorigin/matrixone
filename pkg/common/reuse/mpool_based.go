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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

type mpoolBased[T ReusableObject] struct {
	pool *mpool.MPool
	opts *Options[T]
	c    *checker[T]
}

func newMpoolBased[T ReusableObject](
	capacity int64,
	opts *Options[T]) Pool[T] {
	opts.adjust()
	var v T
	c := newChecker[T](opts.enableChecker)
	mp, err := mpool.NewMPool(fmt.Sprintf("reuse-%s", v.TypeName()), opts.memCapacity, 0)
	if err != nil {
		panic(err)
	}
	return &mpoolBased[T]{
		pool: mp,
		opts: opts,
		c:    c,
	}
}

func (p *mpoolBased[T]) Alloc() *T {
	var t T
	data, err := p.pool.Alloc(int(unsafe.Sizeof(t)))
	if err != nil {
		panic(err)
	}
	v := (*T)(unsafe.Pointer(unsafe.SliceData(data)))
	p.c.created(v)
	p.c.got(v)
	return v
}

func (p *mpoolBased[T]) Free(v *T) {
	p.c.free(v)
	p.opts.release(v)
	p.pool.Free(unsafe.Slice((*byte)(unsafe.Pointer(v)), unsafe.Sizeof(*v)))
}
