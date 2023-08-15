// Copyright 2022 Matrix Origin
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

package fileservice

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
)

// RCPool represents a pool of reference counting objects
type RCPool[T any] struct {
	pool sync.Pool
}

type RCPoolItem[T any] struct {
	pool  *RCPool[T]
	Value T
	count atomic.Int32
	logs  *rcPoolItemLogs
}

type rcPoolItemLogs struct {
	sync.Mutex
	logs [][2]string
}

var debuggingRCPool = os.Getenv("DEBUG_RC_POOL") != ""

func NewRCPool[T any](
	newFunc func() T,
) *RCPool[T] {
	var ret *RCPool[T]
	ret = &RCPool[T]{
		pool: sync.Pool{
			New: func() any {

				item := &RCPoolItem[T]{
					pool:  ret,
					Value: newFunc(),
				}

				if debuggingRCPool {
					item.logs = new(rcPoolItemLogs)
					runtime.SetFinalizer(
						item,
						func(item *RCPoolItem[T]) {
							if item.count.Load() != 0 {
								buf := new(strings.Builder)
								for i, log := range item.logs.logs {
									fmt.Fprintf(buf, "\n%d: %s, %s", i, log[0], log[1])
								}
								panic(buf.String() + "item gc with non-zero reference count")
							}
						},
					)
				}

				return item
			},
		},
	}
	return ret
}

func (r *RCPool[T]) Get() *RCPoolItem[T] {
	item := r.pool.Get().(*RCPoolItem[T])
	if !item.count.CompareAndSwap(0, 1) {
		panic("invalid object reference count")
	}
	item.log("get")
	return item
}

func (r *RCPoolItem[T]) Retain() {
	r.log("retain")
	r.count.Add(1)
}

func (r *RCPoolItem[T]) Release() {
	r.log("release")
	if c := r.count.Add(-1); c == 0 {
		if r.pool != nil {
			r.pool.pool.Put(r)
		}
	} else if c < 0 {
		panic("bad release")
	}
}

func (r *RCPoolItem[T]) log(what string) {
	if debuggingRCPool {
		r.logs.Lock()
		r.logs.logs = append(r.logs.logs, [2]string{what, string(debug.Stack())})
		r.logs.Unlock()
	}
}
