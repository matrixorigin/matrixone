// Copyright 2021 - 2022 Matrix Origin
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

package async

type pair[T any] struct {
	value T
	err   error
}

type Future[T any] struct {
	c      chan pair[T]
	ready  bool
	result pair[T]
}

func AsyncCall[T any](fn func(...interface{}) (T, error), args ...interface{}) *Future[T] {
	var f Future[T]
	// Buffered size 1, so goroutine will not block.
	f.c = make(chan pair[T], 1)
	go func() {
		v, e := fn(args...)
		f.c <- pair[T]{v, e}
	}()
	return &f
}

func (f *Future[T]) BlockForReady() {
	if !f.ready {
		f.result = <-f.c
		f.ready = true
	}
}

func (f *Future[T]) IsReady() bool {
	if f.ready {
		return true
	}
	select {
	case f.result = <-f.c:
		f.ready = true
	default:
		// no ready yet.
	}
	return f.ready
}

func (f *Future[T]) Get() (T, error) {
	f.BlockForReady()
	return f.result.value, f.result.err
}

func (f *Future[T]) MustGet() T {
	v, e := f.Get()
	if e != nil {
		panic(e)
	}
	return v
}
