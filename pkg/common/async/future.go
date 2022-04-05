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

type pair struct {
	value interface{}
	err   error
}

type Future struct {
	c      chan pair
	ready  bool
	result pair
}

func AsyncCall(fn func(...interface{}) (interface{}, error), args ...interface{}) *Future {
	var f Future
	// Buffered size 1, so goroutine will not block.
	f.c = make(chan pair, 1)
	go func() {
		v, e := fn(args...)
		f.c <- pair{v, e}
	}()
	return &f
}

func (f *Future) BlockForReady() {
	if !f.ready {
		f.result = <-f.c
		f.ready = true
	}
}

func (f *Future) IsReady() bool {
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

func (f *Future) Get() (interface{}, error) {
	f.BlockForReady()
	return f.result.value, f.result.err
}

func (f *Future) MustGet() interface{} {
	v, e := f.Get()
	if e != nil {
		panic(e)
	}
	return v
}
