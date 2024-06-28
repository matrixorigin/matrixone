// Copyright 2024 Matrix Origin
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

package malloc

import (
	"hash/maphash"
	"runtime"
	"runtime/debug"
	"sync"
	"unsafe"
)

type StacktraceID uint64

func GetStacktraceID(skip int) StacktraceID {
	pcs := pcs1024Pool.Get().(*[]uintptr)
	defer func() {
		*pcs = (*pcs)[:cap(*pcs)]
		pcs1024Pool.Put(pcs)
	}()

	n := runtime.Callers(2+skip, *pcs)
	*pcs = (*pcs)[:n]

	hasher := hasherPool.Get().(*maphash.Hash)
	defer func() {
		hasher.Reset()
		hasherPool.Put(hasher)
	}()
	for _, pc := range *pcs {
		hasher.Write(
			unsafe.Slice((*byte)(unsafe.Pointer(&pc)), unsafe.Sizeof(pc)),
		)
	}
	id := StacktraceID(hasher.Sum64())

	if _, ok := stackIDToInfo.Load(id); ok {
		return id
	}

	info := debug.Stack()
	stackIDToInfo.LoadOrStore(id, info)

	return id
}

var stackIDToInfo sync.Map

var pcs1024Pool = sync.Pool{
	New: func() any {
		slice := make([]uintptr, 1024)
		return &slice
	},
}

func (s StacktraceID) String() string {
	if v, ok := stackIDToInfo.Load(s); ok {
		return string(v.([]byte))
	}
	panic("bad stack id")
}
