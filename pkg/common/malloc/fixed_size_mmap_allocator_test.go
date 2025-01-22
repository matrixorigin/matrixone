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
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"testing"
	"unsafe"
)

func TestMmapAllocator(t *testing.T) {
	testAllocator(t, func() Allocator {
		return NewShardedAllocator(
			runtime.GOMAXPROCS(0),
			func() Allocator {
				return NewClassAllocator(NewFixedSizeMmapAllocator)
			},
		)
	})
}

func BenchmarkMmapAllocator(b *testing.B) {
	for _, n := range benchNs {
		benchmarkAllocator(b, func() Allocator {
			return NewShardedAllocator(
				runtime.GOMAXPROCS(0),
				func() Allocator {
					return NewClassAllocator(NewFixedSizeMmapAllocator)
				},
			)
		}, n)
	}
}

func FuzzMmapAllocator(f *testing.F) {
	fuzzAllocator(f, func() Allocator {
		return NewShardedAllocator(
			runtime.GOMAXPROCS(0),
			func() Allocator {
				return NewClassAllocator(NewFixedSizeMmapAllocator)
			},
		)
	})
}

func TestSlab(t *testing.T) {
	const objectSize = 4
	base := unsafe.Pointer(unsafe.SliceData(make([]byte, objectSize*64)))
	slab := &_Slab{
		base:       base,
		objectSize: objectSize,
	}

	var ptrs []unsafe.Pointer
	for i := 0; i < 64; i++ {
		ptr, ok := slab.allocate()
		if !ok {
			t.Fatal()
		}
		if uintptr(ptr) != uintptr(base)+uintptr(i*objectSize) {
			t.Fatal()
		}
		ptrs = append(ptrs, ptr)
	}

	_, ok := slab.allocate()
	if ok {
		t.Fatal()
	}

	for i, ptr := range ptrs {
		empty := slab.free(ptr)
		if empty != (i == 63) {
			t.Fatal()
		}
	}

}

func TestMmapLongRun(t *testing.T) {
	t.Skip()

	go http.ListenAndServe(":9998", nil)

	chanCap := 10
	ch1 := make(chan Deallocator, chanCap)
	ch2 := make(chan Deallocator, chanCap)
	ch3 := make(chan Deallocator, chanCap)

	numThreads := runtime.GOMAXPROCS(0)
	allocator := NewShardedAllocator(
		numThreads,
		func() Allocator {
			return NewClassAllocator(NewFixedSizeMmapAllocator)
		},
	)

	for range numThreads {
		go func() {
			for i := 0; ; i++ {
				size := i%(2*MB) + 1
				_, dec, err := allocator.Allocate(uint64(size), NoHints)
				if err != nil {
					panic(err)
				}
				select {
				case ch1 <- dec:
				case ch2 <- dec:
				case ch3 <- dec:
				}
			}
		}()
	}

	for {
		select {
		case dec := <-ch1:
			dec.Deallocate(NoHints)
		case dec := <-ch2:
			dec.Deallocate(NoHints)
		case dec := <-ch3:
			dec.Deallocate(NoHints)
		}
	}

}
