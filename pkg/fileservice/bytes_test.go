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

package fileservice

import (
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/stretchr/testify/assert"
)

func TestBytes(t *testing.T) {
	t.Run("Bytes without refs", func(t *testing.T) {
		bytes, deallocator, err := ioAllocator().Allocate(42, malloc.NoHints)
		assert.Nil(t, err)
		bs := &Bytes{
			bytes:       bytes,
			deallocator: deallocator,
		}
		bs.refs.Store(1)
		bs.Release()
	})
}

func TestBytesError(t *testing.T) {
	t.Run("Bytes get invalid memory", func(t *testing.T) {
		bytes, deallocator, err := ioAllocator().Allocate(42, malloc.NoHints)
		assert.Nil(t, err)
		bs := &Bytes{
			bytes:       bytes,
			deallocator: deallocator,
		}
		bs.refs.Store(1)

		// deallocate memory
		bs.Release()

		// nil pointer
		assert.Panics(t, func() { bs.Bytes() }, "get invalid memory")
	})

	t.Run("Bytes double free", func(t *testing.T) {
		bytes, deallocator, err := ioAllocator().Allocate(42, malloc.NoHints)
		assert.Nil(t, err)
		bs := &Bytes{
			bytes:       bytes,
			deallocator: deallocator,
		}
		bs.refs.Store(1)

		// deallocate memory
		bs.Release()

		// double free
		assert.Panics(t, func() { bs.Release() }, "double free")
	})

	t.Run("Bytes nil deallocator", func(t *testing.T) {
		data := []byte("123")
		bs := NewBytes(data)

		// deallocate memory
		bs.Release()

		assert.Panics(t, func() { bs.Release() }, "double free")
	})
}

// TestBytesResurrection: a released object must not be resurrectable.
// Regression for the sequence NewBytes -> Release -> Retain: before the CAS
// guard, Retain silently brought refs back to 1 and Bytes() returned the
// cleared slice without panicking, defeating both use-after-free and
// double-free detection.
func TestBytesResurrection(t *testing.T) {
	deallocated := 0
	bs := &Bytes{
		bytes:       []byte("123"),
		deallocator: malloc.FuncDeallocator(func() { deallocated++ }),
	}
	bs.refs.Store(1)

	bs.Release()
	assert.Equal(t, 1, deallocated, "exactly one deallocation")

	// Retain after the final Release must panic, not resurrect.
	assert.Panics(t, func() { bs.Retain() }, "retain after free")
	// And the object stays terminally dead.
	assert.Panics(t, func() { bs.Bytes() }, "use after free")
	assert.Panics(t, func() { bs.Slice(1) }, "slice after free")
	assert.Panics(t, func() { bs.Size() }, "size after free")
	assert.Equal(t, 1, deallocated, "still exactly one deallocation")
}

// TestBytesOrderRetainThenRelease: deterministic order where Retain wins
// before the final Release. The object must stay alive for the retainer and
// be deallocated exactly once, by the last releaser.
func TestBytesOrderRetainThenRelease(t *testing.T) {
	deallocated := 0
	bs := &Bytes{
		bytes:       []byte("123"),
		deallocator: malloc.FuncDeallocator(func() { deallocated++ }),
	}
	bs.refs.Store(1)

	retained := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		bs.Retain() // wins while refs is still positive
		close(retained)
		assert.Equal(t, []byte("123"), bs.Bytes())
		bs.Release() // this becomes the final release
	}()

	<-retained
	bs.Release() // owner's release: not final, must not deallocate yet
	<-done

	assert.Equal(t, 1, deallocated, "exactly one deallocation")
	assert.Equal(t, int32(0), bs.refs.Load(), "terminal refcount")
	assert.Panics(t, func() { bs.Retain() }, "terminally dead")
}

// TestBytesOrderReleaseThenRetain: deterministic order where the final
// Release wins before the Retain. The late Retain must panic instead of
// resurrecting the freed object.
func TestBytesOrderReleaseThenRetain(t *testing.T) {
	deallocated := 0
	bs := &Bytes{
		bytes:       []byte("123"),
		deallocator: malloc.FuncDeallocator(func() { deallocated++ }),
	}
	bs.refs.Store(1)

	released := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		bs.Release() // final release wins
		close(released)
	}()

	<-released
	assert.Panics(t, func() { bs.Retain() }, "late retain must not resurrect")
	<-done

	assert.Equal(t, 1, deallocated, "exactly one deallocation")
}

// TestBytesConcurrent is a -race stress over mixed Retain/Release, with the
// deterministic orders covered by the two tests above. The base reference is
// held until all goroutines finish, then the terminal state is asserted.
func TestBytesConcurrent(t *testing.T) {
	deallocated := 0
	bs := &Bytes{
		bytes:       []byte("123"),
		deallocator: malloc.FuncDeallocator(func() { deallocated++ }),
	}
	bs.refs.Store(1)

	nthread := 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < nthread; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // barrier: maximize interleaving
			for j := 0; j < 100; j++ {
				bs.Retain()
				_ = bs.Bytes()
				bs.Release()
			}
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(t, 0, deallocated, "alive while base reference held")
	bs.Release()
	assert.Equal(t, 1, deallocated, "exactly one deallocation")
	assert.Panics(t, func() { bs.Release() }, "double free")
}
