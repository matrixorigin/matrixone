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

		// nil pointer
		ptr := bs.Bytes()
		assert.Nil(t, ptr)

		// double free
		assert.Panics(t, func() { bs.Release() }, "double free")
	})

	t.Run("Bytes without refs", func(t *testing.T) {
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
		ptr := bs.Bytes()
		assert.Nil(t, ptr)

		// increment deallocated bytes and try to deallocate again
		bs.Retain() // refs = 1
		assert.Panics(t, func() { bs.Release() }, "deallocate nil pointer")
	})

	t.Run("Bytes nil deallocator", func(t *testing.T) {
		bs := &Bytes{
			bytes:       []byte("123"),
			deallocator: nil,
		}
		bs.refs.Store(1)

		// deallocate memory
		bs.Release()

		// nil pointer
		ptr := bs.Bytes()
		assert.Nil(t, ptr)

		assert.Panics(t, func() { bs.Release() }, "double free")

		// increment deallocated bytes and try to deallocate again
		bs.Retain() // refs = 0
		bs.Retain() // refs = 1
		assert.Panics(t, func() { bs.Release() }, "deallocate nil pointer")
	})
}
