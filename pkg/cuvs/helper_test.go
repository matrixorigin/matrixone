//go:build gpu

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

package cuvs

import (
	"testing"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/util"
)

func TestPinnedMemory(t *testing.T) {
	size := uint64(1024 * 1024) // 1MB
	ptr, err := GpuAllocPinned(size)
	if err != nil {
		t.Fatalf("Failed to allocate pinned memory: %v", err)
	}
	if ptr == nil {
		t.Fatal("GpuAllocPinned returned nil")
	}

	// Verify we can write to it
	slice := util.UnsafeSlice[byte](ptr, int(size))
	for i := range slice {
		slice[i] = byte(i % 256)
	}

	err = GpuFreePinned(ptr)
	if err != nil {
		t.Fatalf("Failed to free pinned memory: %v", err)
	}
}

func TestPinnedPool(t *testing.T) {
	size := uint64(1024)
	pool := &PinnedPool{
		New: func() unsafe.Pointer {
			ptr, err := GpuAllocPinned(size)
			if err != nil {
				return nil
			}
			return ptr
		},
	}

	// First Get should trigger New
	p1 := pool.Get()
	if p1 == nil {
		t.Fatal("First Get() returned nil")
	}

	// Put it back
	pool.Put(p1)

	// Second Get should return the same pointer
	p2 := pool.Get()
	if p2 == nil {
		t.Fatal("Second Get() returned nil")
	}
	if p1 != p2 {
		t.Fatalf("Expected same pointer, got %p and %p", p1, p2)
	}

	// Third Get should trigger New (since pool is empty now)
	p3 := pool.Get()
	if p3 == nil {
		t.Fatal("Third Get() returned nil")
	}
	if p3 == p2 {
		t.Fatal("Expected a different pointer for third Get()")
	}

	// Put everything back and test Destroy
	pool.Put(p2)
	pool.Put(p3)

	if err := pool.Destroy(); err != nil {
		t.Fatalf("Destroy failed: %v", err)
	}

	// Verify items are cleared by removing New and checking if Get returns nil
	pool.New = nil
	p4 := pool.Get()
	if p4 != nil {
		t.Fatal("Destroy did not clear the pool items")
	}
}
