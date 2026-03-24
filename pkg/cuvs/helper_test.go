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
	slice := (*[1 << 30]byte)(ptr)[:size]
	for i := range slice {
		slice[i] = byte(i % 256)
	}

	err = GpuFreePinned(ptr)
	if err != nil {
		t.Fatalf("Failed to free pinned memory: %v", err)
	}
}
