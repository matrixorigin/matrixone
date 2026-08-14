// Copyright 2026 Matrix Origin
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

// MemoryCacheStats is a point-in-time view of the allocator resources dedicated
// to one Memory Cache. Allocated is live size-class-rounded payload. Active
// contains pages still held by the allocator; Active-Allocated is allocator
// slack, separate from caller-visible slice-capacity rounding. Dirty and Muzzy
// are reclaimable pages.
type MemoryCacheStats struct {
	Allocated uint64
	Active    uint64
	Metadata  uint64
	Resident  uint64
	Mapped    uint64
	Retained  uint64
	Dirty     uint64
	Muzzy     uint64
}

// MemoryCacheAllocator supplies the allocation-size contract and resource
// statistics required by the Memory Cache. The selected implementation is kept
// inside this package so fileservice does not depend on allocator-specific APIs.
type MemoryCacheAllocator interface {
	Allocator
	Stats() (MemoryCacheStats, error)
}

// NewMemoryCacheAllocator creates the production allocator for one isolated
// Memory Cache. It intentionally returns an error instead of silently changing
// the backing-size or statistics contract when initialization is unavailable.
func NewMemoryCacheAllocator() (MemoryCacheAllocator, error) {
	return NewJemallocAllocator()
}
