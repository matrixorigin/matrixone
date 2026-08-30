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

package fscache

type Data interface {
	// Size is the logical payload length visible to cache consumers.
	Size() int64
	// Capacity is the allocator-backed capacity retained while the data is live.
	// Cache admission and physical-memory metrics use this value.
	Capacity() int64
	Bytes() []byte
	Slice(length int) Data
	Retain()
	Release()
}

// DataOwner identifies the allocator that owns a cache data buffer. It must
// stay non-zero sized: Go is allowed to reuse addresses for zero-sized values,
// which would make distinct allocators indistinguishable by pointer identity.
type DataOwner struct{ _ byte }

// DataOwnership is implemented by cache data that can identify its backing
// allocator and be copied into another cache allocator without losing its
// cache-specific representation. A MemCache uses this at its admission
// boundary so its capacity, allocator arena, and fragmentation metrics all
// describe the same bytes.
//
// Data implementations that do not implement this interface are still valid:
// MemCache copies their Bytes into its own ordinary cache data representation.
type DataOwnership interface {
	Data
	CacheDataOwner() *DataOwner
	RehomeCacheData(copyData func([]byte) Data) Data
}

// DataCacheReservation is implemented by data whose allocation has reserved
// cache capacity before the data can be retained by the FIFO. Cache insertion
// commits the reservation; Release handles the uninserted path.
type DataCacheReservation interface {
	Data
	CommitCacheReservation()
}

// DataCacheAdmission identifies data that must not be admitted into a
// particular cache. It is used for a read buffer when that cache cannot grant
// capacity without blocking the read; the caller still owns and releases it.
type DataCacheAdmission interface {
	Data
	CacheAdmissionAllowed(*DataOwner) bool
}
