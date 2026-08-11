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

//go:build !cgo

package malloc

import "github.com/matrixorigin/matrixone/pkg/common/moerr"

// JemallocStats keeps the same API shape in non-cgo builds so callers can
// compile. Stats always returns NotSupported in this configuration.
type JemallocStats struct {
	Allocated uint64
	Active    uint64
	Metadata  uint64
	Resident  uint64
	Mapped    uint64
	Retained  uint64
	Dirty     uint64
	Muzzy     uint64
}

type JemallocAllocator struct{}

func NewJemallocAllocator() (*JemallocAllocator, error) {
	return nil, moerr.NewNotSupportedNoCtx("memory cache jemalloc allocator requires cgo")
}

func (*JemallocAllocator) Allocate(uint64, Hints) ([]byte, Deallocator, error) {
	return nil, nil, moerr.NewNotSupportedNoCtx("memory cache jemalloc allocator requires cgo")
}

func (*JemallocAllocator) BackingSize(uint64) (uint64, error) {
	return 0, moerr.NewNotSupportedNoCtx("memory cache jemalloc allocator requires cgo")
}

func (*JemallocAllocator) BackingSizeContract() (BackingSizeContract, error) {
	return backingSizeContractUnknown, moerr.NewNotSupportedNoCtx("memory cache jemalloc allocator requires cgo")
}

func (*JemallocAllocator) Arena() uint { return 0 }

func (*JemallocAllocator) Stats() (JemallocStats, error) {
	return JemallocStats{}, moerr.NewNotSupportedNoCtx("memory cache jemalloc allocator requires cgo")
}
