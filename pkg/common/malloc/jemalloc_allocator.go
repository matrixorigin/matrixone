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

//go:build cgo

package malloc

/*
#cgo CFLAGS: -I${SRCDIR}/../../../thirdparties/install/include
#cgo LDFLAGS: -L${SRCDIR}/../../../thirdparties/install/lib -ljemalloc
#cgo linux LDFLAGS: -lpthread -ldl

#define JEMALLOC_NO_DEMANGLE

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <jemalloc.h>

typedef struct {
	size_t allocated;
	size_t active;
	size_t metadata;
	size_t resident;
	size_t mapped;
	size_t retained;
	size_t dirty;
	size_t muzzy;
} mo_jemalloc_stats;

static int
mo_jemalloc_create_arena(unsigned *arena) {
	size_t size = sizeof(*arena);
	return je_mallctl("arenas.create", arena, &size, NULL, 0);
}

static void *
mo_jemalloc_mallocx(size_t size, unsigned arena) {
	return je_mallocx(size, MALLOCX_ARENA(arena) | MALLOCX_TCACHE_NONE);
}

static void
mo_jemalloc_dallocx(void *ptr) {
	je_dallocx(ptr, MALLOCX_TCACHE_NONE);
}

static size_t
mo_jemalloc_nallocx(size_t size) {
	return je_nallocx(size, MALLOCX_TCACHE_NONE);
}

static int
mo_jemalloc_read_size(const char *name, size_t *value) {
	size_t size = sizeof(*value);
	return je_mallctl(name, value, &size, NULL, 0);
}

static int
mo_jemalloc_arena_stat(unsigned arena, const char *suffix, size_t *value) {
	char name[96];
	if (snprintf(name, sizeof(name), "stats.arenas.%u.%s", arena, suffix) >= (int)sizeof(name)) {
		return EINVAL;
	}
	return mo_jemalloc_read_size(name, value);
}

static int
mo_jemalloc_read_stats(unsigned arena, mo_jemalloc_stats *stats) {
	uint64_t epoch = 1;
	size_t page = 0;
	size_t small = 0;
	size_t large = 0;
	size_t pactive = 0;
	size_t pdirty = 0;
	size_t pmuzzy = 0;
	size_t base = 0;
	size_t internal = 0;

	memset(stats, 0, sizeof(*stats));
	if (je_mallctl("epoch", NULL, NULL, &epoch, sizeof(epoch)) != 0) {
		return EINVAL;
	}
	if (mo_jemalloc_read_size("arenas.page", &page) != 0 ||
		mo_jemalloc_arena_stat(arena, "small.allocated", &small) != 0 ||
		mo_jemalloc_arena_stat(arena, "large.allocated", &large) != 0 ||
		mo_jemalloc_arena_stat(arena, "pactive", &pactive) != 0 ||
		mo_jemalloc_arena_stat(arena, "pdirty", &pdirty) != 0 ||
		mo_jemalloc_arena_stat(arena, "pmuzzy", &pmuzzy) != 0 ||
		mo_jemalloc_arena_stat(arena, "mapped", &stats->mapped) != 0 ||
		mo_jemalloc_arena_stat(arena, "retained", &stats->retained) != 0 ||
		mo_jemalloc_arena_stat(arena, "base", &base) != 0 ||
		mo_jemalloc_arena_stat(arena, "internal", &internal) != 0) {
		return EINVAL;
	}

	stats->allocated = small + large;
	stats->active = pactive * page;
	stats->dirty = pdirty * page;
	stats->muzzy = pmuzzy * page;
	stats->metadata = base + internal;
	// jemalloc reports arena resident as active + dirty + muzzy + metadata.
	stats->resident = stats->active + stats->dirty + stats->muzzy + stats->metadata;
	return 0;
}
*/
import "C"

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// JemallocAllocator uses one explicit jemalloc arena. It deliberately bypasses
// per-thread tcaches so a Memory Cache's arena statistics and reclamation are
// attributable to that cache instead of to whichever goroutine last used it.
// The arena is process-lifetime: callers can hold cache data past a cache Flush,
// so destroying it on Close would turn a valid retained value into use-after-free.
type JemallocAllocator struct {
	arena           uint
	deallocatorPool *ClosureDeallocatorPool[jemallocDeallocatorArgs, *jemallocDeallocatorArgs]
}

type jemallocDeallocatorArgs struct {
	ptr unsafe.Pointer
}

func (jemallocDeallocatorArgs) As(Trait) bool {
	return false
}

// NewJemallocAllocator creates an isolated arena. Failure is returned rather
// than falling back to another allocator: cache capacity accounting depends on
// this allocator's nallocx size-class contract.
func NewJemallocAllocator() (*JemallocAllocator, error) {
	var arena C.uint
	if err := C.mo_jemalloc_create_arena(&arena); err != 0 {
		return nil, moerr.NewInternalErrorNoCtxf("create jemalloc arena: %d", int(err))
	}

	ret := &JemallocAllocator{arena: uint(arena)}
	ret.deallocatorPool = NewClosureDeallocatorPool(
		func(_ Hints, args *jemallocDeallocatorArgs) {
			C.mo_jemalloc_dallocx(args.ptr)
		},
	)
	return ret, nil
}

var _ MemoryCacheAllocator = new(JemallocAllocator)

func (j *JemallocAllocator) Allocate(size uint64, hints Hints) ([]byte, Deallocator, error) {
	backingSize, err := j.BackingSize(size)
	if err != nil {
		return nil, nil, err
	}

	ptr := C.mo_jemalloc_mallocx(C.size_t(size), C.uint(j.arena))
	if ptr == nil {
		return nil, nil, moerr.NewOOMNoCtx()
	}
	if backingSize > uint64(maxIntValue()) {
		C.mo_jemalloc_dallocx(ptr)
		return nil, nil, moerr.NewInternalErrorNoCtxf("jemalloc allocation %d overflows int", backingSize)
	}

	buf := unsafe.Slice((*byte)(ptr), int(backingSize))
	if hints&NoClear == 0 {
		clear(buf[:int(size)])
	}
	return buf[:int(size)], j.deallocatorPool.Get(jemallocDeallocatorArgs{ptr: ptr}), nil
}

func (j *JemallocAllocator) BackingSize(size uint64) (uint64, error) {
	if size == 0 {
		return 0, moerr.NewInvalidInputNoCtx("backing size requires a positive request")
	}
	if size > uint64(maxIntValue()) {
		return 0, moerr.NewInternalErrorNoCtxf("jemalloc allocation %d overflows int", size)
	}
	backingSize := uint64(C.mo_jemalloc_nallocx(C.size_t(size)))
	if backingSize < size {
		return 0, moerr.NewInternalErrorNoCtxf("jemalloc cannot allocate %d bytes", size)
	}
	return backingSize, nil
}

func (*JemallocAllocator) BackingSizeContract() (BackingSizeContract, error) {
	return BackingSizeContractClass, nil
}

// Arena returns the allocator's jemalloc arena index for diagnostics.
func (j *JemallocAllocator) Arena() uint {
	return j.arena
}

// Stats refreshes jemalloc's epoch and returns stats for this allocator only.
func (j *JemallocAllocator) Stats() (MemoryCacheStats, error) {
	var stats C.mo_jemalloc_stats
	if err := C.mo_jemalloc_read_stats(C.uint(j.arena), &stats); err != 0 {
		return MemoryCacheStats{}, moerr.NewInternalErrorNoCtxf("read jemalloc arena %d stats: %d", j.arena, int(err))
	}
	return MemoryCacheStats{
		Allocated: uint64(stats.allocated),
		Active:    uint64(stats.active),
		Metadata:  uint64(stats.metadata),
		Resident:  uint64(stats.resident),
		Mapped:    uint64(stats.mapped),
		Retained:  uint64(stats.retained),
		Dirty:     uint64(stats.dirty),
		Muzzy:     uint64(stats.muzzy),
	}, nil
}
