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
mo_jemalloc_arena_resident(unsigned arena, size_t *value) {
	return mo_jemalloc_arena_stat(arena, "resident", value);
}

static int
mo_jemalloc_arena_purge(unsigned arena) {
	char name[96];
	if (snprintf(name, sizeof(name), "arena.%u.purge", arena) >= (int)sizeof(name)) {
		return EINVAL;
	}
	return je_mallctl(name, NULL, NULL, NULL, 0);
}

static int
mo_jemalloc_destroy_arena(unsigned arena) {
	char name[96];
	if (snprintf(name, sizeof(name), "arena.%u.destroy", arena) >= (int)sizeof(name)) {
		return EINVAL;
	}
	return je_mallctl(name, NULL, NULL, NULL, 0);
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
		mo_jemalloc_arena_resident(arena, &stats->resident) != 0 ||
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
	return 0;
}
*/
import "C"

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// JemallocAllocator uses one explicit jemalloc arena. It deliberately bypasses
// per-thread tcaches so a Memory Cache's arena statistics and reclamation are
// attributable to that cache instead of to whichever goroutine last used it.
// Closing a cache generation prevents new allocations but retains the arena
// until the last outstanding allocation is returned.
type JemallocAllocator struct {
	arena           *jemallocArena
	deallocatorPool *ClosureDeallocatorPool[jemallocDeallocatorArgs, *jemallocDeallocatorArgs]
}

type jemallocArena struct {
	mu         sync.Mutex
	index      uint
	refs       uint64 // allocator owner plus every successful allocation
	closed     bool
	destroyed  bool
	destroyErr error
}

var liveJemallocArenas atomic.Int64

type jemallocDeallocatorArgs struct {
	ptr   unsafe.Pointer
	arena *jemallocArena
}

func (jemallocDeallocatorArgs) As(Trait) bool {
	return false
}

func (a *jemallocArena) retain(allowClosed bool) (uint, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.destroyed {
		return 0, moerr.NewInvalidStateNoCtxf("jemalloc arena %d is destroyed", a.index)
	}
	if a.closed && !allowClosed {
		return 0, moerr.NewInvalidStateNoCtxf("jemalloc arena %d is closed", a.index)
	}
	a.refs++
	return a.index, nil
}

func (a *jemallocArena) release() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.refs == 0 {
		panic("jemalloc arena reference underflow")
	}
	a.refs--
	if !a.closed || a.refs != 0 {
		return nil
	}
	return a.destroyLocked()
}

func (a *jemallocArena) close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return a.destroyErr
	}
	a.closed = true
	if a.refs == 0 {
		panic("jemalloc arena owner reference is missing")
	}
	a.refs--
	if a.refs != 0 {
		return nil
	}
	return a.destroyLocked()
}

func (a *jemallocArena) destroyLocked() error {
	if a.destroyed {
		return a.destroyErr
	}
	if err := C.mo_jemalloc_destroy_arena(C.uint(a.index)); err != 0 {
		a.destroyErr = moerr.NewInternalErrorNoCtxf("destroy jemalloc arena %d: %d", a.index, int(err))
		return a.destroyErr
	}
	a.destroyed = true
	liveJemallocArenas.Add(-1)
	return nil
}

// NewJemallocAllocator creates an isolated arena. Failure is returned rather
// than falling back to another allocator: cache capacity accounting depends on
// this allocator's nallocx size-class contract.
func NewJemallocAllocator() (*JemallocAllocator, error) {
	var arena C.uint
	if err := C.mo_jemalloc_create_arena(&arena); err != 0 {
		return nil, moerr.NewInternalErrorNoCtxf("create jemalloc arena: %d", int(err))
	}

	ret := &JemallocAllocator{
		arena: &jemallocArena{index: uint(arena), refs: 1},
	}
	liveJemallocArenas.Add(1)
	ret.deallocatorPool = NewClosureDeallocatorPool(
		func(_ Hints, args *jemallocDeallocatorArgs) {
			C.mo_jemalloc_dallocx(args.ptr)
			if err := args.arena.release(); err != nil {
				panic(err)
			}
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
	arena, err := j.arena.retain(false)
	if err != nil {
		return nil, nil, err
	}

	ptr := C.mo_jemalloc_mallocx(C.size_t(size), C.uint(arena))
	if ptr == nil {
		if err := j.arena.release(); err != nil {
			return nil, nil, err
		}
		return nil, nil, moerr.NewOOMNoCtx()
	}
	if backingSize > uint64(maxIntValue()) {
		C.mo_jemalloc_dallocx(ptr)
		if err := j.arena.release(); err != nil {
			return nil, nil, err
		}
		return nil, nil, moerr.NewInternalErrorNoCtxf("jemalloc allocation %d overflows int", backingSize)
	}

	buf := unsafe.Slice((*byte)(ptr), int(backingSize))
	if hints&NoClear == 0 {
		clear(buf[:int(size)])
	}
	return buf[:int(size)], j.deallocatorPool.Get(jemallocDeallocatorArgs{ptr: ptr, arena: j.arena}), nil
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
	return j.arena.index
}

// Stats refreshes jemalloc's epoch and returns stats for this allocator only.
func (j *JemallocAllocator) Stats() (stats MemoryCacheStats, err error) {
	arena, err := j.arena.retain(true)
	if err != nil {
		return MemoryCacheStats{}, err
	}
	defer func() {
		if releaseErr := j.arena.release(); err == nil && releaseErr != nil {
			err = releaseErr
		}
	}()

	var nativeStats C.mo_jemalloc_stats
	if cErr := C.mo_jemalloc_read_stats(C.uint(arena), &nativeStats); cErr != 0 {
		return MemoryCacheStats{}, moerr.NewInternalErrorNoCtxf("read jemalloc arena %d stats: %d", arena, int(cErr))
	}
	return MemoryCacheStats{
		Allocated: uint64(nativeStats.allocated),
		Active:    uint64(nativeStats.active),
		Metadata:  uint64(nativeStats.metadata),
		Resident:  uint64(nativeStats.resident),
		Mapped:    uint64(nativeStats.mapped),
		Retained:  uint64(nativeStats.retained),
		Dirty:     uint64(nativeStats.dirty),
		Muzzy:     uint64(nativeStats.muzzy),
	}, nil
}

// Reclaim purges this arena's unused dirty pages. Callers use it only after an
// explicit cache-eviction boundary so normal cache turnover remains hot.
func (j *JemallocAllocator) Reclaim() error {
	arena, err := j.arena.retain(true)
	if err != nil {
		return err
	}
	defer func() {
		if releaseErr := j.arena.release(); releaseErr != nil {
			panic(releaseErr)
		}
	}()
	if cErr := C.mo_jemalloc_arena_purge(C.uint(arena)); cErr != 0 {
		return moerr.NewInternalErrorNoCtxf("purge jemalloc arena %d: %d", arena, int(cErr))
	}
	return nil
}

// Close prevents new allocations from this cache generation. The native arena
// is destroyed after the last allocation's deallocator runs.
func (j *JemallocAllocator) Close() error {
	return j.arena.close()
}

// nativeResident reads jemalloc's arena resident statistic directly. It keeps
// the allocator metric aligned with jemalloc's resident definition rather than
// reconstructing it from individual page-state counters.
func (j *JemallocAllocator) nativeResident() (residentValue uint64, err error) {
	arena, err := j.arena.retain(true)
	if err != nil {
		return 0, err
	}
	defer func() {
		if releaseErr := j.arena.release(); err == nil && releaseErr != nil {
			err = releaseErr
		}
	}()
	var resident C.size_t
	if cErr := C.mo_jemalloc_arena_resident(C.uint(arena), &resident); cErr != 0 {
		return 0, moerr.NewInternalErrorNoCtxf("read jemalloc arena %d resident: %d", arena, int(cErr))
	}
	return uint64(resident), nil
}
