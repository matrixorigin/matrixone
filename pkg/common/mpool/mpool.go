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

package mpool

import (
	"fmt"
	"math"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/stack"
)

// Mo's extremely simple memory pool.
// Stats
type MPoolStats struct {
	NumAlloc         atomic.Int64 // number of allocations
	NumFree          atomic.Int64 // number of frees
	NumAllocBytes    atomic.Int64 // number of bytes allocated
	NumFreeBytes     atomic.Int64 // number of bytes freed
	NumCurrBytes     atomic.Int64 // current number of bytes
	NumCrossPoolFree atomic.Int64 // number of cross pool free
	HighWaterMark    atomic.Int64 // high water mark

	// xpool frees are really bugs.  we always record them for debugging.
	mu        sync.Mutex
	xpoolFree map[string]detailInfo
}

func (s *MPoolStats) Init() {
	s.xpoolFree = make(map[string]detailInfo)
}

func (s *MPoolStats) Report(tab string) string {
	if s.HighWaterMark.Load() == 0 {
		// empty, reduce noise.
		return ""
	}

	ret := ""
	ret += fmt.Sprintf("%s allocations : %d\n", tab, s.NumAlloc.Load())
	ret += fmt.Sprintf("%s frees : %d\n", tab, s.NumFree.Load())
	ret += fmt.Sprintf("%s alloc bytes : %d\n", tab, s.NumAllocBytes.Load())
	ret += fmt.Sprintf("%s free bytes : %d\n", tab, s.NumFreeBytes.Load())
	ret += fmt.Sprintf("%s current bytes : %d\n", tab, s.NumCurrBytes.Load())
	ret += fmt.Sprintf("%s cross pool frees : %d\n", tab, s.NumCrossPoolFree.Load())
	ret += fmt.Sprintf("%s high water mark : %d\n", tab, s.HighWaterMark.Load())
	return ret
}

func (s *MPoolStats) ReportJson() string {
	if s.HighWaterMark.Load() == 0 {
		return ""
	}

	ret := "{"
	ret += fmt.Sprintf("\"alloc\": %d,", s.NumAlloc.Load())
	ret += fmt.Sprintf("\"free\": %d,", s.NumFree.Load())
	ret += fmt.Sprintf("\"allocBytes\": %d,", s.NumAllocBytes.Load())
	ret += fmt.Sprintf("\"freeBytes\": %d,", s.NumFreeBytes.Load())
	ret += fmt.Sprintf("\"currBytes\": %d,", s.NumCurrBytes.Load())
	ret += fmt.Sprintf("\"crossPoolFrees\": %d,", s.NumCrossPoolFree.Load())
	ret += fmt.Sprintf("\"highWaterMark\": %d", s.HighWaterMark.Load())
	ret += "}"
	return ret
}

// Update alloc stats, return curr bytes
func (s *MPoolStats) RecordAlloc(tag string, sz int64) int64 {
	s.NumAlloc.Add(1)
	s.NumAllocBytes.Add(sz)
	curr := s.NumCurrBytes.Add(sz)
	hwm := s.HighWaterMark.Load()
	if curr > hwm {
		swapped := s.HighWaterMark.CompareAndSwap(hwm, curr)
		if swapped && curr/GB != hwm/GB {
			logutil.Infof("MPool %s new high watermark\n%s", tag, s.Report("    "))
		}
	}
	return curr
}

// Update free stats, return curr bytes.
func (s *MPoolStats) RecordFree(tag string, sz int64) int64 {
	if sz < 0 {
		logutil.Errorf("Mpool %s free bug, stats: %s", tag, s.Report("    "))
		panic(moerr.NewInternalErrorNoCtx("mpool freed -1"))
	}
	s.NumFree.Add(1)
	s.NumFreeBytes.Add(sz)
	curr := s.NumCurrBytes.Add(-sz)
	if curr < 0 {
		logutil.Errorf("Mpool %s free bug, stats: %s", tag, s.Report("    "))
		panic(moerr.NewInternalErrorNoCtx("mpool freed more bytes than alloc"))
	}
	return curr
}

func (s *MPoolStats) RecordXPoolFree(detail string, nb int64) {
	s.NumCrossPoolFree.Add(1)

	s.mu.Lock()
	defer s.mu.Unlock()
	info := s.xpoolFree[detail]
	info.cnt += 1
	info.bytes += nb
	s.xpoolFree[detail] = info
}

func (s *MPoolStats) RecordManyFrees(tag string, nfree, sz int64) int64 {
	if sz < 0 {
		logutil.Errorf("Mpool %s free bug, stats: %s", tag, s.Report("    "))
		panic(moerr.NewInternalErrorNoCtx("mpool freed -1"))
	}
	s.NumFree.Add(nfree)
	s.NumFreeBytes.Add(sz)
	curr := s.NumCurrBytes.Add(-sz)
	if curr < 0 {
		logutil.Errorf("Mpool %s free many bug, stats: %s", tag, s.Report("    "))
		panic(moerr.NewInternalErrorNoCtx("mpool freemany freed more bytes than alloc"))
	}
	return curr
}

const (
	kMemHdrSz = 16
)

const (
	B  = 1
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB
	TB = 1024 * GB
	PB = 1024 * TB
)

// Memory header, kMemHdrSz bytes.
type memHdr struct {
	poolId  int64
	allocSz int32
	guard   [3]uint8
	offHeap bool
}

func init() {
	if unsafe.Sizeof(memHdr{}) != kMemHdrSz {
		panic("memory header size assertion failed")
	}
}

func (pHdr *memHdr) SetGuard() {
	pHdr.guard[0] = 0xDE
	pHdr.guard[1] = 0xAD
	pHdr.guard[2] = 0xBF
}

func (pHdr *memHdr) CheckGuard() bool {
	return pHdr.guard[0] == 0xDE && pHdr.guard[1] == 0xAD && pHdr.guard[2] == 0xBF
}

type detailInfo struct {
	cnt, bytes int64
}

type mpoolDetails struct {
	mu    sync.Mutex
	alloc map[string]detailInfo
	free  map[string]detailInfo
}

func newMpoolDetails() *mpoolDetails {
	mpd := mpoolDetails{}
	mpd.alloc = make(map[string]detailInfo)
	mpd.free = make(map[string]detailInfo)
	return &mpd
}

func (d *mpoolDetails) recordAlloc(k string, nb int64) {
	if d == nil {
		return
	}
	d.mu.Lock()

	defer d.mu.Unlock()

	info := d.alloc[k]
	info.cnt += 1
	info.bytes += nb
	d.alloc[k] = info
}

func (d *mpoolDetails) recordFree(k string, nb int64) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	info := d.free[k]
	info.cnt += 1
	info.bytes += nb
	d.free[k] = info
}

func (d *mpoolDetails) reportJson() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	ret := `{"alloc": {`
	allocs := make([]string, 0)
	for k, v := range d.alloc {
		kvs := fmt.Sprintf("\"%s\": [%d, %d]", k, v.cnt, v.bytes)
		allocs = append(allocs, kvs)
	}
	ret += strings.Join(allocs, ",")
	ret += `}, "free": {`
	frees := make([]string, 0)
	for k, v := range d.free {
		kvs := fmt.Sprintf("\"%s\": [%d, %d]", k, v.cnt, v.bytes)
		frees = append(frees, kvs)
	}
	ret += strings.Join(frees, ",")
	ret += "}}"
	return ret
}

// The memory pool.
type MPool struct {
	id      int64      // mpool generated, used to look up the MPool
	tag     string     // user supplied, for debug/inspect
	cap     int64      // pool capacity
	stats   MPoolStats // stats
	details *mpoolDetails

	noLock bool
	ptrs   map[unsafe.Pointer]memHdr
}

const (
	NoFixed = 1 << iota
	NoLock
)

func (mp *MPool) recordPtrHdr(ptr unsafe.Pointer, pHdr memHdr) error {
	if !mp.noLock {
		return gRecordPtr(ptr, pHdr)
	} else {
		_, ok := mp.ptrs[ptr]
		if ok {
			return moerr.NewInternalErrorNoCtx("ptr already recorded")
		}
		mp.ptrs[ptr] = pHdr
		return nil
	}
}
func (mp *MPool) removePtrHdr(ptr unsafe.Pointer) (memHdr, bool) {
	if !mp.noLock {
		return gRemovePtr(ptr)
	} else {
		hdr, ok := mp.ptrs[ptr]
		delete(mp.ptrs, ptr)
		return hdr, ok
	}
}

func (mp *MPool) deallocateAllPtrs() {
	for ptr, hdr := range mp.ptrs {
		allocateSize := int(hdr.allocSz) + kMemHdrSz
		simpleCAllocator().Deallocate(unsafe.Slice((*byte)(ptr), allocateSize), uint64(allocateSize))
	}
	mp.ptrs = nil
}

func (mp *MPool) EnableDetailRecording() {
	if mp.details == nil {
		mp.details = newMpoolDetails()
	}
}

func (mp *MPool) DisableDetailRecording() {
	mp.details = nil
}

func (mp *MPool) getDetailK() string {
	if mp == nil || mp.details == nil {
		return ""
	}
	f := stack.Caller(2)
	k := fmt.Sprintf("%v:%n", f, f)
	return k
}

func (mp *MPool) Stats() *MPoolStats {
	return &mp.stats
}

func (mp *MPool) Cap() int64 {
	if mp.cap == 0 {
		return common.PiB
	}
	return mp.cap
}

const (
	xxxIWouldRatherUseAfterFreeCrashLaterThanLeak = true
)

func (mp *MPool) destroy() {
	if mp.stats.NumAlloc.Load() < mp.stats.NumFree.Load() {
		// this is a memory leak,
		logutil.Errorf("mp error: %s", mp.stats.Report(""))

		// here we MUST free all the memories allocated by this mpool.
		// otherwise it is a memory leak.  Whoever still holds
		// a pointer of this mpool is a bug (the cross pool case).
		//
		// We are so messed up because the cross pool free.
		// If a pointer is handed out to someone else and we free here
		// it will be a use after free.   We risk a crash or a leak.
		// Eitherway we are screwed.
		if xxxIWouldRatherUseAfterFreeCrashLaterThanLeak {
			mp.deallocateAllPtrs()
		}
	}

	// Here we just compensate whatever left over in mp.stats
	// into globalStats.
	globalStats.RecordManyFrees(mp.tag,
		mp.stats.NumAlloc.Load()-mp.stats.NumFree.Load(),
		mp.stats.NumCurrBytes.Load())
}

// New a MPool.   Tag is user supplied, used for debugging/diagnostics.
func NewMPool(tag string, cap int64, flag int) (*MPool, error) {
	start := time.Now()
	defer func() {
		v2.TxnMpoolNewDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	if cap > 0 {
		// simple sanity check
		if cap < 1024*1024 {
			return nil, moerr.NewInternalErrorNoCtxf("mpool cap %d too small", cap)
		}
		if cap > GlobalCap() {
			return nil, moerr.NewInternalErrorNoCtxf("mpool cap %d too big, global cap %d", cap, globalCap.Load())
		}
	}

	noLock := flag&NoLock != 0

	id := atomic.AddInt64(&nextPool, 1)
	var mp MPool
	mp.id = id
	mp.tag = tag
	mp.cap = cap
	mp.noLock = noLock

	mp.stats.Init()
	mp.ptrs = make(map[unsafe.Pointer]memHdr)
	globalPools.Store(id, &mp)
	return &mp, nil
}

func MustNew(tag string) *MPool {
	mp, err := NewMPool(tag, 0, NoFixed)
	if err != nil {
		panic(err)
	}
	return mp
}

func MustNewNoLock(tag string) *MPool {
	mp, err := NewMPool(tag, 0, NoLock)
	if err != nil {
		panic(err)
	}
	return mp
}

func MustNewZero() *MPool {
	return MustNew("must_new_zero")
}

func MustNewNoFixed() *MPool {
	return MustNew("must_new_no_fixed")
}

func MustNewZeroNoFixed() *MPool {
	return MustNew("must_new_zero_no_fixed")
}

func (mp *MPool) ReportJson() string {
	ss := mp.stats.ReportJson()
	if ss == "" {
		return fmt.Sprintf("{\"%s\": \"\"}", mp.tag)
	}
	ret := fmt.Sprintf("{\"%s\": %s", mp.tag, ss)
	if mp.details != nil {
		ret += `,\n "detailed_alloc": `
		ret += mp.details.reportJson()
	}

	return ret + "}"
}

func (mp *MPool) CurrNB() int64 {
	return mp.stats.NumCurrBytes.Load()
}

func DeleteMPool(mp *MPool) {
	start := time.Now()
	defer func() {
		v2.TxnMpoolDeleteDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	if mp == nil {
		return
	}

	// logutil.Infof("destroy mpool %s, cap %d, stats\n%s", mp.tag, mp.cap, mp.Report())
	mp.destroy()
	globalPools.Delete(mp.id)
}

var nextPool int64
var globalCap atomic.Int64
var globalStats MPoolStats
var globalPools sync.Map
var globalPtrs map[unsafe.Pointer]memHdr
var globalPtrsMu sync.Mutex

func InitCap(cap int64) {
	if cap < GB {
		globalCap.Store(GB)
	} else {
		globalCap.Store(cap)
	}
}

func GlobalStats() *MPoolStats {
	return &globalStats
}

func GlobalCap() int64 {
	n := globalCap.Load()
	if n == 0 {
		return PB
	}
	return n
}

var CapLimit = math.MaxInt32 // 2GB - 1

func (mp *MPool) Alloc(sz int, offHeap bool) ([]byte, error) {
	detailk := mp.getDetailK()
	return mp.allocWithDetailK(detailk, int64(sz), offHeap)
}

func (mp *MPool) allocWithDetailK(detailk string, sz int64, offHeap bool) ([]byte, error) {
	// reject unexpected alloc size.
	if sz < 0 || sz+kMemHdrSz > int64(CapLimit) {
		logutil.Errorf("mpool memory allocation exceed limit with requested size %d: %s", sz, string(debug.Stack()))
		return nil, moerr.NewInternalErrorNoCtxf("mpool memory allocation exceed limit with requested size %d", sz)
	}
	if sz == 0 {
		return nil, nil
	}
	return mp.alloc(detailk, sz, offHeap)
}

func (mp *MPool) alloc(detailk string, sz int64, offHeap bool) ([]byte, error) {
	var bs []byte
	var err error

	hdr := memHdr{
		poolId:  mp.id,
		allocSz: int32(sz),
		offHeap: offHeap,
	}
	hdr.SetGuard()

	if offHeap {
		gcurr := globalStats.RecordAlloc("global", sz)
		if gcurr > GlobalCap() {
			// compensate global
			globalStats.RecordFree("global", sz)
			return nil, moerr.NewOOMNoCtx()
		}
		mycurr := mp.stats.RecordAlloc(mp.tag, sz)
		if mycurr > mp.Cap() {
			// compensate both global and my
			mp.stats.RecordFree(mp.tag, sz)
			globalStats.RecordFree("global", sz)
			return nil, moerr.NewInternalErrorNoCtxf("mpool out of space, alloc %d bytes, cap %d", sz, mp.cap)
		}
		bs, err = simpleCAllocator().Allocate(uint64(sz))
		if err != nil {
			panic(err)
		}
		if mp.details != nil {
			mp.details.recordAlloc(detailk, sz)
		}
	} else {
		bs = make([]byte, sz)
	}

	// always record the ptr, offHeap or not.
	mp.recordPtrHdr(unsafe.Pointer(&bs[0]), hdr)
	return bs, nil
}

func (mp *MPool) Free(bs []byte) {
	detailk := mp.getDetailK()
	mp.freeWithDetailK(detailk, bs)
}

func (mp *MPool) freeWithDetailK(detailk string, bs []byte) {
	if bs == nil || cap(bs) == 0 {
		return
	}
	bs = bs[:1]
	ptr := unsafe.Pointer(&bs[0])
	mp.freePtr(detailk, ptr)
}

func (mp *MPool) freePtr(detailk string, ptr unsafe.Pointer) {
	hdr, ok := mp.removePtrHdr(ptr)
	if !ok {
		// this is a double free.
		panic(moerr.NewInternalErrorNoCtx("invalid ptr, double free"))
	}

	if hdr.poolId != mp.id {
		sz := int64(hdr.allocSz)
		mp.stats.RecordXPoolFree(detailk, sz)
		globalStats.RecordXPoolFree(detailk, sz)
		return
	}

	if !hdr.offHeap {
		return
	}
	sz := int64(hdr.allocSz)
	mp.stats.RecordFree(mp.tag, sz)
	globalStats.RecordFree("global", sz)
	if mp.details != nil {
		mp.details.recordFree(detailk, sz)
	}

	simpleCAllocator().Deallocate(unsafe.Slice((*byte)(ptr), sz), uint64(sz))
}

func (mp *MPool) reAllocWithDetailK(detailk string, old []byte, sz int64, offHeap bool, bufferMore bool) ([]byte, error) {
	if sz <= int64(cap(old)) {
		return old[:sz], nil
	}

	newSz := sz
	if bufferMore {
		newSz = calculateNewCap(int64(cap(old)), sz)
	}

	ret, err := mp.allocWithDetailK(detailk, int64(newSz), offHeap)
	if err != nil {
		return nil, err
	}
	copy(ret, old)
	mp.freeWithDetailK(detailk, old)
	return ret[:sz], nil
}

func (mp *MPool) Grow(old []byte, sz int, offHeap bool) ([]byte, error) {
	detailk := mp.getDetailK()
	return mp.reAllocWithDetailK(detailk, old, int64(sz), offHeap, true)
}

func (mp *MPool) Grow2(old []byte, old2 []byte, sz int, offHeap bool) ([]byte, error) {
	len1 := len(old)
	len2 := len(old2)
	if sz < len1+len2 {
		return nil, moerr.NewInternalErrorNoCtxf("mpool grow2 actually shrinks, %d+%d, %d", len1, len2, sz)
	}
	detailk := mp.getDetailK()
	ret, err := mp.reAllocWithDetailK(detailk, old, int64(sz), offHeap, true)
	if err != nil {
		return nil, err
	}

	copy(ret[len1:len1+len2], old2)
	return ret, nil
}

// ReallocZero is like Realloc, but it clears the memory.
func (mp *MPool) ReallocZero(old []byte, sz int, offHeap bool) ([]byte, error) {
	detailk := mp.getDetailK()
	if cap(old) == 0 {
		return mp.allocWithDetailK(detailk, int64(sz), offHeap)
	}

	if !offHeap {
		return mp.reAllocWithDetailK(detailk, old, int64(sz), offHeap, false)
	}

	oldsz := len(old)
	oldcap := cap(old)
	if sz <= oldsz {
		return old[:sz], nil
	} else if sz <= oldcap {
		old = old[:sz]
		clear(old[oldsz:])
		return old, nil
	}

	oldptr := unsafe.Pointer(&old[0])
	newbs, err := simpleCAllocator().ReallocZero(old, uint64(sz))
	if err != nil {
		return nil, err
	}
	newptr := unsafe.Pointer(&newbs[0])
	mp.removePtrHdr(oldptr)
	mp.recordPtrHdr(newptr, memHdr{
		poolId:  mp.id,
		allocSz: int32(sz),
		offHeap: offHeap,
	})
	globalStats.RecordFree("global", int64(oldcap))
	mp.stats.RecordFree(mp.tag, int64(oldcap))
	globalStats.RecordAlloc("global", int64(sz))
	mp.stats.RecordAlloc(mp.tag, int64(sz))
	return newbs, nil
}

func makeSliceWithCapWithDetailK[T any](detailk string, n, cap int, mp *MPool, offHeap bool) ([]T, error) {
	var t T
	tsz := unsafe.Sizeof(t)
	bs, err := mp.allocWithDetailK(detailk, int64(tsz)*int64(cap), offHeap)
	if err != nil {
		return nil, err
	}
	ptr := unsafe.Pointer(&bs[0])
	tptr := (*T)(ptr)
	ret := unsafe.Slice(tptr, cap)
	return ret[:n:cap], nil
}

func MakeSlice[T any](n int, mp *MPool, offHeap bool) ([]T, error) {
	detailk := mp.getDetailK()
	return makeSliceWithCapWithDetailK[T](detailk, n, n, mp, offHeap)
}

func MakeSliceArgs[T any](mp *MPool, offHeap bool, args ...T) ([]T, error) {
	detailk := mp.getDetailK()
	ret, err := makeSliceWithCapWithDetailK[T](detailk, len(args), len(args), mp, offHeap)
	if err != nil {
		return ret, err
	}
	copy(ret, args)
	return ret, nil
}

func FreeSlice[T any](mp *MPool, bs []T) {
	if cap(bs) == 0 {
		return
	}
	detailk := mp.getDetailK()
	mp.freePtr(detailk, unsafe.Pointer(&bs[0]))
}

// Report memory usage in json.
func ReportMemUsage(tag string) string {
	gstat := fmt.Sprintf("{\"global\":%s}", globalStats.ReportJson())
	if tag == "global" {
		return "[" + gstat + "]"
	}

	var poolStats []string
	if tag == "" {
		poolStats = append(poolStats, gstat)
	}

	gather := func(key, value any) bool {
		mp := value.(*MPool)
		if tag == "" || tag == mp.tag {
			poolStats = append(poolStats, mp.ReportJson())
		}
		return true
	}
	globalPools.Range(gather)

	return "[" + strings.Join(poolStats, ",") + "]"
}

func MPoolControl(tag string, cmd string) string {
	if tag == "" || tag == "global" {
		return "Cannot enable detail on mpool global stats"
	}

	cmdFunc := func(key, value any) bool {
		mp := value.(*MPool)
		if tag == mp.tag {
			switch cmd {
			case "enable_detail":
				mp.EnableDetailRecording()
			case "disable_detail":
				mp.DisableDetailRecording()
			}
		}
		return true
	}

	globalPools.Range(cmdFunc)
	return "ok"
}

var simpleCAllocator = sync.OnceValue(func() *malloc.SimpleCAllocator {
	sca := malloc.NewSimpleCAllocator(
		v2.MallocCounter.WithLabelValues("mpool-allocate"),
		v2.MallocGauge.WithLabelValues("mpool-inuse"),
		v2.MallocCounter.WithLabelValues("mpool-allocate-objects"),
		v2.MallocGauge.WithLabelValues("mpool-inuse-objects"),
	)
	return sca
})

func init() {
	globalStats.Init()
	globalPtrs = make(map[unsafe.Pointer]memHdr)
}

func gRecordPtr(ptr unsafe.Pointer, hdr memHdr) error {
	globalPtrsMu.Lock()
	defer globalPtrsMu.Unlock()
	_, ok := globalPtrs[ptr]
	if ok {
		return moerr.NewInternalErrorNoCtx("ptr already recorded")
	}
	globalPtrs[ptr] = hdr
	return nil
}

func gRemovePtr(ptr unsafe.Pointer) (memHdr, bool) {
	globalPtrsMu.Lock()
	defer globalPtrsMu.Unlock()
	hdr, ok := globalPtrs[ptr]
	delete(globalPtrs, ptr)
	return hdr, ok
}

// alignUp rounds n up to a multiple of a. a must be a power of 2.
func alignUp(n, a int64) int64 {
	return (n + a - 1) &^ (a - 1)
}

// divRoundUp returns ceil(n / a).
func divRoundUp(n, a int64) int64 {
	// a is generally a power of two. This will get inlined and
	// the compiler will optimize the division.
	return (n + a - 1) / a
}

// Returns size of the memory block that mallocgc will allocate if you ask for the size.
func roundupsize(size int64) int64 {
	if size < _MaxSmallSize {
		if size <= smallSizeMax-8 {
			return int64(class_to_size[size_to_class8[divRoundUp(size, smallSizeDiv)]])
		} else {
			return int64(class_to_size[size_to_class128[divRoundUp(size-smallSizeMax, largeSizeDiv)]])
		}
	}
	if size+_PageSize < size {
		return size
	}
	return alignUp(size, _PageSize)
}

// copy-paste from go slice grow strategy.
func calculateNewCap(oldCap int64, requiredSize int64) int64 {
	newcap := oldCap
	doublecap := newcap + newcap
	if requiredSize > doublecap {
		newcap = requiredSize
	} else {
		// performance: use a larger threshold (256 -> 4096)
		const threshold = 4096
		if newcap < threshold {
			newcap = doublecap
		} else {
			for 0 < newcap && newcap < requiredSize {
				newcap += (newcap + 3*threshold) / 4
			}
			if newcap <= 0 {
				newcap = requiredSize
			}
		}
	}
	newcap = roundupsize(newcap)
	if newcap > int64(CapLimit) && requiredSize <= int64(CapLimit) {
		newcap = int64(CapLimit)
	}
	return newcap
}
