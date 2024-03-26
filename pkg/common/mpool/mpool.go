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
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/stack"
)

// Mo's extremely simple memory pool.

// Stats
type MPoolStats struct {
	NumAlloc      atomic.Int64 // number of allocations
	NumFree       atomic.Int64 // number of frees
	NumAllocBytes atomic.Int64 // number of bytes allocated
	NumFreeBytes  atomic.Int64 // number of bytes freed
	NumCurrBytes  atomic.Int64 // current number of bytes
	HighWaterMark atomic.Int64 // high water mark
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
	NumFixedPool = 5
	kMemHdrSz    = 16
	kStripeSize  = 128
	B            = 1
	KB           = 1024
	MB           = 1024 * KB
	GB           = 1024 * MB
	TB           = 1024 * GB
	PB           = 1024 * TB
)

// Pool emement size
var PoolElemSize = [NumFixedPool]int32{64, 128, 256, 512, 1024}

// Memory header, kMemHdrSz bytes.
type memHdr struct {
	poolId       int64
	allocSz      int32
	fixedPoolIdx int8
	guard        [3]uint8
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

func (pHdr *memHdr) ToSlice(sz, cap int) []byte {
	ptr := unsafe.Add(unsafe.Pointer(pHdr), kMemHdrSz)
	bs := unsafe.Slice((*byte)(ptr), cap)
	return bs[:sz]
}

// pool for fixed elements.  Note that we preconfigure the pool size.
// We should consider implement some kind of growing logic.
type fixedPool struct {
	m      sync.Mutex
	noLock bool
	fpIdx  int8
	poolId int64
	eleSz  int32
	// holds buffers allocated, it is not really used in alloc/free
	// but hold here for bookkeeping.
	buf   [][]byte
	flist unsafe.Pointer
}

// Initaialze a fixed pool
func (fp *fixedPool) initPool(tag string, poolid int64, idx int, noLock bool) {
	eleSz := PoolElemSize[idx]
	fp.poolId = poolid
	fp.fpIdx = int8(idx)
	fp.noLock = noLock
	fp.eleSz = eleSz
}

func (fp *fixedPool) nextPtr(ptr unsafe.Pointer) unsafe.Pointer {
	iptr := *(*unsafe.Pointer)(unsafe.Add(ptr, kMemHdrSz))
	return iptr
}
func (fp *fixedPool) setNextPtr(ptr unsafe.Pointer, next unsafe.Pointer) {
	iptr := (*unsafe.Pointer)(unsafe.Add(ptr, kMemHdrSz))
	*iptr = next
}

func (fp *fixedPool) alloc(sz int32) *memHdr {
	if !fp.noLock {
		fp.m.Lock()
		defer fp.m.Unlock()
	}

	if fp.flist == nil {
		buf := make([]byte, kStripeSize*(fp.eleSz+kMemHdrSz))
		fp.buf = append(fp.buf, buf)
		// return the first one
		ret := (unsafe.Pointer)(&buf[0])
		pHdr := (*memHdr)(ret)
		pHdr.poolId = fp.poolId
		pHdr.allocSz = sz
		pHdr.fixedPoolIdx = fp.fpIdx
		pHdr.SetGuard()

		ptr := unsafe.Add(ret, fp.eleSz+kMemHdrSz)
		// and thread the rest
		for i := 1; i < kStripeSize; i++ {
			pHdr := (*memHdr)(ptr)
			pHdr.poolId = fp.poolId
			pHdr.allocSz = -1
			pHdr.fixedPoolIdx = fp.fpIdx
			pHdr.SetGuard()
			fp.setNextPtr(ptr, fp.flist)
			fp.flist = ptr
			ptr = unsafe.Add(ptr, fp.eleSz+kMemHdrSz)
		}
		return (*memHdr)(ret)
	} else {
		ret := fp.flist
		fp.flist = fp.nextPtr(fp.flist)
		pHdr := (*memHdr)(ret)
		pHdr.allocSz = sz
		// Zero slice.  Go requires slice to be zeroed.
		bs := unsafe.Slice((*byte)(unsafe.Add(ret, kMemHdrSz)), fp.eleSz)
		// the compiler will optimize this loop to memclr
		for i := range bs {
			bs[i] = 0
		}
		return pHdr
	}
}

func (fp *fixedPool) free(hdr *memHdr) {
	if hdr.poolId != fp.poolId || hdr.fixedPoolIdx != fp.fpIdx ||
		hdr.allocSz < 0 || hdr.allocSz > fp.eleSz ||
		!hdr.CheckGuard() {
		panic(moerr.NewInternalErrorNoCtx("mpool fixed pool hdr corruption.   Possible double free"))
	}

	if !fp.noLock {
		fp.m.Lock()
		defer fp.m.Unlock()
	}
	ptr := unsafe.Pointer(hdr)
	fp.setNextPtr(ptr, fp.flist)
	fp.flist = ptr
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

func (d *mpoolDetails) recordAlloc(nb int64) {
	f := stack.Caller(2)
	k := fmt.Sprintf("%v", f)
	d.mu.Lock()
	defer d.mu.Unlock()

	info := d.alloc[k]
	info.cnt += 1
	info.bytes += nb
	d.alloc[k] = info
}

func (d *mpoolDetails) recordFree(nb int64) {
	f := stack.Caller(2)
	k := fmt.Sprintf("%v", f)
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
	id         int64      // mpool generated, used to look up the MPool
	tag        string     // user supplied, for debug/inspect
	cap        int64      // pool capacity
	stats      MPoolStats // stats
	noFixed    bool
	noLock     bool
	available  int32 // 0: available, 1: unavailable
	inUseCount int32 // number of in use call
	pools      [NumFixedPool]fixedPool
	details    *mpoolDetails

	// To remove: this thing is highly unlikely to be of any good use.
	sels *sync.Pool
}

const (
	NoFixed = 1
	NoLock  = 2
)

const (
	Available = iota
	Unavailable
)

func (mp *MPool) PutSels(sels []int64) {
	mp.sels.Put(&sels)
}
func (mp *MPool) GetSels() []int64 {
	ss := mp.sels.Get().(*[]int64)
	return (*ss)[:0]
}

func (mp *MPool) EnableDetailRecording() {
	if mp.details == nil {
		mp.details = newMpoolDetails()
	}
}

func (mp *MPool) DisableDetailRecording() {
	mp.details = nil
}

func (mp *MPool) Stats() *MPoolStats {
	return &mp.stats
}

func (mp *MPool) Cap() int64 {
	if mp.cap == 0 {
		return PB
	}
	return mp.cap
}

func (mp *MPool) destroy() {
	if atomic.LoadInt32(&mp.inUseCount) != 0 {
		logutil.Errorf("Mpool %s already in use", mp.tag)
		return
	}
	if !atomic.CompareAndSwapInt32(&mp.available, 0, 1) {
		logutil.Errorf("Mpool %s double destroy", mp.tag)
		return
	}
	if mp.stats.NumAlloc.Load() < mp.stats.NumFree.Load() {
		logutil.Errorf("mp error: %s", mp.stats.Report(""))
	}

	// We do not call each individual fixedPool's destroy
	// because they recorded pooled elements alloc/frees.
	// Those are not reflected in globalStats.
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
			return nil, moerr.NewInternalErrorNoCtx("mpool cap %d too small", cap)
		}
		if cap > GlobalCap() {
			return nil, moerr.NewInternalErrorNoCtx("mpool cap %d too big, global cap %d", cap, globalCap)
		}
	}

	id := atomic.AddInt64(&nextPool, 1)
	var mp MPool
	mp.id = id
	mp.tag = tag
	mp.cap = cap

	mp.noFixed = (flag & NoFixed) != 0
	mp.noLock = (flag & NoFixed) != 0

	if !mp.noFixed {
		for i := 0; i < NumFixedPool; i++ {
			mp.pools[i].initPool(mp.tag, mp.id, i, mp.noLock)
		}
	}

	mp.sels = &sync.Pool{
		New: func() any {
			ss := make([]int64, 0, 16)
			return &ss
		},
	}

	globalPools.Store(id, &mp)
	// logutil.Infof("creating mpool %s, cap %d, fixed size %v", tag, cap, sz)
	return &mp, nil
}

func MustNew(tag string) *MPool {
	mp, err := NewMPool(tag, 0, 0)
	if err != nil {
		panic(err)
	}
	return mp
}

func MustNewZero() *MPool {
	return MustNew("must_new_zero")
}

func MustNewNoFixed(tag string) *MPool {
	mp, err := NewMPool(tag, 0, NoFixed)
	if err != nil {
		panic(err)
	}
	return mp
}

func MustNewZeroNoFixed() *MPool {
	return MustNewNoFixed("must_new_zero_no_fixed")
}

func (mp *MPool) Report() string {
	ret := fmt.Sprintf("    mpool stats: %s", mp.Stats().Report("        "))
	return ret
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
	globalPools.Delete(mp.id)
	mp.destroy()
}

var nextPool int64
var globalCap int64
var globalStats MPoolStats
var globalPools sync.Map
var crossPoolFreeCounter atomic.Int64

func InitCap(cap int64) {
	if cap < GB {
		globalCap = GB
	} else {
		globalCap = cap
	}
}

func TotalCrossPoolFreeCounter() int64 {
	return crossPoolFreeCounter.Load()
}

func GlobalStats() *MPoolStats {
	return &globalStats
}
func GlobalCap() int64 {
	if globalCap == 0 {
		return PB
	}
	return globalCap
}

func sizeToIdx(size int) int {
	for i, sz := range PoolElemSize {
		if int32(size) <= sz {
			return i
		}
	}
	return NumFixedPool
}

func (mp *MPool) Alloc(sz int) ([]byte, error) {
	// reject unexpected alloc size.
	if sz < 0 || sz > GB {
		logutil.Errorf("Invalid alloc size %d: %s", sz, string(debug.Stack()))
		return nil, moerr.NewInternalErrorNoCtx("Invalid alloc size %d", sz)
	}

	if sz == 0 {
		return nil, nil
	}

	if atomic.LoadInt32(&mp.available) == Unavailable {
		return nil, moerr.NewInternalErrorNoCtx("mpool %s unavailable for alloc", mp.tag)
	}

	// update in use count
	atomic.AddInt32(&mp.inUseCount, 1)
	defer atomic.AddInt32(&mp.inUseCount, -1)

	idx := NumFixedPool
	requiredSpaceWithoutHeader := sz
	if !mp.noFixed {
		idx = sizeToIdx(requiredSpaceWithoutHeader)
		if idx < NumFixedPool {
			requiredSpaceWithoutHeader = int(mp.pools[idx].eleSz)
		}
	}

	tempSize := int64(requiredSpaceWithoutHeader + kMemHdrSz)
	gcurr := globalStats.RecordAlloc("global", tempSize)
	if gcurr > GlobalCap() {
		globalStats.RecordFree("global", tempSize)
		return nil, moerr.NewOOMNoCtx()
	}
	mycurr := mp.stats.RecordAlloc(mp.tag, tempSize)
	if mycurr > mp.Cap() {
		mp.stats.RecordFree(mp.tag, tempSize)
		globalStats.RecordFree("global", tempSize)
		return nil, moerr.NewInternalErrorNoCtx("mpool out of space, alloc %d bytes, cap %d", sz, mp.cap)
	}

	// from fixed pool
	if idx < NumFixedPool {
		bs := mp.pools[idx].alloc(int32(requiredSpaceWithoutHeader))
		if mp.details != nil {
			mp.details.recordAlloc(int64(bs.allocSz))
		}
		return bs.ToSlice(sz, int(mp.pools[idx].eleSz)), nil
	}

	return alloc(sz, requiredSpaceWithoutHeader, mp), nil
}

func (mp *MPool) Free(bs []byte) {
	if bs == nil || cap(bs) == 0 {
		return
	}
	bs = bs[:1]
	hdr := unsafe.Add((unsafe.Pointer)(&bs[0]), -kMemHdrSz)
	pHdr := (*memHdr)(hdr)

	if !pHdr.CheckGuard() {
		panic(moerr.NewInternalErrorNoCtx("invalid free, mp header corruption"))
	}
	if atomic.LoadInt32(&mp.available) == Unavailable {
		panic(moerr.NewInternalErrorNoCtx("mpool %s unavailable for free", mp.tag))
	}

	// if cross pool free.
	if pHdr.poolId != mp.id {
		crossPoolFreeCounter.Add(1)
		otherPool, ok := globalPools.Load(pHdr.poolId)
		if !ok {
			panic(moerr.NewInternalErrorNoCtx("invalid mpool id %d", pHdr.poolId))
		}
		(otherPool.(*MPool)).Free(bs)
		return
	}

	atomic.AddInt32(&mp.inUseCount, 1)
	defer atomic.AddInt32(&mp.inUseCount, -1)
	// double free check
	if atomic.LoadInt32(&pHdr.allocSz) == -1 {
		panic(moerr.NewInternalErrorNoCtx("free size -1, possible double free"))
	}

	recordSize := int64(pHdr.allocSz) + kMemHdrSz
	mp.stats.RecordFree(mp.tag, recordSize)
	globalStats.RecordFree("global", recordSize)
	if mp.details != nil {
		mp.details.recordFree(int64(pHdr.allocSz))
	}

	// free from fixed pool
	if pHdr.fixedPoolIdx < NumFixedPool {
		mp.pools[pHdr.fixedPoolIdx].free(pHdr)
	} else {
		// non fixed pool just mark it freed
		if !atomic.CompareAndSwapInt32(&pHdr.allocSz, pHdr.allocSz, -1) {
			panic(moerr.NewInternalErrorNoCtx("free size -1, possible double free"))
		}
	}
}

func (mp *MPool) reAlloc(old []byte, sz int) ([]byte, error) {
	if sz <= cap(old) {
		return old[:sz], nil
	}
	ret, err := mp.Alloc(sz)
	if err != nil {
		return nil, err
	}
	copy(ret, old)
	mp.Free(old)
	return ret, nil
}

// alignUp rounds n up to a multiple of a. a must be a power of 2.
func alignUp(n, a int) int {
	return (n + a - 1) &^ (a - 1)
}

// divRoundUp returns ceil(n / a).
func divRoundUp(n, a int) int {
	// a is generally a power of two. This will get inlined and
	// the compiler will optimize the division.
	return (n + a - 1) / a
}

// Returns size of the memory block that mallocgc will allocate if you ask for the size.
func roundupsize(size int) int {
	if size < _MaxSmallSize {
		if size <= smallSizeMax-8 {
			return int(class_to_size[size_to_class8[divRoundUp(size, smallSizeDiv)]])
		} else {
			return int(class_to_size[size_to_class128[divRoundUp(size-smallSizeMax, largeSizeDiv)]])
		}
	}
	if size+_PageSize < size {
		return size
	}
	return alignUp(size, _PageSize)
}

// Grow is like reAlloc, but we try to be a little bit more aggressive on growing
// the slice.
func (mp *MPool) Grow(old []byte, sz int) ([]byte, error) {
	if sz < len(old) {
		return nil, moerr.NewInternalErrorNoCtx("mpool grow actually shrinks, %d, %d", len(old), sz)
	}
	if sz <= cap(old) {
		return old[:sz], nil
	}
	newCap := calculateNewCap(cap(old), sz)

	ret, err := mp.reAlloc(old, newCap)
	if err != nil {
		return old, err
	}
	return ret[:sz], nil
}

// copy-paste from go slice grow strategy.
func calculateNewCap(oldCap int, requiredSize int) int {
	newcap := oldCap
	doublecap := newcap + newcap
	if requiredSize > doublecap {
		newcap = requiredSize
	} else {
		const threshold = 256
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
	return newcap
}

func (mp *MPool) Grow2(old []byte, old2 []byte, sz int) ([]byte, error) {
	len1 := len(old)
	len2 := len(old2)
	if sz < len1+len2 {
		return nil, moerr.NewInternalErrorNoCtx("mpool grow2 actually shrinks, %d+%d, %d", len1, len2, sz)
	}
	ret, err := mp.Grow(old, sz)
	if err != nil {
		return nil, err
	}
	copy(ret[len1:len1+len2], old2)
	return ret, nil
}

/*
func (mp *MPool) Increase(nb int64) error {
	gcurr := globalStats.RecordAlloc("global", nb)
	if gcurr > GlobalCap() {
		globalStats.RecordFree(mp.tag, nb)
		return moerr.NewOOMNoCtx()
	}

	// check if it is under my cap
	mycurr := mp.stats.RecordAlloc(mp.tag, nb)
	if mycurr > mp.Cap() {
		mp.stats.RecordFree(mp.tag, nb)
		return moerr.NewInternalErrorNoCtx("mpool out of space, alloc %d bytes, cap %d", nb, mp.cap)
	}
	return nil
}

func (mp *MPool) Decrease(nb int64) {
	mp.stats.RecordFree(mp.tag, nb)
	globalStats.RecordFree("global", nb)
}
*/

func MakeSliceWithCap[T any](n, cap int, mp *MPool) ([]T, error) {
	var t T
	tsz := unsafe.Sizeof(t)
	bs, err := mp.Alloc(int(tsz) * cap)
	if err != nil {
		return nil, err
	}
	ptr := unsafe.Pointer(&bs[0])
	tptr := (*T)(ptr)
	ret := unsafe.Slice(tptr, cap)
	return ret[:n:cap], nil
}

func MakeSlice[T any](n int, mp *MPool) ([]T, error) {
	return MakeSliceWithCap[T](n, n, mp)
}

func MakeSliceArgs[T any](mp *MPool, args ...T) ([]T, error) {
	ret, err := MakeSlice[T](len(args), mp)
	if err != nil {
		return ret, err
	}
	copy(ret, args)
	return ret, nil
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
