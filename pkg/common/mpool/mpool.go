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
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/stack"
)

// Mo's extremely simple memory pool.

// Stats
type MPoolStats struct {
	NumAlloc      atomic.Int64 // number of allocations
	NumFree       atomic.Int64 // number of frees
	NumGoAlloc    atomic.Int64 // number of go runtime alloc
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
	ret += fmt.Sprintf("%s go runtime allocations : %d\n", tab, s.NumGoAlloc.Load())
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
	ret += fmt.Sprintf("\"goalloc\": %d,", s.NumGoAlloc.Load())
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
	NumFixedPool = 7
	kMemHdrSz    = 16
	B            = 1
	KB           = 1024
	MB           = 1024 * KB
	GB           = 1024 * MB
	TB           = 1024 * GB
	PB           = 1024 * TB
)

// Fixed pool configurations.  Number of entries in fixed pool.
//
// Small: esp, 0 is a valid value.
var NoFixed = []int{0, 0, 0, 0, 0, 0, 0}
var Small = []int{1024, 1024, 1024, 1024, 512, 0, 0}
var Mid = []int{4096, 4096, 1024, 1024, 1024, 512, 512}
var Large = []int{32 * 1024, 16 * 1024, 8 * 1024, 4 * 1024, 1024, 1024, 1024}

// Pool emement size
var PoolElemSize = []int{64, 128, 256, 512, 1024, 2048, 4096}

// Zeros, enough for largest pool element
var ZeroSlice = make([]byte, 4096)

// Memory header, kMemHdrSz bytes.
type memHdr struct {
	poolId       int64
	allocSz      int32
	fixedPoolIdx int8
	guard        [3]uint8
}

func (pHdr *memHdr) SetGuard() {
	pHdr.guard[0] = 0xDE
	pHdr.guard[1] = 0xAD
	pHdr.guard[2] = 0xBF
}

func (pHdr *memHdr) CheckGuard() bool {
	return pHdr.guard[0] == 0xDE && pHdr.guard[1] == 0xAD && pHdr.guard[2] == 0xBF
}

// pool for fixed elements.  Note that we preconfigure the pool size.
// We should consider implement some kind of growing logic.
type fixedPool struct {
	eleSz  int
	eleCnt int
	stats  MPoolStats
	buf    []uint64
	flist  *freelist
}

// Initaialze a fixed pool
func (fp *fixedPool) initPool(tag string, poolid int64, idx int, eleCnt int, cap int64) (int64, error) {
	eleSz := PoolElemSize[idx]
	fp.eleSz = eleSz
	fp.eleCnt = eleCnt

	nb := (kMemHdrSz + eleSz) * eleCnt
	if nb == 0 {
		return 0, nil
	}

	if int64(nb) >= cap {
		return 0, moerr.NewInternalErrorNoCtx("initPool failed, not enough space %d < %d", nb, cap)
	}

	// The poll, is considered allocated, so do accouting with global stats.
	curr := globalStats.RecordAlloc(tag, int64(nb))
	if curr > GlobalCap() {
		// OOM, return nb back to globalStats
		globalStats.RecordFree(tag, int64(nb))
		return 0, moerr.NewOOMNoCtx()
	}

	fp.flist = make_freelist(int32(eleCnt))

	// Really allocate buffer, and put hdr of each slot into pool.
	// nb is always 8x,
	fp.buf = make([]uint64, nb/8)
	ptr := unsafe.Pointer(&fp.buf[0])

	for i := 0; i < eleCnt; i++ {
		offset := (kMemHdrSz + eleSz) * i
		hdr := unsafe.Add(ptr, offset)
		pHdr := (*memHdr)(hdr)
		pHdr.poolId = poolid
		pHdr.fixedPoolIdx = int8(idx)
		pHdr.allocSz = -1
		pHdr.SetGuard()

		fp.flist.put(hdr)
	}
	return int64(nb), nil
}

/*
func (fp *fixedPool) destroy() {
	// not necessary, but doing it anyway
	// original here to maintain stats.  All relavent stats are maintained
	// in MPool itself.
	fp.flist.destroy()
	fp.buf = nil
}
*/

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
	id      int64      // mpool generated, used to look up the MPool
	tag     string     // user supplied, for debug/inspect
	cap     int64      // pool capacity
	stats   MPoolStats // stats
	pools   [7]fixedPool
	sels    *sync.Pool // weirdness, keep old API but this should go away.
	details *mpoolDetails
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

func (mp *MPool) FixedPoolStats(i int) *MPoolStats {
	if i < 0 || i > NumFixedPool {
		panic(moerr.NewInternalErrorNoCtx("accessing stats of %d-th fixed pool", i))
	}
	return &mp.pools[i].stats
}

func (mp *MPool) initPool(sz []int) error {
	var tot int64
	cap := mp.Cap()
	for i, cnt := range sz {
		nb, err := mp.pools[i].initPool(mp.tag, mp.id, i, cnt, cap-tot)
		if err != nil {
			return err
		} else if nb > 0 {
			mp.stats.RecordAlloc(mp.tag, nb)
		}
		tot += nb
	}
	return nil
}

func (mp *MPool) destroy() {
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

// For test.
func MustNewZero() *MPool {
	return MustNewZeroWithTag("zero_fixed_mp_for_test")
}

func MustNewZeroWithTag(tag string) *MPool {
	mp, err := NewMPool(tag, 0, NoFixed)
	if err != nil {
		panic(err)
	}
	return mp
}

// New a MPool.   Tag is user supplied, used for debugging/diagnostics.
func NewMPool(tag string, cap int64, sz []int) (*MPool, error) {
	if len(sz) != NumFixedPool {
		return nil, moerr.NewInternalErrorNoCtx("invalid mpool size config")
	}

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
	err := mp.initPool(sz)
	if err != nil {
		mp.destroy()
		return nil, err
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

func (mp *MPool) Report() string {
	ret := fmt.Sprintf("    mpool stats: %s", mp.Stats().Report("        "))
	for i := range mp.pools {
		ret += fmt.Sprintf("        fixed pool %d stats: %s", i, mp.FixedPoolStats(i).Report("            "))
	}
	return ret
}

func (mp *MPool) ReportJson() string {
	ss := mp.stats.ReportJson()
	if ss == "" {
		return fmt.Sprintf("{\"%s\": \"\"}", mp.tag)
	}
	ret := fmt.Sprintf("{\"%s\": %s", mp.tag, ss)
	for i := range mp.pools {
		ps := mp.FixedPoolStats(i).ReportJson()
		if ps != "" {
			ret += fmt.Sprintf(",\n \"Fixed-%d\": %s", i, ps)
		}
	}

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

func InitCap(cap int64) {
	if cap < GB {
		globalCap = GB
	} else {
		globalCap = cap
	}
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
		if int(size) <= sz {
			return i
		}
	}
	return NumFixedPool
}

func (fp *fixedPool) alloc(sz int) []byte {
	if fp.eleCnt == 0 {
		return nil
	}
	// We have already done the accounting when we init the fixed pool
	// so we don't do any global, or mpool level accounting.
	hdr := fp.flist.get()
	if hdr != nil {
		// alloc from fixed pool, record alloc bytes
		fp.stats.RecordAlloc("", int64(sz))
	} else {
		// failure to alloc, record this stats
		fp.stats.NumGoAlloc.Add(1)
		return nil
	}

	pHdr := (*memHdr)(hdr)
	pHdr.allocSz = int32(sz)
	bPtr := (*byte)(unsafe.Add(hdr, kMemHdrSz))

	// Keep cap as best as we can
	bs := unsafe.Slice(bPtr, fp.eleSz)
	// zero the content
	copy(bs, ZeroSlice)
	return bs[:sz]
}

func (mp *MPool) Alloc(sz int) ([]byte, error) {
	if sz < 0 || sz > GB {
		return nil, moerr.NewInternalErrorNoCtx("Invalid alloc size %d", sz)
	}

	if sz == 0 {
		// Alloc size of 0, return nil instead of a []byte{}.  Otherwise,
		// later when we try to free, we will not be able to get a[0]
		return nil, nil
	}

	idx := sizeToIdx(sz)
	if idx < NumFixedPool {
		bs := mp.pools[idx].alloc(sz)
		if bs != nil {
			return bs, nil
		}
	}

	// fallback to go alloc, first, check we are under cap
	gcurr := globalStats.RecordAlloc("global", int64(sz))
	if gcurr > GlobalCap() {
		globalStats.RecordFree("global", int64(sz))
		return nil, moerr.NewOOMNoCtx()
	}

	// check if it is under my cap
	mycurr := mp.stats.RecordAlloc(mp.tag, int64(sz))
	if mycurr > mp.Cap() {
		mp.stats.RecordFree(mp.tag, int64(sz))
		return nil, moerr.NewInternalErrorNoCtx("mpool out of space, alloc %d bytes, cap %d", sz, mp.cap)
	}

	if mp.details != nil {
		mp.details.recordAlloc(int64(sz))
	}

	// allocate!
	bs := make([]uint64, (sz+kMemHdrSz+7)/8)
	hdr := unsafe.Pointer(&bs[0])
	pHdr := (*memHdr)(hdr)
	pHdr.poolId = mp.id
	pHdr.fixedPoolIdx = NumFixedPool
	pHdr.allocSz = int32(sz)
	pHdr.SetGuard()

	return unsafe.Slice((*byte)(unsafe.Add(hdr, kMemHdrSz)), sz), nil
}

func (fp *fixedPool) free(hdr unsafe.Pointer) {
	pHdr := (*memHdr)(hdr)
	fp.stats.RecordFree("", int64(pHdr.allocSz))

	if pHdr.allocSz == -1 {
		// double free.
		panic(moerr.NewInternalErrorNoCtx("free size -1, possible double free"))
	}
	pHdr.allocSz = -1
	fp.flist.put(hdr)
}

func (mp *MPool) Free(bs []byte) {
	if bs == nil || cap(bs) == 0 {
		// free nil is OK.
		return
	}

	bs = bs[:1]
	pb := (unsafe.Pointer)(&bs[0])
	offset := -kMemHdrSz
	hdr := unsafe.Add(pb, offset)
	pHdr := (*memHdr)(hdr)

	if !pHdr.CheckGuard() {
		panic(moerr.NewInternalErrorNoCtx("mp header corruption"))
	}

	if pHdr.poolId == mp.id {
		if pHdr.fixedPoolIdx < NumFixedPool {
			mp.pools[pHdr.fixedPoolIdx].free(hdr)
		} else {
			if pHdr.allocSz == -1 {
				// double free.
				panic(moerr.NewInternalErrorNoCtx("free size -1, possible double free"))
			}
			if mp.details != nil {
				mp.details.recordFree(int64(pHdr.allocSz))
			}
			mp.stats.RecordFree(mp.tag, int64(pHdr.allocSz))
			globalStats.RecordFree(mp.tag, int64(pHdr.allocSz))
			pHdr.allocSz = -1
		}
	} else {
		// cross pool free.
		otherPool, ok := globalPools.Load(pHdr.poolId)
		if !ok {
			panic(moerr.NewInternalErrorNoCtx("invalid mpool id %d", pHdr.poolId))
		}
		(otherPool.(*MPool)).Free(bs)
	}
}

func (mp *MPool) Realloc(old []byte, sz int) ([]byte, error) {
	if sz <= cap(old) {
		return old[:sz], nil
	}
	ret, err := mp.Alloc(sz)
	if err != nil {
		return ret, err
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

// Grow is like Realloc but we try to be a little bit more aggressive on growing
// the slice.
func (mp *MPool) Grow(old []byte, sz int) ([]byte, error) {
	if sz < len(old) {
		return nil, moerr.NewInternalErrorNoCtx("mpool grow actually shrinks, %d, %d", len(old), sz)
	}
	if sz <= cap(old) {
		return old[:sz], nil
	}

	// copy-paste go slice's grow strategy
	newcap := cap(old)
	doublecap := newcap + newcap
	if sz > doublecap {
		newcap = sz
	} else {
		const threshold = 256
		if newcap < threshold {
			newcap = doublecap
		} else {
			for 0 < newcap && newcap < sz {
				newcap += (newcap + 3*threshold) / 4
			}
			if newcap <= 0 {
				newcap = sz
			}
		}
	}
	newcap = roundupsize(newcap)

	ret, err := mp.Realloc(old, newcap)
	if err != nil {
		return ret, err
	}
	return ret[:sz], nil
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

func (mp *MPool) PutSels(sels []int64) {
	mp.sels.Put(&sels)
}

func (mp *MPool) GetSels() []int64 {
	ss := mp.sels.Get().(*[]int64)
	return (*ss)[:0]
}

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
