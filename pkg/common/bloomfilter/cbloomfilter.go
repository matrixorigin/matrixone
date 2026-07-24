// Copyright 2021 Matrix Origin
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

package bloomfilter

/*
#include <stdlib.h>
#include <string.h>
#include "../../../cgo/bloom.h"
*/
import "C"
import (
	"fmt"
	"math"
	"runtime"
	"sync/atomic"
	"unsafe"

	_ "github.com/matrixorigin/matrixone/cgo"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// CBloomFilter is a wrapper around the C implementation of a bloom filter.
type CBloomFilter struct {
	ptr    *C.bloomfilter_t
	refcnt int32
}

// MAX_K_SEED is the maximum number of hash functions allowed by the C implementation.
const MAX_K_SEED = 64

// nextPow2 returns the next power of 2 greater than or equal to v.
// If v is 0, it returns 0. If the next power of 2 overflows uint64, it also returns 0.
// nextPow2 returns the next power of 2 greater than or equal to v.
// If v is 0, it returns 0. If the next power of 2 overflows uint64, it also returns 0.
func nextPow2(v uint64) uint64 {
	if v == 0 {
		return 0
	}
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	v++
	return v
}

func ComputeMemAndHashCountC(rowCount int64, probability float64) (uint64, uint32) {
	// This is a heuristic to keep k small for performance reasons.
	// It may result in a higher false positive rate than the given probability.
	k := uint32(1)
	if rowCount < 10001 {
		k = 1
	} else if rowCount < 10_0001 {
		k = 1
	} else if rowCount < 100_0001 {
		k = 1
	} else if rowCount < 1000_0001 {
		k = 2
	} else if rowCount < 1_0000_0001 {
		k = 3
	} else if rowCount < 10_0000_0001 {
		k = 3
	} else if rowCount < 100_0000_0001 {
		k = 3
	} else {
		panic("unsupport rowCount")
	}
	k *= 3

	if probability <= 0 {
		probability = 0.001
	}
	m_float := -float64(k) * float64(rowCount) / math.Log(1-math.Pow(probability, 1.0/float64(k)))
	//m_float := -float64(rowCount) * math.Log(probability) / (math.Ln2 * math.Ln2)
	m := uint64(m_float)

	m = nextPow2(m)
	if m == 0 && m_float > 0 {
		// This means nextPow2 overflowed. Use the max power of 2.
		m = 1 << 63
	}

	return m, k
}

// NewCBloomFilterWithProbaility creates a new CBloomFilter with optimal parameters
// derived from the expected number of elements (rowcnt) and the desired false positive probability.
func NewCBloomFilterWithProbability(rowcnt int64, probability float64) *CBloomFilter {
	if rowcnt <= 0 {
		rowcnt = 2
	}
	nbit, k := ComputeMemAndHashCountC(rowcnt, probability)
	//os.Stderr.WriteString(fmt.Sprintf("bloom k %d m %d\n", k, nbit))
	return NewCBloomFilter(uint64(nbit), uint32(k))
}

// NewCBloomFilter creates a new CBloomFilter with a specific number of bits (nbits)
// and number of hash functions (k).
func NewCBloomFilter(nbits uint64, k uint32) *CBloomFilter {
	ptr := C.bloomfilter_init(C.uint64_t(nbits), C.uint32_t(k))
	if ptr == nil {
		panic("C.bloomfilter_init: Failed to allocate C.bloomfilter_t")
	}
	return &CBloomFilter{ptr: ptr, refcnt: 1}
}

// NewCBloomFilterWithSeed creates a new CBloomFilter with a specific number of bits (nbits),
// number of hash functions (k), and a specific seed.
func NewCBloomFilterWithSeed(nbits uint64, k uint32, seed uint64) *CBloomFilter {
	ptr := C.bloomfilter_init_with_seed(C.uint64_t(nbits), C.uint32_t(k), C.uint64_t(seed))
	if ptr == nil {
		panic("C.bloomfilter_init_with_seed: Failed to allocate C.bloomfilter_t")
	}
	return &CBloomFilter{ptr: ptr, refcnt: 1}
}

func (bf *CBloomFilter) Ptr() *C.bloomfilter_t {
	return bf.ptr
}

// Free releases the C memory allocated for the bloom filter.
func (bf *CBloomFilter) Free() {
	if bf != nil && bf.ptr != nil {
		if atomic.AddInt32(&bf.refcnt, -1) == 0 {
			C.bloomfilter_free(bf.ptr)
			bf.ptr = nil
		}
	}
}

// Share the CBloomfilter and increment the reference counter
func (bf *CBloomFilter) SharePointer() *CBloomFilter {
	atomic.AddInt32(&bf.refcnt, 1)
	return bf
}

// Check CBloomFilter is Valid
func (bf *CBloomFilter) Valid() bool {
	return (bf != nil && bf.ptr != nil)
}

// Exact reports whether membership is exact. A bloom filter is approximate (it
// has false positives), so this is always false. It lets CBloomFilter satisfy
// the engine.MembershipFilter interface alongside the exact bitset filters.
func (bf *CBloomFilter) Exact() bool { return false }

// Add inserts a byte slice into the bloom filter.
func (bf *CBloomFilter) Add(data []byte) {
	if bf == nil || bf.ptr == nil || data == nil {
		return
	}
	C.bloomfilter_add(bf.ptr, unsafe.Pointer(unsafe.SliceData(data)), C.size_t(len(data)))
	runtime.KeepAlive(data)
}

// Test checks if a byte slice is possibly in the bloom filter.
func (bf *CBloomFilter) Test(data []byte) bool {
	if bf == nil || bf.ptr == nil || data == nil {
		return false
	}
	return bool(C.bloomfilter_test(bf.ptr, unsafe.Pointer(unsafe.SliceData(data)), C.size_t(len(data))))
}

// TestAndAdd checks if a byte slice is in the bloom filter and adds it if it's not.
func (bf *CBloomFilter) TestAndAdd(data []byte) bool {
	if bf == nil || bf.ptr == nil || data == nil {
		return false
	}
	return bool(C.bloomfilter_test_and_add(bf.ptr, unsafe.Pointer(unsafe.SliceData(data)), C.size_t(len(data))))
}

// Marshal serializes the bloom filter into a byte slice.
func (bf *CBloomFilter) Marshal() ([]byte, error) {
	if bf == nil || bf.ptr == nil {
		return nil, moerr.NewInternalErrorNoCtx("CBloomFilter.Marshal: CBloomFilter or C.bloomfilter_t is nil")
	}
	var clen C.size_t
	dataPtr := C.bloomfilter_marshal(bf.ptr, &clen)
	if dataPtr == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to marhsal CBloomFilter")
	}
	return C.GoBytes(unsafe.Pointer(dataPtr), C.int(clen)), nil
}

// Unmarshal reconstructs the bloom filter from a byte slice.
func (bf *CBloomFilter) Unmarshal(data []byte) error {
	if bf.ptr != nil {
		return moerr.NewInternalErrorNoCtx("CBloomFilter:Unmarshal ptr is not nil")
	}

	if len(data) == 0 {
		return moerr.NewInternalErrorNoCtx("Invalid bloomfilter data: empty slice")
	}
	// Allocate C memory and copy data to it, because bloomfilter_unmarshal
	// just casts the pointer and we want a stable C allocation that we can free.
	cData := C.malloc(C.size_t(len(data)))
	C.memcpy(cData, unsafe.Pointer(&data[0]), C.size_t(len(data)))
	runtime.KeepAlive(data)

	ptr := C.bloomfilter_unmarshal((*C.uint8_t)(cData), C.size_t(len(data)))
	if ptr == nil {
		C.free(cData)
		return moerr.NewInternalErrorNoCtx("Failed to unmarshal bloomfilter")
	}
	bf.ptr = ptr
	atomic.StoreInt32(&bf.refcnt, 1)
	return nil
}

// TestAndAddVector tests and adds all elements in the vector to the bloom filter.
// It invokes the callback function for all elements in the vector.
func (bf *CBloomFilter) TestAndAddVector(v *vector.Vector, callBack func(bool, bool, int)) {
	if bf == nil || bf.ptr == nil {
		return
	}

	if v.GetType().IsFixedLen() {
		bf.testAndAddFixedVector(v, callBack)
	} else {
		bf.testAndAddVarlenaVector(v, callBack)
	}
}

// TestVector tests all elements in the vector against the bloom filter.
// It invokes the callback function for all elements in the vector.
func (bf *CBloomFilter) TestVector(v *vector.Vector, callBack func(bool, bool, int)) []uint8 {
	if bf == nil || bf.ptr == nil {
		return nil
	}

	if v.GetType().IsFixedLen() {
		return bf.testFixedVector(v, callBack)
	} else {
		return bf.testVarlenaVector(v, callBack)
	}
}

// AddVector adds all elements in the vector to the bloom filter.
func (bf *CBloomFilter) AddVector(v *vector.Vector) {
	if bf == nil || bf.ptr == nil {
		return
	}

	if v.GetType().IsFixedLen() {
		bf.addFixedVector(v)
	} else {
		bf.addVarlenaVector(v)
	}
}

// A CONSTANT vector stores a single physical value (or none, for const-null) but reports a
// logical Length() equal to the row count. Passing the logical length to C over the 1-element
// (or empty) physical buffer causes out-of-range reads — false negatives — or a panic on the
// empty const-null buffer (#25621). These helpers make every method process only the physical
// element(s) and broadcast the single result across all logical rows.

// vecPhysCount returns the number of PHYSICAL elements to feed to C (1 for a non-null constant,
// 0 for a const-null, v.Length() otherwise) and whether v is a constant vector.
func vecPhysCount(v *vector.Vector) (nitem int, isConst bool) {
	if v.IsConst() {
		// A zero-logical-length constant still retains its single physical value (e.g.
		// NewConstFixed(...) then the public SetLength(0) on a reused/shrunk vector), but
		// it has no rows to process. Return 0 so every helper stays a no-op — preserving
		// the original `if v.Length() == 0 { return }` contract — instead of feeding the
		// retained value to C and mutating the filter for a logically empty input.
		if v.IsConstNull() || v.Length() == 0 {
			return 0, true
		}
		return 1, true
	}
	return v.Length(), false
}

// emitConstNull reports every logical row of a const-null vector as NULL with a not-present (0)
// result and returns the all-zero logical-length result slice.
func emitConstNull(v *vector.Vector, callBack func(bool, bool, int)) []uint8 {
	n := v.Length()
	if callBack != nil {
		for j := 0; j < n; j++ {
			callBack(false, true, j)
		}
	}
	return make([]uint8, n)
}

// broadcastResults expands physResults to a logical-length result slice and invokes callBack
// once per logical row. For a constant every row shares physResults[0] and is never NULL;
// otherwise row j uses physResults[j] and its own null bit.
func broadcastResults(v *vector.Vector, physResults []uint8, isConst bool, callBack func(bool, bool, int)) []uint8 {
	n := v.Length()
	if !isConst {
		if callBack != nil {
			nulls := v.GetNulls()
			for j := 0; j < n; j++ {
				callBack(physResults[j] != 0, nulls.Contains(uint64(j)), j)
			}
		}
		return physResults
	}
	out := make([]uint8, n)
	r := physResults[0]
	for j := 0; j < n; j++ {
		out[j] = r
	}
	if callBack != nil {
		for j := 0; j < n; j++ {
			callBack(r != 0, false, j)
		}
	}
	return out
}

// broadcastAddResults maps physical test-AND-add results onto logical rows.
// Unlike broadcastResults it is STATE-AWARE for a constant vector: test-and-add
// is stateful, so the single physical value is tested+added once (row 0's
// pre-insert result), and every subsequent logical row now observes the
// just-inserted value and reports present (true). Broadcasting row 0's
// pre-insert result to all rows (as the read-only broadcastResults does) would
// wrongly report repeated logical values as new to a dedup callback and make
// constant and flat representations of the same rows observably different.
func broadcastAddResults(v *vector.Vector, physResults []uint8, isConst bool, callBack func(bool, bool, int)) {
	if callBack == nil {
		return
	}
	n := v.Length()
	if !isConst {
		nulls := v.GetNulls()
		for j := 0; j < n; j++ {
			callBack(physResults[j] != 0, nulls.Contains(uint64(j)), j)
		}
		return
	}
	// Constant, non-null: row 0 carries the pre-insert result; after it inserts
	// the value, rows 1..n-1 always observe it (bloom filters have no false
	// negatives), so they report present.
	for j := 0; j < n; j++ {
		present := j > 0 || physResults[0] != 0
		callBack(present, false, j)
	}
}

func (bf *CBloomFilter) testAndAddFixedVector(v *vector.Vector, callBack func(bool, bool, int)) {
	if v.IsConstNull() {
		emitConstNull(v, callBack)
		return
	}
	nitem, isConst := vecPhysCount(v)
	if nitem == 0 {
		return
	}
	fixedData := v.GetData()
	typeSize := v.GetType().TypeSize()
	nulls := v.GetNulls()
	nullbm := nulls.GetBitmap()

	var nullptr unsafe.Pointer
	var nulllen C.size_t
	if !isConst && nullbm != nil {
		nullptr = unsafe.Pointer(nullbm.Ptr())
		nulllen = C.size_t(nullbm.Size())
	}

	physResults := make([]uint8, nitem)
	C.bloomfilter_test_and_add_fixed(bf.ptr, unsafe.Pointer(&fixedData[0]), C.size_t(len(fixedData)), C.size_t(typeSize), C.size_t(nitem), nullptr, nulllen, unsafe.Pointer(&physResults[0]))
	runtime.KeepAlive(fixedData)
	runtime.KeepAlive(nullptr)

	broadcastAddResults(v, physResults, isConst, callBack)
}

func (bf *CBloomFilter) testAndAddVarlenaVector(v *vector.Vector, callBack func(bool, bool, int)) {
	if v.IsConstNull() {
		emitConstNull(v, callBack)
		return
	}
	nitem, isConst := vecPhysCount(v)
	if nitem == 0 {
		return
	}
	varlenData := vector.MustFixedColWithTypeCheck[types.Varlena](v)
	typeSize := v.GetType().TypeSize()
	area := v.GetArea()
	nulls := v.GetNulls()
	nullbm := nulls.GetBitmap()

	var nullptr *uint64
	var nulllen int
	if !isConst && nullbm != nil {
		nullptr = nullbm.Ptr()
		nulllen = nullbm.Size()
	}

	physResults := make([]uint8, nitem)
	C.bloomfilter_test_and_add_varlena(bf.ptr, unsafe.Pointer(&varlenData[0]), C.size_t(len(varlenData)*typeSize), C.size_t(typeSize), C.size_t(nitem), unsafe.Pointer(unsafe.SliceData(area)), C.size_t(len(area)), unsafe.Pointer(nullptr), C.size_t(nulllen), unsafe.Pointer(&physResults[0]))

	broadcastAddResults(v, physResults, isConst, callBack)
	runtime.KeepAlive(varlenData)
	runtime.KeepAlive(area)
	runtime.KeepAlive(nullptr)
}

func (bf *CBloomFilter) testFixedVector(v *vector.Vector, callBack func(bool, bool, int)) []uint8 {
	if v.IsConstNull() {
		return emitConstNull(v, callBack)
	}
	nitem, isConst := vecPhysCount(v)
	if nitem == 0 {
		return make([]uint8, v.Length())
	}
	fixedData := v.GetData()
	typeSize := v.GetType().TypeSize()
	nulls := v.GetNulls()
	nullbm := nulls.GetBitmap()

	var nullptr unsafe.Pointer
	var nulllen C.size_t
	if !isConst && nullbm != nil {
		nullptr = unsafe.Pointer(nullbm.Ptr())
		nulllen = C.size_t(nullbm.Size())
	}

	physResults := make([]uint8, nitem)
	C.bloomfilter_test_fixed(bf.ptr, unsafe.Pointer(&fixedData[0]), C.size_t(len(fixedData)), C.size_t(typeSize), C.size_t(nitem), nullptr, nulllen, unsafe.Pointer(&physResults[0]))
	runtime.KeepAlive(fixedData)
	runtime.KeepAlive(nullptr)

	return broadcastResults(v, physResults, isConst, callBack)
}

func (bf *CBloomFilter) testVarlenaVector(v *vector.Vector, callBack func(bool, bool, int)) []uint8 {
	if v.IsConstNull() {
		return emitConstNull(v, callBack)
	}
	nitem, isConst := vecPhysCount(v)
	if nitem == 0 {
		return make([]uint8, v.Length())
	}
	varlenData := vector.MustFixedColWithTypeCheck[types.Varlena](v)
	typeSize := v.GetType().TypeSize()
	area := v.GetArea()
	nulls := v.GetNulls()
	nullbm := nulls.GetBitmap()

	var nullptr *uint64
	var nulllen int
	if !isConst && nullbm != nil {
		nullptr = nullbm.Ptr()
		nulllen = nullbm.Size()
	}

	physResults := make([]uint8, nitem)
	C.bloomfilter_test_varlena(bf.ptr, unsafe.Pointer(&varlenData[0]), C.size_t(len(varlenData)*typeSize), C.size_t(typeSize), C.size_t(nitem), unsafe.Pointer(unsafe.SliceData(area)), C.size_t(len(area)), unsafe.Pointer(nullptr), C.size_t(nulllen), unsafe.Pointer(&physResults[0]))

	out := broadcastResults(v, physResults, isConst, callBack)
	runtime.KeepAlive(varlenData)
	runtime.KeepAlive(area)
	runtime.KeepAlive(nullptr)

	return out
}

func (bf *CBloomFilter) addFixedVector(v *vector.Vector) {
	if v.IsConstNull() {
		return
	}
	nitem, isConst := vecPhysCount(v)
	if nitem == 0 {
		return
	}
	fixedData := v.GetData()
	typeSize := v.GetType().TypeSize()
	nulls := v.GetNulls()
	nullbm := nulls.GetBitmap()

	var nullptr unsafe.Pointer
	var nulllen C.size_t
	if !isConst && nullbm != nil {
		nullptr = unsafe.Pointer(nullbm.Ptr())
		nulllen = C.size_t(nullbm.Size())
	}

	C.bloomfilter_add_fixed(bf.ptr, unsafe.Pointer(&fixedData[0]), C.size_t(len(fixedData)), C.size_t(typeSize), C.size_t(nitem), nullptr, nulllen)
	runtime.KeepAlive(fixedData)
	runtime.KeepAlive(nullptr)
}
func (bf *CBloomFilter) addVarlenaVector(v *vector.Vector) {
	if v.IsConstNull() {
		return
	}
	nitem, isConst := vecPhysCount(v)
	if nitem == 0 {
		return
	}
	varlenData := vector.MustFixedColWithTypeCheck[types.Varlena](v)
	typeSize := v.GetType().TypeSize()
	area := v.GetArea()
	nulls := v.GetNulls()
	nullbm := nulls.GetBitmap()

	var nullptr *uint64
	var nulllen int
	if !isConst && nullbm != nil {
		nullptr = nullbm.Ptr()
		nulllen = nullbm.Size()
	}

	C.bloomfilter_add_varlena(bf.ptr, unsafe.Pointer(&varlenData[0]), C.size_t(len(varlenData)*typeSize), C.size_t(typeSize), C.size_t(nitem), unsafe.Pointer(unsafe.SliceData(area)), C.size_t(len(area)), unsafe.Pointer(nullptr), C.size_t(nulllen))
	runtime.KeepAlive(varlenData)
	runtime.KeepAlive(area)
	runtime.KeepAlive(nullptr)
}

// Merge merges another bloom filter into this one (bf becomes bf OR other).
// The nbits, seed, and k MUST be the same for both bloom filters.
func (bf *CBloomFilter) Merge(other *CBloomFilter) error {
	if bf == nil || bf.ptr == nil {
		return moerr.NewInternalErrorNoCtx("CBloomFilter:Merge called on a nil or invalid bloom filter")
	}
	if other == nil || other.ptr == nil {
		return moerr.NewInternalErrorNoCtx("CBloomFilter:Merge called with a nil or invalid 'other' bloom filter")
	}

	// The C function expects nbits, seed, and k to be the same.
	// The C implementation already checks this and returns an error code if they differ.
	ret := C.bloomfilter_or(bf.ptr, bf.ptr, other.ptr)
	runtime.KeepAlive(other)
	if ret != 0 {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("failed to merge bloom filters, error code: %d", ret))
	}
	return nil
}

func (bf *CBloomFilter) GetNbits() uint64 {
	if bf == nil || bf.ptr == nil {
		return 0
	}
	return uint64(C.bloomfilter_get_nbits(bf.ptr))
}

func (bf *CBloomFilter) GetSeed() uint64 {
	if bf == nil || bf.ptr == nil {
		return 0
	}
	return uint64(C.bloomfilter_get_seed(bf.ptr))
}

func (bf *CBloomFilter) GetK() uint32 {
	if bf == nil || bf.ptr == nil {
		return 0
	}
	return uint32(C.bloomfilter_get_k(bf.ptr))
}
