// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package docfilter

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// bloomFpProbability is the false-positive rate for the CBloomFilter fallback
// (used for non-integer PKs). Matches the previous fulltext/IVF values.
const bloomFpProbability = 0.001

// MembershipFilter is the primary-key membership PROBE contract (fulltext calls
// the PK doc_id). Its method set is the consumer interface
// engine.MembershipFilter (Test/TestVector/Valid/Exact/Free) plus Share — so a
// value can be stored directly in engine.FilterHint.BF without this package
// importing engine (a pkg/common -> pkg/vm/engine layering inversion). The
// consumer half is kept compatible with engine.MembershipFilter by a
// compile-time assertion in package disttae; that shared method set's single
// source of truth lives in engine.
//
// This is intentionally free of any cgo/unsafe detail: a general filter
// filter implementation only needs to probe. The C-bridge methods live on the
// separate CFilter contract below, which the cgo search bridge type-asserts.
//
// Callers obtain one via New (from tagged bytes produced by Build) and never
// need to know which concrete structure (cbitmap / Sorted64 / bloom) backs it.
type MembershipFilter interface {
	// Test reports whether the raw fixed bytes of a single key may be present.
	Test(data []byte) bool
	// TestVector tests every row of a key vector, invoking cb(exist, isnull, row).
	TestVector(v *vector.Vector, cb func(bool, bool, int)) []uint8
	// Valid reports whether the filter is usable.
	Valid() bool
	// Exact reports whether membership is exact (a set, no false positives)
	// rather than approximate (a bloom filter).
	Exact() bool
	// Free releases resources / drops one share.
	Free()
	// Share returns a filter for one parallel reader (refcount or per-reader wrapper).
	Share() MembershipFilter
}

// CFilter is the C-search ADAPTER contract: a MembershipFilter that can also
// expose an opaque probe pointer + structure tag for the cgo vector-index
// filtered-search bridge (pkg/vectorindex/usearchex), whose C predicate tests
// each candidate key against the structure directly. The bridge type-asserts a
// MembershipFilter to CFilter, so general filters that cannot back a C search
// are not forced to expose unsafe.Pointer details.
type CFilter interface {
	MembershipFilter
	// CHandle returns the opaque pointer borrowed by the C bridge: either a
	// native handle or a validated, Go-owned Sorted64 payload.
	CHandle() unsafe.Pointer
	// CKind reports the structure tag (TagBloom / TagCRoaring / TagCbitmap /
	// TagSorted64), telling the bridge which C membership test to dispatch.
	CKind() byte
}

var (
	_ MembershipFilter = (*CbitmapFilter)(nil)
	_ MembershipFilter = (*CRoaringFilter)(nil)
	_ MembershipFilter = (*cbloomFilter)(nil)
	_ MembershipFilter = (*Sorted64Filter)(nil)
	// Every concrete filter also satisfies the C-search bridge adapter.
	_ CFilter = (*CbitmapFilter)(nil)
	_ CFilter = (*CRoaringFilter)(nil)
	_ CFilter = (*cbloomFilter)(nil)
	_ CFilter = (*Sorted64Filter)(nil)
)

// Build serializes the best doc_id filter for the whole vector and returns the
// tagged bytes: an exact set (cbitmap for a cost-effective integer-id range,
// else Sorted64) for integer PKs, or a CBloomFilter for non-integer PKs. Dense
// integer and bloom probes read the column buffer directly in C. Callers just
// transport the bytes; New reconstructs the right filter from the tag.
func Build(v *vector.Vector) ([]byte, error) {
	return BuildWithMemoryAdmission(v, nil)
}

// BuildWithMemoryAdmission is Build with CN-wide peak-memory admission at the
// allocation site. A denied reservation is an error because silently omitting
// an exact prefilter can change Top-K results.
func BuildWithMemoryAdmission(
	v *vector.Vector,
	admission MemoryAdmission,
) ([]byte, error) {
	bytes, ok := buildAllocationBytes(v)
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx(
			"docfilter: invalid filter allocation size")
	}
	release, err := acquireMemory(admission, bytes)
	if err != nil {
		return nil, err
	}
	if release != nil {
		defer release()
	}
	if SupportsBitset(*v.GetType()) {
		return buildTaggedIntegerFilter(v)
	}
	return buildTaggedBloomBytes(v)
}

// New reconstructs a MembershipFilter from the tagged bytes produced by Build.
func New(data []byte) (MembershipFilter, error) {
	return NewWithMemoryAdmission(data, nil)
}

// NewWithMemoryAdmission is New with one shared lease covering the retained
// wire payload and any reconstructed allocation. The last shared reader frees
// the filter, refreshes RSS, and then releases the reservation.
func NewWithMemoryAdmission(
	data []byte,
	admission MemoryAdmission,
) (MembershipFilter, error) {
	if len(data) <= 1 {
		return nil, moerr.NewInternalErrorNoCtx("docfilter: empty filter payload")
	}
	tag, payload := data[0], data[1:]
	switch tag {
	case TagCbitmap, TagCRoaring, TagSorted64, TagBloom:
	default:
		return nil, moerr.NewInternalErrorNoCtx("docfilter: unknown filter tag")
	}
	bytes, ok := reconstructAllocationBytes(tag, payload)
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("docfilter: invalid filter allocation size")
	}
	release, err := acquireMemory(admission, bytes)
	if err != nil {
		return nil, err
	}
	owned := false
	defer func() {
		if !owned && release != nil {
			release()
		}
	}()
	switch tag {
	case TagCbitmap:
		f, err := NewCbitmapFilter(payload)
		if err != nil {
			return nil, err
		}
		f.memoryRelease = release
		owned = true
		return f, nil
	case TagCRoaring:
		f, err := NewCRoaringFilter(payload)
		if err != nil {
			return nil, err
		}
		f.memoryRelease = release
		owned = true
		return f, nil
	case TagSorted64:
		f, err := NewSorted64Filter(payload)
		if err != nil {
			return nil, err
		}
		f.memoryRelease = release
		owned = true
		return f, nil
	case TagBloom:
		bf := &bloomfilter.CBloomFilter{}
		if err := bf.Unmarshal(payload); err != nil {
			return nil, err
		}
		owned = true
		return &cbloomFilter{bf: bf, memory: newSharedMemoryRelease(release)}, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("docfilter: unknown filter tag")
	}
}

func buildTaggedBloomBytes(v *vector.Vector) ([]byte, error) {
	return buildBloomBytesMode(v, true)
}

func buildBloomBytesMode(v *vector.Vector, tagged bool) ([]byte, error) {
	bf := bloomfilter.NewCBloomFilterWithProbability(int64(v.Length()), bloomFpProbability)
	defer bf.Free()
	bf.AddVector(v)
	if tagged {
		return bf.MarshalWithPrefix(TagBloom)
	}
	return bf.Marshal()
}

// cbloomFilter adapts *bloomfilter.CBloomFilter to MembershipFilter (the non-integer
// PK fallback), so New can return it behind the interface like the bitsets.
type cbloomFilter struct {
	bf     *bloomfilter.CBloomFilter
	memory *sharedMemoryRelease
}

func (f *cbloomFilter) Test(data []byte) bool {
	return f != nil && f.bf != nil && f.bf.Test(data)
}

func (f *cbloomFilter) TestVector(v *vector.Vector, cb func(bool, bool, int)) []uint8 {
	if f == nil || f.bf == nil {
		return nil
	}
	return f.bf.TestVector(v, cb)
}

func (f *cbloomFilter) Valid() bool {
	return f != nil && f.bf != nil && f.bf.Valid()
}

// Exact is false: a bloom filter is approximate (has false positives).
func (f *cbloomFilter) Exact() bool { return false }

// CHandle returns the underlying C bloomfilter_t* for the cgo search bridge.
func (f *cbloomFilter) CHandle() unsafe.Pointer {
	if f == nil || f.bf == nil {
		return nil
	}
	return unsafe.Pointer(f.bf.Ptr())
}

// CKind reports the bloom structure tag.
func (f *cbloomFilter) CKind() byte { return TagBloom }

func (f *cbloomFilter) Free() {
	if f != nil && f.bf != nil {
		f.bf.Free()
		f.memory.free()
		f.bf = nil
		f.memory = nil
	}
}

func (f *cbloomFilter) Share() MembershipFilter {
	f.memory.share()
	return &cbloomFilter{bf: f.bf.SharePointer(), memory: f.memory}
}
