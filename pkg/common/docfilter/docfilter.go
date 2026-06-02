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

// Package docfilter provides an exact, roaring64-backed doc_id membership
// filter used to prune the fulltext index scan to the candidate documents that
// pass a surrounding relational predicate.
//
// It is the integer-PK alternative to the CBloomFilter pushdown filter: when
// doc_id is an integer type, a roaring64 bitset is cheaper to build (no
// hashing), exact (no false positives, so no re-verification is needed), and
// fast to probe. For non-integer PKs the caller falls back to CBloomFilter.
//
// RoaringDocFilter satisfies the engine.DocIDFilter interface structurally
// (Test/TestVector/Valid/Free) so it can be stored in engine.FilterHint.BF
// alongside *bloomfilter.CBloomFilter.
package docfilter

import (
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// Tag bytes prefixed to the serialized doc_id filter so the reader knows how to
// interpret the payload. They share the same []byte transport channel
// (defines.FulltextBloomFilter -> FilterHint.BloomFilter) the CBloomFilter uses.
const (
	TagBloom  byte = 0 // payload is a marshaled *bloomfilter.CBloomFilter
	TagBitset byte = 1 // payload is a marshaled roaring64.Bitmap
)

// SupportsBitset reports whether a doc_id column type can be filtered with a
// roaring64 bitset (i.e. it is a fixed-width integer type). Non-integer PKs
// (varchar/uuid/decimal/composite) must use the CBloomFilter fallback.
func SupportsBitset(t types.Type) bool {
	switch t.Oid {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		return true
	default:
		return false
	}
}

// rawIntToUint64 decodes the raw little-endian fixed bytes of an integer doc_id
// (width 1/2/4/8) into a uint64 by zero-extension. The SAME decode is used on
// the build side (each candidate value) and the probe side (each scanned
// doc_id), so the mapping is consistent regardless of signedness or width.
func rawIntToUint64(b []byte) uint64 {
	var x uint64
	n := len(b)
	if n > 8 {
		n = 8
	}
	for i := 0; i < n; i++ {
		x |= uint64(b[i]) << (8 * uint(i))
	}
	return x
}

// BuildBitset builds a roaring64 bitset from the candidate doc_id vector,
// skipping nulls. The vector must be a fixed-width integer type (see
// SupportsBitset).
func BuildBitset(v *vector.Vector) *roaring64.Bitmap {
	bm := roaring64.New()
	n := v.Length()
	for i := 0; i < n; i++ {
		if v.IsNull(uint64(i)) {
			continue
		}
		bm.Add(rawIntToUint64(v.GetRawBytesAt(i)))
	}
	return bm
}

// MarshalBitset serializes a roaring64 bitset to bytes (no tag prefix).
func MarshalBitset(bm *roaring64.Bitmap) ([]byte, error) {
	return bm.MarshalBinary()
}

// NewBitmap returns an empty roaring64 bitset for incremental building (e.g.
// when only a selected subset of an integer-PK vector should be added).
func NewBitmap() *roaring64.Bitmap {
	return roaring64.New()
}

// AddRaw adds a single integer doc_id, given as its raw fixed little-endian
// bytes, to bm — using the same decode as Test/BuildBitset so build and probe
// stay consistent.
func AddRaw(bm *roaring64.Bitmap, raw []byte) {
	bm.Add(rawIntToUint64(raw))
}

// RoaringDocFilter wraps a roaring64 bitset and implements engine.DocIDFilter.
type RoaringDocFilter struct {
	bm *roaring64.Bitmap
}

// NewRoaringDocFilter deserializes a roaring64 bitset payload (no tag prefix)
// into a RoaringDocFilter.
func NewRoaringDocFilter(data []byte) (*RoaringDocFilter, error) {
	bm := roaring64.New()
	if err := bm.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return &RoaringDocFilter{bm: bm}, nil
}

// Test reports whether the raw fixed bytes of a single doc_id are present.
func (f *RoaringDocFilter) Test(data []byte) bool {
	if f == nil || f.bm == nil {
		return false
	}
	return f.bm.Contains(rawIntToUint64(data))
}

// TestVector tests every row of a doc_id vector. For each row it invokes
// cb(exist, isnull, row) and returns a parallel []uint8 of 0/1 membership
// (nulls are reported as non-existent), matching CBloomFilter.TestVector.
func (f *RoaringDocFilter) TestVector(v *vector.Vector, cb func(bool, bool, int)) []uint8 {
	if f == nil || f.bm == nil {
		return nil
	}
	n := v.Length()
	res := make([]uint8, n)
	for i := 0; i < n; i++ {
		isnull := v.IsNull(uint64(i))
		exist := false
		if !isnull {
			exist = f.bm.Contains(rawIntToUint64(v.GetRawBytesAt(i)))
		}
		if exist {
			res[i] = 1
		}
		if cb != nil {
			cb(exist, isnull, i)
		}
	}
	return res
}

// Share returns a new wrapper over the SAME underlying bitmap, for handing one
// filter to each parallel reader. roaring64 Contains is safe for concurrent
// reads, and each reader's Free() clears only its own wrapper reference (the
// shared bitmap is reclaimed by GC once all wrappers drop it) — so a reader
// closing never nils a filter another reader is still using.
func (f *RoaringDocFilter) Share() *RoaringDocFilter {
	if f == nil {
		return nil
	}
	return &RoaringDocFilter{bm: f.bm}
}

// Valid reports whether the filter is usable.
func (f *RoaringDocFilter) Valid() bool {
	return f != nil && f.bm != nil
}

// Free drops the underlying bitset reference (pure Go, GC reclaims).
func (f *RoaringDocFilter) Free() {
	if f != nil {
		f.bm = nil
	}
}
