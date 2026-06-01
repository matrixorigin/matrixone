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
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// TagCbitmap marks a payload serialized by the dense cbitmap filter, alongside
// TagBloom / TagBitset / TagCRoaring in the shared reader-side transport.
const TagCbitmap byte = 3

// MaxCbitmapBits caps the dense bitset size. A dense bitmap is indexed by the
// doc_id value, so its size is O(max value), not O(count) — only viable when
// the max id is bounded. Above this the caller must fall back (CRoaring).
// 2^27 bits = 16 MB (covers dense integer PKs up to ~134M).
const MaxCbitmapBits = uint64(1) << 27

// CbitmapFeasible reports whether a max doc_id value is small enough that a
// dense cbitmap is worthwhile (vs the compact CRoaring filter).
func CbitmapFeasible(maxVal uint64) bool {
	return maxVal+1 <= MaxCbitmapBits
}

// BuildIntegerDocFilter builds the best exact filter for an integer doc_id
// vector and returns the tag byte to prepend + the serialized payload: a dense
// cbitmap when the id range is bounded (fastest), else a compact CRoaring
// bitset (sparse-safe). The reader picks the structure from the tag.
func BuildIntegerDocFilter(v *vector.Vector) (byte, []byte, error) {
	if data, ok, err := BuildCbitmapBytes(v); err != nil {
		return 0, nil, err
	} else if ok {
		return TagCbitmap, data, nil
	}
	data, err := BuildCRoaringBytes(v)
	if err != nil {
		return 0, nil, err
	}
	return TagCRoaring, data, nil
}

// BuildIntegerDocFilterU64 is the explicit-set variant of BuildIntegerDocFilter
// (e.g. IVF centroid-filtered keys).
func BuildIntegerDocFilterU64(vals []uint64) (byte, []byte, error) {
	if data, ok, err := BuildCbitmapBytesU64(vals); err != nil {
		return 0, nil, err
	} else if ok {
		return TagCbitmap, data, nil
	}
	data, err := BuildCRoaringBytesU64(vals)
	if err != nil {
		return 0, nil, err
	}
	return TagCRoaring, data, nil
}

// CbitmapDocFilter wraps the dense (C-assisted) bitmap.Bitmap and implements
// engine.DocIDFilter. Pure-Go membership (one shift+mask), so it is the fastest
// exact filter for dense, bounded integer doc_ids.
type CbitmapDocFilter struct {
	bm *bitmap.Bitmap
}

// buildCbitmap builds a dense bitmap from the given values, returning ok=false
// (no error) if the max value exceeds MaxCbitmapBits so the caller can fall back.
func buildCbitmap(vals []uint64) (*bitmap.Bitmap, bool) {
	// Too many candidate keys: skip the dense bitmap (CRoaring is more
	// memory-efficient for large sets). Same budget as the id-range cap below
	// (a dense bitmap is only worthwhile within MaxCbitmapBits), and an early
	// out before the max scan. For unique PKs count <= maxId+1, so this only
	// binds on pathological duplicate-heavy input.
	if uint64(len(vals)) > MaxCbitmapBits {
		return nil, false
	}
	var maxv uint64
	for _, v := range vals {
		if v > maxv {
			maxv = v
		}
	}
	if len(vals) > 0 && !CbitmapFeasible(maxv) {
		return nil, false
	}
	bm := &bitmap.Bitmap{}
	bm.InitWithSize(int64(maxv + 1))
	bm.AddMany(vals)
	return bm, true
}

// BuildCbitmapBytes builds a dense bitset from an integer doc_id vector and
// returns its serialization (no tag). ok=false means the id range is too large
// for a dense bitmap (caller should use CRoaring instead).
func BuildCbitmapBytes(v *vector.Vector) (data []byte, ok bool, err error) {
	vals := vectorToU64(v)
	bm, ok := buildCbitmap(vals)
	if !ok {
		return nil, false, nil
	}
	return bm.Marshal(), true, nil
}

// BuildCbitmapBytesU64 is the explicit-set variant (e.g. IVF centroid-filtered
// keys). ok=false means the id range is too large for a dense bitmap.
func BuildCbitmapBytesU64(vals []uint64) (data []byte, ok bool, err error) {
	bm, ok := buildCbitmap(vals)
	if !ok {
		return nil, false, nil
	}
	return bm.Marshal(), true, nil
}

// vectorToU64 decodes a fixed integer vector's non-null rows to []uint64.
func vectorToU64(v *vector.Vector) []uint64 {
	n := v.Length()
	out := make([]uint64, 0, n)
	for i := 0; i < n; i++ {
		if v.IsNull(uint64(i)) {
			continue
		}
		out = append(out, rawIntToUint64(v.GetRawBytesAt(i)))
	}
	return out
}

// NewCbitmapDocFilter deserializes a dense bitset payload (no tag prefix).
func NewCbitmapDocFilter(data []byte) (*CbitmapDocFilter, error) {
	if len(data) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("cbitmap: empty payload")
	}
	bm := &bitmap.Bitmap{}
	if err := bm.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return &CbitmapDocFilter{bm: bm}, nil
}

// Test reports whether the raw fixed bytes of a single doc_id are present.
func (f *CbitmapDocFilter) Test(data []byte) bool {
	if f == nil || f.bm == nil {
		return false
	}
	return f.bm.Contains(rawIntToUint64(data))
}

// TestVector tests every row of an integer doc_id vector.
func (f *CbitmapDocFilter) TestVector(v *vector.Vector, cb func(bool, bool, int)) []uint8 {
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

// Valid reports whether the filter is usable.
func (f *CbitmapDocFilter) Valid() bool {
	return f != nil && f.bm != nil
}

// Share returns a new wrapper over the same bitmap for a parallel reader; the
// underlying bitmap is read-only and GC-reclaimed once all wrappers drop it.
func (f *CbitmapDocFilter) Share() DocIDFilter {
	if f == nil {
		return nil
	}
	return &CbitmapDocFilter{bm: f.bm}
}

// Free drops this wrapper's reference (pure Go, GC reclaims the bitmap).
func (f *CbitmapDocFilter) Free() {
	if f != nil {
		f.bm = nil
	}
}
