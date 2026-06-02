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
	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// bloomFpProbability is the false-positive rate for the CBloomFilter fallback
// (used for non-integer PKs). Matches the previous fulltext/IVF values.
const bloomFpProbability = 0.001

// DocIDFilter is a doc_id membership filter. Its method set is a superset of
// engine.DocIDFilter (it adds Share), so a DocIDFilter value can be stored
// directly in engine.FilterHint.BF without this package importing engine.
//
// Callers obtain one via New (from tagged bytes produced by Build) and never
// need to know which concrete structure (cbitmap / CRoaring / bloom) backs it.
type DocIDFilter interface {
	// Test reports whether the raw fixed bytes of a single doc_id may be present.
	Test(data []byte) bool
	// TestVector tests every row of a doc_id vector, invoking cb(exist, isnull, row).
	TestVector(v *vector.Vector, cb func(bool, bool, int)) []uint8
	// Valid reports whether the filter is usable.
	Valid() bool
	// Free releases resources / drops one share.
	Free()
	// Share returns a filter for one parallel reader (refcount or per-reader wrapper).
	Share() DocIDFilter
}

var (
	_ DocIDFilter = (*CbitmapDocFilter)(nil)
	_ DocIDFilter = (*CRoaringDocFilter)(nil)
	_ DocIDFilter = (*bloomDocFilter)(nil)
)

// Build serializes the best doc_id filter for the whole vector and returns the
// tagged bytes: an exact bitset (cbitmap for a bounded integer-id range, else
// CRoaring) for integer PKs, or a CBloomFilter for non-integer PKs. Build and
// probe read the column buffer directly in C (one cgo call). Callers just
// transport the bytes; New reconstructs the right filter from the tag.
func Build(v *vector.Vector) ([]byte, error) {
	if SupportsBitset(*v.GetType()) {
		tag, payload, err := BuildIntegerDocFilter(v)
		if err != nil {
			return nil, err
		}
		return append([]byte{tag}, payload...), nil
	}
	payload, err := buildBloomBytes(v)
	if err != nil {
		return nil, err
	}
	return append([]byte{TagBloom}, payload...), nil
}

// New reconstructs a DocIDFilter from the tagged bytes produced by Build.
func New(data []byte) (DocIDFilter, error) {
	if len(data) <= 1 {
		return nil, moerr.NewInternalErrorNoCtx("docfilter: empty filter payload")
	}
	tag, payload := data[0], data[1:]
	switch tag {
	case TagCbitmap:
		f, err := NewCbitmapDocFilter(payload)
		if err != nil {
			return nil, err
		}
		return f, nil
	case TagCRoaring:
		f, err := NewCRoaringDocFilter(payload)
		if err != nil {
			return nil, err
		}
		return f, nil
	case TagBloom:
		bf := &bloomfilter.CBloomFilter{}
		if err := bf.Unmarshal(payload); err != nil {
			return nil, err
		}
		return &bloomDocFilter{bf: bf}, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("docfilter: unknown filter tag")
	}
}

// buildBloomBytes builds a CBloomFilter over the vector and marshals it (the
// non-integer-PK fallback).
func buildBloomBytes(v *vector.Vector) ([]byte, error) {
	bf := bloomfilter.NewCBloomFilterWithProbability(int64(v.Length()), bloomFpProbability)
	defer bf.Free()
	bf.AddVector(v)
	return bf.Marshal()
}

// bloomDocFilter adapts *bloomfilter.CBloomFilter to DocIDFilter (the non-integer
// PK fallback), so New can return it behind the interface like the bitsets.
type bloomDocFilter struct {
	bf *bloomfilter.CBloomFilter
}

func (f *bloomDocFilter) Test(data []byte) bool {
	return f != nil && f.bf != nil && f.bf.Test(data)
}

func (f *bloomDocFilter) TestVector(v *vector.Vector, cb func(bool, bool, int)) []uint8 {
	if f == nil || f.bf == nil {
		return nil
	}
	return f.bf.TestVector(v, cb)
}

func (f *bloomDocFilter) Valid() bool {
	return f != nil && f.bf != nil && f.bf.Valid()
}

func (f *bloomDocFilter) Free() {
	if f != nil && f.bf != nil {
		f.bf.Free()
	}
}

func (f *bloomDocFilter) Share() DocIDFilter {
	return &bloomDocFilter{bf: f.bf.SharePointer()}
}
