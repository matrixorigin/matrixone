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

// Package docfilter provides an exact, integer-PK doc_id membership filter used
// to prune the fulltext / IVF index scan to the candidate documents that pass a
// surrounding relational predicate.
//
// For an integer PK it builds an exact bitset — a dense cbitmap for a bounded
// id range (cbitmap.go), else the compact C CRoaring bitset (croaring.go, via
// pkg cgo). Both are cheaper to build than a bloom filter (no hashing) and exact
// (no false positives, so no re-verification is needed). For non-integer PKs the
// caller falls back to a CBloomFilter. Build produces a tagged payload and New
// reconstructs the right structure from the tag; callers hold the result behind
// the MembershipFilter interface and never see the concrete structure.
//
// WARNING — transport endianness: the cbitmap payload (TagCbitmap) is serialized
// in HOST byte order (a raw memcpy of [base][nbits][bitmap words]) for speed, so
// it is only valid when exchanged between MO nodes of the SAME endianness. The
// CRoaring (TagCRoaring) and CBloomFilter (TagBloom) payloads use portable
// serialization and are endianness-independent. All current MO targets are
// little-endian and a big-endian build fails to compile (see the static guard in
// cgo/cbitmap.c). Before deploying MO on a big-endian or mixed-endian cluster,
// switch cbitmap (de)serialization to an explicit little-endian format.
package docfilter

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// TagBloom marks a payload serialized by the CBloomFilter fallback (non-integer
// PKs). It shares the reader-side transport channel
// (defines.FulltextMembershipFilter -> FilterHint.MembershipFilterBytes) with TagCRoaring
// (croaring.go) and TagCbitmap (cbitmap.go); New dispatches on the tag.
const TagBloom byte = 0

// SupportsBitset reports whether a doc_id column type can be filtered with an
// exact integer bitset (cbitmap / CRoaring), i.e. it is a fixed-width integer
// type. Non-integer PKs (varchar/uuid/decimal/composite) use the CBloomFilter
// fallback.
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
// the build side (in C) and the single-key probe side, so the mapping is
// consistent regardless of signedness or width.
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
