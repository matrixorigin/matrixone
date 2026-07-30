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

// Package keycodec defines the canonical byte contract shared by resident
// hashmaps and spill partitioning.
package keycodec

import (
	"math"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// Float32Codec holds the SQL comparison normalization for one FLOAT32 type.
// Construct it once per vector so scale processing is not repeated per row.
type Float32Codec struct {
	normalizer types.Float32ScaleNormalizer
}

// NewFloat32Codec returns the canonical key codec for a FLOAT32 scale.
func NewFloat32Codec(scale int32) Float32Codec {
	return Float32Codec{
		normalizer: types.NewFloat32ScaleNormalizer(scale),
	}
}

// CanonicalBits makes SQL-equal FLOAT32 values use one hash key. Scaled
// values are normalized exactly as scalar comparisons normalize them, and
// signed zero uses the single all-zero representation.
func (c Float32Codec) CanonicalBits(value float32) uint32 {
	bits := math.Float32bits(c.normalizer.Normalize(value))
	if bits<<1 == 0 {
		return 0
	}
	return bits
}

// CanonicalBytes returns the native-endian bytes used by resident hashmaps and
// spill partitioning.
func (c Float32Codec) CanonicalBytes(value float32) [4]byte {
	bits := c.CanonicalBits(value)
	return *(*[4]byte)(unsafe.Pointer(&bits))
}

// CanonicalFloat64Bits makes SQL-equal signed zero values use one hash key.
func CanonicalFloat64Bits(value float64) uint64 {
	bits := math.Float64bits(value)
	if bits<<1 == 0 {
		return 0
	}
	return bits
}

// CanonicalFloat64Bytes returns the native-endian bytes used by the resident
// hashmaps and spill partitioning. Returning the bytes by value avoids exposing
// mutable package-global storage to callers.
func CanonicalFloat64Bytes(value float64) [8]byte {
	bits := CanonicalFloat64Bits(value)
	return *(*[8]byte)(unsafe.Pointer(&bits))
}

// HashCombine merges one column hash into the hash state for a composite key.
func HashCombine(hash, columnHash uint64) uint64 {
	return hash ^ (columnHash + 0x9e3779b97f4a7c15 + (hash << 6) + (hash >> 2))
}

// ComputeXXHash computes the canonical partition hash for a set of typed key
// vectors. Type dispatch happens once per vector; row loops remain specialized
// so common raw-byte keys do not pay a per-row codec dispatch cost.
func ComputeXXHash(keyVecs []*vector.Vector, hashValues []uint64, seed uint64) {
	if len(hashValues) == 0 {
		return
	}

	rowCount := len(hashValues)
	for i := 0; i < rowCount; i++ {
		hashValues[i] = seed
	}
	if len(keyVecs) == 0 {
		return
	}

	for _, vec := range keyVecs {
		switch vec.GetType().Oid {
		case types.T_float32:
			computeFloat32XXHash(vec, hashValues)
			continue
		case types.T_float64:
			computeFloat64XXHash(vec, hashValues)
			continue
		}
		if vec.IsConst() {
			columnHash := uint64(0)
			if !vec.IsConstNull() {
				columnHash = xxhash.Sum64(vec.GetRawBytesAt(0))
			}
			for i := 0; i < rowCount; i++ {
				hashValues[i] = HashCombine(hashValues[i], columnHash)
			}
			continue
		}

		n := rowCount
		if vec.Length() < n {
			n = vec.Length()
		}
		if vec.GetNulls().Any() {
			nulls := vec.GetNulls()
			for i := 0; i < n; i++ {
				if nulls.Contains(uint64(i)) {
					hashValues[i] = HashCombine(hashValues[i], 0)
				} else {
					hashValues[i] = HashCombine(hashValues[i], xxhash.Sum64(vec.GetRawBytesAt(i)))
				}
			}
		} else {
			for i := 0; i < n; i++ {
				hashValues[i] = HashCombine(hashValues[i], xxhash.Sum64(vec.GetRawBytesAt(i)))
			}
		}
	}
}

func computeFloat32XXHash(vec *vector.Vector, hashValues []uint64) {
	rowCount := len(hashValues)
	codec := NewFloat32Codec(vec.GetType().Scale)
	if vec.IsConst() {
		columnHash := uint64(0)
		if !vec.IsConstNull() {
			values := vector.MustFixedColNoTypeCheck[float32](vec)
			value := codec.CanonicalBytes(values[0])
			columnHash = xxhash.Sum64(value[:])
		}
		for i := 0; i < rowCount; i++ {
			hashValues[i] = HashCombine(hashValues[i], columnHash)
		}
		return
	}

	n := rowCount
	if vec.Length() < n {
		n = vec.Length()
	}
	values := vector.MustFixedColNoTypeCheck[float32](vec)
	if vec.GetNulls().Any() {
		nulls := vec.GetNulls()
		for i := 0; i < n; i++ {
			if nulls.Contains(uint64(i)) {
				hashValues[i] = HashCombine(hashValues[i], 0)
			} else {
				value := codec.CanonicalBytes(values[i])
				hashValues[i] = HashCombine(hashValues[i], xxhash.Sum64(value[:]))
			}
		}
		return
	}
	for i := 0; i < n; i++ {
		value := codec.CanonicalBytes(values[i])
		hashValues[i] = HashCombine(hashValues[i], xxhash.Sum64(value[:]))
	}
}

func computeFloat64XXHash(vec *vector.Vector, hashValues []uint64) {
	rowCount := len(hashValues)
	if vec.IsConst() {
		columnHash := uint64(0)
		if !vec.IsConstNull() {
			values := vector.MustFixedColNoTypeCheck[float64](vec)
			value := CanonicalFloat64Bytes(values[0])
			columnHash = xxhash.Sum64(value[:])
		}
		for i := 0; i < rowCount; i++ {
			hashValues[i] = HashCombine(hashValues[i], columnHash)
		}
		return
	}

	n := rowCount
	if vec.Length() < n {
		n = vec.Length()
	}
	values := vector.MustFixedColNoTypeCheck[float64](vec)
	if vec.GetNulls().Any() {
		nulls := vec.GetNulls()
		for i := 0; i < n; i++ {
			if nulls.Contains(uint64(i)) {
				hashValues[i] = HashCombine(hashValues[i], 0)
			} else {
				value := CanonicalFloat64Bytes(values[i])
				hashValues[i] = HashCombine(hashValues[i], xxhash.Sum64(value[:]))
			}
		}
		return
	}
	for i := 0; i < n; i++ {
		value := CanonicalFloat64Bytes(values[i])
		hashValues[i] = HashCombine(hashValues[i], xxhash.Sum64(value[:]))
	}
}
