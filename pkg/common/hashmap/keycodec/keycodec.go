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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

var positiveFloat64Zero [8]byte

// CanonicalFloat64Bits makes SQL-equal signed zero values use one hash key.
func CanonicalFloat64Bits(value float64) uint64 {
	if value == 0 {
		return 0
	}
	return math.Float64bits(value)
}

// BytesAt returns the canonical hash-key bytes for one vector row.
func BytesAt(vec *vector.Vector, row int) []byte {
	if vec.GetType().Oid == types.T_float64 &&
		vector.GetFixedAtNoTypeCheck[float64](vec, row) == 0 {
		return positiveFloat64Zero[:]
	}
	return vec.GetRawBytesAt(row)
}
