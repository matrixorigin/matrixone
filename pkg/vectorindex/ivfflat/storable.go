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

package ivfflat

import (
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/quantizer"
)

// MaxSquaredNormF32 is the largest squared L2 norm an index computing distances in float32
// stores: the squared distance of two such vectors is at most (||a|| + ||b||)^2 <= 4 *
// MaxSquaredNormF32, the float32 maximum.
const MaxSquaredNormF32 = float64(math.MaxFloat32) / 4

// MaxSquaredNormF64 is MaxSquaredNormF32 for an index computing distances in float64.
const MaxSquaredNormF64 = math.MaxFloat64 / 4

// bf16Overflow is the smallest float32 magnitude that rounds to infinity as bf16.
const bf16Overflow = float32(3.3961775e38)

// Storage is how an IVF-FLAT index stores a vector of its base column.
type Storage struct {
	// Base is the type of the source column.
	Base types.T
	// F32 reports that the centroids are float32, so distances are computed in float32.
	F32 bool
	// Entry is the type of the entries column.
	Entry types.T
	// Mul and Add map a float32 element to an int8 or uint8 entry element.
	Mul, Add float64
}

// NewStorage returns the storage of an index on a base column with quantization q, whose
// trained int8/uint8 bounds are min and max when bounded is true.
func NewStorage(base types.T, q string, min, max float64, bounded bool) Storage {
	s := Storage{Base: base, Entry: base, Mul: 1}
	qt, quantized := quantizer.ToVectorType(q)
	if quantized {
		s.Entry = qt
	}
	s.F32 = base != types.T_array_float64 || quantized
	if bounded {
		// typeswitch:partial only the integer entry types have trained bounds
		switch s.Entry {
		case types.T_array_int8:
			s.Mul, s.Add = quantizer.Int8Params(min, max)
		case types.T_array_uint8:
			s.Mul, s.Add = quantizer.Uint8Params(min, max)
		}
	}
	return s
}

// Check returns the reason the index cannot store vec, the bytes of a base vector, or nil.
func (s Storage) Check(vec []byte) error {
	// typeswitch:partial int8/uint8 bases need no check; vecf8/vecf4 cannot carry a vector index (rejected at DDL)
	switch s.Base {
	case types.T_array_float64:
		return s.checkF64(types.BytesToArray[float64](vec))
	case types.T_array_float32:
		return s.checkF32(types.BytesToArray[float32](vec))
	case types.T_array_bf16:
		return s.checkF32(types.BF16ToFloat32Slice(types.BytesToArray[types.BF16](vec)))
	case types.T_array_float16:
		return s.checkF32(types.Float16ToFloat32Slice(types.BytesToArray[types.Float16](vec)))
	}
	// int8 and uint8 elements are stored as they are and their norm is bounded
	return nil
}

// CheckQuery returns the reason the index cannot search for query, a vector decoded to the
// centroid element type T, or nil.
func CheckQuery[T types.RealNumbers](s Storage, query []T) error {
	switch q := any(query).(type) {
	case []float32:
		return s.checkF32(q)
	case []float64:
		return s.checkF64(q)
	}
	return nil
}

func (s Storage) checkF64(vec []float64) error {
	if len(vec) == 0 {
		return moerr.NewIndexVectorDimensionNoCtx("the vector is empty")
	}
	for _, e := range vec {
		if math.IsNaN(e) || math.IsInf(e, 0) {
			return moerr.NewIndexVectorNonFiniteNoCtx()
		}
	}
	if !s.F32 {
		var sum float64
		for _, e := range vec {
			sum += e * e
		}
		if sum > MaxSquaredNormF64 {
			return moerr.NewIndexVectorOutOfRangeNoCtx(fmt.Sprintf("the squared L2 norm %g exceeds %g", sum, MaxSquaredNormF64))
		}
		return nil
	}
	f32 := make([]float32, len(vec))
	for i, e := range vec {
		f32[i] = float32(e)
		if math.IsInf(float64(f32[i]), 0) {
			return moerr.NewIndexVectorOutOfRangeNoCtx(fmt.Sprintf("vector element %v cannot be stored as float32", e))
		}
	}
	return s.checkF32(f32)
}

func (s Storage) checkF32(vec []float32) error {
	if len(vec) == 0 {
		return moerr.NewIndexVectorDimensionNoCtx("the vector is empty")
	}
	limit := float32(math.Inf(1))
	// typeswitch:partial only the low-precision float entry types have a finite limit below float32
	switch s.Entry {
	case types.T_array_float16:
		limit = vectorindex.Float16Overflow
	case types.T_array_bf16:
		limit = bf16Overflow
	}
	fmul, fadd := float32(s.Mul), float32(s.Add)
	// typeswitch:partial only the integer entry types are scaled
	scaled := s.Entry == types.T_array_int8 || s.Entry == types.T_array_uint8
	var sum float64
	for _, f := range vec {
		if f != f || math.IsInf(float64(f), 0) {
			return moerr.NewIndexVectorNonFiniteNoCtx()
		}
		if f >= limit || f <= -limit {
			return moerr.NewIndexVectorOutOfRangeNoCtx(fmt.Sprintf("vector element %v cannot be stored as %s", f, quantizer.SQLTypeName(s.Entry)))
		}
		if scaled {
			if q := float32(f*fmul) + fadd; q != q || math.IsInf(float64(q), 0) {
				return moerr.NewIndexVectorOutOfRangeNoCtx(fmt.Sprintf("vector element %v cannot be stored as %s", f, quantizer.SQLTypeName(s.Entry)))
			}
		}
		sum += float64(f) * float64(f)
	}
	if sum > MaxSquaredNormF32 {
		return moerr.NewIndexVectorOutOfRangeNoCtx(fmt.Sprintf("the squared L2 norm %g exceeds %g", sum, MaxSquaredNormF32))
	}
	return nil
}
