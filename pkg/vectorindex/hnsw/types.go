// Copyright 2022 Matrix Origin
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

package hnsw

import (
	"fmt"
	"math"
	"strings"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/quantizer"
	usearch "github.com/unum-cloud/usearch/golang"
)

func QuantizationToUsearch(typid int32) (usearch.Quantization, error) {
	switch typid {
	case int32(types.T_array_float32):
		return usearch.F32, nil
	case int32(types.T_array_float64):
		return usearch.F64, nil
	case int32(types.T_array_float16):
		return usearch.F16, nil
	case int32(types.T_array_bf16):
		return usearch.BF16, nil
	case int32(types.T_array_int8):
		return usearch.I8, nil
	case int32(types.T_array_uint8):
		return usearch.U8, nil
	default:
		return usearch.F32, moerr.NewInternalErrorNoCtx(fmt.Sprintf("HNSW Quantization: type id not supported (type id = %d)", typid))
	}
}

// StorageQuantization returns the element type an HNSW index stores its vectors in: its
// QUANTIZATION when set, else the type of the base column.
func StorageQuantization(quantization string, baseType int32) (usearch.Quantization, error) {
	switch strings.ToLower(quantization) {
	case "":
		return QuantizationToUsearch(baseType)
	case metric.Quantization_F32_Str:
		return usearch.F32, nil
	case metric.Quantization_F64_Str:
		return usearch.F64, nil
	case metric.Quantization_F16_Str:
		return usearch.F16, nil
	case metric.Quantization_BF16_Str:
		return usearch.BF16, nil
	case metric.Quantization_INT8_Str:
		return usearch.I8, nil
	case metric.Quantization_UINT8_Str:
		return usearch.U8, nil
	default:
		return usearch.F32, moerr.NewNotSupportedNoCtxf("hnsw quantization %q", quantization)
	}
}

// QuantizeBounds is the [min,max] range an int8/uint8 model maps its vectors onto the
// integer range with; Set is false until it is trained.
type QuantizeBounds struct {
	Min, Max float64
	Set      bool
}

// NeedsQuantizeBounds reports whether vectors stored as kind are mapped with a trained range.
func NeedsQuantizeBounds(kind usearch.Quantization) bool {
	return kind == usearch.I8 || kind == usearch.U8
}

// quantizeParams returns the affine map q(x) = x*mul + add of bounds for kind.
func quantizeParams(kind usearch.Quantization, bounds QuantizeBounds) (mul, add float64) {
	if kind == usearch.U8 {
		return quantizer.Uint8Params(bounds.Min, bounds.Max)
	}
	return quantizer.Int8Params(bounds.Min, bounds.Max)
}

// TrainQuantizeBounds returns the range an int8/uint8 index maps the sample vectors with.
func TrainQuantizeBounds[T types.ArrayElement](sample [][]T) QuantizeBounds {
	var min, max float64
	switch x := any(sample).(type) {
	case [][]float32:
		min, max = quantizer.TrainInt8(x)
	case [][]float64:
		min, max = quantizer.TrainInt8(x)
	default:
		widened := make([][]float32, len(sample))
		for i, v := range sample {
			widened[i] = toFloat32(v)
		}
		min, max = quantizer.TrainInt8(widened)
	}
	return QuantizeBounds{Min: min, Max: max, Set: true}
}

// toFloat32 returns vec as float32: vec itself when it already is, else a widened copy.
func toFloat32[T types.ArrayElement](vec []T) []float32 {
	switch x := any(vec).(type) {
	case []float32:
		return x
	case []float64:
		out := make([]float32, len(x))
		for i, e := range x {
			out[i] = float32(e)
		}
		return out
	}
	out, _ := WidenToFloat32(vec)
	return out
}

// RescaleDistances turns the squared L2 distances usearch computes between quantized vectors
// back into the units of the base column: the affine map scales every difference by mul.
func RescaleDistances(distances []float32, kind usearch.Quantization, bounds QuantizeBounds) {
	if !NeedsQuantizeBounds(kind) {
		return
	}
	mul, _ := quantizeParams(kind, bounds)
	scale := mul * mul
	for i := range distances {
		distances[i] = float32(float64(distances[i]) / scale)
	}
}

// ToStorage converts vec to kind, the element type the index stores: usearch reads its
// input as exactly that type. A vector already of that type is used as it is. The second
// result holds the vector usearch reads, which the caller keeps alive while usearch reads the
// pointer.
//
// An int8/uint8 kind maps each element with the trained bounds; a vecint8/vecuint8 vector
// mapped with the range of its own type is used as it is.
func ToStorage[T types.ArrayElement](vec []T, kind usearch.Quantization, bounds QuantizeBounds) (unsafe.Pointer, any, error) {
	if len(vec) == 0 {
		return nil, nil, moerr.NewInternalErrorNoCtx("usearch vector is empty")
	}
	if !allFinite(vec) {
		return nil, nil, moerr.NewInvalidInputNoCtx("hnsw vector has a NaN or infinite element")
	}
	if storedAsIs(vec, kind, bounds) {
		return unsafe.Pointer(&vec[0]), vec, nil
	}
	f64, isF64 := any(vec).([]float64)
	f32 := toFloat32(vec)
	src := func(i int) float64 {
		if isF64 {
			return f64[i]
		}
		return float64(f32[i])
	}
	switch kind {
	case usearch.F32:
		for i := range f32 {
			if err := checkNarrowed(src(i), float64(f32[i]), kind); err != nil {
				return nil, nil, err
			}
		}
		return unsafe.Pointer(&f32[0]), f32, nil
	case usearch.F64:
		out := make([]float64, len(f32))
		for i := range out {
			out[i] = src(i)
		}
		return unsafe.Pointer(&out[0]), out, nil
	case usearch.F16:
		out := make([]types.Float16, len(f32))
		for i, x := range f32 {
			out[i] = types.Float16FromFloat32(x)
			if err := checkNarrowed(src(i), float64(out[i].ToFloat32()), kind); err != nil {
				return nil, nil, err
			}
		}
		return unsafe.Pointer(&out[0]), out, nil
	case usearch.BF16:
		out := make([]types.BF16, len(f32))
		for i, x := range f32 {
			out[i] = types.BF16FromFloat32(x)
			if err := checkNarrowed(src(i), float64(out[i].ToFloat32()), kind); err != nil {
				return nil, nil, err
			}
		}
		return unsafe.Pointer(&out[0]), out, nil
	case usearch.I8, usearch.U8:
		if !bounds.Set {
			return nil, nil, moerr.NewInternalErrorNoCtx("hnsw int8/uint8 model has no trained quantization range")
		}
		mul, add := quantizeParams(kind, bounds)
		if err := checkQuantizeParams(mul, add, bounds); err != nil {
			return nil, nil, err
		}
		if isF64 {
			for i := range f32 {
				if err := checkNarrowed(f64[i], float64(f32[i]), usearch.F32); err != nil {
					return nil, nil, err
				}
			}
		}
		if kind == usearch.U8 {
			out := quantizer.ApplyUint8(f32, mul, add)
			return unsafe.Pointer(&out[0]), out, nil
		}
		out := quantizer.ApplyInt8(f32, mul, add)
		return unsafe.Pointer(&out[0]), out, nil
	default:
		return nil, nil, moerr.NewNotSupportedNoCtxf("hnsw storage type %v", kind)
	}
}

// CheckStorable returns the reason an index storing kind cannot store vec, or nil. It
// applies the same element checks as ToStorage; the range of an int8/uint8 index does not
// reject a vector.
func CheckStorable[T types.ArrayElement](vec []T, kind usearch.Quantization) error {
	var bounds QuantizeBounds
	if NeedsQuantizeBounds(kind) {
		bounds = QuantizeBounds{Min: -1, Max: 1, Set: true}
	}
	_, _, err := ToStorage(vec, kind, bounds)
	return err
}

// allFinite reports whether no element of vec is NaN or infinite.
func allFinite[T types.ArrayElement](vec []T) bool {
	switch x := any(vec).(type) {
	case []float32:
		return metric.AllFiniteF32(x)
	case []float64:
		for _, e := range x {
			if e-e != 0 {
				return false
			}
		}
	case []types.Float16:
		for _, e := range x {
			if uint16(e)&0x7c00 == 0x7c00 {
				return false
			}
		}
	case []types.BF16:
		for _, e := range x {
			if uint16(e)&0x7f80 == 0x7f80 {
				return false
			}
		}
	}
	return true
}

// storedAsIs reports whether vec already is in the element type kind stores.
func storedAsIs[T types.ArrayElement](vec []T, kind usearch.Quantization, bounds QuantizeBounds) bool {
	switch any(vec).(type) {
	case []float32:
		return kind == usearch.F32
	case []float64:
		return kind == usearch.F64
	case []types.Float16:
		return kind == usearch.F16
	case []types.BF16:
		return kind == usearch.BF16
	case []int8:
		return kind == usearch.I8 && bounds == BaseBounds(int32(types.T_array_int8))
	case []uint8:
		return kind == usearch.U8 && bounds == BaseBounds(int32(types.T_array_uint8))
	}
	return false
}

// checkQuantizeParams rejects a range whose map q(x) = x*mul + add is not finite in the
// float32 arithmetic it is applied in.
func checkQuantizeParams(mul, add float64, bounds QuantizeBounds) error {
	fmul, fadd := float64(float32(mul)), float64(float32(add))
	if fmul == 0 || math.IsInf(fmul, 0) || math.IsInf(fadd, 0) {
		return moerr.NewOutOfRangeNoCtxf("hnsw quantization", "range [%v, %v] cannot be mapped onto int8/uint8 in float32", bounds.Min, bounds.Max)
	}
	return nil
}

// storedFloat32 returns vec as float32 after narrowing it to kind, the float element type
// the index stores.
func storedFloat32[T types.ArrayElement](vec []T, kind usearch.Quantization) []float32 {
	f32 := toFloat32(vec)
	switch kind {
	case usearch.F16:
		out := make([]float32, len(f32))
		for i, x := range f32 {
			out[i] = types.Float16FromFloat32(x).ToFloat32()
		}
		return out
	case usearch.BF16:
		out := make([]float32, len(f32))
		for i, x := range f32 {
			out[i] = types.BF16FromFloat32(x).ToFloat32()
		}
		return out
	}
	return f32
}

// checkNarrowed rejects a finite element that overflows the element type the index stores.
func checkNarrowed(x, stored float64, kind usearch.Quantization) error {
	if math.IsInf(stored, 0) && !math.IsInf(x, 0) {
		return moerr.NewOutOfRangeNoCtxf("hnsw quantization", "vector element %v cannot be stored as %s", x, kind.String())
	}
	return nil
}

// BaseBounds returns the bounds an int8/uint8 index of a vecint8/vecuint8 column maps its
// vectors with: the range of the column type, so stored elements equal the column's values
// and need no training. Other columns have none.
func BaseBounds(baseType int32) QuantizeBounds {
	switch baseType {
	case int32(types.T_array_int8):
		return QuantizeBounds{Min: math.MinInt8, Max: math.MaxInt8, Set: true}
	case int32(types.T_array_uint8):
		return QuantizeBounds{Min: 0, Max: math.MaxUint8, Set: true}
	}
	return QuantizeBounds{}
}

// WidenToFloat32 converts a vector of a vecf16/vecbf16/vecint8/vecuint8 column to float32;
// ok is false for any other value.
func WidenToFloat32(v any) (out []float32, ok bool) {
	switch x := v.(type) {
	case []types.Float16:
		out = make([]float32, len(x))
		for i, e := range x {
			out[i] = e.ToFloat32()
		}
	case []types.BF16:
		out = make([]float32, len(x))
		for i, e := range x {
			out[i] = e.ToFloat32()
		}
	case []int8:
		out = make([]float32, len(x))
		for i, e := range x {
			out[i] = float32(e)
		}
	case []uint8:
		out = make([]float32, len(x))
		for i, e := range x {
			out[i] = float32(e)
		}
	default:
		return nil, false
	}
	return out, true
}

// BytesToFloat32 decodes a vector of a vecf16/vecbf16/vecint8/vecuint8 column into float32;
// ok is false for any other type.
func BytesToFloat32(typ int32, data []byte) ([]float32, bool) {
	switch typ {
	case int32(types.T_array_float16):
		return WidenToFloat32(types.BytesToArray[types.Float16](data))
	case int32(types.T_array_bf16):
		return WidenToFloat32(types.BytesToArray[types.BF16](data))
	case int32(types.T_array_int8):
		return WidenToFloat32(types.BytesToArray[int8](data))
	case int32(types.T_array_uint8):
		return WidenToFloat32(types.BytesToArray[uint8](data))
	}
	return nil, false
}
