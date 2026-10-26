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
func TrainQuantizeBounds[T types.RealNumbers](sample [][]T) QuantizeBounds {
	min, max := quantizer.TrainInt8(sample)
	return QuantizeBounds{Min: min, Max: max, Set: true}
}

// RescaleDistances turns the squared L2 distances usearch computes between quantized vectors
// back into the units of the base column: the affine map scales every difference by mul.
func RescaleDistances(distances []float32, kind usearch.Quantization, bounds QuantizeBounds) {
	if !NeedsQuantizeBounds(kind) {
		return
	}
	mul, _ := quantizeParams(kind, bounds)
	scale := float32(mul * mul)
	for i := range distances {
		distances[i] /= scale
	}
}

// ToStorage converts vec to kind, the element type the index stores: usearch reads its
// input as exactly that type. The second result holds the converted vector, which the
// caller keeps alive while usearch reads the pointer.
//
// An int8/uint8 kind maps each element with the trained bounds.
func ToStorage[T types.RealNumbers](vec []T, kind usearch.Quantization, bounds QuantizeBounds) (unsafe.Pointer, any, error) {
	if len(vec) == 0 {
		return nil, nil, moerr.NewInternalErrorNoCtx("usearch vector is empty")
	}
	switch kind {
	case usearch.F32:
		if v, ok := any(vec).([]float32); ok {
			return unsafe.Pointer(&v[0]), v, nil
		}
		out := make([]float32, len(vec))
		for i, x := range vec {
			out[i] = float32(x)
		}
		return unsafe.Pointer(&out[0]), out, nil
	case usearch.F64:
		if v, ok := any(vec).([]float64); ok {
			return unsafe.Pointer(&v[0]), v, nil
		}
		out := make([]float64, len(vec))
		for i, x := range vec {
			out[i] = float64(x)
		}
		return unsafe.Pointer(&out[0]), out, nil
	case usearch.F16:
		out := make([]types.Float16, len(vec))
		for i, x := range vec {
			out[i] = types.Float16FromFloat32(float32(x))
		}
		return unsafe.Pointer(&out[0]), out, nil
	case usearch.BF16:
		out := make([]types.BF16, len(vec))
		for i, x := range vec {
			out[i] = types.BF16FromFloat32(float32(x))
		}
		return unsafe.Pointer(&out[0]), out, nil
	case usearch.I8, usearch.U8:
		if !bounds.Set {
			return nil, nil, moerr.NewInternalErrorNoCtx("hnsw int8/uint8 model has no trained quantization range")
		}
		f32 := make([]float32, len(vec))
		for i, x := range vec {
			f32[i] = float32(x)
		}
		mul, add := quantizeParams(kind, bounds)
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
