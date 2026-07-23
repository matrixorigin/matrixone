// Copyright 2021 - 2025 Matrix Origin
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

package onnx

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	ort "github.com/yalue/onnxruntime_go"
)

// Output scalars are normalized to the exact set bytejson.CreateByteJSON
// accepts (int64 / uint64 / float64 / bool / string), so the operator can build
// a ByteJson directly from Run's value tree with no json-text round-trip.

// float32ToJSONFloat64 widens a float32 through its shortest decimal
// representation. This yields the same float64 that json text parsing produced
// before (so rendered output is unchanged: 1.9999883, not 1.9999883079528809).
// NaN/Inf are rejected, matching encoding/json's behavior on the old path.
func float32ToJSONFloat64(v float32) (float64, error) {
	f := float64(v)
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, nonFiniteErr()
	}
	f, _ = strconv.ParseFloat(strconv.FormatFloat(f, 'g', -1, 32), 64)
	return f, nil
}

func checkFiniteFloat64(v float64) (float64, error) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, nonFiniteErr()
	}
	return v, nil
}

func nonFiniteErr() error {
	return moerr.NewInvalidInputNoCtx("onnx: model produced a non-finite float (NaN or Inf), not representable in json")
}

func intsToAny[T signed](s []T) []any {
	out := make([]any, len(s))
	for i, x := range s {
		out[i] = int64(x)
	}
	return out
}

func uintsToAny[T unsigned](s []T) []any {
	out := make([]any, len(s))
	for i, x := range s {
		out[i] = uint64(x)
	}
	return out
}

func f32sToAny(s []float32) ([]any, error) {
	out := make([]any, len(s))
	for i, x := range s {
		f, err := float32ToJSONFloat64(x)
		if err != nil {
			return nil, err
		}
		out[i] = f
	}
	return out, nil
}

func f64sToAny(s []float64) ([]any, error) {
	out := make([]any, len(s))
	for i, x := range s {
		f, err := checkFiniteFloat64(x)
		if err != nil {
			return nil, err
		}
		out[i] = f
	}
	return out, nil
}

func boolsToAny(s []bool) []any {
	out := make([]any, len(s))
	for i, x := range s {
		out[i] = x
	}
	return out
}

// f16BytesToAny converts raw little-endian float16 bytes to normalized floats.
func f16BytesToAny(raw []byte) ([]any, error) {
	out := make([]any, len(raw)/2)
	for i := range out {
		f, err := float32ToJSONFloat64(float16ToFloat32(binary.LittleEndian.Uint16(raw[2*i:])))
		if err != nil {
			return nil, err
		}
		out[i] = f
	}
	return out, nil
}

// tensorData asserts v is a tensor of element type T and returns its data.
func tensorData[T ort.TensorData](v ort.Value, dt DType) ([]T, error) {
	t, ok := v.(*ort.Tensor[T])
	if !ok {
		return nil, typeMismatch(v, dt)
	}
	return t.GetData(), nil
}

// anyTensorFlat reads a tensor Value of any supported element type into a flat
// []any of normalized json scalars, inferring the element type from the
// concrete Go type. Used on the NULL-output_shape path where the caller did not
// declare a dtype.
//
// Model-produced (auto-allocated) outputs never pass through ParseShape, so
// the raw-byte cap is applied here as well. The element count is checked
// against MaxTensorBytes/8 — conservative for narrow element types, exact for
// the widest — which keeps the guard to a single check for all branches.
func anyTensorFlat(v ort.Value) ([]any, error) {
	if n := v.GetShape().FlattenedSize(); n < 0 || n > MaxTensorBytes/8 {
		return nil, moerr.NewInvalidInputNoCtxf(
			"onnx: model output tensor of %d elements exceeds the %d MB limit",
			n, MaxTensorBytes>>20)
	}
	switch t := v.(type) {
	case *ort.Tensor[float32]:
		return f32sToAny(t.GetData())
	case *ort.Tensor[float64]:
		return f64sToAny(t.GetData())
	case *ort.Tensor[int8]:
		return intsToAny(t.GetData()), nil
	case *ort.Tensor[uint8]:
		return uintsToAny(t.GetData()), nil
	case *ort.Tensor[int16]:
		return intsToAny(t.GetData()), nil
	case *ort.Tensor[uint16]:
		return uintsToAny(t.GetData()), nil
	case *ort.Tensor[int32]:
		return intsToAny(t.GetData()), nil
	case *ort.Tensor[uint32]:
		return uintsToAny(t.GetData()), nil
	case *ort.Tensor[int64]:
		return intsToAny(t.GetData()), nil
	case *ort.Tensor[uint64]:
		return uintsToAny(t.GetData()), nil
	case *ort.Tensor[bool]:
		return boolsToAny(t.GetData()), nil
	case *ort.CustomDataTensor:
		// Best-effort: treat raw bytes as float16 (the only custom type this
		// package produces). Other custom types are unsupported here.
		if t.DataType() == ort.TensorElementDataTypeFloat16 {
			return f16BytesToAny(t.GetData())
		}
		return nil, moerr.NewNotSupportedNoCtx("onnx: unsupported custom output tensor type")
	default:
		return nil, moerr.NewNotSupportedNoCtxf("onnx: unsupported output value type %T", v)
	}
}

// scalarKey stringifies a scalar for use as a json object key (ONNX map keys
// are typically int64 or string).
func scalarKey(v any) string {
	return fmt.Sprintf("%v", v)
}

func typeMismatch(v ort.Value, dt DType) error {
	return moerr.NewInvalidInputNoCtxf(
		"onnx: output value type %T does not match declared dtype %q", v, string(dt))
}

func wrapErr(err error) error {
	return moerr.NewInternalErrorNoCtxf("onnx: %v", err)
}
