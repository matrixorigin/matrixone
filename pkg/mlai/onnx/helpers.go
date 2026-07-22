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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	ort "github.com/yalue/onnxruntime_go"
)

// anyTensorFlat reads a tensor Value of any supported element type into a flat
// []any of json scalars, inferring the element type from the concrete Go type.
// Used on the NULL-output_shape path where the caller did not declare a dtype.
func anyTensorFlat(v ort.Value) ([]any, error) {
	switch t := v.(type) {
	case *ort.Tensor[float32]:
		return toAny(t.GetData()), nil
	case *ort.Tensor[float64]:
		return toAny(t.GetData()), nil
	case *ort.Tensor[int8]:
		return toAny(t.GetData()), nil
	case *ort.Tensor[uint8]:
		return toAny(t.GetData()), nil
	case *ort.Tensor[int16]:
		return toAny(t.GetData()), nil
	case *ort.Tensor[uint16]:
		return toAny(t.GetData()), nil
	case *ort.Tensor[int32]:
		return toAny(t.GetData()), nil
	case *ort.Tensor[uint32]:
		return toAny(t.GetData()), nil
	case *ort.Tensor[int64]:
		return toAny(t.GetData()), nil
	case *ort.Tensor[uint64]:
		return toAny(t.GetData()), nil
	case *ort.Tensor[bool]:
		return toAny(t.GetData()), nil
	case *ort.CustomDataTensor:
		// Best-effort: treat raw bytes as float16 (the only custom type this
		// package produces). Other custom types are unsupported here.
		if t.DataType() == ort.TensorElementDataTypeFloat16 {
			raw := t.GetData()
			out := make([]any, len(raw)/2)
			for i := range out {
				out[i] = float16ToFloat32(binary.LittleEndian.Uint16(raw[2*i:]))
			}
			return out, nil
		}
		return nil, moerr.NewNotSupportedNoCtx("onnx: unsupported custom output tensor type")
	default:
		return nil, moerr.NewNotSupportedNoCtxf("onnx: unsupported output value type %T", v)
	}
}

func toAny[T any](s []T) []any {
	out := make([]any, len(s))
	for i, x := range s {
		out[i] = x
	}
	return out
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
