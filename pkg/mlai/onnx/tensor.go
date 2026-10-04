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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	ort "github.com/yalue/onnxruntime_go"
)

// buildInputTensor turns the flat json `input` array into an ort.Value tensor
// shaped and typed per `s`. The json input is a flat array of scalars whose
// length must equal the product of the dimensions.
func buildInputTensor(inputJSON []byte, s *Shape) (ort.Value, error) {
	n, err := s.NumElements()
	if err != nil {
		return nil, err
	}
	shape := ort.NewShape(s.Dim...)

	switch s.Dtype {
	case DTBool:
		vals, err := decodeBools(inputJSON, n)
		if err != nil {
			return nil, err
		}
		return ort.NewTensor(shape, vals)
	case DTFloat16:
		nums, err := decodeNumbers(inputJSON, n)
		if err != nil {
			return nil, err
		}
		raw := make([]byte, 2*n)
		for i, num := range nums {
			f, err := num.Float64()
			if err != nil {
				return nil, badNumber(num)
			}
			binary.LittleEndian.PutUint16(raw[2*i:], float32ToFloat16(float32(f)))
		}
		return ort.NewCustomDataTensor(shape, raw, ort.TensorElementDataTypeFloat16)
	default:
		nums, err := decodeNumbers(inputJSON, n)
		if err != nil {
			return nil, err
		}
		return numericTensor(shape, s.Dtype, nums)
	}
}

// buildOutputTensor pre-allocates an empty output tensor matching the declared
// output shape and dtype. Pre-allocating (rather than letting Run
// auto-allocate) gives predictable types and works around an onnxruntime_go
// sizing bug for auto-allocated float16 outputs.
func buildOutputTensor(s *Shape) (ort.Value, error) {
	n, err := s.NumElements()
	if err != nil {
		return nil, err
	}
	shape := ort.NewShape(s.Dim...)
	switch s.Dtype {
	case DTFloat16:
		return ort.NewCustomDataTensor(shape, make([]byte, 2*n), ort.TensorElementDataTypeFloat16)
	case DTBool:
		return ort.NewEmptyTensor[bool](shape)
	case DTInt8:
		return ort.NewEmptyTensor[int8](shape)
	case DTUint8:
		return ort.NewEmptyTensor[uint8](shape)
	case DTInt16:
		return ort.NewEmptyTensor[int16](shape)
	case DTUint16:
		return ort.NewEmptyTensor[uint16](shape)
	case DTInt32:
		return ort.NewEmptyTensor[int32](shape)
	case DTUint32:
		return ort.NewEmptyTensor[uint32](shape)
	case DTInt64:
		return ort.NewEmptyTensor[int64](shape)
	case DTUint64:
		return ort.NewEmptyTensor[uint64](shape)
	case DTFloat32:
		return ort.NewEmptyTensor[float32](shape)
	case DTFloat64:
		return ort.NewEmptyTensor[float64](shape)
	default:
		return nil, moerr.NewInvalidInputNoCtxf("onnx: unsupported output dtype %q", string(s.Dtype))
	}
}

// numericTensor builds a typed tensor from decoded json numbers for the
// integer / 32-64 bit float dtypes.
func numericTensor(shape ort.Shape, dt DType, nums []json.Number) (ort.Value, error) {
	switch dt {
	case DTInt8:
		return intTensor[int8](shape, dt, nums, 8)
	case DTUint8:
		return uintTensor[uint8](shape, dt, nums, 8)
	case DTInt16:
		return intTensor[int16](shape, dt, nums, 16)
	case DTUint16:
		return uintTensor[uint16](shape, dt, nums, 16)
	case DTInt32:
		return intTensor[int32](shape, dt, nums, 32)
	case DTUint32:
		return uintTensor[uint32](shape, dt, nums, 32)
	case DTInt64:
		return intTensor[int64](shape, dt, nums, 64)
	case DTUint64:
		return uintTensor[uint64](shape, dt, nums, 64)
	case DTFloat32:
		out := make([]float32, len(nums))
		for i, num := range nums {
			f, err := num.Float64()
			if err != nil {
				return nil, badNumber(num)
			}
			out[i] = float32(f)
		}
		return ort.NewTensor(shape, out)
	case DTFloat64:
		out := make([]float64, len(nums))
		for i, num := range nums {
			f, err := num.Float64()
			if err != nil {
				return nil, badNumber(num)
			}
			out[i] = f
		}
		return ort.NewTensor(shape, out)
	default:
		return nil, moerr.NewInvalidInputNoCtxf("onnx: unsupported dtype %q", string(dt))
	}
}

type signed interface {
	~int8 | ~int16 | ~int32 | ~int64
}
type unsigned interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64
}

// intTensor / uintTensor parse json numbers as integers of the declared width.
// Parse and range errors are reported, never silently truncated: "1.5" as an
// int dtype is an error (not 0), and 300 as int8 is an error (not a wrap to 44).
func intTensor[T signed](shape ort.Shape, dt DType, nums []json.Number, bitSize int) (ort.Value, error) {
	out := make([]T, len(nums))
	for i, num := range nums {
		v, err := strconv.ParseInt(string(num), 10, bitSize)
		if err != nil {
			return nil, badIntInput(num, dt, err)
		}
		out[i] = T(v)
	}
	return ort.NewTensor(shape, out)
}

func uintTensor[T unsigned](shape ort.Shape, dt DType, nums []json.Number, bitSize int) (ort.Value, error) {
	out := make([]T, len(nums))
	for i, num := range nums {
		v, err := strconv.ParseUint(string(num), 10, bitSize)
		if err != nil {
			return nil, badIntInput(num, dt, err)
		}
		out[i] = T(v)
	}
	return ort.NewTensor(shape, out)
}

func badIntInput(num json.Number, dt DType, err error) error {
	if errors.Is(err, strconv.ErrRange) {
		return moerr.NewInvalidInputNoCtxf(
			"onnx: input %q out of range for dtype %q", string(num), string(dt))
	}
	return moerr.NewInvalidInputNoCtxf(
		"onnx: invalid integer input %q for dtype %q", string(num), string(dt))
}

// decodeNumbers decodes a flat json array of numbers and checks its length.
// Trailing content after the array (e.g. "[1,2]junk") is rejected.
func decodeNumbers(js []byte, want int64) ([]json.Number, error) {
	dec := json.NewDecoder(bytes.NewReader(js))
	dec.UseNumber()
	var arr []json.Number
	if err := dec.Decode(&arr); err != nil {
		return nil, moerr.NewInvalidInputNoCtxf("onnx: invalid input json: %v", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, moerr.NewInvalidInputNoCtx("onnx: trailing data after input json array")
	}
	if int64(len(arr)) != want {
		return nil, moerr.NewInvalidInputNoCtxf(
			"onnx: input has %d elements, shape needs %d", len(arr), want)
	}
	return arr, nil
}

func decodeBools(js []byte, want int64) ([]bool, error) {
	var arr []bool
	if err := json.Unmarshal(js, &arr); err != nil {
		return nil, moerr.NewInvalidInputNoCtxf("onnx: invalid bool input json: %v", err)
	}
	if int64(len(arr)) != want {
		return nil, moerr.NewInvalidInputNoCtxf(
			"onnx: input has %d elements, shape needs %d", len(arr), want)
	}
	return arr, nil
}

func badNumber(n json.Number) error {
	return moerr.NewInvalidInputNoCtxf("onnx: bad numeric input %q", string(n))
}
