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
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// DType is the tensor element type, spelled as in the SQL shape json
// ("int16", "float32", ...). The names mirror the onnxruntime element types
// (e.g. C.ONNX_TENSOR_ELEMENT_DATA_TYPE_INT16 -> "int16").
type DType string

const (
	DTInt8    DType = "int8"
	DTUint8   DType = "uint8"
	DTInt16   DType = "int16"
	DTUint16  DType = "uint16"
	DTInt32   DType = "int32"
	DTUint32  DType = "uint32"
	DTInt64   DType = "int64"
	DTUint64  DType = "uint64"
	DTFloat16 DType = "float16"
	DTFloat32 DType = "float32"
	DTFloat64 DType = "float64"
	DTBool    DType = "bool"
)

// valid reports whether d is a dtype supported by this package. String tensors
// are intentionally unsupported in v1.
func (d DType) valid() bool {
	switch d {
	case DTInt8, DTUint8, DTInt16, DTUint16, DTInt32, DTUint32,
		DTInt64, DTUint64, DTFloat16, DTFloat32, DTFloat64, DTBool:
		return true
	default:
		return false
	}
}

// Shape describes a tensor: its dimensions and element dtype.
//
//	{"dim": [1, 1, 4], "dtype": "int16"}
type Shape struct {
	Dim   []int64 `json:"dim"`
	Dtype DType   `json:"dtype"`
}

// ParseShape decodes and validates a shape json object.
func ParseShape(js []byte) (*Shape, error) {
	var s Shape
	if err := json.Unmarshal(js, &s); err != nil {
		return nil, moerr.NewInvalidInputNoCtxf("onnx: invalid shape json: %v", err)
	}
	if len(s.Dim) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("onnx: shape has no dimensions")
	}
	if !s.Dtype.valid() {
		return nil, moerr.NewInvalidInputNoCtxf(
			"onnx: unsupported dtype %q", string(s.Dtype))
	}
	if _, err := s.NumElements(); err != nil {
		return nil, err
	}
	return &s, nil
}

// NumElements returns the product of all dimensions, guarding against negative
// dims and int overflow.
func (s *Shape) NumElements() (int64, error) {
	n := int64(1)
	for _, d := range s.Dim {
		if d < 0 {
			return 0, moerr.NewInvalidInputNoCtxf(
				"onnx: negative dimension %d", d)
		}
		if d != 0 && n > (1<<62)/d {
			return 0, moerr.NewInvalidInputNoCtx("onnx: tensor too large")
		}
		n *= d
	}
	return n, nil
}
