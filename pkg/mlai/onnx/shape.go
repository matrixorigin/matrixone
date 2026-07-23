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
	return d.width() != 0
}

// width returns the element width in bytes, or 0 for an unsupported dtype.
func (d DType) width() int64 {
	switch d {
	case DTInt8, DTUint8, DTBool:
		return 1
	case DTInt16, DTUint16, DTFloat16:
		return 2
	case DTInt32, DTUint32, DTFloat32:
		return 4
	case DTInt64, DTUint64, DTFloat64:
		return 8
	default:
		return 0
	}
}

// MaxTensorBytes caps the in-memory size of a single declared tensor
// (element count x element width). Tensors are allocated from the
// user-supplied shape BEFORE onnxruntime validates it against the model, so
// an unchecked shape would let plain SQL input OOM the CN. 64 MB mirrors
// types.MaxBlobLen: a result tensor larger than that could not be returned
// as a json value anyway.
const MaxTensorBytes = 64 << 20

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
	n, err := s.NumElements()
	if err != nil {
		return nil, err
	}
	// Bound the declared tensor size; the division form avoids any overflow
	// in n*width.
	if w := s.Dtype.width(); n > MaxTensorBytes/w {
		return nil, moerr.NewInvalidInputNoCtxf(
			"onnx: declared tensor size %d elements x %d bytes exceeds the %d MB limit",
			n, w, MaxTensorBytes>>20)
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
