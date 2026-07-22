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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	ort "github.com/yalue/onnxruntime_go"
)

// tensorToNested converts an output tensor Value to a nested []any reshaped
// per outShape.Dim, ready for json marshalling. outShape carries the caller's
// declared output shape/dtype; the flat data is read from the Value.
func tensorToNested(v ort.Value, outShape *Shape) (any, error) {
	flat, err := flatData(v, outShape.Dtype)
	if err != nil {
		return nil, err
	}
	n, err := outShape.NumElements()
	if err != nil {
		return nil, err
	}
	if int64(len(flat)) != n {
		return nil, moerr.NewInternalErrorNoCtxf(
			"onnx: output has %d elements, output_shape needs %d", len(flat), n)
	}
	nested, _ := reshape(flat, outShape.Dim)
	return nested, nil
}

// reshape folds a flat slice into nested []any following dims. Returns the
// nested value and the number of flat elements consumed.
func reshape(flat []any, dims []int64) (any, int) {
	if len(dims) == 0 {
		return flat[0], 1
	}
	if len(dims) == 1 {
		return flat[:dims[0]], int(dims[0])
	}
	out := make([]any, dims[0])
	consumed := 0
	for i := int64(0); i < dims[0]; i++ {
		sub, k := reshape(flat[consumed:], dims[1:])
		out[i] = sub
		consumed += k
	}
	return out, consumed
}

// flatData reads an output tensor Value as a flat []any of normalized json
// scalars (int64/uint64/float64/bool), dispatching on the caller-declared dtype.
func flatData(v ort.Value, dt DType) ([]any, error) {
	switch dt {
	case DTFloat16:
		ct, ok := v.(*ort.CustomDataTensor)
		if !ok {
			return nil, typeMismatch(v, dt)
		}
		return f16BytesToAny(ct.GetData())
	case DTFloat32:
		data, err := tensorData[float32](v, dt)
		if err != nil {
			return nil, err
		}
		return f32sToAny(data)
	case DTFloat64:
		data, err := tensorData[float64](v, dt)
		if err != nil {
			return nil, err
		}
		return f64sToAny(data)
	case DTInt8:
		return intFlat[int8](v, dt)
	case DTUint8:
		return uintFlat[uint8](v, dt)
	case DTInt16:
		return intFlat[int16](v, dt)
	case DTUint16:
		return uintFlat[uint16](v, dt)
	case DTInt32:
		return intFlat[int32](v, dt)
	case DTUint32:
		return uintFlat[uint32](v, dt)
	case DTInt64:
		return intFlat[int64](v, dt)
	case DTUint64:
		return uintFlat[uint64](v, dt)
	case DTBool:
		data, err := tensorData[bool](v, dt)
		if err != nil {
			return nil, err
		}
		return boolsToAny(data), nil
	default:
		return nil, moerr.NewInvalidInputNoCtxf("onnx: unsupported output dtype %q", string(dt))
	}
}

func intFlat[T interface {
	signed
	ort.TensorData
}](v ort.Value, dt DType) ([]any, error) {
	data, err := tensorData[T](v, dt)
	if err != nil {
		return nil, err
	}
	return intsToAny(data), nil
}

func uintFlat[T interface {
	unsigned
	ort.TensorData
}](v ort.Value, dt DType) ([]any, error) {
	data, err := tensorData[T](v, dt)
	if err != nil {
		return nil, err
	}
	return uintsToAny(data), nil
}

// valueToJSON renders an arbitrary output Value (tensor, sequence, or map) into
// a json-encodable structure. Used when output_shape is NULL, i.e. the caller
// does not know / declare a tensor shape (e.g. sklearn sequence-of-maps).
func valueToJSON(v ort.Value) (any, error) {
	switch t := v.(type) {
	case *ort.Sequence:
		vals, err := t.GetValues()
		if err != nil {
			return nil, wrapErr(err)
		}
		out := make([]any, len(vals))
		for i, sv := range vals {
			j, err := valueToJSON(sv)
			if err != nil {
				return nil, err
			}
			out[i] = j
		}
		return out, nil
	case *ort.Map:
		keys, vals, err := t.GetKeysAndValues()
		if err != nil {
			return nil, wrapErr(err)
		}
		return mapToJSON(keys, vals)
	default:
		// A tensor of some element type. Read it generically by its ONNX type.
		return anyTensorToJSON(v)
	}
}

// mapToJSON builds a json object from an ONNX Map's key and value tensors.
// Keys are stringified; values become json scalars.
func mapToJSON(keys, vals ort.Value) (any, error) {
	kflat, err := anyTensorFlat(keys)
	if err != nil {
		return nil, err
	}
	vflat, err := anyTensorFlat(vals)
	if err != nil {
		return nil, err
	}
	if len(kflat) != len(vflat) {
		return nil, moerr.NewInternalErrorNoCtx("onnx: map key/value length mismatch")
	}
	out := make(map[string]any, len(kflat))
	for i := range kflat {
		out[scalarKey(kflat[i])] = vflat[i]
	}
	return out, nil
}

// anyTensorToJSON renders a tensor Value of unknown element type as a flat json
// array (shape is not reconstructed in the NULL-output_shape path).
func anyTensorToJSON(v ort.Value) (any, error) {
	return anyTensorFlat(v)
}
