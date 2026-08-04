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

package function

import (
	"bytes"
	"math"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// SerialEncodedTypeSizeBound returns the maximum number of bytes which the
// production component encoder can append for one non-NULL value of typ.
// Unlike a function's generic VARCHAR return width, this contract follows the
// concrete tuple encoding and the input type's declared payload bound.
//
// The bool is false only when the production encoder does not support typ.
// Callers must retain their representation-independent fallback in that case.
func SerialEncodedTypeSizeBound(typ types.Type) (uint64, bool) {
	if fixed, ok := serialFixedEncodedTypeSizeBound(typ.Oid); ok {
		return fixed, true
	}

	payload, ok := serialTypePayloadSizeBound(typ)
	if !ok || payload > (math.MaxUint64-3)/2 {
		return 0, false
	}
	// string-type code + bytes code + terminator; every payload byte can be
	// zero and therefore require one escape byte.
	return 2*payload + 3, true
}

func serialFixedEncodedTypeSizeBound(oid types.T) (uint64, bool) {
	// Integer-like values use one type byte, one ordered-length byte, and the
	// maximum bytes of their concrete storage width. The remaining encodings
	// are fixed-width in Packer.
	switch oid {
	case types.T_bool:
		return 1, true
	case types.T_int8, types.T_uint8:
		return 3, true
	case types.T_int16, types.T_uint16, types.T_year:
		return 4, true
	case types.T_int32, types.T_uint32, types.T_date:
		return 6, true
	case types.T_bit, types.T_int64, types.T_uint64,
		types.T_time, types.T_datetime, types.T_timestamp:
		return 10, true
	case types.T_float32:
		return 5, true
	case types.T_float64:
		return 9, true
	case types.T_enum:
		// Enum code followed by an encoded uint16 (type + length + data).
		return 5, true
	case types.T_decimal64:
		return 9, true
	case types.T_decimal128:
		return 17, true
	case types.T_uuid:
		return 17, true
	default:
		return 0, false
	}
}

func serialTypePayloadSizeBound(typ types.Type) (uint64, bool) {
	declared := int64(typ.Width)
	declaredOr := func(fallback int64) uint64 {
		if declared <= 0 {
			declared = fallback
		}
		return uint64(declared)
	}
	switch typ.Oid {
	case types.T_char:
		return declaredOr(types.MaxCharLen) * utf8.UTFMax, true
	case types.T_varchar:
		// CHAR/VARCHAR widths are characters, while the packer consumes stored
		// UTF-8 bytes. One valid character can therefore occupy UTFMax bytes.
		return declaredOr(types.MaxVarcharLen) * utf8.UTFMax, true
	case types.T_binary:
		return declaredOr(types.MaxBinaryLen), true
	case types.T_varbinary:
		return declaredOr(types.MaxVarBinaryLen), true
	case types.T_array_float32, types.T_array_float64,
		types.T_array_bf16, types.T_array_float16,
		types.T_array_int8, types.T_array_uint8:
		dimension := declared
		if dimension <= 0 {
			dimension = types.MaxArrayDimension
		}
		return uint64(dimension) * uint64(typ.GetArrayElementSize()), true
	case types.T_json, types.T_blob, types.T_text, types.T_geometry, types.T_datalink:
		bound := int64(types.MaxBlobLen)
		if declared > bound {
			bound = declared
		}
		return uint64(bound), true
	default:
		return 0, false
	}
}

// SerialValueEncoder is the exact component encoder used by serial and
// serial_full. Runtime-filter tuple materialization obtains these encoders once
// per component instead of duplicating the persistent index-key format.
type SerialValueEncoder func(
	v *vector.Vector,
	idx int,
	packer *types.Packer,
)

func NewSerialValueEncoder(
	v *vector.Vector,
) (SerialValueEncoder, error) {
	return getPackFun(v)
}

// SerialTypeSupported reports whether the production tuple encoder has an
// implementation for oid. Keep planner contracts aligned with getPackFun:
// accepting a type here promises that NewSerialValueEncoder will succeed for
// a vector of that type.
func SerialTypeSupported(oid types.T) bool {
	switch oid {
	case types.T_bool,
		types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_date, types.T_time, types.T_datetime, types.T_timestamp,
		types.T_enum, types.T_year,
		types.T_decimal64, types.T_decimal128,
		types.T_uuid,
		types.T_json, types.T_char, types.T_varchar,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text,
		types.T_geometry,
		types.T_array_float32, types.T_array_float64,
		types.T_array_bf16, types.T_array_float16,
		types.T_array_int8, types.T_array_uint8,
		types.T_datalink:
		return true
	default:
		return false
	}
}

// SerialEncodedValueSizeBound returns a no-allocation upper bound for the
// bytes which the production component encoder appends for one non-NULL
// value. Integer encodings use their fixed worst case; byte encodings account
// for the packer's zero-byte escaping exactly.
func SerialEncodedValueSizeBound(
	v *vector.Vector,
	idx int,
) (uint64, error) {
	if v == nil || idx < 0 || idx >= v.Length() {
		return 0, moerr.NewInternalErrorNoCtx(
			"invalid serial runtime-filter component")
	}
	if fixed, ok := serialFixedEncodedTypeSizeBound(v.GetType().Oid); ok {
		return fixed, nil
	}
	switch v.GetType().Oid {
	case types.T_json, types.T_char, types.T_varchar,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text,
		types.T_geometry,
		types.T_array_float32, types.T_array_float64,
		types.T_array_bf16, types.T_array_float16,
		types.T_array_int8, types.T_array_uint8,
		types.T_datalink:
		value := v.GetBytesAt(idx)
		// string-type code + bytes code + terminator; every embedded zero
		// gains one escape byte.
		return uint64(len(value) + bytes.Count(value, []byte{0}) + 3), nil
	default:
		return 0, moerr.NewInternalErrorNoCtxf(
			"not supported serial runtime-filter type %s",
			v.GetType().String(),
		)
	}
}
