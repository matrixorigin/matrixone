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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

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
	switch v.GetType().Oid {
	case types.T_bool:
		return 1, nil
	case types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_date, types.T_time, types.T_datetime, types.T_timestamp,
		types.T_year:
		// One type byte, one integer-length byte, and at most eight data
		// bytes. Narrow integers simply use less than this bound.
		return 10, nil
	case types.T_float32:
		return 5, nil
	case types.T_float64:
		return 9, nil
	case types.T_enum:
		// Enum code + encoded uint16 type/value.
		return 11, nil
	case types.T_decimal64:
		return 9, nil
	case types.T_decimal128:
		return 17, nil
	case types.T_uuid:
		return 17, nil
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
