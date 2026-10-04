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

package partitionhash

import "github.com/matrixorigin/matrixone/pkg/container/types"

// Compatible reports whether group hashing and the legacy window partition
// comparator use the same equality relation for typ.
func Compatible(typ types.T) bool {
	// This is an allowlist because a newly introduced type must prove both hash
	// support and equality compatibility before the optimizer may select it.
	switch typ {
	case types.T_bool, types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_date, types.T_datetime, types.T_time, types.T_timestamp,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_TS, types.T_Rowid, types.T_Blockid, types.T_uuid,
		types.T_enum, types.T_year,
		types.T_varchar, types.T_blob, types.T_binary,
		types.T_varbinary, types.T_text, types.T_datalink, types.T_geometry:
		return true
	default:
		return false
	}
}
