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

package colexec

import "github.com/matrixorigin/matrixone/pkg/container/types"

// SupportsTotalLockTableRange reports whether lockop can encode a range that
// covers the type's complete physical keyspace. Keep this capability below the
// lockop package so both physical compilation and logical-plan admission can
// use the same predicate without creating a package cycle.
func SupportsTotalLockTableRange(t types.Type) bool {
	switch t.Oid {
	case types.T_bool, types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_date, types.T_year, types.T_time, types.T_datetime, types.T_timestamp,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_uuid, types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_enum:
		return true
	default:
		// Other types have no lock-row fetcher.
		return false
	}
}
