// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colexec

import "github.com/matrixorigin/matrixone/pkg/container/types"

// SortRollupTypeEligible restricts sorted grouping to types for which SQL
// order equality and hash-grouping equality have the same domain. Planner and
// executor share this fail-closed capability check.
func SortRollupTypeEligible(oid types.T) bool {
	switch oid {
	case types.T_bool, types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_date, types.T_time, types.T_datetime, types.T_timestamp, types.T_year,
		types.T_uuid, types.T_enum:
		return true
	default:
		return false
	}
}
