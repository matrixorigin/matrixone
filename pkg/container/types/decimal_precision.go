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

package types

// DecimalMaxPrecision returns the supported precision of a decimal storage type,
// or zero for a non-decimal type.
func DecimalMaxPrecision(oid T) int32 {
	switch oid {
	case T_decimal64:
		return 18
	case T_decimal128:
		return 38
	case T_decimal256:
		return 76
	default:
		return 0
	}
}
