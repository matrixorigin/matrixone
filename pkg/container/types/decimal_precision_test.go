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

import "testing"

func TestDecimalMaxPrecision(t *testing.T) {
	for _, tc := range []struct {
		oid  T
		want int32
	}{{T_decimal64, 18}, {T_decimal128, 38}, {T_decimal256, 76}, {T_int64, 0}, {T_any, 0}} {
		if got := DecimalMaxPrecision(tc.oid); got != tc.want {
			t.Errorf("type %v: got %d, want %d", tc.oid, got, tc.want)
		}
	}
}
