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

package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIsAttributedDecimalOid pins the decimal-family gate for numeric
// attribute masks: only the DECIMAL OIDs may legally carry a nonzero
// UNSIGNED/ZEROFILL mask.
func TestIsAttributedDecimalOid(t *testing.T) {
	for _, oid := range []T{T_decimal64, T_decimal128, T_decimal256} {
		require.True(t, oid.IsAttributedDecimalOid(),
			"%s must accept numeric attributes", oid)
	}
	for _, oid := range []T{T_int64, T_uint64, T_float64, T_varchar, T_bool, T_timestamp, T_uuid} {
		require.False(t, oid.IsAttributedDecimalOid(),
			"%s must not carry numeric attributes", oid)
	}
}
