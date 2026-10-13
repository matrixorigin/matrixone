// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jsonvalue

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestJSONTableScalarCompatibilityAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, json string
		target     types.Type
		status     ConversionStatus
	}{
		{"integer", `7`, types.T_int32.ToType(), StatusSuccess},
		{"exact_decimal_integer", `"7.000"`, types.T_int32.ToType(), StatusSuccess},
		{"exact_exponent_integer", `"7E1"`, types.T_int32.ToType(), StatusSuccess},
		{"fractional_integer", `1.25`, types.T_int32.ToType(), StatusStatementError},
		{"fractional_integer_text", `"1.25"`, types.T_uint32.ToType(), StatusStatementError},
		{"fractional_uppercase_exponent", `"1E-2"`, types.T_int32.ToType(), StatusStatementError},
		{"bounded_huge_exponent", `"1e-2147483647"`, types.T_int32.ToType(), StatusStatementError},
		{"lossy_text", `"ab"`, types.New(types.T_varchar, 1, 0), StatusStatementError},
		{"exact_text", `"a"`, types.New(types.T_varchar, 1, 0), StatusSuccess},
		{"supported_decimal_truncation", `1.25`, types.New(types.T_decimal64, 3, 1), StatusTruncated},
		{"range_uses_on_error", `300`, types.T_int8.ToType(), StatusRangeError},
		{"invalid_uses_on_error", `"invalid"`, types.T_int32.ToType(), StatusConversionError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value, err := types.ParseStringToByteJson(tc.json)
			require.NoError(t, err)
			result := ConvertTableScalarWithContext(context.Background(), value, tc.target, ConversionOptions{})
			require.Equal(t, tc.status, result.Status)
			if tc.status == StatusStatementError {
				require.ErrorContains(t, result.Err, "requires the scalar compatibility gate")
				require.Nil(t, result.Warning)
			}
		})
	}
}
