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

package function

import "github.com/matrixorigin/matrixone/pkg/container/types"

// ParseDecimal64CastString applies the normal string-to-DECIMAL64 CAST
// conversion, including the CAST numeric-token grammar.
func ParseDecimal64CastString(s string, width, scale int32) (types.Decimal64, error) {
	return parseDecimal64CastString(s, width, scale)
}

// ParseExplicitDecimal64CastString applies the explicit DECIMAL64 CAST
// conversion, including its range-clamping behavior.
func ParseExplicitDecimal64CastString(s string, width, scale int32) (types.Decimal64, error) {
	result, err := parseExplicitDecimal64CastString(s, width, scale)
	if err == nil {
		return result, nil
	}
	if clamped, clampErr := clampDecimal64CastString(s, width, scale); clampErr == nil {
		return clamped, nil
	}
	return result, err
}

// ParseDecimal128CastString applies the normal string-to-DECIMAL128 CAST
// conversion, including the CAST numeric-token grammar.
func ParseDecimal128CastString(s string, width, scale int32) (types.Decimal128, error) {
	return parseDecimal128CastString(s, width, scale)
}

// ParseExplicitDecimal128CastString applies the explicit DECIMAL128 CAST
// conversion, including its range-clamping behavior.
func ParseExplicitDecimal128CastString(s string, width, scale int32) (types.Decimal128, error) {
	result, err := parseExplicitDecimal128CastString(s, width, scale)
	if err == nil {
		return result, nil
	}
	if clamped, clampErr := clampDecimal128CastString(s, width, scale); clampErr == nil {
		return clamped, nil
	}
	return result, err
}

// ParseDecimal256CastString applies the normal string-to-DECIMAL256 CAST
// conversion, including the CAST numeric-token grammar.
func ParseDecimal256CastString(s string, width, scale int32) (types.Decimal256, error) {
	return parseDecimal256CastString(s, width, scale)
}

// ParseExplicitDecimal256CastString applies the explicit DECIMAL256 CAST
// conversion, including its range-clamping behavior.
func ParseExplicitDecimal256CastString(s string, width, scale int32) (types.Decimal256, error) {
	result, err := parseExplicitDecimal256CastString(s, width, scale)
	if err == nil {
		return result, nil
	}
	if clamped, clampErr := clampDecimal256CastString(s, width, scale); clampErr == nil {
		return clamped, nil
	}
	return result, err
}
