// Copyright 2021 Matrix Origin
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

import (
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// MoYear represents MySQL YEAR type, stored as int16.
// Named MoYear to avoid conflict with interval.go's Year constant.
// Valid values: 0 (displayed as 0000), 1901-2155
type MoYear int16

const (
	MinYearValue MoYear = 1901
	MaxYearValue MoYear = 2155
)

// ParseMoYear parses a string value to MoYear type.
// Supports the following formats:
// - 4-digit string '1901'-'2155': parsed as the corresponding year
// - 2-digit string '0'-'69': converted to years 2000-2069
// - 2-digit string '70'-'99': converted to years 1970-1999
// - String '0' or '00': interpreted as year 2000
func ParseMoYear(s string) (MoYear, error) {
	if s == "" {
		return 0, moerr.NewInvalidInputNoCtx("invalid year format: empty string")
	}

	// Try to parse as integer
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, moerr.NewInvalidInputNoCtxf("invalid year format: %s", s)
	}

	length := len(s)

	// Handle 4-digit year
	if length == 4 {
		// Special case: "0000" should be parsed as 0 (the zero value)
		if s == "0000" {
			return 0, nil
		}
		// If starts with '0' (value 0-999), treat as 2-digit year rule
		// e.g., '0024'->2024, '0070'->1970, '0100'->invalid
		if s[0] == '0' {
			if v >= 1 && v <= 69 {
				return MoYear(2000 + v), nil
			}
			if v >= 70 && v <= 99 {
				return MoYear(1900 + v), nil
			}
			// 0100-0999 is invalid
			return 0, moerr.NewInvalidInputNoCtxf("year value out of range: %s", s)
		}
		return ParseMoYearFromInt(v)
	}

	// Handle 1-2 digit year (string format)
	// For string input: '0' or '00' -> 2000
	if length <= 2 {
		if v >= 0 && v <= 69 {
			return MoYear(2000 + v), nil
		}
		if v >= 70 && v <= 99 {
			return MoYear(1900 + v), nil
		}
	}

	return 0, moerr.NewInvalidInputNoCtxf("invalid year value: %s", s)
}

// ParseMoYearFromInt parses an integer value to MoYear type.
// Supports the following formats:
// - 4-digit number 1901-2155: parsed as the corresponding year
// - 2-digit number 1-69: converted to years 2001-2069
// - 2-digit number 70-99: converted to years 1970-1999
// - Number 0: stored as 0000
func ParseMoYearFromInt(v int64) (MoYear, error) {
	// Handle zero specially
	if v == 0 {
		return 0, nil
	}

	// Handle 2-digit year (numeric format)
	// For numeric input: 1-69 -> 2001-2069, 70-99 -> 1970-1999
	if v >= 1 && v <= 69 {
		return MoYear(2000 + v), nil
	}
	if v >= 70 && v <= 99 {
		return MoYear(1900 + v), nil
	}

	// Handle 4-digit year
	if v >= int64(MinYearValue) && v <= int64(MaxYearValue) {
		return MoYear(v), nil
	}

	return 0, moerr.NewInvalidInputNoCtxf("year value out of range: %d", v)
}

// ValidateMoYear checks if a MoYear value is valid.
// Valid values are: 0, or 1901-2155
func ValidateMoYear(y MoYear) error {
	if y == 0 {
		return nil
	}
	if y >= MinYearValue && y <= MaxYearValue {
		return nil
	}
	return moerr.NewInvalidInputNoCtxf("year value out of range: %d", y)
}

// String returns the string representation of MoYear in YYYY format.
// The special value 0 is displayed as "0000".
func (y MoYear) String() string {
	return fmt.Sprintf("%04d", y)
}

// ToInt64 converts MoYear to int64.
func (y MoYear) ToInt64() int64 {
	return int64(y)
}
