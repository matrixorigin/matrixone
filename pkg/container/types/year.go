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

import (
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type MoYear int16

const (
	MinYearValue MoYear = 1901
	MaxYearValue MoYear = 2155
)

func ParseMoYear(s string) (MoYear, error) {
	if s == "" {
		return 0, moerr.NewInvalidInputNoCtx("invalid year format: empty string")
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, moerr.NewInvalidInputNoCtxf("invalid year format: %s", s)
	}
	if len(s) == 4 {
		if s == "0000" {
			return 0, nil
		}
		if s[0] == '0' {
			return 0, moerr.NewInvalidInputNoCtxf("invalid year value: %s", s)
		}
		return ParseMoYearFromInt(v)
	}
	if len(s) <= 2 {
		if v >= 0 && v <= 69 {
			return MoYear(2000 + v), nil
		}
		if v >= 70 && v <= 99 {
			return MoYear(1900 + v), nil
		}
	}
	return 0, moerr.NewInvalidInputNoCtxf("invalid year value: %s", s)
}

func ParseMoYearFromInt(v int64) (MoYear, error) {
	if v == 0 {
		return 0, nil
	}
	if v >= 1 && v <= 69 {
		return MoYear(2000 + v), nil
	}
	if v >= 70 && v <= 99 {
		return MoYear(1900 + v), nil
	}
	if v >= int64(MinYearValue) && v <= int64(MaxYearValue) {
		return MoYear(v), nil
	}
	return 0, moerr.NewInvalidInputNoCtxf("year value out of range: %d", v)
}

func ValidateMoYear(y MoYear) error {
	if y == 0 || (y >= MinYearValue && y <= MaxYearValue) {
		return nil
	}
	return moerr.NewInvalidInputNoCtxf("year value out of range: %d", y)
}

func (y MoYear) String() string {
	return fmt.Sprintf("%04d", y)
}

func (y MoYear) ToInt64() int64 {
	return int64(y)
}
