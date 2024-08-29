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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func ParseIntToEnum(input int64) (Enum, error) {
	if input < 1 || input > MaxEnumLen {
		return Enum(0), moerr.NewInvalidInputNoCtxf("invalid enum value %d", input)
	}
	return Enum(input), nil
}

// ParseEnum return item index with item name or value.
func ParseEnum(enumStr string, name string) (Enum, error) {
	if len(enumStr) == 0 {
		return 0, moerr.NewInternalErrorNoCtxf("convert to MySQL enum failed: enum define is empty %v", enumStr)
	}
	elems := strings.Split(enumStr, ",")
	enumName, err := parseEnumName(elems, name)
	if err == nil {
		return enumName, nil
	}
	// if name doesn't exist, it's maybe an integer
	num, err := strconv.ParseUint(name, 0, 64)
	if err == nil {
		return parseEnumValue(elems, uint16(num))
	}

	return 0, moerr.NewInternalErrorNoCtxf("convert to MySQL enum failed: item %s is not in enum %v", name, elems)
}

// ParseEnumName return item index with item name.
func ParseEnumName(enumStr string, name string) (Enum, error) {
	if len(enumStr) == 0 {
		return 0, moerr.NewInternalErrorNoCtxf("convert to MySQL enum failed: enum define is empty %v", enumStr)
	}
	elems := strings.Split(enumStr, ",")
	return parseEnumName(elems, name)
}
func parseEnumName(elems []string, name string) (Enum, error) {
	for i, n := range elems {
		if strings.EqualFold(n, name) {
			return Enum(uint16(i) + 1), nil
		}
	}
	return Enum(1), moerr.NewInternalErrorNoCtxf("convert to MySQL enum failed: item %s is not in enum %v", name, elems)
}

// ParseEnumValue return item index with special number.
func ParseEnumValue(enumStr string, number uint16) (Enum, error) {
	if len(enumStr) == 0 {
		return 0, moerr.NewInternalErrorNoCtxf("convert to MySQL enum failed: enum define is empty %v", enumStr)
	}
	elems := strings.Split(enumStr, ",")
	return parseEnumValue(elems, number)
}
func parseEnumValue(elems []string, number uint16) (Enum, error) {
	if number == 0 || number > uint16(len(elems)) {
		return 0, moerr.NewInternalErrorNoCtxf("convert to MySQL enum failed: number %d overflow enum boundary [1, %d]", number, len(elems))
	}

	return Enum(number), nil
}

// ParseEnumIndex return item value with index.
func ParseEnumIndex(enumStr string, index Enum) (string, error) {
	if len(enumStr) == 0 {
		return "", moerr.NewInternalErrorNoCtxf("parse MySQL enum failed: enum type length err %d", len(enumStr))
	}
	elems := strings.Split(enumStr, ",")
	if index == 0 || index > Enum(len(elems)) {
		return "", moerr.NewInternalErrorNoCtxf("parse MySQL enum failed: index %d overflow enum boundary [1, %d]", index, len(elems))
	}
	return elems[index-1], nil
}

func (e Enum) String() string {
	return strconv.Itoa(int(e))
}
