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

package enum

import (
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ParseEnum return item index with item name or value.
func ParseEnum(enumStr string, name string) (uint16, error) {
	if len(enumStr) == 0 {
		return 0, moerr.NewInternalErrorNoCtx("convert to MySQL enum failed: enum define is empty %v", enumStr)
	}
	elems := strings.Split(enumStr, ",")
	enumName, err := ParseEnumName(elems, name)
	if err == nil {
		return enumName, nil
	}
	// if name doesn't exist, it's maybe an integer
	num, err := strconv.ParseUint(name, 0, 64)
	if err == nil {
		return ParseEnumValue(elems, uint16(num))
	}

	return 0, moerr.NewInternalErrorNoCtx("convert to MySQL enum failed: item %s is not in enum %v", name, elems)
}

// ParseEnumName return item index with item name.
func ParseEnumName(elems []string, name string) (uint16, error) {
	for i, n := range elems {
		if strings.EqualFold(n, name) {
			return uint16(i) + 1, nil
		}
	}
	return 0, moerr.NewInternalErrorNoCtx("convert to MySQL enum failed: item %s is not in enum %v", name, elems)
}

// ParseEnumValue return item index with special number.
func ParseEnumValue(elems []string, number uint16) (uint16, error) {
	if number == 0 || number > uint16(len(elems)) {
		return 0, moerr.NewInternalErrorNoCtx("convert to MySQL enum failed: number %d overflow enum boundary [1, %d]", number, len(elems))
	}

	return number, nil
}

// ParseEnumIndex return item value with index.
func ParseEnumIndex(enumStr string, index uint16) (string, error) {
	if len(enumStr) == 0 {
		return "", moerr.NewInternalErrorNoCtx("parse MySQL enum failed: enum type length err %d", len(enumStr))
	}
	elems := strings.Split(enumStr, ",")
	if index == 0 || index > uint16(len(elems)) {
		return "", moerr.NewInternalErrorNoCtx("parse MySQL enum failed: index %d overflow enum boundary [1, %d]", index, len(elems))
	}
	return elems[index-1], nil
}
