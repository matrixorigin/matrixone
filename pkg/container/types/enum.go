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
	"context"
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
	return ParseEnumWithContext(defaultParseContext, enumStr, name)
}

// ParseEnumWithContext parses with the caller's error reporting context.
func ParseEnumWithContext(ctx context.Context, enumStr string, name string) (Enum, error) {
	value, truncated, err := parseEnumAssignmentWithContext(ctx, enumStr, name)
	if err != nil {
		return 0, err
	}
	if truncated {
		return 0, moerr.NewInternalErrorf(parseErrorContext(ctx), "convert to MySQL enum failed: item %s is not in enum %v", name, strings.Split(enumStr, ","))
	}
	return value, nil
}

// ParseEnumAssignment separates recoverable value truncation from invalid
// metadata. The caller owns SQL-mode policy and diagnostics.
func ParseEnumAssignment(enumStr, name string) (Enum, bool, error) {
	return parseEnumAssignmentWithContext(defaultParseContext, enumStr, name)
}

func parseEnumAssignmentWithContext(ctx context.Context, enumStr, name string) (Enum, bool, error) {
	elems := strings.Split(enumStr, ",")
	enumName, err := parseEnumNameWithContext(ctx, elems, name)
	if err != nil {
		return 0, false, err
	}
	if enumName != 0 {
		return enumName, false, nil
	}
	return parseEnumNumericTextWithContext(ctx, name, len(elems))
}

func validateEnumMemberCount(count int) error {
	return validateEnumMemberCountWithContext(defaultParseContext, count)
}

func validateEnumMemberCountWithContext(ctx context.Context, count int) error {
	if count < 1 || count > MaxEnumLen {
		return moerr.NewInvalidInputf(parseErrorContext(ctx), "invalid enum member count %d", count)
	}
	return nil
}

// ParseEnumNumberAssignment validates the wide numeric source before narrowing.
// Unlike a quoted zero, numeric zero is a truncated assignment.
func ParseEnumNumberAssignment(enumStr string, number uint64) (Enum, bool, error) {
	count := strings.Count(enumStr, ",") + 1
	if err := validateEnumMemberCount(count); err != nil {
		return 0, false, err
	}
	if number == 0 || number > uint64(count) {
		return 0, true, nil
	}
	return Enum(number), false, nil
}

// ParseEnumNumericText is shared with indexed label lookup. MySQL only tries
// decimal ordinal fallback for at most five bytes after removing end spaces.
func ParseEnumNumericText(input string, memberCount int) (Enum, bool, error) {
	return parseEnumNumericTextWithContext(defaultParseContext, input, memberCount)
}

func parseEnumNumericTextWithContext(ctx context.Context, input string, memberCount int) (Enum, bool, error) {
	if err := validateEnumMemberCountWithContext(ctx, memberCount); err != nil {
		return 0, false, err
	}
	input = strings.TrimRight(input, " ")
	if len(input) == 0 || len(input) > 5 {
		return 0, true, nil
	}
	number, ok := parseEnumSetDecimal(input)
	if !ok || number > uint64(memberCount) {
		return 0, true, nil
	}
	return Enum(number), false, nil
}

// parseEnumSetDecimal consumes the whole MySQL decimal integer spelling.
// A negative sign applies unsigned arithmetic after checked magnitude parsing.
func parseEnumSetDecimal(input string) (uint64, bool) {
	input = strings.TrimLeft(input, " \t\n\r\v\f")
	negative := false
	if len(input) > 0 && (input[0] == '+' || input[0] == '-') {
		negative = input[0] == '-'
		input = input[1:]
	}
	if len(input) == 0 {
		return 0, false
	}
	var number uint64
	for i := range len(input) {
		if input[i] < '0' || input[i] > '9' {
			return 0, false
		}
		digit := uint64(input[i] - '0')
		if number > (^uint64(0)-digit)/10 {
			return 0, false
		}
		number = number*10 + digit
	}
	if negative {
		number = -number
	}
	return number, true
}

// ParseEnumName return item index with item name.
func ParseEnumName(enumStr string, name string) (Enum, error) {
	elems := strings.Split(enumStr, ",")
	return parseEnumName(elems, name)
}
func parseEnumName(elems []string, name string) (Enum, error) {
	return parseEnumNameWithContext(defaultParseContext, elems, name)
}

func parseEnumNameWithContext(ctx context.Context, elems []string, name string) (Enum, error) {
	if err := validateEnumMemberCountWithContext(ctx, len(elems)); err != nil {
		return 0, err
	}
	// Preserve raw padded labels in existing MO definitions before trying the
	// MySQL trailing-space spelling. Do not normalize stored domain metadata.
	for {
		for i, n := range elems {
			if strings.EqualFold(n, name) {
				return Enum(i + 1), nil
			}
		}
		trimmed := strings.TrimRight(name, " ")
		if trimmed == name {
			return 0, nil
		}
		name = trimmed
	}
}

// ParseEnumValue return item index with special number.
func ParseEnumValue(enumStr string, number uint16) (Enum, error) {
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
	elems := strings.Split(enumStr, ",")
	if index == 0 || index > Enum(len(elems)) {
		return "", moerr.NewInternalErrorNoCtxf("parse MySQL enum failed: index %d overflow enum boundary [1, %d]", index, len(elems))
	}
	return elems[index-1], nil
}

func (e Enum) String() string {
	return strconv.Itoa(int(e))
}
