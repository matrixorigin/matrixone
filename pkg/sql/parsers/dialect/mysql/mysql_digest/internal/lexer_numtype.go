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
//
// Portions derived from github.com/rashiq/mysql-digest; see ../LICENSE.

package internal

// Matches MySQL's int_token() in sql_lex.cc.

const (
	maxLong             = "2147483647" // 2^31 - 1
	maxLongLen          = 10
	minSignedLong       = "2147483648"          // 2^31 (absolute value of -2^31)
	maxLongLong         = "9223372036854775807" // 2^63 - 1
	maxLongLongLen      = 19
	minSignedLongLong   = "9223372036854775808"  // 2^63 (absolute value of -2^63)
	maxUnsignedLongLong = "18446744073709551615" // 2^64 - 1
	maxUnsignedLongLen  = 20
)

func ClassifyInteger(s string) int {
	if len(s) == 0 {
		return NUM
	}

	// Quick path for short numbers
	if len(s) < maxLongLen {
		return NUM
	}

	// Parse sign and normalize
	neg := false
	offset := 0

	if s[0] == '+' {
		offset++
	} else if s[0] == '-' {
		offset++
		neg = true
	}

	// Skip leading zeros
	str := s[offset:]
	for len(str) > 0 && str[0] == '0' {
		str = str[1:]
	}

	length := len(str)

	// After normalization, check again
	if length < maxLongLen {
		return NUM
	}

	return classifyByMagnitude(str, length, neg)
}

func classifyByMagnitude(str string, length int, neg bool) int {
	var cmp string
	var smaller, bigger int

	if neg {
		// Negative numbers
		if length == maxLongLen {
			cmp = minSignedLong
			smaller = NUM
			bigger = LONG_NUM
		} else if length < maxLongLongLen {
			return LONG_NUM
		} else if length > maxLongLongLen {
			return DECIMAL_NUM
		} else {
			// length == maxLongLongLen
			cmp = minSignedLongLong
			smaller = LONG_NUM
			bigger = DECIMAL_NUM
		}
	} else {
		// Positive numbers
		if length == maxLongLen {
			cmp = maxLong
			smaller = NUM
			bigger = LONG_NUM
		} else if length < maxLongLongLen {
			return LONG_NUM
		} else if length > maxLongLongLen {
			if length > maxUnsignedLongLen {
				return DECIMAL_NUM
			}
			cmp = maxUnsignedLongLong
			smaller = ULONGLONG_NUM
			bigger = DECIMAL_NUM
		} else {
			// length == maxLongLongLen
			cmp = maxLongLong
			smaller = LONG_NUM
			bigger = ULONGLONG_NUM
		}
	}

	// Compare digit by digit
	return compareDigits(str, cmp, smaller, bigger)
}

func compareDigits(str, cmp string, smaller, bigger int) int {
	for i := 0; i < len(str) && i < len(cmp); i++ {
		if str[i] < cmp[i] {
			return smaller
		}
		if str[i] > cmp[i] {
			return bigger
		}
	}
	return smaller // Equal means it fits in the smaller type
}
