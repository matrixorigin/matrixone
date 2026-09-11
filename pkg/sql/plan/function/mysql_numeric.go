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

package function

import "math"

// parseMySQLIntegerPrefix implements the integer part of MySQL's conversion
// from a character value to a signed integer. It deliberately consumes bytes,
// rather than runes: binary string arguments must remain byte-oriented, and
// only ASCII whitespace is ignored before the optional sign and digit prefix.
//
// The caller needs the converted value, not a conversion error. MySQL treats a
// non-numeric prefix as zero and clamps an overflowing signed conversion. The
// clamp also lets this parser stop at the first overflowing digit, preventing
// an arbitrarily long VARCHAR/BLOB value from causing proportional work.
func parseMySQLIntegerPrefix(value []byte) int64 {
	pos := 0
	for pos < len(value) && isMySQLNumericWhitespace(value[pos]) {
		pos++
	}

	negative := false
	if pos < len(value) {
		switch value[pos] {
		case '+':
			pos++
		case '-':
			negative = true
			pos++
		}
	}

	if pos == len(value) || value[pos] < '0' || value[pos] > '9' {
		return 0
	}

	limit := uint64(math.MaxInt64)
	if negative {
		limit++ // abs(math.MinInt64)
	}
	var number uint64
	for pos < len(value) {
		ch := value[pos]
		if ch < '0' || ch > '9' {
			break
		}
		digit := uint64(ch - '0')
		if number > (limit-digit)/10 {
			if negative {
				return math.MinInt64
			}
			return math.MaxInt64
		}
		number = number*10 + digit
		pos++
	}

	if negative {
		if number == uint64(math.MaxInt64)+1 {
			return math.MinInt64
		}
		return -int64(number)
	}
	return int64(number)
}

func isMySQLNumericWhitespace(ch byte) bool {
	switch ch {
	case ' ', '\t', '\n', '\v', '\f', '\r':
		return true
	default:
		return false
	}
}
