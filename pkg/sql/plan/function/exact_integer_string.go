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

import "strings"

// NormalizeExactIntegerString normalizes a complete decimal/scientific string to
// an integer string of at most 20 digits without constructing an arbitrary-
// precision number. The scan is linear in the supplied text and allocates only
// the bounded result, so inputs such as 1e1000000 cannot amplify memory.
func NormalizeExactIntegerString(prefix string) (string, bool) {
	prefix = strings.Trim(prefix, " \t\n\v\f\r")
	if numeric, ok := GetNumericStringPrefix(prefix); !ok || numeric != prefix {
		return "", false
	}
	if prefix == "" {
		return "", false
	}
	i := 0
	negative := false
	if prefix[i] == '+' || prefix[i] == '-' {
		negative = prefix[i] == '-'
		i++
		if i == len(prefix) {
			return "", false
		}
	}
	mantissaEnd := len(prefix)
	for j := i; j < len(prefix); j++ {
		if prefix[j] == 'e' || prefix[j] == 'E' {
			mantissaEnd = j
			break
		}
	}
	digitCount, fractionalDigits := 0, 0
	firstNonZero, lastNonZero := -1, -1
	seenDot := false
	for j := i; j < mantissaEnd; j++ {
		switch c := prefix[j]; {
		case c >= '0' && c <= '9':
			if c != '0' {
				if firstNonZero < 0 {
					firstNonZero = digitCount
				}
				lastNonZero = digitCount
			}
			digitCount++
			if seenDot {
				fractionalDigits++
			}
		case c == '.' && !seenDot:
			seenDot = true
		default:
			return "", false
		}
	}
	if digitCount == 0 {
		return "", false
	}
	if firstNonZero < 0 {
		return "0", true
	}

	exponent := 0
	if mantissaEnd < len(prefix) {
		j := mantissaEnd + 1
		exponentNegative := false
		if j < len(prefix) && (prefix[j] == '+' || prefix[j] == '-') {
			exponentNegative = prefix[j] == '-'
			j++
		}
		if j == len(prefix) {
			return "", false
		}
		capValue := len(prefix) + 64
		for ; j < len(prefix); j++ {
			c := prefix[j]
			if c < '0' || c > '9' {
				return "", false
			}
			if exponent < capValue {
				digit := int(c - '0')
				if exponent > (capValue-digit)/10 {
					exponent = capValue
				} else {
					exponent = exponent*10 + digit
				}
			}
		}
		if exponentNegative {
			exponent = -exponent
		}
	}

	scale := fractionalDigits - exponent
	endDigit := digitCount
	appendZeros := 0
	if scale > 0 {
		if scale > digitCount-1-lastNonZero {
			return "", false
		}
		endDigit -= scale
	} else if scale < 0 {
		appendZeros = -scale
	}
	resultDigits := endDigit - firstNonZero + appendZeros
	if resultDigits <= 0 || resultDigits > 20 {
		return "", false
	}

	var normalized strings.Builder
	normalized.Grow(resultDigits + 1)
	if negative {
		normalized.WriteByte('-')
	}
	digitIndex := 0
	for j := i; j < mantissaEnd && digitIndex < endDigit; j++ {
		c := prefix[j]
		if c == '.' {
			continue
		}
		if digitIndex >= firstNonZero {
			normalized.WriteByte(c)
		}
		digitIndex++
	}
	for range appendZeros {
		normalized.WriteByte('0')
	}
	return normalized.String(), true
}
