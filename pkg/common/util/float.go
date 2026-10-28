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

package util

import (
	"math"
	"slices"
	"strconv"
)

// AppendMySQLFloat appends the SQL text representation of an approximate number.
// bitSize must be 32 (FLOAT, six significant digits) or 64 (DOUBLE, shortest
// round-trip digits). Scientific notation follows MySQL my_gcvt: lowercase e,
// no positive exponent sign, and no exponent zero padding. Nonfinite values
// retain Go's spelling; callers admitting only finite numbers must check first.
// This uses MySQL's wide-buffer notation policy. It does not reproduce the
// context-dependent field widths and additional rounding of scalar expressions.
func AppendMySQLFloat(dst []byte, value float64, bitSize int) []byte {
	if bitSize == 32 {
		value = float64(float32(value))
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return strconv.AppendFloat(dst, value, 'g', -1, bitSize)
	}
	precision := -1
	if bitSize == 32 {
		precision = 5 // one leading digit plus five fractional digits
	}
	var scratch [32]byte
	scientific := strconv.AppendFloat(scratch[:0], value, 'e', precision, bitSize)
	negative := scientific[0] == '-'
	if negative {
		scientific = scientific[1:]
	}
	e := 1
	for scientific[e] != 'e' {
		e++
	}
	exponent := 0
	for _, c := range scientific[e+2:] {
		exponent = exponent*10 + int(c-'0')
	}
	if scientific[e+1] == '-' {
		exponent = -exponent
	}
	// Compact the mantissa into significant digits in the same scratch buffer.
	digits := scientific[:0]
	for _, c := range scientific[:e] {
		if c != '.' {
			digits = append(digits, c)
		}
	}
	for len(digits) > 1 && digits[len(digits)-1] == '0' {
		digits = digits[:len(digits)-1]
	}
	decpt := exponent + 1
	// MySQL MAX_DECPT_FOR_F_FORMAT is DBL_DIG (15). Fractional large
	// doubles remain fixed when their significant digits extend past decpt.
	fixed := decpt >= -14 && (decpt <= 15 || len(digits) > decpt)
	required := len(scientific)
	if fixed {
		if decpt <= 0 {
			required = 2 - decpt + len(digits)
		} else {
			required = max(decpt, len(digits))
			if len(digits) > decpt {
				required++ // decimal point
			}
		}
	}
	if negative {
		required++
	}
	dst = slices.Grow(dst, required)
	if negative {
		dst = append(dst, '-')
	}
	if fixed {
		if decpt <= 0 {
			dst = append(dst, '0', '.')
			for ; decpt < 0; decpt++ {
				dst = append(dst, '0')
			}
			return append(dst, digits...)
		}
		for i, c := range digits {
			if i == decpt {
				dst = append(dst, '.')
			}
			dst = append(dst, c)
		}
		for i := len(digits); i < decpt; i++ {
			dst = append(dst, '0')
		}
		return dst
	}
	dst = append(dst, digits[0])
	if len(digits) > 1 {
		dst = append(dst, '.')
		dst = append(dst, digits[1:]...)
	}
	dst = append(dst, 'e')
	return strconv.AppendInt(dst, int64(exponent), 10)
}
