// Copyright 2024 Matrix Origin
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

package bytejson

import (
	"bytes"
	"math/bits"
	"strconv"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// numberParts is the result of parsing out a valid JSON number. It contains
// the parts of a number. The parts are used for integer conversion.
type numberParts struct {
	neg  bool
	intp []byte
	frac []byte
	exp  []byte
}

// parseNumber constructs numberParts from given []byte. The logic here is
// similar to consumeNumber above with the difference of having to construct
// numberParts. The slice fields in numberParts are subslices of the input.
func parseNumberParts(input []byte) (numberParts, bool) {
	var neg bool
	var intp []byte
	var frac []byte
	var exp []byte

	s := input
	if len(s) == 0 {
		return numberParts{}, false
	}

	// Optional -
	if s[0] == '-' {
		neg = true
		s = s[1:]
		if len(s) == 0 {
			return numberParts{}, false
		}
	}

	// Digits
	switch {
	case s[0] == '0':
		// Skip first 0 and no need to store.
		s = s[1:]

	case '1' <= s[0] && s[0] <= '9':
		intp = s
		n := 1
		s = s[1:]
		for len(s) > 0 && '0' <= s[0] && s[0] <= '9' {
			s = s[1:]
			n++
		}
		intp = intp[:n]

	default:
		return numberParts{}, false
	}

	// . followed by 1 or more digits.
	if len(s) >= 2 && s[0] == '.' && '0' <= s[1] && s[1] <= '9' {
		frac = s[1:]
		n := 1
		s = s[2:]
		for len(s) > 0 && '0' <= s[0] && s[0] <= '9' {
			s = s[1:]
			n++
		}
		frac = frac[:n]
	}

	// e or E followed by an optional - or + and
	// 1 or more digits.
	if len(s) >= 2 && (s[0] == 'e' || s[0] == 'E') {
		s = s[1:]
		exp = s
		n := 0
		if s[0] == '+' || s[0] == '-' {
			s = s[1:]
			n++
			if len(s) == 0 {
				return numberParts{}, false
			}
		}
		for len(s) > 0 && '0' <= s[0] && s[0] <= '9' {
			s = s[1:]
			n++
		}
		exp = exp[:n]
	}

	return numberParts{
		neg:  neg,
		intp: intp,
		frac: bytes.TrimRight(frac, "0"), // Remove unnecessary 0s to the right.
		exp:  exp,
	}, true
}

// normalizeToIntString returns an integer string in normal form without the
// E-notation for given numberParts. It will return false if it is not an
// integer or if the exponent exceeds than max/min int value.
func normalizeToIntString(n numberParts) (string, bool) {
	intpSize := len(n.intp)
	fracSize := len(n.frac)

	if intpSize == 0 && fracSize == 0 {
		return "0", true
	}

	var exp int
	if len(n.exp) > 0 {
		i, err := strconv.ParseInt(string(n.exp), 10, 32)
		if err != nil {
			return "", false
		}
		exp = int(i)
	}

	var num []byte
	if exp >= 0 {
		// For positive E, shift fraction digits into integer part and also pad
		// with zeroes as needed.

		// If there are more digits in fraction than the E value, then the
		// number is not an integer.
		if fracSize > exp {
			return "", false
		}

		// Make sure resulting digits are within max value limit to avoid
		// unnecessarily constructing a large byte slice that may simply fail
		// later on.
		const maxDigits = 20 // Max uint64 value has 20 decimal digits.
		if intpSize+exp > maxDigits {
			return "", false
		}

		// Set cap to make a copy of integer part when appended.
		num = n.intp[:len(n.intp):len(n.intp)]
		num = append(num, n.frac...)
		for i := 0; i < exp-fracSize; i++ {
			num = append(num, '0')
		}
	} else {
		// For negative E, shift digits in integer part out.

		// If there are fractions, then the number is not an integer.
		if fracSize > 0 {
			return "", false
		}

		// index is where the decimal point will be after adjusting for negative
		// exponent.
		index := intpSize + exp
		if index < 0 {
			return "", false
		}

		num = n.intp
		// If any of the digits being shifted to the right of the decimal point
		// is non-zero, then the number is not an integer.
		for i := index; i < intpSize; i++ {
			if num[i] != '0' {
				return "", false
			}
		}
		num = num[:index]
	}

	if n.neg {
		return "-" + string(num), true
	}
	return string(num), true
}

// indexNeedEscapeInString returns the index of the character that needs
// escaping. If no characters need escaping, this returns the input length.
func indexNeedEscapeInString(s string) int {
	for i, r := range s {
		if r < ' ' || r == '\\' || r == '"' || r == utf8.RuneError {
			return i
		}
	}
	return len(s)
}

func appendString(out []byte, in string) ([]byte, error) {
	out = append(out, '"')
	i := indexNeedEscapeInString(in)
	in, out = in[i:], append(out, in[:i]...)
	for len(in) > 0 {
		switch r, n := utf8.DecodeRuneInString(in); {
		case r == utf8.RuneError && n == 1:
			return out, moerr.NewInvalidInputNoCtx("invalid UTF-8")
		case r < ' ' || r == '"' || r == '\\':
			out = append(out, '\\')
			switch r {
			case '"', '\\':
				out = append(out, byte(r))
			case '\b':
				out = append(out, 'b')
			case '\f':
				out = append(out, 'f')
			case '\n':
				out = append(out, 'n')
			case '\r':
				out = append(out, 'r')
			case '\t':
				out = append(out, 't')
			default:
				out = append(out, 'u')
				out = append(out, "0000"[1+(bits.Len32(uint32(r))-1)/4:]...)
				out = strconv.AppendUint(out, uint64(r), 16)
			}
			in = in[n:]
		default:
			i := indexNeedEscapeInString(in[n:])
			in, out = in[n+i:], append(out, in[:n+i]...)
		}
	}
	out = append(out, '"')
	return out, nil
}
