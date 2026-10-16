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

package types

import (
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// NormalizeWindowInterval preserves the distinction between an invalid frame
// argument (error) and NULL interval arithmetic (null). numeric describes the
// SQL source domain, not the text used to transport a prepared parameter.
// This is deliberately separate from the general DATE_ADD interval grammar.
func NormalizeWindowInterval(text string, unit IntervalType, numeric bool) (value int64, normalized IntervalType, null bool, err error) {
	text = strings.TrimSpace(text)
	if numeric && (text == "NaN" || text == "Inf" || text == "+Inf" || text == "-Inf") {
		return 0, unit, false, moerr.NewInvalidInputNoCtx("non-finite window interval")
	}
	if unit <= IntervalTypeInvalid || unit >= IntervalTypeMax {
		return 0, unit, false, moerr.NewInvalidInputNoCtx("invalid window interval unit")
	}
	if unit > Year {
		// MySQL compound intervals consume integer digit runs. In particular,
		// a dot is a separator, not a fractional SECOND extension. Short forms
		// are right aligned; an extra field makes the arithmetic NULL.
		if strings.HasPrefix(text, "-") {
			return 0, unit, false, windowIntervalNegativeError()
		}
		_, count, splitErr := splitIntervalNumberFields(text, typeMaxLength(unit)+1)
		if splitErr != nil || count > typeMaxLength(unit) {
			return 0, unit, true, nil
		}
		if count == 0 {
			text = "0"
		}
		value, normalized, err = NormalizeInterval(text, unit)
		return value, normalized, false, err
	}
	places := 0
	if unit == Second {
		places = 6
		normalized = MicroSecond
	} else {
		normalized = unit
	}
	value, err = windowIntervalScalar(text, places, numeric)
	return value, normalized, false, err
}

func windowIntervalNegativeError() error {
	return moerr.NewParseErrorNoCtx("Window '<unnamed window>': frame start or end is negative, NULL or of non-integral type")
}

// windowIntervalScalar converts a decimal prefix by moving its decimal point.
// It never converts exact integers/decimals through float64, nor constructs a
// huge power of ten for an untrusted exponent. SECOND truncates to microseconds;
// other numeric source units use MySQL's nearest-integer conversion.
func windowIntervalScalar(text string, places int, numeric bool) (int64, error) {
	i := 0
	negative := false
	if i < len(text) && (text[i] == '+' || text[i] == '-') {
		negative = text[i] == '-'
		i++
	}
	start := i
	for i < len(text) && text[i] >= '0' && text[i] <= '9' {
		i++
	}
	digits := text[start:i]
	fraction := 0
	if (places != 0 || numeric) && i < len(text) && text[i] == '.' {
		i++
		start = i
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
		}
		fraction = i - start
		digits += text[start:i]
	}
	if len(digits) == 0 {
		if numeric {
			return 0, moerr.NewInvalidInputNoCtx("invalid numeric window interval")
		}
		return 0, nil // no numeric prefix, e.g. 'abc' DAY
	}
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		return 0, nil
	}
	if negative {
		return 0, windowIntervalNegativeError()
	}
	exponent := 0
	if (places != 0 || numeric) && i < len(text) && (text[i] == 'e' || text[i] == 'E') {
		i++
		exponentNegative := false
		if i < len(text) && (text[i] == '+' || text[i] == '-') {
			exponentNegative = text[i] == '-'
			i++
		}
		// A larger exponent cannot affect an in-range nonzero result. Capping
		// its accumulation also bounds work/storage for arbitrarily long text.
		limit := len(text) + 32
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			if exponent < limit {
				exponent = min(limit, exponent*10+int(text[i]-'0'))
			}
			i++
		}
		if exponentNegative {
			exponent = -exponent
		}
	}
	shift := places + exponent - fraction
	integerDigits := len(digits) + shift
	if integerDigits > 19 {
		return 0, moerr.NewOutOfRangeNoCtx("int64", "window interval")
	}
	roundUp := numeric && places == 0 && integerDigits >= 0 && integerDigits < len(digits) && digits[integerDigits] >= '5'
	if integerDigits <= 0 {
		if roundUp {
			return 1, nil
		}
		return 0, nil
	}
	if shift >= 0 {
		digits += strings.Repeat("0", shift)
	} else {
		digits = digits[:integerDigits]
	}
	number, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || number > math.MaxInt64 || (roundUp && number == math.MaxInt64) {
		return 0, moerr.NewOutOfRangeNoCtx("int64", "window interval")
	}
	if roundUp {
		number++
	}
	return int64(number), nil
}
