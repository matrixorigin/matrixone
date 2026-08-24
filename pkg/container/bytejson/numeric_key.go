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

package bytejson

import (
	"encoding/binary"
	"math"
	"math/big"
	"strconv"
)

const (
	canonicalNegativeInteger byte = 0x80
	canonicalPositiveInteger byte = 0x81
	canonicalFloat           byte = 0x82
	canonicalDecimalMarker   byte = 0x83

	canonicalMinInt64Float    = -9223372036854775808.0
	canonicalUint64LimitFloat = 18446744073709551616.0
)

type canonicalNumber struct {
	marker  byte
	payload uint64
	decimal *numericKey
}

// numericKey represents sign * digits * 10^exponent. digits has neither
// leading nor trailing zeroes, so the representation is unique. Keeping the
// exponent as a big.Int makes work proportional to input length even for
// values such as 1e2147483647.
type numericKey struct {
	sign     int8
	digits   string
	exponent big.Int
	adjusted big.Int
}

// CanonicalNumberSize returns the exact key size for a JSON numeric value.
// The key domain is shared by equality-based consumers: every pair for which
// CompareByteJson returns zero has identical canonical bytes. Non-numeric or
// malformed internal values return ok=false.
func CanonicalNumberSize(value ByteJson) (size int, ok bool) {
	number, ok := canonicalNumberFromByteJSON(value)
	if !ok {
		return 0, false
	}
	if number.decimal == nil {
		return 9, true
	}
	exponentSize := (number.decimal.exponent.BitLen() + 7) / 8
	return 1 + 1 + 1 + 4 + exponentSize + len(number.decimal.digits), true
}

// AppendCanonicalNumber appends the equality key for a JSON numeric value.
// INT64, UINT64, and FLOAT64 stay on a fixed-width allocation-free path.
// DECIMAL uses an exact normalized coefficient/exponent key only when it
// cannot reuse an integer or JSON-visible FLOAT64 representation.
func AppendCanonicalNumber(dst []byte, value ByteJson) ([]byte, bool) {
	number, ok := canonicalNumberFromByteJSON(value)
	if !ok {
		return dst, false
	}
	dst = append(dst, number.marker)
	if number.decimal == nil {
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], number.payload)
		return append(dst, encoded[:]...), true
	}

	if number.decimal.sign < 0 {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	dst = append(dst, byte(number.decimal.exponent.Sign()+1))
	exponentSize := (number.decimal.exponent.BitLen() + 7) / 8
	var encodedSize [4]byte
	binary.LittleEndian.PutUint32(encodedSize[:], uint32(exponentSize))
	dst = append(dst, encodedSize[:]...)
	exponentOffset := len(dst)
	dst = append(dst, make([]byte, exponentSize)...)
	number.decimal.exponent.FillBytes(dst[exponentOffset:])
	dst = append(dst, number.decimal.digits...)
	return dst, true
}

func canonicalNumberFromByteJSON(value ByteJson) (canonicalNumber, bool) {
	switch value.Type {
	case TpCodeInt64:
		return canonicalInteger(value.GetInt64()), true
	case TpCodeUint64:
		return canonicalNumber{marker: canonicalPositiveInteger, payload: value.GetUint64()}, true
	case TpCodeFloat64:
		return canonicalFloat64(value.GetFloat64()), true
	case TpCodeDecimal:
		return canonicalDecimal(value.GetString())
	default:
		return canonicalNumber{}, false
	}
}

func canonicalInteger(value int64) canonicalNumber {
	if value < 0 {
		return canonicalNumber{marker: canonicalNegativeInteger, payload: uint64(value)}
	}
	return canonicalNumber{marker: canonicalPositiveInteger, payload: uint64(value)}
}

func canonicalFloat64(value float64) canonicalNumber {
	raw := math.Float64bits(value)
	switch {
	case value == 0:
		return canonicalNumber{marker: canonicalPositiveInteger}
	case value < 0 && value >= canonicalMinInt64Float && math.Trunc(value) == value:
		return canonicalNumber{marker: canonicalNegativeInteger, payload: uint64(int64(value))}
	case value > 0 && value < canonicalUint64LimitFloat && math.Trunc(value) == value:
		return canonicalNumber{marker: canonicalPositiveInteger, payload: uint64(value)}
	default:
		return canonicalNumber{marker: canonicalFloat, payload: raw}
	}
}

func canonicalDecimal(text []byte) (canonicalNumber, bool) {
	numberText := string(text)
	key, ok := numericKeyFromText(numberText)
	if !ok {
		return canonicalNumber{}, false
	}
	if integer, ok := canonicalIntegerFromNumericKey(&key); ok {
		return integer, true
	}

	floating, err := strconv.ParseFloat(numberText, 64)
	if err == nil && !math.IsInf(floating, 0) && !math.IsNaN(floating) {
		visible, visibleOK := numericKeyFromText(strconv.FormatFloat(floating, 'g', -1, 64))
		if visibleOK && compareNumericKeys(&key, &visible) == 0 {
			return canonicalFloat64(floating), true
		}
	}
	return canonicalNumber{marker: canonicalDecimalMarker, decimal: &key}, true
}

func canonicalIntegerFromNumericKey(key *numericKey) (canonicalNumber, bool) {
	if key.sign == 0 {
		return canonicalNumber{marker: canonicalPositiveInteger}, true
	}
	if key.exponent.Sign() < 0 || !key.exponent.IsInt64() {
		return canonicalNumber{}, false
	}
	exponent := key.exponent.Int64()
	if exponent > 19 || int64(len(key.digits))+exponent > 20 {
		return canonicalNumber{}, false
	}
	value := uint64(0)
	for i := range key.digits {
		digit := uint64(key.digits[i] - '0')
		if value > (math.MaxUint64-digit)/10 {
			return canonicalNumber{}, false
		}
		value = value*10 + digit
	}
	for ; exponent > 0; exponent-- {
		if value > math.MaxUint64/10 {
			return canonicalNumber{}, false
		}
		value *= 10
	}
	if key.sign > 0 {
		return canonicalNumber{marker: canonicalPositiveInteger, payload: value}, true
	}
	const minInt64Magnitude = uint64(1) << 63
	if value > minInt64Magnitude {
		return canonicalNumber{}, false
	}
	if value == minInt64Magnitude {
		return canonicalInteger(math.MinInt64), true
	}
	return canonicalInteger(-int64(value)), true
}

func numericKeyFromByteJSON(value ByteJson) (numericKey, bool) {
	switch value.Type {
	case TpCodeInt64:
		return numericKeyFromText(strconv.FormatInt(value.GetInt64(), 10))
	case TpCodeUint64:
		return numericKeyFromText(strconv.FormatUint(value.GetUint64(), 10))
	case TpCodeFloat64:
		floating := value.GetFloat64()
		if math.IsNaN(floating) || math.IsInf(floating, 0) {
			return numericKey{}, false
		}
		return numericKeyFromText(strconv.FormatFloat(floating, 'g', -1, 64))
	case TpCodeDecimal:
		return numericKeyFromText(string(value.GetString()))
	default:
		return numericKey{}, false
	}
}

func numericKeyFromText(text string) (numericKey, bool) {
	var key numericKey
	if text == "" {
		return key, false
	}
	index := 0
	key.sign = 1
	if text[index] == '-' || text[index] == '+' {
		if text[index] == '-' {
			key.sign = -1
		}
		index++
	}
	if index == len(text) {
		return numericKey{}, false
	}
	digits := make([]byte, 0, len(text))
	fractionDigits := 0
	hasDot := false
	for index < len(text) && text[index] != 'e' && text[index] != 'E' {
		ch := text[index]
		switch {
		case ch >= '0' && ch <= '9':
			digits = append(digits, ch)
			if hasDot {
				fractionDigits++
			}
		case ch == '.' && !hasDot:
			hasDot = true
		default:
			return numericKey{}, false
		}
		index++
	}
	if len(digits) == 0 {
		return numericKey{}, false
	}
	exponentText := "0"
	if index < len(text) {
		index++
		if index == len(text) {
			return numericKey{}, false
		}
		exponentText = text[index:]
	}
	if _, ok := key.exponent.SetString(exponentText, 10); !ok {
		return numericKey{}, false
	}
	first := 0
	for first < len(digits) && digits[first] == '0' {
		first++
	}
	if first == len(digits) {
		key.sign = 0
		key.digits = "0"
		return key, true
	}
	last := len(digits) - 1
	for last >= first && digits[last] == '0' {
		last--
	}
	key.digits = string(digits[first : last+1])
	var delta big.Int
	delta.SetInt64(int64(len(digits) - 1 - last - fractionDigits))
	key.exponent.Add(&key.exponent, &delta)
	key.adjusted.Set(&key.exponent)
	delta.SetInt64(int64(len(key.digits) - 1))
	key.adjusted.Add(&key.adjusted, &delta)
	return key, true
}

func compareNumericKeys(left, right *numericKey) int {
	if left.sign != right.sign {
		return compareInt64(int64(left.sign), int64(right.sign))
	}
	if left.sign == 0 {
		return 0
	}
	result := left.adjusted.Cmp(&right.adjusted)
	if result == 0 {
		length := max(len(left.digits), len(right.digits))
		for index := 0; index < length; index++ {
			leftDigit, rightDigit := byte('0'), byte('0')
			if index < len(left.digits) {
				leftDigit = left.digits[index]
			}
			if index < len(right.digits) {
				rightDigit = right.digits[index]
			}
			if leftDigit < rightDigit {
				result = -1
				break
			}
			if leftDigit > rightDigit {
				result = 1
				break
			}
		}
	}
	if left.sign < 0 {
		return -result
	}
	return result
}

func compareNonFiniteNumeric(left, right ByteJson) (int, bool) {
	leftFloat, leftNonFinite := nonFiniteFloat64(left)
	rightFloat, rightNonFinite := nonFiniteFloat64(right)
	if !leftNonFinite && !rightNonFinite {
		return 0, false
	}
	if leftNonFinite && rightNonFinite {
		return compareFloat64(leftFloat, rightFloat), true
	}
	if leftNonFinite {
		if math.IsInf(leftFloat, -1) {
			return -1, true
		}
		return 1, true
	}
	if math.IsInf(rightFloat, -1) {
		return 1, true
	}
	return -1, true
}

func nonFiniteFloat64(value ByteJson) (float64, bool) {
	if value.Type != TpCodeFloat64 {
		return 0, false
	}
	floating := value.GetFloat64()
	return floating, math.IsNaN(floating) || math.IsInf(floating, 0)
}
