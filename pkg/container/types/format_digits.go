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

package types

// Digit writers for the temporal String functions. They produce what
// fmt.Sprintf("%02d") and "%04d" produce for values in range, without fmt's
// per-call parsing and allocation. Callers check the range and fall back to
// fmt for anything else.

// append2Digits appends v (0..99) as two digits.
func append2Digits(dst []byte, v int) []byte {
	return append(dst, hundredToChars[v][0], hundredToChars[v][1])
}

// append4Digits appends v (0..9999) as four digits.
func append4Digits(dst []byte, v int) []byte {
	dst = append2Digits(dst, v/100)
	return append2Digits(dst, v%100)
}

// appendClock appends "hh:mm:ss" for hour, minute and second in 0..99.
func appendClock(dst []byte, hour, minute, sec int) []byte {
	dst = append2Digits(dst, hour)
	dst = append(dst, ':')
	dst = append2Digits(dst, minute)
	dst = append(dst, ':')
	return append2Digits(dst, sec)
}

// appendDateClock appends "yyyy-mm-dd hh:mm:ss" for year in 0..9999 and the
// other fields in 0..99.
func appendDateClock(dst []byte, year, month, day, hour, minute, sec int) []byte {
	dst = append4Digits(dst, year)
	dst = append(dst, '-')
	dst = append2Digits(dst, month)
	dst = append(dst, '-')
	dst = append2Digits(dst, day)
	dst = append(dst, ' ')
	return appendClock(dst, hour, minute, sec)
}

// appendFraction appends '.' and the first digits digits (1..9) of the
// microseconds usec (0..999999) written as six digits and padded with zeros
// to nine: what the formatters' "%06d", pad-to-9, truncate produced.
func appendFraction(dst []byte, usec int64, digits int32) []byte {
	var frac [9]byte
	v := usec
	for i := 5; i >= 0; i-- {
		frac[i] = byte(v%10) + '0'
		v /= 10
	}
	frac[6], frac[7], frac[8] = '0', '0', '0'
	dst = append(dst, '.')
	return append(dst, frac[:digits]...)
}
