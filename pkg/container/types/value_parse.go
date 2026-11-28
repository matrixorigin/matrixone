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

import (
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ParseValue parses s, the text of a value of type t, into the value
// (as vector.GetAny returns it): the one text-to-value parse for a
// constant. An integer may be written as a float that is exactly an
// integer ("2024.0"); a timestamp is read in loc, or in time.Local when loc
// is nil. ENUM and SET values need their member list, which t does not
// carry, and vectors are not parsed: those, and types with no text form,
// return an error.
func ParseValue(t Type, s string, loc *time.Location) (any, error) {
	switch t.Oid {
	case T_bool:
		return ParseBool(s)
	case T_bit:
		width := int(t.Width)
		if width <= 0 || width > 64 {
			width = 64
		}
		return strconv.ParseUint(s, 10, width)
	case T_int8:
		v, err := parseIntWithFloatFallback(s, 8)
		return int8(v), err
	case T_int16:
		v, err := parseIntWithFloatFallback(s, 16)
		return int16(v), err
	case T_int32:
		v, err := parseIntWithFloatFallback(s, 32)
		return int32(v), err
	case T_int64:
		return parseIntWithFloatFallback(s, 64)
	case T_uint8:
		v, err := parseUintWithFloatFallback(s, 8)
		return uint8(v), err
	case T_uint16:
		v, err := parseUintWithFloatFallback(s, 16)
		return uint16(v), err
	case T_uint32:
		v, err := parseUintWithFloatFallback(s, 32)
		return uint32(v), err
	case T_uint64:
		return parseUintWithFloatFallback(s, 64)
	case T_float32:
		v, err := strconv.ParseFloat(s, 32)
		return float32(v), err
	case T_float64:
		return strconv.ParseFloat(s, 64)
	case T_bf16, T_float16, T_float8, T_float4:
		v, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return nil, err
		}
		switch t.Oid {
		case T_bf16:
			return BF16FromFloat32(float32(v)), nil
		case T_float16:
			return Float16FromFloat32(float32(v)), nil
		case T_float8:
			return Float8FromFloat32(float32(v)), nil
		case T_float4:
			return Float4FromFloat32(float32(v)), nil
		default:
			panic("unreachable")
		}
	case T_decimal64:
		return ParseDecimal64(s, t.Width, t.Scale)
	case T_decimal128:
		return ParseDecimal128(s, t.Width, t.Scale)
	case T_decimal256:
		return ParseDecimal256(s, t.Width, t.Scale)
	case T_date:
		return ParseDateCast(s)
	case T_datetime:
		return ParseDatetime(s, t.Scale)
	case T_timestamp:
		if loc == nil {
			loc = time.Local
		}
		return ParseTimestamp(loc, s, t.Scale)
	case T_time:
		return ParseTime(s, t.Scale)
	case T_year:
		return ParseMoYear(s)
	case T_uuid:
		return ParseUuid(s)
	case T_json:
		j, err := ParseStringToByteJson(s)
		if err != nil {
			return nil, err
		}
		return j.Marshal()
	case T_char, T_varchar, T_text, T_binary, T_varbinary, T_blob, T_datalink:
		return []byte(s), nil
	default:
		return nil, moerr.NewNotSupportedNoCtxf("parsing a %s value from text", t.String())
	}
}

// Float-domain boundary constants for 64-bit overflow detection.
//
// math.MaxInt64 = 2^63 - 1 and math.MaxUint64 = 2^64 - 1 are odd numbers that
// cannot be exactly represented in float64 (only 53 bits of mantissa). Comparing
// float64(f) > math.MaxInt64 silently compares against 2^63 (the nearest float64),
// which lets f == 2^63 slip through and then int64(f) wraps to -2^63.
//
// Using the exact float64 values 2^63 and 2^64 as strict upper bounds closes
// this gap: any f ≥ 2^63 is out of int64 range; any f ≥ 2^64 is out of uint64
// range. Both 2^63 and 2^64 are exactly representable in float64 (powers of 2).
//
// Additionally, float64 can represent consecutive integers exactly only up to
// 2^53. Beyond that, distinct source integers collapse to the same float64
// (e.g. "-9223372036854775809.0" and "-9223372036854775808.0" both parse to
// -2^63). When we reach the float fallback for a 64-bit target, we therefore
// reject any |f| ≥ 2^53: genuine integer strings would have succeeded in
// ParseInt and never reached this path, so a float in the non-exact range
// here implies the source string is a non-integer (decimal/exponent form)
// that we cannot safely round to int64/uint64.
const (
	float64MaxInt64Exclusive  = 0x1p63 // 2^63, one past max int64
	float64MaxUint64Exclusive = 0x1p64 // 2^64, one past max uint64
	float64IntExactLimit      = 0x1p53 // 2^53, largest |x| with consecutive-integer precision
)

// parseIntWithFloatFallback is the integer rule of ParseValue (the one getColData also follows):
// 1. ParseInt succeeds → use it
// 2. ParseInt fails with ErrRange → reject (no fallback for overflow)
// 3. ParseInt fails otherwise → ParseFloat + range check
func parseIntWithFloatFallback(s string, bitSize int) (int64, error) {
	v, err := strconv.ParseInt(s, 10, bitSize)
	if err == nil {
		return v, nil
	}
	if errors.Is(err, strconv.ErrRange) {
		return 0, err
	}
	f, ferr := strconv.ParseFloat(s, 64)
	if ferr != nil {
		return 0, err
	}
	// Reject non-finite values ("nan" / "inf" parse successfully to NaN/±Inf).
	// NaN in particular is dangerous: NaN < x and NaN >= x are both false, so
	// the range checks below silently accept it and int64(NaN) is undefined.
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, err
	}
	if bitSize == 64 {
		// Exact-float boundaries: MaxInt64 rounds up to 2^63 in float64.
		// Also reject any |f| ≥ 2^53 — beyond float64's consecutive-integer
		// precision, round-trip through float is ambiguous (see const comment).
		if f < math.MinInt64 || f >= float64MaxInt64Exclusive {
			return 0, err
		}
		if f >= float64IntExactLimit || f <= -float64IntExactLimit {
			return 0, err
		}
	} else {
		// Compare f against the type's float-domain bounds BEFORE truncation.
		// Go's int64(f) truncates toward zero, so a naive post-truncation
		// bounds check silently accepts e.g. int32 "-2147483648.9" (truncates
		// to -2^31, which passes a ">= -2^31" check). Both ±(2^(N-1)) and
		// (2^(N-1) - 1) are exactly representable in float64 for N ≤ 53, so
		// the inclusive comparison has no rounding slack.
		lo := float64(int64(-1) << (bitSize - 1)) // -2^(N-1)
		hi := float64(int64(1)<<(bitSize-1) - 1)  //  2^(N-1) - 1
		if f < lo || f > hi {
			return 0, err
		}
	}
	return int64(f), nil
}

// parseUintWithFloatFallback is the unsigned integer rule of ParseValue:
// 1. ParseUint succeeds → use it
// 2. ParseUint fails with ErrRange → reject (no fallback for overflow)
// 3. ParseUint fails otherwise → ParseFloat + range check
func parseUintWithFloatFallback(s string, bitSize int) (uint64, error) {
	v, err := strconv.ParseUint(s, 10, bitSize)
	if err == nil {
		return v, nil
	}
	if errors.Is(err, strconv.ErrRange) {
		return 0, err
	}
	f, ferr := strconv.ParseFloat(s, 64)
	if ferr != nil || f < 0 {
		return 0, err
	}
	// Reject non-finite values (same NaN/Inf risk as parseIntWithFloatFallback).
	// Note: f < 0 above already rejects -Inf, but NaN is not ordered so must be
	// handled explicitly.
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, err
	}
	if bitSize == 64 {
		// Same boundary issue as int64 (MaxUint64 rounds up to 2^64) plus the
		// 2^53 precision limit for round-tripping integers through float64.
		if f >= float64MaxUint64Exclusive {
			return 0, err
		}
		if f >= float64IntExactLimit {
			return 0, err
		}
	} else {
		// Compare f against MaxUintN BEFORE truncation, to reject values like
		// uint32 "4294967295.9" that Go's uint64(f) would truncate to 2^32 - 1
		// and let through. 2^N - 1 is exactly representable in float64 for
		// N ≤ 53, so the inclusive upper bound has no rounding slack. The
		// lower bound is implicit: f < 0 is rejected above.
		hi := float64(uint64(1)<<bitSize - 1) // 2^N - 1
		if f > hi {
			return 0, err
		}
	}
	return uint64(f), nil
}
