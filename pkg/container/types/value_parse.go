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
	"context"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// TextParseOptions are the choices a text parse leaves to its caller.
type TextParseOptions struct {
	// Ctx is the error-reporting context of parse errors; nil means the
	// default one.
	Ctx context.Context
	// Loc is the time zone timestamp text is read in; nil means time.Local.
	Loc *time.Location
	// AllowTruncation keeps a decimal whose extra fraction digits were
	// rounded away instead of returning the truncation error.
	AllowTruncation bool
	// ZeroInvalidTemporal reads invalid date, datetime and timestamp text as
	// the zero value (MySQL's non-strict LOAD DATA).
	ZeroInvalidTemporal bool
	// NumericYear reads YEAR text as a number, in which 0 is year 0000, as
	// MySQL reads an unquoted value; otherwise it is a year string, in which
	// '0' is 2000.
	NumericYear bool
}

// ZeroTemporalText is the text of the zero value of a temporal type that has
// one ("0000-00-00" for a date), which ZeroInvalidTemporal produces.
func ZeroTemporalText(t T) (string, bool) {
	switch t {
	case T_date:
		return "0000-00-00", true
	case T_datetime, T_timestamp:
		return "0000-00-00 00:00:00", true
	case T_time, T_year:
		return "", false
	default:
		return "", false
	}
}

// textParser is a resolved parser: typed is a func(string) (V, error) for
// the value type V of the parsed type, boxed the same parse returning any.
type textParser struct {
	typed any
	boxed func(string) (any, error)
}

func newTextParser[V any](f func(string) (V, error)) textParser {
	return textParser{typed: f, boxed: func(s string) (any, error) {
		v, err := f(s)
		return v, err
	}}
}

// ParseFunc returns the parser of the text of a value of type t, resolved
// once so that each call does no type dispatch: the one text-to-value parse.
// V is t's value type, as vector.GetAny returns it: the Go element type for a
// fixed-size type, []byte for a varlena one (a vector as its encoded
// elements). An integer may be written as a float ("2024.0"); MEDIUMINT
// is range-checked; a FLOAT(p,s) or DOUBLE(p,s) value is rounded to s
// digits; a low-precision float that is NaN, infinite or out of range is
// rejected; a vector must have the column's dimension. ENUM and SET values
// need their member list, which t does not carry: those, and types with no
// text form, return an error, as does a V that is not t's value type. Adding
// a type means adding its case to textParserOf.
func ParseFunc[V any](t Type, opts TextParseOptions) (func(string) (V, error), error) {
	p, err := textParserOf(t, opts)
	if err != nil {
		return nil, err
	}
	f, ok := p.typed.(func(string) (V, error))
	if !ok {
		return nil, moerr.NewInternalErrorNoCtxf("ParseFunc: %T is not the value type of %s", *new(V), t.String())
	}
	return f, nil
}

// ParseAnyFunc is ParseFunc for a caller that does not know t's value type:
// the parse returns the value as any.
func ParseAnyFunc(t Type, opts TextParseOptions) (func(string) (any, error), error) {
	p, err := textParserOf(t, opts)
	if err != nil {
		return nil, err
	}
	return p.boxed, nil
}

// ParseValue parses s, the text of a value of type t, into the value (as
// vector.GetAny returns it): ParseFunc for a single value, such as a
// constant. A timestamp is read in loc, or in time.Local when loc is nil.
func ParseValue(t Type, s string, loc *time.Location) (any, error) {
	p, err := textParserOf(t, TextParseOptions{Loc: loc})
	if err != nil {
		return nil, err
	}
	return p.boxed(s)
}

func textParserOf(t Type, opts TextParseOptions) (textParser, error) {
	ctx := opts.Ctx
	if ctx == nil {
		ctx = moerr.Context()
	}
	switch t.Oid {
	case T_bool:
		return newTextParser(func(s string) (bool, error) { return ParseBoolWithContext(ctx, s) }), nil
	case T_bit:
		width := int(t.Width)
		if width <= 0 || width > 64 {
			width = 64
		}
		return newTextParser(func(s string) (uint64, error) { return strconv.ParseUint(s, 10, width) }), nil
	case T_int8:
		return newTextParser(func(s string) (int8, error) {
			v, err := parseIntWithFloatFallback(s, 8)
			return int8(v), err
		}), nil
	case T_int16:
		return newTextParser(func(s string) (int16, error) {
			v, err := parseIntWithFloatFallback(s, 16)
			return int16(v), err
		}), nil
	case T_int32:
		bits := 32
		if t.IsMediumInt() {
			bits = 24
		}
		return newTextParser(func(s string) (int32, error) {
			v, err := parseIntWithFloatFallback(s, bits)
			return int32(v), err
		}), nil
	case T_int64:
		return newTextParser(func(s string) (int64, error) { return parseIntWithFloatFallback(s, 64) }), nil
	case T_uint8:
		return newTextParser(func(s string) (uint8, error) {
			v, err := parseUintWithFloatFallback(s, 8)
			return uint8(v), err
		}), nil
	case T_uint16:
		return newTextParser(func(s string) (uint16, error) {
			v, err := parseUintWithFloatFallback(s, 16)
			return uint16(v), err
		}), nil
	case T_uint32:
		bits := 32
		if t.IsMediumInt() {
			bits = 24
		}
		return newTextParser(func(s string) (uint32, error) {
			v, err := parseUintWithFloatFallback(s, bits)
			return uint32(v), err
		}), nil
	case T_uint64:
		return newTextParser(func(s string) (uint64, error) { return parseUintWithFloatFallback(s, 64) }), nil
	case T_float32:
		if t.Scale < 0 || t.Width == 0 {
			return newTextParser(func(s string) (float32, error) {
				v, err := strconv.ParseFloat(s, 32)
				return float32(v), err
			}), nil
		}
		return newTextParser(func(s string) (float32, error) {
			d, err := ParseDecimal128WithContext(ctx, s, t.Width, t.Scale)
			return float32(Decimal128ToFloat64(d, t.Scale)), err
		}), nil
	case T_float64:
		if t.Scale < 0 || t.Width == 0 {
			return newTextParser(func(s string) (float64, error) { return strconv.ParseFloat(s, 64) }), nil
		}
		return newTextParser(func(s string) (float64, error) {
			d, err := ParseDecimal128WithContext(ctx, s, t.Width, t.Scale)
			return Decimal128ToFloat64(d, t.Scale), err
		}), nil
	case T_bf16:
		return newTextParser(lowPrecFloatParser(T_bf16, BF16FromFloat32)), nil
	case T_float16:
		return newTextParser(lowPrecFloatParser(T_float16, Float16FromFloat32)), nil
	case T_float8:
		return newTextParser(lowPrecFloatParser(T_float8, Float8FromFloat32)), nil
	case T_float4:
		return newTextParser(lowPrecFloatParser(T_float4, Float4FromFloat32)), nil
	case T_decimal64:
		return newTextParser(func(s string) (Decimal64, error) {
			d, err := ParseDecimal64WithContext(ctx, s, t.Width, t.Scale)
			return d, allowTruncation(err, opts)
		}), nil
	case T_decimal128:
		return newTextParser(func(s string) (Decimal128, error) {
			d, err := ParseDecimal128WithContext(ctx, s, t.Width, t.Scale)
			return d, allowTruncation(err, opts)
		}), nil
	case T_decimal256:
		return newTextParser(func(s string) (Decimal256, error) {
			d, err := ParseDecimal256WithContext(ctx, s, t.Width, t.Scale)
			return d, allowTruncation(err, opts)
		}), nil
	case T_date:
		return newTextParser(func(s string) (Date, error) {
			d, err := ParseDateCastWithContext(ctx, s)
			if err != nil && opts.ZeroInvalidTemporal {
				return ZeroDate, nil
			}
			return d, err
		}), nil
	case T_datetime:
		return newTextParser(func(s string) (Datetime, error) {
			d, err := ParseDatetimeWithContext(ctx, s, t.Scale)
			if err != nil && opts.ZeroInvalidTemporal {
				return ZeroDatetime, nil
			}
			return d, err
		}), nil
	case T_timestamp:
		loc := opts.Loc
		if loc == nil {
			loc = time.Local
		}
		return newTextParser(func(s string) (Timestamp, error) {
			d, err := ParseTimestampWithContext(ctx, loc, s, t.Scale)
			if err == nil && !ValidTimestamp(d) {
				err = moerr.NewInvalidInputf(ctx, "timestamp out of range: %s", s)
			}
			if err != nil && opts.ZeroInvalidTemporal {
				return ZeroTimestamp, nil
			}
			return d, err
		}), nil
	case T_time:
		return newTextParser(func(s string) (Time, error) { return ParseTimeWithContext(ctx, s, t.Scale) }), nil
	case T_year:
		if opts.NumericYear {
			return newTextParser(func(s string) (MoYear, error) {
				d, err := strconv.ParseInt(s, 10, 64)
				if err != nil {
					return 0, err
				}
				return ParseMoYearFromIntWithContext(ctx, d)
			}), nil
		}
		return newTextParser(func(s string) (MoYear, error) { return ParseMoYearWithContext(ctx, s) }), nil
	case T_uuid:
		return newTextParser(ParseUuid), nil
	case T_json:
		return newTextParser(func(s string) ([]byte, error) {
			j, err := ParseStringToByteJsonWithContext(ctx, s)
			if err != nil {
				return nil, err
			}
			return j.Marshal()
		}), nil
	case T_char, T_varchar, T_text, T_binary, T_varbinary, T_blob, T_datalink:
		return newTextParser(func(s string) ([]byte, error) { return []byte(s), nil }), nil
	case T_array_float32:
		return newTextParser(arrayParser[float32](ctx, t)), nil
	case T_array_float64:
		return newTextParser(arrayParser[float64](ctx, t)), nil
	case T_array_bf16:
		return newTextParser(arrayParser[BF16](ctx, t)), nil
	case T_array_float16:
		return newTextParser(arrayParser[Float16](ctx, t)), nil
	case T_array_int8:
		return newTextParser(arrayParser[int8](ctx, t)), nil
	case T_array_uint8:
		return newTextParser(arrayParser[uint8](ctx, t)), nil
	case T_array_float8, T_array_float4:
		// "[...]" is quantized; the exact form (a JSON object) is the cell as written
		f, _ := t.Oid.BlockScaledFormat()
		return newTextParser(func(s string) ([]byte, error) {
			cell, err := StringToBlockScaled(f, s)
			if err != nil {
				return nil, err
			}
			return cell, checkArrayDim(t, BlockScaledDim(cell))
		}), nil
	default:
		return textParser{}, moerr.NewNotSupportedNoCtxf("parsing a %s value from text", t.String())
	}
}

// lowPrecFloatParser parses a low-precision float: rounded to float32 by
// round-to-odd, so the second rounding to the target is exact, and rejected
// when NaN, infinite or out of the target's range rather than saturated.
func lowPrecFloatParser[F LowPrecFloat](oid T, from func(float32) F) func(string) (F, error) {
	return func(s string) (F, error) {
		var zero F
		f, f64, err := Float32RoundToOddString(s)
		if err != nil {
			return zero, err
		}
		if err := RejectNarrowFloatInput(f, oid, f64, s); err != nil {
			return zero, err
		}
		return CanonicalLowPrecFloat(from(f)), nil
	}
}

func arrayParser[E ArrayElement](ctx context.Context, t Type) func(string) ([]byte, error) {
	return func(s string) ([]byte, error) {
		arr, err := stringToArrayWithContext[E](ctx, s)
		if err != nil {
			return nil, err
		}
		if err := checkArrayDim(t, len(arr)); err != nil {
			return nil, err
		}
		return ArrayToBytes(arr), nil
	}
}

// checkArrayDim checks a vector of dim elements against the declared
// dimension of t (MaxArrayDimension accepts any).
func checkArrayDim(t Type, dim int) error {
	if int(t.Width) != MaxArrayDimension && int(t.Width) != dim {
		return moerr.NewArrayDefMismatchNoCtx(int(t.Width), dim)
	}
	return nil
}

func allowTruncation(err error, opts TextParseOptions) error {
	if err != nil && opts.AllowTruncation && moerr.IsMoErrCode(err, moerr.ErrDataTruncated) {
		return nil
	}
	return err
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

// parseIntWithFloatFallback is the integer rule of ParseFunc:
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

// parseUintWithFloatFallback is the unsigned integer rule of ParseFunc:
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
