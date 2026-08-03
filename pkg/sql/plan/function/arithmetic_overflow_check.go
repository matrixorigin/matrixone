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

package function

import (
	"context"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// Overflow checking functions for integer arithmetic operations
// These functions implement strict overflow detection for SELECT queries,
// matching MySQL 8.0 strict mode behavior (ERROR 1690: BIGINT value is out of range)

// signedInt constrains the signed integer types used by checked arithmetic.
type signedInt interface {
	int8 | int16 | int32 | int64
}

// unsignedInt constrains the unsigned integer types used by checked arithmetic.
type unsignedInt interface {
	uint8 | uint16 | uint32 | uint64
}

// integerTypeName returns the type name used in out-of-range error messages.
// It is only called on the error path, so the interface conversion cost does
// not affect the vectorized fast path.
func integerTypeName[T signedInt | unsignedInt](v T) string {
	switch any(v).(type) {
	case int8:
		return "int8"
	case int16:
		return "int16"
	case int32:
		return "int32"
	case int64:
		return "int64"
	case uint8:
		return "uint8"
	case uint16:
		return "uint16"
	case uint32:
		return "uint32"
	default:
		return "uint64"
	}
}

// addSignedWithOverflowCheck checks for overflow in signed integer addition.
// Overflow happened iff the wrapped result moved in the opposite direction of v2.
func addSignedWithOverflowCheck[T signedInt](ctx context.Context, v1, v2 T) (T, error) {
	result := v1 + v2
	if (v2 > 0 && result < v1) || (v2 < 0 && result > v1) {
		return 0, moerr.NewOutOfRangef(ctx, integerTypeName(v1), "(%d + %d)", v1, v2)
	}
	return result, nil
}

// addUnsignedWithOverflowCheck checks for overflow in unsigned integer addition.
func addUnsignedWithOverflowCheck[T unsignedInt](ctx context.Context, v1, v2 T) (T, error) {
	result := v1 + v2
	if result < v1 {
		return 0, moerr.NewOutOfRangef(ctx, integerTypeName(v1), "(%d + %d)", v1, v2)
	}
	return result, nil
}

// subSignedWithOverflowCheck checks for overflow in signed integer subtraction.
// The exact result must move in the opposite direction of v2; this also covers
// v1 - MinInt (e.g. 0 - (-9223372036854775808)) which two's-complement wraps.
func subSignedWithOverflowCheck[T signedInt](ctx context.Context, v1, v2 T) (T, error) {
	result := v1 - v2
	if (v2 > 0 && result > v1) || (v2 < 0 && result < v1) {
		return 0, moerr.NewOutOfRangef(ctx, integerTypeName(v1), "(%d - %d)", v1, v2)
	}
	return result, nil
}

// subUnsignedWithOverflowCheck checks for underflow in unsigned integer subtraction.
func subUnsignedWithOverflowCheck[T unsignedInt](ctx context.Context, v1, v2 T) (T, error) {
	if v1 < v2 {
		return 0, moerr.NewOutOfRangef(ctx, integerTypeName(v1), "(%d - %d)", v1, v2)
	}
	return v1 - v2, nil
}

// mulSignedWithOverflowCheck checks for overflow in signed integer multiplication.
// MinInt * -1 is detected explicitly because result/v2 wraps back to v1 in that
// case (v < 0 && -v == v identifies the most negative value without a per-type
// constant); every other overflow fails the division round-trip check.
func mulSignedWithOverflowCheck[T signedInt](ctx context.Context, v1, v2 T) (T, error) {
	if v1 == 0 || v2 == 0 {
		return 0, nil
	}
	if (v1 < 0 && -v1 == v1 && v2 == -1) || (v2 < 0 && -v2 == v2 && v1 == -1) {
		return 0, moerr.NewOutOfRangef(ctx, integerTypeName(v1), "(%d * %d)", v1, v2)
	}
	result := v1 * v2
	if result/v2 != v1 {
		return 0, moerr.NewOutOfRangef(ctx, integerTypeName(v1), "(%d * %d)", v1, v2)
	}
	return result, nil
}

// mulUnsignedWithOverflowCheck checks for overflow in unsigned integer multiplication.
func mulUnsignedWithOverflowCheck[T unsignedInt](ctx context.Context, v1, v2 T) (T, error) {
	if v1 == 0 || v2 == 0 {
		return 0, nil
	}
	result := v1 * v2
	if result/v2 != v1 {
		return 0, moerr.NewOutOfRangef(ctx, integerTypeName(v1), "(%d * %d)", v1, v2)
	}
	return result, nil
}

// floatDivToInt64WithOverflowCheck implements DIV for float operands.
// The quotient is computed in float64 and truncated toward zero; a quotient
// whose magnitude reaches 2^63 (or NaN from Inf/Inf) raises out-of-range,
// matching MySQL 8.4 ERROR 1690 "BIGINT value is out of range". MySQL computes
// DIV on unsigned magnitudes, so a quotient of exactly -2^63 errors as well
// even though it is representable as MinInt64 (verified against MySQL 8.4.8).
// float64(math.MaxInt64) rounds up to 2^63, so the comparisons below reject
// exactly the quotients with magnitude >= 2^63.
func floatDivToInt64WithOverflowCheck[T float32 | float64](ctx context.Context, v1, v2 T) (int64, error) {
	q := float64(v1) / float64(v2)
	if math.IsNaN(q) || q >= float64(math.MaxInt64) || q <= float64(math.MinInt64) {
		return 0, moerr.NewOutOfRangef(ctx, "BIGINT", "(%v DIV %v)", v1, v2)
	}
	return int64(q), nil
}

// addFloat64WithOverflowCheck checks for overflow in float64 addition
func addFloat64WithOverflowCheck(ctx context.Context, v1, v2 float64) (float64, error) {
	return checkFloat64Result(ctx, v1+v2, "+", v1, v2)
}

// addFloat32WithOverflowCheck checks for overflow in float32 addition
func addFloat32WithOverflowCheck(ctx context.Context, v1, v2 float32) (float32, error) {
	return checkFloat32Result(ctx, v1+v2, "+", v1, v2)
}

func subFloat64WithOverflowCheck(ctx context.Context, v1, v2 float64) (float64, error) {
	return checkFloat64Result(ctx, v1-v2, "-", v1, v2)
}

func subFloat32WithOverflowCheck(ctx context.Context, v1, v2 float32) (float32, error) {
	return checkFloat32Result(ctx, v1-v2, "-", v1, v2)
}

func mulFloat64WithOverflowCheck(ctx context.Context, v1, v2 float64) (float64, error) {
	return checkFloat64Result(ctx, v1*v2, "*", v1, v2)
}

func mulFloat32WithOverflowCheck(ctx context.Context, v1, v2 float32) (float32, error) {
	return checkFloat32Result(ctx, v1*v2, "*", v1, v2)
}

func divFloat64WithOverflowCheck(ctx context.Context, v1, v2 float64) (float64, error) {
	return checkFloat64Result(ctx, v1/v2, "/", v1, v2)
}

func divFloat32WithOverflowCheck(ctx context.Context, v1, v2 float32) (float32, error) {
	return checkFloat32Result(ctx, v1/v2, "/", v1, v2)
}

func checkFloat64Result(ctx context.Context, result float64, op string, v1, v2 float64) (float64, error) {
	if math.IsInf(result, 0) && !math.IsInf(v1, 0) && !math.IsInf(v2, 0) {
		return 0, moerr.NewOutOfRangef(ctx, "float64", "DOUBLE value is out of range in '(%v %s %v)'", v1, op, v2)
	}
	return result, nil
}

func checkFloat32Result(ctx context.Context, result float32, op string, v1, v2 float32) (float32, error) {
	if math.IsInf(float64(result), 0) && !math.IsInf(float64(v1), 0) && !math.IsInf(float64(v2), 0) {
		return 0, moerr.NewOutOfRangef(ctx, "float32", "FLOAT value is out of range in '(%v %s %v)'", v1, op, v2)
	}
	return result, nil
}
