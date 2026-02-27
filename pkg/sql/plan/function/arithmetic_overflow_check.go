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

// addInt64WithOverflowCheck checks for overflow in int64 addition
func addInt64WithOverflowCheck(ctx context.Context, v1, v2 int64) (int64, error) {
	result := v1 + v2
	// Overflow detection:
	// - If both operands have the same sign, result must have the same sign
	// - Positive + Positive overflow if result is negative or zero
	// - Negative + Negative overflow if result is positive or zero (when both are negative)
	if (v1 > 0 && v2 > 0 && result <= 0) || (v1 < 0 && v2 < 0 && result >= 0) {
		return 0, moerr.NewOutOfRangef(ctx, "int64", "(%d + %d)", v1, v2)
	}
	return result, nil
}

// addInt32WithOverflowCheck checks for overflow in int32 addition
func addInt32WithOverflowCheck(ctx context.Context, v1, v2 int32) (int32, error) {
	result := v1 + v2
	if (v1 > 0 && v2 > 0 && result <= 0) || (v1 < 0 && v2 < 0 && result >= 0) {
		return 0, moerr.NewOutOfRangef(ctx, "int32", "(%d + %d)", v1, v2)
	}
	return result, nil
}

// addInt16WithOverflowCheck checks for overflow in int16 addition
func addInt16WithOverflowCheck(ctx context.Context, v1, v2 int16) (int16, error) {
	result := v1 + v2
	if (v1 > 0 && v2 > 0 && result <= 0) || (v1 < 0 && v2 < 0 && result >= 0) {
		return 0, moerr.NewOutOfRangef(ctx, "int16", "(%d + %d)", v1, v2)
	}
	return result, nil
}

// addInt8WithOverflowCheck checks for overflow in int8 addition
func addInt8WithOverflowCheck(ctx context.Context, v1, v2 int8) (int8, error) {
	result := v1 + v2
	if (v1 > 0 && v2 > 0 && result <= 0) || (v1 < 0 && v2 < 0 && result >= 0) {
		return 0, moerr.NewOutOfRangef(ctx, "int8", "(%d + %d)", v1, v2)
	}
	return result, nil
}

// addUint64WithOverflowCheck checks for overflow in uint64 addition
func addUint64WithOverflowCheck(ctx context.Context, v1, v2 uint64) (uint64, error) {
	result := v1 + v2
	// Unsigned overflow: if result < either operand, overflow occurred
	if result < v1 || result < v2 {
		return 0, moerr.NewOutOfRangef(ctx, "uint64", "(%d + %d)", v1, v2)
	}
	return result, nil
}

// addUint32WithOverflowCheck checks for overflow in uint32 addition
func addUint32WithOverflowCheck(ctx context.Context, v1, v2 uint32) (uint32, error) {
	result := v1 + v2
	if result < v1 || result < v2 {
		return 0, moerr.NewOutOfRangef(ctx, "uint32", "(%d + %d)", v1, v2)
	}
	return result, nil
}

// addUint16WithOverflowCheck checks for overflow in uint16 addition
func addUint16WithOverflowCheck(ctx context.Context, v1, v2 uint16) (uint16, error) {
	result := v1 + v2
	if result < v1 || result < v2 {
		return 0, moerr.NewOutOfRangef(ctx, "uint16", "(%d + %d)", v1, v2)
	}
	return result, nil
}

// addUint8WithOverflowCheck checks for overflow in uint8 addition
func addUint8WithOverflowCheck(ctx context.Context, v1, v2 uint8) (uint8, error) {
	result := v1 + v2
	if result < v1 || result < v2 {
		return 0, moerr.NewOutOfRangef(ctx, "uint8", "(%d + %d)", v1, v2)
	}
	return result, nil
}

// subInt64WithOverflowCheck checks for overflow in int64 subtraction
func subInt64WithOverflowCheck(ctx context.Context, v1, v2 int64) (int64, error) {
	result := v1 - v2
	// Overflow detection for subtraction:
	// - Positive - Negative overflow if result is negative
	// - Negative - Positive overflow if result is positive
	if (v1 > 0 && v2 < 0 && result < 0) || (v1 < 0 && v2 > 0 && result > 0) {
		return 0, moerr.NewOutOfRangef(ctx, "int64", "(%d - %d)", v1, v2)
	}
	return result, nil
}

// subInt32WithOverflowCheck checks for overflow in int32 subtraction
func subInt32WithOverflowCheck(ctx context.Context, v1, v2 int32) (int32, error) {
	result := v1 - v2
	if (v1 > 0 && v2 < 0 && result < 0) || (v1 < 0 && v2 > 0 && result > 0) {
		return 0, moerr.NewOutOfRangef(ctx, "int32", "(%d - %d)", v1, v2)
	}
	return result, nil
}

// subInt16WithOverflowCheck checks for overflow in int16 subtraction
func subInt16WithOverflowCheck(ctx context.Context, v1, v2 int16) (int16, error) {
	result := v1 - v2
	if (v1 > 0 && v2 < 0 && result < 0) || (v1 < 0 && v2 > 0 && result > 0) {
		return 0, moerr.NewOutOfRangef(ctx, "int16", "(%d - %d)", v1, v2)
	}
	return result, nil
}

// subInt8WithOverflowCheck checks for overflow in int8 subtraction
func subInt8WithOverflowCheck(ctx context.Context, v1, v2 int8) (int8, error) {
	result := v1 - v2
	if (v1 > 0 && v2 < 0 && result < 0) || (v1 < 0 && v2 > 0 && result > 0) {
		return 0, moerr.NewOutOfRangef(ctx, "int8", "(%d - %d)", v1, v2)
	}
	return result, nil
}

// subUint64WithOverflowCheck checks for underflow in uint64 subtraction
func subUint64WithOverflowCheck(ctx context.Context, v1, v2 uint64) (uint64, error) {
	if v1 < v2 {
		return 0, moerr.NewOutOfRangef(ctx, "uint64", "(%d - %d)", v1, v2)
	}
	return v1 - v2, nil
}

// subUint32WithOverflowCheck checks for underflow in uint32 subtraction
func subUint32WithOverflowCheck(ctx context.Context, v1, v2 uint32) (uint32, error) {
	if v1 < v2 {
		return 0, moerr.NewOutOfRangef(ctx, "uint32", "(%d - %d)", v1, v2)
	}
	return v1 - v2, nil
}

// subUint16WithOverflowCheck checks for underflow in uint16 subtraction
func subUint16WithOverflowCheck(ctx context.Context, v1, v2 uint16) (uint16, error) {
	if v1 < v2 {
		return 0, moerr.NewOutOfRangef(ctx, "uint16", "(%d - %d)", v1, v2)
	}
	return v1 - v2, nil
}

// subUint8WithOverflowCheck checks for underflow in uint8 subtraction
func subUint8WithOverflowCheck(ctx context.Context, v1, v2 uint8) (uint8, error) {
	if v1 < v2 {
		return 0, moerr.NewOutOfRangef(ctx, "uint8", "(%d - %d)", v1, v2)
	}
	return v1 - v2, nil
}

// mulInt64WithOverflowCheck checks for overflow in int64 multiplication
func mulInt64WithOverflowCheck(ctx context.Context, v1, v2 int64) (int64, error) {
	if v1 == 0 || v2 == 0 {
		return 0, nil
	}

	// Special case: -9223372036854775808 * -1 overflows
	if v1 == math.MinInt64 && v2 == -1 || v2 == math.MinInt64 && v1 == -1 {
		return 0, moerr.NewOutOfRangef(ctx, "int64", "(%d * %d)", v1, v2)
	}

	result := v1 * v2
	// Check if division brings back the original value
	if result/v2 != v1 {
		return 0, moerr.NewOutOfRangef(ctx, "int64", "(%d * %d)", v1, v2)
	}
	return result, nil
}

// mulInt32WithOverflowCheck checks for overflow in int32 multiplication
func mulInt32WithOverflowCheck(ctx context.Context, v1, v2 int32) (int32, error) {
	if v1 == 0 || v2 == 0 {
		return 0, nil
	}

	if v1 == math.MinInt32 && v2 == -1 || v2 == math.MinInt32 && v1 == -1 {
		return 0, moerr.NewOutOfRangef(ctx, "int32", "(%d * %d)", v1, v2)
	}

	result := v1 * v2
	if result/v2 != v1 {
		return 0, moerr.NewOutOfRangef(ctx, "int32", "(%d * %d)", v1, v2)
	}
	return result, nil
}

// mulInt16WithOverflowCheck checks for overflow in int16 multiplication
func mulInt16WithOverflowCheck(ctx context.Context, v1, v2 int16) (int16, error) {
	if v1 == 0 || v2 == 0 {
		return 0, nil
	}

	if v1 == math.MinInt16 && v2 == -1 || v2 == math.MinInt16 && v1 == -1 {
		return 0, moerr.NewOutOfRangef(ctx, "int16", "(%d * %d)", v1, v2)
	}

	result := v1 * v2
	if result/v2 != v1 {
		return 0, moerr.NewOutOfRangef(ctx, "int16", "(%d * %d)", v1, v2)
	}
	return result, nil
}

// mulInt8WithOverflowCheck checks for overflow in int8 multiplication
func mulInt8WithOverflowCheck(ctx context.Context, v1, v2 int8) (int8, error) {
	if v1 == 0 || v2 == 0 {
		return 0, nil
	}

	if v1 == math.MinInt8 && v2 == -1 || v2 == math.MinInt8 && v1 == -1 {
		return 0, moerr.NewOutOfRangef(ctx, "int8", "(%d * %d)", v1, v2)
	}

	result := v1 * v2
	if result/v2 != v1 {
		return 0, moerr.NewOutOfRangef(ctx, "int8", "(%d * %d)", v1, v2)
	}
	return result, nil
}

// mulUint64WithOverflowCheck checks for overflow in uint64 multiplication
func mulUint64WithOverflowCheck(ctx context.Context, v1, v2 uint64) (uint64, error) {
	if v1 == 0 || v2 == 0 {
		return 0, nil
	}

	if v1 > math.MaxUint64/v2 {
		return 0, moerr.NewOutOfRangef(ctx, "uint64", "(%d * %d)", v1, v2)
	}
	return v1 * v2, nil
}

// mulUint32WithOverflowCheck checks for overflow in uint32 multiplication
func mulUint32WithOverflowCheck(ctx context.Context, v1, v2 uint32) (uint32, error) {
	if v1 == 0 || v2 == 0 {
		return 0, nil
	}

	if v1 > math.MaxUint32/v2 {
		return 0, moerr.NewOutOfRangef(ctx, "uint32", "(%d * %d)", v1, v2)
	}
	return v1 * v2, nil
}

// mulUint16WithOverflowCheck checks for overflow in uint16 multiplication
func mulUint16WithOverflowCheck(ctx context.Context, v1, v2 uint16) (uint16, error) {
	if v1 == 0 || v2 == 0 {
		return 0, nil
	}

	if v1 > math.MaxUint16/v2 {
		return 0, moerr.NewOutOfRangef(ctx, "uint16", "(%d * %d)", v1, v2)
	}
	return v1 * v2, nil
}

// mulUint8WithOverflowCheck checks for overflow in uint8 multiplication
func mulUint8WithOverflowCheck(ctx context.Context, v1, v2 uint8) (uint8, error) {
	if v1 == 0 || v2 == 0 {
		return 0, nil
	}

	if v1 > math.MaxUint8/v2 {
		return 0, moerr.NewOutOfRangef(ctx, "uint8", "(%d * %d)", v1, v2)
	}
	return v1 * v2, nil
}

// addFloat64WithOverflowCheck checks for overflow in float64 addition
func addFloat64WithOverflowCheck(ctx context.Context, v1, v2 float64) (float64, error) {
	result := v1 + v2
	if math.IsInf(result, 0) {
		return 0, moerr.NewOutOfRangef(ctx, "float64", "DOUBLE value is out of range in '(%v + %v)'", v1, v2)
	}
	return result, nil
}

// addFloat32WithOverflowCheck checks for overflow in float32 addition
func addFloat32WithOverflowCheck(ctx context.Context, v1, v2 float32) (float32, error) {
	result := v1 + v2
	if math.IsInf(float64(result), 0) {
		return 0, moerr.NewOutOfRangef(ctx, "float32", "FLOAT value is out of range in '(%v + %v)'", v1, v2)
	}
	return result, nil
}
