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

package arrowbridge

import (
	"context"
	"math/big"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// RenderStagingDecimalText renders the exact value coeff x 10^-scale per the
// LOAD attributed-DECIMAL text staging encoding rule: fixed-point text when
// the schema is well-formed (0 <= scale <= 76, precision <= 76,
// scale <= precision); bounded scientific text otherwise. digits(coeff) is
// bounded by the physical content actually read, so no schema claim can
// amplify an allocation beyond the decoded bytes. The engine's decimal
// parsers consume both rendered forms, including exponent forms whose scale
// application is allocation-free. The scale negation widens to int64 FIRST:
// negating math.MinInt32 in int32 would wrap the scientific exponent's sign
// and silently render a huge value as 1e-2147483648 (quantizing to zero at
// the target) instead of 1e2147483648 (rejected as out of range).
func RenderStagingDecimalText(coeff *big.Int, precision, scale int32) string {
	if scale >= 0 && scale <= 76 && precision >= 0 && precision <= 76 && scale <= precision {
		return renderStagingFixedPointText(coeff, scale)
	}
	return coeff.String() + "e" + strconv.FormatInt(-int64(scale), 10)
}

// renderStagingFixedPointText renders coeff x 10^-scale with scale fractional
// digits, sign faithful (coefficient -4 at scale 3 renders "-0.004"; 100 at
// scale 2 renders "1.00"; scale-surplus digits are zero-padded, never
// rounded).
func renderStagingFixedPointText(coeff *big.Int, scale int32) string {
	neg := coeff.Sign() < 0
	digits := new(big.Int).Abs(coeff).Text(10)
	if scale > 0 {
		if int(scale) >= len(digits) {
			digits = strings.Repeat("0", int(scale)-len(digits)+1) + digits
		}
		text := digits[:len(digits)-int(scale)] + "." + digits[len(digits)-int(scale):]
		if neg {
			return "-" + text
		}
		return text
	}
	if neg {
		return "-" + digits
	}
	return digits
}

// arrowDecimalPoweredTypes enumerates the Arrow decimal array kinds whose
// values render coefficient text for staged admission. Decimal64/Decimal32
// physicals are absent from Arrow IPC load sources in this gate; the checked
// widening rule retains its own contract.
type arrowDecimalTypeInfo struct {
	precision int32
	scale     int32
}

func arrowDecimalTypeInfoOf(typ arrow.DataType) (arrowDecimalTypeInfo, bool) {
	switch typed := typ.(type) {
	case *arrow.Decimal128Type:
		return arrowDecimalTypeInfo{precision: typed.Precision, scale: typed.Scale}, true
	case *arrow.Decimal256Type:
		return arrowDecimalTypeInfo{precision: typed.Precision, scale: typed.Scale}, true
	}
	return arrowDecimalTypeInfo{}, false
}

// appendArrowStageTextValue renders one Arrow value to text into the
// scan-owned staging vector. Borrowed Arrow buffers are never mutated or
// padded in place; the rendered bytes are fresh memory. Select cases mirror
// the Parquet staging encodings: decimal logical types render coeff x
// 10^-scale; integer physicals render exact integer text; float physicals
// render shortest round-trip text; bool renders "1"/"0"; string kinds are
// borrowed raw by the STRING conversion and never reach this renderer.
func appendArrowStageTextValue(
	ctx context.Context,
	vec *vector.Vector,
	values arrow.Array,
	row int,
	isNull bool,
	mp *mpool.MPool,
) error {
	if isNull {
		return vector.AppendBytes(vec, nil, true, mp)
	}
	if info, ok := arrowDecimalTypeInfoOf(values.DataType()); ok {
		var text string
		switch typed := values.(type) {
		case *array.Decimal128:
			text = RenderStagingDecimalText(typed.Value(row).BigInt(), info.precision, info.scale)
		case *array.Decimal256:
			text = RenderStagingDecimalText(typed.Value(row).BigInt(), info.precision, info.scale)
		default:
			return moerr.NewInvalidInputf(ctx, "unsupported Arrow decimal array %T for staged decimal text", values)
		}
		return vector.AppendBytes(vec, []byte(text), false, mp)
	}
	var text string
	switch typed := values.(type) {
	case *array.Int8:
		text = strconv.FormatInt(int64(typed.Value(row)), 10)
	case *array.Int16:
		text = strconv.FormatInt(int64(typed.Value(row)), 10)
	case *array.Int32:
		text = strconv.FormatInt(int64(typed.Value(row)), 10)
	case *array.Int64:
		text = strconv.FormatInt(typed.Value(row), 10)
	case *array.Uint8:
		text = strconv.FormatUint(uint64(typed.Value(row)), 10)
	case *array.Uint16:
		text = strconv.FormatUint(uint64(typed.Value(row)), 10)
	case *array.Uint32:
		text = strconv.FormatUint(uint64(typed.Value(row)), 10)
	case *array.Uint64:
		text = strconv.FormatUint(typed.Value(row), 10)
	case *array.Float16:
		text = strconv.FormatFloat(float64(typed.Value(row).Float32()), 'g', -1, 32)
	case *array.Float32:
		text = strconv.FormatFloat(float64(typed.Value(row)), 'g', -1, 32)
	case *array.Float64:
		text = strconv.FormatFloat(typed.Value(row), 'g', -1, 64)
	case *array.Boolean:
		if typed.Value(row) {
			text = "1"
		} else {
			text = "0"
		}
	default:
		return moerr.NewInvalidInputf(ctx, "unsupported Arrow array %T for staged decimal text", values)
	}
	return vector.AppendBytes(vec, []byte(text), false, mp)
}

const (
	// arrowDecimal128MaxDigits and arrowDecimal256MaxDigits bound the
	// coefficient digits the physical Arrow decimal storage can hold:
	// BigInt interprets the value as a signed two's-complement integer, so
	// Decimal128 carries at most |2^127| (39 digits) and Decimal256 at most
	// |2^255| (77 digits), whatever precision the schema declares.
	arrowDecimal128MaxDigits = 39
	arrowDecimal256MaxDigits = 77
	// stagingFixedPointMaxDigitSpan bounds the digit run the fixed-point
	// staging form can emit: the well-formedness gate caps the source scale
	// at 76, so zero padding grows the run to at most scale+1 characters
	// before the point is inserted.
	stagingFixedPointMaxDigitSpan = 77
	// stagingScientificMaxSuffix bounds the bytes the scientific staging form
	// appends past the coefficient digits: the 'e' separator plus the decimal
	// text of the negated int32 scale (one sign plus up to 10 digits).
	stagingScientificMaxSuffix = 12
)

// renderedStageTextBound conservatively upper-bounds the rendered text
// payload one staged row can produce for the given Arrow source type:
// decimals charge the larger of their two render forms (see
// renderedStageDecimalBound), integers at most 20 digits + sign, floats
// shortest round-trip text with a small exponent, and bools a single byte.
// Batch budgeting must count this payload or a window of individually valid
// rows can exceed the configured byte budget once rendered.
func renderedStageTextBound(dt arrow.DataType) (int, error) {
	switch dt.(type) {
	case *arrow.Decimal128Type:
		return renderedStageDecimalBound(arrowDecimal128MaxDigits), nil
	case *arrow.Decimal256Type:
		return renderedStageDecimalBound(arrowDecimal256MaxDigits), nil
	}
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return 21, nil
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		return 32, nil
	case arrow.BOOL:
		return 1, nil
	}
	return 0, moerr.NewInvalidInputNoCtxf("unsupported Arrow staged text type %s", dt)
}

// renderedStageDecimalBound upper-bounds the rendered bytes of one staged
// decimal row as the larger of the two reachable render forms: fixed-point
// text needs a sign, the coefficient digits zero-padded to the source span,
// and the point; bounded scientific text needs a sign, the physical
// coefficient digits, and the exponent suffix. Both forms charge the
// physical digit span rather than the declared precision, so no schema claim
// can under-budget a window of individually valid rows.
func renderedStageDecimalBound(maxDigits int) int {
	fixedPoint := 1 + max(maxDigits, stagingFixedPointMaxDigitSpan) + 1
	scientific := 1 + maxDigits + stagingScientificMaxSuffix
	return max(fixedPoint, scientific)
}

// isArrowStageTextSource reports the Arrow source kinds that materialize
// rendered text for an attributed-DECIMAL staging target. String kinds bind
// to the borrowed raw-varlen conversion; NULL binds to the null conversion;
// binary/nested/temporal kinds keep the existing LOAD rejection.
func isArrowStageTextSource(id arrow.Type) bool {
	switch id {
	case arrow.DECIMAL128, arrow.DECIMAL256,
		arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64,
		arrow.BOOL:
		return true
	}
	return false
}
