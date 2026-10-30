// Copyright 2023 Matrix Origin
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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// This file implements the unsigned DECIMAL domain admission step. It is a
// plan-composed identity function evaluated on the raw source value BEFORE the
// assignment cast quantizes to the target scale, mirroring MySQL's
// Field_new_decimal::store_value order (sign checked before scale rounding).
//
// Three entries exist on the same build-time policy axis as the assignment
// cast family:
//   - unsigned_decimal_admission        — sql_mode resolved at execution time
//   - unsigned_decimal_admission_strict — always strict; used only inside the
//     authoring-time DEFAULT validation composition
//   - unsigned_decimal_admission_ignore — INSERT/UPDATE IGNORE policy
//
// parameters[0] is the source value; parameters[1] is the target type carrier
// (decimal64/128/256 with the destination width/scale), exactly like cast;
// parameters[2] is a constant carrying the destination column name used by the
// 1264 strict error and its lenient warning. The result type equals the source
// type: admitted values pass through unchanged and the assignment cast
// performs the ordinary conversion.

type unsignedDecimalAdmissionEntry uint8

const (
	// unsignedDecimalAdmissionMode resolves STRICT_TRANS_TABLES /
	// STRICT_ALL_TABLES / TRADITIONAL at execution time.
	unsignedDecimalAdmissionMode unsignedDecimalAdmissionEntry = iota
	// unsignedDecimalAdmissionStrict always rejects out-of-domain values.
	unsignedDecimalAdmissionStrict
	// unsignedDecimalAdmissionIgnore always adjusts out-of-domain values.
	unsignedDecimalAdmissionIgnore
)

// NewUnsignedDecimalAdmission is the sql-mode-resolved DML entry.
func NewUnsignedDecimalAdmission(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return newUnsignedDecimalAdmission(parameters, result, proc, length, selectList, unsignedDecimalAdmissionMode)
}

// NewUnsignedDecimalAdmissionStrict is the authoring-time validation entry.
func NewUnsignedDecimalAdmissionStrict(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return newUnsignedDecimalAdmission(parameters, result, proc, length, selectList, unsignedDecimalAdmissionStrict)
}

// NewUnsignedDecimalAdmissionIgnore is the INSERT/UPDATE IGNORE entry.
func NewUnsignedDecimalAdmissionIgnore(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return newUnsignedDecimalAdmission(parameters, result, proc, length, selectList, unsignedDecimalAdmissionIgnore)
}

func newUnsignedDecimalAdmission(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	entry unsignedDecimalAdmissionEntry,
) error {
	if len(parameters) != 3 {
		return moerr.NewInvalidInputNoCtxf("unsigned decimal admission expects 3 parameters, got %d", len(parameters))
	}
	targetType := parameters[1].GetType()
	if !targetType.Oid.IsDecimal() {
		return moerr.NewInvalidInputNoCtxf("unsigned decimal admission target must be decimal, got %s", targetType.Oid)
	}
	column := unsignedDecimalAdmissionColumnLabel(parameters[2])
	from := parameters[0]
	if from.GetType().Oid == types.T_any {
		// Scalar NULL source (defensive; the plan layer never composes the
		// admission over NULL-by-construction sources): identity nulls, no
		// domain diagnostics. The result wrapper follows the varlena layout
		// used for ANY vectors, mirroring appendNulls.
		if r, ok := result.(*vector.FunctionResult[types.Varlena]); ok {
			for i := 0; i < length; i++ {
				if err := r.AppendBytes(nil, true); err != nil {
					return err
				}
			}
			return nil
		}
		return moerr.NewInternalErrorNoCtx("unsigned decimal admission: unexpected result wrapper for scalar null source")
	}
	switch from.GetType().Oid {
	case types.T_decimal64:
		return unsignedDecimalAdmissionFixed[types.Decimal64](proc, from, result, length, selectList, entry, column)
	case types.T_decimal128:
		return unsignedDecimalAdmissionFixed[types.Decimal128](proc, from, result, length, selectList, entry, column)
	case types.T_decimal256:
		return unsignedDecimalAdmissionFixed[types.Decimal256](proc, from, result, length, selectList, entry, column)
	case types.T_float32:
		return unsignedDecimalAdmissionFixed[float32](proc, from, result, length, selectList, entry, column)
	case types.T_float64:
		return unsignedDecimalAdmissionFixed[float64](proc, from, result, length, selectList, entry, column)
	case types.T_int8:
		return unsignedDecimalAdmissionFixed[int8](proc, from, result, length, selectList, entry, column)
	case types.T_int16:
		return unsignedDecimalAdmissionFixed[int16](proc, from, result, length, selectList, entry, column)
	case types.T_int32:
		return unsignedDecimalAdmissionFixed[int32](proc, from, result, length, selectList, entry, column)
	case types.T_int64:
		return unsignedDecimalAdmissionFixed[int64](proc, from, result, length, selectList, entry, column)
	case types.T_uint8:
		return unsignedDecimalAdmissionFixed[uint8](proc, from, result, length, selectList, entry, column)
	case types.T_uint16:
		return unsignedDecimalAdmissionFixed[uint16](proc, from, result, length, selectList, entry, column)
	case types.T_uint32:
		return unsignedDecimalAdmissionFixed[uint32](proc, from, result, length, selectList, entry, column)
	case types.T_uint64:
		return unsignedDecimalAdmissionFixed[uint64](proc, from, result, length, selectList, entry, column)
	case types.T_bool:
		return unsignedDecimalAdmissionFixed[bool](proc, from, result, length, selectList, entry, column)
	case types.T_bit:
		return unsignedDecimalAdmissionFixed[uint64](proc, from, result, length, selectList, entry, column)
	// TIME carries an explicit sign in this engine (Time < 0), and YEAR values
	// are 0 or 1901..2155 by construction (ValidateMoYear): both belong to the
	// fixed-size source domain rather than the temporal identity group, and
	// YEAR keeps its physical MoYear representation so the framework result
	// wrapper matches.
	case types.T_time:
		return unsignedDecimalAdmissionFixed[types.Time](proc, from, result, length, selectList, entry, column)
	case types.T_year:
		return unsignedDecimalAdmissionFixed[types.MoYear](proc, from, result, length, selectList, entry, column)
	case types.T_json:
		return unsignedDecimalAdmissionJSON(proc, from, result, length, selectList, entry, column)
	case types.T_varchar, types.T_char, types.T_text, types.T_blob,
		types.T_varbinary, types.T_binary, types.T_datalink:
		return unsignedDecimalAdmissionVarlena(proc, from, *targetType, result, length, selectList, entry, column)
	default:
		// Date, datetime, timestamp and enum ordinals are non-negative by
		// construction in this engine, and no remaining source kind can
		// classify negative, so their admission is a plain identity copy of
		// the fixed-size kind still reachable here.
		switch from.GetType().Oid {
		case types.T_date:
			return unsignedDecimalAdmissionFixed[types.Date](proc, from, result, length, selectList, entry, column)
		case types.T_datetime:
			return unsignedDecimalAdmissionFixed[types.Datetime](proc, from, result, length, selectList, entry, column)
		case types.T_timestamp:
			return unsignedDecimalAdmissionFixed[types.Timestamp](proc, from, result, length, selectList, entry, column)
		case types.T_enum:
			return unsignedDecimalAdmissionFixed[types.Enum](proc, from, result, length, selectList, entry, column)
		default:
			return moerr.NewInvalidInputNoCtxf("unsigned decimal admission source type %s is not supported", from.GetType().Oid)
		}
	}
}

// unsignedDecimalAdmissionColumnLabel resolves the destination column name
// carried by the constant third parameter. Generic replay paths that cannot
// name their destination compose an empty constant; the legacy type-label
// fallback keeps those diagnostics non-empty.
func unsignedDecimalAdmissionColumnLabel(nameCarrier *vector.Vector) string {
	if nameCarrier != nil {
		if name := string(nameCarrier.GetBytesAt(0)); name != "" {
			return name
		}
	}
	return "decimal"
}

// unsignedDecimalNegative reports the pre-rounding negative-nonzero fact for
// fixed-size sources. Engine decimals are two's-complement coefficients, so a
// set sign already implies nonzero; float signed zeros compare equal to zero
// and are admitted, matching MySQL's sign check on a zero decimal.
func unsignedDecimalNegative[T types.FixedSizeTExceptStrType](v T) bool {
	switch value := any(v).(type) {
	case types.Decimal64:
		return value.Sign()
	case types.Decimal128:
		return value.Sign()
	case types.Decimal256:
		return value.Sign()
	case float32:
		return value < 0
	case float64:
		return value < 0
	case int8:
		return value < 0
	case int16:
		return value < 0
	case int32:
		return value < 0
	case int64:
		return value < 0
	case types.Time:
		return value < 0
	default:
		return false
	}
}

// unsignedDecimalAdmissionFixed evaluates admission over fixed-size sources.
// Violations: strict entries error with the 1264 column diagnostic; lenient
// entries substitute the additive identity of the source type and append one
// matched warning. NULLs and skipped rows pass as NULLs without diagnostics.
func unsignedDecimalAdmissionFixed[T types.FixedSizeTExceptStrType](
	proc *process.Process,
	from *vector.Vector,
	result vector.FunctionResultWrapper,
	length int,
	selectList *FunctionSelectList,
	entry unsignedDecimalAdmissionEntry,
	column string,
) error {
	rs := vector.MustFunctionResult[T](result)
	src := vector.GenerateFunctionFixedTypeParameter[T](from)
	var zero T
	for i := uint64(0); i < uint64(length); i++ {
		if functionRowSkipped(selectList, i) {
			if err := rs.Append(zero, true); err != nil {
				return err
			}
			continue
		}
		v, null := src.GetValue(i)
		if null {
			if err := rs.Append(zero, true); err != nil {
				return err
			}
			continue
		}
		if !unsignedDecimalNegative(v) {
			if err := rs.Append(v, false); err != nil {
				return err
			}
			continue
		}
		if err := unsignedDecimalAdmissionRejectOrClip(proc, entry, column, i); err != nil {
			return err
		}
		appendUnsignedDecimalOutOfRangeWarning(proc, column, i)
		if err := rs.Append(zero, false); err != nil {
			return err
		}
	}
	return nil
}

// unsignedDecimalAdmissionRejectOrClip returns the strict 1264 assignment
// diagnostic (MySQL ER_WARN_DATA_OUT_OF_RANGE with the destination column and
// 1-based statement row) for the strict policies and nil for the lenient
// ones, whose matched warning the caller appends.
func unsignedDecimalAdmissionRejectOrClip(proc *process.Process, entry unsignedDecimalAdmissionEntry, column string, row uint64) error {
	strict := entry == unsignedDecimalAdmissionStrict ||
		(entry == unsignedDecimalAdmissionMode && isStrictSqlMode(proc))
	if !strict {
		return nil
	}
	ctx := context.Background()
	if proc != nil && proc.Ctx != nil {
		ctx = proc.Ctx
	}
	return moerr.NewOutOfRangeValueForColumn(ctx, column, row+1)
}

// appendUnsignedDecimalOutOfRangeWarning emits the lenient 1264 warning that
// pairs with a clipped value. The strict helper above is the error twin and
// must carry the same column/row context.
func appendUnsignedDecimalOutOfRangeWarning(proc *process.Process, column string, row uint64) {
	if proc == nil {
		return
	}
	if appender, ok := proc.GetWarningSink().(warningDiagnosticAppender); ok {
		appender.AppendWarningDiagnostic(moerr.ER_WARN_DATA_OUT_OF_RANGE,
			fmt.Sprintf("Out of range value for column '%s' at row %d", column, row+1))
	}
}

// unsignedDecimalJSONNegative classifies decoded JSON scalars against the
// unsigned domain BY VALUE, mirroring the jsonToScalar conversion the outer
// jsonSourceTo* cast family applies to the same row. Only a value that
// converts successfully and lands below zero is an admission violation: JSON
// signed zeros ('-0.00') convert to -0.0, which compares equal to zero and
// therefore admits cleanly. A conversion failure is NOT an admission
// decision — returning false here passes the row through untouched so the
// outer cast reports its own conversion error, instead of admission clipping
// an unconvertible string to zero and suppressing that error. Literals,
// dates, arrays, objects and opaques likewise pass through for the cast to
// classify.
func unsignedDecimalJSONNegative(bj bytejson.ByteJson) bool {
	f, _, ok := jsonToScalar(bj)
	if !ok {
		return false
	}
	return f < 0
}

// unsignedDecimalAdmissionJSON evaluates admission over encoded JSON sources.
// The encoding has no text grammar the text-varlena classifier could follow,
// so admission decodes the scalar first. Violations clip to an encoded JSON
// numeric zero so the outer cast converts exactly 0; admitted rows pass
// through with their original encoding untouched.
func unsignedDecimalAdmissionJSON(
	proc *process.Process,
	from *vector.Vector,
	result vector.FunctionResultWrapper,
	length int,
	selectList *FunctionSelectList,
	entry unsignedDecimalAdmissionEntry,
	column string,
) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	src := vector.GenerateFunctionStrParameter(from)
	var zeroJSON []byte
	for i := uint64(0); i < uint64(length); i++ {
		if functionRowSkipped(selectList, i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		v, null := src.GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		if !unsignedDecimalJSONNegative(types.DecodeJson(v)) {
			if err := rs.AppendBytes(v, false); err != nil {
				return err
			}
			continue
		}
		if err := unsignedDecimalAdmissionRejectOrClip(proc, entry, column, i); err != nil {
			return err
		}
		appendUnsignedDecimalOutOfRangeWarning(proc, column, i)
		if zeroJSON == nil {
			encoded, err := types.EncodeJson(bytejson.ByteJson{Type: bytejson.TpCodeInt64, Data: make([]byte, 8)})
			if err != nil {
				return err
			}
			zeroJSON = encoded
		}
		if err := rs.AppendBytes(zeroJSON, false); err != nil {
			return err
		}
	}
	return nil
}

// unsignedDecimalBinaryOutcome decodes one IsBin source row and renders it as
// canonical fixed-point text for the outer cast's text grammar.
type unsignedDecimalBinaryOutcome struct {
	text     string
	negative bool
}

// renderUnsignedDecimalBinarySource decodes a MySQL binary decimal payload
// through the exact entry the outer assignment cast dispatches to when its
// source vector is IsBin (strToDecimal64/128/256), then renders the canonical
// fixed-point text. Re-parsing that text through the outer cast yields the
// identical value, so the plan-composed identity wrapper preserves the binary
// conversion contract without depending on the IsBin flag surviving the
// function-executor result reset (which clears it). Decode errors propagate
// unchanged, exactly as the cast propagates them.
func renderUnsignedDecimalBinarySource(v []byte, targetType types.Type) (outcome unsignedDecimalBinaryOutcome, err error) {
	s := convertByteSliceToString(v)
	switch targetType.Oid {
	case types.T_decimal64:
		d, e := types.ParseDecimal64FromByte(s, targetType.Width, targetType.Scale)
		if e != nil {
			return outcome, e
		}
		return unsignedDecimalBinaryOutcome{text: d.Format(targetType.Scale), negative: d.Sign()}, nil
	case types.T_decimal128:
		d, e := types.ParseDecimal128FromByte(s, targetType.Width, targetType.Scale)
		if e != nil {
			return outcome, e
		}
		return unsignedDecimalBinaryOutcome{text: d.Format(targetType.Scale), negative: d.Sign()}, nil
	default:
		d, e := types.ParseDecimal256FromByte(s, targetType.Width, targetType.Scale)
		if e != nil {
			return outcome, e
		}
		return unsignedDecimalBinaryOutcome{text: d.Format(targetType.Scale), negative: d.Sign()}, nil
	}
}

// unsignedDecimalAdmissionBinary evaluates admission over IsBin varlena
// sources: byte payloads decoded as MySQL binary decimals (the outer cast's
// ParseDecimal*FromByte branch). The byte-level magnitude cannot be reused
// for classification because the flag does not reach the outer cast after
// the executor reset; classifying the decoded decimal and re-rendering it as
// text keeps admission and conversion on one numeric fact.
func unsignedDecimalAdmissionBinary(
	proc *process.Process,
	from *vector.Vector,
	targetType types.Type,
	result vector.FunctionResultWrapper,
	length int,
	selectList *FunctionSelectList,
	entry unsignedDecimalAdmissionEntry,
	column string,
) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	src := vector.GenerateFunctionStrParameter(from)
	for i := uint64(0); i < uint64(length); i++ {
		if functionRowSkipped(selectList, i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		v, null := src.GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		outcome, err := renderUnsignedDecimalBinarySource(v, targetType)
		if err != nil {
			return err
		}
		if !outcome.negative {
			if err := rs.AppendBytes([]byte(outcome.text), false); err != nil {
				return err
			}
			continue
		}
		if err := unsignedDecimalAdmissionRejectOrClip(proc, entry, column, i); err != nil {
			return err
		}
		appendUnsignedDecimalOutOfRangeWarning(proc, column, i)
		if err := rs.AppendBytes([]byte("0"), false); err != nil {
			return err
		}
	}
	return nil
}

// unsignedDecimalStringOutcome is the classification of a text-varlena source
// against the assignment cast's complete pre-quantization conversion outcome.
type unsignedDecimalStringOutcome uint8

const (
	// unsignedDecimalStringPass: the value converts non-negative.
	unsignedDecimalStringPass unsignedDecimalStringOutcome = iota
	// unsignedDecimalStringViolate: the value the cast stores is negative
	// nonzero before target-scale rounding.
	unsignedDecimalStringViolate
	// unsignedDecimalStringViolateRetained: under the IGNORE policy the cast
	// retains a negative nonzero numeric prefix after a conversion failure at
	// either stage; admission owns the truncation diagnostic first and the
	// 1264 clip second.
	unsignedDecimalStringViolateRetained
	// unsignedDecimalStringAbstain: a conversion failure whose diagnostics
	// stay with the cast (lexical errors, no-prefix invalid input, special
	// syntax, retained-prefix overflow). Admission must not run.
	unsignedDecimalStringAbstain
)

// classifyUnsignedDecimalStringSource derives the value the assignment cast
// will store from a text varlena source, before target-scale quantization,
// following the strToDecimal64/128/256 sub-paths exactly:
//   - token accepted and complete consumption validated: the effective sign is
//     the cast's own combine rule over the radix expansion (stored negative
//     iff the scanned sign differs from the token sign), nonzero iff the
//     scanned mantissa contains a nonzero digit;
//   - failure at tokenization or after tokenization: under the IGNORE policy
//     the cast retains the numeric prefix when it re-parses at the target;
//     that retained prefix classifies with the pure decimal grammar (sign
//     plus nonzero digit). A retained prefix whose own re-parse fails keeps
//     the cast's original error, so admission abstains.
//
// targetType supplies the destination width/scale for the retained-prefix
// re-parse decision.
func classifyUnsignedDecimalStringSource(s string, ignorePolicy bool, targetType types.Type) unsignedDecimalStringOutcome {
	token, tokenErr := parseCastNumericToken(s)
	if tokenErr == nil {
		expanded, expandErr := prefixedDigitsToDecimalString(token.digits, token.base)
		if expandErr == nil {
			var facts decimalPrefixFacts
			prefix, scannedNegative, ok := scanDecimalPrefix(expanded, &facts)
			if ok && prefix == expanded {
				if scannedNegative != token.negative && facts.first >= 0 {
					return unsignedDecimalStringViolate
				}
				return unsignedDecimalStringPass
			}
		}
	}
	// Conversion failure at either stage: the cast keeps its own diagnostics
	// for special syntax under every policy, and for every failure under the
	// non-IGNORE policies.
	if !ignorePolicy || isAssignmentSpecialNumericSyntax(s) {
		return unsignedDecimalStringAbstain
	}
	prefix, ok := assignmentDecimalPrefix(s)
	if !ok {
		// No numeric prefix: the cast stores the zero default with its
		// invalid-input warning; that value is non-negative by construction.
		return unsignedDecimalStringAbstain
	}
	if !unsignedDecimalPrefixReparses(prefix, targetType) {
		// A retained prefix whose conversion overflows keeps the cast's
		// original error; nothing is stored and admission abstains.
		return unsignedDecimalStringAbstain
	}
	var prefixFacts decimalPrefixFacts
	_, prefixNegative, _ := scanDecimalPrefix(prefix, &prefixFacts)
	if prefixNegative && prefixFacts.first >= 0 {
		return unsignedDecimalStringViolateRetained
	}
	return unsignedDecimalStringPass
}

// unsignedDecimalPrefixReparses mirrors the IGNORE retention re-parse of the
// strToDecimal* assignment casts: converting the retained prefix at the
// destination width/scale must succeed for retention to happen at all.
func unsignedDecimalPrefixReparses(prefix string, targetType types.Type) bool {
	var err error
	switch targetType.Oid {
	case types.T_decimal64:
		_, err = parseDecimal64CastString(prefix, targetType.Width, targetType.Scale)
	case types.T_decimal128:
		_, err = parseDecimal128CastString(prefix, targetType.Width, targetType.Scale)
	default:
		_, err = parseDecimal256CastString(prefix, targetType.Width, targetType.Scale)
	}
	return err == nil
}

// classifyMySQLDecimalPrefixSource classifies the charset-255 prefix branch of
// the assignment casts: parseMySQLDecimal*Prefix scans the leading numeric
// prefix, so the sign of that prefix decides the domain check.
func classifyMySQLDecimalPrefixSource(s string) unsignedDecimalStringOutcome {
	var facts decimalPrefixFacts
	_, negative, _ := scanDecimalPrefix(s, &facts)
	if negative && facts.first >= 0 {
		return unsignedDecimalStringViolate
	}
	return unsignedDecimalStringPass
}

func unsignedDecimalAdmissionVarlena(
	proc *process.Process,
	from *vector.Vector,
	targetType types.Type,
	result vector.FunctionResultWrapper,
	length int,
	selectList *FunctionSelectList,
	entry unsignedDecimalAdmissionEntry,
	column string,
) error {
	// Mirror the cast branch predicate: the private numeric marker charset
	// selects the prefix grammar (matching strToDecimal64 & friends) even for
	// a const IsBin source, and every other IsBin source converts through the
	// binary decimal decoder.
	isBin := from.GetIsBin()
	mysqlPrefix := targetType.Charset == 255 && (!isBin || from.IsConst())
	if isBin && !mysqlPrefix {
		return unsignedDecimalAdmissionBinary(proc, from, targetType, result, length, selectList, entry, column)
	}
	rs := vector.MustFunctionResult[types.Varlena](result)
	src := vector.GenerateFunctionStrParameter(from)
	for i := uint64(0); i < uint64(length); i++ {
		if functionRowSkipped(selectList, i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		v, null := src.GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		s := convertByteSliceToString(v)
		var outcome unsignedDecimalStringOutcome
		if mysqlPrefix {
			outcome = classifyMySQLDecimalPrefixSource(s)
		} else {
			outcome = classifyUnsignedDecimalStringSource(s, entry == unsignedDecimalAdmissionIgnore, targetType)
		}
		switch outcome {
		case unsignedDecimalStringViolate, unsignedDecimalStringViolateRetained:
			if err := unsignedDecimalAdmissionRejectOrClip(proc, entry, column, i); err != nil {
				return err
			}
			if outcome == unsignedDecimalStringViolateRetained {
				appendTruncatedAssignmentConversionWarning(proc, "DECIMAL", s)
			}
			appendUnsignedDecimalOutOfRangeWarning(proc, column, i)
			if err := rs.AppendBytes([]byte("0"), false); err != nil {
				return err
			}
		default:
			if err := rs.AppendBytes(v, false); err != nil {
				return err
			}
		}
	}
	return nil
}
