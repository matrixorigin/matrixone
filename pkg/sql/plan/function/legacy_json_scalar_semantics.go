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

package function

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type legacyJSONScalarSemanticsContextKey struct{}

// WithLegacyJSONScalarSemantics reconstructs the pre-#31121 mixed
// JSON/string, JSON/numeric and JSON/BOOL comparison and COALESCE JSON
// binding and evaluation semantics only while verifying complete persisted
// expression trees for DUMP/LOAD. It must not be used by ordinary SQL or new
// DDL, nor escape the verifier's private process.
func WithLegacyJSONScalarSemantics(ctx context.Context) context.Context {
	return context.WithValue(ctx, legacyJSONScalarSemanticsContextKey{}, true)
}

func LegacyJSONScalarSemantics(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	legacy, _ := ctx.Value(legacyJSONScalarSemanticsContextKey{}).(bool)
	return legacy
}

// legacyJSONScalarCheck reproduces the pre-#31121 overload resolution for the
// function families whose binding this change set extends:
//
//   - mixed JSON/string equality (<=>, =, !=) selected the fixed
//     varchar-cast comparison rule, casting the JSON operand to text instead
//     of comparing JSON string values by content;
//   - mixed JSON/string ordering (<, <=, >, >=) failed with wrong-parameters
//     instead of resolving to the JSON ordering overload;
//   - mixed JSON/BOOL ordering resolved through the generic comparison cast
//     rule, which admitted (BOOL, BOOL) with an implicit CAST of the JSON
//     operand into SQL boolean semantics, while the mixed JSON/BOOL equality
//     family kept its direct admission with the older kernel values (a JSON
//     string was a category of its own and never parsed as boolean text, and
//     a JSON literal null folded into the SQL NULL contract);
//   - mixed JSON/binary-string equality (BINARY, VARBINARY, BLOB) bound the
//     forward leg through the generic comparison cast row - the ONLY leg of
//     that family that resolved at all, since every reverse and ordering leg
//     already failed with wrong-parameters in both eras;
//   - COALESCE casts its JSON branches to the common character target
//     before CoalesceStr, which unquotes string scalars and loses the JSON
//     document spelling that the emitted text carries today.
//
// The current check result is returned untouched for every other function or
// argument shape, so ordinary unrelated overloads never observe this era.
func legacyJSONScalarCheck(id int32, overloads []overload, inputs []types.Type, current checkResult) (checkResult, bool) {
	if len(inputs) == 0 {
		return current, false
	}
	switch id {
	case EQUAL, NOT_EQUAL, NULL_SAFE_EQUAL:
		if len(inputs) != 2 {
			return current, false
		}
		if IsJSONStringComparison(inputs[0].Oid, inputs[1].Oid) {
			return legacyJSONStringEqualityCheck(inputs), true
		}
		if jsonBinaryStringComparisonNotSupported(inputs[0].Oid, inputs[1].Oid) {
			// The predecessor bound the forward (JSON, binary-string) leg
			// through the comparison cast row and rejected every reverse leg;
			// the shared tail reproduces both sides cell-for-cell.
			return legacyJSONStringEqualityCheck(inputs), true
		}
		return current, false
	case GREAT_THAN, GREAT_EQUAL, LESS_THAN, LESS_EQUAL:
		if len(inputs) != 2 {
			return current, false
		}
		if IsJSONStringComparison(inputs[0].Oid, inputs[1].Oid) {
			return newCheckResultWithFailure(failedFunctionParametersWrong), true
		}
		if IsJSONBooleanComparison(inputs[0].Oid, inputs[1].Oid) {
			return legacyJSONBooleanOrderingCastCheck(inputs[0], inputs[1]), true
		}
		return current, false
	case BETWEEN:
		if len(inputs) != 3 ||
			(!isMixedJSONBooleanBetweenInputs(inputs) && !isMixedJSONNumericBetweenInputs(inputs)) {
			return current, false
		}
		// The predecessor lowered neither form: plain BETWEEN kept the same
		// cast-based admission as the ordering tail, inserting an implicit
		// CAST of the JSON operand, while NOT BETWEEN was lowered to < OR >
		// at the binder. That covers the mixed JSON/BOOL triples and the
		// mixed JSON/numeric triples (whose cast folds parsed a JSON string
		// as numeric text and collapsed a JSON literal null into SQL NULL).
		// A persisted CHECK from either source must therefore rebind through
		// the historical tail, with the cast legs the cast-era bind authored.
		return betweenLegacyCheckTail(overloads, inputs), true
	case COALESCE:
		legacy, ok := legacyCoalesceJSONCastCheck(overloads, inputs, current)
		return legacy, ok
	}
	return current, false
}

// legacyJSONBooleanOrderingCastCheck reproduces the pre-#31121 ordering tail
// for the mixed JSON/BOOL pair: the ordering checks fell through to the
// generic comparison cast rule, which admitted (BOOL, BOOL) and inserted an
// implicit CAST of the JSON operand. The shared SQL comparison executors
// then folded exactly the payload a predecessor server folded: a JSON
// literal null collapsed to SQL NULL, "true"/"false" text parsed as boolean
// text, and every other document category failed that cast.
func legacyJSONBooleanOrderingCastCheck(left, right types.Type) checkResult {
	hasCast, t1, t2 := comparisonTypeCastRule(left, right)
	if !hasCast || !otherCompareOperatorSupports(t1, t2) {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	return newCheckResultWithCast(0, []types.Type{t1, t2})
}

// legacyJSONStringEqualityCheck is the pre-#31121 tail of the shared equality
// type check: derive the cast rule, then admit the fixed comparison overload
// with the cast it requires. A JSON date/decimal peer keeps the same overall
// shape; only the JSON string category was carved out by the current kernel.
func legacyJSONStringEqualityCheck(inputs []types.Type) checkResult {
	hasCast, left, right := comparisonTypeCastRule(inputs[0], inputs[1])
	if !equalAndNotEqualOperatorSupports(left, right) {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	targets := []types.Type{left, right}
	if left.Oid.IsDecimal() && right.Oid.IsDecimal() {
		aligned, ok := alignDecimalComparisonTypes(targets, inputs)
		if !ok {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
		hasCast = hasCast || aligned
	}
	if hasCast {
		return newCheckResultWithCast(0, targets)
	}
	return newCheckResultWithSuccess(0)
}

// legacyCoalesceJSONCastCheck rewrites a resolved mixed JSON/character
// COALESCE result back to the pre-#31121 cast layout, in which the JSON
// branches are cast to the same character target as the remaining branches.
// The resulting bound tree matches a predecessor catalog fold exactly:
// CoalesceStr(CAST(json AS character), character), whose folded value is the
// unquoted text rendering of the JSON branch.
func legacyCoalesceJSONCastCheck(overloads []overload, inputs []types.Type, current checkResult) (checkResult, bool) {
	hasJSON := false
	for _, input := range inputs {
		if input.Oid == types.T_json {
			hasJSON = true
			break
		}
	}
	if !hasJSON || len(current.finalType) == 0 {
		return current, false
	}
	// The common character target equals the resolved overload's argument
	// type; any non-JSON branch already carries it in the current result.
	var target types.Type
	for j, input := range inputs {
		if input.Oid != types.T_json && j < len(current.finalType) {
			target = current.finalType[j]
			break
		}
	}
	if target.Oid == types.T_any || target.Oid == types.T_json {
		// Every branch is JSON, or no character branch retained a target:
		// both eras resolve this shape through the same non-JSON rule.
		return current, false
	}
	castTypes := make([]types.Type, len(inputs))
	for j, input := range inputs {
		castTypes[j] = target
		if input.Oid != types.T_json && j < len(current.finalType) && current.finalType[j].Oid != types.T_any {
			castTypes[j] = current.finalType[j]
		}
	}
	return newCheckResultWithCast(current.idx, castTypes), true
}
