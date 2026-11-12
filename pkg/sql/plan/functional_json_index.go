// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"regexp"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

var functionalJSONPath = regexp.MustCompile(`^\$(?:\.[A-Za-z_][A-Za-z0-9_]*|\[[0-9]+\])*$`)

// This only extends DDL eligibility. The existing JSON functions, text CAST,
// hidden generated column and BTREE owners retain all execution semantics.
// In particular, no JSON value is admitted as a physical index key.
func functionalJSONTextCastEligible(expr *Expr, cols []*ColDef) bool {
	if expr == nil || !functionalTextType(expr.Typ) || expr.Typ.Width <= 0 ||
		expr.Typ.ValidateCollation() != nil || expr.Typ.CollationVersion != 0 ||
		expr.Typ.Charset > uint32(types.CharsetUTF8) || !functionalJSONFunction(expr, "cast", function.CAST, 1) {
		return false
	}
	args := expr.GetF().Args
	if len(args) != 2 || args[1].GetT() == nil || !sameFunctionalValueType(expr.Typ, args[1].Typ) {
		return false
	}
	unquote := args[0]
	if !functionalJSONFunction(unquote, "json_unquote", function.JSON_UNQUOTE, 0) ||
		len(unquote.GetF().Args) != 1 {
		return false
	}
	return functionalJSONExtractChainEligible(unquote.GetF().Args[0], cols)
}

// functionalJSONNumericCastEligible admits the distinct numeric case,
// CAST(JSON_EXTRACT(<ordinary JSON column>, <constant single-location path>)
// AS <integer target>), in either bound form the binder produces: the
// explicit-cast overload-1 form (the SIGNED-family spellings) and the
// flag-set overload-0 form (the plain integer spellings, the issue's exact
// AS INT among them). A flag-clear overload-0 node is the implicit adapter
// excluded here; the two forms are disjoint by flag value and target domain.
// The hidden key type is the integer cast target — JSON itself is never a
// physical key — and the position/count, result-gate and supportability rules
// downstream are unchanged.
func functionalJSONNumericCastEligible(expr *Expr, cols []*ColDef) bool {
	if expr == nil || !types.T(expr.Typ.Id).IsInteger() ||
		expr.Typ.ValidateCollation() != nil || expr.Typ.CollationVersion != 0 ||
		expr.Typ.Charset > uint32(types.CharsetUTF8) {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	_, overload := function.DecodeOverloadID(fn.Func.Obj)
	if (overload == 0 && !fn.SyntaxExplicitCast) || (overload != 0 && overload != 1) ||
		!functionalJSONFunction(expr, "cast", function.CAST, overload) {
		return false
	}
	args := fn.Args
	if len(args) != 2 || args[1].GetT() == nil || !sameFunctionalValueType(expr.Typ, args[1].Typ) {
		return false
	}
	return functionalJSONExtractChainEligible(args[0], cols)
}

// functionalJSONExtractChainEligible validates the shared json_extract tail of
// both JSON index cases: json_extract on its (json, varchar) overload over an
// ordinary JSON column of the table plus a constant single-location path.
func functionalJSONExtractChainEligible(extract *Expr, cols []*ColDef) bool {
	if !functionalJSONFunction(extract, "json_extract", function.JSON_EXTRACT, 0) ||
		extract.Typ.Id != int32(types.T_json) || len(extract.GetF().Args) != 2 {
		return false
	}
	source, path := extract.GetF().Args[0], extract.GetF().Args[1]
	if source == nil || path == nil || path.GetLit() == nil || path.GetLit().Isnull ||
		!functionalJSONPath.MatchString(path.GetLit().GetSval()) {
		return false
	}
	if _, err := bytejson.ParseJsonPath(path.GetLit().GetSval()); err != nil {
		return false
	}
	// Keep this extension independent of generated-column/FK conversion and
	// session-sensitive JSON producers. Only an ordinary JSON column is added.
	ref := source.GetCol()
	if ref == nil || ref.RelPos != 0 || ref.ColPos < 0 || int(ref.ColPos) >= len(cols) {
		return false
	}
	col := cols[ref.ColPos]
	return col != nil && !col.Hidden && !col.Typ.AutoIncr && col.GeneratedCol == nil &&
		col.Typ.Id == int32(types.T_json) && sameFunctionalValueType(source.Typ, col.Typ)
}

// functionalJSONFunction is the shared registry verifier for bound F-nodes:
// the node must carry the expected name and encoded overload ID, re-resolve
// in the function registry to the same encoded ID with the same implicit
// argument-cast expectations, and equal the resolved return type (except for
// CAST, whose overload identity is the contract). The scalar admission path
// in functional_index.go relies on it too; the JSON in the name is historical.
func functionalJSONFunction(expr *Expr, name string, id, overload int32) bool {
	return verifyFunctionalIndexOverload(expr, name, id, overload, false)
}

// verifyFunctionalIndexOverload is the shared body behind the registry
// verifiers. permitWidthRefinement admits one measured deviation from exact
// return-type identity: substring and left derive the bound result width
// from constant argument values, while the registry re-derives the same
// overload's return type from the argument types alone — a widthless
// envelope the constant-refined width narrows. The refined width is a pure
// function of the persisted text, re-derives identically at every replay
// (the round-trip assertions pin it exactly), and never widens past the
// type-level envelope, so the scalar path permits same-OID, same-scale,
// narrower-or-equal widths; every other caller keeps exact identity.
func verifyFunctionalIndexOverload(expr *Expr, name string, id, overload int32, permitWidthRefinement bool) bool {
	if expr == nil || expr.Typ.ValidateCollation() != nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != name ||
		fn.Func.Obj != int64(id)<<32|int64(overload) {
		return false
	}
	args := make([]types.Type, len(fn.Args))
	for i, arg := range fn.Args {
		if arg == nil || arg.Typ.ValidateCollation() != nil || strings.HasPrefix(types.T(arg.Typ.Id).String(), "unexpected type:") {
			return false
		}
		var err error
		args[i], err = types.TypeFromPlan(arg.Typ)
		if err != nil {
			return false
		}
	}
	resolved, err := function.GetFunctionByNameWithOverload(context.Background(), name, args, overload)
	if err != nil || resolved.GetEncodedOverloadID() != fn.Func.Obj {
		return false
	}
	if targets, needed := resolved.ShouldDoImplicitTypeCast(); needed {
		for i := range args {
			if len(targets) != len(args) || !args[i].Eq(targets[i]) {
				return false
			}
		}
	}
	if id == function.CAST {
		return true
	}
	if sameFunctionalValueType(resolved.GetReturnType().PlanType(), expr.Typ) {
		return true
	}
	if permitWidthRefinement {
		registry := resolved.GetReturnType().PlanType()
		return registry.Id == expr.Typ.Id && registry.Scale == expr.Typ.Scale &&
			(registry.Width == 0 || expr.Typ.Width <= registry.Width)
	}
	return false
}

// Equality widens a CHAR operand to VARCHAR. Strip only that value-preserving
// wrapper for matching; keep the original base-row predicate as a residual.
// functionalJSONComparisonOperand unwraps the value-preserving casts the
// equality binder inserts around an operand, so the structurally compared
// candidate can still match its persisted expression; the original base-row
// predicate always remains as a residual. Three wrappers exist on this seam:
//   - comparing a CHAR-typed operand widens it to VARCHAR (a comparison
//     overload-2 CAST);
//   - comparing a narrower integer key widens it to the comparison common
//     integer type through a flag-clear overload-0 CAST (probe-verified:
//     `cast(json_extract(doc,'$.a') as int) = 1` compares on BIGINT while
//     the hidden key is INT). Every source value injects into the wider
//     type exactly — same-signedness widening, or unsigned into a wider
//     signed type — so stripping preserves the equality.
//   - comparing an integer key whose literal peer carries a DECIMAL common
//     type promotes it through the same flag-clear overload-0 CAST
//     (probe-verified: `crc32(name)=123` and
//     `cast(json_extract(doc,'$.a') as unsigned)=4294967296` wrap in
//     CAST(... AS DECIMAL128(38,0)), `... as int)=1.5` in DECIMAL128(11,1)).
//     The promotion is stripped only when every source value injects
//     exactly: the decimal's integer-digit capacity (width minus scale)
//     must cover the source type's widest value. An insufficient-capacity
//     adapter — for example DECIMAL(18,0) for a uint64 key — is lossy,
//     stays unstripped, and never matches.
//     A user-written explicit cast keeps its syntax flag, stays unstripped,
//     and never matches any arm.
func functionalJSONComparisonOperand(expr *Expr, cols []*ColDef, tag int32) *Expr {
	if functionalJSONFunction(expr, "cast", function.CAST, 2) && len(expr.GetF().Args) == 2 {
		inner := expr.GetF().Args[0]
		from, to := inner.Typ, expr.Typ
		if expr.GetF().Args[1].GetT() == nil || !sameFunctionalValueType(to, expr.GetF().Args[1].Typ) ||
			from.Id != int32(types.T_char) || to.Id != int32(types.T_varchar) || to.Width < from.Width {
			return expr
		}
		from.Id, from.Width = to.Id, to.Width
		if !sameFunctionalValueType(from, to) {
			return expr
		}
		normalized := DeepCopyExpr(inner)
		if !normalizeFunctionalExpression(normalized, tag) || !functionalJSONTextCastEligible(normalized, cols) {
			return expr
		}
		return inner
	}
	// The narrower-integer widening and the integer-key DECIMAL promotion of
	// the bullets above.
	if fn := expr.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "cast" &&
		fn.Func.Obj == int64(function.CAST)<<32 && !fn.SyntaxExplicitCast && len(fn.Args) == 2 &&
		fn.Args[1].GetT() != nil && sameFunctionalValueType(expr.Typ, fn.Args[1].Typ) {
		inner := fn.Args[0]
		from, to := types.T(inner.Typ.Id), types.T(expr.Typ.Id)
		if from.IsInteger() && to.IsInteger() &&
			(from.IsUnsignedInt() == to.IsUnsignedInt() && from.TypeLen() <= to.TypeLen() ||
				from.IsUnsignedInt() && to.IsSignedInt() && from.TypeLen() < to.TypeLen()) {
			normalized := DeepCopyExpr(inner)
			if normalizeFunctionalExpression(normalized, tag) {
				return inner
			}
		}
		if from.IsInteger() && to.IsDecimal() && decimalPromotionCoversSource(expr.Typ, from) {
			normalized := DeepCopyExpr(inner)
			if normalizeFunctionalExpression(normalized, tag) {
				return inner
			}
		}
	}
	return expr
}

// decimalPromotionCoversSource reports whether the flag-clear comparison cast
// from the integer family type `from` into the decimal plan type `decimal` is
// value-exact: the decimal's integer-digit capacity (width minus scale) must
// be able to hold the widest value of the source type. Every
// integer-to-decimal cast with sufficient capacity injects exactly - scale
// only places the decimal point - so equality over the stripped comparison is
// preserved, and any row the base predicate admits still satisfies the added
// hidden equality rewrapped into the same promotion. Insufficient capacity is
// lossy and must stay unstripped.
func decimalPromotionCoversSource(decimal Type, from types.T) bool {
	capacity := decimal.Width - decimal.Scale
	return capacity >= int32(integerDigitRequirement(from))
}

// integerDigitRequirement returns the decimal integer-digit count that can
// hold every value of the integer family type. It is computed from the wire
// width and signedness of the family member rather than a per-member switch,
// staying correct for future family members.
func integerDigitRequirement(from types.T) int {
	bits := uint(from.TypeLen()) * 8
	if bits < 8 {
		// Unknown family member: fail closed with an unreachable capacity.
		return 1 << 30
	}
	var widest uint64
	if bits >= 64 {
		widest = ^uint64(0)
	} else {
		widest = (uint64(1) << bits) - 1
	}
	if from.IsSignedInt() {
		widest >>= 1
	}
	return len(strconv.FormatUint(widest, 10))
}
