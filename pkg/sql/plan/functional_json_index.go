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
	extract := unquote.GetF().Args[0]
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

func functionalJSONFunction(expr *Expr, name string, id, overload int32) bool {
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
	return id == function.CAST || sameFunctionalValueType(resolved.GetReturnType().PlanType(), expr.Typ)
}

// Equality widens a CHAR operand to VARCHAR. Strip only that value-preserving
// wrapper for matching; keep the original base-row predicate as a residual.
func functionalJSONComparisonOperand(expr *Expr, cols []*ColDef, tag int32) *Expr {
	if !functionalJSONFunction(expr, "cast", function.CAST, 2) || len(expr.GetF().Args) != 2 {
		return expr
	}
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
