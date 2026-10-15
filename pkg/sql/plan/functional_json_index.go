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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

var functionalJSONPath = regexp.MustCompile(`^\$(?:\.[A-Za-z_][A-Za-z0-9_]*|\[[0-9]+\])*$`)

func functionalKnownFunction(expr *Expr, id int32, overloads ...int32) bool {
	resolved, ok := functionalResolvedFunction(expr)
	if !ok {
		return false
	}
	// JSON producers bypass the ordinary expression walk, which validates
	// registry result metadata. Apply that contract at this admission boundary.
	if id != function.CAST && !sameFunctionalValueType(resolved.GetReturnType().PlanType(), expr.Typ) {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || int32(fn.Func.Obj>>32) != id {
		return false
	}
	for _, overload := range overloads {
		if int32(fn.Func.Obj) == overload {
			return true
		}
	}
	return false
}

// Only a bounded explicit text cast admits the JSON extraction producer. The
// existing ordinary functional policy still owns every other expression.
func functionalJSONTextCastEligible(expr *Expr, cols []*ColDef, visiting map[int32]bool) bool {
	if expr == nil || !functionalKnownFunction(expr, function.CAST, 0, 1) ||
		!functionalTextType(expr.Typ) || expr.Typ.Width <= 0 ||
		expr.Typ.CollationVersion != 0 || expr.Typ.Charset > uint32(types.CharsetUTF8) {
		return false
	}
	fn := expr.GetF()
	if len(fn.Args) != 2 || fn.Args[0] == nil || fn.Args[1] == nil || fn.Args[1].GetT() == nil ||
		!sameFunctionalValueType(expr.Typ, fn.Args[1].Typ) {
		return false
	}
	unquote := fn.Args[0]
	if !functionalKnownFunction(unquote, function.JSON_UNQUOTE, 0) ||
		!functionalTextType(unquote.Typ) || len(unquote.GetF().Args) != 1 {
		return false
	}
	extract := unquote.GetF().Args[0]
	if extract == nil || !functionalKnownFunction(extract, function.JSON_EXTRACT, 0) ||
		extract.Typ.Id != int32(types.T_json) || len(extract.GetF().Args) != 2 {
		return false
	}
	args := extract.GetF().Args
	if args[0] == nil || args[0].Typ.Id != int32(types.T_json) || args[1] == nil || args[1].GetLit() == nil ||
		args[1].GetLit().Isnull || !functionalJSONPath.MatchString(args[1].GetLit().GetSval()) {
		return false
	}
	return functionalJSONInputEligible(args[0], cols, visiting)
}

func functionalJSONInputEligible(expr *Expr, cols []*ColDef, visiting map[int32]bool) bool {
	if expr == nil || expr.Typ.Id != int32(types.T_json) {
		return false
	}
	if expr.GetLit() != nil {
		return true
	}
	if ref := expr.GetCol(); ref != nil {
		if ref.RelPos != 0 || ref.ColPos < 0 || int(ref.ColPos) >= len(cols) || visiting[ref.ColPos] {
			return false
		}
		col := cols[ref.ColPos]
		if col == nil || col.Hidden || col.Typ.AutoIncr || col.Typ.Id != int32(types.T_json) {
			return false
		}
		if col.GeneratedCol == nil {
			return true
		}
		visiting[ref.ColPos] = true
		ok := functionalJSONInputEligible(col.GeneratedCol.Expr, cols, visiting)
		delete(visiting, ref.ColPos)
		return ok
	}
	if !functionalKnownFunction(expr, function.CAST, 0, 1) || len(expr.GetF().Args) != 2 {
		return false
	}
	fn := expr.GetF()
	if fn.Args[0] == nil || fn.Args[1] == nil || fn.Args[1].GetT() == nil ||
		!sameFunctionalValueType(expr.Typ, fn.Args[1].Typ) {
		return false
	}
	if fn.Args[0].Typ.Id == int32(types.T_json) {
		return functionalJSONInputEligible(fn.Args[0], cols, visiting)
	}
	// A direct integer image supports dependent FK CASCADE/SET NULL keys without
	// relaxing the canonical prohibition on ordinary generated FK columns.
	return functionalJSONIntegerInputEligible(fn.Args[0], cols, visiting)
}

func functionalJSONIntegerInputEligible(expr *Expr, cols []*ColDef, visiting map[int32]bool) bool {
	if expr == nil || !types.T(expr.Typ.Id).IsInteger() {
		return false
	}
	if expr.GetLit() != nil {
		return true
	}
	ref := expr.GetCol()
	if ref == nil || ref.RelPos != 0 || ref.ColPos < 0 || int(ref.ColPos) >= len(cols) || visiting[ref.ColPos] {
		return false
	}
	col := cols[ref.ColPos]
	if col == nil || col.Hidden || col.Typ.AutoIncr || !types.T(col.Typ.Id).IsInteger() {
		return false
	}
	if col.GeneratedCol == nil {
		return true
	}
	visiting[ref.ColPos] = true
	ok := functionalJSONIntegerInputEligible(col.GeneratedCol.Expr, cols, visiting)
	delete(visiting, ref.ColPos)
	return ok
}

// Following generated dependencies is necessary for a key such as lower(g)
// when g's independently bound expression produces a bounded JSON text value.
func functionalExpressionUsesJSON(expr *Expr, cols []*ColDef, visiting map[int32]bool) bool {
	return function.ExpressionUsesJSONAccess(expr, cols, visiting)
}

func functionalIndexUsesJSON(table *TableDef, index *pb.IndexDef) bool {
	for _, col := range functionalIndexColumns(table, index) {
		if functionalExpressionUsesJSON(col.GeneratedCol.Expr, table.Cols, make(map[int32]bool)) {
			return true
		}
	}
	return false
}

// The existing secondary-index emitter owns the marker. There is no new AST
// flag or per-query state; COPY/LIKE replay derives it from the final schema.
func setFunctionalJSONIndexSemantics(ctx context.Context, table *TableDef, index *pb.IndexDef) error {
	if table == nil || index == nil || !functionalIndexUsesJSON(table, index) {
		return nil
	}
	params := make(map[string]string)
	var err error
	if index.IndexAlgoParams != "" {
		params, err = catalog.IndexParamsStringToMap(index.IndexAlgoParams)
		if err != nil {
			return err
		}
	}
	params[pb.FunctionalJSONSemanticsKey] = "1"
	index.IndexAlgoParams, err = catalog.IndexParamsMapToJsonString(params)
	return err
}

func validateFunctionalJSONIndexSemantics(ctx context.Context, table *TableDef, index *pb.IndexDef) error {
	marker, err := pb.JSONFunctionalIndexSemantics(index)
	if err != nil {
		return err
	}
	if marker != functionalIndexUsesJSON(table, index) {
		return moerr.NewInvalidInput(ctx, "inconsistent JSON functional index semantics metadata")
	}
	return nil
}

// The equality binder widens CHAR keys into its VARCHAR comparison domain.
// Match only this known value-preserving wrapper; the original predicate stays
// as a residual and the persisted JSON expression still matches exactly.
func functionalJSONComparisonOperand(expr *Expr, cols []*ColDef, tag int32) *Expr {
	if expr == nil || !functionalKnownFunction(expr, function.CAST, 2) {
		return expr
	}
	fn := expr.GetF()
	if len(fn.Args) != 2 || fn.Args[0] == nil || fn.Args[1] == nil || fn.Args[1].GetT() == nil || !sameFunctionalValueType(expr.Typ, fn.Args[1].Typ) {
		return expr
	}
	inner := fn.Args[0]
	from, to := inner.Typ, expr.Typ
	if from.Id != int32(types.T_char) || to.Id != int32(types.T_varchar) || to.Width < from.Width || from.Charset != to.Charset || from.CollationVersion != to.CollationVersion || from.PadSpace != to.PadSpace || from.Scale != to.Scale {
		return expr
	}
	normalized := DeepCopyExpr(inner)
	if !normalizeFunctionalExpression(normalized, tag) || !functionalJSONTextCastEligible(normalized, cols, make(map[int32]bool)) {
		return expr
	}
	return inner
}
