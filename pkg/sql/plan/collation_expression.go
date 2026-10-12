// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// Expression provenance belongs to plan.Type, not to the persisted runtime
// Type. Resolving an expression must never upgrade its source table's identity.
type expressionCollation struct {
	charset, version, rank uint32
	conflict               bool
}

func (b *baseBinder) bindCollateExpr(ast *tree.CollateExpr, depth int32) (*Expr, error) {
	ctx := b.GetContext()
	definition, known := collation.Lookup(ast.Collation)
	if !known {
		return nil, moerr.NewUnknownCollation(ctx, ast.Collation)
	}
	identity, admitted := collation.ResolveSQL(ast.Collation)
	version := uint32(collation.RevisionLegacy)
	if definition.Semantics == collation.UCA400 {
		identity, admitted, version = definition.Identity, true, uint32(collation.RevisionV1)
	}
	if !admitted {
		return nil, moerr.NewNotSupportedf(ctx, "collation '%s' is disabled", ast.Collation)
	}
	if err := collation.RequireLegacy(uint32(identity), version, 0); err != nil {
		return nil, err
	}
	expr, err := b.impl.BindExpr(ast.Expr, depth, false)
	if err != nil {
		return nil, err
	}
	source := makeTypeByPlan2Expr(expr)
	_, parameter := unwrapParenExpr(ast.Expr).(*tree.ParamExpr)
	if parameter {
		// COLLATE fixes a marker's text domain across EXECUTE type changes,
		// including binary protocol values and NULL. A binary literal has no
		// such parameter constraint and remains subject to charset validation.
		source.Oid, source.Charset = types.T_varchar, types.CharsetUTF8
	}
	if (source.Oid.IsMySQLString() && source.Charset == types.CharsetBinary) || (!parameter && isNullLiteralExpr(expr)) {
		if definition.Charset != collation.CharsetBinary {
			return nil, moerr.NewCollationCharsetMismatch(ctx, definition.Name, "binary")
		}
	} else if definition.Charset == collation.CharsetBinary {
		return nil, moerr.NewCollationCharsetMismatch(ctx, definition.Name, "utf8mb4")
	}
	target := expr.Typ
	if !types.T(target.Id).IsMySQLString() {
		textType := types.New(types.T_varchar, types.MaxVarcharLen, 0)
		target = makePlan2Type(&textType)
	}
	if parameter && (types.T(target.Id) == types.T_binary || types.T(target.Id) == types.T_varbinary) {
		target.Id = int32(types.T_varchar)
	}
	if identity == collation.BinaryIdentity {
		target.Id = int32(types.T_varbinary)
		target.PadSpace = false
	}
	selected := expressionCollation{charset: uint32(identity), version: version, rank: 0}
	selected.apply(&target)
	// A real cast preserves the raw source column for projection, index safety,
	// and runtime parameter rebinding. Relabeling a shared node loses all three.
	return appendCastBeforeExpr(ctx, expr, target)
}

func (c expressionCollation) apply(typ *plan.Type) {
	typ.Charset, typ.CollationVersion = c.charset, c.version
	typ.CollationCoercibility, typ.CollationCoercibilitySet = c.rank, true
	typ.CollationMergeConflict = c.conflict
}

func collationForExpression(expr *Expr) (expressionCollation, bool) {
	if expr == nil || !types.T(expr.Typ.Id).IsMySQLString() {
		return expressionCollation{}, false
	}
	c := expressionCollation{charset: expr.Typ.Charset, version: expr.Typ.CollationVersion,
		conflict: expr.Typ.CollationMergeConflict, rank: 5}
	if expr.Typ.CollationCoercibilitySet {
		c.rank = expr.Typ.CollationCoercibility
		return c, true
	}
	switch {
	case isNullLiteralExpr(expr):
		c.rank = 6
	case expr.GetCol() != nil || expr.GetCorr() != nil || expr.GetV() != nil:
		c.rank = 2
	case expr.GetSub() != nil && expr.GetSub().Typ == plan.SubqueryRef_SCALAR:
		// Old plans may omit provenance on a materialized scalar result.
		// Newly bound subqueries retain the projected source's exact rank.
		c.rank = 2
	case expr.GetLit() != nil || expr.GetP() != nil:
		c.rank = 4
	case expr.GetF() != nil && expr.GetF().Func != nil:
		fn := expr.GetF()
		name := strings.ToLower(fn.Func.ObjName)
		switch name {
		case "user", "current_user", "version", "database", "schema":
			c.rank = 3
		case "cast", "convert":
			c.rank = 2
		default:
			// Foreign/old plans may lack provenance. Read types only; never
			// evaluate a CASE branch while inferring its collation.
			if index := preparedStringDomainSourceIndex(name, len(fn.Args)); index >= 0 {
				if child, ok := collationForExpression(fn.Args[index]); ok {
					c.rank = child.rank
				}
			}
		}
	}
	return c, true
}

func expressionCollationName(c expressionCollation) string {
	if c.charset == uint32(types.CharsetLegacy) {
		return "legacy"
	}
	d, err := collation.EffectiveDefinition(c.charset, c.version)
	if err != nil {
		return "unknown"
	}
	return d.Name
}

func illegalExpressionCollations(ctx context.Context, operation string, left, right expressionCollation) error {
	ranks := [...]string{"EXPLICIT", "NONE", "IMPLICIT", "SYSCONST", "COERCIBLE", "NUMERIC", "IGNORABLE"}
	return moerr.NewIllegalMixOfCollations(ctx, expressionCollationName(left), ranks[min(left.rank, 6)],
		expressionCollationName(right), ranks[min(right.rank, 6)], operation)
}

// Resolve the strongest rank before checking equal-rank conflicts. Otherwise
// adding a stronger third operand makes a left-to-right resolver order-dependent.
func resolveExpressionCollation(ctx context.Context, operation string, candidates []expressionCollation) (expressionCollation, bool, error) {
	if len(candidates) == 0 {
		return expressionCollation{}, false, nil
	}
	winningRank := uint32(6)
	active := false
	for _, candidate := range candidates {
		winningRank = min(winningRank, candidate.rank)
		active = active || candidate.rank == 0 || candidate.version != 0 || candidate.conflict
	}
	var chosen expressionCollation
	chosenSet := false
	for _, candidate := range candidates {
		if candidate.rank != winningRank {
			continue
		}
		if !chosenSet {
			chosen, chosenSet = candidate, true
			continue
		}
		if chosen.charset == candidate.charset && chosen.version == candidate.version {
			chosen.conflict = chosen.conflict || candidate.conflict
			continue
		}
		if winningRank == 0 || chosen.version != candidate.version {
			return expressionCollation{}, false, illegalExpressionCollations(ctx, operation, chosen, candidate)
		}
		// Equal implicit ranks in one charset prefer the text _bin rule.
		// Binary bytes keep their own NO PAD domain, distinct from text _bin.
		if chosen.charset == uint32(types.CharsetBinary) || candidate.charset == uint32(types.CharsetBinary) {
			chosen.charset, chosen.version = uint32(types.CharsetBinary), 0
		} else if chosen.charset == uint32(types.CharsetUTF8MB4Bin) || candidate.charset == uint32(types.CharsetUTF8MB4Bin) {
			chosen.charset = uint32(types.CharsetUTF8MB4Bin)
		} else if types.IsUnicodeCollation(uint8(chosen.charset)) && types.IsUnicodeCollation(uint8(candidate.charset)) {
			// utf8mb4 is the repertoire superset for UCA400, not an alias.
			chosen.charset = uint32(types.CharsetUTF8MB4UnicodeCI)
		} else if active {
			return expressionCollation{}, false, illegalExpressionCollations(ctx, operation, chosen, candidate)
		}
	}
	if chosen.conflict {
		return expressionCollation{}, false, illegalExpressionCollations(ctx, operation, chosen, chosen)
	}
	return chosen, active, nil
}

func expressionCollationIndexes(name string, arity int) []int {
	if indexes := padSpaceValueArgumentIndexes(name, arity); len(indexes) > 0 {
		return indexes
	}
	switch name {
	case "name_const":
		if arity == 2 {
			return []int{1}
		}
	case "=", "<=>", "<", "<=", ">", ">=", "<>", "!=", "between", "in", "not_in",
		"concat", "concat_ws", "nullif", "union", "values":
		indexes := make([]int, arity)
		for i := range indexes {
			indexes[i] = i
		}
		return indexes
	}
	if index := preparedStringDomainSourceIndex(name, arity); index >= 0 {
		return []int{index}
	}
	return nil
}

func expressionCollationCandidates(args []*Expr, indexes []int) []expressionCollation {
	var candidates []expressionCollation
	var collect func(*Expr)
	collect = func(expr *Expr) {
		if expr == nil {
			return
		}
		if list := expr.GetList(); list != nil {
			for _, item := range list.List {
				collect(item)
			}
		} else if candidate, ok := collationForExpression(expr); ok {
			candidates = append(candidates, candidate)
		}
	}
	for _, index := range indexes {
		collect(args[index])
	}
	return candidates
}

func normalizeExpressionCollations(ctx context.Context, name string, args []*Expr) ([]*Expr, error) {
	// Tuple IN resolves each position independently in handleTupleIn.
	if (name == "in" || name == "not_in") && len(args) > 0 && args[0].GetList() != nil {
		return args, nil
	}
	indexes := expressionCollationIndexes(name, len(args))
	selected, active, err := resolveExpressionCollation(ctx, name, expressionCollationCandidates(args, indexes))
	if err != nil || !active {
		return args, err
	}
	result := append([]*Expr(nil), args...)
	var convert func(*Expr) (*Expr, error)
	convert = func(expr *Expr) (*Expr, error) {
		if expr == nil {
			return expr, nil
		}
		if list := expr.GetList(); list != nil {
			copy := *expr
			items := make([]*Expr, len(list.List))
			for i, item := range list.List {
				items[i], err = convert(item)
				if err != nil {
					return nil, err
				}
			}
			copy.Expr = &plan.Expr_List{List: &plan.ExprList{List: items}}
			return &copy, nil
		}
		if !types.T(expr.Typ.Id).IsMySQLString() {
			return expr, nil
		}
		target := expr.Typ
		selected.apply(&target)
		if selected.charset == uint32(types.CharsetBinary) {
			target.PadSpace = false
		} else if types.T(target.Id) == types.T_binary || types.T(target.Id) == types.T_varbinary {
			// The losing byte operand must become a text vector, not just carry
			// text metadata on a physical type whose comparator ignores it.
			target.Id = int32(types.T_varchar)
		}
		if target.Id == expr.Typ.Id && target.SameCollation(expr.Typ) {
			return expr, nil
		}
		if target.Id == expr.Typ.Id && target.Charset == expr.Typ.Charset && target.CollationVersion == expr.Typ.CollationVersion {
			copy := *expr
			copy.Typ = target
			return &copy, nil
		}
		return appendCastBeforeExpr(ctx, expr, target)
	}
	for _, index := range indexes {
		result[index], err = convert(args[index])
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func setExpressionCollation(ctx context.Context, name string, args []*Expr, typ *plan.Type) error {
	if !types.T(typ.Id).IsMySQLString() {
		return nil
	}
	if name == "cast" && len(args) == 2 && args[1].GetT() != nil {
		if args[1].Typ.CollationCoercibilitySet {
			selected, _ := collationForExpression(args[1])
			selected.apply(typ)
		}
		return nil
	}
	indexes := expressionCollationIndexes(name, len(args))
	if len(indexes) == 0 {
		return nil
	}
	selected, active, err := resolveExpressionCollation(ctx, name, expressionCollationCandidates(args, indexes))
	if err != nil {
		return err
	}
	if active {
		selected.apply(typ)
	} else if name != "union" && name != "values" && len(expressionCollationCandidates(args, indexes)) > 0 {
		// Keep historical byte semantics, while retaining the expression's
		// actual coercibility for a later native or explicit consumer.
		typ.CollationCoercibility, typ.CollationCoercibilitySet = selected.rank, true
	}
	return nil
}
