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

package plan

import (
	"context"
	"math"
	"math/big"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

const (
	minAffineSumFamilySize = 3
	// floor((10^38 - 1) / (2^64 - 1)). Requiring every shifted input
	// endpoint's absolute value to stay at or below this value proves that
	// SUM's DECIMAL(38,0) result cannot overflow even at the maximum
	// representable input-row cardinality.
	maxExactAffineSumInput int64 = 5421010862427522170
)

type affineSumCandidate struct {
	oldPos int32
	base   *planpb.Expr
	shift  int64
}

type affineSumFamily struct {
	base       *planpb.Expr
	candidates []affineSumCandidate
	anchor0    affineSumCandidate
	anchor1    affineSumCandidate
	anchorPos0 int32
	anchorPos1 int32
}

type affineIntegerRange struct {
	min int64
	max int64
}

// rewriteAffineSumFamilies replaces three or more exact shifted SUMs over one
// statically range-proven integer expression with two adjacent SUM anchors. It
// runs only after every aggregate consumer in this query block has been bound.
// Consumer expressions are prepared on copies and installed together with the
// compacted aggregate list, so an unexpected reference shape leaves the
// context intact.
func (builder *QueryBuilder) rewriteAffineSumFamilies(
	ctx *BindContext,
	exprLists [][]*planpb.Expr,
	orderBys []*planpb.OrderBySpec,
) {
	if ctx == nil || len(ctx.aggregates) < minAffineSumFamilySize {
		return
	}

	familiesByHash := make(map[uint64][]*affineSumFamily)
	var families []*affineSumFamily
	for oldPos, aggregate := range ctx.aggregates {
		base, shift, ok := builder.extractExactAffineSum(aggregate)
		if !ok {
			continue
		}

		hash := exprStructuralHash(base)
		var family *affineSumFamily
		for _, candidateFamily := range familiesByHash[hash] {
			if exprStructuralEqual(candidateFamily.base, base) {
				family = candidateFamily
				break
			}
		}
		if family == nil {
			family = &affineSumFamily{base: base}
			familiesByHash[hash] = append(familiesByHash[hash], family)
			families = append(families, family)
		}
		family.candidates = append(family.candidates, affineSumCandidate{
			oldPos: int32(oldPos),
			base:   base,
			shift:  shift,
		})
	}

	eligibleByOldPos := make(map[int32]*affineSumFamily)
	eligibleFamilies := make([]*affineSumFamily, 0, len(families))
	for _, family := range families {
		if !selectAffineSumAnchors(family) {
			continue
		}
		eligibleFamilies = append(eligibleFamilies, family)
		for _, candidate := range family.candidates {
			eligibleByOldPos[candidate.oldPos] = family
		}
	}
	if len(eligibleFamilies) == 0 {
		return
	}

	newAggregates := make([]*planpb.Expr, 0, len(ctx.aggregates))
	directPositions := make(map[int32]int32, len(ctx.aggregates))
	for oldPos, aggregate := range ctx.aggregates {
		family := eligibleByOldPos[int32(oldPos)]
		if family != nil {
			switch int32(oldPos) {
			case family.anchor0.oldPos:
				family.anchorPos0 = int32(len(newAggregates))
				directPositions[int32(oldPos)] = family.anchorPos0
				newAggregates = append(newAggregates, aggregate)
			case family.anchor1.oldPos:
				family.anchorPos1 = int32(len(newAggregates))
				directPositions[int32(oldPos)] = family.anchorPos1
				newAggregates = append(newAggregates, aggregate)
			}
			continue
		}
		directPositions[int32(oldPos)] = int32(len(newAggregates))
		newAggregates = append(newAggregates, aggregate)
	}

	replacements := make(map[int32]*planpb.Expr, len(ctx.aggregates))
	for oldPos, newPos := range directPositions {
		replacements[oldPos] = GetColExpr(
			ctx.aggregates[oldPos].Typ, ctx.aggregateTag, newPos)
	}
	for _, family := range eligibleFamilies {
		anchor0 := GetColExpr(
			ctx.aggregates[family.anchor0.oldPos].Typ,
			ctx.aggregateTag,
			family.anchorPos0,
		)
		anchor1 := GetColExpr(
			ctx.aggregates[family.anchor1.oldPos].Typ,
			ctx.aggregateTag,
			family.anchorPos1,
		)
		for _, candidate := range family.candidates {
			switch candidate.shift {
			case family.anchor0.shift:
				replacements[candidate.oldPos] = DeepCopyExpr(anchor0)
			case family.anchor1.shift:
				replacements[candidate.oldPos] = DeepCopyExpr(anchor1)
			default:
				delta, ok := checkedAffineSub(candidate.shift, family.anchor0.shift)
				if !ok {
					return
				}
				derived, err := builder.buildAffineSumResult(
					anchor0, anchor1, delta)
				if err != nil || !sameAffineResultType(derived, ctx.aggregates[candidate.oldPos]) {
					return
				}
				replacements[candidate.oldPos] = derived
			}
		}
	}

	rewrittenLists := make([][]*planpb.Expr, len(exprLists))
	for i, exprList := range exprLists {
		var ok bool
		rewrittenLists[i], ok = cloneAndRewriteAffineAggregateRefs(
			exprList, ctx.aggregateTag, replacements)
		if !ok {
			return
		}
	}
	rewrittenOrderExprs := make([]*planpb.Expr, len(orderBys))
	for i, orderBy := range orderBys {
		if orderBy == nil || orderBy.Expr == nil {
			continue
		}
		var ok bool
		rewrittenOrderExprs[i], ok = cloneAndRewriteAffineAggregateRef(
			orderBy.Expr, ctx.aggregateTag, replacements)
		if !ok {
			return
		}
	}
	rewrittenAggregateByAst := make(map[string]int32, len(ctx.aggregateByAst))
	for ast, oldPos := range ctx.aggregateByAst {
		if newPos, retained := directPositions[oldPos]; retained {
			rewrittenAggregateByAst[ast] = newPos
		}
	}

	ctx.aggregates = newAggregates
	for i := range exprLists {
		copy(exprLists[i], rewrittenLists[i])
	}
	for i, rewritten := range rewrittenOrderExprs {
		if rewritten != nil {
			orderBys[i].Expr = rewritten
		}
	}
	ctx.aggregateByAst = rewrittenAggregateByAst
}

func (builder *QueryBuilder) extractExactAffineSum(
	aggregate *planpb.Expr,
) (base *planpb.Expr, shift int64, ok bool) {
	if aggregate == nil || types.T(aggregate.Typ.Id) != types.T_decimal128 ||
		aggregate.Typ.Scale != 0 || aggregate.PreparedNumeric != nil {
		return nil, 0, false
	}
	fn := aggregate.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "sum" ||
		len(fn.Args) != 1 || fn.AggConfigType != planpb.AggregateConfigType_AGG_CONFIG_NONE ||
		len(fn.AggConfig) != 0 || uint64(fn.Func.Obj)&function.Distinct != 0 ||
		!canonicalBoundFunction(builder.GetContext(), aggregate, "sum") {
		return nil, 0, false
	}

	argument := fn.Args[0]
	if argument == nil || argument.PreparedNumeric != nil {
		return nil, 0, false
	}
	base, shift = argument, 0
	if arithmetic := argument.GetF(); arithmetic != nil && arithmetic.Func != nil &&
		len(arithmetic.Args) == 2 && types.T(argument.Typ.Id) == types.T_int64 {
		switch arithmetic.Func.ObjName {
		case "+":
			if !canonicalBoundFunction(builder.GetContext(), argument, "+") {
				return nil, 0, false
			}
			base, shift, ok = affineBaseAndShift(arithmetic.Args[0], arithmetic.Args[1])
			if !ok {
				base, shift, ok = affineBaseAndShift(arithmetic.Args[1], arithmetic.Args[0])
			}
			if !ok {
				base, shift = argument, 0
			}
		case "-":
			if !canonicalBoundFunction(builder.GetContext(), argument, "-") {
				return nil, 0, false
			}
			base, shift, ok = affineBaseAndShift(arithmetic.Args[0], arithmetic.Args[1])
			if ok {
				if shift == math.MinInt64 {
					return nil, 0, false
				}
				shift = -shift
			} else {
				base, shift = argument, 0
			}
		}
	}

	baseRange, ok := exactAffineIntegerRange(builder.GetContext(), base)
	if !ok {
		return nil, 0, false
	}
	shiftedMin, ok := checkedAffineAdd(baseRange.min, shift)
	if !ok {
		return nil, 0, false
	}
	shiftedMax, ok := checkedAffineAdd(baseRange.max, shift)
	if !ok {
		return nil, 0, false
	}
	if affineAbsExceedsExactSumInput(shiftedMin) ||
		affineAbsExceedsExactSumInput(shiftedMax) {
		return nil, 0, false
	}
	return base, shift, true
}

// exactAffineIntegerRange is the reusable proof boundary for deterministic,
// exact integer bases. It deliberately recognizes only operations whose full
// declared input domains fit signed 64-bit without overflow. Adding another
// expression family requires extending this range proof, not adding a SQL-text
// or benchmark-specific exception.
func exactAffineIntegerRange(
	ctx context.Context,
	expr *planpb.Expr,
) (affineIntegerRange, bool) {
	// PreparedNumeric marks expressions whose value or overload may be rebound
	// from runtime parameters.  The proof is reusable only when every node in
	// the base expression is immutable across executions.
	if expr == nil || expr.PreparedNumeric != nil {
		return affineIntegerRange{}, false
	}
	if col := expr.GetCol(); col != nil {
		return affineIntegerTypeRange(types.T(expr.Typ.Id))
	}
	if lit := expr.GetLit(); lit != nil && !lit.Isnull {
		return affineIntegerLiteralRange(expr)
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return affineIntegerRange{}, false
	}

	switch fn.Func.ObjName {
	case "cast":
		if len(fn.Args) < 1 || types.T(expr.Typ.Id) != types.T_int64 ||
			!canonicalExactAffineIntegerCast(ctx, expr) {
			return affineIntegerRange{}, false
		}
		return exactAffineIntegerRange(ctx, fn.Args[0])

	case "+", "-", "*":
		if len(fn.Args) != 2 || types.T(expr.Typ.Id) != types.T_int64 ||
			!canonicalBoundFunction(ctx, expr, fn.Func.ObjName) {
			return affineIntegerRange{}, false
		}
		left, ok := exactAffineIntegerRange(ctx, fn.Args[0])
		if !ok {
			return affineIntegerRange{}, false
		}
		right, ok := exactAffineIntegerRange(ctx, fn.Args[1])
		if !ok {
			return affineIntegerRange{}, false
		}
		return combineAffineIntegerRanges(fn.Func.ObjName, left, right)

	case "unary_plus":
		if len(fn.Args) != 1 || types.T(expr.Typ.Id) != types.T_int64 ||
			!canonicalBoundFunction(ctx, expr, "unary_plus") {
			return affineIntegerRange{}, false
		}
		return exactAffineIntegerRange(ctx, fn.Args[0])

	case "unary_minus":
		if len(fn.Args) != 1 || types.T(expr.Typ.Id) != types.T_int64 ||
			!canonicalBoundFunction(ctx, expr, "unary_minus") {
			return affineIntegerRange{}, false
		}
		input, ok := exactAffineIntegerRange(ctx, fn.Args[0])
		if !ok || input.min == math.MinInt64 {
			return affineIntegerRange{}, false
		}
		return affineIntegerRange{min: -input.max, max: -input.min}, true
	}
	return affineIntegerRange{}, false
}

func canonicalExactAffineIntegerCast(ctx context.Context, expr *planpb.Expr) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" || len(fn.Args) < 1 {
		return false
	}
	_, overloadID := function.DecodeOverloadID(fn.Func.Obj)
	if overloadID != 0 && overloadID != 1 {
		return false
	}
	rebound, err := appendCastBeforeExprWithOverload(
		ctx, DeepCopyExpr(fn.Args[0]), expr.Typ, overloadID)
	if err != nil || rebound == nil || rebound.GetF() == nil || rebound.GetF().Func == nil {
		return false
	}
	return rebound.GetF().Func.Obj == fn.Func.Obj &&
		sameAffineResultType(rebound, expr) && exprStructuralEqual(rebound, expr)
}

func affineIntegerTypeRange(typ types.T) (affineIntegerRange, bool) {
	switch typ {
	case types.T_int8:
		return affineIntegerRange{min: math.MinInt8, max: math.MaxInt8}, true
	case types.T_int16:
		return affineIntegerRange{min: math.MinInt16, max: math.MaxInt16}, true
	case types.T_int32:
		return affineIntegerRange{min: math.MinInt32, max: math.MaxInt32}, true
	case types.T_int64:
		return affineIntegerRange{min: math.MinInt64, max: math.MaxInt64}, true
	case types.T_uint8:
		return affineIntegerRange{max: math.MaxUint8}, true
	case types.T_uint16:
		return affineIntegerRange{max: math.MaxUint16}, true
	case types.T_uint32:
		return affineIntegerRange{max: math.MaxUint32}, true
	default:
		return affineIntegerRange{}, false
	}
}

func affineIntegerLiteralRange(expr *planpb.Expr) (affineIntegerRange, bool) {
	lit := expr.GetLit()
	// Literal.Src is execution-time provenance. In particular, prepared
	// parameters can be specialized to a literal and later restored from Src.
	// Treat only source-free literals as immutable proof inputs so a reusable
	// plan cannot bake one execution's value into the affine rewrite.
	if lit == nil || lit.Isnull || lit.Src != nil || expr.PreparedNumeric != nil {
		return affineIntegerRange{}, false
	}
	var value int64
	switch types.T(expr.Typ.Id) {
	case types.T_int8:
		v, ok := lit.Value.(*planpb.Literal_I8Val)
		if !ok || v.I8Val < math.MinInt8 || v.I8Val > math.MaxInt8 {
			return affineIntegerRange{}, false
		}
		value = int64(v.I8Val)
	case types.T_int16:
		v, ok := lit.Value.(*planpb.Literal_I16Val)
		if !ok || v.I16Val < math.MinInt16 || v.I16Val > math.MaxInt16 {
			return affineIntegerRange{}, false
		}
		value = int64(v.I16Val)
	case types.T_int32:
		v, ok := lit.Value.(*planpb.Literal_I32Val)
		if !ok {
			return affineIntegerRange{}, false
		}
		value = int64(v.I32Val)
	case types.T_int64:
		v, ok := lit.Value.(*planpb.Literal_I64Val)
		if !ok {
			return affineIntegerRange{}, false
		}
		value = v.I64Val
	case types.T_uint8:
		v, ok := lit.Value.(*planpb.Literal_U8Val)
		if !ok || v.U8Val > math.MaxUint8 {
			return affineIntegerRange{}, false
		}
		value = int64(v.U8Val)
	case types.T_uint16:
		v, ok := lit.Value.(*planpb.Literal_U16Val)
		if !ok || v.U16Val > math.MaxUint16 {
			return affineIntegerRange{}, false
		}
		value = int64(v.U16Val)
	case types.T_uint32:
		v, ok := lit.Value.(*planpb.Literal_U32Val)
		if !ok {
			return affineIntegerRange{}, false
		}
		value = int64(v.U32Val)
	case types.T_uint64:
		v, ok := lit.Value.(*planpb.Literal_U64Val)
		if !ok || v.U64Val > math.MaxInt64 {
			return affineIntegerRange{}, false
		}
		value = int64(v.U64Val)
	default:
		return affineIntegerRange{}, false
	}
	return affineIntegerRange{min: value, max: value}, true
}

func combineAffineIntegerRanges(
	op string,
	left, right affineIntegerRange,
) (affineIntegerRange, bool) {
	switch op {
	case "+":
		min, ok := checkedAffineAdd(left.min, right.min)
		if !ok {
			return affineIntegerRange{}, false
		}
		max, ok := checkedAffineAdd(left.max, right.max)
		return affineIntegerRange{min: min, max: max}, ok
	case "-":
		min, ok := checkedAffineSub(left.min, right.max)
		if !ok {
			return affineIntegerRange{}, false
		}
		max, ok := checkedAffineSub(left.max, right.min)
		return affineIntegerRange{min: min, max: max}, ok
	case "*":
		products := [4]*big.Int{
			new(big.Int).Mul(big.NewInt(left.min), big.NewInt(right.min)),
			new(big.Int).Mul(big.NewInt(left.min), big.NewInt(right.max)),
			new(big.Int).Mul(big.NewInt(left.max), big.NewInt(right.min)),
			new(big.Int).Mul(big.NewInt(left.max), big.NewInt(right.max)),
		}
		minimum, maximum := products[0], products[0]
		for _, product := range products[1:] {
			if product.Cmp(minimum) < 0 {
				minimum = product
			}
			if product.Cmp(maximum) > 0 {
				maximum = product
			}
		}
		if !minimum.IsInt64() || !maximum.IsInt64() {
			return affineIntegerRange{}, false
		}
		return affineIntegerRange{min: minimum.Int64(), max: maximum.Int64()}, true
	}
	return affineIntegerRange{}, false
}

func checkedAffineAdd(left, right int64) (int64, bool) {
	if (right > 0 && left > math.MaxInt64-right) ||
		(right < 0 && left < math.MinInt64-right) {
		return 0, false
	}
	return left + right, true
}

func checkedAffineSub(left, right int64) (int64, bool) {
	if (right > 0 && left < math.MinInt64+right) ||
		(right < 0 && left > math.MaxInt64+right) {
		return 0, false
	}
	return left - right, true
}

func affineAbsExceedsExactSumInput(value int64) bool {
	if value == math.MinInt64 {
		return true
	}
	if value < 0 {
		value = -value
	}
	return value > maxExactAffineSumInput
}

func affineBaseAndShift(base, literal *planpb.Expr) (*planpb.Expr, int64, bool) {
	if base == nil || literal == nil || types.T(literal.Typ.Id) != types.T_int64 {
		return nil, 0, false
	}
	lit := literal.GetLit()
	if lit == nil || lit.Isnull || lit.Src != nil || literal.PreparedNumeric != nil {
		return nil, 0, false
	}
	value, ok := lit.Value.(*planpb.Literal_I64Val)
	if !ok {
		return nil, 0, false
	}
	return base, value.I64Val, true
}

func canonicalBoundFunction(ctx context.Context, expr *planpb.Expr, name string) bool {
	rebound, err := BindFuncExprImplByPlanExpr(ctx, name, DeepCopyExprList(expr.GetF().Args))
	if err != nil || rebound == nil || rebound.GetF() == nil || rebound.GetF().Func == nil {
		return false
	}
	return rebound.GetF().Func.Obj == expr.GetF().Func.Obj &&
		sameAffineResultType(rebound, expr) && exprStructuralEqual(rebound, expr)
}

func selectAffineSumAnchors(family *affineSumFamily) bool {
	if family == nil || len(family.candidates) < minAffineSumFamilySize {
		return false
	}
	byShift := make(map[int64]affineSumCandidate, len(family.candidates))
	minShift, maxShift := family.candidates[0].shift, family.candidates[0].shift
	for _, candidate := range family.candidates {
		if _, exists := byShift[candidate.shift]; !exists {
			byShift[candidate.shift] = candidate
		}
		if candidate.shift < minShift {
			minShift = candidate.shift
		}
		if candidate.shift > maxShift {
			maxShift = candidate.shift
		}
	}
	anchorFound := false
	for shift, candidate := range byShift {
		if shift == math.MaxInt64 {
			continue
		}
		next, exists := byShift[shift+1]
		if !exists {
			continue
		}
		leftRadius, leftFits := checkedAffineSub(shift, minShift)
		rightRadius, rightFits := checkedAffineSub(maxShift, shift)
		if !leftFits || !rightFits ||
			leftRadius > maxExactAffineSumInput || rightRadius > maxExactAffineSumInput {
			continue
		}
		if !anchorFound || shift < family.anchor0.shift {
			family.anchor0 = candidate
			family.anchor1 = next
			anchorFound = true
		}
	}
	return anchorFound
}

func (builder *QueryBuilder) buildAffineSumResult(
	anchor0, anchor1 *planpb.Expr,
	delta int64,
) (*planpb.Expr, error) {
	difference, err := BindFuncExprImplByPlanExpr(
		builder.GetContext(), "-", []*planpb.Expr{DeepCopyExpr(anchor1), DeepCopyExpr(anchor0)})
	if err != nil {
		return nil, err
	}
	scaled, err := BindFuncExprImplByPlanExpr(
		builder.GetContext(), "*", []*planpb.Expr{difference, makePlan2Int64ConstExprWithType(delta)})
	if err != nil {
		return nil, err
	}
	return BindFuncExprImplByPlanExpr(
		builder.GetContext(), "+", []*planpb.Expr{DeepCopyExpr(anchor0), scaled})
}

func sameAffineResultType(left, right *planpb.Expr) bool {
	if left == nil || right == nil {
		return false
	}
	l, r := left.Typ, right.Typ
	return l.Id == r.Id && l.NotNullable == r.NotNullable &&
		l.AutoIncr == r.AutoIncr && l.Width == r.Width && l.Scale == r.Scale &&
		l.Table == r.Table && l.Enumvalues == r.Enumvalues &&
		l.Charset == r.Charset && l.PadSpace == r.PadSpace
}

func cloneAndRewriteAffineAggregateRefs(
	exprs []*planpb.Expr,
	aggregateTag int32,
	replacements map[int32]*planpb.Expr,
) ([]*planpb.Expr, bool) {
	rewritten := make([]*planpb.Expr, len(exprs))
	for i, expr := range exprs {
		var ok bool
		rewritten[i], ok = cloneAndRewriteAffineAggregateRef(expr, aggregateTag, replacements)
		if !ok {
			return nil, false
		}
	}
	return rewritten, true
}

func cloneAndRewriteAffineAggregateRef(
	expr *planpb.Expr,
	aggregateTag int32,
	replacements map[int32]*planpb.Expr,
) (*planpb.Expr, bool) {
	hasRef, safe := affineExprHasAggregateRef(expr, aggregateTag, replacements)
	if !safe {
		return nil, false
	}
	if !hasRef {
		return expr, true
	}
	return rewriteAffineAggregateRef(DeepCopyExpr(expr), aggregateTag, replacements)
}

func affineExprHasAggregateRef(
	expr *planpb.Expr,
	aggregateTag int32,
	replacements map[int32]*planpb.Expr,
) (hasRef, safe bool) {
	if expr == nil {
		return false, true
	}
	switch impl := expr.Expr.(type) {
	case *planpb.Expr_Lit:
		// These are executable leaves. Literal.Src is provenance rather than
		// an evaluated child and must not be remapped.
		return false, impl.Lit != nil
	case *planpb.Expr_P:
		return false, impl.P != nil
	case *planpb.Expr_V:
		return false, impl.V != nil
	case *planpb.Expr_Raw:
		return false, impl.Raw != nil
	case *planpb.Expr_T:
		return false, impl.T != nil
	case *planpb.Expr_Max:
		return false, impl.Max != nil
	case *planpb.Expr_Vec:
		return false, impl.Vec != nil
	case *planpb.Expr_Fold:
		return false, impl.Fold != nil
	case *planpb.Expr_Col:
		if impl.Col == nil {
			return false, false
		}
		if impl.Col.RelPos != aggregateTag {
			return false, true
		}
		_, ok := replacements[impl.Col.ColPos]
		return true, ok
	case *planpb.Expr_Corr:
		if impl.Corr == nil {
			return false, false
		}
		if impl.Corr.RelPos == aggregateTag {
			return true, false
		}
		return false, true
	case *planpb.Expr_F:
		if impl.F == nil || impl.F.Func == nil {
			return false, false
		}
		return affineExprListHasAggregateRef(impl.F.Args, aggregateTag, replacements)
	case *planpb.Expr_List:
		if impl.List == nil {
			return false, false
		}
		return affineExprListHasAggregateRef(impl.List.List, aggregateTag, replacements)
	case *planpb.Expr_Sub:
		if impl.Sub == nil {
			return false, false
		}
		return affineExprHasAggregateRef(impl.Sub.Child, aggregateTag, replacements)
	case *planpb.Expr_W:
		if impl.W == nil {
			return false, false
		}
		hasRef, safe = affineExprHasAggregateRef(impl.W.WindowFunc, aggregateTag, replacements)
		if !safe {
			return hasRef, false
		}
		for _, list := range [][]*planpb.Expr{impl.W.PartitionBy} {
			listHasRef, listSafe := affineExprListHasAggregateRef(list, aggregateTag, replacements)
			hasRef = hasRef || listHasRef
			if !listSafe {
				return hasRef, false
			}
		}
		for _, orderBy := range impl.W.OrderBy {
			if orderBy == nil {
				continue
			}
			itemHasRef, itemSafe := affineExprHasAggregateRef(orderBy.Expr, aggregateTag, replacements)
			hasRef = hasRef || itemHasRef
			if !itemSafe {
				return hasRef, false
			}
		}
		// DeepCopyExpr requires a complete frame for window expressions. Keep the
		// preflight traversal at least as strict as the clone it guards, otherwise
		// a malformed consumer could pass validation and panic before the atomic
		// rewrite is installed.
		if impl.W.Frame == nil || impl.W.Frame.Start == nil || impl.W.Frame.End == nil {
			return hasRef, false
		}
		for _, bound := range []*planpb.FrameBound{impl.W.Frame.Start, impl.W.Frame.End} {
			itemHasRef, itemSafe := affineExprHasAggregateRef(bound.Val, aggregateTag, replacements)
			hasRef = hasRef || itemHasRef
			if !itemSafe {
				return hasRef, false
			}
		}
		return hasRef, true
	default:
		// A newly added expression variant may own executable children. Until
		// this traversal explicitly understands it, abort the whole rewrite
		// rather than leaving a stale aggregate slot hidden inside it.
		return false, false
	}
}

func affineExprListHasAggregateRef(
	exprs []*planpb.Expr,
	aggregateTag int32,
	replacements map[int32]*planpb.Expr,
) (hasRef, safe bool) {
	for _, expr := range exprs {
		itemHasRef, itemSafe := affineExprHasAggregateRef(expr, aggregateTag, replacements)
		hasRef = hasRef || itemHasRef
		if !itemSafe {
			return hasRef, false
		}
	}
	return hasRef, true
}

func rewriteAffineAggregateRef(
	expr *planpb.Expr,
	aggregateTag int32,
	replacements map[int32]*planpb.Expr,
) (*planpb.Expr, bool) {
	if expr == nil {
		return nil, true
	}
	switch impl := expr.Expr.(type) {
	case *planpb.Expr_Lit, *planpb.Expr_P, *planpb.Expr_V,
		*planpb.Expr_Raw, *planpb.Expr_T, *planpb.Expr_Max,
		*planpb.Expr_Vec, *planpb.Expr_Fold, *planpb.Expr_Corr:
		return expr, true
	case *planpb.Expr_Col:
		if impl.Col != nil && impl.Col.RelPos == aggregateTag {
			replacement, ok := replacements[impl.Col.ColPos]
			if !ok {
				return nil, false
			}
			return DeepCopyExpr(replacement), true
		}
	case *planpb.Expr_F:
		if impl.F == nil || impl.F.Func == nil {
			return nil, false
		}
		for i := range impl.F.Args {
			var ok bool
			impl.F.Args[i], ok = rewriteAffineAggregateRef(impl.F.Args[i], aggregateTag, replacements)
			if !ok {
				return nil, false
			}
		}
	case *planpb.Expr_List:
		if impl.List == nil {
			return expr, true
		}
		for i := range impl.List.List {
			var ok bool
			impl.List.List[i], ok = rewriteAffineAggregateRef(impl.List.List[i], aggregateTag, replacements)
			if !ok {
				return nil, false
			}
		}
	case *planpb.Expr_Sub:
		if impl.Sub != nil {
			var ok bool
			impl.Sub.Child, ok = rewriteAffineAggregateRef(impl.Sub.Child, aggregateTag, replacements)
			if !ok {
				return nil, false
			}
		}
	case *planpb.Expr_W:
		if impl.W == nil {
			return expr, true
		}
		var ok bool
		impl.W.WindowFunc, ok = rewriteAffineAggregateRef(impl.W.WindowFunc, aggregateTag, replacements)
		if !ok {
			return nil, false
		}
		for i := range impl.W.PartitionBy {
			impl.W.PartitionBy[i], ok = rewriteAffineAggregateRef(impl.W.PartitionBy[i], aggregateTag, replacements)
			if !ok {
				return nil, false
			}
		}
		for _, orderBy := range impl.W.OrderBy {
			if orderBy == nil {
				continue
			}
			orderBy.Expr, ok = rewriteAffineAggregateRef(orderBy.Expr, aggregateTag, replacements)
			if !ok {
				return nil, false
			}
		}
		if impl.W.Frame != nil {
			for _, bound := range []*planpb.FrameBound{impl.W.Frame.Start, impl.W.Frame.End} {
				if bound == nil {
					continue
				}
				bound.Val, ok = rewriteAffineAggregateRef(bound.Val, aggregateTag, replacements)
				if !ok {
					return nil, false
				}
			}
		}
	default:
		return nil, false
	}
	return expr, true
}
