// Copyright 2022 Matrix Origin
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
	"encoding/binary"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	mosort "github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func NewHavingBinder(builder *QueryBuilder, ctx *BindContext) *HavingBinder {
	b := &HavingBinder{
		insideAgg: false,
	}
	b.sysCtx = builder.GetContext()
	b.builder = builder
	b.ctx = ctx
	b.impl = b

	return b
}

func (b *HavingBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	astStr := windowExprAstKey(astExpr)

	if !b.insideAgg {
		if colPos, ok := lookupGroupByAst(b.ctx, astExpr, astStr); ok {
			return &plan.Expr{
				Typ: b.ctx.groupOutputType(colPos),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: b.ctx.groupTag,
						ColPos: colPos,
					},
				},
			}, nil
		}
	}

	if colPos, ok := b.ctx.aggregateByAst[astStr]; ok {
		if !b.insideAgg {
			return &plan.Expr{
				Typ: b.ctx.aggregates[colPos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: b.ctx.aggregateTag,
						ColPos: colPos,
					},
				},
			}, nil
		} else {
			return nil, moerr.NewInvalidInput(b.GetContext(), "nestted aggregate function")
		}
	}

	if colPos, ok := b.ctx.sampleByAst[astStr]; ok {
		return &plan.Expr{
			Typ: b.ctx.sampleFunc.columns[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: b.ctx.sampleTag,
					ColPos: colPos,
				},
			},
		}, nil
	}

	if colPos, ok := b.ctx.windowByAst[astStr]; ok {
		return &plan.Expr{
			Typ: b.ctx.windows[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: b.ctx.windowTag,
					ColPos: colPos,
				},
			},
		}, nil
	}

	return b.baseBindExpr(astExpr, depth, isRoot)
}

func (b *HavingBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	if b.insideAgg {
		expr, err := b.baseBindColRef(astExpr, depth, isRoot)
		if err != nil {
			return nil, err
		}

		if _, ok := expr.Expr.(*plan.Expr_Corr); ok {
			return nil, moerr.NewNYI(b.GetContext(), "correlated columns in aggregate function")
		}

		return expr, nil
	} else if b.builder.mysqlCompatible {
		expr, err := b.baseBindColRef(astExpr, depth, isRoot)
		if err != nil {
			return nil, err
		}

		if _, ok := expr.Expr.(*plan.Expr_Corr); ok {
			return nil, moerr.NewNYI(b.GetContext(), "correlated columns in aggregate function")
		}
		newExpr, _ := BindFuncExprImplByPlanExpr(b.builder.compCtx.GetContext(), "any_value", []*plan.Expr{expr})
		colPos := len(b.ctx.aggregates)
		b.ctx.aggregates = append(b.ctx.aggregates, newExpr)
		return &plan.Expr{
			Typ: b.ctx.aggregates[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: b.ctx.aggregateTag,
					ColPos: int32(colPos),
				},
			},
		}, nil
	} else if b.builder.mysqlFullGroupByCompat {
		expr, err := b.baseBindColRef(astExpr, depth, isRoot)
		if err != nil {
			return nil, err
		}

		if corr, ok := expr.Expr.(*plan.Expr_Corr); ok {
			if b.corrColRefTargetsCurrentGroup(corr.Corr) || b.corrColRefTargetsGroup(corr.Corr) {
				return expr, nil
			}
			return nil, b.newGroupByColumnError(astExpr)
		}
		if !b.builder.mysqlFullGroupByAllowsColRef(b.ctx, expr) {
			return nil, b.newGroupByColumnError(astExpr)
		}

		newExpr, _ := BindFuncExprImplByPlanExpr(b.builder.compCtx.GetContext(), "any_value", []*plan.Expr{expr})
		colPos := len(b.ctx.aggregates)
		b.ctx.aggregates = append(b.ctx.aggregates, newExpr)
		return &plan.Expr{
			Typ: b.ctx.aggregates[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: b.ctx.aggregateTag,
					ColPos: int32(colPos),
				},
			},
		}, nil
	} else {
		if expr, err := b.baseBindColRef(astExpr, depth, isRoot); err == nil {
			if corr, ok := expr.Expr.(*plan.Expr_Corr); ok &&
				(b.corrColRefTargetsCurrentGroup(corr.Corr) || b.corrColRefTargetsGroup(corr.Corr)) {
				return expr, nil
			}
		}
		return nil, b.newGroupByColumnError(astExpr)
	}
}

func (b *HavingBinder) newGroupByColumnError(astExpr *tree.UnresolvedName) error {
	return moerr.NewSyntaxErrorf(b.GetContext(), "column %q must appear in the GROUP BY clause or be used in an aggregate function", tree.String(astExpr, dialect.MYSQL))
}

// validateCountArgs validates COUNT function arguments against MySQL semantics.
//   - COUNT(*), COUNT(expr): always valid
//   - COUNT(expr1, expr2, ...): only valid with DISTINCT
//   - COUNT((expr1, expr2, ...)): only valid with DISTINCT
//
// Call this from every binder path that constructs a COUNT aggregate or window
// function (HavingBinder.BindAggFunc, bindWindowFuncExpr) before the call to
// bindFuncExprImplByAstExpr.
func validateCountArgs(ctx context.Context, funcName string, astExpr *tree.FuncExpr) error {
	if funcName != "count" {
		return nil
	}
	if len(astExpr.Exprs) == 0 {
		// COUNT(*)
		return nil
	}
	// COUNT((a, b, ...)) — tuple-as-single-arg, only valid with DISTINCT.
	if len(astExpr.Exprs) == 1 {
		if _, ok := astExpr.Exprs[0].(*tree.Tuple); ok && astExpr.Type != tree.FUNC_TYPE_DISTINCT {
			return moerr.NewSyntaxErrorf(ctx, "Incorrect arguments to COUNT")
		}
		return nil
	}
	// COUNT(a, b, ...) — multiple separate args, only valid with DISTINCT.
	if astExpr.Type != tree.FUNC_TYPE_DISTINCT {
		return moerr.NewSyntaxErrorf(ctx, "Incorrect arguments to COUNT")
	}
	return nil
}

func (b *HavingBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	if b.insideAgg {
		return nil, moerr.NewSyntaxErrorf(b.GetContext(), "aggregate function %s calls cannot be nested", funcName)
	}

	if err := validateCountArgs(b.GetContext(), funcName, astExpr); err != nil {
		return nil, err
	}

	b.insideAgg = true
	var expr *plan.Expr
	var err error
	if strings.EqualFold(funcName, NamePercentileCont) || strings.EqualFold(funcName, NamePercentileDisc) {
		expr, err = b.bindOrderedSetPercentileAgg(funcName, astExpr, depth, isRoot)
	} else {
		expr, err = b.bindPreparedNumericAggregateFuncExpr(funcName, astExpr.Exprs, depth)
	}
	if err != nil {
		b.insideAgg = false
		return nil, err
	}

	// Normalize COUNT(DISTINCT (a, b)) → COUNT(DISTINCT a, b) by expanding
	// the tuple arg into separate args. This must happen for every aggregate
	// expression (not just inside optimizeDistinctAgg), otherwise the executor
	// cannot build a correct multi-column distinct key from a single T_tuple vector.
	//
	// Only an AST tuple binds to Expr_List and can be expanded. A multi-column
	// row subquery — e.g. COUNT(DISTINCT (SELECT a, b ...)) — also carries
	// Typ.Id == T_tuple but is an Expr_Sub: GetList() is nil there, and leaving
	// it unexpanded would let downstream either nil-deref, error as NYI, or
	// silently collapse to the subquery's first column. Reject it explicitly.
	f := expr.GetF()
	if funcName == "count" && astExpr.Type == tree.FUNC_TYPE_DISTINCT &&
		len(f.Args) == 1 && f.Args[0].Typ.Id == int32(types.T_tuple) {
		list := f.Args[0].GetList()
		if list == nil {
			return nil, moerr.NewNotSupported(b.GetContext(),
				"COUNT(DISTINCT ...) with a multi-column subquery argument")
		}
		f.Args = list.List
	}

	if astExpr.Type == tree.FUNC_TYPE_DISTINCT {
		if funcName != "max" && funcName != "min" && funcName != "any_value" {
			expr.GetF().Func.Obj = int64(uint64(expr.GetF().Func.Obj) | function.Distinct)
		}
	}
	if funcName == NameGroupConcat {
		if err := b.bindGroupConcatOrderBy(astExpr, expr, depth, isRoot); err != nil {
			b.insideAgg = false
			return nil, err
		}
	}
	b.insideAgg = false

	if b.ctx.timeTag > 0 && b.ctx.sliding {
		expr, err = b.remapAggToTimeWindowCacheAgg(expr)
		if err != nil {
			return nil, err
		}
	}

	colPos := int32(len(b.ctx.aggregates))
	astStr := semanticAstKey(astExpr)
	b.ctx.aggregateByAst[astStr] = colPos
	b.ctx.aggregates = append(b.ctx.aggregates, expr)

	return &plan.Expr{
		Typ: expr.Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: b.ctx.aggregateTag,
				ColPos: colPos,
			},
		},
	}, nil
}

// bindOrderedSetPercentileAgg converts the SQL-standard
// PERCENTILE_{CONT,DISC}(p) WITHIN GROUP (ORDER BY value) shape into the
// executor's ordinary two-argument aggregate shape: [value, p]. The direct
// percentile argument is retained until compile time, where it is evaluated
// and moved into the aggregate extra configuration.
func (b *HavingBinder) bindOrderedSetPercentileAgg(
	funcName string,
	astExpr *tree.FuncExpr,
	depth int32,
	isRoot bool,
) (*plan.Expr, error) {
	if b.ctx != nil && b.ctx.timeTag > 0 {
		return nil, moerr.NewNotSupported(b.GetContext(),
			"ordered-set percentile aggregates in time windows")
	}
	if !astExpr.WithinGroup {
		return nil, moerr.NewSyntaxErrorf(b.GetContext(),
			"%s requires WITHIN GROUP (ORDER BY ...)", funcName)
	}
	if len(astExpr.Exprs) != 1 {
		return nil, moerr.NewSyntaxErrorf(b.GetContext(),
			"%s requires exactly one percentile argument", funcName)
	}
	if len(astExpr.OrderBy) != 1 {
		return nil, moerr.NewSyntaxErrorf(b.GetContext(),
			"%s requires exactly one WITHIN GROUP ORDER BY expression", funcName)
	}

	orderExpr := astExpr.OrderBy[0]
	if orderExpr == nil || orderExpr.Expr == nil {
		return nil, moerr.NewSyntaxErrorf(b.GetContext(),
			"%s requires an ORDER BY expression", funcName)
	}
	value, err := b.BindExpr(orderExpr.Expr, depth, isRoot)
	if err != nil {
		return nil, err
	}
	percentile, err := b.BindExpr(astExpr.Exprs[0], depth, false)
	if err != nil {
		return nil, err
	}

	var expr *plan.Expr
	if b.builder == nil || b.builder.compCtx == nil {
		expr, err = BindFuncExprImplByPlanExpr(
			b.GetContext(), funcName, []*plan.Expr{value, percentile})
	} else {
		expr, err = bindFuncExprAndConstFold(
			b.GetContext(), b.builder.compCtx.GetProcess(), funcName,
			[]*plan.Expr{value, percentile},
		)
	}
	if err != nil {
		return nil, err
	}
	fn := expr.GetF()
	if fn == nil {
		return nil, moerr.NewInternalError(b.GetContext(),
			"invalid ordered-set percentile expression")
	}
	if orderExpr.Direction == tree.Descending {
		fn.AggConfig = []byte{1}
	} else {
		fn.AggConfig = []byte{0}
	}
	fn.AggConfigType = plan.AggregateConfigType_AGG_CONFIG_NONE
	return expr, nil
}

func (b *HavingBinder) remapAggToTimeWindowCacheAgg(expr *Expr) (*Expr, error) {
	f := expr.Expr.(*plan.Expr_F).F

	funcId, _ := function.DecodeOverloadID(f.Func.Obj)
	switch funcId {
	case function.AVG:
		typ := makeTypeByPlan2Type(f.Args[0].Typ)
		fGet, err := function.GetFunctionByName(b.GetContext(), "avg_tw_cache", []types.Type{typ})
		if err != nil {
			return nil, err
		}
		f.Func.Obj = fGet.GetEncodedOverloadID()
		f.Func.ObjName = "avg_tw_cache"
		expr.Typ.Id = int32(fGet.GetReturnType().Oid)
		expr.Typ.Width = fGet.GetReturnType().Width
		expr.Typ.Scale = fGet.GetReturnType().Scale
	case function.MAX_BY, function.MAX_BY_NON_NULL:
		if b.ctx == nil || !b.ctx.explicitSliding {
			return expr, nil
		}
		// A sliding window combines winners from several child buckets. The
		// value alone is not a mergeable max_by state because its order/tie
		// columns have already been consumed by the child aggregate. Refuse the
		// query until a typed cache/result pair (like AVG_TW_*) is available.
		return nil, moerr.NewNotSupported(b.GetContext(), "max_by aggregates in a sliding time window")
	}
	return expr, nil
}

func (b *HavingBinder) remapAggToTimeWindowResultAgg(expr *Expr) (*Expr, error) {
	obj := expr.Expr.(*plan.Expr_F).F.Func

	funcId, _ := function.DecodeOverloadID(obj.Obj)
	switch funcId {
	case function.SUM:
		arg := expr.GetF().Args[0]
		typ := makeTypeByPlan2Type(arg.Typ)
		fGet, err := function.GetFunctionByName(b.GetContext(), "sum", []types.Type{typ})
		if err != nil {
			return nil, err
		}
		obj.Obj = fGet.GetEncodedOverloadID()
		obj.ObjName = "sum"
		expr.Typ.Id = int32(fGet.GetReturnType().Oid)
		expr.Typ.Width = fGet.GetReturnType().Width
		expr.Typ.Scale = fGet.GetReturnType().Scale
	case function.COUNT, function.STARCOUNT:
		// COUNT(*) is bound as STARCOUNT in the child Aggregate.  A GAPFILL
		// tumbling window consumes one partial row per existing bucket, so the
		// second stage must merge that partial count instead of counting the
		// partial row itself (which would return 1 for every non-empty bucket).
		fGet, err := function.GetFunctionByName(b.GetContext(), "sum", []types.Type{types.T_int64.ToType()})
		if err != nil {
			return nil, err
		}
		obj.Obj = fGet.GetEncodedOverloadID()
		obj.ObjName = "sum"
		expr.Typ.Id = int32(fGet.GetReturnType().Oid)
		expr.Typ.Width = fGet.GetReturnType().Width
		expr.Typ.Scale = fGet.GetReturnType().Scale
	case function.AVG_TW_CACHE:
		typ := makeTypeByPlan2Type(expr.Typ)
		fGet, err := function.GetFunctionByName(b.GetContext(), "avg_tw_result", []types.Type{typ})
		if err != nil {
			return nil, err
		}
		obj.Obj = fGet.GetEncodedOverloadID()
		obj.ObjName = "avg_tw_result"
		expr.Typ.Id = int32(fGet.GetReturnType().Oid)
		expr.Typ.Width = fGet.GetReturnType().Width
		expr.Typ.Scale = fGet.GetReturnType().Scale
	case function.MAX_BY, function.MAX_BY_NON_NULL:
		// For a tumbling window the child Aggregate has already produced exactly
		// one fully merged winner for each (partition, bucket). TimeWin either
		// forwards that row or runs the GAPFILL state machine over that one row,
		// so the outer aggregate is an identity operation. Retaining max_by here
		// would construct a one-argument max_by and fail during Prepare.
		arg := expr.GetF().Args[0]
		typ := makeTypeByPlan2Type(arg.Typ)
		fGet, err := function.GetFunctionByName(b.GetContext(), "any_value", []types.Type{typ})
		if err != nil {
			return nil, err
		}
		obj.Obj = fGet.GetEncodedOverloadID()
		obj.ObjName = "any_value"
		expr.Typ.Id = int32(fGet.GetReturnType().Oid)
		expr.Typ.Width = fGet.GetReturnType().Width
		expr.Typ.Scale = fGet.GetReturnType().Scale
	}
	return expr, nil
}

func makeTimeWindowProjectionExpr(ctx context.Context, bindCtx *BindContext, astExpr tree.Expr, colPos int32) (*plan.Expr, error) {
	expr := &plan.Expr{
		Typ: bindCtx.times[colPos].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: bindCtx.timeTag,
				ColPos: colPos,
			},
		},
	}
	if bindCtx.sliding && isCountFuncExpr(astExpr) && types.T(expr.Typ.Id) != types.T_int64 {
		int64Type := types.T_int64.ToType()
		return appendCastBeforeExpr(ctx, expr, makePlan2Type(&int64Type))
	}
	return expr, nil
}

func isCountFuncExpr(astExpr tree.Expr) bool {
	funcExpr, ok := astExpr.(*tree.FuncExpr)
	if !ok {
		return false
	}
	funcRef, ok := funcExpr.Func.FunctionReference.(*tree.UnresolvedName)
	return ok && strings.EqualFold(funcRef.ColName(), "count")
}

const groupConcatOrderConfigVersion = byte(2)

func (b *HavingBinder) bindGroupConcatOrderBy(
	astExpr *tree.FuncExpr,
	expr *plan.Expr,
	depth int32,
	isRoot bool,
) error {
	if len(astExpr.OrderBy) < 1 {
		return nil
	}

	fn := expr.GetF()
	if fn == nil {
		return moerr.NewInternalError(b.GetContext(), "invalid group_concat expression")
	}
	concatArgCount := len(fn.Args) - 1
	if concatArgCount < 1 {
		return moerr.NewSyntaxError(b.GetContext(), "group_concat requires arguments")
	}
	separatorLiteral := fn.Args[concatArgCount].GetLit()
	if separatorLiteral == nil {
		return moerr.NewInternalError(b.GetContext(), "invalid group_concat separator")
	}

	orderExprs := make([]*plan.Expr, 0, len(astExpr.OrderBy))
	orderFlags := make([]byte, 0, len(astExpr.OrderBy))
	orderArgIndexes := make([]uint32, 0, len(astExpr.OrderBy))
	for _, order := range astExpr.OrderBy {
		orderExpr := order.Expr
		orderArgIndex := -1
		if numVal, ok := order.Expr.(*tree.NumVal); ok {
			switch numVal.Kind() {
			case tree.Int:
				if numVal.Negative() {
					break
				}
				colPos, ok := numVal.Uint64()
				if !ok {
					break
				}
				if colPos < 1 || colPos > uint64(concatArgCount) {
					return moerr.NewSyntaxErrorf(b.GetContext(), "ORDER BY position %v is not in group_concat arguments", colPos)
				}
				orderExpr = astExpr.Exprs[colPos-1]
				orderArgIndex = int(colPos - 1)
			}
		}

		if _, ok := orderExpr.(*tree.Subquery); ok {
			return moerr.NewNotSupported(b.GetContext(), "subquery in group_concat ORDER BY")
		}

		var boundExpr *plan.Expr
		if orderArgIndex >= 0 {
			// Reuse the already-bound aggregate argument. Rebinding an ordinal
			// expression such as RAND() would evaluate it a second time and sort
			// by values different from those being concatenated.
			boundExpr = fn.Args[orderArgIndex]
		} else {
			oldInsideAgg := b.insideAgg
			b.insideAgg = true
			var err error
			boundExpr, err = b.BindExpr(orderExpr, depth, isRoot)
			b.insideAgg = oldInsideAgg
			if err != nil {
				return err
			}
		}
		if hasSubquery(boundExpr) {
			return moerr.NewNotSupported(b.GetContext(), "subquery in group_concat ORDER BY")
		}
		// A literal key is equal for every input row and has no effect on the
		// ordering. Do not expose it as an executor key (NULL has type ANY).
		if boundExpr.GetLit() != nil {
			continue
		}
		// ENUM/SET values are exposed through display conversion functions, but
		// ORDER BY must use their internal ordinal/bitmap representation.
		orderKey, err := b.groupConcatOrderKey(boundExpr)
		if err != nil {
			return err
		}
		if orderKey != boundExpr {
			// ENUM/SET display arguments cannot be reused because their ORDER
			// BY semantics use the internal index/bitmap value.
			orderArgIndex = -1
		}
		boundExpr = orderKey
		if !mosort.IsSupportedType(types.T(boundExpr.Typ.Id)) {
			return moerr.NewNotSupportedf(
				b.GetContext(),
				"group_concat ORDER BY type %s",
				types.T(boundExpr.Typ.Id).String(),
			)
		}

		orderBy := plan.OrderBySpec{
			Flag: plan.OrderBySpec_INTERNAL,
		}
		switch order.Direction {
		case tree.Ascending:
			orderBy.Flag |= plan.OrderBySpec_ASC
		case tree.Descending:
			orderBy.Flag |= plan.OrderBySpec_DESC
		}

		switch order.NullsPosition {
		case tree.NullsFirst:
			orderBy.Flag |= plan.OrderBySpec_NULLS_FIRST
		case tree.NullsLast:
			orderBy.Flag |= plan.OrderBySpec_NULLS_LAST
		}

		orderFlags = append(orderFlags, byte(orderBy.Flag))
		if orderArgIndex < 0 {
			orderExprs = append(orderExprs, boundExpr)
			orderArgIndex = concatArgCount + len(orderExprs) - 1
		}
		orderArgIndexes = append(orderArgIndexes, uint32(orderArgIndex))
	}
	if len(orderFlags) == 0 {
		return nil
	}

	config := encodeGroupConcatOrderConfig(
		concatArgCount,
		orderFlags,
		orderArgIndexes,
		separatorLiteral.GetSval(),
	)
	args := make([]*plan.Expr, 0, concatArgCount+len(orderExprs))
	args = append(args, fn.Args[:concatArgCount]...)
	args = append(args, orderExprs...)
	fn.Args = args
	fn.AggConfig = config
	fn.AggConfigType = plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER
	return nil
}

func (b *HavingBinder) groupConcatOrderKey(expr *plan.Expr) (*plan.Expr, error) {
	if isEnumOrSetDisplayValueExpr(expr) {
		fn := expr.GetF()
		if len(fn.Args) > 1 {
			return fn.Args[1], nil
		}
	}
	if storageType := b.ctx.mysqlSpecialOrderTypeForExpr(expr); storageType != nil {
		return makeMySQLSpecialOrderKey(b.GetContext(), expr, storageType)
	}
	return expr, nil
}

func encodeGroupConcatOrderConfig(
	concatArgCount int,
	orderFlags []byte,
	orderArgIndexes []uint32,
	separator string,
) []byte {
	separatorBytes := []byte(separator)
	config := make([]byte, 0, 13+len(orderFlags)+4*len(orderArgIndexes)+len(separatorBytes))
	config = append(config, groupConcatOrderConfigVersion)

	var encodedUint32 [4]byte
	binary.BigEndian.PutUint32(encodedUint32[:], uint32(concatArgCount))
	config = append(config, encodedUint32[:]...)
	binary.BigEndian.PutUint32(encodedUint32[:], uint32(len(orderFlags)))
	config = append(config, encodedUint32[:]...)
	config = append(config, orderFlags...)
	for _, index := range orderArgIndexes {
		binary.BigEndian.PutUint32(encodedUint32[:], index)
		config = append(config, encodedUint32[:]...)
	}
	binary.BigEndian.PutUint32(encodedUint32[:], uint32(len(separatorBytes)))
	config = append(config, encodedUint32[:]...)
	config = append(config, separatorBytes...)
	return config
}

func (b *HavingBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	if b.insideAgg {
		return nil, moerr.NewSyntaxError(b.GetContext(), "aggregate function calls cannot contain window function calls")
	}
	return bindWindowFuncExpr(b, b.ctx, funcName, astExpr, depth, isRoot)
}

func (b *HavingBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return b.baseBindSubquery(astExpr, isRoot)
}

func (b *HavingBinder) makeFrameConstValue(expr tree.Expr, typ *plan.Type) (*plan.Expr, error) {
	return makeWindowFrameConstValue(b.baseBindExpr, b.builder.compCtx.GetProcess(), b.GetContext(), expr, typ)
}

func (b *HavingBinder) BindTimeWindowFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	if astExpr.Type == tree.FUNC_TYPE_DISTINCT {
		return nil, moerr.NewNotSupported(b.GetContext(), "DISTINCT in time window")
	}
	if strings.EqualFold(funcName, NameGroupConcat) && len(astExpr.OrderBy) > 0 && b.ctx.sliding {
		return nil, moerr.NewNotSupported(
			b.GetContext(),
			"ordered group_concat in sliding time window",
		)
	}
	var err error

	forgeColCnt := int32(0)
	for _, expr := range b.ctx.times {
		if e, ok := expr.Expr.(*plan.Expr_Col); ok {
			if e.Col.Name == TimeWindowStart {
				forgeColCnt++
			}
			if e.Col.Name == TimeWindowEnd {
				forgeColCnt++
			}
		}
	}

	colPos := int32(len(b.ctx.times))
	aggColPos := colPos - forgeColCnt

	expr := DeepCopyExpr(b.ctx.aggregates[aggColPos])
	outerFn := expr.Expr.(*plan.Expr_F).F
	outerFn.Args = []*plan.Expr{
		{
			Typ: b.ctx.aggregates[aggColPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: b.ctx.aggregateTag,
					ColPos: aggColPos,
				},
			},
		},
	}
	// The outer time-window aggregate consumes the inner aggregate result, so
	// argument-layout-dependent configuration from the inner aggregate cannot
	// be reused.
	outerFn.AggConfig = nil
	outerFn.AggConfigType = plan.AggregateConfigType_AGG_CONFIG_NONE
	if b.ctx.sliding {
		expr, err = b.remapAggToTimeWindowResultAgg(expr)
		if err != nil {
			return nil, err
		}
	}
	b.ctx.times = append(b.ctx.times, expr)

	astStr := semanticAstKey(astExpr)
	b.ctx.timeByAst[astStr] = colPos

	return makeTimeWindowProjectionExpr(b.GetContext(), b.ctx, astExpr, colPos)
}
