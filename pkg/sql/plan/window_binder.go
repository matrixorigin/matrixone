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
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type windowFuncExprBinder interface {
	BindExpr(tree.Expr, int32, bool) (*plan.Expr, error)
	bindFuncExprImplByAstExpr(string, []tree.Expr, int32) (*plan.Expr, error)
	bindPreparedRowsFrameBound(tree.Expr) (*plan.Expr, error)
	makeFrameConstValue(tree.Expr, *plan.Type) (*plan.Expr, error)
	GetContext() context.Context
}

func windowExprAstKey(astExpr tree.Expr) string {
	funcExpr, ok := astExpr.(*tree.FuncExpr)
	if !ok || funcExpr.WindowSpec == nil || funcExpr.WindowSpec.Frame == nil || funcExpr.WindowSpec.HasFrame {
		return semanticAstKey(astExpr)
	}

	funcExprCopy := *funcExpr
	windowSpecCopy := *funcExpr.WindowSpec
	windowSpecCopy.HasFrame = true
	funcExprCopy.WindowSpec = &windowSpecCopy
	return semanticAstKey(&funcExprCopy)
}

func semanticAstKey(astExpr tree.Expr) string {
	display := tree.String(astExpr, dialect.MYSQL)
	identity := tree.StringWithOpts(astExpr, dialect.MYSQL, tree.WithParamExprOffset())
	if identity == display {
		return display
	}
	return identity + "\x00" + display
}

func semanticAstDisplayName(key string) string {
	if separator := strings.LastIndexByte(key, 0); separator >= 0 {
		return key[separator+1:]
	}
	return key
}

func windowFuncAstName(astExpr *tree.FuncExpr) string {
	if astExpr.FuncName != nil {
		return astExpr.FuncName.Origin()
	}
	if funcRef, ok := astExpr.Func.FunctionReference.(*tree.UnresolvedName); ok {
		return funcRef.ColName()
	}
	return "unknown"
}

func findNestedWindowFuncNameInExprs(exprs ...tree.Expr) (string, bool) {
	for _, expr := range exprs {
		if name, ok := findNestedWindowFuncName(expr); ok {
			return name, true
		}
	}
	return "", false
}

func findNestedWindowFuncNameInOrderBy(orderBy tree.OrderBy) (string, bool) {
	for _, order := range orderBy {
		if order == nil {
			continue
		}
		if name, ok := findNestedWindowFuncName(order.Expr); ok {
			return name, true
		}
	}
	return "", false
}

func findNestedWindowFuncName(expr tree.Expr) (string, bool) {
	switch e := expr.(type) {
	case nil:
		return "", false
	case *tree.FuncExpr:
		if e.WindowSpec != nil {
			return windowFuncAstName(e), true
		}
		if name, ok := findNestedWindowFuncNameInExprs(e.Exprs...); ok {
			return name, true
		}
		return findNestedWindowFuncNameInOrderBy(e.OrderBy)
	case *tree.BinaryExpr:
		return findNestedWindowFuncNameInExprs(e.Left, e.Right)
	case *tree.UnaryExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.ComparisonExpr:
		return findNestedWindowFuncNameInExprs(e.Left, e.Right, e.Escape)
	case *tree.AndExpr:
		return findNestedWindowFuncNameInExprs(e.Left, e.Right)
	case *tree.XorExpr:
		return findNestedWindowFuncNameInExprs(e.Left, e.Right)
	case *tree.OrExpr:
		return findNestedWindowFuncNameInExprs(e.Left, e.Right)
	case *tree.NotExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.IsNullExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.IsNotNullExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.IsUnknownExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.IsNotUnknownExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.IsTrueExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.IsNotTrueExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.IsFalseExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.IsNotFalseExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.ParenExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.CastExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.BitCastExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.Tuple:
		return findNestedWindowFuncNameInExprs(e.Exprs...)
	case *tree.RangeCond:
		return findNestedWindowFuncNameInExprs(e.Left, e.From, e.To)
	case *tree.CaseExpr:
		if name, ok := findNestedWindowFuncName(e.Expr); ok {
			return name, true
		}
		for _, when := range e.Whens {
			if when == nil {
				continue
			}
			if name, ok := findNestedWindowFuncNameInExprs(when.Cond, when.Val); ok {
				return name, true
			}
		}
		return findNestedWindowFuncName(e.Else)
	case *tree.IntervalExpr:
		return findNestedWindowFuncName(e.Expr)
	case *tree.DefaultVal:
		return findNestedWindowFuncName(e.Expr)
	case *tree.SerialExtractExpr:
		return findNestedWindowFuncNameInExprs(e.SerialExpr, e.IndexExpr)
	case *tree.Subquery:
		return "", false
	default:
		return "", false
	}
}

func rejectNestedWindowFunc(ctx context.Context, expr tree.Expr) error {
	if name, ok := findNestedWindowFuncName(expr); ok {
		return moerr.NewSyntaxErrorf(ctx, "You cannot use the window function '%s' in this context", name)
	}
	return nil
}

func validateWindowFuncNoNested(ctx context.Context, astExpr *tree.FuncExpr) error {
	for _, arg := range astExpr.Exprs {
		if err := rejectNestedWindowFunc(ctx, arg); err != nil {
			return err
		}
	}
	if name, ok := findNestedWindowFuncNameInOrderBy(astExpr.OrderBy); ok {
		return moerr.NewSyntaxErrorf(ctx, "You cannot use the window function '%s' in this context", name)
	}

	ws := astExpr.WindowSpec
	if ws == nil {
		return nil
	}
	for _, group := range ws.PartitionBy {
		if err := rejectNestedWindowFunc(ctx, group); err != nil {
			return err
		}
	}
	if name, ok := findNestedWindowFuncNameInOrderBy(ws.OrderBy); ok {
		return moerr.NewSyntaxErrorf(ctx, "You cannot use the window function '%s' in this context", name)
	}
	if ws.Frame != nil {
		if ws.Frame.Start != nil {
			if err := rejectNestedWindowFunc(ctx, ws.Frame.Start.Expr); err != nil {
				return err
			}
		}
		if ws.Frame.End != nil {
			if err := rejectNestedWindowFunc(ctx, ws.Frame.End.Expr); err != nil {
				return err
			}
		}
	}
	return nil
}

func rejectWindowResultDependency(ctx context.Context, expr *plan.Expr, windowTag int32) error {
	if HasTag(expr, windowTag) {
		return moerr.NewSyntaxError(ctx, "You cannot use a window function result in another window function in the same query block")
	}
	return nil
}

func bindWindowFuncExpr(b windowFuncExprBinder, ctx *BindContext, funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	if astExpr.Type == tree.FUNC_TYPE_DISTINCT {
		return nil, moerr.NewNYI(b.GetContext(), "DISTINCT in window function")
	}

	if err := validateCountArgs(b.GetContext(), funcName, astExpr); err != nil {
		return nil, err
	}
	if err := validateWindowFuncNoNested(b.GetContext(), astExpr); err != nil {
		return nil, err
	}
	if len(astExpr.OrderBy) > 0 {
		return nil, moerr.NewNYI(b.GetContext(), "function-local ORDER BY in window function")
	}

	astStr := windowExprAstKey(astExpr)

	w := &plan.WindowSpec{}
	ws := astExpr.WindowSpec
	if ws != nil {
		wsCopy := *ws
		ws = &wsCopy
	}

	// window function
	windowFunc, err := b.bindFuncExprImplByAstExpr(funcName, astExpr.Exprs, depth)
	if err != nil {
		return nil, err
	}
	if err = rejectWindowResultDependency(b.GetContext(), windowFunc, ctx.windowTag); err != nil {
		return nil, err
	}
	w.WindowFunc = windowFunc
	w.Name = funcName

	isWinValueFunc := function.GetFunctionIsWinValueFunByName(funcName)
	if isWinValueFunc && !ws.HasFrame {
		ws.Frame = &tree.FrameClause{Type: tree.Rows}
		ws.Frame.Start = &tree.FrameBound{Type: tree.Preceding, UnBounded: true}
		ws.Frame.End = &tree.FrameBound{Type: tree.Following, UnBounded: true}
	}

	for _, group := range ws.PartitionBy {
		expr, err := b.BindExpr(group, depth, isRoot)
		if err != nil {
			return nil, err
		}
		if err = rejectWindowResultDependency(b.GetContext(), expr, ctx.windowTag); err != nil {
			return nil, err
		}
		w.PartitionBy = append(w.PartitionBy, expr)
	}

	if ws.OrderBy != nil {
		w.OrderBy = make([]*plan.OrderBySpec, 0, len(ws.OrderBy))
		for _, order := range ws.OrderBy {
			expr, err := b.BindExpr(order.Expr, depth, isRoot)
			if err != nil {
				return nil, err
			}
			if err = rejectWindowResultDependency(b.GetContext(), expr, ctx.windowTag); err != nil {
				return nil, err
			}

			// Keep enum/set window ordering aligned with definition order.
			if fn := expr.GetF(); fn != nil &&
				(fn.Func.ObjName == moEnumCastIndexToValueFun || fn.Func.ObjName == moSetCastIndexToValueFun) {
				expr = fn.Args[1]
			}

			orderBy := &plan.OrderBySpec{
				Expr: expr,
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

			w.OrderBy = append(w.OrderBy, orderBy)
		}
	}

	switch ws.Frame.Start.Type {
	case tree.Following:
		if ws.Frame.Start.UnBounded {
			return nil, moerr.NewParseError(b.GetContext(), "Window '<unnamed window>': frame start cannot be UNBOUNDED FOLLOWING.")
		}
		if ws.Frame.End.Type == tree.Preceding || ws.Frame.End.Type == tree.CurrentRow {
			return nil, moerr.NewParseError(b.GetContext(), "Window '<unnamed window>': frame start or end is negative, NULL or of non-integral type")
		}
	case tree.CurrentRow:
		if ws.Frame.End.Type == tree.Preceding {
			return nil, moerr.NewParseError(b.GetContext(), "Window '<unnamed window>': frame start or end is negative, NULL or of non-integral type")
		}
	}

	if ws.Frame.End.Type == tree.Preceding && ws.Frame.End.UnBounded {
		return nil, moerr.NewParseError(b.GetContext(), "Window '<unnamed window>': frame end cannot be UNBOUNDED PRECEDING.")
	}

	w.Frame = &plan.FrameClause{
		Type: plan.FrameClause_FrameType(ws.Frame.Type),
		Start: &plan.FrameBound{
			Type:      plan.FrameBound_BoundType(ws.Frame.Start.Type),
			UnBounded: ws.Frame.Start.UnBounded,
		},
		End: &plan.FrameBound{
			Type:      plan.FrameBound_BoundType(ws.Frame.End.Type),
			UnBounded: ws.Frame.End.UnBounded,
		},
	}
	var typ *plan.Type
	switch ws.Frame.Type {
	case tree.Rows:
		typ = &plan.Type{Id: int32(types.T_uint64)}
	case tree.Range:
		if len(w.OrderBy) != 1 && isNRange(ws.Frame) {
			return nil, moerr.NewParseError(b.GetContext(), "Window '<unnamed window>' with RANGE N PRECEDING/FOLLOWING frame requires exactly one ORDER BY expression, of numeric or temporal type")
		}
		if len(w.OrderBy) == 0 {
			break
		}
		typ = &w.OrderBy[0].Expr.Typ
		t := types.Type{Oid: types.T(typ.Id)}
		if !function.GetFunctionIsWinOrderFunByName(funcName) && !t.IsNumericOrTemporal() {
			return nil, moerr.NewParseError(b.GetContext(), "Window '<unnamed window>' with RANGE frame requires ORDER BY expression of numeric or temporal type")
		}
	case tree.Groups:
		return nil, moerr.NewNYI(b.GetContext(), "GROUPS in WINDOW FUNCTION condition")
	}
	if isPreparedWindowIntervalBound(ws.Frame.Start.Expr) || isPreparedWindowIntervalBound(ws.Frame.End.Expr) {
		return nil, moerr.NewNotSupported(b.GetContext(), "prepared parameter markers in interval window frames")
	}
	if ws.Frame.Type == tree.Range &&
		(isWindowFrameParam(ws.Frame.Start.Expr) || isWindowFrameParam(ws.Frame.End.Expr)) {
		return nil, moerr.NewNotSupported(b.GetContext(), "prepared parameter markers in RANGE window frames")
	}
	if ws.Frame.Start.Expr != nil {
		if isWindowFrameParam(ws.Frame.Start.Expr) {
			w.Frame.Start.Val, err = b.bindPreparedRowsFrameBound(ws.Frame.Start.Expr)
		} else {
			w.Frame.Start.Val, err = b.makeFrameConstValue(ws.Frame.Start.Expr, typ)
		}
		if err != nil {
			return nil, err
		}
		if err = rejectWindowResultDependency(b.GetContext(), w.Frame.Start.Val, ctx.windowTag); err != nil {
			return nil, err
		}
	}
	if ws.Frame.End.Expr != nil {
		if isWindowFrameParam(ws.Frame.End.Expr) {
			w.Frame.End.Val, err = b.bindPreparedRowsFrameBound(ws.Frame.End.Expr)
		} else {
			w.Frame.End.Val, err = b.makeFrameConstValue(ws.Frame.End.Expr, typ)
		}
		if err != nil {
			return nil, err
		}
		if err = rejectWindowResultDependency(b.GetContext(), w.Frame.End.Val, ctx.windowTag); err != nil {
			return nil, err
		}
	}

	if colPos, ok := ctx.windowByAst[astStr]; ok {
		return buildWindowColRefExpr(ctx, ctx.windows[colPos].Typ, colPos), nil
	}

	colPos := int32(len(ctx.windows))
	ctx.windows = append(ctx.windows, &plan.Expr{
		Typ:  w.WindowFunc.Typ,
		Expr: &plan.Expr_W{W: w},
	})
	ctx.windowByAst[astStr] = colPos

	return buildWindowColRefExpr(ctx, w.WindowFunc.Typ, colPos), nil
}

func isWindowFrameParam(expr tree.Expr) bool {
	_, ok := expr.(*tree.ParamExpr)
	return ok
}

func isPreparedWindowIntervalBound(expr tree.Expr) bool {
	interval, ok := expr.(*tree.FuncExpr)
	if !ok {
		return false
	}
	return hasWindowFrameParam(interval)
}

func hasWindowFrameParam(expr tree.Expr) bool {
	switch expr := expr.(type) {
	case *tree.ParamExpr:
		return true
	case *tree.FuncExpr:
		for _, arg := range expr.Exprs {
			if hasWindowFrameParam(arg) {
				return true
			}
		}
	case *tree.ParenExpr:
		return hasWindowFrameParam(expr.Expr)
	case *tree.UnaryExpr:
		return hasWindowFrameParam(expr.Expr)
	case *tree.CastExpr:
		return hasWindowFrameParam(expr.Expr)
	}
	return false
}

func (b *baseBinder) bindPreparedRowsFrameBound(expr tree.Expr) (*plan.Expr, error) {
	if b.builder == nil || !b.builder.isPrepareStatement {
		return nil, moerr.NewInvalidInput(b.GetContext(), "only prepare statement can use ? expr")
	}
	bound, err := b.impl.BindExpr(expr, 0, true)
	if err != nil {
		return nil, err
	}
	typ := types.T_uint64.ToType()
	return appendCastBeforeExpr(b.GetContext(), bound, makePlan2Type(&typ))
}

func buildWindowColRefExpr(ctx *BindContext, typ plan.Type, colPos int32) *plan.Expr {
	return &plan.Expr{
		Typ: typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: ctx.windowTag,
				ColPos: colPos,
			},
		},
	}
}

func makeWindowFrameConstValue(
	baseBindExpr func(tree.Expr, int32, bool) (*Expr, error),
	proc *process.Process,
	bindCtx context.Context,
	expr tree.Expr,
	typ *plan.Type,
) (*plan.Expr, error) {
	e, err := baseBindExpr(expr, 0, true)
	if err != nil {
		return nil, err
	}
	if e.Typ.Id == int32(types.T_interval) {
		return resetWindowIntervalExpr(bindCtx, proc, e)
	}
	if typ == nil {
		return e, nil
	}
	e, err = appendCastBeforeExpr(bindCtx, e, *typ)
	if err != nil {
		return nil, err
	}

	executor, err := colexec.NewExpressionExecutor(proc, e)
	if err != nil {
		return nil, err
	}
	defer executor.Free()
	vec, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return nil, err
	}
	c := rule.GetConstantValue(vec, false, 0)

	return &plan.Expr{
		Typ:  *typ,
		Expr: &plan.Expr_Lit{Lit: c},
	}, nil
}

func resetWindowIntervalExpr(bindCtx context.Context, proc *process.Process, e *Expr) (*Expr, error) {
	e1 := e.Expr.(*plan.Expr_List).List.List[0]
	e2 := e.Expr.(*plan.Expr_List).List.List[1]

	intervalTypeStr := e2.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Sval).Sval
	intervalType, err := types.IntervalTypeOf(intervalTypeStr)
	if err != nil {
		return nil, err
	}

	if e1.Typ.Id == int32(types.T_varchar) || e1.Typ.Id == int32(types.T_char) {
		s := e1.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Sval).Sval
		returnNum, returnType, err := types.NormalizeInterval(s, intervalType)
		if err != nil {
			returnNum = math.MaxInt64
			returnType = intervalType
		}

		e.Expr.(*plan.Expr_List).List.List[0] = makePlan2Int64ConstExprWithType(returnNum)
		e.Expr.(*plan.Expr_List).List.List[1] = makePlan2Int64ConstExprWithType(int64(returnType))
		return e, nil
	}

	isTimeUnit := intervalType == types.Second || intervalType == types.Minute ||
		intervalType == types.Hour || intervalType == types.Day
	isDecimalOrFloat := e1.Typ.Id == int32(types.T_decimal64) ||
		e1.Typ.Id == int32(types.T_decimal128) || e1.Typ.Id == int32(types.T_float32) ||
		e1.Typ.Id == int32(types.T_float64)
	lit := e1.GetLit()
	if isTimeUnit && isDecimalOrFloat && lit != nil && !lit.Isnull {
		var floatVal float64
		var hasValue bool

		if dval, ok := lit.Value.(*plan.Literal_Dval); ok {
			floatVal = dval.Dval
			hasValue = true
		} else if fval, ok := lit.Value.(*plan.Literal_Fval); ok {
			floatVal = float64(fval.Fval)
			hasValue = true
		} else if d64val, ok := lit.Value.(*plan.Literal_Decimal64Val); ok {
			d64 := types.Decimal64(d64val.Decimal64Val.A)
			scale := e1.Typ.Scale
			if scale < 0 {
				scale = 0
			}
			floatVal = types.Decimal64ToFloat64(d64, scale)
			hasValue = true
		} else if d128val, ok := lit.Value.(*plan.Literal_Decimal128Val); ok {
			d128 := types.Decimal128{B0_63: uint64(d128val.Decimal128Val.A), B64_127: uint64(d128val.Decimal128Val.B)}
			scale := e1.Typ.Scale
			if scale < 0 {
				scale = 0
			}
			floatVal = types.Decimal128ToFloat64(d128, scale)
			hasValue = true
		}

		if hasValue {
			var finalValue int64
			switch intervalType {
			case types.Second:
				finalValue = int64(math.Round(floatVal * float64(types.MicroSecsPerSec)))
			case types.Minute:
				finalValue = int64(math.Round(floatVal * float64(types.MicroSecsPerSec*types.SecsPerMinute)))
			case types.Hour:
				finalValue = int64(math.Round(floatVal * float64(types.MicroSecsPerSec*types.SecsPerHour)))
			case types.Day:
				finalValue = int64(math.Round(floatVal * float64(types.MicroSecsPerSec*types.SecsPerDay)))
			}
			e.Expr.(*plan.Expr_List).List.List[0] = makePlan2Int64ConstExprWithType(finalValue)
			e.Expr.(*plan.Expr_List).List.List[1] = makePlan2Int64ConstExprWithType(int64(types.MicroSecond))
			return e, nil
		}
	}

	typ := &plan.Type{Id: int32(types.T_int64)}
	numberExpr, err := appendCastBeforeExpr(bindCtx, e1, *typ)
	if err != nil {
		return nil, err
	}

	executor, err := colexec.NewExpressionExecutor(proc, numberExpr)
	if err != nil {
		return nil, err
	}
	defer executor.Free()
	vec, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return nil, err
	}
	c := rule.GetConstantValue(vec, false, 0)

	var finalValue int64
	if c.Isnull {
		finalValue = math.MaxInt64
	} else if ival, ok := c.Value.(*plan.Literal_I64Val); ok {
		finalValue = ival.I64Val
	} else {
		return nil, moerr.NewInvalidInput(bindCtx, "invalid interval value")
	}

	e.Expr.(*plan.Expr_List).List.List[0] = makePlan2Int64ConstExprWithType(finalValue)
	e.Expr.(*plan.Expr_List).List.List[1] = makePlan2Int64ConstExprWithType(int64(intervalType))

	return e, nil
}
