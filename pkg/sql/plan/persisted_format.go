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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// bindPersistedExpr selects catalog FORMAT semantics before argument binding.
// A post-binding bridge alone cannot recover source domains rejected by the
// transient integer precision contract. Keep the mode scoped to this authoring
// operation; nested non-FORMAT consumers retain their own argument contracts.
func (b *baseBinder) bindPersistedExpr(ast tree.Expr, depth int32, isRoot bool) (*planpb.Expr, error) {
	previous := b.persistedFormatCompatibility
	b.persistedFormatCompatibility = true
	defer func() { b.persistedFormatCompatibility = previous }()
	return b.impl.BindExpr(ast, depth, isRoot)
}

func (b *baseBinder) bindPersistedFormat(astArgs []tree.Expr, depth int32) (*planpb.Expr, error) {
	ctx := b.GetContext()
	if len(astArgs) < 2 || len(astArgs) > 3 {
		return nil, moerr.NewInvalidInput(ctx, "format function has invalid argument count")
	}
	varchar := planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
	args := make([]*planpb.Expr, len(astArgs))
	for i, ast := range astArgs {
		source, err := b.impl.BindExpr(ast, depth, false)
		if err != nil {
			return nil, err
		}
		// Only precision/locale historically accept date-like values. Do not
		// broaden FORMAT's first-argument domain as part of catalog rebuilding.
		if i == 0 && types.T(source.Typ.Id).IsDateRelate() {
			return nil, moerr.NewInvalidInput(ctx, "format function has invalid first argument type")
		}
		args[i], err = appendCastBeforeExpr(ctx, source, varchar)
		if err != nil {
			return nil, err
		}
	}
	id := function.EncodeOverloadID(function.FORMAT, int32(len(args)-2))
	typ := types.T_varchar.ToType()
	result := makePlan2Type(&typ)
	result.NotNullable = function.DeduceNotNullable(id, args)
	return &planpb.Expr{Typ: result, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: getFunctionObjRef(id, "format"), Args: args,
	}}}, nil
}

// preservePersistedFormatCompatibility keeps catalog expressions executable by
// pre-v59 CNs, including after downgrade. Catalog consumers can evaluate locally
// without crossing the remote pipeline gate. Keep their historical string
// argument and approximate rounding contract regardless of the current protocol;
// only transient query expressions opt into typed numeric FORMAT semantics.
// Call before constant folding so folded and non-folded catalog values agree.
func preservePersistedFormatCompatibility(ctx context.Context, expr *planpb.Expr) error {
	return planpb.VisitExprTree(expr, func(current *planpb.Expr) error {
		fn := current.GetF()
		if fn == nil || fn.Func == nil || len(fn.Args) < 2 || fn.Args[0] == nil {
			return nil
		}
		id, overload := function.DecodeOverloadID(fn.Func.Obj)
		if id != function.FORMAT {
			return nil
		}
		varchar := planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
		if makeTypeByPlan2Expr(fn.Args[0]).IsNumeric() {
			arg, err := appendCastBeforeExpr(ctx, fn.Args[0], varchar)
			if err != nil {
				return err
			}
			fn.Args[0] = arg
		}
		if overload == function.FormatIntegerPrecisionOverload || overload == function.FormatIntegerPrecisionLocaleOverload {
			source, err := persistedFormatPrecisionSource(ctx, fn.Args[1])
			if err != nil {
				return err
			}
			precision, err := appendCastBeforeExpr(ctx, source, varchar)
			if err != nil {
				return err
			}
			fn.Args[1] = precision
			fn.Func.Obj = function.EncodeOverloadID(function.FORMAT, int32(len(fn.Args)-2))
		}
		return nil
	})
}

// Undo only this parameter's integer context. A value-producing function or
// user CAST owns its own semantics; stripping conversions recursively through
// it would silently lower the catalog floor for unrelated integer consumers.
func persistedFormatPrecisionSource(ctx context.Context, expr *planpb.Expr) (*planpb.Expr, error) {
	if isIntegerArgumentCast(expr) {
		return expr.GetF().Args[0], nil
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.SyntaxExplicitCast {
		return expr, nil
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	if id != function.CASE && id != function.IFF {
		return expr, nil
	}
	args := append([]*planpb.Expr(nil), fn.Args...)
	for i := range args {
		if i%2 == 0 && i != len(args)-1 {
			continue
		}
		source, err := persistedFormatPrecisionSource(ctx, args[i])
		if err != nil {
			return nil, err
		}
		args[i] = source
	}
	// Reconcile the recovered source domains, not the old INT64 selector type.
	return BindFuncExprImplByPlanExpr(ctx, fn.Func.ObjName, args)
}
