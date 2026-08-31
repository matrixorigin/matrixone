// Copyright 2026 Matrix Origin
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

package plan

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// INTERVAL is an internal (value, unit) expression list rather than a scalar
// value that an executor may materialize. These functions are the complete set
// of generic binder entries that consume that representation and rewrite it to
// ordinary scalar arguments before publishing a plan expression.
func consumesIntervalPseudoType(name string) bool {
	switch strings.ToLower(name) {
	case "date_add", "date_sub", "adddate", "subdate",
		"+", "-",
		"mo_win_truncate", "mo_win_divisor",
		"uuid", "uuid_v1", "uuid_v6", "uuid_v7":
		return true
	default:
		return false
	}
}

func rejectStandaloneIntervalFunctionArgs(
	ctx context.Context,
	name string,
	args []*planpb.Expr,
) error {
	return rejectIntervalFunctionArgs(ctx, name, args, true)
}

// rejectBoundIntervalFunctionArgs validates children produced by the AST
// binder. Nested function expressions have already passed this same boundary,
// so stopping at them keeps deeply nested ordinary expressions linear to bind.
// Structural containers still need recursion because tuple/subquery/window
// nodes can publish children without a generic function call of their own.
func rejectBoundIntervalFunctionArgs(
	ctx context.Context,
	name string,
	args []*planpb.Expr,
) error {
	return rejectIntervalFunctionArgs(ctx, name, args, false)
}

func rejectIntervalFunctionArgs(
	ctx context.Context,
	name string,
	args []*planpb.Expr,
	descendFunctions bool,
) error {
	for _, arg := range args {
		if containsStandaloneIntervalExprInternal(arg, descendFunctions) {
			return moerr.NewNotSupportedf(
				ctx,
				"standalone INTERVAL expression in %s argument",
				strings.ToUpper(name),
			)
		}
	}
	return nil
}

// containsStandaloneIntervalExpr follows executable scalar containers. It does
// not follow Literal.Src, which is provenance rather than an evaluated child,
// or window frame values, where the frame binder has already consumed and
// normalized the interval representation.
func containsStandaloneIntervalExpr(expr *planpb.Expr) bool {
	return containsStandaloneIntervalExprInternal(expr, true)
}

func containsStandaloneIntervalExprInternal(expr *planpb.Expr, descendFunctions bool) bool {
	if expr == nil {
		return false
	}
	if expr.Typ.Id == int32(types.T_interval) {
		return true
	}

	switch item := expr.Expr.(type) {
	case *planpb.Expr_F:
		if !descendFunctions || item.F == nil {
			return false
		}
		for _, arg := range item.F.Args {
			if containsStandaloneIntervalExprInternal(arg, true) {
				return true
			}
		}
	case *planpb.Expr_List:
		if item.List == nil {
			return false
		}
		for _, child := range item.List.List {
			if containsStandaloneIntervalExprInternal(child, descendFunctions) {
				return true
			}
		}
	case *planpb.Expr_Sub:
		return item.Sub != nil && containsStandaloneIntervalExprInternal(item.Sub.Child, descendFunctions)
	case *planpb.Expr_W:
		if item.W == nil {
			return false
		}
		if containsStandaloneIntervalExprInternal(item.W.WindowFunc, descendFunctions) {
			return true
		}
		for _, partition := range item.W.PartitionBy {
			if containsStandaloneIntervalExprInternal(partition, descendFunctions) {
				return true
			}
		}
		for _, order := range item.W.OrderBy {
			if order != nil && containsStandaloneIntervalExprInternal(order.Expr, descendFunctions) {
				return true
			}
		}
	}

	return false
}

func rejectStandaloneIntervalOrderExpr(ctx context.Context, expr *planpb.Expr) error {
	return rejectStandaloneIntervalExpr(ctx, expr, "ORDER BY")
}

func rejectStandaloneIntervalExpr(ctx context.Context, expr *planpb.Expr, clause string) error {
	if containsStandaloneIntervalExpr(expr) {
		return moerr.NewNotSupportedf(ctx, "standalone INTERVAL expression in %s", clause)
	}
	return nil
}
