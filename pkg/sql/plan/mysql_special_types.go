// Copyright 2021 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func isEnumPlanType(typ *plan.Type) bool {
	return typ != nil && typ.Id == int32(types.T_enum) && len(typ.GetEnumvalues()) > 0
}

// isSetPlanType identifies a SET column. SET is stored as T_uint64 with a non-empty
// Enumvalues field holding the comma-separated member list. This is the sole
// discriminator between a plain uint64 column and a SET column — no other code
// path should populate Enumvalues on a T_uint64 type.
func isSetPlanType(typ *plan.Type) bool {
	return typ != nil && typ.Id == int32(types.T_uint64) && len(typ.GetEnumvalues()) > 0
}

func isEnumOrSetPlanType(typ *plan.Type) bool {
	return isEnumPlanType(typ) || isSetPlanType(typ)
}

func mysqlSpecialTypeFuncNames(typ *plan.Type) (string, string, string, error) {
	switch {
	case isEnumPlanType(typ):
		return moEnumCastIndexToValueFun, moEnumCastValueToIndexFun, moEnumCastIndexValueToIndexFun, nil
	case isSetPlanType(typ):
		return moSetCastIndexToValueFun, moSetCastValueToIndexFun, moSetCastIndexValueToIndexFun, nil
	default:
		return "", "", "", moerr.NewInternalErrorNoCtx("not enum/set type")
	}
}

func wrapAstExprForMySQLSpecialType(ctx context.Context, targetType plan.Type, astExpr tree.Expr) (tree.Expr, error) {
	if !isEnumOrSetPlanType(&targetType) {
		return astExpr, nil
	}

	binder := NewDefaultBinder(ctx, nil, nil, targetType, nil)
	boundExpr, err := binder.BindExpr(astExpr, 0, false)
	if err != nil {
		return nil, err
	}

	_, valueToIndex, indexValueToIndex, err := mysqlSpecialTypeFuncNames(&targetType)
	if err != nil {
		return nil, err
	}

	funcName := valueToIndex
	if types.T(boundExpr.Typ.Id).IsInteger() {
		funcName = indexValueToIndex
	}

	return &tree.FuncExpr{
		Func: tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName(funcName)),
		Type: tree.FUNC_TYPE_DEFAULT,
		Exprs: []tree.Expr{
			tree.NewNumVal(targetType.Enumvalues, targetType.Enumvalues, false, tree.P_char),
			astExpr,
		},
	}, nil
}
