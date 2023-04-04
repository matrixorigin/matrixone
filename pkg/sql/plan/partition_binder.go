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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"strings"
)

func NewPartitionBinder(builder *QueryBuilder, ctx *BindContext) *PartitionBinder {
	p := &PartitionBinder{}
	p.sysCtx = builder.GetContext()
	p.builder = builder
	p.ctx = ctx
	p.impl = p
	return p
}

// From: https://dev.mysql.com/doc/refman/8.0/en/partitioning-limitations-functions.html#partitioning-limitations-ceiling-floor
var supportedFunctionsInPartitionExpr = map[string]int{
	"abs":            0,
	"ceiling":        0, //TODO: double check
	"ceil":           0, //TODO: double check
	"datediff":       0,
	"day":            0,
	"dayofmonth":     0,
	"dayofweek":      0,
	"dayofyear":      0,
	"extract":        0,
	"floor":          0, //TODO: double check
	"hour":           0,
	"microsecond":    0,
	"minute":         0,
	"mod":            0,
	"month":          0,
	"quarter":        0,
	"second":         0,
	"time_to_sec":    0,
	"to_days":        0,
	"to_seconds":     0,
	"unix_timestamp": 0,
	"weekday":        0,
	"year":           0,
	"yearweek":       0,
	"hash_value":     0,
}

func functionIsSupported(funcName string) bool {
	if _, ok := supportedFunctionsInPartitionExpr[funcName]; ok {
		return ok
	}
	return false
}

func (p *PartitionBinder) BindExpr(expr tree.Expr, i int32, b bool) (*plan.Expr, error) {
	/*
		Base on reference:
		https://dev.mysql.com/doc/refman/8.0/en/partitioning-limitations.html
		https://dev.mysql.com/doc/refman/8.0/en/partitioning-limitations-functions.html#partitioning-limitations-ceiling-floor
	*/
	switch exprImpl := expr.(type) {
	case *tree.BinaryExpr:
		//unsupported operator
		switch exprImpl.Op {
		case tree.PLUS, tree.MINUS, tree.MULTI, tree.INTEGER_DIV:
			//suggested by Zhang Xiao, only support operator +,-,*,div in version 0.8
		default:
			return nil, moerr.NewInvalidInput(p.GetContext(), "operator %s is not allowed in the partition expression", exprImpl.Op.ToString())
		}
	case *tree.UnaryExpr:
		switch exprImpl.Op {
		case tree.UNARY_MINUS, tree.UNARY_PLUS:
		default:
			return nil, moerr.NewInvalidInput(p.GetContext(), "operator %s is not allowed in the partition expression", exprImpl.Op.ToString())
		}
	case *tree.FuncExpr:
		//supported function
		funcRef, ok := exprImpl.Func.FunctionReference.(*tree.UnresolvedName)
		if !ok {
			return nil, moerr.NewNYI(p.GetContext(), "invalid function expr '%v'", exprImpl)
		}
		funcName := strings.ToLower(funcRef.Parts[0])
		if !functionIsSupported(funcName) {
			return nil, moerr.NewInvalidInput(p.GetContext(), "function %s is not allowed in the partition expression", funcName)
		}
	}
	return p.baseBindExpr(expr, i, b)
}

func (p *PartitionBinder) BindColRef(name *tree.UnresolvedName, i int32, b bool) (*plan.Expr, error) {
	return p.baseBindColRef(name, i, b)
}

func (p *PartitionBinder) BindAggFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError(p.GetContext(), "aggregate functions not allowed in partition clause")
}

func (p *PartitionBinder) BindWinFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError(p.GetContext(), "window functions not allowed in partition clause")
}

func (p *PartitionBinder) BindSubquery(subquery *tree.Subquery, b bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError(p.GetContext(), "subquery not allowed in partition clause")
}
