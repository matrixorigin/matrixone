// Copyright 2021-2024 Matrix Origin
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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewPartitionBinder(builder *QueryBuilder, ctx *BindContext) *PartitionBinder {
	p := &PartitionBinder{}
	p.sysCtx = builder.GetContext()
	p.builder = builder
	p.ctx = ctx
	p.impl = p
	return p
}

func (p *PartitionBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	return p.baseBindExpr(astExpr, depth, isRoot)
}

func (p *PartitionBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	return p.baseBindColRef(astExpr, depth, isRoot)
}

func (p *PartitionBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(p.GetContext(), "cannot bind agregate functions '%s'", funcName)
}

func (p *PartitionBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(p.GetContext(), "cannot bind window functions '%s'", funcName)
}

func (p *PartitionBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewNYI(p.GetContext(), "subquery in JOIN condition")
}

func (p *PartitionBinder) BindTimeWindowFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(p.GetContext(), "cannot bind time window functions '%s'", funcName)
}

func (p *PartitionBinder) buildPartitionDefs(ctx context.Context, option *tree.PartitionOption) (*plan.Partition, error) {
	var defs []*plan.PartitionDef
	switch method := option.PartBy.PType.(type) {
	case *tree.HashType:
		num := option.PartBy.Num
		defs = make([]*plan.PartitionDef, int(num))
		for i := uint64(0); i < num; i++ {
			expr, err := p.constructHashExpression(
				ctx,
				num,
				method,
				i,
			)
			if err != nil {
				return nil, err
			}
			defs[i] = &plan.PartitionDef{Def: expr}
		}
	case *tree.KeyType:
		num := option.PartBy.Num
		defs = make([]*plan.PartitionDef, int(num))
		for i := uint64(0); i < num; i++ {
			expr, err := p.constructKeyExpression(
				ctx,
				num,
				method,
				i,
			)
			if err != nil {
				return nil, err
			}
			defs[i] = &plan.PartitionDef{Def: expr}
		}
	case *tree.RangeType:
		defs = make([]*plan.PartitionDef, len(option.Partitions))
		var expr tree.Expr
		if len(method.ColumnList) == 0 {
			expr = method.Expr
		} else {
			expr = method.ColumnList[0]
		}
		for i := range option.Partitions {
			expr, err := p.constructRangeExpression(
				ctx,
				option,
				expr,
				i,
			)
			if err != nil {
				return nil, err
			}
			defs[i] = &plan.PartitionDef{Def: expr}
		}
	case *tree.ListType:
		defs = make([]*plan.PartitionDef, len(option.Partitions))
		var expr tree.Expr
		if len(method.ColumnList) == 0 {
			expr = method.Expr
		} else {
			expr = method.ColumnList[0]
		}
		for i := range option.Partitions {
			expr, err := p.constructListExpression(
				ctx,
				option,
				expr,
				i,
			)
			if err != nil {
				return nil, err
			}
			defs[i] = &plan.PartitionDef{Def: expr}
		}
	}
	return &plan.Partition{PartitionDefs: defs}, nil
}

func (p *PartitionBinder) constructRangeExpression(
	ctx context.Context,
	option *tree.PartitionOption,
	column tree.Expr,
	position int,
) (*plan.Expr, error) {

	var condition1, condition2, expr tree.Expr
	valuesLessThan := option.Partitions[position].Values.(*tree.ValuesLessThan)
	if len(valuesLessThan.ValueList) > 1 {
		return nil, moerr.NewInternalError(ctx, "RANGE PARTITIONING can only have one parameter")
	}
	condition1 = valuesLessThan.ValueList[0]

	if position > 0 {
		valuesLessThan := option.Partitions[position-1].Values.(*tree.ValuesLessThan)
		if len(valuesLessThan.ValueList) > 1 {
			return nil, moerr.NewInternalError(ctx, "RANGE PARTITIONING can only have one parameter")
		}
		condition2 = valuesLessThan.ValueList[0]
	}

	// col < p0
	// p0 <= col < p1
	// p1 <= col < p2
	if position == 0 {
		expr = tree.NewComparisonExpr(tree.LESS_THAN, column, condition1)
	} else {
		left := tree.NewComparisonExpr(tree.GREAT_THAN_EQUAL, column, condition2)
		right := tree.NewComparisonExpr(tree.LESS_THAN, column, condition1)
		expr = tree.NewAndExpr(left, right)
	}

	planExpr, err := p.BindExpr(expr, 0, false)
	if err != nil {
		return nil, err
	}
	return planExpr, nil
}

func (p *PartitionBinder) constructListExpression(
	_ context.Context,
	option *tree.PartitionOption,
	column tree.Expr,
	position int,
) (*plan.Expr, error) {

	// expr in (...)
	valuesIn := option.Partitions[position].Values.(*tree.ValuesIn)
	expr := tree.NewComparisonExpr(tree.IN, column, tree.NewTuple(valuesIn.ValueList))

	planExpr, err := p.BindExpr(expr, 0, false)
	if err != nil {
		return nil, err
	}
	return planExpr, nil
}

func (p *PartitionBinder) constructHashExpression(
	_ context.Context,
	num uint64,
	hashType *tree.HashType,
	position uint64,
) (*plan.Expr, error) {

	var expr tree.Expr
	// expr % num
	name := tree.NewUnresolvedColName("%")
	arg2 := tree.NewNumVal(num, fmt.Sprintf("%d", num), false, tree.P_uint64)
	expr = &tree.FuncExpr{
		Func:  tree.FuncName2ResolvableFunctionReference(name),
		Exprs: tree.Exprs{hashType.Expr, arg2},
	}
	// expr % num = partition
	name = tree.NewUnresolvedColName("=")
	arg2 = tree.NewNumVal(position, fmt.Sprintf("%d", position), false, tree.P_uint64)
	expr = &tree.FuncExpr{
		Func:  tree.FuncName2ResolvableFunctionReference(name),
		Exprs: tree.Exprs{expr, arg2},
	}

	planExpr, err := p.BindExpr(expr, 0, false)
	if err != nil {
		return nil, err
	}
	return planExpr, nil
}

func (p *PartitionBinder) constructKeyExpression(
	_ context.Context,
	num uint64,
	keyType *tree.KeyType,
	position uint64,
) (*plan.Expr, error) {

	var expr tree.Expr
	name := tree.NewUnresolvedColName("%")
	arg2 := tree.NewNumVal(num, fmt.Sprintf("%d", num), false, tree.P_uint64)
	expr = &tree.FuncExpr{
		Func:  tree.FuncName2ResolvableFunctionReference(name),
		Exprs: tree.Exprs{keyType.ColumnList[0], arg2},
	}

	name = tree.NewUnresolvedColName("=")
	arg2 = tree.NewNumVal(position, fmt.Sprintf("%d", position), false, tree.P_uint64)
	expr = &tree.FuncExpr{
		Func:  tree.FuncName2ResolvableFunctionReference(name),
		Exprs: tree.Exprs{expr, arg2},
	}

	planExpr, err := p.BindExpr(expr, 0, false)
	if err != nil {
		return nil, err
	}
	return planExpr, nil
}
