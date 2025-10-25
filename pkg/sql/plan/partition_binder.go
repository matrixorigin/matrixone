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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
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

	// Check if condition1 is MAXVALUE
	_, isMaxValue := condition1.(*tree.MaxValue)

	// Validate ordering and MAXVALUE position
	// - MAXVALUE must be the last partition
	// - values must be strictly increasing: prev < curr
	if isMaxValue && position != len(option.Partitions)-1 {
		return nil, moerr.NewInvalidInput(ctx, "MAXVALUE must be the last RANGE partition")
	}
	if position > 0 {
		if _, prevIsMax := condition2.(*tree.MaxValue); prevIsMax {
			return nil, moerr.NewInvalidInput(ctx, "MAXVALUE can only appear in the last RANGE partition")
		}
		if !isMaxValue {
			// check strictly increasing: condition2 < condition1
			lt := tree.NewComparisonExpr(tree.LESS_THAN, condition2, condition1)
			boundExpr, err := p.BindExpr(lt, 0, false)
			if err != nil {
				return nil, err
			}

			exec, err := colexec.NewExpressionExecutor(p.builder.compCtx.GetProcess(), boundExpr)
			if err != nil {
				return nil, err
			}
			vec, err := exec.Eval(p.builder.compCtx.GetProcess(), []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			defer exec.Free()
			if err != nil {
				return nil, err
			}
			bs := vector.MustFixedColWithTypeCheck[bool](vec)
			// constant expression, expect single row true
			if len(bs) == 0 || !bs[0] {
				return nil, moerr.NewInvalidInput(ctx, "VALUES LESS THAN value must be strictly increasing for each partition")
			}
		}
	}

	// col < v0
	// v0 <= col < v1
	// v1 <= col < v2
	// v2 <= col (for MAXVALUE partition)
	if position == 0 {
		if isMaxValue {
			// First partition with MAXVALUE: all values go here
			expr = tree.NewComparisonExpr(tree.LESS_THAN, column, condition1)
		} else {
			expr = tree.NewComparisonExpr(tree.LESS_THAN, column, condition1)
		}
	} else {
		if isMaxValue {
			// Last partition with MAXVALUE: column >= condition2
			expr = tree.NewComparisonExpr(tree.GREAT_THAN_EQUAL, column, condition2)
		} else {
			left := tree.NewComparisonExpr(tree.GREAT_THAN_EQUAL, column, condition2)
			right := tree.NewComparisonExpr(tree.LESS_THAN, column, condition1)
			expr = tree.NewAndExpr(left, right)
		}
	}

	planExpr, err := p.BindExpr(expr, 0, false)
	if err != nil {
		return nil, err
	}
	return planExpr, nil
}

func (p *PartitionBinder) constructListExpression(
	ctx context.Context,
	option *tree.PartitionOption,
	column tree.Expr,
	position int,
) (*plan.Expr, error) {

	valuesIn := option.Partitions[position].Values.(*tree.ValuesIn)
	if len(valuesIn.ValueList) == 0 {
		return nil, moerr.NewInvalidInput(ctx, "LIST PARTITIONING must have at least one value")
	}

	// helper to evaluate constant equality between two value expressions
	constEqual := func(a, b tree.Expr) (bool, error) {
		eq := tree.NewComparisonExpr(tree.EQUAL, a, b)
		bound, err := p.BindExpr(eq, 0, false)
		if err != nil {
			return false, err
		}
		exec, err := colexec.NewExpressionExecutor(p.builder.compCtx.GetProcess(), bound)
		if err != nil {
			return false, err
		}
		vec, err := exec.Eval(p.builder.compCtx.GetProcess(), []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
		defer exec.Free()
		if err != nil {
			return false, err
		}
		bs := vector.MustFixedColWithTypeCheck[bool](vec)
		if len(bs) == 0 {
			return false, nil
		}
		return bs[0], nil
	}

	// 1) ensure no duplicates within the same partition's value list
	for i := 0; i < len(valuesIn.ValueList); i++ {
		for j := 0; j < i; j++ {
			eq, err := constEqual(valuesIn.ValueList[i], valuesIn.ValueList[j])
			if err != nil {
				return nil, err
			}
			if eq {
				return nil, moerr.NewInvalidInput(ctx, "duplicate values within the same LIST partition are not allowed")
			}
		}
	}

	// 2) ensure no overlapping values with previous partitions
	for prev := 0; prev < position; prev++ {
		prevValuesIn := option.Partitions[prev].Values.(*tree.ValuesIn)
		for _, pv := range prevValuesIn.ValueList {
			for _, cv := range valuesIn.ValueList {
				eq, err := constEqual(pv, cv)
				if err != nil {
					return nil, err
				}
				if eq {
					return nil, moerr.NewInvalidInput(ctx, "LIST PARTITIONING values must be unique across partitions")
				}
			}
		}
	}

	// 3) build expression: expr IN (...)
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
	// 1) hash(column)
	// function name should be provided as unresolved column name token
	hashName := tree.NewUnresolvedColName("hash_partition")
	expr = &tree.FuncExpr{
		Func:  tree.FuncName2ResolvableFunctionReference(hashName),
		Exprs: tree.Exprs{hashType.Expr},
	}
	// 2) hash(column) % num
	name := tree.NewUnresolvedColName("%")
	// hash() returns uint64, keep modulo constant as uint64 to avoid cross-type issues
	arg2 := tree.NewNumVal(num, fmt.Sprintf("%d", num), false, tree.P_uint64)
	expr = &tree.FuncExpr{
		Func:  tree.FuncName2ResolvableFunctionReference(name),
		Exprs: tree.Exprs{expr, arg2},
	}
	// 3) hash(column) % num = position
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
	// 1) hash(column)
	// function name should be provided as unresolved column name token
	hashName := tree.NewUnresolvedColName("hash_partition")
	expr = &tree.FuncExpr{
		Func:  tree.FuncName2ResolvableFunctionReference(hashName),
		Exprs: tree.Exprs{keyType.ColumnList[0]},
	}
	// 2) hash(column) % num
	name := tree.NewUnresolvedColName("%")
	// hash() returns uint64, keep modulo constant as uint64 to avoid cross-type issues
	arg2 := tree.NewNumVal(num, fmt.Sprintf("%d", num), false, tree.P_uint64)
	expr = &tree.FuncExpr{
		Func:  tree.FuncName2ResolvableFunctionReference(name),
		Exprs: tree.Exprs{expr, arg2},
	}
	// 3) hash(column) % num = position
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
