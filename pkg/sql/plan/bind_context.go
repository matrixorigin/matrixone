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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

func NewBindContext(builder *QueryBuilder, parent *BindContext) *BindContext {
	bc := &BindContext{
		groupByAst:     make(map[string]int32),
		aggregateByAst: make(map[string]int32),
		sampleByAst:    make(map[string]int32),
		projectByExpr:  make(map[string]int32),
		windowByAst:    make(map[string]int32),
		timeByAst:      make(map[string]int32),
		aliasMap:       make(map[string]*aliasItem),
		bindingByTag:   make(map[int32]*Binding),
		bindingByTable: make(map[string]*Binding),
		bindingByCol:   make(map[string]*Binding),
		parent:         parent,
	}
	if parent != nil {
		bc.defaultDatabase = parent.defaultDatabase
		bc.normalCTE = parent.normalCTE
		bc.cteName = parent.cteName
		if parent.recSelect || parent.initSelect || parent.finalSelect {
			bc.cteByName = parent.cteByName
			bc.initSelect = parent.initSelect
			bc.recSelect = parent.recSelect
			bc.finalSelect = parent.finalSelect
			bc.recRecursiveScanNodeId = parent.recRecursiveScanNodeId
			bc.isTryBindingCTE = parent.isTryBindingCTE
		}
		bc.snapshot = parent.snapshot
	}

	return bc
}

func (bc *BindContext) rootTag() int32 {
	if bc.initSelect || bc.recSelect {
		return bc.sinkTag
	} else if bc.resultTag > 0 {
		return bc.resultTag
	} else {
		return bc.projectTag
	}
}

func (bc *BindContext) topTag() int32 {
	if bc.resultTag > 0 {
		return bc.resultTag
	} else {
		return bc.projectTag
	}
}

func (bc *BindContext) findCTE(name string) *CTERef {
	if cte, ok := bc.cteByName[name]; ok {
		return cte
	}

	parent := bc.parent
	for parent != nil && name != parent.cteName {
		if cte, ok := parent.cteByName[name]; ok {
			if !bc.maskedCTEs[name] {
				return cte
			}
		}

		bc = parent
		parent = bc.parent
	}

	return nil
}

func (bc *BindContext) mergeContexts(ctx context.Context, left, right *BindContext) error {
	left.parent = bc
	right.parent = bc
	bc.leftChild = left
	bc.rightChild = right

	for _, binding := range left.bindings {
		bc.bindings = append(bc.bindings, binding)
		bc.bindingByTag[binding.tag] = binding
		bc.bindingByTable[binding.table] = binding
	}

	for _, binding := range right.bindings {
		if _, ok := bc.bindingByTable[binding.table]; ok {
			return moerr.NewInvalidInput(ctx, "table '%s' specified more than once", binding.table)
		}

		bc.bindings = append(bc.bindings, binding)
		bc.bindingByTag[binding.tag] = binding
		bc.bindingByTable[binding.table] = binding
	}

	for col, binding := range left.bindingByCol {
		bc.bindingByCol[col] = binding
	}

	for col, binding := range right.bindingByCol {
		if _, ok := bc.bindingByCol[col]; ok {
			bc.bindingByCol[col] = nil
		} else {
			bc.bindingByCol[col] = binding
		}
	}

	bc.bindingTree = &BindingTreeNode{
		left:  left.bindingTree,
		right: right.bindingTree,
	}

	return nil
}

func (bc *BindContext) addUsingCol(col string, typ plan.Node_JoinType, left, right *BindContext) (*plan.Expr, error) {
	leftBinding, ok := left.bindingByCol[col]
	if !ok {
		return nil, moerr.NewInvalidInput(bc.binder.GetContext(), "column '%s' specified in USING clause does not exist in left table", col)
	}
	if leftBinding == nil {
		return nil, moerr.NewInvalidInput(bc.binder.GetContext(), "common column '%s' appears more than once in left table", col)
	}

	rightBinding, ok := right.bindingByCol[col]
	if !ok {
		return nil, moerr.NewInvalidInput(bc.binder.GetContext(), "column '%s' specified in USING clause does not exist in right table", col)
	}
	if rightBinding == nil {
		return nil, moerr.NewInvalidInput(bc.binder.GetContext(), "common column '%s' appears more than once in right table", col)
	}

	if typ != plan.Node_RIGHT {
		bc.bindingByCol[col] = leftBinding
		bc.bindingTree.using = append(bc.bindingTree.using, NameTuple{
			table: leftBinding.table,
			col:   col,
		})
	} else {
		bc.bindingByCol[col] = rightBinding
		bc.bindingTree.using = append(bc.bindingTree.using, NameTuple{
			table: rightBinding.table,
			col:   col,
		})
	}

	leftPos := leftBinding.colIdByName[col]
	rightPos := rightBinding.colIdByName[col]
	expr, err := BindFuncExprImplByPlanExpr(bc.binder.GetContext(), "=", []*plan.Expr{
		{
			Typ: *leftBinding.types[leftPos],
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: leftBinding.tag,
					ColPos: leftPos,
				},
			},
		},
		{
			Typ: *rightBinding.types[rightPos],
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: rightBinding.tag,
					ColPos: rightPos,
				},
			},
		},
	})

	return expr, err
}

func (bc *BindContext) unfoldStar(ctx context.Context, table string, isSysAccount bool) ([]tree.SelectExpr, []string, error) {
	if len(table) == 0 {
		// unfold *
		var exprs []tree.SelectExpr
		var names []string

		bc.doUnfoldStar(ctx, bc.bindingTree, make(map[string]bool), &exprs, &names, isSysAccount)

		return exprs, names, nil
	} else {
		// unfold tbl.*
		binding, ok := bc.bindingByTable[table]
		if !ok {
			return nil, nil, moerr.NewInvalidInput(ctx, "missing FROM-clause entry for table '%s'", table)
		}

		exprs := make([]tree.SelectExpr, 0)
		names := make([]string, 0)

		for i, col := range binding.cols {
			if binding.colIsHidden[i] {
				continue
			}
			if catalog.ContainExternalHidenCol(col) {
				continue
			}
			//the non-sys account skips the column account_id for the cluster table
			if !isSysAccount && binding.isClusterTable && util.IsClusterTableAttribute(col) {
				continue
			}
			expr, _ := tree.NewUnresolvedName(ctx, table, col)
			exprs = append(exprs, tree.SelectExpr{Expr: expr})
			names = append(names, col)
		}

		return exprs, names, nil
	}
}

func (bc *BindContext) doUnfoldStar(ctx context.Context, root *BindingTreeNode, visitedUsingCols map[string]bool, exprs *[]tree.SelectExpr, names *[]string, isSysAccount bool) {
	if root == nil {
		return
	}
	if root.binding != nil {
		for i, col := range root.binding.cols {
			if root.binding.colIsHidden[i] {
				continue
			}
			if catalog.ContainExternalHidenCol(col) {
				continue
			}
			//the non-sys account skips the column account_id for the cluster table
			if !isSysAccount && root.binding.isClusterTable && util.IsClusterTableAttribute(col) {
				continue
			}
			if !visitedUsingCols[col] {
				expr, _ := tree.NewUnresolvedName(ctx, root.binding.table, col)
				*exprs = append(*exprs, tree.SelectExpr{Expr: expr})
				*names = append(*names, col)
			}
		}

		return
	}

	var handledUsingCols []string

	for _, using := range root.using {
		if catalog.ContainExternalHidenCol(using.col) {
			continue
		}
		//the non-sys account skips the column account_id for the cluster table
		if !isSysAccount && root.binding.isClusterTable && util.IsClusterTableAttribute(using.col) {
			continue
		}
		if !visitedUsingCols[using.col] {
			handledUsingCols = append(handledUsingCols, using.col)
			visitedUsingCols[using.col] = true

			expr, _ := tree.NewUnresolvedName(ctx, using.table, using.col)
			*exprs = append(*exprs, tree.SelectExpr{Expr: expr})
			*names = append(*names, using.col)
		}
	}

	bc.doUnfoldStar(ctx, root.left, visitedUsingCols, exprs, names, isSysAccount)
	bc.doUnfoldStar(ctx, root.right, visitedUsingCols, exprs, names, isSysAccount)

	for _, col := range handledUsingCols {
		delete(visitedUsingCols, col)
	}
}

func (bc *BindContext) qualifyColumnNames(astExpr tree.Expr, expandAlias ExpandAliasMode) (tree.Expr, error) {
	var err error

	switch exprImpl := astExpr.(type) {
	case *tree.ParenExpr:
		astExpr, err = bc.qualifyColumnNames(exprImpl.Expr, expandAlias)

	case *tree.OrExpr:
		exprImpl.Left, err = bc.qualifyColumnNames(exprImpl.Left, expandAlias)
		if err != nil {
			return nil, err
		}

		exprImpl.Right, err = bc.qualifyColumnNames(exprImpl.Right, expandAlias)

	case *tree.NotExpr:
		exprImpl.Expr, err = bc.qualifyColumnNames(exprImpl.Expr, expandAlias)

	case *tree.AndExpr:
		exprImpl.Left, err = bc.qualifyColumnNames(exprImpl.Left, expandAlias)
		if err != nil {
			return nil, err
		}

		exprImpl.Right, err = bc.qualifyColumnNames(exprImpl.Right, expandAlias)

	case *tree.UnaryExpr:
		exprImpl.Expr, err = bc.qualifyColumnNames(exprImpl.Expr, expandAlias)

	case *tree.BinaryExpr:
		exprImpl.Left, err = bc.qualifyColumnNames(exprImpl.Left, expandAlias)
		if err != nil {
			return nil, err
		}

		exprImpl.Right, err = bc.qualifyColumnNames(exprImpl.Right, expandAlias)

	case *tree.ComparisonExpr:
		exprImpl.Left, err = bc.qualifyColumnNames(exprImpl.Left, expandAlias)
		if err != nil {
			return nil, err
		}

		exprImpl.Right, err = bc.qualifyColumnNames(exprImpl.Right, expandAlias)

	case *tree.FuncExpr:
		for i := range exprImpl.Exprs {
			exprImpl.Exprs[i], err = bc.qualifyColumnNames(exprImpl.Exprs[i], expandAlias)
			if err != nil {
				return nil, err
			}
		}
		if exprImpl.WindowSpec != nil {
			for i := range exprImpl.WindowSpec.PartitionBy {
				exprImpl.WindowSpec.PartitionBy[i], err = bc.qualifyColumnNames(exprImpl.WindowSpec.PartitionBy[i], expandAlias)
				if err != nil {
					return nil, err
				}
			}
			for i := range exprImpl.WindowSpec.OrderBy {
				exprImpl.WindowSpec.OrderBy[i].Expr, err = bc.qualifyColumnNames(exprImpl.WindowSpec.OrderBy[i].Expr, expandAlias)
				if err != nil {
					return nil, err
				}
			}
		}

	case *tree.RangeCond:
		exprImpl.Left, err = bc.qualifyColumnNames(exprImpl.Left, expandAlias)
		if err != nil {
			return nil, err
		}

		exprImpl.From, err = bc.qualifyColumnNames(exprImpl.From, expandAlias)
		if err != nil {
			return nil, err
		}

		exprImpl.To, err = bc.qualifyColumnNames(exprImpl.To, expandAlias)

	case *tree.UnresolvedName:
		if !exprImpl.Star && exprImpl.NumParts == 1 {
			col := exprImpl.Parts[0]
			if expandAlias == AliasBeforeColumn {
				if selectItem, ok := bc.aliasMap[col]; ok {
					return selectItem.astExpr, nil
				}
			}

			if binding, ok := bc.bindingByCol[col]; ok {
				if binding != nil {
					exprImpl.NumParts = 2
					exprImpl.Parts[1] = binding.table
					return astExpr, nil
				} else {
					return nil, moerr.NewInvalidInput(bc.binder.GetContext(), "ambiguouse column reference to '%s'", col)
				}
			}

			if expandAlias == AliasAfterColumn {
				if selectItem, ok := bc.aliasMap[col]; ok {
					return selectItem.astExpr, nil
				}
			}
		}

	case *tree.CastExpr:
		exprImpl.Expr, err = bc.qualifyColumnNames(exprImpl.Expr, expandAlias)

	case *tree.IsNullExpr:
		exprImpl.Expr, err = bc.qualifyColumnNames(exprImpl.Expr, expandAlias)

	case *tree.IsNotNullExpr:
		exprImpl.Expr, err = bc.qualifyColumnNames(exprImpl.Expr, expandAlias)

	case *tree.Tuple:
		for i := range exprImpl.Exprs {
			exprImpl.Exprs[i], err = bc.qualifyColumnNames(exprImpl.Exprs[i], expandAlias)
			if err != nil {
				return nil, err
			}
		}

	case *tree.CaseExpr:
		exprImpl.Expr, err = bc.qualifyColumnNames(exprImpl.Expr, expandAlias)
		if err != nil {
			return nil, err
		}

		for _, when := range exprImpl.Whens {
			when.Cond, err = bc.qualifyColumnNames(when.Cond, expandAlias)
			if err != nil {
				return nil, err
			}

			when.Val, err = bc.qualifyColumnNames(when.Val, expandAlias)
			if err != nil {
				return nil, err
			}
		}

		exprImpl.Else, err = bc.qualifyColumnNames(exprImpl.Else, expandAlias)

	case *tree.XorExpr:
		exprImpl.Left, err = bc.qualifyColumnNames(exprImpl.Left, expandAlias)
		if err != nil {
			return nil, err
		}

		exprImpl.Right, err = bc.qualifyColumnNames(exprImpl.Right, expandAlias)
	}

	return astExpr, err
}
