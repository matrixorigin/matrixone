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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewBindContext(builder *QueryBuilder, parent *BindContext) *BindContext {
	bc := &BindContext{
		groupByAst:     make(map[string]int32),
		aggregateByAst: make(map[string]int32),
		projectByExpr:  make(map[string]int32),
		aliasMap:       make(map[string]int32),
		bindingByTag:   make(map[int32]*Binding),
		bindingByTable: make(map[string]*Binding),
		bindingByCol:   make(map[string]*Binding),
		parent:         parent,
	}

	return bc
}

func (bc *BindContext) rootTag() int32 {
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
			if _, ok := bc.maskedCTEs[name]; !ok {
				return cte
			}
		}

		bc = parent
		parent = bc.parent
	}

	return nil
}

func (bc *BindContext) mergeContexts(left, right *BindContext) error {
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
			return errors.New("", fmt.Sprintf("table name %q specified more than once", binding.table))
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

func (bc *BindContext) addUsingCol(col string, typ plan.Node_JoinFlag, left, right *BindContext) (*plan.Expr, error) {
	leftBinding, ok := left.bindingByCol[col]
	if !ok {
		return nil, errors.New("", fmt.Sprintf("column %q specified in USING clause does not exist in left table", col))
	}
	if leftBinding == nil {
		return nil, errors.New("", fmt.Sprintf("common column name %q appears more than once in left table", col))
	}

	rightBinding, ok := right.bindingByCol[col]
	if !ok {
		return nil, errors.New("", fmt.Sprintf("column %q specified in USING clause does not exist in right table", col))
	}
	if rightBinding == nil {
		return nil, errors.New("", fmt.Sprintf("common column name %q appears more than once in right table", col))
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
	expr, err := bindFuncExprImplByPlanExpr("=", []*plan.Expr{
		{
			Typ: leftBinding.types[leftPos],
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: leftBinding.tag,
					ColPos: leftPos,
				},
			},
		},
		{
			Typ: rightBinding.types[rightPos],
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

func (bc *BindContext) unfoldStar(table string) ([]tree.SelectExpr, []string, error) {
	if len(table) == 0 {
		// unfold *
		var exprs []tree.SelectExpr
		var names []string

		bc.doUnfoldStar(bc.bindingTree, make(map[string]any), &exprs, &names)

		return exprs, names, nil
	} else {
		// unfold tbl.*
		binding, ok := bc.bindingByTable[table]
		if !ok {
			return nil, nil, errors.New("", fmt.Sprintf("missing FROM-clause entry for table %q", table))
		}

		exprs := make([]tree.SelectExpr, len(binding.cols))
		names := make([]string, len(binding.cols))

		for i, col := range binding.cols {
			expr, _ := tree.NewUnresolvedName(table, col)
			exprs[i] = tree.SelectExpr{Expr: expr}
			names[i] = col
		}

		return exprs, names, nil
	}
}

func (bc *BindContext) doUnfoldStar(root *BindingTreeNode, visitedUsingCols map[string]any, exprs *[]tree.SelectExpr, names *[]string) {
	if root == nil {
		return
	}
	if root.binding != nil {
		for _, col := range root.binding.cols {
			if _, ok := visitedUsingCols[col]; !ok {
				expr, _ := tree.NewUnresolvedName(root.binding.table, col)
				*exprs = append(*exprs, tree.SelectExpr{Expr: expr})
				*names = append(*names, col)
			}
		}

		return
	}

	var handledUsingCols []string

	for _, using := range root.using {
		if _, ok := visitedUsingCols[using.col]; !ok {
			handledUsingCols = append(handledUsingCols, using.col)
			visitedUsingCols[using.col] = nil

			expr, _ := tree.NewUnresolvedName(using.table, using.col)
			*exprs = append(*exprs, tree.SelectExpr{Expr: expr})
			*names = append(*names, using.col)
		}
	}

	bc.doUnfoldStar(root.left, visitedUsingCols, exprs, names)
	bc.doUnfoldStar(root.right, visitedUsingCols, exprs, names)

	for _, col := range handledUsingCols {
		delete(visitedUsingCols, col)
	}
}

func (bc *BindContext) qualifyColumnNames(astExpr tree.Expr, selectList tree.SelectExprs, expandAlias bool) (tree.Expr, error) {
	var err error

	switch exprImpl := astExpr.(type) {
	case *tree.ParenExpr:
		astExpr, err = bc.qualifyColumnNames(exprImpl.Expr, selectList, expandAlias)

	case *tree.OrExpr:
		exprImpl.Left, err = bc.qualifyColumnNames(exprImpl.Left, selectList, expandAlias)
		if err != nil {
			return nil, err
		}

		exprImpl.Right, err = bc.qualifyColumnNames(exprImpl.Right, selectList, expandAlias)

	case *tree.NotExpr:
		exprImpl.Expr, err = bc.qualifyColumnNames(exprImpl.Expr, selectList, expandAlias)

	case *tree.AndExpr:
		exprImpl.Left, err = bc.qualifyColumnNames(exprImpl.Left, selectList, expandAlias)
		if err != nil {
			return nil, err
		}

		exprImpl.Right, err = bc.qualifyColumnNames(exprImpl.Right, selectList, expandAlias)

	case *tree.UnaryExpr:
		exprImpl.Expr, err = bc.qualifyColumnNames(exprImpl.Expr, selectList, expandAlias)

	case *tree.BinaryExpr:
		exprImpl.Left, err = bc.qualifyColumnNames(exprImpl.Left, selectList, expandAlias)
		if err != nil {
			return nil, err
		}

		exprImpl.Right, err = bc.qualifyColumnNames(exprImpl.Right, selectList, expandAlias)

	case *tree.ComparisonExpr:
		exprImpl.Left, err = bc.qualifyColumnNames(exprImpl.Left, selectList, expandAlias)
		if err != nil {
			return nil, err
		}

		exprImpl.Right, err = bc.qualifyColumnNames(exprImpl.Right, selectList, expandAlias)

	case *tree.FuncExpr:
		for i := range exprImpl.Exprs {
			exprImpl.Exprs[i], err = bc.qualifyColumnNames(exprImpl.Exprs[i], selectList, expandAlias)
			if err != nil {
				return nil, err
			}
		}

	case *tree.RangeCond:
		exprImpl.Left, err = bc.qualifyColumnNames(exprImpl.Left, selectList, expandAlias)
		if err != nil {
			return nil, err
		}

		exprImpl.From, err = bc.qualifyColumnNames(exprImpl.From, selectList, expandAlias)
		if err != nil {
			return nil, err
		}

		exprImpl.To, err = bc.qualifyColumnNames(exprImpl.To, selectList, expandAlias)

	case *tree.UnresolvedName:
		if !exprImpl.Star && exprImpl.NumParts == 1 {
			col := exprImpl.Parts[0]
			if expandAlias {
				if colPos, ok := bc.aliasMap[col]; ok {
					astExpr = selectList[colPos].Expr
					break
				}
			}

			if binding, ok := bc.bindingByCol[col]; ok {
				if binding != nil {
					exprImpl.NumParts = 2
					exprImpl.Parts[1] = binding.table
				} else {
					// return nil, errors.New("", fmt.Sprintf("column reference %q is ambiguous", col))
					return nil, errors.New("", fmt.Sprintf("Column reference '%s' is ambiguous", col))
				}
			}
		}

	case *tree.CastExpr:
		exprImpl.Expr, err = bc.qualifyColumnNames(exprImpl.Expr, selectList, expandAlias)

	case *tree.IsNullExpr:
		exprImpl.Expr, err = bc.qualifyColumnNames(exprImpl.Expr, selectList, expandAlias)

	case *tree.IsNotNullExpr:
		exprImpl.Expr, err = bc.qualifyColumnNames(exprImpl.Expr, selectList, expandAlias)

	case *tree.Tuple:
		for i := range exprImpl.Exprs {
			exprImpl.Exprs[i], err = bc.qualifyColumnNames(exprImpl.Exprs[i], selectList, expandAlias)
			if err != nil {
				return nil, err
			}
		}

	case *tree.CaseExpr:
		exprImpl.Expr, err = bc.qualifyColumnNames(exprImpl.Expr, selectList, expandAlias)
		if err != nil {
			return nil, err
		}

		for _, when := range exprImpl.Whens {
			when.Cond, err = bc.qualifyColumnNames(when.Cond, selectList, expandAlias)
			if err != nil {
				return nil, err
			}

			when.Val, err = bc.qualifyColumnNames(when.Val, selectList, expandAlias)
			if err != nil {
				return nil, err
			}
		}

		exprImpl.Else, err = bc.qualifyColumnNames(exprImpl.Else, selectList, expandAlias)

	case *tree.XorExpr:
		exprImpl.Left, err = bc.qualifyColumnNames(exprImpl.Left, selectList, expandAlias)
		if err != nil {
			return nil, err
		}

		exprImpl.Right, err = bc.qualifyColumnNames(exprImpl.Right, selectList, expandAlias)
	}

	return astExpr, err
}
