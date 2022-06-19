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

package plan2

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/errno"
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
		cteTables:      make(map[string]*plan.TableDef),
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
			return errors.New(errno.DuplicateTable, fmt.Sprintf("table name %q specified more than once", binding.table))
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
		return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("column %q specified in USING clause does not exist in left table", col))
	}
	if leftBinding == nil {
		return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("common column name %q appears more than once in left table", col))
	}

	rightBinding, ok := right.bindingByCol[col]
	if !ok {
		return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("column %q specified in USING clause does not exist in right table", col))
	}
	if rightBinding == nil {
		return nil, errors.New(errno.InvalidColumnReference, fmt.Sprintf("common column name %q appears more than once in right table", col))
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
			return nil, nil, errors.New(errno.UndefinedTable, fmt.Sprintf("missing FROM-clause entry for table %q", table))
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

func (bc *BindContext) qualifyColumnNames(astExpr tree.Expr) error {
	switch exprImpl := astExpr.(type) {
	case *tree.ParenExpr:
		return bc.qualifyColumnNames(exprImpl.Expr)

	case *tree.OrExpr:
		err := bc.qualifyColumnNames(exprImpl.Left)
		if err != nil {
			return err
		}

		return bc.qualifyColumnNames(exprImpl.Right)

	case *tree.NotExpr:
		return bc.qualifyColumnNames(exprImpl.Expr)

	case *tree.AndExpr:
		err := bc.qualifyColumnNames(exprImpl.Left)
		if err != nil {
			return err
		}

		return bc.qualifyColumnNames(exprImpl.Right)

	case *tree.UnaryExpr:
		return bc.qualifyColumnNames(exprImpl.Expr)

	case *tree.BinaryExpr:
		err := bc.qualifyColumnNames(exprImpl.Left)
		if err != nil {
			return err
		}

		return bc.qualifyColumnNames(exprImpl.Right)

	case *tree.ComparisonExpr:
		err := bc.qualifyColumnNames(exprImpl.Left)
		if err != nil {
			return err
		}

		return bc.qualifyColumnNames(exprImpl.Right)

	case *tree.FuncExpr:
		for _, child := range exprImpl.Exprs {
			err := bc.qualifyColumnNames(child)
			if err != nil {
				return err
			}
		}

	case *tree.RangeCond:
		err := bc.qualifyColumnNames(exprImpl.Left)
		if err != nil {
			return err
		}

		err = bc.qualifyColumnNames(exprImpl.From)
		if err != nil {
			return err
		}

		return bc.qualifyColumnNames(exprImpl.To)

	case *tree.UnresolvedName:
		if !exprImpl.Star && exprImpl.NumParts == 1 {
			col := exprImpl.Parts[0]
			if binding, ok := bc.bindingByCol[col]; ok {
				if binding != nil {
					exprImpl.Parts[1] = binding.table
				} else {
					// return errors.New(errno.AmbiguousColumn, fmt.Sprintf("column reference %q is ambiguous", col))
					return errors.New("", fmt.Sprintf("Column reference '%s' is ambiguous", col))
				}
			}
		}

	case *tree.CastExpr:
		return bc.qualifyColumnNames(exprImpl.Expr)

	case *tree.IsNullExpr:
		return bc.qualifyColumnNames(exprImpl.Expr)

	case *tree.IsNotNullExpr:
		return bc.qualifyColumnNames(exprImpl.Expr)

	case *tree.Tuple:
		for _, child := range exprImpl.Exprs {
			err := bc.qualifyColumnNames(child)
			if err != nil {
				return err
			}
		}

	case *tree.CaseExpr:
		err := bc.qualifyColumnNames(exprImpl.Expr)
		if err != nil {
			return err
		}

		for _, when := range exprImpl.Whens {
			err = bc.qualifyColumnNames(when.Cond)
			if err != nil {
				return err
			}

			err = bc.qualifyColumnNames(when.Val)
			if err != nil {
				return err
			}
		}

		return bc.qualifyColumnNames(exprImpl.Else)

	case *tree.XorExpr:
		err := bc.qualifyColumnNames(exprImpl.Left)
		if err != nil {
			return err
		}

		return bc.qualifyColumnNames(exprImpl.Right)
	}

	return nil
}

func (bc *BindContext) qualifyColumnNamesAndExpandAlias(astExpr tree.Expr, selectList tree.SelectExprs) (tree.Expr, error) {
	var err error

	switch exprImpl := astExpr.(type) {
	case *tree.ParenExpr:
		exprImpl.Expr, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Expr, selectList)

	case *tree.OrExpr:
		exprImpl.Left, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Left, selectList)
		if err != nil {
			return nil, err
		}

		exprImpl.Right, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Right, selectList)

	case *tree.NotExpr:
		exprImpl.Expr, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Expr, selectList)

	case *tree.AndExpr:
		exprImpl.Left, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Left, selectList)
		if err != nil {
			return nil, err
		}

		exprImpl.Right, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Right, selectList)

	case *tree.UnaryExpr:
		exprImpl.Expr, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Expr, selectList)

	case *tree.BinaryExpr:
		exprImpl.Left, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Left, selectList)
		if err != nil {
			return nil, err
		}

		exprImpl.Right, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Right, selectList)

	case *tree.ComparisonExpr:
		exprImpl.Left, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Left, selectList)
		if err != nil {
			return nil, err
		}

		exprImpl.Right, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Right, selectList)

	case *tree.FuncExpr:
		for i := range exprImpl.Exprs {
			exprImpl.Exprs[i], err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Exprs[i], selectList)
			if err != nil {
				return nil, err
			}
		}

	case *tree.RangeCond:
		exprImpl.Left, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Left, selectList)
		if err != nil {
			return nil, err
		}

		exprImpl.From, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.From, selectList)
		if err != nil {
			return nil, err
		}

		exprImpl.To, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.To, selectList)

	case *tree.UnresolvedName:
		if !exprImpl.Star && exprImpl.NumParts == 1 {
			col := exprImpl.Parts[0]
			if colPos, ok := bc.aliasMap[col]; ok {
				astExpr = selectList[colPos].Expr
			} else if binding, ok := bc.bindingByCol[col]; ok {
				if binding != nil {
					exprImpl.Parts[1] = binding.table
				} else {
					// return nil, errors.New(errno.AmbiguousColumn, fmt.Sprintf("column reference %q is ambiguous", col))
					return nil, errors.New("", fmt.Sprintf("Column reference '%s' is ambiguous", col))
				}
			}
		}

	case *tree.CastExpr:
		exprImpl.Expr, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Expr, selectList)

	case *tree.IsNullExpr:
		exprImpl.Expr, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Expr, selectList)

	case *tree.IsNotNullExpr:
		exprImpl.Expr, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Expr, selectList)

	case *tree.Tuple:
		for i := range exprImpl.Exprs {
			exprImpl.Exprs[i], err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Exprs[i], selectList)
			if err != nil {
				return nil, err
			}
		}

	case *tree.CaseExpr:
		exprImpl.Expr, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Expr, selectList)
		if err != nil {
			return nil, err
		}

		for _, when := range exprImpl.Whens {
			when.Cond, err = bc.qualifyColumnNamesAndExpandAlias(when.Cond, selectList)
			if err != nil {
				return nil, err
			}

			when.Val, err = bc.qualifyColumnNamesAndExpandAlias(when.Val, selectList)
			if err != nil {
				return nil, err
			}
		}

		exprImpl.Else, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Else, selectList)

	case *tree.XorExpr:
		exprImpl.Left, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Left, selectList)
		if err != nil {
			return nil, err
		}

		exprImpl.Right, err = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Right, selectList)
	}

	return astExpr, err
}
