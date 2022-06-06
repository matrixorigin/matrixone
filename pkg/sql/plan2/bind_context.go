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
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewBindContext(builder *QueryBuilder, parent *BindContext) *BindContext {
	bc := &BindContext{
		groupMapByAst:     make(map[string]int32),
		aggregateMapByAst: make(map[string]int32),
		projectMapByExpr:  make(map[string]int32),
		aliasMap:          make(map[string]int32),
		bindingsByTag:     make(map[int32]*Binding),
		bindingsByName:    make(map[string]*Binding),
		usingColsByName:   make(map[string][]*UsingColumnSet),
		parent:            parent,
	}

	return bc
}

func (bc *BindContext) mergeContext(other *BindContext) {
	for _, binding := range other.bindings {
		if _, ok := bc.bindingsByName[binding.table]; ok {
			panic(errors.New(errno.DuplicateTable, fmt.Sprintf("table name %q specified more than once", binding.table)))
		}

		bc.bindings = append(bc.bindings, binding)
		bc.bindingsByTag[binding.tag] = binding
		bc.bindingsByName[binding.table] = binding
	}

	// TODO: handle USING columns
}

func (bc *BindContext) unfoldStar(table string) ([]tree.SelectExpr, []string, error) {
	if len(table) == 0 {
		// TODO: handle USING columns

		var exprList []tree.SelectExpr
		var nameList []string

		for _, binding := range bc.bindings {
			for _, col := range binding.cols {
				expr, _ := tree.NewUnresolvedName(table, col)
				exprList = append(exprList, tree.SelectExpr{
					Expr: expr,
				})
				nameList = append(nameList, col)
			}
		}

		return exprList, nameList, nil
	} else {
		binding, ok := bc.bindingsByName[table]
		if !ok {
			return nil, nil, errors.New(errno.UndefinedTable, fmt.Sprintf("missing FROM-clause entry for table %q", table))
		}

		exprList := make([]tree.SelectExpr, len(binding.cols))
		nameList := make([]string, len(binding.cols))

		for i, col := range binding.cols {
			expr, _ := tree.NewUnresolvedName(table, col)
			exprList[i] = tree.SelectExpr{
				Expr: expr,
			}
			nameList[i] = col
		}

		return exprList, nameList, nil
	}
}

func (bc *BindContext) qualifyColumnNames(astExpr tree.Expr) {
	switch exprImpl := astExpr.(type) {
	case *tree.ParenExpr:
		bc.qualifyColumnNames(exprImpl.Expr)
	case *tree.OrExpr:
		bc.qualifyColumnNames(exprImpl.Left)
		bc.qualifyColumnNames(exprImpl.Right)
	case *tree.NotExpr:
		bc.qualifyColumnNames(exprImpl.Expr)
	case *tree.AndExpr:
		bc.qualifyColumnNames(exprImpl.Left)
		bc.qualifyColumnNames(exprImpl.Right)
	case *tree.UnaryExpr:
		bc.qualifyColumnNames(exprImpl.Expr)
	case *tree.BinaryExpr:
		bc.qualifyColumnNames(exprImpl.Left)
		bc.qualifyColumnNames(exprImpl.Right)
	case *tree.ComparisonExpr:
		bc.qualifyColumnNames(exprImpl.Left)
		bc.qualifyColumnNames(exprImpl.Right)
	case *tree.FuncExpr:
		for _, child := range exprImpl.Exprs {
			bc.qualifyColumnNames(child)
		}
	case *tree.RangeCond:
		bc.qualifyColumnNames(exprImpl.Left)
		bc.qualifyColumnNames(exprImpl.From)
		bc.qualifyColumnNames(exprImpl.To)
	case *tree.UnresolvedName:
		if !exprImpl.Star && exprImpl.NumParts == 1 {
			col := exprImpl.Parts[0]
			if using, ok := bc.usingColsByName[col]; ok {
				if len(using) > 1 {
					panic(errors.New(errno.AmbiguousColumn, fmt.Sprintf("column reference %q is ambiguous", col)))
				}
				exprImpl.Parts[1] = using[0].primary.table
			} else {
				var found *Binding
				for _, binding := range bc.bindings {
					j := binding.FindColumn(col)
					if j == AmbiguousName {
						panic(errors.New(errno.AmbiguousColumn, fmt.Sprintf("column reference %q is ambiguous", col)))
					}
					if j != NotFound {
						if found != nil {
							panic(errors.New(errno.AmbiguousColumn, fmt.Sprintf("column reference %q is ambiguous", col)))
						} else {
							found = binding
						}
					}
				}
				if found != nil {
					exprImpl.Parts[1] = found.table
				}
			}
		}
	case *tree.CastExpr:
		bc.qualifyColumnNames(exprImpl.Expr)
	case *tree.IsNullExpr:
		bc.qualifyColumnNames(exprImpl.Expr)
	case *tree.IsNotNullExpr:
		bc.qualifyColumnNames(exprImpl.Expr)
	case *tree.Tuple:
		for _, child := range exprImpl.Exprs {
			bc.qualifyColumnNames(child)
		}
	case *tree.CaseExpr:
		bc.qualifyColumnNames(exprImpl.Expr)
		for _, when := range exprImpl.Whens {
			bc.qualifyColumnNames(when.Cond)
			bc.qualifyColumnNames(when.Val)
		}
		bc.qualifyColumnNames(exprImpl.Else)
	case *tree.XorExpr:
		bc.qualifyColumnNames(exprImpl.Left)
		bc.qualifyColumnNames(exprImpl.Right)
	}
}

func (bc *BindContext) qualifyColumnNamesAndExpandAlias(astExpr tree.Expr, selectList tree.SelectExprs) tree.Expr {
	switch exprImpl := astExpr.(type) {
	case *tree.ParenExpr:
		exprImpl.Expr = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Expr, selectList)
	case *tree.OrExpr:
		exprImpl.Left = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Left, selectList)
		exprImpl.Right = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Right, selectList)
	case *tree.NotExpr:
		exprImpl.Expr = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Expr, selectList)
	case *tree.AndExpr:
		exprImpl.Left = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Left, selectList)
		exprImpl.Right = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Right, selectList)
	case *tree.UnaryExpr:
		exprImpl.Expr = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Expr, selectList)
	case *tree.BinaryExpr:
		exprImpl.Left = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Left, selectList)
		exprImpl.Right = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Right, selectList)
	case *tree.ComparisonExpr:
		exprImpl.Left = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Left, selectList)
		exprImpl.Right = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Right, selectList)
	case *tree.FuncExpr:
		for i := range exprImpl.Exprs {
			exprImpl.Exprs[i] = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Exprs[i], selectList)
		}
	case *tree.RangeCond:
		exprImpl.Left = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Left, selectList)
		exprImpl.From = bc.qualifyColumnNamesAndExpandAlias(exprImpl.From, selectList)
		exprImpl.To = bc.qualifyColumnNamesAndExpandAlias(exprImpl.To, selectList)
	case *tree.UnresolvedName:
		if !exprImpl.Star && exprImpl.NumParts == 1 {
			col := exprImpl.Parts[0]
			if colPos, ok := bc.aliasMap[col]; ok {
				astExpr = selectList[colPos].Expr
			} else if using, ok := bc.usingColsByName[col]; ok {
				if len(using) > 1 {
					panic(errors.New(errno.AmbiguousColumn, fmt.Sprintf("column reference %q is ambiguous", col)))
				}
				exprImpl.Parts[1] = using[0].primary.table
			} else {
				var found *Binding
				for _, binding := range bc.bindings {
					j := binding.FindColumn(col)
					if j == AmbiguousName {
						panic(errors.New(errno.AmbiguousColumn, fmt.Sprintf("column reference %q is ambiguous", col)))
					}
					if j != NotFound {
						if found != nil {
							panic(errors.New(errno.AmbiguousColumn, fmt.Sprintf("column reference %q is ambiguous", col)))
						} else {
							found = binding
						}
					}
				}
				if found != nil {
					exprImpl.Parts[1] = found.table
				}
			}
		}
	case *tree.CastExpr:
		exprImpl.Expr = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Expr, selectList)
	case *tree.IsNullExpr:
		exprImpl.Expr = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Expr, selectList)
	case *tree.IsNotNullExpr:
		exprImpl.Expr = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Expr, selectList)
	case *tree.Tuple:
		for i := range exprImpl.Exprs {
			exprImpl.Exprs[i] = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Exprs[i], selectList)
		}
	case *tree.CaseExpr:
		exprImpl.Expr = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Expr, selectList)
		for _, when := range exprImpl.Whens {
			bc.qualifyColumnNamesAndExpandAlias(when.Cond, selectList)
			bc.qualifyColumnNamesAndExpandAlias(when.Val, selectList)
		}
		exprImpl.Else = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Else, selectList)
	case *tree.XorExpr:
		exprImpl.Left = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Left, selectList)
		exprImpl.Right = bc.qualifyColumnNamesAndExpandAlias(exprImpl.Right, selectList)
	}

	return astExpr
}
