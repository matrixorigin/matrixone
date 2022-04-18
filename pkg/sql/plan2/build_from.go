package plan2

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildFrom(stmt tree.TableExprs, ctx CompilerContext, query *Query) error {
	for _, table := range stmt {
		err := buildTable(table, ctx, query)
		if err != nil {
			return err
		}
	}
	return nil
}

func buildTable(stmt tree.TableExpr, ctx CompilerContext, query *Query) error {
	switch tbl := stmt.(type) {
	case *tree.Select:
		buildSelect(tbl, ctx, query)
		return nil
	case *tree.TableName:
		name := string(tbl.ObjectName)
		if len(tbl.SchemaName) > 0 {
			name = strings.Join([]string{string(tbl.SchemaName), name}, ".")
		}
		obj, tableDef := ctx.Resolve(name)

		nodeLength := int32(len(query.Nodes))
		node := &plan.Node{
			NodeType: plan.Node_TABLE_SCAN, //todo confirm NodeType
			NodeId:   nodeLength,
			ObjRef:   obj,
			TableDef: tableDef,
		}
		query.Nodes = append(query.Nodes, node)
		return nil
	case *tree.JoinTableExpr:
		//todo confirm how to deal with alias
		return buildJoinTable(tbl, ctx, query)
	case *tree.ParenTableExpr:
		return buildTable(tbl.Expr, ctx, query)
	case *tree.AliasedTableExpr:
		//todo confirm how to deal with alias
		return buildTable(tbl.Expr, ctx, query)
	case *tree.StatementSource:
		return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table expr: %T", stmt))
	}
	// Values table not support
	return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table expr: %T", stmt))
}

func buildJoinTable(tbl *tree.JoinTableExpr, ctx CompilerContext, query *Query) error {
	err := buildTable(tbl.Right, ctx, query)
	if err != nil {
		return err
	}
	rightNodeId := query.Nodes[len(query.Nodes)-1].NodeId
	rightTableDef := query.Nodes[len(query.Nodes)-1].TableDef

	err = buildTable(tbl.Left, ctx, query)
	if err != nil {
		return err
	}
	leftNodeId := query.Nodes[len(query.Nodes)-1].NodeId
	leftTableDef := query.Nodes[len(query.Nodes)-1].TableDef

	var onList []*plan.Expr
	switch cond := tbl.Cond.(type) {
	case *tree.OnJoinCond:
		exprs, err := splitAndBuildExpr(cond.Expr, ctx, query)
		if err != nil {
			return err
		}
		onList = exprs
	case *tree.UsingJoinCond:
		for _, identifiers := range cond.Cols {
			if len(identifiers) > 1 { //todo confirm
				return errors.New(errno.SyntaxError, fmt.Sprintf("column identifiers more than one '%v'", tree.String(tbl.Cond, dialect.MYSQL)))
			}
			name := string(identifiers[0])
			leftColIndex := getColumnIndex(leftTableDef, name)
			if leftColIndex < 0 {
				return errors.New(errno.InvalidColumnReference, fmt.Sprintf("column '%v' is not exist", string(identifiers[0])))
			}
			rightColIndex := getColumnIndex(rightTableDef, name)
			if rightColIndex < 0 {
				return errors.New(errno.InvalidColumnReference, fmt.Sprintf("column '%v' is not exist", string(identifiers[0])))
			}

			col := &plan.Expr_Col{
				Col: &plan.ColRef{
					Name:   name,
					RelPos: rightColIndex,
					ColPos: leftColIndex,
				},
			}
			onList = append(onList, &plan.Expr{
				Expr: col,
			})
		}
	case *tree.NaturalJoinCond:
		onList = append(onList, getColumnsWithSameName(leftTableDef, rightTableDef)...)
	default:
		return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport join condition '%v'", tree.String(tbl.Cond, dialect.MYSQL)))

	}

	node := &plan.Node{
		NodeType: plan.Node_JOIN,
		NodeId:   int32(len(query.Nodes)),
		OnList:   onList,
		Children: []int32{rightNodeId, leftNodeId},
	}
	query.Nodes = append(query.Nodes, node)

	return nil
}

func getColumnsWithSameName(left *plan.TableDef, right *plan.TableDef) []*plan.Expr {
	var exprs []*plan.Expr

	for leftIdx, leftCol := range left.Cols {
		for rightIdx, rightCol := range right.Cols {
			if leftCol.Name == rightCol.Name {
				exprs = append(exprs, &plan.Expr{
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							Name:   leftCol.Name,
							RelPos: int32(rightIdx), //todo confirm what RelPos means
							ColPos: int32(leftIdx),
						},
					},
				})
			}
		}
	}

	return exprs
}
