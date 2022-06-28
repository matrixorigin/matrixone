// Copyright 2021 - 2022 Matrix Origin
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

package plan2

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildUpdate(stmt *tree.Update, ctx CompilerContext) (*Plan, error) {
	// Check length of update list
	updateColLen := len(stmt.Exprs)
	if updateColLen == 0 {
		return nil, errors.New(errno.CaseNotFound, "no column will be update")
	}
	// Check database's name and table's name
	alsTbl, ok := stmt.Table.(*tree.AliasedTableExpr)
	if !ok {
		return nil, errors.New(errno.FeatureNotSupported, "cannot update from multiple tables")
	}
	tbl, ok := alsTbl.Expr.(*tree.TableName)
	if !ok {
		return nil, errors.New(errno.FeatureNotSupported, "cannot update from multiple tables")
	}
	var dbName string
	if tbl.SchemaName == "" {
		dbName = ctx.DefaultDatabase()
	}
	objRef, tableDef := ctx.Resolve(dbName, string(tbl.ObjectName))
	if tableDef == nil {
		return nil, errors.New(errno.FeatureNotSupported, "cannot find update table")
	}

	// Function of getting def of col from col name
	getColDef := func(name string) *plan.Type {
		for _, col := range tableDef.Cols {
			if col.Name == name {
				return col.Typ
			}
		}
		return nil
	}

	// Check if update primary key
	var updateAttrs []string = nil
	for _, expr := range stmt.Exprs {
		if len(expr.Names) != 1 {
			return nil, errors.New(errno.CaseNotFound, "the set list of update must be one")
		}
		updateColName := expr.Names[0].Parts[0]
		for _, name := range updateAttrs {
			if updateColName == name {
				return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, "update's column name is duplicate")
			}
		}
		updateAttrs = append(updateAttrs, updateColName)
	}

	var useProjectExprs tree.SelectExprs
	// Build projection of primary key
	var priKey string
	var priKeyIdx int32 = -1
	priKeys := ctx.GetPrimaryKeyDef(objRef.SchemaName, tableDef.Name)
	for _, key := range priKeys {
		for _, updateName := range updateAttrs {
			if key.Name == updateName {
				e, _ := tree.NewUnresolvedName(key.Name)
				useProjectExprs = append(useProjectExprs, tree.SelectExpr{Expr: e})
				priKey = key.Name
				priKeyIdx = 0
				break
			}
		}
	}

	// Build projection of primary key
	hideKey := ctx.GetHideKeyDef(objRef.SchemaName, tableDef.Name).GetName()
	if priKeyIdx == -1 {
		if hideKey == "" {
			return nil, errors.New(errno.InternalError, "cannot find hide key now")
		}
		e, _ := tree.NewUnresolvedName(hideKey)
		useProjectExprs = append(useProjectExprs, tree.SelectExpr{Expr: e})
	}

	// Build projection of update expr
	for i, expr := range stmt.Exprs {
		updateColName := updateAttrs[i]

		// wrap up func of cast for update expr
		colTyp := getColDef(updateColName)
		if colTyp == nil {
			return nil, errors.New(errno.CaseNotFound, fmt.Sprintf("set column name [%v] is not found", updateColName))
		}
		wrapExpr, err := wrapCastForExpr(expr.Expr, colTyp)
		if err != nil {
			return nil, err
		}
		useProjectExprs = append(useProjectExprs, tree.SelectExpr{Expr: wrapExpr})
	}

	// build other column which doesn't update
	var otherAttrs []string = nil
	var attrOrders []string = nil
	if priKeyIdx == 0 {
		for _, col := range tableDef.Cols {
			if col.Name == hideKey {
				continue
			}
			attrOrders = append(attrOrders, col.Name)
			find := false
			for _, attr := range updateAttrs {
				if attr == col.Name {
					find = true
				}
			}
			if !find {
				otherAttrs = append(otherAttrs, col.Name)
				e, _ := tree.NewUnresolvedName(col.Name)
				useProjectExprs = append(useProjectExprs, tree.SelectExpr{Expr: e})
			}
		}
	}

	// build the stmt of select and append select node
	if len(stmt.OrderBy) > 0 && (stmt.Where == nil && stmt.Limit == nil) {
		stmt.OrderBy = nil
	}
	selectStmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: useProjectExprs,
			From:  &tree.From{Tables: tree.TableExprs{stmt.Table}},
			Where: stmt.Where,
		},
		OrderBy: stmt.OrderBy,
		Limit:   stmt.Limit,
	}
	//query, binderCtx := newQueryAndSelectCtx(plan.Query_UPDATE)
	//nodeId, err := buildSelect(selectStmt, ctx, query, binderCtx)
	usePlan, err := runBuildSelectByBinder(plan.Query_SELECT, ctx, selectStmt)
	if err != nil {
		return nil, err
	}
	usePlan.Plan.(*plan.Plan_Query).Query.StmtType = plan.Query_UPDATE
	qry := usePlan.Plan.(*plan.Plan_Query).Query

	// build update node
	node := &Node{
		NodeType: plan.Node_UPDATE,
		ObjRef:   objRef,
		TableDef: tableDef,
		Children: []int32{qry.Steps[len(qry.Steps)-1]},
		NodeId:   int32(len(qry.Nodes)),
		UpdateInfo: &plan.UpdateInfo{
			PriKey:      priKey,
			PriKeyIdx:   priKeyIdx,
			HideKey:     hideKey,
			UpdateAttrs: updateAttrs,
			OtherAttrs:  otherAttrs,
			AttrOrders:  attrOrders,
		},
	}
	qry.Nodes = append(qry.Nodes, node)
	qry.Steps[len(qry.Steps)-1] = node.NodeId

	return usePlan, nil
}

func wrapCastForExpr(expr tree.Expr, typ *plan.Type) (tree.Expr, error) {
	var castTyp *tree.T
	switch typ.Id {
	case plan.Type_INT8:
		castTyp = &tree.T{
			InternalType: tree.InternalType{
				Oid:   uint32(defines.MYSQL_TYPE_TINY),
				Width: typ.Width,
			},
		}
	case plan.Type_INT16:
		castTyp = &tree.T{
			InternalType: tree.InternalType{
				Oid:   uint32(defines.MYSQL_TYPE_SHORT),
				Width: typ.Width,
			},
		}
	case plan.Type_INT32:
		castTyp = &tree.T{
			InternalType: tree.InternalType{
				Oid:   uint32(defines.MYSQL_TYPE_LONG),
				Width: typ.Width,
			},
		}
	case plan.Type_INT64:
		castTyp = &tree.T{
			InternalType: tree.InternalType{
				Oid:   uint32(defines.MYSQL_TYPE_LONGLONG),
				Width: typ.Width,
			},
		}
	case plan.Type_UINT8:
		castTyp = &tree.T{
			InternalType: tree.InternalType{
				Oid:      uint32(defines.MYSQL_TYPE_TINY),
				Width:    typ.Width,
				Unsigned: true,
			},
		}
	case plan.Type_UINT16:
		castTyp = &tree.T{
			InternalType: tree.InternalType{
				Oid:      uint32(defines.MYSQL_TYPE_SHORT),
				Width:    typ.Width,
				Unsigned: true,
			},
		}
	case plan.Type_UINT32:
		castTyp = &tree.T{
			InternalType: tree.InternalType{
				Oid:      uint32(defines.MYSQL_TYPE_LONG),
				Width:    typ.Width,
				Unsigned: true,
			},
		}
	case plan.Type_UINT64:
		castTyp = &tree.T{
			InternalType: tree.InternalType{
				Oid:      uint32(defines.MYSQL_TYPE_LONGLONG),
				Width:    typ.Width,
				Unsigned: true,
			},
		}
	case plan.Type_FLOAT32:
		castTyp = &tree.T{
			InternalType: tree.InternalType{
				Oid:       uint32(defines.MYSQL_TYPE_FLOAT),
				Width:     typ.Width,
				Precision: typ.Precision,
			},
		}
	case plan.Type_FLOAT64:
		castTyp = &tree.T{
			InternalType: tree.InternalType{
				Oid:       uint32(defines.MYSQL_TYPE_DOUBLE),
				Width:     typ.Width,
				Precision: typ.Precision,
			},
		}
	case plan.Type_CHAR:
		if typ.Width == 0 {
			castTyp = &tree.T{
				InternalType: tree.InternalType{
					Oid:         uint32(defines.MYSQL_TYPE_STRING),
					Width:       typ.Width,
					DisplayWith: -1,
				},
			}
		} else {
			castTyp = &tree.T{
				InternalType: tree.InternalType{
					Oid:         uint32(defines.MYSQL_TYPE_STRING),
					Width:       typ.Width,
					DisplayWith: typ.Width,
				},
			}
		}
	case plan.Type_VARCHAR:
		castTyp = &tree.T{
			InternalType: tree.InternalType{
				Oid:         uint32(defines.MYSQL_TYPE_STRING),
				Width:       typ.Width,
				DisplayWith: typ.Width,
			},
		}
	case plan.Type_DATE:
		castTyp = &tree.T{
			InternalType: tree.InternalType{
				Oid: uint32(defines.MYSQL_TYPE_DATE),
			},
		}
	case plan.Type_DATETIME:
		castTyp = &tree.T{
			InternalType: tree.InternalType{
				Oid: uint32(defines.MYSQL_TYPE_DATETIME),
			},
		}
	case plan.Type_TIMESTAMP:
		castTyp = &tree.T{
			InternalType: tree.InternalType{
				Oid:       uint32(defines.MYSQL_TYPE_TIMESTAMP),
				Precision: typ.Precision,
			},
		}
	case plan.Type_DECIMAL64, plan.Type_DECIMAL128:
		castTyp = &tree.T{
			InternalType: tree.InternalType{
				Oid:         uint32(defines.MYSQL_TYPE_DECIMAL),
				DisplayWith: typ.Width,
				Precision:   typ.Scale,
			},
		}
	case plan.Type_BOOL:
		castTyp = &tree.T{
			InternalType: tree.InternalType{
				Oid: uint32(defines.MYSQL_TYPE_BOOL),
			},
		}
	default:
		return nil, errors.New(errno.IndeterminateDatatype, fmt.Sprintf("unsupport type: '%v'", typ.GetId().String()))
	}
	return tree.NewCastExpr(expr, castTyp), nil
}
