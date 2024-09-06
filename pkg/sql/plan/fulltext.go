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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// coldef shall copy index type
var (
	fulltext_index_scan_func_name     = "fulltext_index_scan"
	fulltext_index_tokenize_func_name = "fulltext_index_tokenize"

	ftIndexColdefs = []*plan.ColDef{
		// row_id type should be same as index type
		{
			Name: "doc_id",
			Typ: plan.Type{
				Id:          int32(types.T_any),
				NotNullable: false,
			},
		},
		{
			Name: "score",
			Typ: plan.Type{
				Id:          int32(types.T_float32),
				NotNullable: false,
				Width:       4,
			},
		},
	}

	tokenizeColDefs = []*plan.ColDef{
		// row_id type should be same as index type
		{
			Name: "doc_id",
			Typ: plan.Type{
				Id:          int32(types.T_any),
				NotNullable: false,
			},
		},
		{
			Name: "pos",
			Typ: plan.Type{
				Id:          int32(types.T_int32),
				NotNullable: false,
				Width:       4,
			},
		},
		{
			Name: "word",
			Typ: plan.Type{
				Id:          int32(types.T_varchar),
				NotNullable: false,
			},
		},
		{
			Name: "doc_count",
			Typ: plan.Type{
				Id:          int32(types.T_int32),
				NotNullable: false,
				Width:       4,
			},
		},
		{
			Name: "first_doc_id",
			Typ: plan.Type{
				Id:          int32(types.T_any),
				NotNullable: false,
			},
		},
		{
			Name: "last_doc_id",
			Typ: plan.Type{
				Id:          int32(types.T_any),
				NotNullable: false,
			},
		},
	}
)

// arg list [index_table_name, search_against, mode]
func (builder *QueryBuilder) buildFullTextIndexScan(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, childId int32) (int32, error) {

	if len(exprs) != 3 {
		return 0, moerr.NewInvalidInput(builder.GetContext(), "Invalid number of arguments (NARGS != 3).")
	}

	colDefs := _getColDefs(ftIndexColdefs)

	/*
		val, err := builder.compCtx.ResolveVariable("save_query_result", true, false)
		if err == nil {
			if v, _ := val.(int8); v == 0 {
				return 0, moerr.NewNoConfig(builder.GetContext(), "save query result")
			} else {
				logutil.Infof("buildMetaScan : save query result: %v", v)
			}
		} else {
			return 0, err
		}
	*/

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name:  fulltext_index_scan_func_name,
				Param: []byte(""),
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{builder.genNewTag()},
		TblFuncExprList: exprs,
		Children:        []int32{childId},
	}
	return builder.appendNode(node, ctx), nil
}

// seems coldef just check the name but not type.  don't need to change primary key type to match with source table.
func (builder *QueryBuilder) getFullTextColDefs(fn *tree.FuncExpr) ([]*plan.ColDef, error) {

	coldefs := _getColDefs(ftIndexColdefs)

	if val, ok := fn.Exprs[3].(*tree.NumVal); ok {
		oid, ok := val.Int64()
		if !ok {
			return nil, moerr.NewNoConfig(builder.GetContext(), "invalid primary key type (not int64)")
		}

		coldefs[0].Typ.Id = int32(oid)
	} else {
		return nil, moerr.NewNoConfig(builder.GetContext(), "invalid primary key type (not NumVal)")
	}

	return coldefs, nil
}

// select * from index_table, fulltext_index_tokenize(doc_id, concat(body, ' ', title))
// arg list [params, doc_id, content]
func (builder *QueryBuilder) buildFullTextIndexTokenize(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, childId int32) (int32, error) {

	if len(exprs) != 3 {
		return 0, moerr.NewInvalidInput(builder.GetContext(), "Invalid number of arguments (NARGS != 3).")
	}

	colDefs := _getColDefs(tokenizeColDefs)
	params, err := builder.getFullTextParams(tbl.Func)
	if err != nil {
		return 0, err
	}

	scanNode := builder.qry.Nodes[childId]
	if scanNode.NodeType != plan.Node_TABLE_SCAN {
		return 0, moerr.NewNoConfig(builder.GetContext(), "child node is not a TABLE SCAN")
	}

	pkPos := scanNode.TableDef.Name2ColIndex[scanNode.TableDef.Pkey.PkeyColName]
	pkType := scanNode.TableDef.Cols[pkPos].Typ
	colDefs[0].Typ = pkType
	colDefs[4].Typ = pkType
	colDefs[5].Typ = pkType

	/*
	   val, err := builder.compCtx.ResolveVariable("save_query_result", true, false)
	   if err == nil {
	           if v, _ := val.(int8); v == 0 {
	                   return 0, moerr.NewNoConfig(builder.GetContext(), "save query result")
	           } else {
	                   logutil.Infof("buildMetaScan : save query result: %v", v)
	           }
	   } else {
	           return 0, err
	   }
	*/

	// remove the first argment and put the first argument to Param
	exprs = exprs[1:]

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name:  fulltext_index_tokenize_func_name,
				Param: []byte(params),
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{builder.genNewTag()},
		TblFuncExprList: exprs,
		Children:        []int32{childId},
	}
	return builder.appendNode(node, ctx), nil
}

func (builder *QueryBuilder) getFullTextParams(fn *tree.FuncExpr) (string, error) {
	if _, ok := fn.Exprs[0].(*tree.NumVal); ok {
		return fn.Exprs[0].String(), nil
	}
	return "", moerr.NewNoConfig(builder.GetContext(), "first parameter must be string")
}
