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
	}
)

// arg list [source_table_name, index_table_name, search_against, mode]
func (builder *QueryBuilder) buildFullTextIndexScan(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, childId int32) (int32, error) {

	if len(exprs) != 4 {
		return 0, moerr.NewInvalidInput(builder.GetContext(), "Invalid number of arguments (NARGS != 4).")
	}

	colDefs := _getColDefs(ftIndexColdefs)

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

// select * from index_table, fulltext_index_tokenize(doc_id, concat(body, ' ', title))
// arg list [params, doc_id, content]
func (builder *QueryBuilder) buildFullTextIndexTokenize(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, childId int32) (int32, error) {

	if len(exprs) < 3 {
		return 0, moerr.NewInvalidInput(builder.GetContext(), "Invalid number of arguments (NARGS < 3).")
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

	// set type to source table primary key
	colDefs[0].Typ = pkType

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
