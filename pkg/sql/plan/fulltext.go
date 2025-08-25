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
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
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

// arg list [param, source_table_name, index_table_name, search_against, mode]
func (builder *QueryBuilder) buildFullTextIndexScan(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) (int32, error) {

	if len(exprs) != 5 {
		return 0, moerr.NewInvalidInput(builder.GetContext(), "Invalid number of arguments (NARGS != 5).")
	}

	colDefs := _getColDefs(ftIndexColdefs)

	params, err := builder.getFullTextParams(tbl.Func)
	if err != nil {
		return 0, err
	}
	// remove the first argment and put the first argument to Param
	exprs = exprs[1:]

	// get the generated SQL
	sql, err := builder.getFullTextSql(tbl.Func, params)
	if err != nil {
		return 0, err
	}

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{Sql: sql},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name:  fulltext_index_scan_func_name,
				Param: []byte(params),
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{builder.genNewTag()},
		TblFuncExprList: exprs,
		Children:        children,
	}
	return builder.appendNode(node, ctx), nil
}

func (builder *QueryBuilder) getFullTextSql(fn *tree.FuncExpr, params string) (string, error) {
	var param fulltext.FullTextParserParam
	if len(params) > 0 {
		err := json.Unmarshal([]byte(params), &param)
		if err != nil {
			return "", err
		}
	}

	if _, ok := fn.Exprs[2].(*tree.NumVal); !ok {
		return "", moerr.NewInvalidInput(builder.GetContext(), "index table name is not a constant")
	}

	idxtbl := fn.Exprs[2].String()

	if _, ok := fn.Exprs[3].(*tree.NumVal); !ok {
		return "", moerr.NewInvalidInput(builder.GetContext(), "pattern is not a constant")
	}

	pattern := fn.Exprs[3].String()
	modeVal, ok := fn.Exprs[4].(*tree.NumVal)
	if !ok {
		return "", moerr.NewInvalidInput(builder.GetContext(), "mode is not a constant")
	}
	mode, ok := modeVal.Int64()
	if !ok {
		return "", moerr.NewInvalidInput(builder.GetContext(), "mode is not an integer")
	}

	ps, err := fulltext.ParsePattern(pattern, mode)
	if err != nil {
		return "", err
	}
	scoreAlgo, err := fulltext.GetScoreAlgo(builder.compCtx.GetProcess())
	if err != nil {
		return "", err
	}
	return fulltext.PatternToSql(ps, mode, idxtbl, param.Parser, scoreAlgo)
}

// for table scan, primary key type is passed from TableDef
// select f.* from index_table CROSS APPLY fulltext_index_tokenize('param', doc_id, body, title) as f
// arg list [params, doc_id, part1, part2,...]
//
// for values scan, primary key type is passed from second argument
// arg list [params, pktype, doc_id, part1, part2,...]
//
// select f.* from (select cast(column_0 as bigint) as id, column_1 as body, column_2 as title from (values row(1, 'body content', 'title content'))) as src
// cross apply fulltext_index_tokenize('{"parser":"ngram"}', 23, id, body, title) as f;
//
// for composite primary key,
//
// select f.* from (select serial(cast(column_0 as bigint), cast(column_1 as bigint)) as id, column_2 as body, column_3 as title from
// (values row(1, 2, 'body', 'title'), row(2, 3, 'body is heavy', 'I do not know'))) as src
// cross apply fulltext_index_tokenize('{"parser":"ngram"}', 61, id, body, title) as f;
//
// for composite key, use hex string X'abcd' as input of primary key and skip serial()
// select unhex(hex(serial(cast(0 as smallint), cast(1 as int))));
func (builder *QueryBuilder) buildFullTextIndexTokenize(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) (int32, error) {

	if len(exprs) < 3 {
		return 0, moerr.NewInvalidInput(builder.GetContext(), "Invalid number of arguments (NARGS < 3).")
	}

	colDefs := _getColDefs(tokenizeColDefs)
	params, err := builder.getFullTextParams(tbl.Func)
	if err != nil {
		return 0, err
	}

	scanNode := builder.qry.Nodes[children[0]]
	if scanNode.NodeType == plan.Node_TABLE_SCAN {
		if len(exprs) < 3 {
			return 0, moerr.NewInvalidInput(builder.GetContext(), "Invalid number of arguments (NARGS < 3).")
		}
		pkPos := scanNode.TableDef.Name2ColIndex[scanNode.TableDef.Pkey.PkeyColName]
		pkType := scanNode.TableDef.Cols[pkPos].Typ
		// set type to source table primary key
		colDefs[0].Typ = pkType
		// remove the first argment and put the first argument to Param
		exprs = exprs[1:]
	} else {
		// VALUES.  First argument is Params and second argument is pkType
		if len(exprs) < 4 {
			return 0, moerr.NewInvalidInput(builder.GetContext(), "Invalid number of arguments (NARGS < 4).")
		}
		pkType, err := builder.getFullTextPkeyType(tbl.Func)
		if err != nil {
			return 0, err
		}

		colDefs[0].Typ = pkType
		// remove the first two argments and put the first argument to Param
		exprs = exprs[2:]

	}

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
		Children:        children,
	}
	return builder.appendNode(node, ctx), nil
}

func (builder *QueryBuilder) getFullTextParams(fn *tree.FuncExpr) (string, error) {
	if _, ok := fn.Exprs[0].(*tree.NumVal); ok {
		return fn.Exprs[0].String(), nil
	}
	return "", moerr.NewNoConfig(builder.GetContext(), "first parameter must be string")
}

func (builder *QueryBuilder) getFullTextPkeyType(fn *tree.FuncExpr) (plan.Type, error) {
	if v, ok := fn.Exprs[1].(*tree.NumVal); ok {
		if t64, ok2 := v.Int64(); ok2 {
			return plan.Type{
				Id:          int32(t64),
				NotNullable: false,
			}, nil
		}
		return plan.Type{}, moerr.NewNoConfig(builder.GetContext(), "second parameter must be int32")
	}
	return plan.Type{}, moerr.NewNoConfig(builder.GetContext(), "second parameter must be int32")
}
