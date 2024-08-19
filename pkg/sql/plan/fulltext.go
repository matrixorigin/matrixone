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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// coldef shall copy index type
var (
	ftIndexColdefs = []*plan.ColDef{
		// row_id type should be same as index type
		{
			Name: "doc_id",
			Typ: plan.Type{
				Id:          int32(types.T_int64),
				NotNullable: false,
				Width:       8,
			},
		},
		{
			Name: "tfidf",
			Typ: plan.Type{
				Id:          int32(types.T_float32),
				NotNullable: false,
				Width:       4,
			},
		},
	}
)

// arg list [index_table_name, pk_type, []indexpart????, search_against]
func (builder *QueryBuilder) buildFullTextIndexScan(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, childId int32) (int32, error) {
	colDefs := _getColDefs(ftIndexColdefs)
	// colName := findColName(tbl.Func)

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

	logutil.Infof("FULLTEXTINDEXSCAN PLAN PLAN BUILDER")
	for i, e := range exprs {
		logutil.Infof("FULLTEXT PLAN EXPR %d %s", i, e.String())
	}

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name:  "fulltext_index_scan",
				Param: []byte("hello"),
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{builder.genNewTag()},
		TblFuncExprList: exprs,
		Children:        []int32{childId},
	}
	return builder.appendNode(node, ctx), nil
}
