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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var (
	MetaColDefs = []*plan.ColDef{
		makeMetaScanColDef(catalog.QUERY_ID_IDX),
		makeMetaScanColDef(catalog.STATEMENT_IDX),
		makeMetaScanColDef(catalog.ACCOUNT_ID_IDX),
		makeMetaScanColDef(catalog.ROLE_ID_IDX),
		makeMetaScanColDef(catalog.RESULT_PATH_IDX, 4),
		makeMetaScanColDef(catalog.CREATE_TIME_IDX),
		makeMetaScanColDef(catalog.RESULT_SIZE_IDX),
		makeMetaScanColDef(catalog.TABLES_IDX),
		makeMetaScanColDef(catalog.USER_ID_IDX),
		makeMetaScanColDef(catalog.EXPIRED_TIME_IDX),
		makeMetaScanColDef(catalog.COLUMN_MAP_IDX),
		makeMetaScanColDef(catalog.SAVED_ROW_COUNT_IDX),
		makeMetaScanColDef(catalog.QUERY_ROW_COUNT_IDX),
	}
)

func makeMetaScanColDef(index int, widthOverride ...int32) *plan.ColDef {
	typ := makePlan2Type(&catalog.MetaColTypes[index])
	if len(widthOverride) > 0 {
		typ.Width = widthOverride[0]
	}
	return &plan.ColDef{
		Name: catalog.MetaColNames[index],
		Typ:  typ,
	}
}

func (builder *QueryBuilder) buildMetaScan(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	var err error
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
	exprs[0], err = appendCastBeforeExpr(builder.GetContext(), exprs[0], plan.Type{
		Id:          int32(types.T_uuid),
		NotNullable: true,
	})
	if err != nil {
		return 0, err
	}

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name: "meta_scan",
			},
			Cols: MetaColDefs,
		},
		BindingTags:     []int32{builder.genNewBindTag()},
		Children:        children,
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx), nil
}
