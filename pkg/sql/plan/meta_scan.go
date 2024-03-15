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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var (
	MetaColDefs = []*plan.ColDef{
		{
			Name: catalog.MetaColNames[catalog.QUERY_ID_IDX],
			Typ: plan.Type{
				Id:          int32(catalog.MetaColTypes[catalog.QUERY_ID_IDX].Oid),
				NotNullable: false,
			},
		},
		{
			Name: catalog.MetaColNames[catalog.STATEMENT_IDX],
			Typ: plan.Type{
				Id:          int32(catalog.MetaColTypes[catalog.STATEMENT_IDX].Oid),
				NotNullable: false,
			},
		},
		{
			Name: catalog.MetaColNames[catalog.ACCOUNT_ID_IDX],
			Typ: plan.Type{
				Id:          int32(catalog.MetaColTypes[catalog.ACCOUNT_ID_IDX].Oid),
				NotNullable: false,
			},
		},
		{
			Name: catalog.MetaColNames[catalog.ROLE_ID_IDX],
			Typ: plan.Type{
				Id:          int32(catalog.MetaColTypes[catalog.ROLE_ID_IDX].Oid),
				NotNullable: false,
			},
		},
		{
			Name: catalog.MetaColNames[catalog.RESULT_PATH_IDX],
			Typ: plan.Type{
				Id:          int32(catalog.MetaColTypes[catalog.RESULT_PATH_IDX].Oid),
				NotNullable: false,
				Width:       4,
			},
		},
		{
			Name: catalog.MetaColNames[catalog.CREATE_TIME_IDX],
			Typ: plan.Type{
				Id:          int32(catalog.MetaColTypes[catalog.CREATE_TIME_IDX].Oid),
				NotNullable: false,
			},
		},
		{
			Name: catalog.MetaColNames[catalog.RESULT_SIZE_IDX],
			Typ: plan.Type{
				Id:          int32(catalog.MetaColTypes[catalog.RESULT_SIZE_IDX].Oid),
				NotNullable: false,
			},
		},
		{
			Name: catalog.MetaColNames[catalog.TABLES_IDX],
			Typ: plan.Type{
				Id:          int32(catalog.MetaColTypes[catalog.TABLES_IDX].Oid),
				NotNullable: false,
			},
		},
		{
			Name: catalog.MetaColNames[catalog.USER_ID_IDX],
			Typ: plan.Type{
				Id:          int32(catalog.MetaColTypes[catalog.USER_ID_IDX].Oid),
				NotNullable: false,
			},
		},
		{
			Name: catalog.MetaColNames[catalog.EXPIRED_TIME_IDX],
			Typ: plan.Type{
				Id:          int32(catalog.MetaColTypes[catalog.EXPIRED_TIME_IDX].Oid),
				NotNullable: false,
			},
		},
		{
			Name: catalog.MetaColNames[catalog.COLUMN_MAP_IDX],
			Typ: plan.Type{
				Id:          int32(catalog.MetaColTypes[catalog.COLUMN_MAP_IDX].Oid),
				NotNullable: false,
			},
		},
	}
)

func (builder *QueryBuilder) buildMetaScan(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, childId int32) (int32, error) {
	var err error
	val, err := builder.compCtx.ResolveVariable("save_query_result", true, true)
	if err == nil {
		if v, _ := val.(int8); v == 0 {
			return 0, moerr.NewNoConfig(builder.GetContext(), "save query result")
		} else {
			logutil.Infof("buildMetaScan : save query result: %v", v)
		}
	} else {
		return 0, err
	}
	exprs[0], err = appendCastBeforeExpr(builder.GetContext(), exprs[0], &plan.Type{
		Id:          int32(types.T_uuid),
		NotNullable: true,
	})
	if err != nil {
		return 0, err
	}
	// calculate uuid
	vec, err := colexec.EvalExpressionOnce(builder.compCtx.GetProcess(), exprs[0], []*batch.Batch{batch.EmptyForConstFoldBatch})
	if err != nil {
		return 0, err
	}
	uuid := vector.MustFixedCol[types.Uuid](vec)[0]
	vec.Free(builder.compCtx.GetProcess().GetMPool())

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			Name:      uuid.ToString(),
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name: "meta_scan",
			},
			Cols: MetaColDefs,
		},
		BindingTags:     []int32{builder.genNewTag()},
		Children:        []int32{childId},
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx), nil
}
