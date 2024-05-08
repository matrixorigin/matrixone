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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var (
	MetadataScanColDefs   = []*plan.ColDef{}
	MetadataScanColTypes  = []types.Type{}
	Metadata_Rows_Cnt_Pos int32
)

func init() {
	// MAKE SURE THE TYPE ENUM BEGIN FROM 0 OR YOU WILL GET WRONG
	// WHEN YOU FILL THE SLICE
	mlen := len(plan.MetadataScanInfo_MetadataScanInfoType_name)
	MetadataScanColTypes = make([]types.Type, mlen)
	MetadataScanColDefs = make([]*plan.ColDef, mlen)
	for i := range plan.MetadataScanInfo_MetadataScanInfoType_name {
		var tp types.Type
		switch plan.MetadataScanInfo_MetadataScanInfoType(i) {
		case plan.MetadataScanInfo_COL_NAME:
			tp = types.New(types.T_varchar, types.MaxVarcharLen, 0)
		case plan.MetadataScanInfo_OBJECT_NAME:
			tp = types.New(types.T_varchar, types.MaxVarcharLen, 0)
		case plan.MetadataScanInfo_IS_HIDDEN:
			tp = types.New(types.T_bool, 0, 0)
		case plan.MetadataScanInfo_OBJ_LOC:
			tp = types.New(types.T_varchar, types.MaxVarcharLen, 0)
		case plan.MetadataScanInfo_CREATE_TS:
			tp = types.New(types.T_TS, types.MaxVarcharLen, 0)
		case plan.MetadataScanInfo_DELETE_TS:
			tp = types.New(types.T_TS, types.MaxVarcharLen, 0)
		case plan.MetadataScanInfo_ROWS_CNT:
			tp = types.New(types.T_int64, 0, 0)
			Metadata_Rows_Cnt_Pos = i
		case plan.MetadataScanInfo_NULL_CNT:
			tp = types.New(types.T_int64, 0, 0)
		case plan.MetadataScanInfo_COMPRESS_SIZE:
			tp = types.New(types.T_int64, 0, 0)
		case plan.MetadataScanInfo_ORIGIN_SIZE:
			tp = types.New(types.T_int64, 0, 0)
		case plan.MetadataScanInfo_MIN: // TODO: find a way to show this info
			tp = types.New(types.T_varbinary, types.MaxVarcharLen, 0)
		case plan.MetadataScanInfo_MAX: // TODO: find a way to show this info
			tp = types.New(types.T_varbinary, types.MaxVarcharLen, 0)
		case plan.MetadataScanInfo_SUM: // TODO: find a way to show this info
			tp = types.New(types.T_varbinary, types.MaxVarcharLen, 0)
		default:
			panic("unknown types when gen metadata scan info")
		}

		colname := plan.MetadataScanInfo_MetadataScanInfoType_name[i]
		coldef := &plan.ColDef{
			Name: strings.ToLower(colname),
			Typ: plan.Type{
				Id:          int32(tp.Oid),
				NotNullable: true,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}

		MetadataScanColTypes[i] = tp
		MetadataScanColDefs[i] = coldef
	}
}

func (builder *QueryBuilder) buildMetadataScan(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, childId int32) int32 {
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name: "metadata_scan",
			},
			Cols: MetadataScanColDefs,
		},
		BindingTags:     []int32{builder.genNewTag()},
		Children:        []int32{childId},
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx)
}
