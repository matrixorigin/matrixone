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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var (
	TableStatsColDefs  []*plan.ColDef
	TableStatsColTypes []types.Type
)

func init() {
	// Define columns for table_stats output
	// Aligned with pb.StatsInfo fields
	// Maps are represented as JSON TEXT

	TableStatsColTypes = []types.Type{
		types.New(types.T_varchar, types.MaxVarcharLen, 0), // table_name
		types.New(types.T_float64, 0, 0),                   // table_cnt
		types.New(types.T_int64, 0, 0),                     // block_number
		types.New(types.T_int64, 0, 0),                     // approx_object_number
		types.New(types.T_int64, 0, 0),                     // accurate_object_number
		types.New(types.T_float64, 0, 0),                   // sampling_ratio
		types.New(types.T_text, 0, 0),                      // ndv_map (JSON)
		types.New(types.T_text, 0, 0),                      // min_val_map (JSON)
		types.New(types.T_text, 0, 0),                      // max_val_map (JSON)
		types.New(types.T_text, 0, 0),                      // data_type_map (JSON)
		types.New(types.T_text, 0, 0),                      // null_cnt_map (JSON)
		types.New(types.T_text, 0, 0),                      // size_map (JSON)
		types.New(types.T_text, 0, 0),                      // shuffle_range_map (JSON)
	}

	colNames := []string{
		"table_name",
		"table_cnt",
		"block_number",
		"approx_object_number",
		"accurate_object_number",
		"sampling_ratio",
		"ndv_map",
		"min_val_map",
		"max_val_map",
		"data_type_map",
		"null_cnt_map",
		"size_map",
		"shuffle_range_map",
	}

	TableStatsColDefs = make([]*plan.ColDef, len(colNames))
	for i, name := range colNames {
		tp := TableStatsColTypes[i]
		TableStatsColDefs[i] = &plan.ColDef{
			Name: name,
			Typ: plan.Type{
				Id:          int32(tp.Oid),
				Width:       tp.Width,
				Scale:       tp.Scale,
				NotNullable: true,
			},
			Default: &plan.Default{
				NullAbility:  false,
				Expr:         nil,
				OriginString: "",
			},
		}
	}
}

func (builder *QueryBuilder) buildTableStats(_ *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) int32 {
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name: "table_stats",
			},
			Cols: TableStatsColDefs,
		},
		BindingTags:     []int32{builder.genNewBindTag()},
		Children:        children,
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx)
}
