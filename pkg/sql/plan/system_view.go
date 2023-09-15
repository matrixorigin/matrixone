// Copyright 2022 Matrix Origin
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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var MoConfigColNames = []string{
	"node_type",
	"node_id",
	"name",
	"current_value",
	"default_value",
	"internal",
}

var MoConfigColTypes = []types.Type{
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
	types.New(types.T_varchar, types.MaxVarcharLen, 0),
}

var MoConfigColName2Index = map[string]int32{
	"node_type":     0,
	"node_id":       1,
	"name":          2,
	"current_value": 3,
	"default_value": 4,
	"internal":      5,
}

type MoConfigColType int32

const (
	MoConfigColTypeNodeType = iota
	MoConfigColTypeNodeId
	MoConfigColTypeName
	MoConfigColTypeCurrentValue
	MoConfigColTypeDefaultValue
	MoConfigColTypeInternal
)

func (builder *QueryBuilder) buildMoConfigurations(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, childId int32) (int32, error) {
	var err error

	colDefs := make([]*plan.ColDef, 0, len(MoConfigColNames))

	for i, name := range MoConfigColNames {
		colDefs = append(colDefs, &plan.ColDef{
			Name: name,
			Typ: &plan.Type{
				Id:    int32(MoConfigColTypes[i].Oid),
				Width: MoConfigColTypes[i].Width,
			},
		})
	}

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name: "mo_configurations",
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{builder.genNewTag()},
		Children:        []int32{childId},
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx), err
}
