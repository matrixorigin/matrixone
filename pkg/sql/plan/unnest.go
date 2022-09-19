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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

var (
	defaultColDefs = []*plan.ColDef{
		{
			Name: "col",
			Typ: &plan.Type{
				Id:       int32(types.T_varchar),
				Nullable: true,
				Width:    4,
			},
		},
		{
			Name: "seq",
			Typ: &plan.Type{
				Id:       int32(types.T_int32),
				Nullable: true,
			},
		},
		{
			Name: "key",
			Typ: &plan.Type{
				Id:       int32(types.T_varchar),
				Nullable: true,
				Width:    256,
			},
		},
		{
			Name: "path",
			Typ: &plan.Type{
				Id:       int32(types.T_varchar),
				Nullable: true,
				Width:    256,
			},
		},
		{
			Name: "index",
			Typ: &plan.Type{
				Id:       int32(types.T_int32),
				Nullable: true,
				Width:    4,
			},
		},
		{
			Name: "value",
			Typ: &plan.Type{
				Id:       int32(types.T_varchar),
				Nullable: true,
				Width:    1024,
			},
		},
		{
			Name: "this",
			Typ: &plan.Type{
				Id:       int32(types.T_varchar),
				Nullable: true,
				Width:    1024,
			},
		},
	}
)

func _dupType(typ *plan.Type) *plan.Type {
	return &plan.Type{
		Id:        typ.Id,
		Nullable:  typ.Nullable,
		Size:      typ.Size,
		Width:     typ.Width,
		Scale:     typ.Scale,
		Precision: typ.Precision,
	}
}

func _dupColDef(src *plan.ColDef) *plan.ColDef {
	return &plan.ColDef{
		Name: src.Name,
		Typ:  _dupType(src.Typ),
	}
}

func _getDefaultColDefs() []*plan.ColDef {
	ret := make([]*plan.ColDef, 0, len(defaultColDefs))
	for _, v := range defaultColDefs {
		ret = append(ret, _dupColDef(v))
	}
	return ret
}

func (builder *QueryBuilder) buildUnnest(tbl *tree.Unnest, ctx *BindContext) (int32, error) {
	tag := builder.genNewTag()
	paramData, err := tbl.Param.Marshal()
	if err != nil {
		return 0, err
	}
	colDefs := _getDefaultColDefs()
	node := &plan.Node{
		NodeType: plan.Node_UNNEST,
		Cost:     &plan.Cost{},
		TableDef: &plan.TableDef{
			TableType:          catalog.SystemViewRel, //test if ok
			Name:               tbl.String(),
			TableFunctionParam: paramData,
			Cols:               colDefs,
		},
		BindingTags: []int32{tag},
	}
	var scanNode *plan.Node
	switch o := tbl.Param.Origin.(type) {
	case *tree.UnresolvedName:
		schemaName, tableName, colName := o.GetNames()
		objRef, tableDef := builder.compCtx.Resolve(schemaName, tableName)
		if objRef == nil {
			return 0, fmt.Errorf("schema %s not found", schemaName)
		}
		if tableDef == nil {
			return 0, fmt.Errorf("table %s not found", tableName)
		}
		scanNode = &plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			TableDef:    tableDef,
			ObjRef:      objRef,
			Cost:        builder.compCtx.Cost(objRef, nil),
			BindingTags: []int32{builder.genNewTag()},
		}
		for i := 0; i < len(tableDef.Cols); i++ {
			if tableDef.Cols[i].Name == colName {
				tmp := &plan.Expr{
					Typ: tableDef.Cols[i].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							ColPos: int32(i),
							Name:   tableName + "." + tableDef.Cols[i].Name,
						},
					},
				}
				scanNode.ProjectList = append(scanNode.ProjectList, tmp)
				break
			}
		}
	case string:
		if len(o) == 0 {
			return 0, fmt.Errorf("unnest param is empty")
		}
		scanNode = &plan.Node{
			NodeType: plan.Node_VALUE_SCAN,
			TableDef: &plan.TableDef{
				TableFunctionParam: []byte(o),
			},
			ProjectList: []*plan.Expr{
				{
					Typ:  &plan.Type{Id: int32(types.T_varchar)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
				},
			},
		}
	default:
		return 0, fmt.Errorf("unsupport param type %T", o)
	}
	childId := builder.appendNode(scanNode, ctx)
	node.Children = []int32{childId}
	nodeID := builder.appendNode(node, ctx)
	return nodeID, nil
}

func IsUnnestValueScan(node *plan.Node) bool { // distinguish unnest value scan and normal value scan,maybe change to a better way in the future
	// node must be a value scan
	return node.TableDef != nil && len(node.TableDef.TableFunctionParam) > 0
}
