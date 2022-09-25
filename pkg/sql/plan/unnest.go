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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/unnest"
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

func (builder *QueryBuilder) buildUnnest(tbl *tree.TableFunction, ctx *BindContext) (int32, error) {
	tag := builder.genNewTag()
	var (
		err             error
		externParamData []byte
		externParam     *unnest.ExternalParam
	)
	if externParam, err = genTblParam(tbl); err != nil {
		return 0, err
	}
	if externParamData, err = json.Marshal(externParam); err != nil {
		return 0, err
	}

	colDefs := _getDefaultColDefs()
	node := &plan.Node{
		NodeType: plan.Node_TABLE_FUNCTION,
		Cost:     &plan.Cost{},
		TableDef: &plan.TableDef{
			TableType: catalog.SystemViewRel, //test if ok
			//Name:               tbl.String(),
			Cols:               colDefs,
			TableFunctionParam: externParamData,
			TableFunctionName:  "unnest",
		},
		BindingTags: []int32{tag},
	}
	var scanNode *plan.Node
	switch o := tbl.Func.Exprs[0].(type) {
	case *tree.UnresolvedName:
		schemaName, tableName, colName := o.GetNames()
		objRef, tableDef := builder.compCtx.Resolve(schemaName, tableName)
		if objRef == nil {
			return 0, moerr.NewSyntaxError("schema %s not found", schemaName)
		}
		if tableDef == nil {
			return 0, moerr.NewSyntaxError("table %s not found", tableName)
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
	case *tree.NumVal:
		jsonStr := o.String()
		if len(jsonStr) == 0 {
			return 0, moerr.NewInvalidInput("unnest param is empty")
		}
		scanNode = &plan.Node{
			NodeType: plan.Node_VALUE_SCAN,
			TableDef: &plan.TableDef{
				TableFunctionParam: []byte(jsonStr),
			},
			ProjectList: []*plan.Expr{
				{
					Typ:  &plan.Type{Id: int32(types.T_varchar)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
				},
			},
		}
	default:
		return 0, moerr.NewInvalidInput("unsupported unnest param type: %T", o)
	}
	childId := builder.appendNode(scanNode, ctx)
	node.Children = []int32{childId}
	nodeID := builder.appendNode(node, ctx)
	return nodeID, nil
}

func genTblParam(tbl *tree.TableFunction) (*unnest.ExternalParam, error) {
	var err error
	uParam := &unnest.ExternalParam{}
	switch o := tbl.Func.Exprs[0].(type) {
	case *tree.UnresolvedName:
		_, _, uParam.ColName = o.GetNames()
	case *tree.NumVal:
	default:
		return nil, moerr.NewNYI("unsupported unnest param type: %T", o)
	}
	uParam.Path = tbl.Func.Exprs[1].String()
	uParam.Outer = tbl.Func.Exprs[2].(*tree.NumVal).String() == "true"
	return uParam, err
}
