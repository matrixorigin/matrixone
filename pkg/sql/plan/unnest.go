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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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
	paramData, err := tbl.Param.Marshal()
	if err != nil {
		return 0, err
	}
	var scanNode *plan.Node
	var childId int32 = -1
	switch o := tbl.Param.Origin.(type) {
	case *tree.UnresolvedName:
		schemaName, tableName, colName := o.GetNames()
		if len(schemaName) == 0 {
			cteRef := ctx.findCTE(tableName)
			if cteRef != nil {
				subCtx := NewBindContext(builder, ctx)
				subCtx.maskedCTEs = cteRef.maskedCTEs
				subCtx.cteName = tableName
				//reset defaultDatabase
				if len(cteRef.defaultDatabase) > 0 {
					subCtx.defaultDatabase = cteRef.defaultDatabase
				}

				switch stmt := cteRef.ast.Stmt.(type) {
				case *tree.Select:
					childId, err = builder.buildSelect(stmt, subCtx, false)

				case *tree.ParenSelect:
					childId, err = builder.buildSelect(stmt.Select, subCtx, false)

				default:
					err = moerr.NewParseError("unexpected statement: '%v'", tree.String(stmt, dialect.MYSQL))
				}

				if err != nil {
					return 0, err
				}

				if subCtx.isCorrelated {
					return 0, moerr.NewNYI("correlated column in CTE")
				}

				if subCtx.hasSingleRow {
					ctx.hasSingleRow = true
				}

				cols := cteRef.ast.Name.Cols

				if len(cols) > len(subCtx.headings) {
					return 0, moerr.NewSyntaxError("table %q has %d columns available but %d columns specified", tableName, len(subCtx.headings), len(cols))
				}

				for i, col := range cols {
					subCtx.headings[i] = string(col)
				}
				break
			}
			schemaName = ctx.defaultDatabase
		}
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
	case string:
		if len(o) == 0 {
			return 0, moerr.NewInvalidInput("unnest param is empty")
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
		return 0, moerr.NewInvalidInput("unsupported unnest param type: %T", o)
	}
	if scanNode != nil {
		childId = builder.appendNode(scanNode, ctx)
	}
	colDefs := _getDefaultColDefs()
	node := &plan.Node{
		NodeType: plan.Node_UNNEST,
		Cost:     &plan.Cost{},
		TableDef: &plan.TableDef{
			TableType:          "func_table", //test if ok
			Name:               tbl.String(),
			TableFunctionParam: paramData,
			Cols:               colDefs,
		},
		BindingTags: []int32{builder.genNewTag()},
	}
	node.Children = []int32{childId}
	nodeID := builder.appendNode(node, ctx)
	return nodeID, nil
}

func IsUnnestValueScan(node *plan.Node) bool { // distinguish unnest value scan and normal value scan,maybe change to a better way in the future
	// node must be a value scan
	return node.TableDef != nil && len(node.TableDef.TableFunctionParam) > 0
}
