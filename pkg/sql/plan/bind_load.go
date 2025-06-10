// Copyright 2021 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (builder *QueryBuilder) bindLoad(stmt *tree.Load, bindCtx *BindContext) (int32, error) {
	dmlCtx := NewDMLContext()
	builder.qry.LoadTag = true
	lastNodeID, insertColToExpr, err := builder.bindExternalScan(stmt, bindCtx, dmlCtx)
	if err != nil {
		return -1, err
	}

	lastNodeID, colName2Idx, skipUniqueIdx, err := builder.appendNodesForInsertStmt(bindCtx, lastNodeID, dmlCtx.tableDefs[0], dmlCtx.objRefs[0], insertColToExpr)
	if err != nil {
		return -1, err
	}

	return builder.appendDedupAndMultiUpdateNodesForBindInsert(bindCtx, dmlCtx, lastNodeID, colName2Idx, skipUniqueIdx, nil)
}

func (builder *QueryBuilder) bindExternalScan(
	stmt *tree.Load,
	bindCtx *BindContext,
	dmlCtx *DMLContext) (int32, map[string]*plan.Expr, error) {
	externalScanTag := builder.genNewTag()
	err := dmlCtx.ResolveTables(builder.compCtx, tree.TableExprs{stmt.Table}, nil, nil, true)
	if err != nil {
		return -1, nil, err
	}

	ctx := builder.compCtx
	stmt.Param.Local = stmt.Local
	fileName, err := checkFileExist(stmt.Param, ctx)
	if err != nil {
		return -1, nil, err
	}

	if err := InitNullMap(stmt.Param, ctx); err != nil {
		return -1, nil, err
	}

	tableDef := DeepCopyTableDef(dmlCtx.tableDefs[0], true)
	objRef := dmlCtx.objRefs[0]

	// load with columnlist will copy a new tableDef
	tbColToDataCol := make(map[string]int32)
	insertColToExpr := make(map[string]*plan.Expr)
	if stmt.Param.ScanType != tree.INLINE || len(stmt.Param.Tail.ColumnList) > 0 {
		if len(stmt.Param.Tail.ColumnList) > 0 {
			colToIndex := make(map[string]int32, 0)
			var newCols []*ColDef
			colPos := 0

			for i, col := range stmt.Param.Tail.ColumnList {
				switch realCol := col.(type) {
				case *tree.UnresolvedName:
					colName := realCol.ColName()
					if _, ok := tableDef.Name2ColIndex[colName]; !ok {
						return -1, nil, moerr.NewInternalErrorf(ctx.GetContext(), "column '%s' does not exist", colName)
					}
					tbColIdx := tableDef.Name2ColIndex[colName]
					colExpr := &plan.Expr{
						Typ: tableDef.Cols[tbColIdx].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: externalScanTag,
								ColPos: int32(colPos),
								Name:   tableDef.Name + "." + colName,
							},
						},
					}
					insertColToExpr[colName] = colExpr
					colToIndex[colName] = int32(colPos)
					colPos++
					tbColToDataCol[colName] = int32(i)
					newCols = append(newCols, tableDef.Cols[tbColIdx])
				case *tree.VarExpr:
					//NOTE:variable like '@abc' will be passed by.
					name := realCol.Name
					tbColToDataCol[name] = -1 // when in external call, can use len of the map to check load data row whether valid
				default:
					return -1, nil, moerr.NewInternalErrorf(ctx.GetContext(), "unsupported column type %v", realCol)
				}
			}
			tableDef.Cols = newCols
			tableDef.Name2ColIndex = colToIndex
		}
	}

	if len(tbColToDataCol) == 0 {
		idx := 0
		for _, col := range tableDef.Cols {
			if !col.Hidden {
				tbColToDataCol[col.Name] = int32(len(insertColToExpr))

				insertColToExpr[col.Name] = &plan.Expr{
					Typ: col.Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: externalScanTag,
							ColPos: int32(idx),
						},
					},
				}
				idx++
			}
		}
	}

	if err := checkNullMap(stmt, tableDef.Cols, ctx); err != nil {
		return -1, nil, err
	}

	noCompress := getCompressType(stmt.Param, fileName) == tree.NOCOMPRESS
	var offset int64 = 0
	if stmt.Param.Tail.IgnoredLines > 0 && stmt.Param.Parallel && noCompress && !stmt.Param.Local {
		offset, err = IgnoredLines(stmt.Param, ctx)
		if err != nil {
			return -1, nil, err
		}
		stmt.Param.FileStartOff = offset
	}

	if stmt.Param.FileSize-offset < int64(LoadParallelMinSize) {
		stmt.Param.Parallel = false
	}

	stmt.Param.Tail.ColumnList = nil
	if stmt.Param.ScanType != tree.INLINE {
		json_byte, err := json.Marshal(stmt.Param)
		if err != nil {
			return -1, nil, err
		}
		tableDef.Createsql = string(json_byte)
	}

	terminated := ","
	enclosedBy := []byte("\"")
	escapedBy := []byte{0}
	if stmt.Param.Tail.Fields != nil {
		if stmt.Param.Tail.Fields.EnclosedBy != nil {
			if stmt.Param.Tail.Fields.EnclosedBy.Value != 0 {
				enclosedBy = []byte{stmt.Param.Tail.Fields.EnclosedBy.Value}
			}
		}
		if stmt.Param.Tail.Fields.EscapedBy != nil {
			if stmt.Param.Tail.Fields.EscapedBy.Value != 0 {
				escapedBy = []byte{stmt.Param.Tail.Fields.EscapedBy.Value}
			}
		}
		if stmt.Param.Tail.Fields.Terminated != nil {
			terminated = stmt.Param.Tail.Fields.Terminated.Value
		}
	}

	externalScanNode := &plan.Node{
		NodeType: plan.Node_EXTERNAL_SCAN,
		Stats:    &plan.Stats{},
		ObjRef:   objRef,
		TableDef: tableDef,
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_LOAD),
			LoadType:       int32(stmt.Param.ScanType),
			Data:           stmt.Param.Data,
			Format:         stmt.Param.Format,
			IgnoredLines:   uint64(stmt.Param.Tail.IgnoredLines),
			EnclosedBy:     enclosedBy,
			Terminated:     terminated,
			EscapedBy:      escapedBy,
			JsonType:       stmt.Param.JsonData,
			TbColToDataCol: tbColToDataCol,
		},
		BindingTags: []int32{externalScanTag},
	}
	lastNodeId := builder.appendNode(externalScanNode, bindCtx)

	return lastNodeId, insertColToExpr, nil
}
