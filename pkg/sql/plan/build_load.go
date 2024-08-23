// Copyright 2021 - 2022 Matrix Origin
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
	"encoding/json"
	"strings"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

const (
	LoadParallelMinSize = 1 << 20
)

func getNormalExternalProject(stmt *tree.Load, ctx CompilerContext, tableDef *TableDef, tblName string) ([]*Expr, map[string]int32, map[string]int32, *TableDef, error) {
	var externalProject []*Expr
	colToIndex := make(map[string]int32, 0)
	for i, col := range tableDef.Cols {
		if col.Name != catalog.FakePrimaryKeyColName {
			colExpr := &plan.Expr{
				Typ: tableDef.Cols[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						ColPos: int32(i),
						Name:   tblName + "." + tableDef.Cols[i].Name,
					},
				},
			}
			externalProject = append(externalProject, colExpr)
			colToIndex[col.Name] = int32(i)
		}
	}
	return externalProject, colToIndex, colToIndex, tableDef, nil
}

func getExternalWithColListProject(stmt *tree.Load, ctx CompilerContext, tableDef *TableDef, tblName string) ([]*Expr, map[string]int32, map[string]int32, *TableDef, error) {
	var externalProject []*Expr
	colToIndex := make(map[string]int32, 0)
	tbColToDataCol := make(map[string]int32, 0)
	var newCols []*ColDef

	newTableDef := DeepCopyTableDef(tableDef, true)
	colPos := 0
	for i, col := range stmt.Param.Tail.ColumnList {
		switch realCol := col.(type) {
		case *tree.UnresolvedName:
			colName := realCol.ColName()
			if _, ok := newTableDef.Name2ColIndex[colName]; !ok {
				return nil, nil, nil, nil, moerr.NewInternalError(ctx.GetContext(), "column '%s' does not exist", colName)
			}
			tbColIdx := newTableDef.Name2ColIndex[colName]
			colExpr := &plan.Expr{
				Typ: newTableDef.Cols[tbColIdx].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						ColPos: int32(colPos),
						Name:   tblName + "." + colName,
					},
				},
			}
			externalProject = append(externalProject, colExpr)
			colToIndex[colName] = int32(colPos)
			colPos++
			tbColToDataCol[colName] = int32(i)
			newCols = append(newCols, newTableDef.Cols[tbColIdx])
		case *tree.VarExpr:
			//NOTE:variable like '@abc' will be passed by.
			name := realCol.Name
			tbColToDataCol[name] = -1 // when in external call, can use len of the map to check load data row whether valid
		default:
			return nil, nil, nil, nil, moerr.NewInternalError(ctx.GetContext(), "unsupported column type %v", realCol)
		}
	}

	newTableDef.Cols = newCols
	return externalProject, colToIndex, tbColToDataCol, newTableDef, nil
}

func getExternalProject(stmt *tree.Load, ctx CompilerContext, tableDef *TableDef, tblName string) ([]*Expr, map[string]int32, map[string]int32, *TableDef, error) {
	if len(stmt.Param.Tail.ColumnList) == 0 {
		return getNormalExternalProject(stmt, ctx, tableDef, tblName)
	} else {
		return getExternalWithColListProject(stmt, ctx, tableDef, tblName)
	}
}

func buildLoad(stmt *tree.Load, ctx CompilerContext, isPrepareStmt bool) (*Plan, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildLoadHistogram.Observe(time.Since(start).Seconds())
	}()
	tblName := string(stmt.Table.ObjectName)
	tblInfo, err := getDmlTableInfo(ctx, tree.TableExprs{stmt.Table}, nil, nil, "insert")
	if err != nil {
		return nil, err
	}

	stmt.Param.Local = stmt.Local
	fileName, err := checkFileExist(stmt.Param, ctx)
	if err != nil {
		return nil, err
	}

	if err := InitNullMap(stmt.Param, ctx); err != nil {
		return nil, err
	}
	tableDef := tblInfo.tableDefs[0]
	objRef := tblInfo.objRef[0]

	tableDef.Name2ColIndex = map[string]int32{}
	for i, col := range tableDef.Cols {
		if col.Name != catalog.FakePrimaryKeyColName {
			tableDef.Name2ColIndex[col.Name] = int32(i)
		}
	}
	originTableDef := tableDef
	// load with columnlist will copy a new tableDef
	externalProject, colToIndex, tbColToDataCol, tableDef, err := getExternalProject(stmt, ctx, tableDef, tblName)
	if err != nil {
		return nil, err
	}

	if err := checkNullMap(stmt, tableDef.Cols, ctx); err != nil {
		return nil, err
	}

	if stmt.Param.FileSize < LoadParallelMinSize {
		stmt.Param.Parallel = false
	}

	stmt.Param.LoadFile = true
	stmt.Param.Tail.ColumnList = nil
	if stmt.Param.ScanType != tree.INLINE {
		json_byte, err := json.Marshal(stmt.Param)
		if err != nil {
			return nil, err
		}
		tableDef.Createsql = string(json_byte)
	}

	builder := NewQueryBuilder(plan.Query_SELECT, ctx, isPrepareStmt, false)
	bindCtx := NewBindContext(builder, nil)
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
		NodeType:    plan.Node_EXTERNAL_SCAN,
		Stats:       &plan.Stats{},
		ProjectList: externalProject,
		ObjRef:      objRef,
		TableDef:    tableDef,
		ExternScan: &plan.ExternScan{
			Type:           int32(stmt.Param.ScanType),
			Data:           stmt.Param.Data,
			Format:         stmt.Param.Format,
			IgnoredLines:   uint64(stmt.Param.Tail.IgnoredLines),
			EnclosedBy:     enclosedBy,
			Terminated:     terminated,
			EscapedBy:      escapedBy,
			JsonType:       stmt.Param.JsonData,
			TbColToDataCol: tbColToDataCol,
		},
	}
	lastNodeId := builder.appendNode(externalScanNode, bindCtx)

	projectNode := &plan.Node{
		Children: []int32{lastNodeId},
		NodeType: plan.Node_PROJECT,
		Stats:    &plan.Stats{},
	}

	ifExistAutoPkCol, err := getProjectNode(stmt, ctx, projectNode, originTableDef, colToIndex)

	if err != nil {
		return nil, err
	}

	inlineDataSize := unsafe.Sizeof(stmt.Param.Data)
	builder.qry.LoadWriteS3 = true
	noCompress := getCompressType(stmt.Param, fileName) == tree.NOCOMPRESS
	if noCompress && (stmt.Param.FileSize < LoadParallelMinSize || inlineDataSize < LoadParallelMinSize) {
		builder.qry.LoadWriteS3 = false
	}

	if stmt.Param.Parallel && (!noCompress || stmt.Local) {
		projectNode.ProjectList = makeCastExpr(stmt, fileName, originTableDef, projectNode)
	}
	lastNodeId = builder.appendNode(projectNode, bindCtx)
	builder.qry.LoadTag = true

	//append lock node
	if lockNodeId, ok := appendLockNode(
		builder,
		bindCtx,
		lastNodeId,
		originTableDef,
		true,
		false,
		-1,
		nil,
		false,
	); ok {
		lastNodeId = lockNodeId
	}

	// append hidden column to tableDef
	newTableDef := DeepCopyTableDef(originTableDef, true)
	err = buildInsertPlans(ctx, builder, bindCtx, nil, objRef, newTableDef, lastNodeId, ifExistAutoPkCol, nil, nil)
	if err != nil {
		return nil, err
	}
	// use shuffle for load if parallel and no compress
	if stmt.Param.Parallel && (getCompressType(stmt.Param, fileName) == tree.NOCOMPRESS) {
		for i := range builder.qry.Nodes {
			node := builder.qry.Nodes[i]
			if node.NodeType == plan.Node_INSERT {
				if node.Stats.HashmapStats == nil {
					node.Stats.HashmapStats = &plan.HashMapStats{}
				}
				node.Stats.HashmapStats.Shuffle = true
			}
		}
	}

	query := builder.qry
	sqls, err := genSqlsForCheckFKSelfRefer(ctx.GetContext(),
		objRef.SchemaName, newTableDef.Name, newTableDef.Cols, newTableDef.Fkeys)
	if err != nil {
		return nil, err
	}
	query.DetectSqls = sqls
	reduceSinkSinkScanNodes(query)
	builder.tempOptimizeForDML()
	query.StmtType = plan.Query_INSERT

	pn := &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}
	return pn, nil
}

func checkFileExist(param *tree.ExternParam, ctx CompilerContext) (string, error) {
	if param.Local {
		return "", nil
	}
	if param.ScanType == tree.INLINE {
		return "", nil
	}
	param.Ctx = ctx.GetContext()
	if param.ScanType == tree.S3 {
		if err := InitS3Param(param); err != nil {
			return "", err
		}
	} else {
		if err := InitInfileParam(param); err != nil {
			return "", err
		}
	}
	if len(param.Filepath) == 0 {
		return "", nil
	}
	if err := StatFile(param); err != nil {
		if moerror, ok := err.(*moerr.Error); ok {
			if moerror.ErrorCode() == moerr.ErrFileNotFound {
				return "", moerr.NewInvalidInput(ctx.GetContext(), "the file does not exist in load flow")
			} else {
				return "", moerror
			}
		}
		return "", moerr.NewInternalError(ctx.GetContext(), err.Error())
	}
	param.Init = true
	return param.Filepath, nil
}

func getProjectNode(stmt *tree.Load, ctx CompilerContext, node *plan.Node, tableDef *TableDef, colToIndex map[string]int32) (bool, error) {
	tblName := string(stmt.Table.ObjectName)
	ifExistAutoPkCol := false
	node.ProjectList = make([]*plan.Expr, len(tableDef.Cols))
	var tmp *plan.Expr

	for i := 0; i < len(tableDef.Cols); i++ {
		if colListId, ok := colToIndex[tableDef.Cols[i].Name]; ok {
			tmp = &plan.Expr{
				Typ: tableDef.Cols[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						ColPos: int32(colListId),
						Name:   tblName + "." + tableDef.Cols[i].Name,
					},
				},
			}
			node.ProjectList[i] = tmp
			continue
		}

		defExpr, err := getDefaultExpr(ctx.GetContext(), tableDef.Cols[i])
		if err != nil {
			return false, err
		}

		node.ProjectList[i] = defExpr
		if tableDef.Cols[i].Typ.AutoIncr && tableDef.Cols[i].Name == tableDef.Pkey.PkeyColName {
			ifExistAutoPkCol = true
		}
	}

	return ifExistAutoPkCol, nil
}

func InitNullMap(param *tree.ExternParam, ctx CompilerContext) error {
	param.NullMap = make(map[string][]string)

	for i := 0; i < len(param.Tail.Assignments); i++ {
		expr, ok := param.Tail.Assignments[i].Expr.(*tree.FuncExpr)
		if !ok {
			param.Tail.Assignments[i].Expr = nil
			return nil
		}
		if len(expr.Exprs) != 2 {
			param.Tail.Assignments[i].Expr = nil
			return nil
		}

		expr2, ok := expr.Func.FunctionReference.(*tree.UnresolvedName)
		if !ok || expr2.ColName() != "nullif" {
			param.Tail.Assignments[i].Expr = nil
			return nil
		}

		expr3, ok := expr.Exprs[0].(*tree.UnresolvedName)
		if !ok {
			return moerr.NewInvalidInput(ctx.GetContext(), "the nullif func first param is not UnresolvedName form")
		}

		expr4, ok := expr.Exprs[1].(*tree.NumVal)
		if !ok {
			return moerr.NewInvalidInput(ctx.GetContext(), "the nullif func second param is not NumVal form")
		}

		for _, name := range param.Tail.Assignments[i].Names {
			col := name.ColName()
			if col != expr3.ColName() {
				return moerr.NewInvalidInput(ctx.GetContext(), "the nullif func first param must equal to colName")
			}
			param.NullMap[col] = append(param.NullMap[col], strings.ToLower(expr4.String()))
		}
		param.Tail.Assignments[i].Expr = nil
	}
	return nil
}

func checkNullMap(stmt *tree.Load, Cols []*ColDef, ctx CompilerContext) error {
	for k := range stmt.Param.NullMap {
		find := false
		for i := 0; i < len(Cols); i++ {
			if Cols[i].Name == k {
				find = true
			}
		}
		if !find {
			return moerr.NewInvalidInput(ctx.GetContext(), "wrong col name '%s' in nullif function", k)
		}
	}
	return nil
}

func getCompressType(param *tree.ExternParam, filepath string) string {
	if param.CompressType != "" && param.CompressType != tree.AUTO {
		return param.CompressType
	}
	index := strings.LastIndex(filepath, ".")
	if index == -1 {
		return tree.NOCOMPRESS
	}
	tail := string([]byte(filepath)[index+1:])
	switch tail {
	case "gz", "gzip":
		return tree.GZIP
	case "bz2", "bzip2":
		return tree.BZIP2
	case "lz4":
		return tree.LZ4
	default:
		return tree.NOCOMPRESS
	}
}

func makeCastExpr(stmt *tree.Load, fileName string, tableDef *TableDef, node *plan.Node) []*plan.Expr {
	ret := make([]*plan.Expr, 0)
	stringTyp := &plan.Type{
		Id: int32(types.T_varchar),
	}
	for i := 0; i < len(tableDef.Cols); i++ {
		typ := node.ProjectList[i].Typ
		expr := node.ProjectList[i].Expr
		planExpr := &plan.Expr{
			Typ:  *stringTyp,
			Expr: expr,
		}

		planExpr, _ = makePlan2CastExpr(stmt.Param.Ctx, planExpr, typ)
		ret = append(ret, planExpr)
	}

	return ret
}
