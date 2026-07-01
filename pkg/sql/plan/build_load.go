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
	"bufio"
	"context"
	"encoding/json"
	"io"
	"math"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

const (
	LoadParallelMinSize     = int(colexec.WriteS3Threshold)
	LoadWriteS3MinSize      = 1 << 20
	loadFirstLineSampleSize = 1 << 20
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
				return nil, nil, nil, nil, moerr.NewInternalErrorf(ctx.GetContext(), "column '%s' does not exist", colName)
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
			return nil, nil, nil, nil, moerr.NewInternalErrorf(ctx.GetContext(), "unsupported column type %v", realCol)
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

func newReaderWithParam(param *tree.ExternParam, reader io.ReadCloser) (*csvparser.CSVParser, error) {
	fieldsTerminatedBy := "\t"
	fieldsEnclosedBy := "\""
	fieldsEscapedBy := "\\"

	linesTerminatedBy := "\n"
	linesStartingBy := ""

	if param.Tail.Fields != nil {
		if terminated := param.Tail.Fields.Terminated; terminated != nil && terminated.Value != "" {
			fieldsTerminatedBy = terminated.Value
		}
		if enclosed := param.Tail.Fields.EnclosedBy; enclosed != nil && enclosed.Value != 0 {
			fieldsEnclosedBy = string(enclosed.Value)
		}
		if escaped := param.Tail.Fields.EscapedBy; escaped != nil {
			if escaped.Value == 0 {
				fieldsEscapedBy = ""
			} else {
				fieldsEscapedBy = string(escaped.Value)
			}
		}
	}

	if param.Tail.Lines != nil {
		if terminated := param.Tail.Lines.TerminatedBy; terminated != nil && terminated.Value != "" {
			linesTerminatedBy = param.Tail.Lines.TerminatedBy.Value
		}
		if param.Tail.Lines.StartingBy != "" {
			linesStartingBy = param.Tail.Lines.StartingBy
		}
	}

	if param.Format == tree.JSONLINE {
		fieldsTerminatedBy = "\t"
		fieldsEscapedBy = ""
	}

	config := csvparser.CSVConfig{
		FieldsTerminatedBy: fieldsTerminatedBy,
		FieldsEnclosedBy:   fieldsEnclosedBy,
		FieldsEscapedBy:    fieldsEscapedBy,
		LinesTerminatedBy:  linesTerminatedBy,
		LinesStartingBy:    linesStartingBy,
		NotNull:            false,
		Null:               []string{`\N`},
		UnescapedQuote:     true,
		// Comment defaults to 0 (no comment marker): every line is data, matching
		// MySQL LOAD DATA, which does not treat '#' lines as comments.
		Comment: "",
	}

	return csvparser.NewCSVParser(&config, bufio.NewReader(reader), csvparser.ReadBlockSize, false)
}

func IgnoredLines(param *tree.ExternParam, ctx CompilerContext) (offset int64, err error) {
	fs, readPath, err := GetForETLWithType(param, param.Filepath)
	if err != nil {
		return 0, err
	}
	var r io.ReadCloser
	vec := fileservice.IOVector{
		FilePath: readPath,
		Entries: []fileservice.IOEntry{
			0: {
				Offset:            0,
				Size:              -1,
				ReadCloserForRead: &r,
			},
		},
	}

	param.Ctx = ctx.GetContext()
	if err = fs.Read(param.Ctx, &vec); err != nil {
		return 0, err
	}

	bufR := bufio.NewReader(r)
	skipLines := param.Tail.IgnoredLines

	csvReader, err := newReaderWithParam(param, io.NopCloser(bufR))
	if err != nil {
		return 0, err
	}

	var lastRow []csvparser.Field
	for skipLines > 0 {
		lastRow, err = csvReader.Read(lastRow)
		if err != nil {
			if err == io.EOF {
				param.Tail.IgnoredLines = 0
				param.Ctx = nil
				return csvReader.Pos(), nil
			}
			return 0, err
		}
		skipLines--
	}

	param.Tail.IgnoredLines = 0
	param.Ctx = nil
	return csvReader.Pos(), nil
}

func validateLoadParquetOptions(param *tree.ExternParam, ctx CompilerContext) error {
	if param == nil || !isLoadParquetFormat(param) {
		return nil
	}
	if param.Local {
		return moerr.NewNYI(ctx.GetContext(), "load parquet local")
	}
	if loadOptionExists(param, "compression") || hasExplicitLoadCompression(param.CompressType) {
		return moerr.NewBadConfig(ctx.GetContext(), "LOAD DATA with format='parquet' does not support compression option")
	}
	if loadOptionExists(param, "jsondata") || param.JsonData != "" {
		return moerr.NewBadConfig(ctx.GetContext(), "LOAD DATA with format='parquet' does not support jsondata option")
	}
	if loadOptionExists(param, "hive_partitioning") || loadOptionExists(param, "hive_partition_columns") ||
		param.HivePartitioning || len(param.HivePartitionCols) > 0 {
		return moerr.NewBadConfig(ctx.GetContext(), "LOAD DATA with format='parquet' does not support hive partitioning options")
	}
	if param.Tail == nil {
		return nil
	}
	if param.Tail.Fields != nil {
		return moerr.NewBadConfig(ctx.GetContext(), "LOAD DATA with format='parquet' does not support FIELDS option")
	}
	if param.Tail.Lines != nil {
		return moerr.NewBadConfig(ctx.GetContext(), "LOAD DATA with format='parquet' does not support LINES option")
	}
	if param.Tail.IgnoredLines > 0 {
		return moerr.NewBadConfig(ctx.GetContext(), "LOAD DATA with format='parquet' does not support IGNORE LINES")
	}
	if hasLoadUserVariable(param.Tail.ColumnList) {
		return moerr.NewNYI(ctx.GetContext(), "parquet load with @variables in column list")
	}
	if len(param.Tail.Assignments) > 0 {
		return moerr.NewNYI(ctx.GetContext(), "parquet load with SET clause")
	}
	return nil
}

func isLoadParquetFormat(param *tree.ExternParam) bool {
	if param.Format == tree.PARQUET {
		return true
	}
	for i := 0; i+1 < len(param.Option); i += 2 {
		if strings.EqualFold(param.Option[i], "format") &&
			strings.EqualFold(param.Option[i+1], tree.PARQUET) {
			return true
		}
	}
	return false
}

func loadOptionExists(param *tree.ExternParam, key string) bool {
	key = strings.ToLower(key)
	for i := 0; i+1 < len(param.Option); i += 2 {
		if strings.ToLower(param.Option[i]) == key {
			return true
		}
	}
	return false
}

func hasExplicitLoadCompression(compressType string) bool {
	return compressType != "" && !strings.EqualFold(compressType, tree.AUTO)
}

func hasLoadUserVariable(cols []tree.LoadColumn) bool {
	for _, col := range cols {
		if _, ok := col.(*tree.VarExpr); ok {
			return true
		}
	}
	return false
}

func makeLoadExternalStats(param *tree.ExternParam, tableDef *TableDef, offset int64, ctx context.Context) *plan.Stats {
	// LOAD external scan parallelism is currently sized by
	// getParallelSizeForExternalScan as Cost*Rowsize/WriteS3Threshold.
	// Keep Cost*Rowsize close to input bytes, but express Cost/Outcnt/BlockNum
	// as row/cardinality estimates so large LOAD keeps the expected AP path.
	stats := &plan.Stats{Rowsize: 1}
	if param == nil {
		return stats
	}
	var inputSize int64
	if param.ScanType == tree.INLINE {
		inputSize = int64(len(param.Data))
	} else {
		inputSize = param.FileSize - offset
	}
	if inputSize < 0 {
		inputSize = 0
	}
	if inputSize == 0 {
		return stats
	}

	rowSize := estimateLoadRowsize(param, tableDef, inputSize, offset, ctx)
	rowCount := math.Ceil(float64(inputSize) / rowSize)
	if rowCount < 1 {
		rowCount = 1
	}
	stats.Cost = rowCount
	stats.Outcnt = rowCount
	stats.TableCnt = rowCount
	stats.Rowsize = rowSize
	stats.Selectivity = 1
	stats.BlockNum = int32(math.Ceil(rowCount / float64(options.DefaultBlockMaxRows)))
	return stats
}

func estimateLoadRowsize(param *tree.ExternParam, tableDef *TableDef, inputSize int64, offset int64, ctx context.Context) float64 {
	if param != nil && param.ScanType == tree.INLINE && param.Format == tree.CSV {
		if rowSize := inlineCSVRowsize(param.Data, loadLinesTerminatedBy(param)); rowSize > 0 {
			return clampLoadRowsize(rowSize, inputSize)
		}
	}
	if rowSize := estimateLoadRowsizeFromFirstLine(param, inputSize, offset, ctx); rowSize > 0 {
		return rowSize
	}
	if tableDef != nil {
		if rowSize := GetRowSizeFromTableDef(tableDef, true) * 0.8; rowSize > 0 {
			return clampLoadRowsize(rowSize, inputSize)
		}
	}
	return clampLoadRowsize(1, inputSize)
}

func inlineCSVRowsize(data string, terminatedBy string) float64 {
	if terminatedBy == "" {
		terminatedBy = "\n"
	}
	if idx := strings.Index(data, terminatedBy); idx >= 0 {
		return float64(idx + len(terminatedBy))
	}
	return float64(len(data))
}

func loadLinesTerminatedBy(param *tree.ExternParam) string {
	if param != nil && param.Tail != nil && param.Tail.Lines != nil {
		if terminated := param.Tail.Lines.TerminatedBy; terminated != nil && terminated.Value != "" {
			return terminated.Value
		}
	}
	return "\n"
}

func estimateLoadRowsizeFromFirstLine(param *tree.ExternParam, inputSize int64, offset int64, ctx context.Context) float64 {
	lineTerminator := loadLinesTerminatedBy(param)
	if param == nil ||
		param.ScanType == tree.INLINE ||
		param.Local ||
		param.Format == tree.PARQUET ||
		getCompressType(param, param.Filepath) != tree.NOCOMPRESS ||
		(lineTerminator != "\n" && lineTerminator != "\r\n") ||
		strings.HasPrefix(param.Filepath, "SHARED:/query_result/") {
		return 0
	}

	if size := readExternalFirstLineSize(param, inputSize, offset, ctx); size > 0 {
		return clampLoadRowsize(float64(size), inputSize)
	}
	return 0
}

func readExternalFirstLineSize(param *tree.ExternParam, inputSize int64, offset int64, ctx context.Context) int {
	if param == nil {
		return 0
	}
	if ctx == nil {
		ctx = param.Ctx
	}
	if ctx == nil {
		return 0
	}

	sampleSize := int64(loadFirstLineSampleSize)
	if inputSize > 0 && inputSize < sampleSize {
		sampleSize = inputSize
	}
	if sampleSize <= 0 {
		return 0
	}

	fs, readPath, err := GetForETLWithType(param, param.Filepath)
	if err != nil {
		return 0
	}
	var r io.ReadCloser
	vec := fileservice.IOVector{
		FilePath: readPath,
		Entries: []fileservice.IOEntry{
			0: {
				Offset:            offset,
				Size:              sampleSize,
				ReadCloserForRead: &r,
			},
		},
	}
	if err = fs.Read(ctx, &vec); err != nil {
		return 0
	}
	if r == nil {
		return 0
	}
	defer r.Close()

	reader := bufio.NewReader(r)
	if offset == 0 && param.Tail != nil {
		for i := uint64(0); i < param.Tail.IgnoredLines; i++ {
			line, err := reader.ReadString('\n')
			if err != nil || !strings.HasSuffix(line, "\n") {
				return 0
			}
		}
	}

	line, err := reader.ReadString('\n')
	if len(line) == 0 {
		return 0
	}
	if strings.HasSuffix(line, "\n") {
		return len(line)
	}
	if err == io.EOF && inputSize > 0 && inputSize <= sampleSize {
		return len(line)
	}
	return 0
}

func clampLoadRowsize(rowSize float64, inputSize int64) float64 {
	if rowSize < 1 {
		return 1
	}
	if inputSize > 0 && rowSize > float64(inputSize) {
		return float64(inputSize)
	}
	return rowSize
}

func loadParquetMayListFiles(param *tree.ExternParam) bool {
	return param != nil &&
		param.Format == tree.PARQUET &&
		strings.ContainsAny(strings.TrimSpace(param.Filepath), "*?[")
}

func totalLoadFileSize(fileSize []int64) int64 {
	var total int64
	for _, size := range fileSize {
		if size > 0 {
			total += size
		}
	}
	return total
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

	// Note on Hive partitioned external tables: LOAD DATA into any external
	// table (hive or not) is rejected by checkTableType inside getDmlTableInfo
	// above, producing "cannot insert/update/delete from external table".
	// No hive-specific intercept is needed here — and any probe added below
	// would be unreachable dead code. See Phase 8 P8-audit-3 decision to keep
	// the generic external-table error for consistency with all DML on externals.

	stmt.Param.Local = stmt.Local
	fileName, err := checkFileExist(stmt.Param, ctx)
	if err != nil {
		return nil, err
	}

	if err := InitNullMap(stmt.Param, ctx); err != nil {
		return nil, err
	}
	if err := validateLoadParquetOptions(stmt.Param, ctx); err != nil {
		return nil, err
	}
	tableDef := tblInfo.tableDefs[0]
	objRef := tblInfo.objRef[0]
	originTableDef := tableDef

	// If the LOAD target is a writable external table, capture its
	// WRITE_FILE_PATTERN config now: tableDef.Createsql below is reused to carry
	// the LOAD *source* param, which would otherwise clobber the target's.
	externalWriteTarget := false
	externalWriteTargetCreatesql := ""
	if originTableDef.TableType == catalog.SystemExternalRel {
		if _, ok := GetWriteFilePattern(getExternParamFromTableDef(originTableDef)); ok {
			externalWriteTarget = true
			externalWriteTargetCreatesql = originTableDef.Createsql
		}
	}

	// load with columnlist will copy a new tableDef
	externalProject, colToIndex, tbColToDataCol, tableDef, err := getExternalProject(stmt, ctx, tableDef, tblName)
	if err != nil {
		return nil, err
	}

	if err := checkNullMap(stmt, tableDef.Cols, ctx); err != nil {
		return nil, err
	}

	noCompress := getCompressType(stmt.Param, fileName) == tree.NOCOMPRESS
	var offset int64 = 0
	if stmt.Param.Tail.IgnoredLines > 0 && stmt.Param.Parallel && noCompress && !stmt.Param.Local {
		offset, err = IgnoredLines(stmt.Param, ctx)
		if err != nil {
			return nil, err
		}
		stmt.Param.FileStartOff = offset
	}
	stmt.Param.ParallelLoadRequested = stmt.Param.ParallelLoadRequested || stmt.Param.Parallel

	if stmt.Param.FileSize-offset < int64(LoadParallelMinSize) {
		stmt.Param.Parallel = false
	}

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
		Stats:       makeLoadExternalStats(stmt.Param, tableDef, offset, ctx.GetContext()),
		ProjectList: externalProject,
		ObjRef:      objRef,
		TableDef:    tableDef,
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

	inlineDataSize := strings.Count(stmt.Param.Data, "")
	builder.qry.LoadWriteS3 = true

	if noCompress && ((stmt.Param.ScanType == tree.INLINE && inlineDataSize < LoadWriteS3MinSize) || (stmt.Param.ScanType != tree.INLINE && stmt.Param.FileSize-offset < int64(LoadWriteS3MinSize))) {
		builder.qry.LoadWriteS3 = false
	}

	if stmt.Param.Parallel && noCompress && stmt.Param.Format != tree.PARQUET {
		projectNode.ProjectList = makeCastExpr(stmt, fileName, originTableDef, projectNode)
	}
	lastNodeId = builder.appendNode(projectNode, bindCtx)
	builder.qry.LoadTag = true

	// External write target: no lock (no engine relation to lock).
	if !externalWriteTarget {
		//append lock node
		if lockNodeId, ok := appendLockNode(
			builder,
			bindCtx,
			lastNodeId,
			originTableDef,
			true,
			false,
			false,
		); ok {
			lastNodeId = lockNodeId
		}
	}

	// append hidden column to tableDef
	newTableDef := DeepCopyTableDef(originTableDef, true)
	if externalWriteTarget {
		// Restore the target external table's WRITE_FILE_PATTERN config so the
		// executor can build the writer, then attach a minimal external insert.
		// Each parallel scan pipeline writes its own file; no shuffle.
		newTableDef.Createsql = externalWriteTargetCreatesql
		if err = appendExternalInsertPlan(builder, bindCtx, objRef, newTableDef, lastNodeId); err != nil {
			return nil, err
		}
	} else {
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
	if param.ScanType == tree.INLINE {
		return "", nil
	}

	if param.ScanType == tree.S3 {
		if err := InitS3Param(param); err != nil {
			return "", err
		}
	} else {
		if err := InitInfileOrStageParam(param, ctx.GetProcess()); err != nil {
			return "", err
		}
	}
	if param.Local {
		return param.Filepath, nil
	}
	if len(param.Filepath) == 0 {
		return "", nil
	}

	param.Ctx = ctx.GetContext()
	if loadParquetMayListFiles(param) {
		fileList, fileSize, err := ReadDir(param)
		param.Ctx = nil
		if err != nil {
			return "", err
		}
		if len(fileList) == 0 {
			return "", moerr.NewInvalidInput(ctx.GetContext(), "the file does not exist in load flow")
		}
		param.FileSize = totalLoadFileSize(fileSize)
		return param.Filepath, nil
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
			param.NullMap[col] = append(param.NullMap[col], expr4.String())
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
			return moerr.NewInvalidInputf(ctx.GetContext(), "wrong col name '%s' in nullif function", k)
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

// The execution-time width check in getColData (external.go), gated by
// checkLineStrict(param), is the single source of truth for strict vs
// lenient handling. Using lenient cast here avoids baking a strictness
// decision into the plan at PREPARE time, which would become stale when
// a prepared LOAD statement is EXECUTEd under a different sql_mode.
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
