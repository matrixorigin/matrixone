// Copyright 2021 Matrix Origin
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

package frontend

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend/constant"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
)

func getQueryResultDir() string {
	return fileservice.JoinPath(defines.SharedFileServiceName, "/query_result")
}

func getPathOfQueryResultFile(fileName string) string {
	return fmt.Sprintf("%s/%s", getQueryResultDir(), fileName)
}

func openSaveQueryResult(ctx context.Context, ses *Session) bool {
	if ses.ast == nil || ses.tStmt == nil {
		return false
	}
	if ses.tStmt.SqlSourceType == constant.InternalSql {
		return false
	}
	if ses.tStmt.StatementType == "Select" && ses.tStmt.SqlSourceType != constant.CloudUserSql {
		return false
	}
	val, err := ses.GetGlobalVar(ctx, "save_query_result")
	if err != nil {
		return false
	}
	if v, _ := val.(int8); v > 0 {
		if ses.blockIdx == 0 {
			if err = initQueryResulConfig(ctx, ses); err != nil {
				return false
			}
		}
		return true
	}
	return false
}

func initQueryResulConfig(ctx context.Context, ses *Session) error {
	val, err := ses.GetGlobalVar(ctx, "query_result_maxsize")
	if err != nil {
		return err
	}
	switch v := val.(type) {
	case uint64:
		ses.limitResultSize = float64(v)
	case float64:
		ses.limitResultSize = v
	}
	var p uint64
	val, err = ses.GetGlobalVar(ctx, "query_result_timeout")
	if err != nil {
		return err
	}
	switch v := val.(type) {
	case uint64:
		p = v
	case float64:
		p = uint64(v)
	}
	ses.createdTime = time.Now()
	ses.expiredTime = ses.createdTime.Add(time.Hour * time.Duration(p))
	return err
}

func saveQueryResult(ctx context.Context, ses *Session, bat *batch.Batch) error {
	s := ses.curResultSize + float64(bat.Size())/(1024*1024)
	if s > ses.limitResultSize {
		logInfo(ses, ses.GetDebugString(), "open save query result", zap.Float64("current result size:", s))
		return nil
	}
	fs := getGlobalPu().FileService
	// write query result
	path := catalog.BuildQueryResultPath(ses.GetTenantInfo().GetTenant(), uuid.UUID(ses.tStmt.StatementID).String(), ses.GetIncBlockIdx())
	logInfo(ses, ses.GetDebugString(), "open save query result", zap.String("statemant id is:", uuid.UUID(ses.tStmt.StatementID).String()), zap.String("fileservice name is:", fs.Name()), zap.String("write path is:", path), zap.Float64("current result size:", s))
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterQueryResult, path, fs)
	if err != nil {
		return err
	}
	_, err = writer.Write(bat)
	if err != nil {
		return err
	}
	option := objectio.WriteOptions{
		Type: objectio.WriteTS,
		Val:  ses.expiredTime,
	}
	_, err = writer.WriteEnd(ctx, option)
	if err != nil {
		return err
	}
	ses.curResultSize = s
	return err
}

func saveQueryResultMeta(ctx context.Context, ses *Session) error {
	defer func() {
		ses.ResetBlockIdx()
		ses.p = nil
		// TIPs: Session.SetTStmt() do reset the tStmt while query is DONE.
		// Be careful, if you want to do async op.
		ses.tStmt = nil
		ses.curResultSize = 0
	}()
	fs := getGlobalPu().FileService
	// write query result meta
	colMap, err := buildColumnMap(ctx, ses.rs)
	if err != nil {
		return err
	}
	b, err := ses.rs.Marshal()
	if err != nil {
		return err
	}
	buf := new(strings.Builder)
	prefix := ",\n"
	for i := 1; i <= ses.blockIdx; i++ {
		if i > 1 {
			buf.WriteString(prefix)
		}
		buf.WriteString(catalog.BuildQueryResultPath(ses.GetTenantInfo().GetTenant(), uuid.UUID(ses.tStmt.StatementID).String(), i))
	}

	var sp []byte
	if ses.p != nil {
		sp, err = ses.p.Marshal()
		if err != nil {
			return err
		}
	}

	st, err := simpleAstMarshal(ses.ast)
	if err != nil {
		return err
	}
	m := &catalog.Meta{
		QueryId:     ses.tStmt.StatementID,
		Statement:   ses.tStmt.Statement,
		AccountId:   ses.GetTenantInfo().GetTenantID(),
		RoleId:      ses.tStmt.RoleId,
		ResultPath:  buf.String(),
		CreateTime:  types.UnixToTimestamp(ses.createdTime.Unix()),
		ResultSize:  ses.curResultSize,
		Columns:     string(b),
		Tables:      getTablesFromPlan(ses.p),
		UserId:      ses.GetTenantInfo().GetUserID(),
		ExpiredTime: types.UnixToTimestamp(ses.expiredTime.Unix()),
		Plan:        string(sp),
		Ast:         string(st),
		ColumnMap:   colMap,
	}
	metaBat, err := buildQueryResultMetaBatch(m, ses.pool)
	if err != nil {
		return err
	}
	metaPath := catalog.BuildQueryResultMetaPath(ses.GetTenantInfo().GetTenant(), uuid.UUID(ses.tStmt.StatementID).String())
	metaWriter, err := objectio.NewObjectWriterSpecial(objectio.WriterQueryResult, metaPath, fs)
	if err != nil {
		return err
	}
	_, err = metaWriter.Write(metaBat)
	if err != nil {
		return err
	}
	option := objectio.WriteOptions{
		Type: objectio.WriteTS,
		Val:  ses.expiredTime,
	}
	_, err = metaWriter.WriteEnd(ctx, option)
	if err != nil {
		return err
	}
	return err
}

func buildColumnMap(ctx context.Context, rs *plan.ResultColDef) (string, error) {
	if rs == nil {
		return "", moerr.NewInternalError(ctx, "resultColDef is nil")
	}
	m := make(map[string][]int)
	org := make([]string, len(rs.ResultCols))
	for i, col := range rs.ResultCols {
		org[i] = col.Name
		v := m[col.Name]
		m[col.Name] = append(v, i)
	}
	for _, v := range m {
		if len(v) > 1 {
			for i := range v {
				rs.ResultCols[v[i]].Name = fmt.Sprintf("%s_%d", rs.ResultCols[v[i]].Name, i)
			}
		}
	}
	buf := new(strings.Builder)
	for i := range org {
		if i > 0 {
			buf.WriteString(", ")
		}
		if len(rs.ResultCols[i].Typ.Table) > 0 {
			buf.WriteString(fmt.Sprintf("%s.%s -> %s", rs.ResultCols[i].Typ.Table, org[i], rs.ResultCols[i].Name))
		} else {
			buf.WriteString(fmt.Sprintf("%s -> %s", org[i], rs.ResultCols[i].Name))
		}
	}
	return buf.String(), nil
}

func isResultQuery(p *plan.Plan) []string {
	var uuids []string = nil
	if q, ok := p.Plan.(*plan.Plan_Query); ok {
		for _, n := range q.Query.Nodes {
			if n.NodeType == plan.Node_EXTERNAL_SCAN {
				if n.TableDef.TableType == "query_result" {
					uuids = append(uuids, n.TableDef.Name)
				}
			} else if n.NodeType == plan.Node_FUNCTION_SCAN {
				if n.TableDef.TblFunc.Name == "meta_scan" {
					uuids = append(uuids, n.TableDef.Name)
				}
			}
		}
	}
	return uuids
}

func checkPrivilege(uuids []string, reqCtx context.Context, ses *Session) error {
	f := getGlobalPu().FileService
	for _, id := range uuids {
		// var size int64 = -1
		path := catalog.BuildQueryResultMetaPath(ses.GetTenantInfo().GetTenant(), id)
		reader, err := blockio.NewFileReader(f, path)
		if err != nil {
			return err
		}
		idxs := []uint16{catalog.PLAN_IDX, catalog.AST_IDX}
		bats, closeCB, err := reader.LoadAllColumns(reqCtx, idxs, ses.GetMemPool())
		if err != nil {
			return err
		}
		defer func() {
			if closeCB != nil {
				closeCB()
			}
		}()
		bat := bats[0]
		p := bat.Vecs[0].GetStringAt(0)
		pn := &plan.Plan{}
		if err = pn.Unmarshal([]byte(p)); err != nil {
			return err
		}
		a := bat.Vecs[1].GetStringAt(0)
		var ast tree.Statement
		if ast, err = simpleAstUnmarshal([]byte(a)); err != nil {
			return err
		}
		if err = authenticateCanExecuteStatementAndPlan(reqCtx, ses, ast, pn); err != nil {
			return err
		}
	}
	return nil
}

func maySaveQueryResult(ctx context.Context, ses *Session, bat *batch.Batch) error {
	if openSaveQueryResult(ctx, ses) {
		if err := saveQueryResult(ctx, ses, bat); err != nil {
			return err
		}
		if err := saveQueryResultMeta(ctx, ses); err != nil {
			return err
		}
	}
	return nil
}

type simpleAst struct {
	Typ int `json:"age"`
	// opt which fun of determinePrivilegeSetOfStatement need
}

type astType int

const (
	astShowNone astType = iota
	astSelect
	astShowAboutTable
	astExplain
	astValues
	astExecute
)

func simpleAstMarshal(stmt tree.Statement) ([]byte, error) {
	s := simpleAst{}
	switch stmt.(type) {
	case *tree.Select:
		s.Typ = int(astSelect)
	case *tree.ShowTables, *tree.ShowSequences, *tree.ShowCreateTable, *tree.ShowColumns, *tree.ShowCreateView, *tree.ShowCreateDatabase:
		s.Typ = int(astShowAboutTable)
	case *tree.ShowProcessList, *tree.ShowErrors, *tree.ShowWarnings, *tree.ShowVariables,
		*tree.ShowStatus, *tree.ShowTarget, *tree.ShowTableStatus,
		*tree.ShowGrants, *tree.ShowIndex,
		*tree.ShowTableNumber, *tree.ShowColumnNumber,
		*tree.ShowTableValues, *tree.ShowNodeList,
		*tree.ShowLocks, *tree.ShowFunctionOrProcedureStatus, *tree.ShowConnectors:
		s.Typ = int(astShowNone)
	case *tree.ExplainFor, *tree.ExplainAnalyze, *tree.ExplainStmt:
		s.Typ = int(astExplain)
	case *tree.Execute:
		s.Typ = int(astExecute)
	case *tree.ValuesStatement:
		s.Typ = int(astValues)
	default:
		s.Typ = int(astShowNone)
	}
	return json.Marshal(s)
}

func simpleAstUnmarshal(b []byte) (tree.Statement, error) {
	s := &simpleAst{}
	if err := json.Unmarshal(b, s); err != nil {
		return nil, err
	}
	var stmt tree.Statement
	switch astType(s.Typ) {
	case astSelect:
		stmt = &tree.Select{}
	case astShowAboutTable:
		stmt = &tree.ShowTables{}
	case astShowNone:
		stmt = &tree.ShowStatus{}
	case astExplain:
		stmt = &tree.ExplainFor{}
	case astExecute:
		stmt = &tree.Execute{}
	case astValues:
		stmt = &tree.ValuesStatement{}
	}
	return stmt, nil
}

func getTablesFromPlan(p *plan.Plan) string {
	if p == nil {
		return ""
	}
	buf := new(strings.Builder)
	cnt := 0
	if q, ok := p.Plan.(*plan.Plan_Query); ok {
		for _, n := range q.Query.Nodes {
			if n.NodeType == plan.Node_EXTERNAL_SCAN || n.NodeType == plan.Node_TABLE_SCAN {
				if cnt > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(n.TableDef.Name)
				cnt++
			}
		}
	}
	return buf.String()
}

func buildQueryResultMetaBatch(m *catalog.Meta, mp *mpool.MPool) (*batch.Batch, error) {
	var err error
	bat := batch.NewWithSize(len(catalog.MetaColTypes))
	bat.SetAttributes(catalog.MetaColNames)
	for i, t := range catalog.MetaColTypes {
		bat.Vecs[i] = vector.NewVec(t)
	}
	if err = vector.AppendFixed(bat.Vecs[catalog.QUERY_ID_IDX], types.Uuid(m.QueryId), false, mp); err != nil {
		return nil, err
	}
	if err = vector.AppendBytes(bat.Vecs[catalog.STATEMENT_IDX], []byte(m.Statement), false, mp); err != nil {
		return nil, err
	}
	if err = vector.AppendFixed(bat.Vecs[catalog.ACCOUNT_ID_IDX], m.AccountId, false, mp); err != nil {
		return nil, err
	}
	if err = vector.AppendFixed(bat.Vecs[catalog.ROLE_ID_IDX], m.RoleId, false, mp); err != nil {
		return nil, err
	}
	if err = vector.AppendBytes(bat.Vecs[catalog.RESULT_PATH_IDX], []byte(m.ResultPath), false, mp); err != nil {
		return nil, err
	}
	if err = vector.AppendFixed(bat.Vecs[catalog.CREATE_TIME_IDX], m.CreateTime, false, mp); err != nil {
		return nil, err
	}
	if err = vector.AppendFixed(bat.Vecs[catalog.RESULT_SIZE_IDX], m.ResultSize, false, mp); err != nil {
		return nil, err
	}
	if err = vector.AppendBytes(bat.Vecs[catalog.COLUMNS_IDX], []byte(m.Columns), false, mp); err != nil {
		return nil, err
	}
	if err = vector.AppendBytes(bat.Vecs[catalog.TABLES_IDX], []byte(m.Tables), false, mp); err != nil {
		return nil, err
	}
	if err = vector.AppendFixed(bat.Vecs[catalog.USER_ID_IDX], m.UserId, false, mp); err != nil {
		return nil, err
	}
	if err = vector.AppendFixed(bat.Vecs[catalog.EXPIRED_TIME_IDX], m.ExpiredTime, false, mp); err != nil {
		return nil, err
	}
	if err = vector.AppendBytes(bat.Vecs[catalog.PLAN_IDX], []byte(m.Plan), false, mp); err != nil {
		return nil, err
	}
	if err = vector.AppendBytes(bat.Vecs[catalog.AST_IDX], []byte(m.Ast), false, mp); err != nil {
		return nil, err
	}
	if err = vector.AppendBytes(bat.Vecs[catalog.COLUMN_MAP_IDX], []byte(m.ColumnMap), false, mp); err != nil {
		return nil, err
	}
	return bat, nil
}

// resultFileInfo holds the info of the result file
type resultFileInfo struct {
	// the name of the result file
	name string
	// the size of the result file
	size int64
	// the block id of the result file
	blockIndex int64
}

// doDumpQueryResult reads data from the query result, converts it into csv and saves it into
// the file designated by the path.
func doDumpQueryResult(ctx context.Context, ses *Session, eParam *tree.ExportParam) error {
	var err error
	var columnDefs *plan.ResultColDef
	var reader *blockio.BlockReader
	var blocks []objectio.BlockObject
	var files []resultFileInfo

	//step1: open file handler
	if columnDefs, err = openResultMeta(ctx, ses, eParam.QueryId); err != nil {
		return err
	}

	if files, err = getResultFiles(ctx, ses, eParam.QueryId); err != nil {
		return err
	}

	//step2: read every batch from the query result
	columnCount := len(columnDefs.ResultCols)
	indexes := make([]uint16, columnCount)
	for i := range indexes {
		indexes[i] = uint16(i)
	}
	//=====================
	// preparation
	//=====================
	//prepare batch

	tmpBatch := batch.NewWithSize(columnCount)
	defer tmpBatch.Clean(ses.GetMemPool())
	//prepare result set
	mrs := &MysqlResultSet{}
	typs := make([]types.Type, columnCount)
	for i, c := range columnDefs.ResultCols {
		typs[i] = types.New(types.T(c.Typ.Id), c.Typ.Width, c.Typ.Scale)
		mcol := &MysqlColumn{}
		mcol.SetName(c.GetName())
		err = convertEngineTypeToMysqlType(ctx, typs[i].Oid, mcol)
		if err != nil {
			return err
		}
		mrs.AddColumn(mcol)
	}
	mrs.Data = make([][]interface{}, 1)
	for i := 0; i < 1; i++ {
		mrs.Data[i] = make([]interface{}, columnCount)
	}
	exportParam := &ExportConfig{
		userConfig: eParam,
	}
	//prepare output queue
	oq := NewOutputQueue(ctx, ses, columnCount, mrs, exportParam)
	oq.reset()
	oq.ep.OutTofile = true
	//prepare export param
	exportParam.DefaultBufSize = getGlobalPu().SV.ExportDataDefaultFlushSize
	exportParam.UseFileService = true
	exportParam.FileService = getGlobalPu().FileService
	exportParam.Ctx = ctx
	defer func() {
		exportParam.LineBuffer = nil
		exportParam.OutputStr = nil
		if exportParam.AsyncReader != nil {
			_ = exportParam.AsyncReader.Close()
		}
		if exportParam.AsyncWriter != nil {
			_ = exportParam.AsyncWriter.Close()
		}
	}()
	initExportFileParam(exportParam, mrs)

	//open output file
	if err = openNewFile(ctx, exportParam, mrs); err != nil {
		return err
	}

	//read all files
	for _, file := range files {
		reader, blocks, err = openResultFile(ctx, ses, file.name, file.size)
		if err != nil {
			return err
		}

		quit := false
		//read every block
		for _, block := range blocks {
			select {
			case <-ctx.Done():
				quit = true
			default:
			}

			if quit {
				break
			}
			tmpBatch.Clean(ses.GetMemPool())
			bat, release, err := reader.LoadColumns(ctx, indexes, nil, block.BlockHeader().BlockID().Sequence(), ses.GetMemPool())
			if err != nil {
				return err
			}
			defer release()
			tmpBatch = bat

			//step2.1: converts it into the csv string
			//step2.2: writes the csv string into the outfile
			n := tmpBatch.RowCount()
			for j := 0; j < n; j++ { //row index
				select {
				case <-ctx.Done():
					quit = true
				default:
				}

				if quit {
					break
				}

				_, err = extractRowFromEveryVector(ctx, ses, tmpBatch, j, oq, true)
				if err != nil {
					return err
				}
			}
		}
	}

	err = oq.flush()
	if err != nil {
		return err
	}

	err = Close(exportParam)
	if err != nil {
		return err
	}

	return err
}

// openResultMeta checks the query result of the queryId exists or not
func openResultMeta(ctx context.Context, ses *Session, queryId string) (*plan.ResultColDef, error) {
	account := ses.GetTenantInfo()
	if account == nil {
		return nil, moerr.NewInternalError(ctx, "modump does not work without the account info")
	}
	metaFile := catalog.BuildQueryResultMetaPath(account.GetTenant(), queryId)
	// read meta's meta
	reader, err := blockio.NewFileReader(getGlobalPu().FileService, metaFile)
	if err != nil {
		return nil, err
	}
	idxs := make([]uint16, 1)
	idxs[0] = catalog.COLUMNS_IDX
	// read meta's data
	bats, closeCB, err := reader.LoadAllColumns(ctx, idxs, ses.GetMemPool())
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return nil, moerr.NewResultFileNotFound(ctx, makeResultMetaPath(account.GetTenant(), queryId))
		}
		return nil, err
	}
	defer func() {
		if closeCB != nil {
			closeCB()
		}
	}()
	vec := bats[0].Vecs[0]
	def := vec.GetStringAt(0)
	r := &plan.ResultColDef{}
	if err = r.Unmarshal([]byte(def)); err != nil {
		return nil, err
	}
	return r, err
}

// getResultFiles lists all result files of queryId
func getResultFiles(ctx context.Context, ses *Session, queryId string) ([]resultFileInfo, error) {
	_, str, err := ses.GetTxnCompileCtx().GetQueryResultMeta(queryId)
	if err != nil {
		return nil, err
	}
	fileList := strings.Split(str, ",")
	for i := range fileList {
		fileList[i] = strings.TrimSpace(fileList[i])
	}
	rti := make([]resultFileInfo, 0, len(fileList))
	for i, file := range fileList {
		e, err := getGlobalPu().FileService.StatFile(ctx, file)
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
				return nil, moerr.NewResultFileNotFound(ctx, file)
			}
			return nil, err
		}
		rti = append(rti, resultFileInfo{
			name:       e.Name,
			size:       e.Size,
			blockIndex: int64(i + 1),
		})
	}

	return rti, nil
}

// openResultFile reads all blocks of the result file
func openResultFile(ctx context.Context, ses *Session, fileName string, fileSize int64) (*blockio.BlockReader, []objectio.BlockObject, error) {
	// read result's blocks
	filePath := getPathOfQueryResultFile(fileName)
	reader, err := blockio.NewFileReader(getGlobalPu().FileService, filePath)
	if err != nil {
		return nil, nil, err
	}
	bs, err := reader.LoadAllBlocks(ctx, ses.GetMemPool())
	if err != nil {
		return nil, nil, err
	}
	return reader, bs, err
}

// getFileSize finds the fileName in the file handlers ,returns the file size
// and returns -1 if not exists
func getFileSize(files []fileservice.DirEntry, fileName string) int64 {
	for _, file := range files {
		if file.Name == fileName {
			return file.Size
		}
	}
	return -1
}
