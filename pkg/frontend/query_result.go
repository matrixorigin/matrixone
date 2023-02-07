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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"strings"
)

const queryResultPrefix = "%s_%s_"

func getQueryResultDir() string {
	return fileservice.JoinPath(defines.SharedFileServiceName, "/query_result")
}

func getPrefixOfQueryResultFile(accountName, statementId string) string {
	return fmt.Sprintf(queryResultPrefix, accountName, statementId)
}

func getPathOfQueryResultFile(fileName string) string {
	return fmt.Sprintf("%s/%s", getQueryResultDir(), fileName)
}

func openSaveQueryResult(ses *Session) bool {
	if ses.ast == nil || ses.tStmt == nil {
		return false
	}
	if ses.tStmt.SqlSourceType == "internal_sql" || isSimpleResultQuery(ses.ast) {
		return false
	}
	val, err := ses.GetGlobalVar("save_query_result")
	if err != nil {
		return false
	}
	if v, _ := val.(int8); v > 0 {
		if ses.blockIdx == 0 {
			if err = initQueryResulConfig(ses); err != nil {
				return false
			}
		}
		return true
	}
	return false
}

func initQueryResulConfig(ses *Session) error {
	val, err := ses.GetGlobalVar("query_result_maxsize")
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
	val, err = ses.GetGlobalVar("query_result_timeout")
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
	return nil
}

func isSimpleResultQuery(ast tree.Statement) bool {
	switch stmt := ast.(type) {
	case *tree.Select:
		if stmt.With != nil || stmt.OrderBy != nil || stmt.Ep != nil {
			return false
		}
		if clause, ok := stmt.Select.(*tree.SelectClause); ok {
			if len(clause.From.Tables) > 1 || clause.Where != nil || clause.Having != nil || len(clause.GroupBy) > 0 {
				return false
			}
			t := clause.From.Tables[0]
			// judge table
			if j, ok := t.(*tree.JoinTableExpr); ok {
				if j.Right != nil {
					return false
				}
				if a, ok := j.Left.(*tree.AliasedTableExpr); ok {
					if f, ok := a.Expr.(*tree.TableFunction); ok {
						if f.Id() != "result_scan" && f.Id() != "meta_scan" {
							return false
						}
						// judge proj
						for _, selectExpr := range clause.Exprs {
							switch selectExpr.Expr.(type) {
							case tree.UnqualifiedStar:
								continue
							case *tree.UnresolvedName:
								continue
							default:
								return false
							}
						}
						return true
					}
					return false
				}
				return false
			}
			return false
		}
		return false
	case *tree.ParenSelect:
		return isSimpleResultQuery(stmt)
	}
	return false
}

func saveQueryResult(ses *Session, bat *batch.Batch) error {
	s := ses.curResultSize + float64(bat.Size())/(1024*1024)
	if s > ses.limitResultSize {
		return nil
	}
	fs := ses.GetParameterUnit().FileService
	// write query result
	path := catalog.BuildQueryResultPath(ses.GetTenantInfo().GetTenant(), uuid.UUID(ses.tStmt.StatementID).String(), ses.GetIncBlockIdx())
	writer, err := objectio.NewObjectWriter(path, fs)
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
	_, err = writer.WriteEnd(ses.requestCtx, option)
	if err != nil {
		return err
	}
	ses.curResultSize = s
	return nil
}

func saveQueryResultMeta(ses *Session) error {
	defer func() {
		ses.ResetBlockIdx()
		ses.p = nil
		ses.tStmt = nil
		ses.curResultSize = 0
	}()
	fs := ses.GetParameterUnit().FileService
	// write query result meta
	colMap := buildColumnMap(ses.rs)
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

	sp, err := ses.p.Marshal()
	if err != nil {
		return err
	}
	st, err := simpleAstMarshal(ses.ast)
	if err != nil {
		return nil
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
	metaBat, err := buildQueryResultMetaBatch(m, ses.mp)
	if err != nil {
		return err
	}
	metaPath := catalog.BuildQueryResultMetaPath(ses.GetTenantInfo().GetTenant(), uuid.UUID(ses.tStmt.StatementID).String())
	metaWriter, err := objectio.NewObjectWriter(metaPath, fs)
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
	_, err = metaWriter.WriteEnd(ses.requestCtx, option)
	if err != nil {
		return err
	}
	return nil
}

func buildColumnMap(rs *plan.ResultColDef) string {
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
	return buf.String()
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

func checkPrivilege(uuids []string, requestCtx context.Context, ses *Session) error {
	f := ses.GetParameterUnit().FileService
	fs := objectio.NewObjectFS(f, catalog.QueryResultMetaDir)
	dirs, err := fs.ListDir(catalog.QueryResultMetaDir)
	if err != nil {
		return err
	}
	for _, id := range uuids {
		var size int64 = -1
		name := catalog.BuildQueryResultMetaName(ses.GetTenantInfo().GetTenant(), id)
		for _, d := range dirs {
			if d.Name == name {
				size = d.Size
			}
		}
		if size == -1 {
			return moerr.NewQueryIdNotFound(requestCtx, id)
		}
		path := catalog.BuildQueryResultMetaPath(ses.GetTenantInfo().GetTenant(), id)
		reader, err := objectio.NewObjectReader(path, f)
		if err != nil {
			return err
		}
		bs, err := reader.ReadAllMeta(requestCtx, size, ses.mp)
		if err != nil {
			return err
		}
		idxs := []uint16{catalog.PLAN_IDX, catalog.AST_IDX}
		iov, err := reader.Read(requestCtx, bs[0].GetExtent(), idxs, ses.mp)
		if err != nil {
			return err
		}
		bat := batch.NewWithSize(len(idxs))
		for i, e := range iov.Entries {
			bat.Vecs[i] = vector.New(catalog.MetaColTypes[idxs[i]])
			if err = bat.Vecs[i].Read(e.Object.([]byte)); err != nil {
				return err
			}
		}
		p := vector.MustStrCols(bat.Vecs[0])[0]
		pn := &plan.Plan{}
		if err = pn.Unmarshal([]byte(p)); err != nil {
			return err
		}
		a := vector.MustStrCols(bat.Vecs[1])[0]
		var ast tree.Statement
		if ast, err = simpleAstUnmarshal([]byte(a)); err != nil {
			return err
		}
		if err = authenticateCanExecuteStatementAndPlan(requestCtx, ses, ast, pn); err != nil {
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
	case *tree.ShowTables, *tree.ShowCreateTable, *tree.ShowColumns, *tree.ShowCreateView, *tree.ShowCreateDatabase:
		s.Typ = int(astShowAboutTable)
	case *tree.ShowProcessList, *tree.ShowErrors, *tree.ShowWarnings, *tree.ShowVariables,
		*tree.ShowStatus, *tree.ShowTarget, *tree.ShowTableStatus,
		*tree.ShowGrants, *tree.ShowCollation, *tree.ShowIndex,
		*tree.ShowTableNumber, *tree.ShowColumnNumber,
		*tree.ShowTableValues, *tree.ShowNodeList,
		*tree.ShowLocks, *tree.ShowFunctionStatus:
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
		bat.Vecs[i] = vector.New(t)
	}
	if err = bat.Vecs[catalog.QUERY_ID_IDX].Append(types.Uuid(m.QueryId), false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[catalog.STATEMENT_IDX].Append([]byte(m.Statement), false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[catalog.ACCOUNT_ID_IDX].Append(m.AccountId, false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[catalog.ROLE_ID_IDX].Append(m.RoleId, false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[catalog.RESULT_PATH_IDX].Append([]byte(m.ResultPath), false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[catalog.CREATE_TIME_IDX].Append(m.CreateTime, false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[catalog.RESULT_SIZE_IDX].Append(m.ResultSize, false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[catalog.COLUMNS_IDX].Append([]byte(m.Columns), false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[catalog.TABLES_IDX].Append([]byte(m.Tables), false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[catalog.USER_ID_IDX].Append(m.UserId, false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[catalog.EXPIRED_TIME_IDX].Append(m.ExpiredTime, false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[catalog.PLAN_IDX].Append([]byte(m.Plan), false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[catalog.AST_IDX].Append([]byte(m.Ast), false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[catalog.COLUMN_MAP_IDX].Append([]byte(m.ColumnMap), false, mp); err != nil {
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
	var reader objectio.Reader
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
	indexes := make([]uint16, len(columnDefs.ResultCols))
	for i := range indexes {
		indexes[i] = uint16(i)
	}
	//=====================
	// preparation
	//=====================
	//prepare batch
	tmpBatch := batch.NewWithSize(len(columnDefs.ResultCols))
	defer tmpBatch.Clean(ses.GetMemPool())
	//prepare result set
	mrs := &MysqlResultSet{}
	typs := make([]types.Type, len(columnDefs.ResultCols))
	for i, c := range columnDefs.ResultCols {
		typs[i] = types.New(types.T(c.Typ.Id), c.Typ.Width, c.Typ.Scale, c.Typ.Precision)
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
		mrs.Data[i] = make([]interface{}, len(columnDefs.ResultCols))
	}
	exportParam := &ExportParam{
		ExportParam: eParam,
	}
	//prepare output queue
	oq := NewOutputQueue(ctx, nil, mrs, 1, exportParam, ses.GetShowStmtType())
	oq.reset()
	//prepare export param
	exportParam.DefaultBufSize = ses.GetParameterUnit().SV.ExportDataDefaultFlushSize
	exportParam.UseFileService = true
	exportParam.FileService = ses.GetParameterUnit().FileService
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
			tmpBatch = batch.NewWithSize(len(columnDefs.ResultCols))
			ioVector, err := reader.Read(ctx, block.GetExtent(), indexes, ses.GetMemPool())
			if err != nil {
				return err
			}
			//read every column
			for colIndex, entry := range ioVector.Entries {
				tmpBatch.Vecs[colIndex] = vector.New(typs[colIndex])
				err = tmpBatch.Vecs[colIndex].Read(entry.Object.([]byte))
				if err != nil {
					return err
				}
			}
			tmpBatch.InitZsOne(tmpBatch.Vecs[0].Length())

			//step2.1: converts it into the csv string
			//step2.2: writes the csv string into the outfile
			n := vector.Length(tmpBatch.Vecs[0])
			for j := 0; j < n; j++ { //row index
				select {
				case <-ctx.Done():
					quit = true
				default:
				}

				if quit {
					break
				}

				if tmpBatch.Zs[j] <= 0 {
					continue
				}
				_, err = extractRowFromEveryVector(ses, tmpBatch, int64(j), oq)
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
	metaFs := objectio.NewObjectFS(ses.GetParameterUnit().FileService, catalog.QueryResultMetaDir)
	metaFiles, err := metaFs.ListDir(catalog.QueryResultMetaDir)
	if err != nil {
		return nil, err
	}
	account := ses.GetTenantInfo()
	if account == nil {
		return nil, moerr.NewInternalError(ctx, "modump does not work without the account info")
	}
	metaName := catalog.BuildQueryResultMetaName(account.GetTenant(), queryId)
	fileSize := getFileSize(metaFiles, metaName)
	if fileSize < 0 {
		return nil, moerr.NewInternalError(ctx, "there is no result file for the query %s", queryId)
	}
	// read meta's meta
	metaFile := catalog.BuildQueryResultMetaPath(account.GetTenant(), queryId)
	reader, err := objectio.NewObjectReader(metaFile, ses.GetParameterUnit().FileService)
	if err != nil {
		return nil, err
	}
	bs, err := reader.ReadAllMeta(ctx, fileSize, ses.GetMemPool())
	if err != nil {
		return nil, err
	}
	idxs := make([]uint16, 1)
	idxs[0] = catalog.COLUMNS_IDX
	// read meta's data
	iov, err := reader.Read(ctx, bs[0].GetExtent(), idxs, ses.GetMemPool())
	if err != nil {
		return nil, err
	}
	vec := vector.New(catalog.MetaColTypes[catalog.COLUMNS_IDX])
	defer vector.Clean(vec, ses.GetMemPool())
	if err = vec.Read(iov.Entries[0].Object.([]byte)); err != nil {
		return nil, err
	}
	def := vector.MustStrCols(vec)[0]
	r := &plan.ResultColDef{}
	if err = r.Unmarshal([]byte(def)); err != nil {
		return nil, err
	}
	return r, err
}

// getResultFiles lists all result files of queryId
func getResultFiles(ctx context.Context, ses *Session, queryId string) ([]resultFileInfo, error) {
	fs := objectio.NewObjectFS(ses.GetParameterUnit().FileService, getQueryResultDir())
	files, err := fs.ListDir(getQueryResultDir())
	if err != nil {
		return nil, err
	}
	account := ses.GetTenantInfo()
	if account == nil {
		return nil, moerr.NewInternalError(ctx, "modump does not work without the account info")
	}
	prefix := getPrefixOfQueryResultFile(account.GetTenant(), queryId)
	ret := make([]resultFileInfo, 0, len(files))
	for _, file := range files {
		if file.IsDir {
			continue
		}
		if strings.HasPrefix(file.Name, prefix) {
			if !strings.HasSuffix(file.Name, ".blk") {
				return nil, moerr.NewInternalError(ctx, "the query result file %s has the invalid name", file.Name)
			}
			indexOfLastUnderbar := strings.LastIndexByte(file.Name, '_')
			if indexOfLastUnderbar == -1 {
				return nil, moerr.NewInternalError(ctx, "the query result file %s has the invalid name", file.Name)
			}
			blockIndexStart := indexOfLastUnderbar + 1
			blockIndexEnd := len(file.Name) - len(".blk")
			if blockIndexStart >= blockIndexEnd {
				return nil, moerr.NewInternalError(ctx, "the query result file %s has the invalid name", file.Name)
			}
			blockIndexStr := file.Name[blockIndexStart:blockIndexEnd]
			blockIndex, err := strconv.ParseInt(blockIndexStr, 10, 64)
			if err != nil {
				return nil, err
			}
			if blockIndex < 0 {
				return nil, moerr.NewInternalError(ctx, "the query result file %s has the invalid name", file.Name)
			}
			ret = append(ret, resultFileInfo{
				name:       file.Name,
				size:       file.Size,
				blockIndex: blockIndex,
			})
		}
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].blockIndex < ret[j].blockIndex
	})
	return ret, err
}

// openResultFile reads all blocks of the result file
func openResultFile(ctx context.Context, ses *Session, fileName string, fileSize int64) (objectio.Reader, []objectio.BlockObject, error) {
	// read result's blocks
	filePath := getPathOfQueryResultFile(fileName)
	reader, err := objectio.NewObjectReader(filePath, ses.GetParameterUnit().FileService)
	if err != nil {
		return nil, nil, err
	}
	bs, err := reader.ReadAllMeta(ctx, fileSize, ses.GetMemPool())
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
