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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"sort"
	"strconv"

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
		return true
	}
	return false
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
	fs := ses.GetParameterUnit().FileService
	// write query result
	path := catalog.BuildQueryResultPath(ses.GetTenantInfo().GetTenant(), uuid.UUID(ses.tStmt.StatementID).String(), ses.GetBlockIdx())
	writer, err := objectio.NewObjectWriter(path, fs)
	if err != nil {
		return err
	}
	_, err = writer.Write(bat)
	if err != nil {
		return err
	}
	_, err = writer.WriteEnd(ses.requestCtx)
	if err != nil {
		return err
	}
	return nil
}

func saveQueryResultMeta(ses *Session) error {
	defer func() {
		ses.ResetBlockIdx()
	}()
	if ses.blockIdx == 0 {
		return nil
	}
	fs := ses.GetParameterUnit().FileService
	// write query result meta
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

	m := &catalog.Meta{
		QueryId:    ses.tStmt.StatementID,
		Statement:  ses.tStmt.Statement,
		AccountId:  ses.GetTenantInfo().GetTenantID(),
		RoleId:     ses.tStmt.RoleId,
		ResultPath: buf.String(),
		CreateTime: types.CurrentTimestamp(),
		ResultSize: 100, // TODO: implement
		Columns:    string(b),
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
	_, err = metaWriter.WriteEnd(ses.requestCtx)
	if err != nil {
		return err
	}
	return nil
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
