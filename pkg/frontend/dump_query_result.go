package frontend

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const QueryResultPath = "s3:/query_result/%s_%s.blk"
const QueryResultDir = "s3:/query_result"
const QueryResultName = "%s_%s.blk"
const QueryResultMetaPath = "s3:/query_result_meta/%s_%s.blk"
const QueryResultMetaDir = "s3:/query_result_meta"
const QueryResultMetaName = "%s_%s.blk"

type Meta struct {
	QueryId    [16]byte
	Statement  string
	AccountId  uint32
	RoleId     uint32
	ResultPath string
	CreateTime types.Timestamp
	ResultSize float64
	Columns    string
}

var (
	MetaColTypes = []types.Type{
		types.New(types.T_uuid, 0, 0, 0),      // query_id
		types.New(types.T_text, 0, 0, 0),      // statement
		types.New(types.T_uint32, 0, 0, 0),    // account_id
		types.New(types.T_uint32, 0, 0, 0),    // role_id
		types.New(types.T_text, 0, 0, 0),      // result_path
		types.New(types.T_timestamp, 0, 0, 0), // create_time
		types.New(types.T_float64, 0, 0, 0),   // result_size
		types.New(types.T_text, 0, 0, 0),      // columns
	}
	MetaColNames = []string{
		"query_id",
		"statement",
		"account_id",
		"role_id",
		"result_path",
		"create_time",
		"result_size",
		"columns",
	}
)

const (
	QUERY_ID_IDX    = 0
	STATEMENT_IDX   = 1
	ACCOUNT_ID_IDX  = 2
	ROLE_ID_IDX     = 3
	RESULT_PATH_IDX = 4
	CREATE_TIME_IDX = 5
	RESULT_SIZE_IDX = 6
	COLUMNS_IDX     = 7
)

func BuildQueryResultPath(accountName, statementId string) string {
	return fmt.Sprintf(QueryResultPath, accountName, statementId)
}
func BuildQueryResultMetaPath(accountName, statementId string) string {
	return fmt.Sprintf(QueryResultMetaPath, accountName, statementId)
}
func BuildQueryResultMetaName(accountName, statementId string) string {
	return fmt.Sprintf(QueryResultMetaName, accountName, statementId)
}

func BuildQueryResultName(accountName, statementId string) string {
	return fmt.Sprintf(QueryResultName, accountName, statementId)
}

// doDumpQueryResult reads data from the query result, converts it into csv and saves it into
// the file designated by the path.
func doDumpQueryResult(ctx context.Context, ses *Session, exportParam *tree.ExportParam) error {
	var err error
	var columnDefs *plan.ResultColDef
	var reader objectio.Reader
	var blocks []objectio.BlockObject
	//step1: open file handler
	if columnDefs, err = openResultMeta(ctx, ses, exportParam.QueryId); err != nil {
		return err
	}
	reader, blocks, err = openResultFile(ctx, ses, exportParam.QueryId)
	if err != nil {
		return err
	}
	//step2: read every batch from the query result
	indexes := make([]uint16, len(columnDefs.ResultCols))
	for i := range indexes {
		indexes[i] = uint16(i)
	}
	tmpBatch := batch.NewWithSize(len(columnDefs.ResultCols))
	defer tmpBatch.Clean(ses.GetMemPool())
	//open output file
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
	oq := NewOutputQueue(ctx, nil, mrs, 1, exportParam, ses.GetShowStmtType())
	oq.reset()
	exportParam.DefaultBufSize = ses.GetParameterUnit().SV.ExportDataDefaultFlushSize
	initExportFileParam(exportParam, mrs)
	if err = openNewFile(ctx, exportParam, mrs); err != nil {
		return err
	}
	quit := false
	//read every block
	for _, block := range blocks {
		select {
		case <-ctx.Done():
			quit = true
		}

		if quit {
			break
		}
		tmpBatch.Clean(ses.GetMemPool())
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

	err = oq.flush()
	if err != nil {
		return err
	}

	return err
}

// openResultMeta checks the query result of the queryId exists or not
func openResultMeta(ctx context.Context, ses *Session, queryId string) (*plan.ResultColDef, error) {
	metaFs := objectio.NewObjectFS(ses.GetParameterUnit().FileService, QueryResultMetaDir)
	metaFiles, err := metaFs.ListDir(QueryResultMetaDir)
	if err != nil {
		return nil, err
	}
	account := ses.GetTenantInfo()
	if account == nil {
		return nil, moerr.NewInternalError(ctx, "modump does not work without the account info")
	}
	metaName := BuildQueryResultMetaName(account.GetTenant(), queryId)
	fileSize := getFileSize(metaFiles, metaName)
	if fileSize < 0 {
		return nil, moerr.NewInternalError(ctx, "there is no result file for the query %s", queryId)
	}
	// read meta's meta
	metaFile := BuildQueryResultMetaPath(account.GetTenant(), queryId)
	reader, err := objectio.NewObjectReader(metaFile, ses.GetParameterUnit().FileService)
	if err != nil {
		return nil, err
	}
	bs, err := reader.ReadAllMeta(ctx, fileSize, ses.GetMemPool())
	if err != nil {
		return nil, err
	}
	idxs := make([]uint16, 1)
	idxs[0] = COLUMNS_IDX
	// read meta's data
	iov, err := reader.Read(ctx, bs[0].GetExtent(), idxs, ses.GetMemPool())
	if err != nil {
		return nil, err
	}
	vec := vector.New(MetaColTypes[COLUMNS_IDX])
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

func openResultFile(ctx context.Context, ses *Session, queryId string) (objectio.Reader, []objectio.BlockObject, error) {
	fs := objectio.NewObjectFS(ses.GetParameterUnit().FileService, QueryResultDir)
	files, err := fs.ListDir(QueryResultDir)
	if err != nil {
		return nil, nil, err
	}
	account := ses.GetTenantInfo()
	if account == nil {
		return nil, nil, moerr.NewInternalError(ctx, "modump does not work without the account info")
	}

	name := BuildQueryResultName(account.GetTenant(), queryId)
	fileSize := getFileSize(files, name)
	if fileSize == -1 {
		return nil, nil, moerr.NewInternalError(ctx, "there is no result file for the query %s", queryId)
	}
	// read result's meta
	path := BuildQueryResultPath(account.GetTenant(), queryId)
	reader, err := objectio.NewObjectReader(path, ses.GetParameterUnit().FileService)
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
