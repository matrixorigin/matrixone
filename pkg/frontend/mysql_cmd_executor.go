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
	"encoding/binary"
	goErrors "errors"
	"fmt"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/explain"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/logutil/logutil2"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/matrixorigin/matrixone/pkg/util/trace"

	"github.com/google/uuid"
)

func onlyCreateStatementErrorInfo() string {
	return "Only CREATE of DDL is supported in transactions"
}

func parameterModificationInTxnErrorInfo() string {
	return "Uncommitted transaction exists. Please commit or rollback first."
}

var (
	errorComplicateExprIsNotSupported              = goErrors.New("the complicate expression is not supported")
	errorNumericTypeIsNotSupported                 = goErrors.New("the numeric type is not supported")
	errorUnaryMinusForNonNumericTypeIsNotSupported = goErrors.New("unary minus for no numeric type is not supported")
	errorOnlyCreateStatement                       = goErrors.New(onlyCreateStatementErrorInfo())
	errorAdministrativeStatement                   = goErrors.New("administrative command is unsupported in transactions")
	errorParameterModificationInTxn                = goErrors.New(parameterModificationInTxnErrorInfo())
	errorUnclassifiedStatement                     = goErrors.New("unclassified statement appears in uncommitted transaction")
)

const (
	prefixPrepareStmtName       = "__mo_stmt_id"
	prefixPrepareStmtSessionVar = "__mo_stmt_var"
)

func getPrepareStmtName(stmtID uint32) string {
	return fmt.Sprintf("%s_%d", prefixPrepareStmtName, stmtID)
}

func GetPrepareStmtID(name string) (int, error) {
	idx := len(prefixPrepareStmtName) + 1
	if idx >= len(name) {
		return -1, moerr.NewError(moerr.INTERNAL_ERROR, "can not get prepare stmtID")
	}
	return strconv.Atoi(name[idx:])
}

func getPrepareStmtSessionVarName(index int) string {
	return fmt.Sprintf("%s_%d", prefixPrepareStmtSessionVar, index)
}

// TableInfoCache tableInfos of a database
type TableInfoCache struct {
	db         string
	tableInfos map[string][]ColumnInfo
}

type MysqlCmdExecutor struct {
	CmdExecutorImpl

	//for cmd 0x4
	TableInfoCache

	//the count of sql has been processed
	sqlCount uint64

	ses *Session

	routineMgr *RoutineManager

	cancelRequestFunc context.CancelFunc
}

func (mce *MysqlCmdExecutor) PrepareSessionBeforeExecRequest(ses *Session) {
	mce.ses = ses
}

func (mce *MysqlCmdExecutor) GetSession() *Session {
	return mce.ses
}

// get new process id
func (mce *MysqlCmdExecutor) getNextProcessId() string {
	/*
		temporary method:
		routineId + sqlCount
	*/
	routineId := mce.GetSession().protocol.ConnectionID()
	return fmt.Sprintf("%d%d", routineId, mce.sqlCount)
}

func (mce *MysqlCmdExecutor) addSqlCount(a uint64) {
	mce.sqlCount += a
}

func (mce *MysqlCmdExecutor) SetRoutineManager(mgr *RoutineManager) {
	mce.routineMgr = mgr
}

func (mce *MysqlCmdExecutor) GetRoutineManager() *RoutineManager {
	return mce.routineMgr
}

func (mce *MysqlCmdExecutor) RecordStatement(ctx context.Context, ses *Session, proc *process.Process, cw ComputationWrapper, beginIns time.Time) context.Context {
	sessInfo := proc.SessionInfo
	var stmID uuid.UUID
	copy(stmID[:], cw.GetUUID())
	var txnID uuid.UUID
	if handler := ses.GetTxnHandler(); handler.IsValidTxn() {
		copy(txnID[:], handler.GetTxn().Txn().ID)
	}
	var sesID uuid.UUID
	copy(sesID[:], ses.GetUUID())
	fmtCtx := tree.NewFmtCtx(dialect.MYSQL)
	cw.GetAst().Format(fmtCtx)
	trace.ReportStatement(
		ctx,
		&trace.StatementInfo{
			StatementID:          stmID,
			TransactionID:        txnID,
			SessionID:            sesID,
			Account:              "account", //fixme: sessInfo.GetAccount()
			User:                 sessInfo.GetUser(),
			Host:                 sessInfo.GetHost(),
			Database:             sessInfo.GetDatabase(),
			Statement:            fmtCtx.String(),
			StatementFingerprint: "", // fixme
			StatementTag:         "", // fixme
			RequestAt:            util.NowNS(),
		},
	)
	return trace.ContextWithSpanContext(ctx, trace.SpanContextWithID(trace.TraceID(stmID)))
}

// outputPool outputs the data
type outputPool interface {
	resetLineStr()

	reset()

	getEmptyRow() ([]interface{}, error)

	flush() error
}

var _ outputPool = &outputQueue{}
var _ outputPool = &fakeOutputQueue{}

type outputQueue struct {
	proto        MysqlProtocol
	mrs          *MysqlResultSet
	rowIdx       uint64
	length       uint64
	ep           *tree.ExportParam
	lineStr      []byte
	showStmtType ShowStatementType

	getEmptyRowTime time.Duration
	flushTime       time.Duration
}

func (o *outputQueue) resetLineStr() {
	o.lineStr = o.lineStr[:0]
}

func NewOutputQueue(proto MysqlProtocol, mrs *MysqlResultSet, length uint64, ep *tree.ExportParam, showStatementType ShowStatementType) *outputQueue {
	return &outputQueue{
		proto:        proto,
		mrs:          mrs,
		rowIdx:       0,
		length:       length,
		ep:           ep,
		showStmtType: showStatementType,
	}
}

func (o *outputQueue) reset() {
	o.getEmptyRowTime = 0
	o.flushTime = 0
}

/*
getEmptyRow returns a empty space for filling data.
If there is no space, it flushes the data into the protocol
and returns an empty space then.
*/
func (o *outputQueue) getEmptyRow() ([]interface{}, error) {
	//begin := time.Now()
	//defer func() {
	//	o.getEmptyRowTime += time.Since(begin)
	//}()
	if o.rowIdx >= o.length {
		if err := o.flush(); err != nil {
			return nil, err
		}
	}

	row := o.mrs.Data[o.rowIdx]
	o.rowIdx++
	return row, nil
}

/*
flush will force the data flushed into the protocol.
*/
func (o *outputQueue) flush() error {
	//begin := time.Now()
	//defer func() {
	//	o.flushTime += time.Since(begin)
	//}()
	if o.rowIdx <= 0 {
		return nil
	}
	if o.ep.Outfile {
		if err := exportDataToCSVFile(o); err != nil {
			logutil.Errorf("export to csv file error %v \n", err)
			return err
		}
	} else {
		//send group of row
		if o.showStmtType == ShowColumns {
			o.rowIdx = 0
			return nil
		}

		if err := o.proto.SendResultSetTextBatchRowSpeedup(o.mrs, o.rowIdx); err != nil {
			logutil.Errorf("flush error %v \n", err)
			return err
		}
	}
	o.rowIdx = 0
	return nil
}

// fakeOutputQueue saves the data into the session.
type fakeOutputQueue struct {
	mrs *MysqlResultSet
}

func newFakeOutputQueue(mrs *MysqlResultSet) outputPool {
	return &fakeOutputQueue{mrs: mrs}
}

func (foq *fakeOutputQueue) resetLineStr() {}

func (foq *fakeOutputQueue) reset() {}

func (foq *fakeOutputQueue) getEmptyRow() ([]interface{}, error) {
	row := make([]interface{}, foq.mrs.GetColumnCount())
	foq.mrs.AddRow(row)
	return row, nil
}

func (foq *fakeOutputQueue) flush() error {
	return nil
}

const (
	primaryKeyPos = 25
)

/*
handle show create database in plan2 and tae
*/
// func handleShowCreateDatabase(ses *Session) error {
// 	dbNameIndex := ses.Mrs.Name2Index["Database"]
// 	dbsqlIndex := ses.Mrs.Name2Index["Create Database"]
// 	firstRow := ses.Data[0]
// 	dbName := firstRow[dbNameIndex]
// 	createDBSql := fmt.Sprintf("CREATE DATABASE `%s`", dbName)
// 	firstRow[dbsqlIndex] = createDBSql

// 	row := make([]interface{}, 2)
// 	row[0] = dbName
// 	row[1] = createDBSql

// 	ses.Mrs.AddRow(row)
// 	if err := ses.GetMysqlProtocol().SendResultSetTextBatchRowSpeedup(ses.Mrs, 1); err != nil {
// 		logutil.Errorf("handleShowCreateDatabase error %v \n", err)
// 		return err
// 	}
// 	return nil
// }

/*
handle show columns from table in plan2 and tae
*/
func handleShowColumns(ses *Session) error {
	for _, d := range ses.Data {
		row := make([]interface{}, 6)
		colName := string(d[0].([]byte))
		if colName == "PADDR" {
			continue
		}
		row[0] = colName
		typ := types.Type{Oid: types.T(d[1].(int32))}
		row[1] = typ.String()
		if d[2].(int8) == 0 {
			row[2] = "NO"
		} else {
			row[2] = "YES"
		}
		row[3] = d[3]
		row[4] = "NULL"
		row[5] = d[5]
		ses.Mrs.AddRow(row)
	}
	if err := ses.GetMysqlProtocol().SendResultSetTextBatchRowSpeedup(ses.Mrs, ses.Mrs.GetRowCount()); err != nil {
		logutil.Errorf("handleShowCreateTable error %v \n", err)
		return err
	}
	return nil
}

/*
extract the data from the pipeline.
obj: routine obj
TODO:Add error
Warning: The pipeline is the multi-thread environment. The getDataFromPipeline will

	access the shared data. Be careful when it writes the shared data.
*/
func getDataFromPipeline(obj interface{}, bat *batch.Batch) error {
	ses := obj.(*Session)

	if bat == nil {
		return nil
	}

	goID := GetRoutineId()

	logutil.Infof("goid %d \n", goID)
	enableProfile := ses.Pu.SV.EnableProfileGetDataFromPipeline

	var cpuf *os.File = nil
	if enableProfile {
		cpuf, _ = os.Create("cpu_profile")
	}

	begin := time.Now()

	proto := ses.GetMysqlProtocol()
	proto.PrepareBeforeProcessingResultSet()

	//Create a new temporary resultset per pipeline thread.
	mrs := &MysqlResultSet{}
	//Warning: Don't change ResultColumns in this.
	//Reference the shared ResultColumns of the session among multi-thread.
	mrs.Columns = ses.Mrs.Columns
	mrs.Name2Index = ses.Mrs.Name2Index

	begin3 := time.Now()
	countOfResultSet := 1
	//group row
	mrs.Data = make([][]interface{}, countOfResultSet)
	for i := 0; i < countOfResultSet; i++ {
		mrs.Data[i] = make([]interface{}, len(bat.Vecs))
	}
	allocateOutBufferTime := time.Since(begin3)

	oq := NewOutputQueue(proto, mrs, uint64(countOfResultSet), ses.ep, ses.showStmtType)
	oq.reset()

	row2colTime := time.Duration(0)

	procBatchBegin := time.Now()

	n := vector.Length(bat.Vecs[0])

	if enableProfile {
		if err := pprof.StartCPUProfile(cpuf); err != nil {
			return err
		}
	}
	for j := 0; j < n; j++ { //row index
		if oq.ep.Outfile {
			select {
			case <-ses.requestCtx.Done():
				{
					return nil
				}
			default:
				{
				}
			}
		}

		if bat.Zs[j] <= 0 {
			continue
		}
		row, err := extractRowFromEveryVector(ses, bat, int64(j), oq)
		if err != nil {
			return err
		}
		if oq.showStmtType == ShowColumns {
			row2 := make([]interface{}, len(row))
			copy(row2, row)
			ses.Data = append(ses.Data, row2)
		}
	}

	//logutil.Infof("row group -+> %v ", oq.getData())

	err := oq.flush()
	if err != nil {
		return err
	}

	if enableProfile {
		pprof.StopCPUProfile()
	}

	procBatchTime := time.Since(procBatchBegin)
	tTime := time.Since(begin)
	logutil.Infof("rowCount %v \n"+
		"time of getDataFromPipeline : %s \n"+
		"processBatchTime %v \n"+
		"row2colTime %v \n"+
		"allocateOutBufferTime %v \n"+
		"outputQueue.flushTime %v \n"+
		"processBatchTime - row2colTime - allocateOutbufferTime - flushTime %v \n"+
		"restTime(=tTime - row2colTime - allocateOutBufferTime) %v \n"+
		"protoStats %s\n",
		n,
		tTime,
		procBatchTime,
		row2colTime,
		allocateOutBufferTime,
		oq.flushTime,
		procBatchTime-row2colTime-allocateOutBufferTime-oq.flushTime,
		tTime-row2colTime-allocateOutBufferTime,
		proto.GetStats())

	return nil
}

// extractRowFromEveryVector gets the j row from the every vector and outputs the row
func extractRowFromEveryVector(ses *Session, dataSet *batch.Batch, j int64, oq outputPool) ([]interface{}, error) {
	row, err := oq.getEmptyRow()
	if err != nil {
		return nil, err
	}
	var rowIndex = int64(j)
	for i, vec := range dataSet.Vecs { //col index
		rowIndexBackup := rowIndex
		if vec.IsScalarNull() {
			row[i] = nil
			continue
		}
		if vec.IsScalar() {
			rowIndex = 0
		}

		err = extractRowFromVector(ses, vec, i, row, rowIndex)
		if err != nil {
			return nil, err
		}
		rowIndex = rowIndexBackup
	}
	//duplicate rows
	for i := int64(0); i < dataSet.Zs[j]-1; i++ {
		erow, rr := oq.getEmptyRow()
		if rr != nil {
			return nil, rr
		}
		for l := 0; l < len(dataSet.Vecs); l++ {
			erow[l] = row[l]
		}
	}
	return row, nil
}

// extractRowFromVector gets the rowIndex row from the i vector
func extractRowFromVector(ses *Session, vec *vector.Vector, i int, row []interface{}, rowIndex int64) error {
	switch vec.Typ.Oid { //get col
	case types.T_json:
		if !nulls.Any(vec.Nsp) {
			bytes := vec.Col.(*types.Bytes)
			vs := make([]bytejson.ByteJson, 0, len(bytes.Lengths))
			for i, length := range bytes.Lengths {
				off := bytes.Offsets[i]
				vs = append(vs, types.DecodeJson(bytes.Data[off:off+length]))
			}
			row[i] = vs[rowIndex]
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) {
				row[i] = nil
			} else {
				bytes := vec.Col.(*types.Bytes)
				vs := make([]bytejson.ByteJson, 0, len(bytes.Lengths))
				for i, length := range bytes.Lengths {
					off := bytes.Offsets[i]
					vs = append(vs, types.DecodeJson(bytes.Data[off:off+length]))
				}
				row[i] = vs[rowIndex]
			}
		}
	case types.T_bool:
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.([]bool)
			row[i] = vs[rowIndex]
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.([]bool)
				row[i] = vs[rowIndex]
			}
		}
	case types.T_int8:
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.([]int8)
			row[i] = vs[rowIndex]
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.([]int8)
				row[i] = vs[rowIndex]
			}
		}
	case types.T_uint8:
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.([]uint8)
			row[i] = vs[rowIndex]
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.([]uint8)
				row[i] = vs[rowIndex]
			}
		}
	case types.T_int16:
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.([]int16)
			row[i] = vs[rowIndex]
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.([]int16)
				row[i] = vs[rowIndex]
			}
		}
	case types.T_uint16:
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.([]uint16)
			row[i] = vs[rowIndex]
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.([]uint16)
				row[i] = vs[rowIndex]
			}
		}
	case types.T_int32:
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.([]int32)
			row[i] = vs[rowIndex]
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.([]int32)
				row[i] = vs[rowIndex]
			}
		}
	case types.T_uint32:
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.([]uint32)
			row[i] = vs[rowIndex]
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.([]uint32)
				row[i] = vs[rowIndex]
			}
		}
	case types.T_int64:
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.([]int64)
			row[i] = vs[rowIndex]
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.([]int64)
				row[i] = vs[rowIndex]
			}
		}
	case types.T_uint64:
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.([]uint64)
			row[i] = vs[rowIndex]
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.([]uint64)
				row[i] = vs[rowIndex]
			}
		}
	case types.T_float32:
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.([]float32)
			row[i] = vs[rowIndex]
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.([]float32)
				row[i] = vs[rowIndex]
			}
		}
	case types.T_float64:
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.([]float64)
			row[i] = vs[rowIndex]
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.([]float64)
				row[i] = vs[rowIndex]
			}
		}
	case types.T_char:
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.(*types.Bytes)
			row[i] = vs.Get(rowIndex)
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.(*types.Bytes)
				row[i] = vs.Get(rowIndex)
			}
		}
	case types.T_varchar:
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.(*types.Bytes)
			row[i] = vs.Get(rowIndex)
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.(*types.Bytes)
				row[i] = vs.Get(rowIndex)
			}
		}
	case types.T_date:
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.([]types.Date)
			row[i] = vs[rowIndex]
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.([]types.Date)
				row[i] = vs[rowIndex]
			}
		}
	case types.T_datetime:
		precision := vec.Typ.Precision
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.([]types.Datetime)
			row[i] = vs[rowIndex].String2(precision)
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.([]types.Datetime)
				row[i] = vs[rowIndex].String2(precision)
			}
		}
	case types.T_timestamp:
		precision := vec.Typ.Precision
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.([]types.Timestamp)
			row[i] = vs[rowIndex].String2(ses.timeZone, precision)
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.([]types.Timestamp)
				row[i] = vs[rowIndex].String2(ses.timeZone, precision)
			}
		}
	case types.T_decimal64:
		scale := vec.Typ.Scale
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.([]types.Decimal64)
			row[i] = vs[rowIndex].ToStringWithScale(scale)
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) {
				row[i] = nil
			} else {
				vs := vec.Col.([]types.Decimal64)
				row[i] = vs[rowIndex].ToStringWithScale(scale)
			}
		}
	case types.T_decimal128:
		scale := vec.Typ.Scale
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.([]types.Decimal128)
			row[i] = vs[rowIndex].ToStringWithScale(scale)
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) {
				row[i] = nil
			} else {
				vs := vec.Col.([]types.Decimal128)
				row[i] = vs[rowIndex].ToStringWithScale(scale)
			}
		}
	case types.T_blob:
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.(*types.Bytes)
			row[i] = vs.Get(rowIndex)
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.(*types.Bytes)
				row[i] = vs.Get(rowIndex)
			}
		}
	default:
		logutil.Errorf("extractRowFromVector : unsupported type %d \n", vec.Typ.Oid)
		return fmt.Errorf("extractRowFromVector : unsupported type %d", vec.Typ.Oid)
	}
	return nil
}

func (mce *MysqlCmdExecutor) handleChangeDB(requestCtx context.Context, db string) error {
	ses := mce.GetSession()
	txnHandler := ses.GetTxnHandler()
	//TODO: check meta data
	if _, err := ses.Pu.StorageEngine.Database(requestCtx, db, txnHandler.GetTxn()); err != nil {
		//echo client. no such database
		return NewMysqlError(ER_BAD_DB_ERROR, db)
	}
	oldDB := ses.GetDatabaseName()
	ses.SetDatabaseName(db)

	logutil.Infof("User %s change database from [%s] to [%s]\n", ses.protocol.GetUserName(), oldDB, ses.protocol.GetDatabaseName())

	return nil
}

/*
handle "SELECT @@xxx.yyyy"
*/
func (mce *MysqlCmdExecutor) handleSelectVariables(ve *tree.VarExpr) error {
	var err error = nil
	ses := mce.GetSession()
	proto := ses.protocol

	col := new(MysqlColumn)
	col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col.SetName("@@" + ve.Name)
	ses.Mrs.AddColumn(col)

	row := make([]interface{}, 1)
	if ve.System {
		if ve.Global {
			val, err := ses.GetGlobalVar(ve.Name)
			if err != nil {
				return err
			}
			row[0] = val
		} else {
			val, err := ses.GetSessionVar(ve.Name)
			if err != nil {
				return err
			}
			row[0] = val
		}
	} else {
		//user defined variable
		_, val, err := ses.GetUserDefinedVar(ve.Name)
		if err != nil {
			return err
		}
		row[0] = val
	}

	ses.Mrs.AddRow(row)

	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.Mrs)
	resp := NewResponse(ResultResponse, 0, int(COM_QUERY), mer)

	if err := proto.SendResponse(resp); err != nil {
		return fmt.Errorf("routine send response failed. error:%v ", err)
	}
	return err
}

/*
handle Load DataSource statement
*/
func (mce *MysqlCmdExecutor) handleLoadData(requestCtx context.Context, load *tree.Load) error {
	var err error
	ses := mce.GetSession()
	proto := ses.protocol

	logutil.Infof("+++++load data")
	/*
		TODO:support LOCAL
	*/
	if load.Local {
		return fmt.Errorf("LOCAL is unsupported now")
	}
	if load.Param.Tail.Fields == nil || len(load.Param.Tail.Fields.Terminated) == 0 {
		return fmt.Errorf("load need FIELDS TERMINATED BY ")
	}

	if load.Param.Tail.Fields != nil && load.Param.Tail.Fields.EscapedBy != 0 {
		return fmt.Errorf("EscapedBy field is unsupported now")
	}

	/*
		check file
	*/
	exist, isfile, err := PathExists(load.Param.Filepath)
	if err != nil || !exist {
		return fmt.Errorf("file %s does exist. err:%v", load.Param.Filepath, err)
	}

	if !isfile {
		return fmt.Errorf("file %s is a directory", load.Param.Filepath)
	}

	/*
		check database
	*/
	loadDb := string(load.Table.Schema())
	loadTable := string(load.Table.Name())
	if loadDb == "" {
		if proto.GetDatabaseName() == "" {
			return fmt.Errorf("load data need database")
		}

		//then, it uses the database name in the session
		loadDb = ses.protocol.GetDatabaseName()
	}

	txnHandler := ses.GetTxnHandler()
	if ses.InMultiStmtTransactionMode() {
		return fmt.Errorf("do not support the Load in a transaction started by BEGIN/START TRANSACTION statement")
	}
	dbHandler, err := ses.GetStorage().Database(requestCtx, loadDb, txnHandler.GetTxn())
	if err != nil {
		//echo client. no such database
		return NewMysqlError(ER_BAD_DB_ERROR, loadDb)
	}

	//change db to the database in the LOAD DATA statement if necessary
	if loadDb != proto.GetDatabaseName() {
		oldDB := proto.GetDatabaseName()
		proto.SetDatabaseName(loadDb)
		logutil.Infof("User %s change database from [%s] to [%s] in LOAD DATA\n", proto.GetUserName(), oldDB, proto.GetDatabaseName())
	}

	/*
		check table
	*/
	tableHandler, err := dbHandler.Relation(requestCtx, loadTable)
	if err != nil {
		//echo client. no such table
		return NewMysqlError(ER_NO_SUCH_TABLE, loadDb, loadTable)
	}

	/*
		execute load data
	*/
	result, err := mce.LoadLoop(requestCtx, load, dbHandler, tableHandler, loadDb)
	if err != nil {
		return err
	}

	/*
		response
	*/
	info := NewMysqlError(ER_LOAD_INFO, result.Records, result.Deleted, result.Skipped, result.Warnings, result.WriteTimeout).Error()
	resp := NewOkResponse(result.Records, 0, uint16(result.Warnings), 0, int(COM_QUERY), info)
	if err = proto.SendResponse(resp); err != nil {
		return fmt.Errorf("routine send response failed. error:%v ", err)
	}
	return nil
}

/*
handle cmd CMD_FIELD_LIST
*/
func (mce *MysqlCmdExecutor) handleCmdFieldList(requestCtx context.Context, icfl *InternalCmdFieldList) error {
	var err error
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()
	tableName := icfl.tableName

	dbName := ses.GetDatabaseName()
	if dbName == "" {
		return NewMysqlError(ER_NO_DB_ERROR)
	}

	//Get table infos for the database from the cube
	//case 1: there are no table infos for the db
	//case 2: db changed
	var attrs []ColumnInfo
	if ses.IsTaeEngine() {
		if mce.tableInfos == nil || mce.db != dbName {
			txnHandler := ses.GetTxnHandler()
			eng := ses.GetStorage()
			db, err := eng.Database(requestCtx, dbName, txnHandler.GetTxn())
			if err != nil {
				return err
			}

			names, err := db.Relations(requestCtx)
			if err != nil {
				return err
			}
			for _, name := range names {
				table, err := db.Relation(requestCtx, name)
				if err != nil {
					return err
				}

				defs, err := table.TableDefs(requestCtx)
				if err != nil {
					return err
				}
				for _, def := range defs {
					if attr, ok := def.(*engine.AttributeDef); ok {
						attrs = append(attrs, &engineColumnInfo{
							name: attr.Attr.Name,
							typ:  attr.Attr.Type,
						})
					}
				}
			}

			if mce.tableInfos == nil {
				mce.tableInfos = make(map[string][]ColumnInfo)
			}
			mce.tableInfos[tableName] = attrs
		}
	}

	cols, ok := mce.tableInfos[tableName]
	if !ok {
		//just give the empty info when there is no such table.
		attrs = make([]ColumnInfo, 0)
	} else {
		attrs = cols
	}

	for _, c := range attrs {
		col := new(MysqlColumn)
		col.SetName(c.GetName())
		err = convertEngineTypeToMysqlType(c.GetType(), col)
		if err != nil {
			return err
		}

		/*
			mysql CMD_FIELD_LIST response: send the column definition per column
		*/
		err = proto.SendColumnDefinitionPacket(col, int(COM_FIELD_LIST))
		if err != nil {
			return err
		}
	}

	/*
		mysql CMD_FIELD_LIST response: End after the column has been sent.
		send EOF packet
	*/
	err = proto.SendEOFPacketIf(0, 0)
	if err != nil {
		return err
	}

	return err
}

/*
handle setvar
*/
func (mce *MysqlCmdExecutor) handleSetVar(sv *tree.SetVar) error {
	var err error = nil
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()

	setVarFunc := func(system, global bool, name string, value interface{}) error {
		if system {
			if global {
				err = ses.SetGlobalVar(name, value)
				if err != nil {
					return err
				}
			} else {
				err = ses.SetSessionVar(name, value)
				if err != nil {
					return err
				}
			}

			if strings.ToLower(name) == "autocommit" {
				svbt := SystemVariableBoolType{}
				newValue, err2 := svbt.Convert(value)
				if err2 != nil {
					return err2
				}
				err = ses.SetAutocommit(svbt.IsTrue(newValue))
				if err != nil {
					return err
				}
			}
		} else {
			err = ses.SetUserDefinedVar(name, value)
			if err != nil {
				return err
			}
		}
		return nil
	}

	for _, assign := range sv.Assignments {
		name := assign.Name
		var value interface{}

		//TODO: set var needs to be moved into plan2
		//convert into definite type
		value, err = GetSimpleExprValue(assign.Value)
		if err != nil {
			return err
		}

		//TODO : fix SET NAMES after parser is ready
		if name == "names" {
			//replaced into three system variable:
			//character_set_client, character_set_connection, and character_set_results
			replacedBy := []string{
				"character_set_client", "character_set_connection", "character_set_results",
			}
			for _, rb := range replacedBy {
				err = setVarFunc(assign.System, assign.Global, rb, value)
				if err != nil {
					return err
				}
			}
		} else {
			err = setVarFunc(assign.System, assign.Global, name, value)
			if err != nil {
				return err
			}
		}
	}

	resp := NewOkResponse(0, 0, 0, 0, int(COM_QUERY), "")
	if err = proto.SendResponse(resp); err != nil {
		return fmt.Errorf("routine send response failed. error:%v ", err)
	}
	return nil
}

/*
handle show variables
*/
func (mce *MysqlCmdExecutor) handleShowVariables(sv *tree.ShowVariables) error {
	var err error = nil
	ses := mce.GetSession()
	proto := mce.GetSession().protocol

	col1 := new(MysqlColumn)
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col1.SetName("VARIABLE_NAME")

	col2 := new(MysqlColumn)
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col2.SetName("VARIABLE_VALUE")

	ses.Mrs.AddColumn(col1)
	ses.Mrs.AddColumn(col2)

	var hasLike = false
	var likePattern = ""
	if sv.Like != nil {
		hasLike = true
		likePattern = strings.ToLower(sv.Like.Right.String())
	}

	var sysVars map[string]interface{}
	if sv.Global {
		sysVars = make(map[string]interface{})
		for name := range sysVars {
			if val, err := ses.GetGlobalVar(name); err == nil {
				sysVars[name] = val
			} else if !goErrors.Is(err, errorSystemVariableSessionEmpty) {
				return err
			}
		}
	} else {
		sysVars = ses.CopyAllSessionVars()
	}

	rows := make([][]interface{}, 0, len(sysVars))
	for name, value := range sysVars {
		if hasLike && !WildcardMatch(likePattern, name) {
			continue
		}
		row := make([]interface{}, 2)
		row[0] = name
		gsv, ok := gSysVariables.GetDefinitionOfSysVar(name)
		if !ok {
			return errorSystemVariableDoesNotExist
		}
		row[1] = value
		if _, ok := gsv.GetType().(SystemVariableBoolType); ok {
			if value == 1 {
				row[1] = "on"
			} else {
				row[1] = "off"
			}
		}

		rows = append(rows, row)
	}

	//sort by name
	sort.Slice(rows, func(i, j int) bool {
		return rows[i][0].(string) < rows[j][0].(string)
	})

	for _, row := range rows {
		ses.Mrs.AddRow(row)
	}

	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.Mrs)
	resp := NewResponse(ResultResponse, 0, int(COM_QUERY), mer)

	if err := proto.SendResponse(resp); err != nil {
		return fmt.Errorf("routine send response failed. error:%v ", err)
	}
	return err
}

func (mce *MysqlCmdExecutor) handleAnalyzeStmt(requestCtx context.Context, stmt *tree.AnalyzeStmt) error {
	// rewrite analyzeStmt to `select approx_count_distinct(col), .. from tbl`
	// IMO, this approach is simple and future-proof
	// Although this rewriting processing could have been handled in rewrite module,
	// `handleAnalyzeStmt` can be easily managed by cron jobs in the future
	ctx := tree.NewFmtCtx(dialect.MYSQL)
	ctx.WriteString("select ")
	for i, ident := range stmt.Cols {
		if i > 0 {
			ctx.WriteByte(',')
		}
		ctx.WriteString("approx_count_distinct(")
		ctx.WriteString(string(ident))
		ctx.WriteByte(')')
	}
	ctx.WriteString(" from ")
	stmt.Table.Format(ctx)
	sql := ctx.String()
	return mce.doComQuery(requestCtx, sql)
}

// Note: for pass the compile quickly. We will remove the comments in the future.
func (mce *MysqlCmdExecutor) handleExplainStmt(stmt *tree.ExplainStmt) error {
	es := explain.NewExplainDefaultOptions()

	for _, v := range stmt.Options {
		if strings.EqualFold(v.Name, "VERBOSE") {
			if strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL" {
				es.Verbose = true
			} else if strings.EqualFold(v.Value, "FALSE") {
				es.Verbose = false
			} else {
				return errors.New(errno.InvalidOptionValue, fmt.Sprintf("%s requires a Boolean value", v.Name))
			}
		} else if strings.EqualFold(v.Name, "ANALYZE") {
			if strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL" {
				es.Anzlyze = true
			} else if strings.EqualFold(v.Value, "FALSE") {
				es.Anzlyze = false
			} else {
				return errors.New(errno.InvalidOptionValue, fmt.Sprintf("%s requires a Boolean value", v.Name))
			}
		} else if strings.EqualFold(v.Name, "FORMAT") {
			if v.Name == "NULL" {
				return errors.New(errno.InvalidOptionValue, fmt.Sprintf("%s requires a parameter", v.Name))
			} else if strings.EqualFold(v.Value, "TEXT") {
				es.Format = explain.EXPLAIN_FORMAT_TEXT
			} else if strings.EqualFold(v.Value, "JSON") {
				es.Format = explain.EXPLAIN_FORMAT_JSON
			} else if strings.EqualFold(v.Value, "DOT") {
				es.Format = explain.EXPLAIN_FORMAT_DOT
			} else {
				return errors.New(errno.InvalidOptionValue, fmt.Sprintf("unrecognized value for EXPLAIN option \"%s\": \"%s\"", v.Name, v.Value))
			}
		} else {
			return errors.New(errno.InvalidOptionValue, fmt.Sprintf("unrecognized EXPLAIN option \"%s\"", v.Name))
		}
	}

	switch stmt.Statement.(type) {
	case *tree.Delete:
		mce.ses.GetTxnCompileCtx().SetQueryType(TXN_DELETE)
	case *tree.Update:
		mce.ses.GetTxnCompileCtx().SetQueryType(TXN_UPDATE)
	default:
		mce.ses.GetTxnCompileCtx().SetQueryType(TXN_DEFAULT)
	}

	//get query optimizer and execute Optimize
	plan, err := buildPlan(mce.ses.txnCompileCtx, stmt.Statement)
	if err != nil {
		return err
	}

	if err != nil {
		logutil.Errorf("build query plan and optimize failed, error: %v", err)
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("build query plan and optimize failed:'%v'", err))
	}

	// build explain data buffer
	buffer := explain.NewExplainDataBuffer()
	// generator query explain
	explainQuery := explain.NewExplainQueryImpl(plan.GetQuery())
	err = explainQuery.ExplainPlan(buffer, es)
	if err != nil {
		logutil.Errorf("explain Query statement error: %v", err)
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("explain Query statement error:%v", err))
	}

	session := mce.GetSession()
	protocol := session.GetMysqlProtocol()

	explainColName := "QUERY PLAN"
	columns, err := GetExplainColumns(explainColName)
	if err != nil {
		logutil.Errorf("GetColumns from ExplainColumns handler failed, error: %v", err)
		return err
	}

	//	Step 1 : send column count and column definition.
	//send column count
	colCnt := uint64(len(columns))
	err = protocol.SendColumnCountPacket(colCnt)
	if err != nil {
		return err
	}
	//send columns
	//column_count * Protocol::ColumnDefinition packets
	cmd := session.Cmd
	for _, c := range columns {
		mysqlc := c.(Column)
		session.Mrs.AddColumn(mysqlc)
		//	mysql COM_QUERY response: send the column definition per column
		err := protocol.SendColumnDefinitionPacket(mysqlc, cmd)
		if err != nil {
			return err
		}
	}

	//	mysql COM_QUERY response: End after the column has been sent.
	//	send EOF packet
	err = protocol.SendEOFPacketIf(0, 0)
	if err != nil {
		return err
	}

	err = buildMoExplainQuery(explainColName, buffer, session, getDataFromPipeline)
	if err != nil {
		return err
	}

	err = protocol.sendEOFOrOkPacket(0, 0)
	if err != nil {
		return err
	}
	return nil
}

// handlePrepareStmt
func (mce *MysqlCmdExecutor) handlePrepareStmt(st *tree.PrepareStmt) (*PrepareStmt, error) {
	switch st.Stmt.(type) {
	case *tree.Update:
		mce.ses.GetTxnCompileCtx().SetQueryType(TXN_UPDATE)
	case *tree.Delete:
		mce.ses.GetTxnCompileCtx().SetQueryType(TXN_DELETE)
	}
	preparePlan, err := buildPlan(mce.ses.txnCompileCtx, st)
	if err != nil {
		return nil, err
	}

	prepareStmt := &PrepareStmt{
		Name:        preparePlan.GetDcl().GetPrepare().GetName(),
		PreparePlan: preparePlan,
		PrepareStmt: st.Stmt,
	}

	err = mce.ses.SetPrepareStmt(preparePlan.GetDcl().GetPrepare().GetName(), prepareStmt)
	return prepareStmt, err
}

// handlePrepareString
func (mce *MysqlCmdExecutor) handlePrepareString(st *tree.PrepareString) (*PrepareStmt, error) {
	stmts, err := mysql.Parse(st.Sql)
	if err != nil {
		return nil, err
	}
	switch stmts[0].(type) {
	case *tree.Update:
		mce.ses.GetTxnCompileCtx().SetQueryType(TXN_UPDATE)
	case *tree.Delete:
		mce.ses.GetTxnCompileCtx().SetQueryType(TXN_DELETE)
	}

	preparePlan, err := buildPlan(mce.ses.txnCompileCtx, st)
	if err != nil {
		return nil, err
	}

	prepareStmt := &PrepareStmt{
		Name:        preparePlan.GetDcl().GetPrepare().GetName(),
		PreparePlan: preparePlan,
		PrepareStmt: stmts[0],
	}

	err = mce.ses.SetPrepareStmt(preparePlan.GetDcl().GetPrepare().GetName(), prepareStmt)
	return prepareStmt, err
}

// handleDeallocate
func (mce *MysqlCmdExecutor) handleDeallocate(st *tree.Deallocate) error {
	deallocatePlan, err := buildPlan(mce.ses.txnCompileCtx, st)
	if err != nil {
		return err
	}
	mce.ses.RemovePrepareStmt(deallocatePlan.GetDcl().GetDeallocate().GetName())
	return nil
}

// handleCreateAccount creates a new user-level tenant in the context of the tenant SYS
// which has been initialized.
func (mce *MysqlCmdExecutor) handleCreateAccount(ctx context.Context, ca *tree.CreateAccount) error {
	ses := mce.GetSession()
	tenant := ses.GetTenantInfo()

	//step1 : create new account.
	return InitGeneralTenant(ctx, tenant, ca)
}

// handleCreateUser creates the user for the tenant
func (mce *MysqlCmdExecutor) handleCreateUser(ctx context.Context, cu *tree.CreateUser) error {
	ses := mce.GetSession()
	tenant := ses.GetTenantInfo()

	//step1 : create the user
	return InitUser(ctx, tenant, cu)
}

// handleCreateRole creates the new role
func (mce *MysqlCmdExecutor) handleCreateRole(ctx context.Context, cr *tree.CreateRole) error {
	ses := mce.GetSession()
	tenant := ses.GetTenantInfo()

	//step1 : create the role
	return InitRole(ctx, tenant, cr)
}

func GetExplainColumns(explainColName string) ([]interface{}, error) {
	cols := []*plan2.ColDef{
		{Typ: &plan2.Type{Id: int32(types.T_varchar)}, Name: explainColName},
	}
	columns := make([]interface{}, len(cols))
	var err error = nil
	for i, col := range cols {
		c := new(MysqlColumn)
		c.SetName(col.Name)
		err = convertEngineTypeToMysqlType(types.T(col.Typ.Id), c)
		if err != nil {
			return nil, err
		}
		columns[i] = c
	}
	return columns, err
}

func buildMoExplainQuery(explainColName string, buffer *explain.ExplainDataBuffer, session *Session, fill func(interface{}, *batch.Batch) error) error {
	bat := batch.New(true, []string{explainColName})
	rs := buffer.Lines
	vs := make([][]byte, len(rs))

	count := 0
	for _, r := range rs {
		str := []byte(r)
		vs[count] = str
		count++
	}
	vs = vs[:count]
	vec := vector.New(types.T_varchar.ToType())
	if err := vector.Append(vec, vs); err != nil {
		return err
	}
	bat.Vecs[0] = vec
	bat.InitZsOne(count)

	return fill(session, bat)
}

var _ ComputationWrapper = &TxnComputationWrapper{}

type TxnComputationWrapper struct {
	stmt    tree.Statement
	plan    *plan2.Plan
	proc    *process.Process
	ses     *Session
	compile *compile.Compile

	uuid uuid.UUID
}

func InitTxnComputationWrapper(ses *Session, stmt tree.Statement, proc *process.Process) *TxnComputationWrapper {
	uuid, _ := uuid.NewUUID()
	return &TxnComputationWrapper{
		stmt: stmt,
		proc: proc,
		ses:  ses,
		uuid: uuid,
	}
}

func (cwft *TxnComputationWrapper) GetAst() tree.Statement {
	return cwft.stmt
}

func (cwft *TxnComputationWrapper) SetDatabaseName(db string) error {
	return nil
}

func (cwft *TxnComputationWrapper) GetColumns() ([]interface{}, error) {
	var err error
	cols := plan2.GetResultColumnsFromPlan(cwft.plan)
	switch cwft.GetAst().(type) {
	case *tree.ShowColumns:
		cols = []*plan2.ColDef{
			{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Field"},
			{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Type"},
			{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Null"},
			{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Key"},
			{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Default"},
			{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Comment"},
		}
	}
	columns := make([]interface{}, len(cols))
	for i, col := range cols {
		c := new(MysqlColumn)
		c.SetName(col.Name)
		err = convertEngineTypeToMysqlType(types.T(col.Typ.Id), c)
		if err != nil {
			return nil, err
		}
		columns[i] = c
	}
	return columns, err
}

func (cwft *TxnComputationWrapper) GetAffectedRows() uint64 {
	return cwft.compile.GetAffectedRows()
}

func (cwft *TxnComputationWrapper) Compile(requestCtx context.Context, u interface{}, fill func(interface{}, *batch.Batch) error) (interface{}, error) {
	var err error
	cwft.plan, err = buildPlan(cwft.ses.GetTxnCompilerContext(), cwft.stmt)
	if err != nil {
		return nil, err
	}

	if _, ok := cwft.stmt.(*tree.Execute); ok {
		executePlan := cwft.plan.GetDcl().GetExecute()
		stmtName := executePlan.GetName()
		prepareStmt, err := cwft.ses.GetPrepareStmt(stmtName)
		if err != nil {
			return nil, err
		}

		// TODO check if schema change, obj.Obj is zero all the time in 0.6
		// for _, obj := range preparePlan.GetSchemas() {
		// 	newObj, _ := cwft.ses.txnCompileCtx.Resolve(obj.SchemaName, obj.ObjName)
		// 	if newObj == nil || newObj.Obj != obj.Obj {
		// 		return nil, errors.New("", fmt.Sprintf("table '%s' has been changed, please reset prepare statement '%s'", obj.ObjName, stmtName))
		// 	}
		// }

		newPlan := plan2.DeepCopyPlan(prepareStmt.PreparePlan.GetDcl().GetPrepare().Plan)

		// replace ? and @var with their values
		resetParamRule := plan2.NewResetParamRefRule(executePlan.Args)
		resetVarRule := plan2.NewResetVarRefRule(cwft.ses.GetTxnCompilerContext())
		vp := plan2.NewVisitPlan(newPlan, []plan2.VisitPlanRule{resetParamRule, resetVarRule})
		err = vp.Visit()
		if err != nil {
			return nil, err
		}

		// reset plan & stmt
		cwft.stmt = prepareStmt.PrepareStmt
		cwft.plan = newPlan
	} else {
		// replace @var with their values
		resetVarRule := plan2.NewResetVarRefRule(cwft.ses.GetTxnCompilerContext())
		vp := plan2.NewVisitPlan(cwft.plan, []plan2.VisitPlanRule{resetVarRule})
		err = vp.Visit()
		if err != nil {
			return nil, err
		}
	}

	cwft.proc.UnixTime = time.Now().UnixNano()
	txnHandler := cwft.ses.GetTxnHandler()
	cwft.proc.TxnOperator = txnHandler.GetTxn()
	cwft.compile = compile.New(cwft.ses.GetDatabaseName(), cwft.ses.GetSql(), cwft.ses.GetUserName(), requestCtx, cwft.ses.GetStorage(), cwft.proc, cwft.stmt)
	err = cwft.compile.Compile(cwft.plan, cwft.ses, fill)
	if err != nil {
		return nil, err
	}
	return cwft.compile, err
}

func (cwft *TxnComputationWrapper) GetUUID() []byte {
	return cwft.uuid[:]
}

func (cwft *TxnComputationWrapper) Run(ts uint64) error {
	return nil
}

func buildPlan(ctx plan2.CompilerContext, stmt tree.Statement) (*plan2.Plan, error) {
	if s, ok := stmt.(*tree.Insert); ok {
		if _, ok := s.Rows.Select.(*tree.ValuesClause); ok {
			return plan2.BuildPlan(ctx, stmt)
		}
	}
	switch stmt := stmt.(type) {
	case *tree.Select, *tree.ParenSelect,
		*tree.Update, *tree.Delete, *tree.Insert,
		*tree.ShowDatabases, *tree.ShowTables, *tree.ShowColumns,
		*tree.ShowCreateDatabase, *tree.ShowCreateTable:
		opt := plan2.NewBaseOptimizer(ctx)
		optimized, err := opt.Optimize(stmt)
		if err != nil {
			return nil, err
		}
		return &plan2.Plan{
			Plan: &plan2.Plan_Query{
				Query: optimized,
			},
		}, nil
	default:
		return plan2.BuildPlan(ctx, stmt)
	}
}

/*
GetComputationWrapper gets the execs from the computation engine
*/
var GetComputationWrapper = func(db, sql, user string, eng engine.Engine, proc *process.Process, ses *Session) ([]ComputationWrapper, error) {
	var cw []ComputationWrapper = nil
	var stmts []tree.Statement = nil
	var cmdFieldStmt *InternalCmdFieldList
	var err error
	if isCmdFieldListSql(sql) {
		cmdFieldStmt, err = parseCmdFieldList(sql)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, cmdFieldStmt)
	} else {
		stmts, err = parsers.Parse(dialect.MYSQL, sql)
		if err != nil {
			return nil, err
		}
	}

	for _, stmt := range stmts {
		cw = append(cw, InitTxnComputationWrapper(ses, stmt, proc))
	}
	return cw, nil
}

func incStatementCounter(stmt tree.Statement, isInternal bool) {
	switch stmt.(type) {
	case *tree.Select:
		metric.StatementCounter(metric.SQLTypeSelect, isInternal).Inc()
	case *tree.Insert:
		metric.StatementCounter(metric.SQLTypeInsert, isInternal).Inc()
	case *tree.Delete:
		metric.StatementCounter(metric.SQLTypeDelete, isInternal).Inc()
	case *tree.Update:
		metric.StatementCounter(metric.SQLTypeUpdate, isInternal).Inc()
	default:
		metric.StatementCounter(metric.SQLTypeOther, isInternal).Inc()
	}
}

func remindrecordSQLLentencyObserver(stmt tree.Statement, isInternal bool, value float64) {
	switch stmt.(type) {
	case *tree.Select:
		metric.SQLLatencyObserver(metric.SQLTypeSelect, isInternal).Observe(value)
	case *tree.Insert:
		metric.SQLLatencyObserver(metric.SQLTypeInsert, isInternal).Observe(value)
	case *tree.Delete:
		metric.SQLLatencyObserver(metric.SQLTypeDelete, isInternal).Observe(value)
	case *tree.Update:
		metric.SQLLatencyObserver(metric.SQLTypeUpdate, isInternal).Observe(value)
	default:
		metric.SQLLatencyObserver(metric.SQLTypeOther, isInternal).Observe(value)
	}
}

func (mce *MysqlCmdExecutor) beforeRun(stmt tree.Statement) {
	sess := mce.GetSession()
	incStatementCounter(stmt, sess.IsInternal)
}

func (mce *MysqlCmdExecutor) afterRun(stmt tree.Statement, beginInstant time.Time) {
	// TODO: this latency doesn't consider complile and build stage, fix it!
	latency := time.Since(beginInstant).Seconds()
	sess := mce.GetSession()
	remindrecordSQLLentencyObserver(stmt, sess.IsInternal, latency)

}

// execute query
func (mce *MysqlCmdExecutor) doComQuery(requestCtx context.Context, sql string) (retErr error) {
	beginInstant := time.Now()
	ses := mce.GetSession()
	ses.showStmtType = NotShowStatement
	proto := ses.GetMysqlProtocol()
	ses.SetSql(sql)
	ses.ep.Outfile = false

	proc := process.New(mheap.New(ses.GuestMmu))
	proc.Id = mce.getNextProcessId()
	proc.Lim.Size = ses.Pu.SV.ProcessLimitationSize
	proc.Lim.BatchRows = ses.Pu.SV.ProcessLimitationBatchRows
	proc.Lim.PartitionRows = ses.Pu.SV.ProcessLimitationPartitionRows
	proc.SessionInfo = process.SessionInfo{
		User:         ses.GetUserName(),
		Host:         ses.Pu.SV.Host,
		ConnectionID: uint64(proto.ConnectionID()),
		Database:     ses.GetDatabaseName(),
		Version:      serverVersion,
		TimeZone:     ses.timeZone,
	}

	cws, err := GetComputationWrapper(ses.GetDatabaseName(),
		sql,
		ses.GetUserName(),
		ses.Pu.StorageEngine,
		proc, ses)
	if err != nil {
		return NewMysqlError(ER_PARSE_ERROR, err, "")
	}

	defer func() {
		ses.SetMysqlResultSet(nil)
	}()

	var cmpBegin time.Time
	var ret interface{}
	var runner ComputationRunner
	var selfHandle bool
	var fromLoadData = false
	var txnErr error
	var rspLen uint64
	var prepareStmt *PrepareStmt

	stmt := cws[0].GetAst()
	mce.beforeRun(stmt)
	defer mce.afterRun(stmt, beginInstant)
	for _, cw := range cws {
		ses.SetMysqlResultSet(&MysqlResultSet{})
		stmt := cw.GetAst()
		ctx := mce.RecordStatement(requestCtx, ses, proc, cw, beginInstant)

		/*
				if it is in an active or multi-statement transaction, we check the type of the statement.
				Then we decide that if we can execute the statement.

			If we check the active transaction, it will generate the case below.
			case:
			set autocommit = 0;  <- no active transaction
			                     <- no active transaction
			drop table test1;    <- no active transaction, no error
			                     <- has active transaction
			drop table test1;    <- has active transaction, error
			                     <- has active transaction
		*/
		if ses.InActiveTransaction() {
			can := StatementCanBeExecutedInUncommittedTransaction(stmt)
			if !can {
				//is ddl statement
				if IsDDL(stmt) {
					return errorOnlyCreateStatement
				} else if IsAdministrativeStatement(stmt) {
					return errorAdministrativeStatement
				} else if IsParameterModificationStatement(stmt) {
					return errorParameterModificationInTxn
				} else {
					return errorUnclassifiedStatement
				}
			}
		}

		//check transaction states
		switch stmt.(type) {
		case *tree.BeginTransaction:
			err = ses.TxnBegin()
			if err != nil {
				goto handleFailed
			}
		case *tree.CommitTransaction:
			err = ses.TxnCommit()
			if err != nil {
				goto handleFailed
			}
		case *tree.RollbackTransaction:
			err = ses.TxnRollback()
			if err != nil {
				goto handleFailed
			}
		}

		switch st := stmt.(type) {
		case *tree.Select:
			if st.Ep != nil {
				ses.ep = st.Ep
			}
		}

		selfHandle = false
		ses.GetTxnCompileCtx().SetQueryType(TXN_DEFAULT)

		switch st := stmt.(type) {
		case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction:
			selfHandle = true
			err = proto.sendOKPacket(0, 0, 0, 0, "")
			if err != nil {
				goto handleFailed
			}
		case *tree.Use:
			selfHandle = true
			err = mce.handleChangeDB(requestCtx, st.Name)
			if err != nil {
				goto handleFailed
			}
			err = proto.sendOKPacket(0, 0, 0, 0, "")
			if err != nil {
				goto handleFailed
			}
		case *tree.DropDatabase:
			// if the droped database is the same as the one in use, database must be reseted to empty.
			if string(st.Name) == ses.GetDatabaseName() {
				ses.SetUserName("")
			}
		/*case *tree.Load:
		fromLoadData = true
		selfHandle = true
		err = mce.handleLoadData(requestCtx, st)
		if err != nil {
			goto handleFailed
		}*/
		case *tree.PrepareStmt:
			selfHandle = true
			prepareStmt, err = mce.handlePrepareStmt(st)
			if err != nil {
				goto handleFailed
			}
		case *tree.PrepareString:
			selfHandle = true
			prepareStmt, err = mce.handlePrepareString(st)
			if err != nil {
				goto handleFailed
			}
		case *tree.Deallocate:
			selfHandle = true
			err = mce.handleDeallocate(st)
			deallocatePlan, err := buildPlan(mce.ses.txnCompileCtx, st)
			if err != nil {
				goto handleFailed
			}
			mce.ses.RemovePrepareStmt(deallocatePlan.GetDcl().GetDeallocate().GetName())
		case *tree.SetVar:
			selfHandle = true
			err = mce.handleSetVar(st)
			if err != nil {
				goto handleFailed
			}
		case *tree.ShowVariables:
			selfHandle = true
			err = mce.handleShowVariables(st)
			if err != nil {
				goto handleFailed
			}
		case *tree.AnalyzeStmt:
			selfHandle = true
			if err = mce.handleAnalyzeStmt(requestCtx, st); err != nil {
				goto handleFailed
			}
		case *tree.ExplainStmt:
			selfHandle = true
			if err = mce.handleExplainStmt(st); err != nil {
				goto handleFailed
			}
		case *tree.ExplainAnalyze:
			err = errors.New(errno.FeatureNotSupported, "not support explain analyze statement now")
			goto handleFailed
		case *tree.ShowColumns:
			ses.showStmtType = ShowColumns
			ses.Data = nil
		case *tree.Delete:
			ses.GetTxnCompileCtx().SetQueryType(TXN_DELETE)
		case *tree.Update:
			ses.GetTxnCompileCtx().SetQueryType(TXN_UPDATE)
		case *InternalCmdFieldList:
			selfHandle = true
			if err = mce.handleCmdFieldList(requestCtx, st); err != nil {
				goto handleFailed
			}
		case *tree.CreateAccount:
			selfHandle = true
			if err = mce.handleCreateAccount(requestCtx, st); err != nil {
				goto handleFailed
			}
		case *tree.CreateUser:
			selfHandle = true
			if err = mce.handleCreateUser(requestCtx, st); err != nil {
				goto handleFailed
			}
		case *tree.CreateRole:
			selfHandle = true
			if err = mce.handleCreateRole(requestCtx, st); err != nil {
				goto handleFailed
			}
		}

		if selfHandle {
			goto handleSucceeded
		}
		if err = cw.SetDatabaseName(ses.GetDatabaseName()); err != nil {
			goto handleFailed
		}

		cmpBegin = time.Now()

		if ret, err = cw.Compile(requestCtx, ses, ses.outputCallback); err != nil {
			goto handleFailed
		}
		stmt = cw.GetAst()

		runner = ret.(ComputationRunner)
		if !ses.Pu.SV.DisableRecordTimeElapsedOfSqlRequest {
			logutil2.Infof(ctx, "time of Exec.Build : %s", time.Since(cmpBegin).String())
		}

		// cw.Compile might rewrite sql, here we fetch the latest version
		switch cw.GetAst().(type) {
		//produce result set
		case *tree.Select,
			*tree.ShowCreateTable, *tree.ShowCreateDatabase, *tree.ShowTables, *tree.ShowDatabases, *tree.ShowColumns,
			*tree.ShowProcessList, *tree.ShowErrors, *tree.ShowWarnings, *tree.ShowVariables, *tree.ShowStatus,
			*tree.ShowIndex, *tree.ShowCreateView,
			*tree.ExplainFor, *tree.ExplainAnalyze, *tree.ExplainStmt:
			columns, err2 := cw.GetColumns()
			if err2 != nil {
				logutil.Errorf("GetColumns from Computation handler failed. error: %v", err2)
				err = err2
				goto handleFailed
			}
			/*
				Step 1 : send column count and column definition.
			*/
			//send column count
			colCnt := uint64(len(columns))
			err = proto.SendColumnCountPacket(colCnt)
			if err != nil {
				goto handleFailed
			}
			//send columns
			//column_count * Protocol::ColumnDefinition packets
			cmd := ses.Cmd
			for _, c := range columns {
				mysqlc := c.(Column)
				ses.Mrs.AddColumn(mysqlc)

				//logutil.Infof("doComQuery col name %v type %v ",col.Name(),col.ColumnType())
				/*
					mysql COM_QUERY response: send the column definition per column
				*/
				err = proto.SendColumnDefinitionPacket(mysqlc, cmd)
				if err != nil {
					goto handleFailed
				}
			}

			/*
				mysql COM_QUERY response: End after the column has been sent.
				send EOF packet
			*/
			err = proto.SendEOFPacketIf(0, 0)
			if err != nil {
				goto handleFailed
			}

			runBegin := time.Now()
			/*
				Step 2: Start pipeline
				Producing the data row and sending the data row
			*/
			if ses.ep.Outfile {
				ses.ep.DefaultBufSize = ses.Pu.SV.ExportDataDefaultFlushSize
				initExportFileParam(ses.ep, ses.Mrs)
				if err = openNewFile(ses.ep, ses.Mrs); err != nil {
					goto handleFailed
				}
			}
			if err = runner.Run(0); err != nil {
				goto handleFailed
			}
			if ses.showStmtType == ShowColumns {
				if err = handleShowColumns(ses); err != nil {
					goto handleFailed
				}
			}

			if ses.ep.Outfile {
				if err = ses.ep.Writer.Flush(); err != nil {
					goto handleFailed
				}
				if err = ses.ep.File.Close(); err != nil {
					goto handleFailed
				}
			}

			if !ses.Pu.SV.DisableRecordTimeElapsedOfSqlRequest {
				logutil.Infof("time of Exec.Run : %s", time.Since(runBegin).String())
			}
			/*
				Step 3: Say goodbye
				mysql COM_QUERY response: End after the data row has been sent.
				After all row data has been sent, it sends the EOF or OK packet.
			*/
			err = proto.sendEOFOrOkPacket(0, 0)
			if err != nil {
				goto handleFailed
			}
		//just status, no result set
		case *tree.CreateTable, *tree.DropTable, *tree.CreateDatabase, *tree.DropDatabase,
			*tree.CreateIndex, *tree.DropIndex,
			*tree.CreateView, *tree.DropView,
			*tree.Insert, *tree.Update,
			*tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction,
			*tree.SetVar,
			*tree.Load,
			*tree.CreateUser, *tree.DropUser, *tree.AlterUser,
			*tree.CreateRole, *tree.DropRole,
			*tree.Revoke, *tree.Grant,
			*tree.SetDefaultRole, *tree.SetRole, *tree.SetPassword,
			*tree.Delete:
			runBegin := time.Now()

			/*
				Step 1: Start
			*/
			if err = runner.Run(0); err != nil {
				goto handleFailed
			}

			if !ses.Pu.SV.DisableRecordTimeElapsedOfSqlRequest {
				logutil.Infof("time of Exec.Run : %s", time.Since(runBegin).String())
			}

			rspLen = cw.GetAffectedRows()
			echoTime := time.Now()
			if !ses.Pu.SV.DisableRecordTimeElapsedOfSqlRequest {
				logutil.Infof("time of SendResponse %s", time.Since(echoTime).String())
			}
		}
	handleSucceeded:
		//load data handle txn failure internally
		if !fromLoadData {
			txnErr = ses.TxnCommitSingleStatement(stmt)
			if txnErr != nil {
				return txnErr
			}
			switch stmt.(type) {
			case *tree.CreateTable, *tree.DropTable, *tree.CreateDatabase, *tree.DropDatabase,
				*tree.CreateIndex, *tree.DropIndex, *tree.Insert, *tree.Update,
				*tree.CreateView, *tree.DropView, *tree.Load,
				*tree.CreateAccount, *tree.DropAccount, *tree.AlterAccount,
				*tree.CreateUser, *tree.DropUser, *tree.AlterUser,
				*tree.CreateRole, *tree.DropRole, *tree.Revoke, *tree.Grant,
				*tree.SetDefaultRole, *tree.SetRole, *tree.SetPassword, *tree.Delete,
				*tree.Deallocate:
				resp := NewOkResponse(rspLen, 0, 0, 0, int(COM_QUERY), "")
				if err := mce.GetSession().protocol.SendResponse(resp); err != nil {
					return fmt.Errorf("routine send response failed. error:%v ", err)
				}

			case *tree.PrepareStmt, *tree.PrepareString:
				if mce.ses.Cmd == int(COM_STMT_PREPARE) {
					if err := mce.ses.protocol.SendPrepareResponse(prepareStmt); err != nil {
						return fmt.Errorf("routine send response failed. error:%v ", err)
					}
				} else {
					resp := NewOkResponse(rspLen, 0, 0, 0, int(COM_QUERY), "")
					if err := mce.GetSession().protocol.SendResponse(resp); err != nil {
						return fmt.Errorf("routine send response failed. error:%v ", err)
					}
				}
			}
		}
		goto handleNext
	handleFailed:
		if !fromLoadData {
			txnErr = ses.TxnRollbackSingleStatement(stmt)
			if txnErr != nil {
				return txnErr
			}
		}
		return err
	handleNext:
	} // end of for

	return nil
}

// ExecRequest the server execute the commands from the client following the mysql's routine
func (mce *MysqlCmdExecutor) ExecRequest(requestCtx context.Context, req *Request) (resp *Response, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = moerr.NewPanicError(e)
			resp = NewGeneralErrorResponse(COM_QUERY, err)
		}
	}()

	logutil.Infof("cmd %v", req.GetCmd())

	ses := mce.GetSession()
	switch uint8(req.GetCmd()) {
	case COM_QUIT:
		/*resp = NewResponse(
			OkResponse,
			0,
			int(COM_QUIT),
			nil,
		)*/
		return resp, nil
	case COM_QUERY:
		var query = string(req.GetData().([]byte))
		mce.addSqlCount(1)
		logutil.Infof("connection id %d query:%s", ses.GetConnectionID(), SubStringFromBegin(query, int(ses.Pu.SV.LengthOfQueryPrinted)))
		seps := strings.Split(query, " ")
		if len(seps) <= 0 {
			resp = NewGeneralErrorResponse(COM_QUERY, fmt.Errorf("invalid query"))
			return resp, nil
		}

		if strings.ToLower(seps[0]) == "kill" {
			//last one is processID
			/*
				The 'kill query xxx' is processed in an independent connection.
				When a 'Ctrl+C' is received from the user in mysql client shell,
				an independent connection is established and the 'kill query xxx'
				is sent to the server. The server cancels the 'query xxx' after it
				receives the 'kill query xxx'. The server responses the OK.
				Then, the client quit this connection.
			*/
			procIdStr := seps[len(seps)-1]
			procID, err := strconv.ParseUint(procIdStr, 10, 64)
			if err != nil {
				resp = NewGeneralErrorResponse(COM_QUERY, err)
				return resp, nil
			}
			err = mce.GetRoutineManager().killStatement(procID)
			if err != nil {
				resp = NewGeneralErrorResponse(COM_QUERY, err)
				return resp, err
			}
			resp = NewGeneralOkResponse(COM_QUERY)
			return resp, nil
		}

		err := mce.doComQuery(requestCtx, query)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_QUERY, err)
		}
		return resp, nil
	case COM_INIT_DB:
		var dbname = string(req.GetData().([]byte))
		mce.addSqlCount(1)
		query := "use `" + dbname + "`"
		err := mce.doComQuery(requestCtx, query)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_INIT_DB, err)
		}

		return resp, nil
	case COM_FIELD_LIST:
		var payload = string(req.GetData().([]byte))
		mce.addSqlCount(1)
		query := makeCmdFieldListSql(payload)
		err := mce.doComQuery(requestCtx, query)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_FIELD_LIST, err)
		}

		return resp, nil
	case COM_PING:
		resp = NewGeneralOkResponse(COM_PING)

		return resp, nil

	case COM_STMT_PREPARE:
		mce.ses.Cmd = int(COM_STMT_PREPARE)
		sql := string(req.GetData().([]byte))
		mce.addSqlCount(1)

		// rewrite to "prepare stmt_name from 'xxx'"
		newLastStmtID := mce.ses.GenNewStmtId()
		newStmtName := getPrepareStmtName(newLastStmtID)
		sql = fmt.Sprintf("prepare %s from %s", newStmtName, sql)

		err := mce.doComQuery(requestCtx, sql)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_PREPARE, err)
		}
		return resp, nil

	case COM_STMT_EXECUTE:
		data := req.GetData().([]byte)
		sql, err := mce.parseStmtExecute(data)
		if err != nil {
			return NewGeneralErrorResponse(COM_STMT_PREPARE, err), nil
		}
		err = mce.doComQuery(requestCtx, sql)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_PREPARE, err)
		}
		return resp, nil

	case COM_STMT_CLOSE:
		data := req.GetData().([]byte)

		// rewrite to "deallocate prepare stmt_name"
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		stmtName := getPrepareStmtName(stmtID)
		sql := fmt.Sprintf("deallocate prepare %s", stmtName)

		err := mce.doComQuery(requestCtx, sql)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_CLOSE, err)
		}
		return resp, nil

	default:
		err := fmt.Errorf("unsupported command. 0x%x", req.GetCmd())
		resp = NewGeneralErrorResponse(uint8(req.GetCmd()), err)
	}
	return resp, nil
}

func (mce *MysqlCmdExecutor) parseStmtExecute(data []byte) (string, error) {
	// see https://dev.mysql.com/doc/internals/en/com-stmt-execute.html
	pos := 0
	if len(data) < 4 {
		return "", moerr.NewError(moerr.INVALID_INPUT, "malform packet")
	}
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmtName := fmt.Sprintf("%s_%d", prefixPrepareStmtName, stmtID)
	preStmt, err := mce.ses.GetPrepareStmt(stmtName)
	if err != nil {
		return "", err
	}
	names, vars, err := mce.ses.GetMysqlProtocol().ParseExecuteData(preStmt, data, pos)
	if err != nil {
		return "", err
	}
	sql := fmt.Sprintf("execute %s", stmtName)
	if len(names) > 0 {
		sql = sql + fmt.Sprintf(" using @%s", strings.Join(names, ",@"))
		for i := 0; i < len(names); i++ {
			err := mce.ses.SetUserDefinedVar(names[i], vars[i])
			if err != nil {
				return "", err
			}
		}
	}
	return sql, nil
}

func (mce *MysqlCmdExecutor) setCancelRequestFunc(cancelFunc context.CancelFunc) {
	mce.cancelRequestFunc = cancelFunc
}

func (mce *MysqlCmdExecutor) getCancelRequestFunc() context.CancelFunc {
	return mce.cancelRequestFunc
}

func (mce *MysqlCmdExecutor) Close() {
	cancelRequestFunc := mce.getCancelRequestFunc()
	if cancelRequestFunc != nil {
		cancelRequestFunc()
	}

	logutil.Info("----close mce")
	ses := mce.GetSession()
	if ses != nil {
		err := ses.TxnRollback()
		if err != nil {
			logutil.Errorf("rollback txn in mce.Close failed.error:%v", err)
		}
	}
}

/*
StatementCanBeExecutedInUncommittedTransaction checks the statement can be executed in an active transaction.
*/
func StatementCanBeExecutedInUncommittedTransaction(stmt tree.Statement) bool {
	switch st := stmt.(type) {
	//ddl statement
	case *tree.CreateTable, *tree.CreateDatabase, *tree.CreateIndex, *tree.CreateView:
		return true
		//dml statement
	case *tree.Insert, *tree.Update, *tree.Delete, *tree.Select, *tree.Load:
		return true
		//transaction
	case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction:
		return true
		//show
	case *tree.ShowTables, *tree.ShowCreateTable, *tree.ShowCreateDatabase, *tree.ShowDatabases,
		*tree.ShowVariables, *tree.ShowColumns, *tree.ShowErrors, *tree.ShowIndex, *tree.ShowProcessList,
		*tree.ShowStatus, *tree.ShowTarget, *tree.ShowWarnings:
		return true
		//others
	case *tree.PrepareStmt, *tree.Execute, *tree.Deallocate,
		*tree.ExplainStmt, *tree.ExplainAnalyze, *tree.ExplainFor, *InternalCmdFieldList:
		return true
	case *tree.Use:
		/*
			These statements can not be executed in an uncommitted transaction:
				USE SECONDARY ROLE { ALL | NONE }
				USE ROLE role;
		*/
		return !st.IsUseRole()
	}

	return false
}

// IsDDL checks the statement is the DDL statement.
func IsDDL(stmt tree.Statement) bool {
	switch stmt.(type) {
	case *tree.CreateTable, *tree.DropTable,
		*tree.CreateView, *tree.DropView,
		*tree.CreateDatabase, *tree.DropDatabase,
		*tree.CreateIndex, *tree.DropIndex:
		return true
	}
	return false
}

// IsDropStatement checks the statement is the drop statement.
func IsDropStatement(stmt tree.Statement) bool {
	switch stmt.(type) {
	case *tree.DropDatabase, *tree.DropTable, *tree.DropView, *tree.DropIndex:
		return true
	}
	return false
}

// IsAdministrativeStatement checks the statement is the administrative statement.
func IsAdministrativeStatement(stmt tree.Statement) bool {
	switch st := stmt.(type) {
	case *tree.CreateAccount, *tree.DropAccount, *tree.AlterAccount,
		*tree.CreateUser, *tree.DropUser, *tree.AlterUser,
		*tree.CreateRole, *tree.DropRole,
		*tree.Revoke, *tree.Grant,
		*tree.SetDefaultRole, *tree.SetRole, *tree.SetPassword:
		return true
	case *tree.Use:
		return st.IsUseRole()
	}
	return false
}

// IsParameterModificationStatement checks the statement is the statement of parameter modification statement.
func IsParameterModificationStatement(stmt tree.Statement) bool {
	switch stmt.(type) {
	case *tree.SetVar:
		return true
	}
	return false
}

/*
IsStatementToBeCommittedInActiveTransaction checks the statement that need to be committed
in an active transaction.

Currently, it includes the drop statement, the administration statement ,

	the parameter modification statement.
*/
func IsStatementToBeCommittedInActiveTransaction(stmt tree.Statement) bool {
	if stmt == nil {
		return false
	}
	return IsDropStatement(stmt) || IsAdministrativeStatement(stmt) || IsParameterModificationStatement(stmt)
}

func NewMysqlCmdExecutor() *MysqlCmdExecutor {
	return &MysqlCmdExecutor{}
}

/*
convert the type in computation engine to the type in mysql.
*/
func convertEngineTypeToMysqlType(engineType types.T, col *MysqlColumn) error {
	switch engineType {
	case types.T_any:
		col.SetColumnType(defines.MYSQL_TYPE_NULL)
	case types.T_json:
		col.SetColumnType(defines.MYSQL_TYPE_JSON)
	case types.T_bool:
		col.SetColumnType(defines.MYSQL_TYPE_BOOL)
	case types.T_int8:
		col.SetColumnType(defines.MYSQL_TYPE_TINY)
	case types.T_uint8:
		col.SetColumnType(defines.MYSQL_TYPE_TINY)
		col.SetSigned(false)
	case types.T_int16:
		col.SetColumnType(defines.MYSQL_TYPE_SHORT)
	case types.T_uint16:
		col.SetColumnType(defines.MYSQL_TYPE_SHORT)
		col.SetSigned(false)
	case types.T_int32:
		col.SetColumnType(defines.MYSQL_TYPE_LONG)
	case types.T_uint32:
		col.SetColumnType(defines.MYSQL_TYPE_LONG)
		col.SetSigned(false)
	case types.T_int64:
		col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	case types.T_uint64:
		col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
		col.SetSigned(false)
	case types.T_float32:
		col.SetColumnType(defines.MYSQL_TYPE_FLOAT)
	case types.T_float64:
		col.SetColumnType(defines.MYSQL_TYPE_DOUBLE)
	case types.T_char:
		col.SetColumnType(defines.MYSQL_TYPE_STRING)
	case types.T_varchar:
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	case types.T_date:
		col.SetColumnType(defines.MYSQL_TYPE_DATE)
	case types.T_datetime:
		col.SetColumnType(defines.MYSQL_TYPE_DATETIME)
	case types.T_timestamp:
		col.SetColumnType(defines.MYSQL_TYPE_TIMESTAMP)
	case types.T_decimal64:
		col.SetColumnType(defines.MYSQL_TYPE_DECIMAL)
	case types.T_decimal128:
		col.SetColumnType(defines.MYSQL_TYPE_DECIMAL)
	case types.T_blob:
		col.SetColumnType(defines.MYSQL_TYPE_BLOB)
	default:
		return fmt.Errorf("RunWhileSend : unsupported type %d", engineType)
	}
	return nil
}
