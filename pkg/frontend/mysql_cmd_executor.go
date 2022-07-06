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
	goErrors "errors"
	"fmt"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/errno"
	plan3 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/compile2"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2/explain"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	compile1 "github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	errorDatabaseIsNull                            = goErrors.New("the database name is an empty string")
	errorNoSuchGlobalSystemVariable                = goErrors.New("there is no such global system variable")
	errorComplicateExprIsNotSupported              = goErrors.New("the complicate expression is not supported")
	errorNumericTypeIsNotSupported                 = goErrors.New("the numeric type is not supported")
	errorUnaryMinusForNonNumericTypeIsNotSupported = goErrors.New("unary minus for no numeric type is not supported")
	errorFunctionIsNotSupported                    = goErrors.New("function is not supported")
)

//tableInfos of a database
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

	//for load data closing
	loadDataClose *CloseLoadData

	//for export data closing
	exportDataClose *CloseExportData

	ses *Session

	routineMgr *RoutineManager
}

func (mce *MysqlCmdExecutor) PrepareSessionBeforeExecRequest(ses *Session) {
	mce.ses = ses
}

func (mce *MysqlCmdExecutor) GetSession() *Session {
	return mce.ses
}

//get new process id
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

func (oq *outputQueue) ResetLineStr() {
	oq.lineStr = oq.lineStr[:0]
}

func NewOuputQueue(proto MysqlProtocol, mrs *MysqlResultSet, length uint64, ep *tree.ExportParam, showStatementType ShowStatementType) *outputQueue {
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
		if o.showStmtType == ShowCreateTable || o.showStmtType == ShowCreateDatabase || o.showStmtType == ShowColumns {
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

const (
	tableNamePos  = 1
	attrNamePos   = 2
	attrTypPos    = 3
	charWidthPos  = 5
	primaryKeyPos = 10
)

/*
handle show create table in plan2 and tae
*/
func handleShowCreateTable2(ses *Session) error {
	tableName := string(ses.Data[0][tableNamePos].([]byte))
	createStr := fmt.Sprintf("CREATE TABLE `%s` (", tableName)
	rowCount := 0
	var pkDefs []string
	for _, d := range ses.Data {
		colName := string(d[attrNamePos].([]byte))
		if colName == "PADDR" {
			continue
		}
		nullOrNot := ""
		if d[7].(int8) != 0 {
			nullOrNot = "NOT NULL"
		} else {
			nullOrNot = "DEFAULT NULL"
		}
		if rowCount == 0 {
			createStr += "\n"
		} else {
			createStr += ",\n"
		}
		typ := types.Type{Oid: types.T(d[attrTypPos].(int32))}
		typeStr := typ.String()
		if typ.Oid == types.T_varchar || typ.Oid == types.T_char {
			typeStr += fmt.Sprintf("(%d)", d[charWidthPos].(int32))
		}
		createStr += fmt.Sprintf("`%s` %s %s", colName, typeStr, nullOrNot)
		rowCount++
		if string(d[primaryKeyPos].([]byte)) == "p" {
			pkDefs = append(pkDefs, colName)
		}
	}
	if len(pkDefs) != 0 {
		pkStr := "PRIMARY KEY ("
		for _, def := range pkDefs {
			pkStr += fmt.Sprintf("`%s`", def)
		}
		pkStr += ")"
		if rowCount != 0 {
			createStr += ",\n"
		}
		createStr += pkStr
	}

	if rowCount != 0 {
		createStr += "\n"
	}
	createStr += ")"

	row := make([]interface{}, 2)
	row[0] = tableName
	row[1] = createStr

	ses.Mrs.AddRow(row)

	if err := ses.GetMysqlProtocol().SendResultSetTextBatchRowSpeedup(ses.Mrs, 1); err != nil {
		logutil.Errorf("handleShowCreateTable2 error %v \n", err)
		return err
	}
	return nil
}

/*
handle show create database in plan2 and tae
*/
func handleShowCreateDatabase2(ses *Session) error {
	dbNameIndex := ses.Mrs.Name2Index["Database"]
	dbsqlIndex := ses.Mrs.Name2Index["Create Database"]
	firstRow := ses.Data[0]
	dbName := firstRow[dbNameIndex]
	createDBSql := fmt.Sprintf("CREATE DATABASE `%s`", dbName)
	firstRow[dbsqlIndex] = createDBSql

	row := make([]interface{}, 2)
	row[0] = dbName
	row[1] = createDBSql

	ses.Mrs.AddRow(row)
	if err := ses.GetMysqlProtocol().SendResultSetTextBatchRowSpeedup(ses.Mrs, 1); err != nil {
		logutil.Errorf("handleShowCreateDatabase2 error %v \n", err)
		return err
	}
	return nil
}

/*
handle show columns from table in plan2 and tae
*/
func handleShowColumns2(ses *Session) error {
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
		logutil.Errorf("handleShowCreateTable2 error %v \n", err)
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
	enableProfile := ses.Pu.SV.GetEnableProfileGetDataFromPipeline()

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

	oq := NewOuputQueue(proto, mrs, uint64(countOfResultSet), ses.ep, ses.showStmtType)
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
			case <-ses.closeRef.stopExportData:
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
		row, err := oq.getEmptyRow()
		if err != nil {
			return err
		}
		var rowIndex int64 = int64(j)
		if len(bat.Sels) != 0 {
			rowIndex = bat.Sels[j]
		}

		//begin1 := time.Now()
		for i, vec := range bat.Vecs { //col index
			rowIndexBackup := rowIndex
			if vec.IsScalarNull() {
				row[i] = nil
				continue
			}
			if vec.IsScalar() {
				rowIndex = 0
			}

			switch vec.Typ.Oid { //get col
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
					row[i] = vs[rowIndex].String2(precision)
				} else {
					if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
						row[i] = nil
					} else {
						vs := vec.Col.([]types.Timestamp)
						row[i] = vs[rowIndex].String2(precision)
					}
				}
			case types.T_decimal64:
				scale := vec.Typ.Scale
				if !nulls.Any(vec.Nsp) { //all data in this column are not null
					vs := vec.Col.([]types.Decimal64)
					row[i] = vs[rowIndex].Decimal64ToString(scale)
				} else {
					if nulls.Contains(vec.Nsp, uint64(rowIndex)) {
						row[i] = nil
					} else {
						vs := vec.Col.([]types.Decimal64)
						row[i] = vs[rowIndex].Decimal64ToString(scale)
					}
				}
			case types.T_decimal128:
				scale := vec.Typ.Scale
				if !nulls.Any(vec.Nsp) { //all data in this column are not null
					vs := vec.Col.([]types.Decimal128)
					row[i] = vs[rowIndex].Decimal128ToString(scale)
					fmt.Println(row[i])
				} else {
					if nulls.Contains(vec.Nsp, uint64(rowIndex)) {
						row[i] = nil
					} else {
						vs := vec.Col.([]types.Decimal128)
						row[i] = vs[rowIndex].Decimal128ToString(scale)
					}
				}
			default:
				logutil.Errorf("getDataFromPipeline : unsupported type %d \n", vec.Typ.Oid)
				return fmt.Errorf("getDataFromPipeline : unsupported type %d", vec.Typ.Oid)
			}
			rowIndex = rowIndexBackup
		}
		//row2colTime += time.Since(begin1)
		//duplicate rows
		for i := int64(0); i < bat.Zs[j]-1; i++ {
			erow, rr := oq.getEmptyRow()
			if rr != nil {
				return rr
			}
			for l := 0; l < len(bat.Vecs); l++ {
				erow[l] = row[l]
			}
		}
		if oq.showStmtType == ShowCreateDatabase || oq.showStmtType == ShowCreateTable || oq.showStmtType == ShowColumns {
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

func (mce *MysqlCmdExecutor) handleChangeDB(db string) error {
	ses := mce.GetSession()
	txnHandler := ses.GetTxnHandler()
	txnCtx := txnHandler.GetTxn().GetCtx()
	//TODO: check meta data
	if _, err := ses.Pu.StorageEngine.Database(db, txnCtx); err != nil {
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
func (mce *MysqlCmdExecutor) handleLoadData(load *tree.Load) error {
	var err error = nil
	ses := mce.GetSession()
	proto := ses.protocol

	logutil.Infof("+++++load data")
	/*
		TODO:support LOCAL
	*/
	if load.Local {
		return fmt.Errorf("LOCAL is unsupported now")
	}

	if load.Fields == nil || len(load.Fields.Terminated) == 0 {
		return fmt.Errorf("load need FIELDS TERMINATED BY ")
	}

	if load.Fields != nil && load.Fields.EscapedBy != 0 {
		return fmt.Errorf("EscapedBy field is unsupported now")
	}

	/*
		check file
	*/
	exist, isfile, err := PathExists(load.File)
	if err != nil || !exist {
		return fmt.Errorf("file %s does exist. err:%v", load.File, err)
	}

	if !isfile {
		return fmt.Errorf("file %s is a directory.", load.File)
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
	if txnHandler.isTxnState(TxnBegan) {
		return fmt.Errorf("Do not support the Load in a transaction started by BEGIN/START TRANSACTION statement")
	}
	dbHandler, err := ses.Pu.StorageEngine.Database(loadDb, txnHandler.GetTxn().GetCtx())
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
	tableHandler, err := dbHandler.Relation(loadTable, txnHandler.GetTxn().GetCtx())
	if err != nil {
		//echo client. no such table
		return NewMysqlError(ER_NO_SUCH_TABLE, loadDb, loadTable)
	}

	/*
		execute load data
	*/
	result, err := mce.LoadLoop(load, dbHandler, tableHandler, loadDb)
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
func (mce *MysqlCmdExecutor) handleCmdFieldList(tableName string) error {
	var err error = nil
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()

	//TODO:fix it on tae

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
			_, err = txnHandler.StartByAutocommitIfNeeded()
			if err != nil {
				return err
			}

			eng := ses.GetStorage()
			db, err := eng.Database(dbName, txnHandler.GetTxn().GetCtx())
			if err != nil {
				return err
			}

			names := db.Relations(txnHandler.GetTxn().GetCtx())
			for _, name := range names {
				table, err := db.Relation(name, txnHandler.GetTxn().GetCtx())
				if err != nil {
					return err
				}
				defs := table.TableDefs(txnHandler.GetTxn().GetCtx())
				for _, def := range defs {
					if attr, ok := def.(*engine.AttributeDef); ok {
						attrs = append(attrs, &engineColumnInfo{
							name: attr.Attr.Name,
							typ:  attr.Attr.Type,
						})
					}
				}
			}
		}
	} else {
		if mce.tableInfos == nil || mce.db != dbName {
			if ses.Pu.ClusterCatalog == nil {
				return fmt.Errorf("need cluster catalog")
			}
			tableInfos, err := ses.Pu.ClusterCatalog.ListTablesByName(dbName)
			if err != nil {
				return err
			}

			mce.db = ses.GetDatabaseName()
			mce.tableInfos = make(map[string][]ColumnInfo)

			//cache these info in the executor
			for _, table := range tableInfos {
				var infos []ColumnInfo
				for _, column := range table.Columns {
					infos = append(infos, &aoeColumnInfo{info: column})
				}
				mce.tableInfos[table.Name] = infos

			}
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

	var hasLike bool = false
	var likePattern string = ""
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

	var rows [][]interface{}
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

func (mce *MysqlCmdExecutor) handleAnalyzeStmt(stmt *tree.AnalyzeStmt) error {
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
	return mce.doComQuery(sql)
}

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
			} else {
				return errors.New(errno.InvalidOptionValue, fmt.Sprintf("unrecognized value for EXPLAIN option \"%s\": \"%s\"", v.Name, v.Value))
			}
		} else {
			return errors.New(errno.InvalidOptionValue, fmt.Sprintf("unrecognized EXPLAIN option \"%s\"", v.Name))
		}
	}

	//get query optimizer and execute Optimize
	buildPlan, err := buildPlan(mce.ses.txnCompileCtx, stmt.Statement)
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
	explainQuery := explain.NewExplainQueryImpl(buildPlan.GetQuery())
	err = explainQuery.ExplainPlan(buffer, es)
	if err != nil {
		logutil.Errorf("explain Query statement error: %v", err)
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("explain Query statement error:%v", err))
	}

	session := mce.GetSession()
	protocol := session.GetMysqlProtocol()

	attrs := plan.BuildExplainResultColumns()
	columns, err := GetExplainColumns(attrs)
	if err != nil {
		logutil.Errorf("GetColumns from ExplainColumns handler failed, error: %v", err)
		return err
	}
	/*
		Step 1 : send column count and column definition.
	*/
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
		/*
			mysql COM_QUERY response: send the column definition per column
		*/
		err := protocol.SendColumnDefinitionPacket(mysqlc, cmd)
		if err != nil {
			return err
		}
	}
	/*
		mysql COM_QUERY response: End after the column has been sent.
		send EOF packet
	*/
	err = protocol.SendEOFPacketIf(0, 0)
	if err != nil {
		return err
	}

	err = buildMoExplainQuery(attrs, buffer, session, getDataFromPipeline)
	if err != nil {
		return err
	}

	err = protocol.sendEOFOrOkPacket(0, 0)
	if err != nil {
		return err
	}
	return nil
}

func GetExplainColumns(attrs []*plan.Attribute) ([]interface{}, error) {
	//attrs := plan.BuildExplainResultColumns()
	cols := make([]*compile1.Col, len(attrs))
	for i, attr := range attrs {
		cols[i] = &compile1.Col{
			Name: attr.Name,
			Typ:  attr.Type.Oid,
		}
	}
	//e.resultCols = cols
	var mysqlCols []interface{} = make([]interface{}, len(cols))
	var err error = nil
	for i, c := range cols {
		col := new(MysqlColumn)
		col.SetName(c.Name)
		err = convertEngineTypeToMysqlType(c.Typ, col)
		if err != nil {
			return nil, err
		}
		mysqlCols[i] = col
	}
	return mysqlCols, err
}

func buildMoExplainQuery(attrs []*plan.Attribute, buffer *explain.ExplainDataBuffer, session *Session, fill func(interface{}, *batch.Batch) error) error {
	bat := batch.New(true, []string{attrs[0].Name})
	rs := buffer.Lines
	vs := make([][]byte, len(rs))

	count := 0
	for _, r := range rs {
		str := []byte(r)
		vs[count] = str
		count++
	}
	vs = vs[:count]
	vec := vector.New(attrs[0].Type)
	if err := vector.Append(vec, vs); err != nil {
		return err
	}
	bat.Vecs[0] = vec
	bat.InitZsOne(count)

	return fill(session, bat)
}

//----------------------------------------------------------------------------------------------------

type ComputationWrapperImpl struct {
	exec *compile1.Exec
}

func NewComputationWrapperImpl(e *compile1.Exec) *ComputationWrapperImpl {
	return &ComputationWrapperImpl{exec: e}
}

// GetAst gets ast of the statement
func (cw *ComputationWrapperImpl) GetAst() tree.Statement {
	return cw.exec.Statement()
}

//SetDatabaseName sets the database name
func (cw *ComputationWrapperImpl) SetDatabaseName(db string) error {
	return cw.exec.SetSchema(db)
}

func (cw *ComputationWrapperImpl) GetColumns() ([]interface{}, error) {
	columns := cw.exec.Columns()
	var mysqlCols []interface{} = make([]interface{}, len(columns))
	var err error = nil
	for i, c := range columns {
		col := new(MysqlColumn)
		col.SetName(c.Name)
		err = convertEngineTypeToMysqlType(c.Typ, col)
		if err != nil {
			return nil, err
		}
		mysqlCols[i] = col
	}
	return mysqlCols, err
}

func (cw *ComputationWrapperImpl) GetAffectedRows() uint64 {
	return cw.exec.GetAffectedRows()
}

func (cw *ComputationWrapperImpl) Compile(u interface{}, fill func(interface{}, *batch.Batch) error) (interface{}, error) {
	return cw.exec, cw.exec.Compile(u, fill)
}

func (cw *ComputationWrapperImpl) Run(ts uint64) error {
	return cw.exec.Run(ts)
}

var _ ComputationWrapper = &TxnComputationWrapper{}

type TxnComputationWrapper struct {
	stmt    tree.Statement
	plan    *plan2.Plan
	proc    *process.Process
	ses     *Session
	compile *compile2.Compile
}

func InitTxnComputationWrapper(ses *Session, stmt tree.Statement, proc *process.Process) *TxnComputationWrapper {
	return &TxnComputationWrapper{
		stmt: stmt,
		proc: proc,
		ses:  ses,
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
	case *tree.ShowCreateTable:
		cols = []*plan2.ColDef{
			{Typ: &plan2.Type{Id: plan3.Type_TypeId(types.T_char)}, Name: "Table"},
			{Typ: &plan2.Type{Id: plan3.Type_TypeId(types.T_char)}, Name: "Create Table"},
		}
	case *tree.ShowColumns:
		cols = []*plan2.ColDef{
			{Typ: &plan2.Type{Id: plan3.Type_TypeId(types.T_char)}, Name: "Field"},
			{Typ: &plan2.Type{Id: plan3.Type_TypeId(types.T_char)}, Name: "Type"},
			{Typ: &plan2.Type{Id: plan3.Type_TypeId(types.T_char)}, Name: "Null"},
			{Typ: &plan2.Type{Id: plan3.Type_TypeId(types.T_char)}, Name: "Key"},
			{Typ: &plan2.Type{Id: plan3.Type_TypeId(types.T_char)}, Name: "Default"},
			{Typ: &plan2.Type{Id: plan3.Type_TypeId(types.T_char)}, Name: "Comment"},
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

func (cwft *TxnComputationWrapper) Compile(u interface{}, fill func(interface{}, *batch.Batch) error) (interface{}, error) {
	var err error
	cwft.plan, err = buildPlan(cwft.ses.GetTxnCompilerContext(), cwft.stmt)
	if err != nil {
		return nil, err
	}

	cwft.proc.UnixTime = time.Now().UnixNano()
	txnHandler := cwft.ses.GetTxnHandler()
	cwft.proc.Snapshot = txnHandler.GetTxn().GetCtx()
	cwft.compile = compile2.New(cwft.ses.GetDatabaseName(), cwft.ses.GetSql(), cwft.ses.GetUserName(), cwft.ses.GetStorage(), cwft.proc)
	err = cwft.compile.Compile(cwft.plan, cwft.ses, fill)
	if err != nil {
		return nil, err
	}
	return cwft.compile, err
}

func (cwft *TxnComputationWrapper) Run(ts uint64) error {
	return nil
}

func buildPlan(ctx plan2.CompilerContext, stmt tree.Statement) (*plan2.Plan, error) {
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
	stmts, err := parsers.Parse(dialect.MYSQL, sql)
	if err != nil {
		return nil, err
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

//execute query
func (mce *MysqlCmdExecutor) doComQuery(sql string) (retErr error) {
	beginInstant := time.Now()
	ses := mce.GetSession()
	ses.showStmtType = NotShowStatement
	proto := ses.GetMysqlProtocol()
	txnHandler := ses.GetTxnHandler()
	ses.SetSql(sql)

	proc := process.New(mheap.New(ses.GuestMmu))
	proc.Id = mce.getNextProcessId()
	proc.Lim.Size = ses.Pu.SV.GetProcessLimitationSize()
	proc.Lim.BatchRows = ses.Pu.SV.GetProcessLimitationBatchRows()
	proc.Lim.PartitionRows = ses.Pu.SV.GetProcessLimitationPartitionRows()
	proc.SessionInfo = process.SessionInfo{
		User:         ses.GetUserName(),
		Host:         ses.Pu.SV.GetHost(),
		ConnectionID: uint64(proto.ConnectionID()),
		Database:     ses.GetDatabaseName(),
		Version:      serverVersion,
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
		ses.Mrs = nil
		_ = txnHandler.CleanTxn()
	}()

	var cmpBegin time.Time
	var ret interface{}
	var runner ComputationRunner
	var selfHandle = false
	var fromLoadData = false
	var txnErr error
	var rspLen uint64

	stmt := cws[0].GetAst()
	mce.beforeRun(stmt)
	defer mce.afterRun(stmt, beginInstant)
	// it is weired to do for loop here, why don't we ensure that run only one sql once
	// it seems that mysql protocol has done that for us when reading packet from tcp
	type TxnCommand int
	const (
		TxnNoCommand TxnCommand = iota
		TxnBegin
		TxnCommit
		TxnRollback
	)
	for _, cw := range cws {
		ses.Mrs = &MysqlResultSet{}
		stmt := cw.GetAst()

		var fromTxnCommand TxnCommand = TxnNoCommand
		//check transaction states
		switch stmt.(type) {
		case *tree.BeginTransaction:
			fromTxnCommand = TxnBegin
			err = txnHandler.StartByBegin()
			if err != nil {
				goto handleFailed
			}
		case *tree.CommitTransaction:
			fromTxnCommand = TxnCommit
			err = txnHandler.CommitAfterBegin()
			if err != nil {
				goto handleFailed
			}
		case *tree.RollbackTransaction:
			fromTxnCommand = TxnRollback
			err = txnHandler.Rollback()
			if err != nil {
				goto handleFailed
			}
		default:
			_, err = txnHandler.StartByAutocommitIfNeeded()
			if err != nil {
				goto handleFailed
			}
			logutil.Infof("start autocommit txn in default")
		}

		switch st := stmt.(type) {
		case *tree.Select:
			ses.ep.Outfile = false
			if st.Ep != nil {
				mce.exportDataClose = NewCloseExportData()
				ses.ep = st.Ep
				ses.closeRef = mce.exportDataClose
			}
		}

		selfHandle = false

		switch st := stmt.(type) {
		case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction:
			selfHandle = true
			err = proto.sendOKPacket(0, 0, 0, 0, "")
			if err != nil {
				goto handleFailed
			}
		case *tree.Use:
			selfHandle = true
			err = mce.handleChangeDB(st.Name)
			if err != nil {
				goto handleFailed
			}
			err = proto.sendOKPacket(0, 0, 0, 0, "")
			if err != nil {
				goto handleFailed
			}
		case *tree.Insert:
			_, ok := st.Rows.Select.(*tree.ValuesClause)
			if ok {
				selfHandle = true
				rspLen, err = mce.handleInsertValues(st, 0)
				if err != nil {
					goto handleFailed
				}
			}
		case *tree.DropDatabase:
			// if the droped database is the same as the one in use, database must be reseted to empty.
			if string(st.Name) == ses.GetDatabaseName() {
				ses.SetUserName("")
			}
		case *tree.Load:
			fromLoadData = true
			selfHandle = true
			err = mce.handleLoadData(st)
			if err != nil {
				goto handleFailed
			}
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
			if err = mce.handleAnalyzeStmt(st); err != nil {
				goto handleFailed
			}
		case *tree.ExplainStmt:
			selfHandle = true
			if err = mce.handleExplainStmt(st); err != nil {
				goto handleFailed
			}
		case *tree.ExplainAnalyze:
			selfHandle = true
			err = errors.New(errno.FeatureNotSupported, "not support explain analyze statement now")
			goto handleFailed
		case *tree.ShowColumns:
			ses.showStmtType = ShowColumns
			ses.Data = nil
		case *tree.ShowCreateDatabase:
			ses.showStmtType = ShowCreateDatabase
			ses.Data = nil
		case *tree.ShowCreateTable:
			ses.showStmtType = ShowCreateTable
			ses.Data = nil
		case *tree.Delete:
			ses.GetTxnCompileCtx().SetQueryType(TXN_DELETE)
		case *tree.Update:
			ses.GetTxnCompileCtx().SetQueryType(TXN_UPDATE)
		default:
			ses.GetTxnCompileCtx().SetQueryType(TXN_DEFAULT)
		}

		if selfHandle {
			goto handleSucceeded
		}
		if err = cw.SetDatabaseName(ses.GetDatabaseName()); err != nil {
			goto handleFailed
		}

		cmpBegin = time.Now()

		if ret, err = cw.Compile(ses, getDataFromPipeline); err != nil {
			goto handleFailed
		}

		runner = ret.(ComputationRunner)
		if ses.Pu.SV.GetRecordTimeElapsedOfSqlRequest() {
			logutil.Infof("time of Exec.Build : %s", time.Since(cmpBegin).String())
		}

		// cw.Compile might rewrite sql, here we fetch the latest version
		switch cw.GetAst().(type) {
		//produce result set
		case *tree.Select,
			*tree.ShowCreateTable, *tree.ShowCreateDatabase, *tree.ShowTables, *tree.ShowDatabases, *tree.ShowColumns,
			*tree.ShowProcessList, *tree.ShowErrors, *tree.ShowWarnings, *tree.ShowVariables, *tree.ShowStatus,
			*tree.ShowIndex,
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
				ses.ep.DefaultBufSize = ses.Pu.SV.GetExportDataDefaultFlushSize()
				initExportFileParam(ses.ep, ses.Mrs)
				if err = openNewFile(ses.ep, ses.Mrs); err != nil {
					goto handleFailed
				}
			}
			if err = runner.Run(0); err != nil {
				goto handleFailed
			}
			if ses.showStmtType == ShowCreateTable {
				if err = handleShowCreateTable2(ses); err != nil {
					goto handleFailed
				}
			} else if ses.showStmtType == ShowCreateDatabase {
				if err = handleShowCreateDatabase2(ses); err != nil {
					goto handleFailed
				}
			} else if ses.showStmtType == ShowColumns {
				if err = handleShowColumns2(ses); err != nil {
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

			if ses.Pu.SV.GetRecordTimeElapsedOfSqlRequest() {
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

			if ses.Pu.SV.GetRecordTimeElapsedOfSqlRequest() {
				logutil.Infof("time of Exec.Run : %s", time.Since(runBegin).String())
			}

			rspLen = cw.GetAffectedRows()
			echoTime := time.Now()
			if ses.Pu.SV.GetRecordTimeElapsedOfSqlRequest() {
				logutil.Infof("time of SendResponse %s", time.Since(echoTime).String())
			}
		}
	handleSucceeded:
		//load data handle txn failure internally
		if !fromLoadData {
			//txn begin,commit,rollback do not need to be committed
			if fromTxnCommand != TxnNoCommand {
				goto handleNext
			}
			txnErr = txnHandler.CommitAfterAutocommitOnly()
			if txnErr != nil {
				return txnErr
			}
			switch stmt.(type) {
			case *tree.CreateTable, *tree.DropTable, *tree.CreateDatabase, *tree.DropDatabase,
				*tree.CreateIndex, *tree.DropIndex, *tree.Insert, *tree.Update,
				*tree.CreateUser, *tree.DropUser, *tree.AlterUser,
				*tree.CreateRole, *tree.DropRole, *tree.Revoke, *tree.Grant,
				*tree.SetDefaultRole, *tree.SetRole, *tree.SetPassword, *tree.Delete:
				resp := NewOkResponse(rspLen, 0, 0, 0, int(COM_QUERY), "")
				if err := mce.GetSession().protocol.SendResponse(resp); err != nil {
					return fmt.Errorf("routine send response failed. error:%v ", err)
				}
			}
		}
		goto handleNext
	handleFailed:
		if !fromLoadData {
			//the failures due to txn begin,commit,rollback do not need to be rollback.
			if fromTxnCommand == TxnNoCommand {
				txnErr = txnHandler.RollbackAfterAutocommitOnly()
				if txnErr != nil {
					return txnErr
				}
			}
		}
		return err
	handleNext:
	} // end of for

	return nil
}

func (mce *MysqlCmdExecutor) handleDDl(ses *Session, stmt tree.Statement, epoch uint64) error {
	txnHandler := ses.GetTxnHandler()
	switch ddl := stmt.(type) {
	case *tree.CreateDatabase:
		name := string(ddl.Name)
		needCreate := true
		if ddl.IfNotExists {
			_, err := ses.storage.Database(name, txnHandler.GetTxn().GetCtx())
			if err == nil { //exists
				needCreate = false
			}
		}
		if needCreate {
			err := ses.storage.Create(epoch, name, 0, txnHandler.GetTxn().GetCtx())
			if err != nil {
				return err
			}
		}
	case *tree.DropDatabase:
		name := string(ddl.Name)
		needDrop := true
		if ddl.IfExists {
			_, err := ses.storage.Database(name, txnHandler.GetTxn().GetCtx())
			if err != nil { //no exists
				needDrop = false
			}
		}

		if needDrop {
			err := ses.storage.Delete(epoch, name, txnHandler.GetTxn().GetCtx())
			if err != nil {
				return err
			}
		}
	case *tree.CreateTable:
		//dbName := string(ddl.Table.Schema())
		//tableName := string(ddl.Table.Name())
		//
		////check db exists
		//db, err := ses.storage.Database(name, txnHandler.GetTxn().GetCtx())
		//if err == nil { //exists
		//
		//}
	}
	return nil
}

// ExecRequest the server execute the commands from the client following the mysql's routine
func (mce *MysqlCmdExecutor) ExecRequest(req *Request) (resp *Response, err error) {
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
		logutil.Infof("query:%s", SubStringFromBegin(query, int(ses.Pu.SV.GetLengthOfQueryPrinted())))
		seps := strings.Split(query, " ")
		if len(seps) <= 0 {
			resp = NewGeneralErrorResponse(COM_QUERY, fmt.Errorf("invalid query"))
			return resp, nil
		}

		if strings.ToLower(seps[0]) == "kill" {
			//last one is processID
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

		err := mce.doComQuery(query)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_QUERY, err)
		}
		return resp, nil
	case COM_INIT_DB:
		var dbname = string(req.GetData().([]byte))
		mce.addSqlCount(1)
		query := "use " + dbname
		err := mce.doComQuery(query)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_INIT_DB, err)
		}

		return resp, nil
	case COM_FIELD_LIST:
		var payload = string(req.GetData().([]byte))
		//find null
		nullIdx := strings.IndexRune(payload, rune(0))
		var tableName string
		if nullIdx < len(payload) {

			tableName = payload[:nullIdx]
			//wildcard := payload[nullIdx+1:]
			//logutil.Infof("table name %s wildcard [%s] ",tableName,wildcard)
			err := mce.handleCmdFieldList(tableName)
			if err != nil {
				resp = NewGeneralErrorResponse(COM_FIELD_LIST, err)
			}
		} else {
			resp = NewGeneralErrorResponse(COM_FIELD_LIST, fmt.Errorf("wrong format for COM_FIELD_LIST"))
		}

		return resp, nil
	case COM_PING:
		resp = NewGeneralOkResponse(COM_PING)

		return resp, nil
	default:
		err := fmt.Errorf("unsupported command. 0x%x \n", req.GetCmd())
		resp = NewGeneralErrorResponse(uint8(req.GetCmd()), err)
	}
	return resp, nil
}

func (mce *MysqlCmdExecutor) Close() {
	//logutil.Infof("close executor")
	if mce.loadDataClose != nil {
		//logutil.Infof("close process load data")
		mce.loadDataClose.Close()
	}
	if mce.exportDataClose != nil {
		mce.exportDataClose.Close()
	}
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
	default:
		return fmt.Errorf("RunWhileSend : unsupported type %d \n", engineType)
	}
	return nil
}
