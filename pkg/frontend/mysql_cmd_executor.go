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
	"fmt"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2/explain"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

//tableInfos of a database
type TableInfoCache struct {
	db         string
	tableInfos map[string]aoe.TableInfo
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

func (cei *MysqlCmdExecutor) PrepareSessionBeforeExecRequest(ses *Session) {
	cei.ses = ses
}

func (cei *MysqlCmdExecutor) GetSession() *Session {
	return cei.ses
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
	proto   MysqlProtocol
	mrs     *MysqlResultSet
	rowIdx  uint64
	length  uint64
	ep      *tree.ExportParam
	lineStr []byte

	getEmptyRowTime time.Duration
	flushTime       time.Duration
}

func (oq *outputQueue) ResetLineStr() {
	oq.lineStr = oq.lineStr[:0]
}

func NewOuputQueue(proto MysqlProtocol, mrs *MysqlResultSet, length uint64, ep *tree.ExportParam) *outputQueue {
	return &outputQueue{
		proto:  proto,
		mrs:    mrs,
		rowIdx: 0,
		length: length,
		ep:     ep,
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
		if err := o.proto.SendResultSetTextBatchRowSpeedup(o.mrs, o.rowIdx); err != nil {
			//return err
			logutil.Errorf("flush error %v \n", err)
			return err
		}
	}
	o.rowIdx = 0
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

	oq := NewOuputQueue(proto, mrs, uint64(countOfResultSet), ses.ep)
	oq.reset()

	row2colTime := time.Duration(0)

	procBatchBegin := time.Now()

	n := vector.Length(bat.Vecs[0])

	if enableProfile {
		pprof.StartCPUProfile(cpuf)
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
			switch vec.Typ.Oid { //get col
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
					row[i] = vs.Get(int64(rowIndex))
				} else {
					if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
						row[i] = nil
					} else {
						vs := vec.Col.(*types.Bytes)
						row[i] = vs.Get(int64(rowIndex))
					}
				}
			case types.T_varchar:
				if !nulls.Any(vec.Nsp) { //all data in this column are not null
					vs := vec.Col.(*types.Bytes)
					row[i] = vs.Get(int64(rowIndex))
				} else {
					if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
						row[i] = nil
					} else {
						vs := vec.Col.(*types.Bytes)
						row[i] = vs.Get(int64(rowIndex))
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
				if !nulls.Any(vec.Nsp) { //all data in this column are not null
					vs := vec.Col.([]types.Datetime)
					row[i] = vs[rowIndex]
				} else {
					if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
						row[i] = nil
					} else {
						vs := vec.Col.([]types.Datetime)
						row[i] = vs[rowIndex]
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
				return fmt.Errorf("getDataFromPipeline : unsupported type %d \n", vec.Typ.Oid)
			}
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
	//TODO: check meta data
	if _, err := ses.Pu.StorageEngine.Database(db); err != nil {
		//echo client. no such database
		return NewMysqlError(ER_BAD_DB_ERROR, db)
	}
	oldDB := ses.protocol.GetDatabaseName()
	ses.protocol.SetDatabaseName(db)

	logutil.Infof("User %s change database from [%s] to [%s]\n", ses.protocol.GetUserName(), oldDB, ses.protocol.GetDatabaseName())

	return nil
}

//handle SELECT DATABASE()
func (mce *MysqlCmdExecutor) handleSelectDatabase(sel *tree.Select) error {
	var err error = nil
	ses := mce.GetSession()
	proto := ses.protocol

	col := new(MysqlColumn)
	col.SetName("DATABASE()")
	col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	ses.Mrs.AddColumn(col)
	val := ses.protocol.GetDatabaseName()
	if val == "" {
		val = "NULL"
	}
	ses.Mrs.AddRow([]interface{}{val})

	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.Mrs)
	resp := NewResponse(ResultResponse, 0, int(COM_QUERY), mer)

	if err = proto.SendResponse(resp); err != nil {
		return fmt.Errorf("routine send response failed. error:%v ", err)
	}
	return nil
}

/*
handle "SELECT @@max_allowed_packet"
*/
func (mce *MysqlCmdExecutor) handleMaxAllowedPacket() error {
	var err error = nil
	ses := mce.GetSession()
	proto := ses.protocol

	col := new(MysqlColumn)
	col.SetColumnType(defines.MYSQL_TYPE_LONG)
	col.SetName("@@max_allowed_packet")
	ses.Mrs.AddColumn(col)

	var data = make([]interface{}, 1)
	//16MB
	data[0] = 16777216
	ses.Mrs.AddRow(data)

	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.Mrs)
	resp := NewResponse(ResultResponse, 0, int(COM_QUERY), mer)

	if err := proto.SendResponse(resp); err != nil {
		return fmt.Errorf("routine send response failed. error:%v ", err)
	}
	return err
}

/*
handle "SELECT @@version_comment"
*/
func (mce *MysqlCmdExecutor) handleVersionComment() error {
	var err error = nil
	ses := mce.GetSession()
	proto := ses.protocol

	col := new(MysqlColumn)
	col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col.SetName("@@version_comment")
	ses.Mrs.AddColumn(col)

	var data = make([]interface{}, 1)
	data[0] = "MatrixOne"
	ses.Mrs.AddRow(data)

	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.Mrs)
	resp := NewResponse(ResultResponse, 0, int(COM_QUERY), mer)

	if err := proto.SendResponse(resp); err != nil {
		return fmt.Errorf("routine send response failed. error:%v ", err)
	}
	return err
}

/*
handle "SELECT @@session.tx_isolation"
*/
func (mce *MysqlCmdExecutor) handleTxIsolation() error {
	var err error = nil
	ses := mce.GetSession()
	proto := ses.protocol

	col := new(MysqlColumn)
	col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col.SetName("@@session.tx_isolation")
	ses.Mrs.AddColumn(col)

	var data = make([]interface{}, 1)
	data[0] = "REPEATABLE-READ"
	ses.Mrs.AddRow(data)

	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.Mrs)
	resp := NewResponse(ResultResponse, 0, int(COM_QUERY), mer)

	if err := proto.SendResponse(resp); err != nil {
		return fmt.Errorf("routine send response failed. error:%v ", err)
	}
	return err
}

/*
handle "SELECT @@xxx.yyyy"
*/
func (mce *MysqlCmdExecutor) handleSelectVariables(v string) error {
	var err error = nil
	ses := mce.GetSession()
	proto := ses.protocol

	if v == "tx_isolation" || v == "transaction_isolation" {
		col := new(MysqlColumn)
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		col.SetName("@@tx_isolation")
		ses.Mrs.AddColumn(col)

		var data = make([]interface{}, 1)
		data[0] = "REPEATABLE-READ"
		ses.Mrs.AddRow(data)
	} else {
		return fmt.Errorf("unsupported system variable %s", v)
	}

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

	dbHandler, err := ses.Pu.StorageEngine.Database(loadDb)
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
	tableHandler, err := dbHandler.Relation(loadTable)
	if err != nil {
		//echo client. no such table
		return NewMysqlError(ER_NO_SUCH_TABLE, loadDb, loadTable)
	}

	/*
		execute load data
	*/
	result, err := mce.LoadLoop(load, dbHandler, tableHandler)
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

	db := proto.GetDatabaseName()
	if db == "" {
		return NewMysqlError(ER_NO_DB_ERROR)
	}

	//Get table infos for the database from the cube
	//case 1: there are no table infos for the db
	//case 2: db changed
	if mce.tableInfos == nil || mce.db != db {
		if ses.Pu.ClusterCatalog == nil {
			return fmt.Errorf("need cluster catalog")
		}
		tableInfos, err := ses.Pu.ClusterCatalog.ListTablesByName(db)
		if err != nil {
			return err
		}

		mce.db = proto.GetDatabaseName()
		mce.tableInfos = make(map[string]aoe.TableInfo)

		//cache these info in the executor
		for _, table := range tableInfos {
			mce.tableInfos[table.Name] = table
		}
	}

	var attrs []aoe.ColumnInfo
	table, ok := mce.tableInfos[tableName]
	if !ok {
		//just give the empty info when there is no such table.
		attrs = make([]aoe.ColumnInfo, 0)
	} else {
		attrs = table.Columns
	}

	for _, c := range attrs {
		col := new(MysqlColumn)
		col.SetName(c.Name)
		err = convertEngineTypeToMysqlType(c.Type.Oid, col)
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
func (mce *MysqlCmdExecutor) handleSetVar(_ *tree.SetVar) error {
	var err error = nil
	proto := mce.GetSession().protocol

	resp := NewOkResponse(0, 0, 0, 0, int(COM_QUERY), "")
	if err = proto.SendResponse(resp); err != nil {
		return fmt.Errorf("routine send response failed. error:%v ", err)
	}
	return nil
}

/*
handle show variables
*/
func (mce *MysqlCmdExecutor) handleShowVariables(_ *tree.ShowVariables) error {
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
	es := &explain.ExplainOptions{
		Verbose: false,
		Anzlyze: false,
		Format:  explain.EXPLAIN_FORMAT_TEXT,
	}

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

	//get CompilerContext
	ctx := plan2.NewMockCompilerContext()
	qry, err := plan2.BuildPlan(ctx, stmt.Statement)
	if err != nil {
		//fmt.Sprintf("build Query statement error: '%v'", tree.String(stmt, dialect.MYSQL))
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Build Query statement error:'%v'", tree.String(stmt.Statement, dialect.MYSQL)))
	}

	// build explain data buffer
	buffer := explain.NewExplainDataBuffer()
	// generator query explain
	explainQuery := explain.NewExplainQueryImpl(qry)
	explainQuery.ExplainPlan(buffer, es)

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
	cols := make([]*compile.Col, len(attrs))
	for i, attr := range attrs {
		cols[i] = &compile.Col{
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
	exec *compile.Exec
}

func NewComputationWrapperImpl(e *compile.Exec) *ComputationWrapperImpl {
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

func (cw *ComputationWrapperImpl) Compile(u interface{},
	fill func(interface{}, *batch.Batch) error) error {
	return cw.exec.Compile(u, fill)
}

func (cw *ComputationWrapperImpl) Run(ts uint64) error {
	return cw.exec.Run(ts)
}

/*
GetComputationWrapper gets the execs from the computation engine
*/
var GetComputationWrapper = func(db, sql, user string, eng engine.Engine, proc *process.Process) ([]ComputationWrapper, error) {
	comp := compile.New(db, sql, user, eng, proc)
	execs, err := comp.Build()
	if err != nil {
		return nil, err
	}

	var cw []ComputationWrapper = nil
	for _, e := range execs {
		cw = append(cw, NewComputationWrapperImpl(e))
	}
	return cw, err
}

//execute query
func (mce *MysqlCmdExecutor) doComQuery(sql string) error {
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()
	pdHook := ses.GetEpochgc()
	statementCount := uint64(1)

	//pin the epoch with 1
	epoch, _ := pdHook.IncQueryCountAtCurrentEpoch(statementCount)
	defer func() {
		pdHook.DecQueryCountAtEpoch(epoch, statementCount)
	}()

	proc := process.New(mheap.New(ses.GuestMmu))
	proc.Id = mce.getNextProcessId()
	proc.Lim.Size = ses.Pu.SV.GetProcessLimitationSize()
	proc.Lim.BatchRows = ses.Pu.SV.GetProcessLimitationBatchRows()
	proc.Lim.PartitionRows = ses.Pu.SV.GetProcessLimitationPartitionRows()

	cws, err := GetComputationWrapper(proto.GetDatabaseName(),
		sql,
		proto.GetUserName(),
		ses.Pu.StorageEngine,
		proc)
	if err != nil {
		return NewMysqlError(ER_PARSE_ERROR, err,
			"You have an error in your SQL syntax; check the manual that corresponds to your MatrixOne server version for the right syntax to use")
	}

	defer func() {
		ses.Mrs = nil
	}()

	for _, cw := range cws {
		ses.Mrs = &MysqlResultSet{}
		stmt := cw.GetAst()
		//temp try 0 epoch
		pdHook.IncQueryCountAtEpoch(epoch, 1)
		statementCount++

		switch st := stmt.(type) {
		case *tree.Select:
			if st.Ep != nil {
				mce.exportDataClose = NewCloseExportData()
				ses.ep = st.Ep
				ses.closeRef = mce.exportDataClose
			}
			if sc, ok := st.Select.(*tree.SelectClause); ok {
				if len(sc.Exprs) == 1 {
					if fe, ok := sc.Exprs[0].Expr.(*tree.FuncExpr); ok {
						if un, ok := fe.Func.FunctionReference.(*tree.UnresolvedName); ok {
							if strings.ToUpper(un.Parts[0]) == "DATABASE" {
								err = mce.handleSelectDatabase(st)
								if err != nil {
									return err
								}

								//next statement
								continue
							}
						}
					} else if ve, ok := sc.Exprs[0].Expr.(*tree.VarExpr); ok {
						if strings.ToLower(ve.Name) == "max_allowed_packet" {
							err = mce.handleMaxAllowedPacket()
							if err != nil {
								return err
							}

							//next statement
							continue
						} else if strings.ToLower(ve.Name) == "version_comment" {
							err = mce.handleVersionComment()
							if err != nil {
								return err
							}

							//next statement
							continue
						} else if strings.ToLower(ve.Name) == "tx_isolation" {
							err = mce.handleTxIsolation()
							if err != nil {
								return err
							}

							//next statement
							continue
						}
					}
				}
			}
		}

		//check database
		if proto.GetDatabaseName() == "" {
			//if none database has been selected, database operations must be failed.
			switch t := stmt.(type) {
			case *tree.ShowDatabases, *tree.CreateDatabase, *tree.ShowCreateDatabase, *tree.ShowWarnings, *tree.ShowErrors,
				*tree.ShowStatus, *tree.DropDatabase, *tree.Load,
				*tree.Use, *tree.SetVar:
			case *tree.ShowColumns:
				if t.Table.ToTableName().SchemaName == "" {
					return NewMysqlError(ER_NO_DB_ERROR)
				}
			case *tree.ShowTables:
				if t.DBName == "" {
					return NewMysqlError(ER_NO_DB_ERROR)
				}
			default:
				return NewMysqlError(ER_NO_DB_ERROR)
			}
		}

		var selfHandle = false

		switch st := stmt.(type) {
		case *tree.Use:
			selfHandle = true
			err := mce.handleChangeDB(st.Name)
			if err != nil {
				return err
			}
			err = proto.sendOKPacket(0, 0, 0, 0, "")
			if err != nil {
				return err
			}
		case *tree.DropDatabase:
			// if the droped database is the same as the one in use, database must be reseted to empty.
			if string(st.Name) == proto.GetDatabaseName() {
				proto.SetUserName("")
			}
		case *tree.Load:
			selfHandle = true
			err = mce.handleLoadData(st)
			if err != nil {
				return err
			}
		case *tree.SetVar:
			selfHandle = true
			err = mce.handleSetVar(st)
			if err != nil {
				return err
			}
		case *tree.ShowVariables:
			selfHandle = true
			err = mce.handleShowVariables(st)
			if err != nil {
				return err
			}
		case *tree.AnalyzeStmt:
			selfHandle = true
			if err = mce.handleAnalyzeStmt(st); err != nil {
				return err
			}
		case *tree.ExplainStmt:
			selfHandle = true
			if err = mce.handleExplainStmt(st); err != nil {
				return err
			}
		case *tree.ExplainAnalyze:
			selfHandle = true
			return errors.New(errno.FeatureNotSupported, "not support explain analyze statement now")
		}

		if selfHandle {
			continue
		}
		if err = cw.SetDatabaseName(proto.GetDatabaseName()); err != nil {
			return err
		}

		cmpBegin := time.Now()
		if err = cw.Compile(ses, getDataFromPipeline); err != nil {
			return err
		}

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
			columns, err := cw.GetColumns()
			if err != nil {
				logutil.Errorf("GetColumns from Computation handler failed. error: %v", err)
			}
			/*
				Step 1 : send column count and column definition.
			*/
			//send column count
			colCnt := uint64(len(columns))
			err = proto.SendColumnCountPacket(colCnt)
			if err != nil {
				return err
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
				err := proto.SendColumnDefinitionPacket(mysqlc, cmd)
				if err != nil {
					return err
				}
			}

			/*
				mysql COM_QUERY response: End after the column has been sent.
				send EOF packet
			*/
			err = proto.SendEOFPacketIf(0, 0)
			if err != nil {
				return err
			}

			runBegin := time.Now()
			/*
				Step 2: Start pipeline
				Producing the data row and sending the data row
			*/
			if ses.ep.Outfile {
				ses.ep.DefaultBufSize = ses.Pu.SV.GetExportDataDefaultFlushSize()
				initExportFileParam(ses.ep, ses.Mrs)
				if err := openNewFile(ses.ep, ses.Mrs); err != nil {
					return err
				}
			}
			if er := cw.Run(epoch); er != nil {
				return er
			}
			if ses.ep.Outfile {
				if err = ses.ep.Writer.Flush(); err != nil {
					return err
				}
				if err = ses.ep.File.Close(); err != nil {
					return err
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
				return err
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
			if er := cw.Run(epoch); er != nil {
				return er
			}
			if ses.Pu.SV.GetRecordTimeElapsedOfSqlRequest() {
				logutil.Infof("time of Exec.Run : %s", time.Since(runBegin).String())
			}

			//record ddl drop xxx after the success
			switch stmt.(type) {
			case *tree.DropTable, *tree.DropDatabase,
				*tree.DropIndex, *tree.DropUser, *tree.DropRole:
				//test ddl
				pdHook.IncDDLCountAtEpoch(epoch, 1)
			}

			/*
				Step 2: Echo client
			*/
			resp := NewOkResponse(
				cw.GetAffectedRows(),
				0,
				0,
				0,
				int(COM_QUERY),
				nil,
			)
			echoTime := time.Now()
			if err = proto.SendResponse(resp); err != nil {
				return err
			}
			if ses.Pu.SV.GetRecordTimeElapsedOfSqlRequest() {
				logutil.Infof("time of SendResponse %s", time.Since(echoTime).String())
			}
		}
	}

	return nil
}

// ExecRequest the server execute the commands from the client following the mysql's routine
func (mce *MysqlCmdExecutor) ExecRequest(req *Request) (*Response, error) {
	var resp *Response = nil
	logutil.Infof("cmd %v", req.GetCmd())

	ses := mce.GetSession()
	if ses.Pu.SV.GetRejectWhenHeartbeatFromPDLeaderIsTimeout() {
		pdHook := ses.GetEpochgc()
		if !pdHook.CanAcceptSomething() {
			resp = NewGeneralErrorResponse(uint8(req.GetCmd()), fmt.Errorf("heartbeat from pdleader is timeout. the server reject sql request. cmd %d \n", req.GetCmd()))
			return resp, nil
		}
	}

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
		err := mce.handleChangeDB(dbname)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_INIT_DB, err)
		} else {
			resp = NewGeneralOkResponse(COM_INIT_DB)
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
