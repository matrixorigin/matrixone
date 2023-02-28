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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/explain"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/sync/errgroup"
)

func onlyCreateStatementErrorInfo() string {
	return "Only CREATE of DDL is supported in transactions"
}

func administrativeCommandIsUnsupportedInTxnErrorInfo() string {
	return "administrative command is unsupported in transactions"
}

func parameterModificationInTxnErrorInfo() string {
	return "Uncommitted transaction exists. Please commit or rollback first."
}

func unclassifiedStatementInUncommittedTxnErrorInfo() string {
	return "unclassified statement appears in uncommitted transaction"
}

func abortTransactionErrorInfo() string {
	return "Previous DML conflicts with existing constraints or data format. This transaction has to be aborted"
}

func writeWriteConflictsErrorInfo() string {
	return "Write conflicts detected. Previous transaction need to be aborted."
}

const (
	prefixPrepareStmtName       = "__mo_stmt_id"
	prefixPrepareStmtSessionVar = "__mo_stmt_var"
)

func getPrepareStmtName(stmtID uint32) string {
	return fmt.Sprintf("%s_%d", prefixPrepareStmtName, stmtID)
}

func GetPrepareStmtID(ctx context.Context, name string) (int, error) {
	idx := len(prefixPrepareStmtName) + 1
	if idx >= len(name) {
		return -1, moerr.NewInternalError(ctx, "can not get Prepare stmtID")
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

	doQueryFunc doComQueryFunc

	mu sync.Mutex
}

func (mce *MysqlCmdExecutor) CancelRequest() {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	if mce.cancelRequestFunc != nil {
		mce.cancelRequestFunc()
	}
}

func (mce *MysqlCmdExecutor) ChooseDoQueryFunc(choice bool) {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	if choice {
		mce.doQueryFunc = mce.doComQueryInProgress
	} else {
		mce.doQueryFunc = mce.doComQuery
	}
}

func (mce *MysqlCmdExecutor) GetDoQueryFunc() doComQueryFunc {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	if mce.doQueryFunc == nil {
		mce.doQueryFunc = mce.doComQuery
	}
	return mce.doQueryFunc
}

func (mce *MysqlCmdExecutor) SetSession(ses *Session) {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	mce.ses = ses
}

func (mce *MysqlCmdExecutor) GetSession() *Session {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	return mce.ses
}

// get new process id
func (mce *MysqlCmdExecutor) getNextProcessId() string {
	/*
		temporary method:
		routineId + sqlCount
	*/
	routineId := mce.GetSession().GetMysqlProtocol().ConnectionID()
	return fmt.Sprintf("%d%d", routineId, mce.GetSqlCount())
}

func (mce *MysqlCmdExecutor) GetSqlCount() uint64 {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	return mce.sqlCount
}

func (mce *MysqlCmdExecutor) addSqlCount(a uint64) {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	mce.sqlCount += a
}

func (mce *MysqlCmdExecutor) SetRoutineManager(mgr *RoutineManager) {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	mce.routineMgr = mgr
}

func (mce *MysqlCmdExecutor) GetRoutineManager() *RoutineManager {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	return mce.routineMgr
}

var RecordStatement = func(ctx context.Context, ses *Session, proc *process.Process, cw ComputationWrapper, envBegin time.Time, envStmt string, useEnv bool) context.Context {
	if !motrace.GetTracerProvider().IsEnable() {
		return ctx
	}
	sessInfo := proc.SessionInfo
	tenant := ses.GetTenantInfo()
	if tenant == nil {
		tenant, _ = GetTenantInfo(ctx, "internal")
	}
	var txnID uuid.UUID
	var txn TxnOperator
	var err error
	if handler := ses.GetTxnHandler(); handler.IsValidTxn() {
		txn, err = handler.GetTxn()
		if err != nil {
			logErrorf(ses.GetConciseProfile(), "RecordStatement. error:%v", err)
		} else {
			copy(txnID[:], txn.Txn().ID)
		}
	}
	var sesID uuid.UUID
	copy(sesID[:], ses.GetUUID())
	requestAt := envBegin
	if !useEnv {
		requestAt = time.Now()
	}
	var stmID uuid.UUID
	var statement tree.Statement = nil
	var text string
	if cw != nil {
		copy(stmID[:], cw.GetUUID())
		statement = cw.GetAst()
		ses.ast = statement
		fmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))
		statement.Format(fmtCtx)
		text = SubStringFromBegin(fmtCtx.String(), int(ses.GetParameterUnit().SV.LengthOfQueryPrinted))
	} else {
		stmID = uuid.New()
		text = SubStringFromBegin(envStmt, int(ses.GetParameterUnit().SV.LengthOfQueryPrinted))
	}
	stm := &motrace.StatementInfo{
		StatementID:          stmID,
		TransactionID:        txnID,
		SessionID:            sesID,
		Account:              tenant.GetTenant(),
		RoleId:               proc.SessionInfo.RoleId,
		User:                 tenant.GetUser(),
		Host:                 sessInfo.GetHost(),
		Database:             ses.GetDatabaseName(),
		Statement:            text,
		StatementFingerprint: "", // fixme: (Reserved)
		StatementTag:         "", // fixme: (Reserved)
		SqlSourceType:        ses.sqlSourceType,
		RequestAt:            requestAt,
		StatementType:        getStatementType(statement).GetStatementType(),
		QueryType:            getStatementType(statement).GetQueryType(),
	}
	if ses.sqlSourceType != "internal_sql" {
		ses.tStmt = stm
		ses.pushQueryId(types.Uuid(stmID).ToString())
	}
	if !stm.IsZeroTxnID() {
		stm.Report(ctx)
	}
	sc := trace.SpanContextWithID(trace.TraceID(stmID), trace.SpanKindStatement)
	proc.WithSpanContext(sc)
	reqCtx := ses.GetRequestContext()
	ses.SetRequestContext(trace.ContextWithSpanContext(reqCtx, sc))
	return motrace.ContextWithStatement(trace.ContextWithSpanContext(ctx, sc), stm)
}

var RecordParseErrorStatement = func(ctx context.Context, ses *Session, proc *process.Process, envBegin time.Time, envStmt string) context.Context {
	ctx = RecordStatement(ctx, ses, proc, nil, envBegin, envStmt, true)
	tenant := ses.GetTenantInfo()
	if tenant == nil {
		tenant, _ = GetTenantInfo(ctx, "internal")
	}
	incStatementCounter(tenant.GetTenant(), nil)
	incStatementErrorsCounter(tenant.GetTenant(), nil)
	return ctx
}

// RecordStatementTxnID record txnID after TxnBegin or Compile(autocommit=1)
var RecordStatementTxnID = func(ctx context.Context, ses *Session) {
	var err error
	var txn TxnOperator
	if stm := motrace.StatementFromContext(ctx); ses != nil && stm != nil && stm.IsZeroTxnID() {
		if handler := ses.GetTxnHandler(); handler.IsValidTxn() {
			txn, err = handler.GetTxn()
			if err != nil {
				logErrorf(ses.GetConciseProfile(), "RecordStatementTxnID. error:%v", err)
			} else {
				stm.SetTxnID(txn.Txn().ID)
			}

		}
		stm.Report(ctx)
	}
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
	ses          *Session
	ctx          context.Context
	proto        MysqlProtocol
	mrs          *MysqlResultSet
	rowIdx       uint64
	length       uint64
	ep           *ExportParam
	lineStr      []byte
	showStmtType ShowStatementType
}

func (oq *outputQueue) resetLineStr() {
	oq.lineStr = oq.lineStr[:0]
}

func NewOutputQueue(ctx context.Context, ses *Session, columnCount int, mrs *MysqlResultSet, ep *ExportParam) *outputQueue {
	const countOfResultSet = 1
	if ctx == nil {
		ctx = ses.GetRequestContext()
	}
	if mrs == nil {
		//Create a new temporary result set per pipeline thread.
		mrs = &MysqlResultSet{}
		//Warning: Don't change ResultColumns in this.
		//Reference the shared ResultColumns of the session among multi-thread.
		sesMrs := ses.GetMysqlResultSet()
		mrs.Columns = sesMrs.Columns
		mrs.Name2Index = sesMrs.Name2Index

		//group row
		mrs.Data = make([][]interface{}, countOfResultSet)
		for i := 0; i < countOfResultSet; i++ {
			mrs.Data[i] = make([]interface{}, columnCount)
		}
	}

	if ep == nil {
		ep = ses.GetExportParam()
	}

	return &outputQueue{
		ctx:          ctx,
		ses:          ses,
		proto:        ses.GetMysqlProtocol(),
		mrs:          mrs,
		rowIdx:       0,
		length:       uint64(countOfResultSet),
		ep:           ep,
		showStmtType: ses.GetShowStmtType(),
	}
}

func (oq *outputQueue) reset() {}

/*
getEmptyRow returns an empty space for filling data.
If there is no space, it flushes the data into the protocol
and returns an empty space then.
*/
func (oq *outputQueue) getEmptyRow() ([]interface{}, error) {
	if oq.rowIdx >= oq.length {
		if err := oq.flush(); err != nil {
			return nil, err
		}
	}

	row := oq.mrs.Data[oq.rowIdx]
	oq.rowIdx++
	return row, nil
}

/*
flush will force the data flushed into the protocol.
*/
func (oq *outputQueue) flush() error {
	if oq.rowIdx <= 0 {
		return nil
	}
	if oq.ep.Outfile {
		if err := exportDataToCSVFile(oq); err != nil {
			logErrorf(oq.ses.GetConciseProfile(), "export to csv file error %v", err)
			return err
		}
	} else {
		//send group of row
		if oq.showStmtType == ShowTableStatus {
			oq.rowIdx = 0
			return nil
		}

		if err := oq.proto.SendResultSetTextBatchRowSpeedup(oq.mrs, oq.rowIdx); err != nil {
			logErrorf(oq.ses.GetConciseProfile(), "flush error %v", err)
			return err
		}
	}
	oq.rowIdx = 0
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

func handleShowTableStatus(ses *Session, stmt *tree.ShowTableStatus, proc *process.Process) error {
	db, err := ses.GetStorage().Database(ses.requestCtx, stmt.DbName, proc.TxnOperator)
	if err != nil {
		return err
	}
	mrs := ses.GetMysqlResultSet()
	for _, row := range ses.data {
		tableName := string(row[0].([]byte))
		r, err := db.Relation(ses.requestCtx, tableName)
		if err != nil {
			return err
		}
		_, err = r.Ranges(ses.requestCtx, nil)
		if err != nil {
			return err
		}
		row[3], err = r.Rows(ses.requestCtx)
		if err != nil {
			return err
		}
		mrs.AddRow(row)
	}
	if err := ses.GetMysqlProtocol().SendResultSetTextBatchRowSpeedup(mrs, mrs.GetRowCount()); err != nil {
		logErrorf(ses.GetConciseProfile(), "handleShowTableStatus error %v", err)
		return err
	}
	return nil
}

/*
extract the data from the pipeline.
obj: session
Warning: The pipeline is the multi-thread environment. The getDataFromPipeline will
access the shared data. Be careful when it writes the shared data.
*/
func getDataFromPipeline(obj interface{}, bat *batch.Batch) error {
	ses := obj.(*Session)
	if openSaveQueryResult(ses) {
		if bat == nil {
			if err := saveQueryResultMeta(ses); err != nil {
				return err
			}
		} else {
			if err := saveQueryResult(ses, bat); err != nil {
				return err
			}
		}
	}
	if bat == nil {
		return nil
	}

	begin := time.Now()
	proto := ses.GetMysqlProtocol()
	proto.ResetStatistics()

	oq := NewOutputQueue(ses.GetRequestContext(), ses, len(bat.Vecs), nil, nil)
	row2colTime := time.Duration(0)
	procBatchBegin := time.Now()
	n := vector.Length(bat.Vecs[0])
	requestCtx := ses.GetRequestContext()
	for j := 0; j < n; j++ { //row index
		if oq.ep.Outfile {
			select {
			case <-requestCtx.Done():
				return nil
			default:
			}
		}

		if bat.Zs[j] <= 0 {
			continue
		}
		row, err := extractRowFromEveryVector(ses, bat, int64(j), oq)
		if err != nil {
			return err
		}
		if oq.showStmtType == ShowTableStatus {
			row2 := make([]interface{}, len(row))
			copy(row2, row)
			ses.AppendData(row2)
		}
	}

	err := oq.flush()
	if err != nil {
		return err
	}

	procBatchTime := time.Since(procBatchBegin)
	tTime := time.Since(begin)
	logInfof(ses.GetConciseProfile(), "rowCount %v \n"+
		"time of getDataFromPipeline : %s \n"+
		"processBatchTime %v \n"+
		"row2colTime %v \n"+
		"restTime(=totalTime - row2colTime) %v \n"+
		"protoStats %s",
		n,
		tTime,
		procBatchTime,
		row2colTime,
		tTime-row2colTime,
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

func formatFloatNum[T types.Floats](num T, Typ types.Type) T {
	if Typ.Precision == -1 || Typ.Width == 0 {
		return num
	}
	pow := math.Pow10(int(Typ.Precision))
	t := math.Abs(float64(num))
	upperLimit := math.Pow10(int(Typ.Width))
	if t >= upperLimit {
		t = upperLimit - 1
	} else {
		t *= pow
		t = math.Round(t)
	}
	if t >= upperLimit {
		t = upperLimit - 1
	}
	t /= pow
	if num < 0 {
		t = -1 * t
	}
	return T(t)
}

// extractRowFromVector gets the rowIndex row from the i vector
func extractRowFromVector(ses *Session, vec *vector.Vector, i int, row []interface{}, rowIndex int64) error {
	timeZone := ses.GetTimeZone()
	switch vec.Typ.Oid { //get col
	case types.T_json:
		if !nulls.Any(vec.Nsp) {
			row[i] = types.DecodeJson(vec.GetBytes(rowIndex))
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) {
				row[i] = nil
			} else {
				row[i] = types.DecodeJson(vec.GetBytes(rowIndex))
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
			row[i] = formatFloatNum(vs[rowIndex], vec.Typ)
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.([]float32)
				row[i] = formatFloatNum(vs[rowIndex], vec.Typ)
			}
		}
	case types.T_float64:
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.([]float64)
			row[i] = formatFloatNum(vs[rowIndex], vec.Typ)
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.([]float64)
				row[i] = formatFloatNum(vs[rowIndex], vec.Typ)
			}
		}
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			row[i] = vec.GetBytes(rowIndex)
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				row[i] = vec.GetBytes(rowIndex)
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
	case types.T_time:
		precision := vec.Typ.Precision
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.([]types.Time)
			row[i] = vs[rowIndex].String2(precision)
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.([]types.Time)
				row[i] = vs[rowIndex].String2(precision)
			}
		}
	case types.T_timestamp:
		precision := vec.Typ.Precision
		if !nulls.Any(vec.Nsp) { //all data in this column are not null
			vs := vec.Col.([]types.Timestamp)
			row[i] = vs[rowIndex].String2(timeZone, precision)
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.([]types.Timestamp)
				row[i] = vs[rowIndex].String2(timeZone, precision)
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
	case types.T_uuid:
		if !nulls.Any(vec.Nsp) {
			vs := vec.Col.([]types.Uuid)
			row[i] = vs[rowIndex].ToString()
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.([]types.Uuid)
				row[i] = vs[rowIndex].ToString()
			}
		}
	case types.T_Rowid:
		if !nulls.Any(vec.Nsp) {
			vs := vec.Col.([]types.Rowid)
			row[i] = vs[rowIndex]
		} else {
			if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
				row[i] = nil
			} else {
				vs := vec.Col.([]types.Rowid)
				row[i] = vs[rowIndex]
			}
		}
	default:
		logErrorf(ses.GetConciseProfile(), "extractRowFromVector : unsupported type %d", vec.Typ.Oid)
		return moerr.NewInternalError(ses.requestCtx, "extractRowFromVector : unsupported type %d", vec.Typ.Oid)
	}
	return nil
}

func doUse(ctx context.Context, ses *Session, db string) error {
	txnHandler := ses.GetTxnHandler()
	var txn TxnOperator
	var err error
	txn, err = txnHandler.GetTxn()
	if err != nil {
		return err
	}
	//TODO: check meta data
	if _, err = ses.GetParameterUnit().StorageEngine.Database(ctx, db, txn); err != nil {
		//echo client. no such database
		return moerr.NewBadDB(ctx, db)
	}
	oldDB := ses.GetDatabaseName()
	ses.SetDatabaseName(db)

	logInfof(ses.GetConciseProfile(), "User %s change database from [%s] to [%s]", ses.GetUserName(), oldDB, ses.GetDatabaseName())

	return nil
}

func (mce *MysqlCmdExecutor) handleChangeDB(requestCtx context.Context, db string) error {
	return doUse(requestCtx, mce.GetSession(), db)
}

func (mce *MysqlCmdExecutor) handleDump(requestCtx context.Context, dump *tree.MoDump) error {
	var err error
	if !dump.DumpDatabase {
		return doDumpQueryResult(requestCtx, mce.GetSession(), dump.ExportParams)
	}
	dump.OutFile = maybeAppendExtension(dump.OutFile)
	exists, err := fileExists(dump.OutFile)
	if exists {
		return moerr.NewFileAlreadyExists(requestCtx, dump.OutFile)
	}
	if err != nil {
		return err
	}
	if dump.MaxFileSize != 0 && dump.MaxFileSize < mpool.MB {
		return moerr.NewInvalidInput(requestCtx, "max file size must be larger than 1MB")
	}
	if len(dump.Database) == 0 {
		return moerr.NewInvalidInput(requestCtx, "No database selected")
	}
	return mce.dumpData(requestCtx, dump)
}

func (mce *MysqlCmdExecutor) dumpData(requestCtx context.Context, dump *tree.MoDump) error {
	ses := mce.GetSession()
	txnHandler := ses.GetTxnHandler()
	bh := ses.GetBackgroundExec(requestCtx)
	defer bh.Close()
	dbName := string(dump.Database)
	var (
		db        engine.Database
		err       error
		showDbDDL = false
		dbDDL     string
		tables    []string
	)
	var txn TxnOperator
	txn, err = txnHandler.GetTxn()
	if err != nil {
		return err
	}
	if db, err = ses.GetParameterUnit().StorageEngine.Database(requestCtx, dbName, txn); err != nil {
		return moerr.NewBadDB(requestCtx, dbName)
	}
	err = bh.Exec(requestCtx, fmt.Sprintf("use `%s`", dbName))
	if err != nil {
		return err
	}
	if len(dump.Tables) == 0 {
		dbDDL = fmt.Sprintf("DROP DATABASE IF EXISTS `%s`;\n", dbName)
		createSql, err := getDDL(bh, requestCtx, fmt.Sprintf("SHOW CREATE DATABASE `%s`;", dbName))
		if err != nil {
			return err
		}
		dbDDL += createSql + "\n\nUSE `" + dbName + "`;\n\n"
		showDbDDL = true
		tables, err = db.Relations(requestCtx)
		if err != nil {
			return err
		}
	} else {
		tables = make([]string, len(dump.Tables))
		for i, t := range dump.Tables {
			tables[i] = string(t.ObjectName)
		}
	}

	params := make([]*dumpTable, 0, len(tables))
	for _, tblName := range tables {
		if strings.HasPrefix(tblName, "%!%") { //skip hidden table
			continue
		}
		table, err := db.Relation(requestCtx, tblName)
		if err != nil {
			return err
		}
		tblDDL, err := getDDL(bh, requestCtx, fmt.Sprintf("SHOW CREATE TABLE `%s`;", tblName))
		if err != nil {
			return err
		}
		tableDefs, err := table.TableDefs(requestCtx)
		if err != nil {
			return err
		}
		attrs, isView, err := getAttrFromTableDef(tableDefs)
		if err != nil {
			return err
		}
		if isView {
			tblDDL = fmt.Sprintf("DROP VIEW IF EXISTS `%s`;\n", tblName) + tblDDL + "\n\n"
		} else {
			tblDDL = fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", tblName) + tblDDL + "\n\n"
		}
		params = append(params, &dumpTable{tblName, tblDDL, table, attrs, isView})
	}
	return mce.dumpData2File(requestCtx, dump, dbDDL, params, showDbDDL)
}

func (mce *MysqlCmdExecutor) dumpData2File(requestCtx context.Context, dump *tree.MoDump, dbDDL string, params []*dumpTable, showDbDDL bool) error {
	ses := mce.GetSession()
	var (
		err         error
		f           *os.File
		curFileSize int64 = 0
		curFileIdx  int64 = 1
		buf         *bytes.Buffer
		rbat        *batch.Batch
	)
	f, err = createDumpFile(requestCtx, dump.OutFile)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			if f != nil {
				f.Close()
			}
			if buf != nil {
				buf.Reset()
			}
			if rbat != nil {
				rbat.Clean(ses.mp)
			}
			removeFile(dump.OutFile, curFileIdx)
		}
	}()
	buf = new(bytes.Buffer)
	if showDbDDL {
		_, err = buf.WriteString(dbDDL)
		if err != nil {
			return err
		}
	}
	f, curFileIdx, curFileSize, err = writeDump2File(requestCtx, buf, dump, f, curFileIdx, curFileSize)
	if err != nil {
		return err
	}
	for _, param := range params {
		if param.isView {
			continue
		}
		_, err = buf.WriteString(param.ddl)
		if err != nil {
			return err
		}
		f, curFileIdx, curFileSize, err = writeDump2File(requestCtx, buf, dump, f, curFileIdx, curFileSize)
		if err != nil {
			return err
		}
		rds, err := param.rel.NewReader(requestCtx, 1, nil, nil)
		if err != nil {
			return err
		}
		for {
			bat, err := rds[0].Read(requestCtx, param.attrs, nil, ses.mp)
			if err != nil {
				return err
			}
			if bat == nil {
				break
			}

			buf.WriteString("INSERT INTO ")
			buf.WriteString(param.name)
			buf.WriteString(" VALUES ")
			rbat, err = convertValueBat2Str(requestCtx, bat, ses.mp, ses.GetTimeZone())
			if err != nil {
				return err
			}
			for i := 0; i < rbat.Length(); i++ {
				if i != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString("(")
				for j := 0; j < rbat.VectorCount(); j++ {
					if j != 0 {
						buf.WriteString(", ")
					}
					buf.WriteString(rbat.GetVector(int32(j)).GetString(int64(i)))
				}
				buf.WriteString(")")
			}
			buf.WriteString(";\n")
			f, curFileIdx, curFileSize, err = writeDump2File(requestCtx, buf, dump, f, curFileIdx, curFileSize)
			if err != nil {
				return err
			}
		}
		buf.WriteString("\n\n\n")
	}
	if !showDbDDL {
		return nil
	}
	for _, param := range params {
		if !param.isView {
			continue
		}
		_, err = buf.WriteString(param.ddl)
		if err != nil {
			return err
		}
		f, curFileIdx, curFileSize, err = writeDump2File(requestCtx, buf, dump, f, curFileIdx, curFileSize)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
handle "SELECT @@xxx.yyyy"
*/
func (mce *MysqlCmdExecutor) handleSelectVariables(ve *tree.VarExpr) error {
	var err error = nil
	ses := mce.GetSession()
	mrs := ses.GetMysqlResultSet()
	proto := ses.GetMysqlProtocol()

	col := new(MysqlColumn)
	col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col.SetName("@@" + ve.Name)
	mrs.AddColumn(col)

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

	mrs.AddRow(row)

	mer := NewMysqlExecutionResult(0, 0, 0, 0, mrs)
	resp := NewResponse(ResultResponse, 0, int(COM_QUERY), mer)

	if err := proto.SendResponse(ses.GetRequestContext(), resp); err != nil {
		return moerr.NewInternalError(ses.GetRequestContext(), "routine send response failed. error:%v ", err)
	}
	return err
}

func doCmdFieldList(requestCtx context.Context, ses *Session, icfl *InternalCmdFieldList) error {
	dbName := ses.GetDatabaseName()
	if dbName == "" {
		return moerr.NewNoDB(requestCtx)
	}

	//Get table infos for the database from the cube
	//case 1: there are no table infos for the db
	//case 2: db changed
	//NOTE: it costs too much time.
	//It just reduces the information in the auto-completion (auto-rehash) of the mysql client.
	//var attrs []ColumnInfo
	//
	//if mce.tableInfos == nil || mce.db != dbName {
	//	txnHandler := ses.GetTxnHandler()
	//	eng := ses.GetStorage()
	//	db, err := eng.Database(requestCtx, dbName, txnHandler.GetTxn())
	//	if err != nil {
	//		return err
	//	}
	//
	//	names, err := db.Relations(requestCtx)
	//	if err != nil {
	//		return err
	//	}
	//	for _, name := range names {
	//		table, err := db.Relation(requestCtx, name)
	//		if err != nil {
	//			return err
	//		}
	//
	//		defs, err := table.TableDefs(requestCtx)
	//		if err != nil {
	//			return err
	//		}
	//		for _, def := range defs {
	//			if attr, ok := def.(*engine.AttributeDef); ok {
	//				attrs = append(attrs, &engineColumnInfo{
	//					name: attr.Attr.Name,
	//					typ:  attr.Attr.Type,
	//				})
	//			}
	//		}
	//	}
	//
	//	if mce.tableInfos == nil {
	//		mce.tableInfos = make(map[string][]ColumnInfo)
	//	}
	//	mce.tableInfos[tableName] = attrs
	//}
	//
	//cols, ok := mce.tableInfos[tableName]
	//if !ok {
	//	//just give the empty info when there is no such table.
	//	attrs = make([]ColumnInfo, 0)
	//} else {
	//	attrs = cols
	//}
	//
	//for _, c := range attrs {
	//	col := new(MysqlColumn)
	//	col.SetName(c.GetName())
	//	err = convertEngineTypeToMysqlType(c.GetType(), col)
	//	if err != nil {
	//		return err
	//	}
	//
	//	/*
	//		mysql CMD_FIELD_LIST response: send the column definition per column
	//	*/
	//	err = proto.SendColumnDefinitionPacket(col, int(COM_FIELD_LIST))
	//	if err != nil {
	//		return err
	//	}
	//}
	return nil
}

/*
handle cmd CMD_FIELD_LIST
*/
func (mce *MysqlCmdExecutor) handleCmdFieldList(requestCtx context.Context, icfl *InternalCmdFieldList) error {
	var err error
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()

	err = doCmdFieldList(requestCtx, ses, icfl)
	if err != nil {
		return err
	}

	/*
		mysql CMD_FIELD_LIST response: End after the column has been sent.
		send EOF packet
	*/
	err = proto.sendEOFOrOkPacket(0, 0)
	if err != nil {
		return err
	}

	return err
}

func doSetVar(ctx context.Context, ses *Session, sv *tree.SetVar) error {
	var err error = nil
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

		value, err = GetSimpleExprValue(assign.Value, ses)
		if err != nil {
			return err
		}

		if systemVar, ok := gSysVarsDefs[name]; ok {
			if isDefault, ok := value.(bool); ok && isDefault {
				value = systemVar.Default
			}
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
	return err
}

/*
handle setvar
*/
func (mce *MysqlCmdExecutor) handleSetVar(ctx context.Context, sv *tree.SetVar) error {
	ses := mce.GetSession()
	err := doSetVar(ctx, ses, sv)
	if err != nil {
		return err
	}

	return nil
}

func doShowErrors(ses *Session) error {
	var err error

	levelCol := new(MysqlColumn)
	levelCol.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	levelCol.SetName("Level")

	CodeCol := new(MysqlColumn)
	CodeCol.SetColumnType(defines.MYSQL_TYPE_SHORT)
	CodeCol.SetName("Code")

	MsgCol := new(MysqlColumn)
	MsgCol.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	MsgCol.SetName("Message")

	mrs := ses.GetMysqlResultSet()

	mrs.AddColumn(levelCol)
	mrs.AddColumn(CodeCol)
	mrs.AddColumn(MsgCol)

	info := ses.GetErrInfo()

	for i := info.length() - 1; i >= 0; i-- {
		row := make([]interface{}, 3)
		row[0] = "Error"
		row[1] = info.codes[i]
		row[2] = info.msgs[i]
		mrs.AddRow(row)
	}

	return err
}

func (mce *MysqlCmdExecutor) handleShowErrors() error {
	var err error
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()
	err = doShowErrors(ses)
	if err != nil {
		return err
	}

	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := NewResponse(ResultResponse, 0, int(COM_QUERY), mer)

	if err := proto.SendResponse(ses.requestCtx, resp); err != nil {
		return moerr.NewInternalError(ses.requestCtx, "routine send response failed. error:%v ", err)
	}
	return err
}

func doShowVariables(ses *Session, proc *process.Process, sv *tree.ShowVariables) error {
	if sv.Like != nil && sv.Where != nil {
		return moerr.NewSyntaxError(ses.GetRequestContext(), "like clause and where clause cannot exist at the same time")
	}

	var err error = nil

	col1 := new(MysqlColumn)
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col1.SetName("Variable_name")

	col2 := new(MysqlColumn)
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col2.SetName("Value")

	mrs := ses.GetMysqlResultSet()
	mrs.AddColumn(col1)
	mrs.AddColumn(col2)

	var hasLike = false
	var likePattern = ""
	var isIlike = false
	if sv.Like != nil {
		hasLike = true
		if sv.Like.Op == tree.ILIKE {
			isIlike = true
		}
		likePattern = strings.ToLower(sv.Like.Right.String())
	}

	var sysVars map[string]interface{}
	if sv.Global {
		sysVars = make(map[string]interface{})
		for k, v := range gSysVarsDefs {
			sysVars[k] = v.Default
		}
	} else {
		sysVars = ses.CopyAllSessionVars()
	}

	rows := make([][]interface{}, 0, len(sysVars))
	for name, value := range sysVars {
		if hasLike {
			s := name
			if isIlike {
				s = strings.ToLower(s)
			}
			if !WildcardMatch(likePattern, s) {
				continue
			}
		}
		row := make([]interface{}, 2)
		row[0] = name
		gsv, ok := GSysVariables.GetDefinitionOfSysVar(name)
		if !ok {
			return moerr.NewInternalError(ses.GetRequestContext(), errorSystemVariableDoesNotExist())
		}
		row[1] = value
		if _, ok := gsv.GetType().(SystemVariableBoolType); ok {
			v, ok := value.(int8)
			if ok {
				if v == 1 {
					row[1] = "on"
				} else {
					row[1] = "off"
				}
			}
		}
		rows = append(rows, row)
	}

	if sv.Where != nil {
		bat, err := constructVarBatch(ses, rows)
		if err != nil {
			return err
		}
		binder := plan2.NewDefaultBinder(proc.Ctx, nil, nil, &plan2.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}, []string{"variable_name", "value"})
		planExpr, err := binder.BindExpr(sv.Where.Expr, 0, false)
		if err != nil {
			return err
		}

		vec, err := colexec.EvalExpr(bat, proc, planExpr)
		if err != nil {
			return err
		}
		bs := vector.GetColumn[bool](vec)
		sels := proc.Mp().GetSels()
		for i, b := range bs {
			if b {
				sels = append(sels, int64(i))
			}
		}
		bat.Shrink(sels)
		proc.Mp().PutSels(sels)
		v0 := vector.MustStrCols(bat.Vecs[0])
		v1 := vector.MustStrCols(bat.Vecs[1])
		rows = rows[:len(v0)]
		for i := range v0 {
			rows[i][0] = v0[i]
			rows[i][1] = v1[i]
		}
		bat.Clean(proc.Mp())
	}

	//sort by name
	sort.Slice(rows, func(i, j int) bool {
		return rows[i][0].(string) < rows[j][0].(string)
	})

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return err
}

/*
handle show variables
*/
func (mce *MysqlCmdExecutor) handleShowVariables(sv *tree.ShowVariables, proc *process.Process) error {
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()
	err := doShowVariables(ses, proc, sv)
	if err != nil {
		return err
	}
	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := NewResponse(ResultResponse, 0, int(COM_QUERY), mer)

	if err := proto.SendResponse(ses.requestCtx, resp); err != nil {
		return moerr.NewInternalError(ses.requestCtx, "routine send response failed. error:%v ", err)
	}
	return err
}

func constructVarBatch(ses *Session, rows [][]interface{}) (*batch.Batch, error) {
	bat := batch.New(true, []string{"Variable_name", "Value"})
	typ := types.New(types.T_varchar, types.MaxVarcharLen, 0, 0)
	cnt := len(rows)
	bat.Zs = make([]int64, cnt)
	for i := range bat.Zs {
		bat.Zs[i] = 1
	}
	v0 := make([]string, cnt)
	v1 := make([]string, cnt)
	for i, row := range rows {
		v0[i] = row[0].(string)
		v1[i] = fmt.Sprintf("%v", row[1])
	}
	bat.Vecs[0] = vector.NewWithStrings(typ, v0, nil, ses.GetMemPool())
	bat.Vecs[1] = vector.NewWithStrings(typ, v1, nil, ses.GetMemPool())
	return bat, nil
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
	return mce.GetDoQueryFunc()(requestCtx, sql)
}

// Note: for pass the compile quickly. We will remove the comments in the future.
func (mce *MysqlCmdExecutor) handleExplainStmt(requestCtx context.Context, stmt *tree.ExplainStmt) error {
	es, err := getExplainOption(requestCtx, stmt.Options)
	if err != nil {
		return err
	}

	ses := mce.GetSession()

	switch stmt.Statement.(type) {
	case *tree.Delete:
		ses.GetTxnCompileCtx().SetQueryType(TXN_DELETE)
	case *tree.Update:
		ses.GetTxnCompileCtx().SetQueryType(TXN_UPDATE)
	default:
		ses.GetTxnCompileCtx().SetQueryType(TXN_DEFAULT)
	}

	//get query optimizer and execute Optimize
	plan, err := buildPlan(requestCtx, ses, ses.GetTxnCompileCtx(), stmt.Statement)
	if err != nil {
		return err
	}
	if plan.GetQuery() == nil {
		return moerr.NewNotSupported(requestCtx, "the sql query plan does not support explain.")
	}
	// generator query explain
	explainQuery := explain.NewExplainQueryImpl(plan.GetQuery())

	// build explain data buffer
	buffer := explain.NewExplainDataBuffer()
	err = explainQuery.ExplainPlan(requestCtx, buffer, es)
	if err != nil {
		return err
	}

	protocol := ses.GetMysqlProtocol()

	explainColName := "QUERY PLAN"
	columns, err := GetExplainColumns(requestCtx, explainColName)
	if err != nil {
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
	cmd := ses.GetCmd()
	mrs := ses.GetMysqlResultSet()
	for _, c := range columns {
		mysqlc := c.(Column)
		mrs.AddColumn(mysqlc)
		//	mysql COM_QUERY response: send the column definition per column
		err := protocol.SendColumnDefinitionPacket(requestCtx, mysqlc, int(cmd))
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

	err = buildMoExplainQuery(explainColName, buffer, ses, getDataFromPipeline)
	if err != nil {
		return err
	}

	err = protocol.sendEOFOrOkPacket(0, 0)
	if err != nil {
		return err
	}
	return nil
}

func doPrepareStmt(ctx context.Context, ses *Session, st *tree.PrepareStmt) (*PrepareStmt, error) {
	switch st.Stmt.(type) {
	case *tree.Update:
		ses.GetTxnCompileCtx().SetQueryType(TXN_UPDATE)
	case *tree.Delete:
		ses.GetTxnCompileCtx().SetQueryType(TXN_DELETE)
	}
	preparePlan, err := buildPlan(ctx, ses, ses.GetTxnCompileCtx(), st)
	if err != nil {
		return nil, err
	}

	prepareStmt := &PrepareStmt{
		Name:        preparePlan.GetDcl().GetPrepare().GetName(),
		PreparePlan: preparePlan,
		PrepareStmt: st.Stmt,
	}

	err = ses.SetPrepareStmt(preparePlan.GetDcl().GetPrepare().GetName(), prepareStmt)
	return prepareStmt, err
}

// handlePrepareStmt
func (mce *MysqlCmdExecutor) handlePrepareStmt(ctx context.Context, st *tree.PrepareStmt) (*PrepareStmt, error) {
	return doPrepareStmt(ctx, mce.GetSession(), st)
}

func doPrepareString(ctx context.Context, ses *Session, st *tree.PrepareString) (*PrepareStmt, error) {
	stmts, err := mysql.Parse(ctx, st.Sql)
	if err != nil {
		return nil, err
	}
	switch stmts[0].(type) {
	case *tree.Update:
		ses.GetTxnCompileCtx().SetQueryType(TXN_UPDATE)
	case *tree.Delete:
		ses.GetTxnCompileCtx().SetQueryType(TXN_DELETE)
	}

	preparePlan, err := buildPlan(ses.GetRequestContext(), ses, ses.GetTxnCompileCtx(), st)
	if err != nil {
		return nil, err
	}

	prepareStmt := &PrepareStmt{
		Name:        preparePlan.GetDcl().GetPrepare().GetName(),
		PreparePlan: preparePlan,
		PrepareStmt: stmts[0],
	}

	err = ses.SetPrepareStmt(preparePlan.GetDcl().GetPrepare().GetName(), prepareStmt)
	return prepareStmt, err
}

// handlePrepareString
func (mce *MysqlCmdExecutor) handlePrepareString(ctx context.Context, st *tree.PrepareString) (*PrepareStmt, error) {
	return doPrepareString(ctx, mce.GetSession(), st)
}

func doDeallocate(ctx context.Context, ses *Session, st *tree.Deallocate) error {
	deallocatePlan, err := buildPlan(ctx, ses, ses.GetTxnCompileCtx(), st)
	if err != nil {
		return err
	}
	ses.RemovePrepareStmt(deallocatePlan.GetDcl().GetDeallocate().GetName())
	return nil
}

func doReset(ctx context.Context, ses *Session, st *tree.Reset) error {
	return nil
}

// handleDeallocate
func (mce *MysqlCmdExecutor) handleDeallocate(ctx context.Context, st *tree.Deallocate) error {
	return doDeallocate(ctx, mce.GetSession(), st)
}

// handleReset
func (mce *MysqlCmdExecutor) handleReset(ctx context.Context, st *tree.Reset) error {
	return doReset(ctx, mce.GetSession(), st)
}

// handleCreateAccount creates a new user-level tenant in the context of the tenant SYS
// which has been initialized.
func (mce *MysqlCmdExecutor) handleCreateAccount(ctx context.Context, ca *tree.CreateAccount) error {
	//step1 : create new account.
	return InitGeneralTenant(ctx, mce.GetSession(), ca)
}

// handleDropAccount drops a new user-level tenant
func (mce *MysqlCmdExecutor) handleDropAccount(ctx context.Context, da *tree.DropAccount) error {
	return doDropAccount(ctx, mce.GetSession(), da)
}

// handleDropAccount drops a new user-level tenant
func (mce *MysqlCmdExecutor) handleAlterAccount(ctx context.Context, aa *tree.AlterAccount) error {
	return doAlterAccount(ctx, mce.GetSession(), aa)
}

// handleAlterDatabaseConfig alter a database's mysql_compatbility_mode
func (mce *MysqlCmdExecutor) handleAlterDataBaseConfig(ctx context.Context, ad *tree.AlterDataBaseConfig) error {
	return doAlterDatabaseConfig(ctx, mce.GetSession(), ad)
}

// handleAlterAccountConfig alter a account's mysql_compatbility_mode
func (mce *MysqlCmdExecutor) handleAlterAccountConfig(ctx context.Context, st *tree.AlterDataBaseConfig) error {
	return doAlterAccountConfig(ctx, mce.GetSession(), st)
}

// handleCreateUser creates the user for the tenant
func (mce *MysqlCmdExecutor) handleCreateUser(ctx context.Context, cu *tree.CreateUser) error {
	ses := mce.GetSession()
	tenant := ses.GetTenantInfo()

	//step1 : create the user
	return InitUser(ctx, ses, tenant, cu)
}

// handleDropUser drops the user for the tenant
func (mce *MysqlCmdExecutor) handleDropUser(ctx context.Context, du *tree.DropUser) error {
	return doDropUser(ctx, mce.GetSession(), du)
}

// handleCreateRole creates the new role
func (mce *MysqlCmdExecutor) handleCreateRole(ctx context.Context, cr *tree.CreateRole) error {
	ses := mce.GetSession()
	tenant := ses.GetTenantInfo()

	//step1 : create the role
	return InitRole(ctx, ses, tenant, cr)
}

// handleDropRole drops the role
func (mce *MysqlCmdExecutor) handleDropRole(ctx context.Context, dr *tree.DropRole) error {
	return doDropRole(ctx, mce.GetSession(), dr)
}

func (mce *MysqlCmdExecutor) handleCreateFunction(ctx context.Context, cf *tree.CreateFunction) error {
	ses := mce.GetSession()
	tenant := ses.GetTenantInfo()

	return InitFunction(ctx, ses, tenant, cf)
}

func (mce *MysqlCmdExecutor) handleDropFunction(ctx context.Context, df *tree.DropFunction) error {
	return doDropFunction(ctx, mce.GetSession(), df)
}

// handleGrantRole grants the role
func (mce *MysqlCmdExecutor) handleGrantRole(ctx context.Context, gr *tree.GrantRole) error {
	return doGrantRole(ctx, mce.GetSession(), gr)
}

// handleRevokeRole revokes the role
func (mce *MysqlCmdExecutor) handleRevokeRole(ctx context.Context, rr *tree.RevokeRole) error {
	return doRevokeRole(ctx, mce.GetSession(), rr)
}

// handleGrantRole grants the privilege to the role
func (mce *MysqlCmdExecutor) handleGrantPrivilege(ctx context.Context, gp *tree.GrantPrivilege) error {
	return doGrantPrivilege(ctx, mce.GetSession(), gp)
}

// handleRevokePrivilege revokes the privilege from the user or role
func (mce *MysqlCmdExecutor) handleRevokePrivilege(ctx context.Context, rp *tree.RevokePrivilege) error {
	return doRevokePrivilege(ctx, mce.GetSession(), rp)
}

// handleSwitchRole switches the role to another role
func (mce *MysqlCmdExecutor) handleSwitchRole(ctx context.Context, sr *tree.SetRole) error {
	return doSwitchRole(ctx, mce.GetSession(), sr)
}

func doKill(ctx context.Context, rm *RoutineManager, ses *Session, k *tree.Kill) error {
	var err error
	//true: kill a connection
	//false: kill a query in a connection
	idThatKill := uint64(ses.GetConnectionID())
	if !k.Option.Exist || k.Option.Typ == tree.KillTypeConnection {
		err = rm.kill(ctx, true, idThatKill, k.ConnectionId, "")
	} else {
		err = rm.kill(ctx, false, idThatKill, k.ConnectionId, k.StmtOption.StatementId)
	}
	return err
}

// handleKill kill a connection or query
func (mce *MysqlCmdExecutor) handleKill(ctx context.Context, k *tree.Kill) error {
	var err error
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()
	err = doKill(ctx, mce.GetRoutineManager(), ses, k)
	if err != nil {
		return err
	}
	resp := NewGeneralOkResponse(COM_QUERY)
	if err = proto.SendResponse(ctx, resp); err != nil {
		return moerr.NewInternalError(ctx, "routine send response failed. error:%v ", err)
	}
	return err
}

// handleShowAccounts lists the info of accounts
func (mce *MysqlCmdExecutor) handleShowAccounts(ctx context.Context, sa *tree.ShowAccounts) error {
	var err error
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()
	err = doShowAccounts(ctx, ses, sa)
	if err != nil {
		return err
	}
	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := NewResponse(ResultResponse, 0, int(COM_QUERY), mer)

	if err = proto.SendResponse(ctx, resp); err != nil {
		return moerr.NewInternalError(ctx, "routine send response failed. error:%v ", err)
	}
	return err
}

func GetExplainColumns(ctx context.Context, explainColName string) ([]interface{}, error) {
	cols := []*plan2.ColDef{
		{Typ: &plan2.Type{Id: int32(types.T_varchar)}, Name: explainColName},
	}
	columns := make([]interface{}, len(cols))
	var err error = nil
	for i, col := range cols {
		c := new(MysqlColumn)
		c.SetName(col.Name)
		err = convertEngineTypeToMysqlType(ctx, types.T(col.Typ.Id), c)
		if err != nil {
			return nil, err
		}
		columns[i] = c
	}
	return columns, err
}

func getExplainOption(requestCtx context.Context, options []tree.OptionElem) (*explain.ExplainOptions, error) {
	es := explain.NewExplainDefaultOptions()
	if options == nil {
		return es, nil
	} else {
		for _, v := range options {
			if strings.EqualFold(v.Name, "VERBOSE") {
				if strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL" {
					es.Verbose = true
				} else if strings.EqualFold(v.Value, "FALSE") {
					es.Verbose = false
				} else {
					return nil, moerr.NewInvalidInput(requestCtx, "invalid explain option '%s', valud '%s'", v.Name, v.Value)
				}
			} else if strings.EqualFold(v.Name, "ANALYZE") {
				if strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL" {
					es.Analyze = true
				} else if strings.EqualFold(v.Value, "FALSE") {
					es.Analyze = false
				} else {
					return nil, moerr.NewInvalidInput(requestCtx, "invalid explain option '%s', valud '%s'", v.Name, v.Value)
				}
			} else if strings.EqualFold(v.Name, "FORMAT") {
				if strings.EqualFold(v.Value, "TEXT") {
					es.Format = explain.EXPLAIN_FORMAT_TEXT
				} else if strings.EqualFold(v.Value, "JSON") {
					return nil, moerr.NewNotSupported(requestCtx, "Unsupport explain format '%s'", v.Value)
				} else if strings.EqualFold(v.Value, "DOT") {
					return nil, moerr.NewNotSupported(requestCtx, "Unsupport explain format '%s'", v.Value)
				} else {
					return nil, moerr.NewInvalidInput(requestCtx, "invalid explain option '%s', valud '%s'", v.Name, v.Value)
				}
			} else {
				return nil, moerr.NewInvalidInput(requestCtx, "invalid explain option '%s', valud '%s'", v.Name, v.Value)
			}
		}
		return es, nil
	}
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
	vec := vector.NewWithBytes(types.T_varchar.ToType(), vs, nil, session.GetMemPool())
	bat.Vecs[0] = vec
	bat.InitZsOne(count)

	err := fill(session, bat)
	vec.Free(session.GetMemPool())
	return err
}

var _ ComputationWrapper = &TxnComputationWrapper{}
var _ ComputationWrapper = &NullComputationWrapper{}

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

func (cwft *TxnComputationWrapper) GetProcess() *process.Process {
	return cwft.proc
}

func (cwft *TxnComputationWrapper) SetDatabaseName(db string) error {
	return nil
}

func (cwft *TxnComputationWrapper) GetColumns() ([]interface{}, error) {
	var err error
	cols := plan2.GetResultColumnsFromPlan(cwft.plan)
	switch cwft.GetAst().(type) {
	case *tree.ShowColumns:
		if len(cols) == 7 {
			cols = []*plan2.ColDef{
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Field"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Type"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Null"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Key"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Default"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Extra"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Comment"},
			}
		} else {
			cols = []*plan2.ColDef{
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Field"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Type"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Collation"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Null"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Key"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Default"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Extra"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Privileges"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Comment"},
			}
		}
	}
	columns := make([]interface{}, len(cols))
	for i, col := range cols {
		c := new(MysqlColumn)
		c.SetName(col.Name)
		c.SetOrgName(col.Name)
		c.SetTable(col.Typ.Table)
		c.SetOrgTable(col.Typ.Table)
		c.SetAutoIncr(col.Typ.AutoIncr)
		c.SetSchema(cwft.ses.GetTxnCompileCtx().DefaultDatabase())
		err = convertEngineTypeToMysqlType(cwft.ses.requestCtx, types.T(col.Typ.Id), c)
		if err != nil {
			return nil, err
		}
		setColFlag(c)
		setColLength(c, col.Typ.Width)
		setCharacter(c)
		c.SetDecimal(uint8(col.Typ.Scale))
		convertMysqlTextTypeToBlobType(c)
		columns[i] = c
	}
	return columns, err
}

func (cwft *TxnComputationWrapper) GetClock() clock.Clock {
	rt := runtime.ProcessLevelRuntime()
	return rt.Clock()
}

func (cwft *TxnComputationWrapper) GetAffectedRows() uint64 {
	return cwft.compile.GetAffectedRows()
}

func (cwft *TxnComputationWrapper) Compile(requestCtx context.Context, u interface{}, fill func(interface{}, *batch.Batch) error) (interface{}, error) {
	var err error
	defer RecordStatementTxnID(requestCtx, cwft.ses)
	if cwft.ses.IfInitedTempEngine() {
		requestCtx = context.WithValue(requestCtx, defines.TemporaryDN{}, cwft.ses.GetTempTableStorage())
		cwft.ses.SetRequestContext(requestCtx)
		cwft.proc.Ctx = context.WithValue(cwft.proc.Ctx, defines.TemporaryDN{}, cwft.ses.GetTempTableStorage())
	}
	cacheHit := cwft.plan != nil
	if !cacheHit {
		cwft.plan, err = buildPlan(requestCtx, cwft.ses, cwft.ses.GetTxnCompileCtx(), cwft.stmt)
	} else if cwft.ses != nil && cwft.ses.GetTenantInfo() != nil {
		cwft.ses.accountId = getAccountId(requestCtx)
		err = authenticateCanExecuteStatementAndPlan(requestCtx, cwft.ses, cwft.stmt, cwft.plan)
	}
	if err != nil {
		return nil, err
	}
	cwft.ses.p = cwft.plan
	if ids := isResultQuery(cwft.plan); ids != nil {
		if err = checkPrivilege(ids, requestCtx, cwft.ses); err != nil {
			return nil, err
		}
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
		// 		return nil, moerr.NewInternalError("", fmt.Sprintf(ctx, "table '%s' has been changed, please reset Prepare statement '%s'", obj.ObjName, stmtName))
		// 	}
		// }

		preparePlan := prepareStmt.PreparePlan.GetDcl().GetPrepare()
		if len(executePlan.Args) != len(preparePlan.ParamTypes) {
			return nil, moerr.NewInvalidInput(requestCtx, "Incorrect arguments to EXECUTE")
		}
		newPlan := plan2.DeepCopyPlan(preparePlan.Plan)

		// replace ? and @var with their values
		resetParamRule := plan2.NewResetParamRefRule(requestCtx, executePlan.Args)
		resetVarRule := plan2.NewResetVarRefRule(cwft.ses.GetTxnCompileCtx(), cwft.ses.GetTxnCompileCtx().GetProcess())
		constantFoldRule := plan2.NewConstantFoldRule(cwft.ses.GetTxnCompileCtx())
		vp := plan2.NewVisitPlan(newPlan, []plan2.VisitPlanRule{resetParamRule, resetVarRule, constantFoldRule})
		err = vp.Visit(requestCtx)
		if err != nil {
			return nil, err
		}

		// reset plan & stmt
		cwft.stmt = prepareStmt.PrepareStmt
		cwft.plan = newPlan
		// reset some special stmt for execute statement
		switch cwft.stmt.(type) {
		case *tree.ShowTableStatus:
			cwft.ses.showStmtType = ShowTableStatus
			cwft.ses.SetData(nil)
		case *tree.SetVar, *tree.ShowVariables, *tree.ShowErrors, *tree.ShowWarnings:
			return nil, nil
		}

		//check privilege
		err = authenticateUserCanExecutePrepareOrExecute(requestCtx, cwft.ses, prepareStmt.PrepareStmt, newPlan)
		if err != nil {
			return nil, err
		}
	} else {
		var vp *plan2.VisitPlan
		if cacheHit {
			vp = plan2.NewVisitPlan(cwft.plan, []plan2.VisitPlanRule{plan2.NewResetVarRefRule(cwft.ses.GetTxnCompileCtx(), cwft.ses.GetTxnCompileCtx().GetProcess()), plan2.NewRecomputeRealTimeRelatedFuncRule(cwft.ses.GetTxnCompileCtx().GetProcess())})
		} else {
			vp = plan2.NewVisitPlan(cwft.plan, []plan2.VisitPlanRule{plan2.NewResetVarRefRule(cwft.ses.GetTxnCompileCtx(), cwft.ses.GetTxnCompileCtx().GetProcess())})
		}
		err = vp.Visit(requestCtx)
		if err != nil {
			return nil, err
		}
	}

	txnHandler := cwft.ses.GetTxnHandler()
	if cacheHit && cwft.plan.NeedImplicitTxn() {
		cwft.proc.TxnOperator, err = txnHandler.GetTxn()
		if err != nil {
			return nil, err
		}
	} else if cwft.plan.GetQuery().GetLoadTag() {
		cwft.proc.TxnOperator = txnHandler.GetTxnOnly()
	} else if cwft.plan.NeedImplicitTxn() {
		cwft.proc.TxnOperator, err = txnHandler.GetTxn()
		if err != nil {
			return nil, err
		}
	}
	addr := ""
	if len(cwft.ses.GetParameterUnit().ClusterNodes) > 0 {
		addr = cwft.ses.GetParameterUnit().ClusterNodes[0].Addr
	}
	cwft.proc.FileService = cwft.ses.GetParameterUnit().FileService
	cwft.compile = compile.New(addr, cwft.ses.GetDatabaseName(), cwft.ses.GetSql(), cwft.ses.GetUserName(), requestCtx, cwft.ses.GetStorage(), cwft.proc, cwft.stmt)

	if _, ok := cwft.stmt.(*tree.ExplainAnalyze); ok {
		fill = func(obj interface{}, bat *batch.Batch) error { return nil }
	}
	err = cwft.compile.Compile(requestCtx, cwft.plan, cwft.ses, fill)
	if err != nil {
		return nil, err
	}
	// check if it is necessary to initialize the temporary engine
	if cwft.compile.NeedInitTempEngine(cwft.ses.IfInitedTempEngine()) {
		// 0. init memory-non-dist storage
		dnStore, err := cwft.ses.SetTempTableStorage(cwft.GetClock())
		if err != nil {
			return nil, err
		}

		// temporary storage is passed through Ctx
		requestCtx = context.WithValue(requestCtx, defines.TemporaryDN{}, cwft.ses.GetTempTableStorage())

		// 1. init memory-non-dist engine
		tempEngine := memoryengine.New(
			requestCtx,
			memoryengine.NewDefaultShardPolicy(
				mpool.MustNewZero(),
			),
			memoryengine.RandomIDGenerator,
			clusterservice.NewMOCluster(
				nil,
				0,
				clusterservice.WithDisableRefresh(),
				clusterservice.WithServices(nil, []metadata.DNService{
					*dnStore,
				})),
		)

		// 2. bind the temporary engine to the session and txnHandler
		cwft.ses.SetTempEngine(requestCtx, tempEngine)
		cwft.compile.SetTempEngine(requestCtx, tempEngine)
		txnHandler := cwft.ses.txnCompileCtx.txnHandler
		txnHandler.SetTempEngine(tempEngine)

		// 3. init temp-db to store temporary relations
		err = tempEngine.Create(requestCtx, defines.TEMPORARY_DBNAME, cwft.ses.txnHandler.txn)
		if err != nil {
			return nil, err
		}

		// 4. add auto_IncrementTable fortemp-db
		colexec.CreateAutoIncrTable(cwft.ses.GetStorage(), requestCtx, cwft.proc, defines.TEMPORARY_DBNAME)

		cwft.ses.InitTempEngine = true
	}
	return cwft.compile, err
}

func (cwft *TxnComputationWrapper) RecordExecPlan(ctx context.Context) error {
	if stm := motrace.StatementFromContext(ctx); stm != nil {
		stm.SetExecPlan(cwft.plan, SerializeExecPlan)
	}
	return nil
}

func (cwft *TxnComputationWrapper) GetUUID() []byte {
	return cwft.uuid[:]
}

func (cwft *TxnComputationWrapper) Run(ts uint64) error {
	logDebugf(cwft.ses.GetConciseProfile(), "compile.Run begin")
	defer func() {
		logDebugf(cwft.ses.GetConciseProfile(), "compile.Run end")
	}()
	err := cwft.compile.Run(ts)
	return err
}

func (cwft *TxnComputationWrapper) GetLoadTag() bool {
	return cwft.plan.GetQuery().GetLoadTag()
}

type NullComputationWrapper struct {
	*TxnComputationWrapper
}

func InitNullComputationWrapper(ses *Session, stmt tree.Statement, proc *process.Process) *NullComputationWrapper {
	return &NullComputationWrapper{
		TxnComputationWrapper: InitTxnComputationWrapper(ses, stmt, proc),
	}
}

func (ncw *NullComputationWrapper) GetAst() tree.Statement {
	return ncw.stmt
}

func (ncw *NullComputationWrapper) SetDatabaseName(db string) error {
	return nil
}

func (ncw *NullComputationWrapper) GetColumns() ([]interface{}, error) {
	return []interface{}{}, nil
}

func (ncw *NullComputationWrapper) GetAffectedRows() uint64 {
	return 0
}

func (ncw *NullComputationWrapper) Compile(requestCtx context.Context, u interface{}, fill func(interface{}, *batch.Batch) error) (interface{}, error) {
	return nil, nil
}

func (ncw *NullComputationWrapper) RecordExecPlan(ctx context.Context) error {
	return nil
}

func (ncw *NullComputationWrapper) GetUUID() []byte {
	return ncw.uuid[:]
}

func (ncw *NullComputationWrapper) Run(ts uint64) error {
	return nil
}

func (ncw *NullComputationWrapper) GetLoadTag() bool {
	return false
}

func buildPlan(requestCtx context.Context, ses *Session, ctx plan2.CompilerContext, stmt tree.Statement) (*plan2.Plan, error) {
	var ret *plan2.Plan
	var err error
	if ses != nil {
		ses.accountId = getAccountId(requestCtx)
	}
	if s, ok := stmt.(*tree.Insert); ok {
		if _, ok := s.Rows.Select.(*tree.ValuesClause); ok {
			ret, err = plan2.BuildPlan(ctx, stmt)
			if err != nil {
				return nil, err
			}
		}
	}
	if ret != nil {
		if ses != nil && ses.GetTenantInfo() != nil {
			err = authenticateCanExecuteStatementAndPlan(requestCtx, ses, stmt, ret)
			if err != nil {
				return nil, err
			}
		}
		return ret, err
	}
	switch stmt := stmt.(type) {
	case *tree.Select, *tree.ParenSelect, *tree.ValuesStatement,
		*tree.Update, *tree.Delete, *tree.Insert,
		*tree.ShowDatabases, *tree.ShowTables, *tree.ShowColumns, *tree.ShowColumnNumber, *tree.ShowTableNumber,
		*tree.ShowCreateDatabase, *tree.ShowCreateTable, *tree.ShowIndex,
		*tree.ExplainStmt, *tree.ExplainAnalyze:
		opt := plan2.NewBaseOptimizer(ctx)
		optimized, err := opt.Optimize(stmt)
		if err != nil {
			return nil, err
		}
		ret = &plan2.Plan{
			Plan: &plan2.Plan_Query{
				Query: optimized,
			},
		}
	default:
		ret, err = plan2.BuildPlan(ctx, stmt)
	}
	if ret != nil {
		if ses != nil && ses.GetTenantInfo() != nil {
			err = authenticateCanExecuteStatementAndPlan(requestCtx, ses, stmt, ret)
			if err != nil {
				return nil, err
			}
		}
	}
	return ret, err
}

/*
GetComputationWrapper gets the execs from the computation engine
*/
var GetComputationWrapper = func(db, sql, user string, eng engine.Engine, proc *process.Process, ses *Session) ([]ComputationWrapper, error) {
	var cw []ComputationWrapper = nil
	if cached := ses.getCachedPlan(sql); cached != nil {
		for i, stmt := range cached.stmts {
			tcw := InitTxnComputationWrapper(ses, stmt, proc)
			tcw.plan = cached.plans[i]
			cw = append(cw, tcw)
		}
		return cw, nil
	}

	var stmts []tree.Statement = nil
	var cmdFieldStmt *InternalCmdFieldList
	var err error
	if isCmdFieldListSql(sql) {
		cmdFieldStmt, err = parseCmdFieldList(proc.Ctx, sql)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, cmdFieldStmt)
	} else {
		stmts, err = parsers.Parse(proc.Ctx, dialect.MYSQL, sql)
		if err != nil {
			return nil, err
		}
	}

	for _, stmt := range stmts {
		cw = append(cw, InitTxnComputationWrapper(ses, stmt, proc))
	}
	return cw, nil
}

func getStmtExecutor(ses *Session, proc *process.Process, base *baseStmtExecutor, stmt tree.Statement) (StmtExecutor, error) {
	var err error
	var ret StmtExecutor
	switch st := stmt.(type) {
	//PART 1: the statements with the result set
	case *tree.Select:
		ret = (&SelectExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			sel: st,
		})
	case *tree.ValuesStatement:
		ret = (&ValuesStmtExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			sel: st,
		})
	case *tree.ShowCreateTable:
		ret = (&ShowCreateTableExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			sct: st,
		})
	case *tree.ShowCreateDatabase:
		ret = (&ShowCreateDatabaseExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			scd: st,
		})
	case *tree.ShowTables:
		ret = (&ShowTablesExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			st: st,
		})
	case *tree.ShowDatabases:
		ret = (&ShowDatabasesExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			sd: st,
		})
	case *tree.ShowColumns:
		ret = (&ShowColumnsExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			sc: st,
		})
	case *tree.ShowProcessList:
		ret = (&ShowProcessListExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			spl: st,
		})
	case *tree.ShowStatus:
		ret = (&ShowStatusExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			ss: st,
		})
	case *tree.ShowTableStatus:
		ret = (&ShowTableStatusExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			sts: st,
		})
	case *tree.ShowGrants:
		ret = (&ShowGrantsExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			sg: st,
		})
	case *tree.ShowIndex:
		ret = (&ShowIndexExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			si: st,
		})
	case *tree.ShowCreateView:
		ret = (&ShowCreateViewExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			scv: st,
		})
	case *tree.ShowTarget:
		ret = (&ShowTargetExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			st: st,
		})
	case *tree.ExplainFor:
		ret = (&ExplainForExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			ef: st,
		})
	case *tree.ExplainStmt:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&ExplainStmtExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			es: st,
		})
	case *tree.ShowVariables:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&ShowVariablesExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			sv: st,
		})
	case *tree.ShowErrors:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&ShowErrorsExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			se: st,
		})
	case *tree.ShowWarnings:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&ShowWarningsExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			sw: st,
		})
	case *tree.AnalyzeStmt:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&AnalyzeStmtExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			as: st,
		})
	case *tree.ExplainAnalyze:
		ret = (&ExplainAnalyzeExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			ea: st,
		})
	case *InternalCmdFieldList:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&InternalCmdFieldListExecutor{
			resultSetStmtExecutor: &resultSetStmtExecutor{
				base,
			},
			icfl: st,
		})
	//PART 2: the statement with the status only
	case *tree.BeginTransaction:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&BeginTxnExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			bt: st,
		})
	case *tree.CommitTransaction:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&CommitTxnExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			ct: st,
		})
	case *tree.RollbackTransaction:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&RollbackTxnExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			rt: st,
		})
	case *tree.SetRole:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&SetRoleExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			sr: st,
		})
	case *tree.Use:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&UseExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			u: st,
		})
	case *tree.MoDump:
		//TODO:
		err = moerr.NewInternalError(proc.Ctx, "needs to add modump")
	case *tree.DropDatabase:
		ret = (&DropDatabaseExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			dd: st,
		})
	case *tree.PrepareStmt:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&PrepareStmtExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			ps: st,
		})
	case *tree.PrepareString:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&PrepareStringExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			ps: st,
		})
	case *tree.Deallocate:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&DeallocateExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			d: st,
		})
	case *tree.SetVar:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&SetVarExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			sv: st,
		})
	case *tree.Delete:
		ret = (&DeleteExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			d: st,
		})
	case *tree.Update:
		ret = (&UpdateExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			u: st,
		})
	case *tree.CreateAccount:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&CreateAccountExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			ca: st,
		})
	case *tree.DropAccount:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&DropAccountExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			da: st,
		})
	case *tree.AlterAccount:
		ret = (&AlterAccountExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			aa: st,
		})
	case *tree.CreateUser:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&CreateUserExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			cu: st,
		})
	case *tree.DropUser:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&DropUserExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			du: st,
		})
	case *tree.AlterUser:
		ret = (&AlterUserExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			au: st,
		})
	case *tree.CreateRole:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&CreateRoleExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			cr: st,
		})
	case *tree.DropRole:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&DropRoleExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			dr: st,
		})
	case *tree.Grant:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&GrantExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			g: st,
		})
	case *tree.Revoke:
		base.ComputationWrapper = InitNullComputationWrapper(ses, st, proc)
		ret = (&RevokeExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			r: st,
		})
	case *tree.CreateTable:
		ret = (&CreateTableExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			ct: st,
		})
	case *tree.DropTable:
		ret = (&DropTableExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			dt: st,
		})
	case *tree.CreateDatabase:
		ret = (&CreateDatabaseExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			cd: st,
		})
	case *tree.CreateIndex:
		ret = (&CreateIndexExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			ci: st,
		})
	case *tree.DropIndex:
		ret = (&DropIndexExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			di: st,
		})
	case *tree.CreateView:
		ret = (&CreateViewExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			cv: st,
		})
	case *tree.AlterView:
		ret = (&AlterViewExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			av: st,
		})
	case *tree.DropView:
		ret = (&DropViewExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			dv: st,
		})
	case *tree.Insert:
		ret = (&InsertExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			i: st,
		})
	case *tree.Load:
		ret = (&LoadExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			l: st,
		})
	case *tree.SetDefaultRole:
		ret = (&SetDefaultRoleExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			sdr: st,
		})
	case *tree.SetPassword:
		ret = (&SetPasswordExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			sp: st,
		})
	case *tree.TruncateTable:
		ret = (&TruncateTableExecutor{
			statusStmtExecutor: &statusStmtExecutor{
				base,
			},
			tt: st,
		})
	//PART 3: hybrid
	case *tree.Execute:
		ret = &ExecuteExecutor{
			baseStmtExecutor: base,
			e:                st,
		}
	default:
		return nil, moerr.NewInternalError(proc.Ctx, "no such statement %s", stmt.String())
	}
	return ret, err
}

var GetStmtExecList = func(db, sql, user string, eng engine.Engine, proc *process.Process, ses *Session) ([]StmtExecutor, error) {
	var stmtExecList []StmtExecutor = nil
	var stmtExec StmtExecutor
	var stmts []tree.Statement = nil
	var cmdFieldStmt *InternalCmdFieldList
	var err error

	appendStmtExec := func(se StmtExecutor) {
		stmtExecList = append(stmtExecList, se)
	}

	if isCmdFieldListSql(sql) {
		cmdFieldStmt, err = parseCmdFieldList(proc.Ctx, sql)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, cmdFieldStmt)
	} else {
		stmts, err = parsers.Parse(proc.Ctx, dialect.MYSQL, sql)
		if err != nil {
			return nil, err
		}
	}

	for _, stmt := range stmts {
		cw := InitTxnComputationWrapper(ses, stmt, proc)
		base := &baseStmtExecutor{}
		base.ComputationWrapper = cw
		stmtExec, err = getStmtExecutor(ses, proc, base, stmt)
		if err != nil {
			return nil, err
		}
		appendStmtExec(stmtExec)
	}
	return stmtExecList, nil
}

func incStatementCounter(tenant string, stmt tree.Statement) {
	metric.StatementCounter(tenant, getStatementType(stmt).GetQueryType()).Inc()
}

func incTransactionCounter(tenant string) {
	metric.TransactionCounter(tenant).Inc()
}

func incTransactionErrorsCounter(tenant string, t metric.SQLType) {
	if t == metric.SQLTypeRollback {
		return
	}
	metric.TransactionErrorsCounter(tenant, t).Inc()
}

func incStatementErrorsCounter(tenant string, stmt tree.Statement) {
	metric.StatementErrorsCounter(tenant, getStatementType(stmt).GetQueryType()).Inc()
}

type unknownStatementType struct {
	tree.StatementType
}

func (unknownStatementType) GetStatementType() string { return "Unknown" }
func (unknownStatementType) GetQueryType() string     { return tree.QueryTypeOth }

func getStatementType(stmt tree.Statement) tree.StatementType {
	switch stmt.(type) {
	case tree.StatementType:
		return stmt
	default:
		return unknownStatementType{}
	}
}

// authenticateUserCanExecuteStatement checks the user can execute the statement
func authenticateUserCanExecuteStatement(requestCtx context.Context, ses *Session, stmt tree.Statement) error {
	requestCtx, span := trace.Debug(requestCtx, "authenticateUserCanExecuteStatement")
	defer span.End()
	if ses.skipCheckPrivilege() {
		return nil
	}
	if ses.skipAuthForSpecialUser() {
		return nil
	}
	var havePrivilege bool
	var err error
	if ses.GetTenantInfo() != nil {
		ses.SetPrivilege(determinePrivilegeSetOfStatement(stmt))
		havePrivilege, err = authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(requestCtx, ses, stmt)
		if err != nil {
			return err
		}

		if !havePrivilege {
			err = moerr.NewInternalError(requestCtx, "do not have privilege to execute the statement")
			return err
		}

		havePrivilege, err = authenticateUserCanExecuteStatementWithObjectTypeNone(requestCtx, ses, stmt)
		if err != nil {
			return err
		}

		if !havePrivilege {
			err = moerr.NewInternalError(requestCtx, "do not have privilege to execute the statement")
			return err
		}
	}
	return err
}

// authenticateCanExecuteStatementAndPlan checks the user can execute the statement and its plan
func authenticateCanExecuteStatementAndPlan(requestCtx context.Context, ses *Session, stmt tree.Statement, p *plan.Plan) error {
	if ses.skipCheckPrivilege() {
		return nil
	}
	if ses.skipAuthForSpecialUser() {
		return nil
	}
	yes, err := authenticateUserCanExecuteStatementWithObjectTypeDatabaseAndTable(requestCtx, ses, stmt, p)
	if err != nil {
		return err
	}
	if !yes {
		return moerr.NewInternalError(requestCtx, "do not have privilege to execute the statement")
	}
	return nil
}

// authenticatePrivilegeOfPrepareAndExecute checks the user can execute the Prepare or Execute statement
func authenticateUserCanExecutePrepareOrExecute(requestCtx context.Context, ses *Session, stmt tree.Statement, p *plan.Plan) error {
	err := authenticateUserCanExecuteStatement(requestCtx, ses, stmt)
	if err != nil {
		return err
	}
	err = authenticateCanExecuteStatementAndPlan(requestCtx, ses, stmt, p)
	if err != nil {
		return err
	}
	return err
}

// canExecuteStatementInUncommittedTxn checks the user can execute the statement in an uncommitted transaction
func (mce *MysqlCmdExecutor) canExecuteStatementInUncommittedTransaction(requestCtx context.Context, stmt tree.Statement) error {
	can, err := StatementCanBeExecutedInUncommittedTransaction(mce.GetSession(), stmt)
	if err != nil {
		return err
	}
	if !can {
		//is ddl statement
		if IsDDL(stmt) {
			return moerr.NewInternalError(requestCtx, onlyCreateStatementErrorInfo())
		} else if IsAdministrativeStatement(stmt) {
			return moerr.NewInternalError(requestCtx, administrativeCommandIsUnsupportedInTxnErrorInfo())
		} else if IsParameterModificationStatement(stmt) {
			return moerr.NewInternalError(requestCtx, parameterModificationInTxnErrorInfo())
		} else {
			return moerr.NewInternalError(requestCtx, unclassifiedStatementInUncommittedTxnErrorInfo())
		}
	}
	return nil
}

func (ses *Session) getSqlType(sql string) {
	tenant := ses.GetTenantInfo()
	if tenant == nil || strings.HasPrefix(sql, cmdFieldListSql) {
		ses.sqlSourceType = intereSql
		return
	}
	flag, _, _ := isSpecialUser(tenant.User)
	if flag {
		ses.sqlSourceType = intereSql
		return
	}
	p1 := strings.Index(sql, "/*")
	p2 := strings.Index(sql, "*/")
	if p1 < 0 || p2 < 0 || p2 <= p1+1 {
		ses.sqlSourceType = externSql
		return
	}
	source := strings.TrimSpace(sql[p1+2 : p2])
	if source == cloudUserTag {
		ses.sqlSourceType = cloudUserSql
	} else if source == cloudNoUserTag {
		ses.sqlSourceType = cloudNoUserSql
	} else {
		ses.sqlSourceType = externSql
	}
}

func (mce *MysqlCmdExecutor) processLoadLocal(ctx context.Context, param *tree.ExternParam, writer *io.PipeWriter) (err error) {
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()
	defer func() {
		err2 := writer.Close()
		if err == nil {
			err = err2
		}
	}()
	err = plan2.InitInfileParam(param)
	if err != nil {
		return
	}
	err = proto.sendLocalInfileRequest(param.Filepath)
	if err != nil {
		return
	}
	start := time.Now()
	var msg interface{}
	msg, err = proto.GetTcpConnection().Read(goetty.ReadOptions{})
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrInvalidInput) {
			err = moerr.NewFileNotFound(ctx, param.Filepath)
		}
		proto.SetSequenceID(proto.GetSequenceId() + 1)
		return
	}

	packet, ok := msg.(*Packet)
	if !ok {
		proto.SetSequenceID(proto.GetSequenceId() + 1)
		err = moerr.NewInvalidInput(ctx, "invalid packet")
		return
	}

	proto.SetSequenceID(uint8(packet.SequenceID + 1))
	seq := uint8(packet.SequenceID + 1)
	length := packet.Length
	if length == 0 {
		return
	}

	skipWrite := false
	// If inner error occurs(unexpected or expected(ctrl-c)), proc.LoadLocalReader will be closed.
	// Then write will return error, but we need to read the rest of the data and not write it to pipe.
	// So we need a flag[skipWrite] to tell us whether we need to write the data to pipe.
	// https://github.com/matrixorigin/matrixone/issues/6665#issuecomment-1422236478

	_, err = writer.Write(packet.Payload)
	if err != nil {
		skipWrite = true // next, we just need read the rest of the data,no need to write it to pipe.
		logErrorf(ses.GetConciseProfile(), "load local '%s', write error: %v", param.Filepath, err)
	}
	epoch, printEvery, minReadTime, maxReadTime, minWriteTime, maxWriteTime := uint64(0), uint64(1024), 24*time.Hour, time.Nanosecond, 24*time.Hour, time.Nanosecond
	for {
		readStart := time.Now()
		msg, err = proto.GetTcpConnection().Read(goetty.ReadOptions{})
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrInvalidInput) {
				seq += 1
				proto.SetSequenceID(seq)
				err = nil
			}
			break
		}
		readTime := time.Since(readStart)
		if readTime > maxReadTime {
			maxReadTime = readTime
		}
		if readTime < minReadTime {
			minReadTime = readTime
		}
		packet, ok = msg.(*Packet)
		if !ok {
			err = moerr.NewInvalidInput(ctx, "invalid packet")
			seq += 1
			proto.SetSequenceID(seq)
			break
		}
		seq = uint8(packet.SequenceID + 1)
		proto.SetSequenceID(seq)

		writeStart := time.Now()
		if !skipWrite {
			_, err = writer.Write(packet.Payload)
			if err != nil {
				logErrorf(ses.GetConciseProfile(), "load local '%s', epoch: %d, write error: %v", param.Filepath, epoch, err)
				skipWrite = true
			}
			writeTime := time.Since(writeStart)
			if writeTime > maxWriteTime {
				maxWriteTime = writeTime
			}
			if writeTime < minWriteTime {
				minWriteTime = writeTime
			}
		}
		if epoch%printEvery == 0 {
			logInfof(ses.GetConciseProfile(), "load local '%s', epoch: %d, skipWrite: %v, minReadTime: %s, maxReadTime: %s, minWriteTime: %s, maxWriteTime: %s,", param.Filepath, epoch, skipWrite, minReadTime.String(), maxReadTime.String(), minWriteTime.String(), maxWriteTime.String())
			minReadTime, maxReadTime, minWriteTime, maxWriteTime = 24*time.Hour, time.Nanosecond, 24*time.Hour, time.Nanosecond
		}
		epoch += 1
	}
	logInfof(ses.GetConciseProfile(), "load local '%s', read&write all data from client cost: %s", param.Filepath, time.Since(start))
	return
}

// execute query
func (mce *MysqlCmdExecutor) doComQuery(requestCtx context.Context, sql string) (retErr error) {
	beginInstant := time.Now()
	ses := mce.GetSession()
	ses.getSqlType(sql)
	ses.SetShowStmtType(NotShowStatement)
	proto := ses.GetMysqlProtocol()
	ses.SetSql(sql)
	ses.GetExportParam().Outfile = false
	pu := ses.GetParameterUnit()
	proc := process.New(
		requestCtx,
		ses.GetMemPool(),
		ses.GetTxnHandler().GetTxnClient(),
		ses.GetTxnHandler().GetTxnOperator(),
		pu.FileService)
	proc.Id = mce.getNextProcessId()
	proc.Lim.Size = pu.SV.ProcessLimitationSize
	proc.Lim.BatchRows = pu.SV.ProcessLimitationBatchRows
	proc.Lim.MaxMsgSize = pu.SV.MaxMessageSize
	proc.Lim.PartitionRows = pu.SV.ProcessLimitationPartitionRows
	proc.SessionInfo = process.SessionInfo{
		User:              ses.GetUserName(),
		Host:              pu.SV.Host,
		ConnectionID:      uint64(proto.ConnectionID()),
		Database:          ses.GetDatabaseName(),
		Version:           pu.SV.ServerVersionPrefix + serverVersion.Load().(string),
		TimeZone:          ses.GetTimeZone(),
		StorageEngine:     pu.StorageEngine,
		LastInsertID:      ses.GetLastInsertID(),
		AutoIncrCaches:    ses.GetAutoIncrCaches(),
		AutoIncrCacheSize: ses.pu.SV.AutoIncrCacheSize,
	}
	if ses.GetTenantInfo() != nil {
		proc.SessionInfo.Account = ses.GetTenantInfo().GetTenant()
		proc.SessionInfo.AccountId = ses.GetTenantInfo().GetTenantID()
		proc.SessionInfo.Role = ses.GetTenantInfo().GetDefaultRole()
		proc.SessionInfo.RoleId = ses.GetTenantInfo().GetDefaultRoleID()
		proc.SessionInfo.UserId = ses.GetTenantInfo().GetUserID()

		if len(ses.GetTenantInfo().GetVersion()) != 0 {
			proc.SessionInfo.Version = ses.GetTenantInfo().GetVersion()
		}
	} else {
		proc.SessionInfo.Account = sysAccountName
		proc.SessionInfo.AccountId = sysAccountID
		proc.SessionInfo.RoleId = moAdminRoleID
		proc.SessionInfo.UserId = rootID
	}
	proc.SessionInfo.QueryId = ses.QueryId
	ses.txnCompileCtx.SetProcess(proc)
	cws, err := GetComputationWrapper(ses.GetDatabaseName(),
		sql,
		ses.GetUserName(),
		pu.StorageEngine,
		proc, ses)
	if err != nil {
		requestCtx = RecordParseErrorStatement(requestCtx, ses, proc, beginInstant, sql)
		retErr = err
		if _, ok := err.(*moerr.Error); !ok {
			retErr = moerr.NewParseError(requestCtx, err.Error())
		}
		logStatementStringStatus(requestCtx, ses, sql, fail, retErr)
		return retErr
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
	var err2 error
	var columns []interface{}
	var mrs *MysqlResultSet
	canCache := true
	var loadLocalErrGroup *errgroup.Group
	var loadLocalWriter *io.PipeWriter

	singleStatement := len(cws) == 1
	for i, cw := range cws {
		if cwft, ok := cw.(*TxnComputationWrapper); ok {
			if cwft.stmt.GetQueryType() == tree.QueryTypeDDL || cwft.stmt.GetQueryType() == tree.QueryTypeDCL ||
				cwft.stmt.GetQueryType() == tree.QueryTypeOth ||
				cwft.stmt.GetQueryType() == tree.QueryTypeTCL {
				if _, ok := cwft.stmt.(*tree.SetVar); !ok {
					ses.cleanCache()
				}
				canCache = false
			}
		}

		ses.SetMysqlResultSet(&MysqlResultSet{})
		stmt := cw.GetAst()
		requestCtx = RecordStatement(requestCtx, ses, proc, cw, beginInstant, sql, singleStatement)
		tenant := ses.GetTenantName(stmt)
		//skip PREPARE statement here
		if ses.GetTenantInfo() != nil && !IsPrepareStatement(stmt) {
			err = authenticateUserCanExecuteStatement(requestCtx, ses, stmt)
			if err != nil {
				logStatementStatus(requestCtx, ses, stmt, fail, err)
				return err
			}
		}

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
			err = mce.canExecuteStatementInUncommittedTransaction(requestCtx, stmt)
			if err != nil {
				logStatementStatus(requestCtx, ses, stmt, fail, err)
				return err
			}
		}

		//check transaction states
		switch stmt.(type) {
		case *tree.BeginTransaction:
			err = ses.TxnBegin()
			if err != nil {
				goto handleFailed
			}
			RecordStatementTxnID(requestCtx, ses)
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
				ses.SetExportParam(st.Ep)
			}
		}

		selfHandle = false
		ses.GetTxnCompileCtx().SetQueryType(TXN_DEFAULT)

		switch st := stmt.(type) {
		case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction:
			selfHandle = true
		case *tree.SetRole:
			selfHandle = true
			ses.InvalidatePrivilegeCache()
			//switch role
			err = mce.handleSwitchRole(requestCtx, st)
			if err != nil {
				goto handleFailed
			}
		case *tree.Use:
			selfHandle = true
			//use database
			err = mce.handleChangeDB(requestCtx, st.Name)
			if err != nil {
				goto handleFailed
			}
			err = changeVersion(requestCtx, ses, st.Name)
			if err != nil {
				goto handleFailed
			}
		case *tree.MoDump:
			selfHandle = true
			//dump
			err = mce.handleDump(requestCtx, st)
			if err != nil {
				goto handleFailed
			}
		case *tree.DropDatabase:
			ses.InvalidatePrivilegeCache()
			// if the droped database is the same as the one in use, database must be reseted to empty.
			if string(st.Name) == ses.GetDatabaseName() {
				ses.SetDatabaseName("")
			}
		case *tree.PrepareStmt:
			selfHandle = true
			prepareStmt, err = mce.handlePrepareStmt(requestCtx, st)
			if err != nil {
				goto handleFailed
			}
			err = authenticateUserCanExecutePrepareOrExecute(requestCtx, ses, prepareStmt.PrepareStmt, prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
			if err != nil {
				goto handleFailed
			}
		case *tree.PrepareString:
			selfHandle = true
			prepareStmt, err = mce.handlePrepareString(requestCtx, st)
			if err != nil {
				goto handleFailed
			}
			err = authenticateUserCanExecutePrepareOrExecute(requestCtx, ses, prepareStmt.PrepareStmt, prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
			if err != nil {
				goto handleFailed
			}
		case *tree.Deallocate:
			selfHandle = true
			err = mce.handleDeallocate(requestCtx, st)
			if err != nil {
				goto handleFailed
			}
		case *tree.Reset:
			selfHandle = true
			err = mce.handleReset(requestCtx, st)
			if err != nil {
				goto handleFailed
			}
		case *tree.SetVar:
			selfHandle = true
			err = mce.handleSetVar(requestCtx, st)
			if err != nil {
				goto handleFailed
			}
		case *tree.ShowVariables:
			selfHandle = true
			err = mce.handleShowVariables(st, proc)
			if err != nil {
				goto handleFailed
			}
		case *tree.ShowErrors, *tree.ShowWarnings:
			selfHandle = true
			err = mce.handleShowErrors()
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
			if err = mce.handleExplainStmt(requestCtx, st); err != nil {
				goto handleFailed
			}
		case *tree.ExplainAnalyze:
			ses.SetData(nil)
			switch st.Statement.(type) {
			case *tree.Delete:
				ses.GetTxnCompileCtx().SetQueryType(TXN_DELETE)
			case *tree.Update:
				ses.GetTxnCompileCtx().SetQueryType(TXN_UPDATE)
			default:
				ses.GetTxnCompileCtx().SetQueryType(TXN_DEFAULT)
			}
		case *tree.ShowTableStatus:
			ses.SetShowStmtType(ShowTableStatus)
			ses.SetData(nil)
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
			ses.InvalidatePrivilegeCache()
			if err = mce.handleCreateAccount(requestCtx, st); err != nil {
				goto handleFailed
			}
		case *tree.DropAccount:
			selfHandle = true
			ses.InvalidatePrivilegeCache()
			if err = mce.handleDropAccount(requestCtx, st); err != nil {
				goto handleFailed
			}
		case *tree.AlterAccount:
			ses.InvalidatePrivilegeCache()
			selfHandle = true
			if err = mce.handleAlterAccount(requestCtx, st); err != nil {
				goto handleFailed
			}
		case *tree.AlterDataBaseConfig:
			ses.InvalidatePrivilegeCache()
			selfHandle = true
			if st.IsAccountLevel {
				if err = mce.handleAlterAccountConfig(requestCtx, st); err != nil {
					goto handleFailed
				}
			} else {
				if err = mce.handleAlterDataBaseConfig(requestCtx, st); err != nil {
					goto handleFailed
				}
			}
		case *tree.CreateUser:
			selfHandle = true
			ses.InvalidatePrivilegeCache()
			if err = mce.handleCreateUser(requestCtx, st); err != nil {
				goto handleFailed
			}
		case *tree.DropUser:
			selfHandle = true
			ses.InvalidatePrivilegeCache()
			if err = mce.handleDropUser(requestCtx, st); err != nil {
				goto handleFailed
			}
		case *tree.AlterView:
			ses.InvalidatePrivilegeCache()
		case *tree.AlterUser: //TODO
			ses.InvalidatePrivilegeCache()
		case *tree.CreateRole:
			selfHandle = true
			ses.InvalidatePrivilegeCache()
			if err = mce.handleCreateRole(requestCtx, st); err != nil {
				goto handleFailed
			}
		case *tree.DropRole:
			selfHandle = true
			ses.InvalidatePrivilegeCache()
			if err = mce.handleDropRole(requestCtx, st); err != nil {
				goto handleFailed
			}
		case *tree.CreateFunction:
			selfHandle = true
			if err = mce.handleCreateFunction(requestCtx, st); err != nil {
				goto handleFailed
			}
		case *tree.DropFunction:
			selfHandle = true
			if err = mce.handleDropFunction(requestCtx, st); err != nil {
				goto handleFailed
			}
		case *tree.Grant:
			selfHandle = true
			ses.InvalidatePrivilegeCache()
			switch st.Typ {
			case tree.GrantTypeRole:
				if err = mce.handleGrantRole(requestCtx, &st.GrantRole); err != nil {
					goto handleFailed
				}
			case tree.GrantTypePrivilege:
				if err = mce.handleGrantPrivilege(requestCtx, &st.GrantPrivilege); err != nil {
					goto handleFailed
				}
			}
		case *tree.Revoke:
			selfHandle = true
			ses.InvalidatePrivilegeCache()
			switch st.Typ {
			case tree.RevokeTypeRole:
				if err = mce.handleRevokeRole(requestCtx, &st.RevokeRole); err != nil {
					goto handleFailed
				}
			case tree.RevokeTypePrivilege:
				if err = mce.handleRevokePrivilege(requestCtx, &st.RevokePrivilege); err != nil {
					goto handleFailed
				}
			}
		case *tree.Kill:
			selfHandle = true
			ses.InvalidatePrivilegeCache()
			if err = mce.handleKill(requestCtx, st); err != nil {
				goto handleFailed
			}
		case *tree.ShowAccounts:
			selfHandle = true
			if err = mce.handleShowAccounts(requestCtx, st); err != nil {
				goto handleFailed
			}
		case *tree.Load:
			if st.Local {
				proc.LoadLocalReader, loadLocalWriter = io.Pipe()
			}
		}

		if selfHandle {
			goto handleSucceeded
		}
		if err = cw.SetDatabaseName(ses.GetDatabaseName()); err != nil {
			goto handleFailed
		}

		cmpBegin = time.Now()

		if ret, err = cw.Compile(requestCtx, ses, ses.GetOutputCallback()); err != nil {
			goto handleFailed
		}
		stmt = cw.GetAst()
		// reset some special stmt for execute statement
		switch st := stmt.(type) {
		case *tree.SetVar:
			err = mce.handleSetVar(requestCtx, st)
			if err != nil {
				goto handleFailed
			} else {
				goto handleSucceeded
			}
		case *tree.ShowVariables:
			err = mce.handleShowVariables(st, proc)
			if err != nil {
				goto handleFailed
			} else {
				goto handleSucceeded
			}
		case *tree.ShowErrors, *tree.ShowWarnings:
			err = mce.handleShowErrors()
			if err != nil {
				goto handleFailed
			} else {
				goto handleSucceeded
			}
		}

		runner = ret.(ComputationRunner)

		logInfof(ses.GetConciseProfile(), "time of Exec.Build : %s", time.Since(cmpBegin).String())

		mrs = ses.GetMysqlResultSet()
		// cw.Compile might rewrite sql, here we fetch the latest version
		switch statement := cw.GetAst().(type) {
		//produce result set
		case *tree.Select,
			*tree.ShowCreateTable, *tree.ShowCreateDatabase, *tree.ShowTables, *tree.ShowDatabases, *tree.ShowColumns,
			*tree.ShowProcessList, *tree.ShowStatus, *tree.ShowTableStatus, *tree.ShowGrants,
			*tree.ShowIndex, *tree.ShowCreateView, *tree.ShowTarget, *tree.ShowCollation, *tree.ValuesStatement,
			*tree.ExplainFor, *tree.ExplainStmt, *tree.ShowTableNumber, *tree.ShowColumnNumber, *tree.ShowTableValues, *tree.ShowLocks, *tree.ShowNodeList, *tree.ShowFunctionStatus:
			columns, err = cw.GetColumns()
			if err != nil {
				logErrorf(ses.GetConciseProfile(), "GetColumns from Computation handler failed. error: %v", err)
				goto handleFailed
			}
			if c, ok := cw.(*TxnComputationWrapper); ok {
				ses.rs = &plan.ResultColDef{ResultCols: plan2.GetResultColumnsFromPlan(c.plan)}
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
			cmd := ses.GetCmd()
			for _, c := range columns {
				mysqlc := c.(Column)
				mrs.AddColumn(mysqlc)
				/*
					mysql COM_QUERY response: send the column definition per column
				*/
				err = proto.SendColumnDefinitionPacket(requestCtx, mysqlc, int(cmd))
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
			ep := ses.GetExportParam()
			if ep.Outfile {
				ep.DefaultBufSize = pu.SV.ExportDataDefaultFlushSize
				initExportFileParam(ep, mrs)
				if err = openNewFile(requestCtx, ep, mrs); err != nil {
					goto handleFailed
				}
			}
			if err = runner.Run(0); err != nil {
				goto handleFailed
			}

			switch ses.GetShowStmtType() {
			case ShowTableStatus:
				if err = handleShowTableStatus(ses, statement.(*tree.ShowTableStatus), proc); err != nil {
					goto handleFailed
				}
			}

			if ep.Outfile {
				if err = ep.Writer.Flush(); err != nil {
					goto handleFailed
				}
				if err = ep.File.Close(); err != nil {
					goto handleFailed
				}
			}

			logInfof(ses.GetConciseProfile(), "time of Exec.Run : %s", time.Since(runBegin).String())

			/*
				Step 3: Say goodbye
				mysql COM_QUERY response: End after the data row has been sent.
				After all row data has been sent, it sends the EOF or OK packet.
			*/
			err = proto.sendEOFOrOkPacket(0, 0)
			if err != nil {
				goto handleFailed
			}

			/*
				Step 4: Serialize the execution plan by json
			*/
			if cwft, ok := cw.(*TxnComputationWrapper); ok {
				_ = cwft.RecordExecPlan(requestCtx)
			}
		//just status, no result set
		case *tree.CreateTable, *tree.DropTable, *tree.CreateDatabase, *tree.DropDatabase,
			*tree.CreateIndex, *tree.DropIndex,
			*tree.CreateView, *tree.DropView,
			*tree.AlterView,
			*tree.Insert, *tree.Update,
			*tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction,
			*tree.SetVar,
			*tree.Load,
			*tree.CreateUser, *tree.DropUser, *tree.AlterUser,
			*tree.CreateRole, *tree.DropRole,
			*tree.Revoke, *tree.Grant,
			*tree.SetDefaultRole, *tree.SetRole, *tree.SetPassword,
			*tree.Delete, *tree.TruncateTable:
			//change privilege
			switch cw.GetAst().(type) {
			case *tree.DropTable, *tree.DropDatabase, *tree.DropIndex, *tree.DropView,
				*tree.CreateUser, *tree.DropUser, *tree.AlterUser,
				*tree.CreateRole, *tree.DropRole,
				*tree.Revoke, *tree.Grant,
				*tree.SetDefaultRole, *tree.SetRole:
				ses.InvalidatePrivilegeCache()
			}
			runBegin := time.Now()
			/*
				Step 1: Start
			*/

			if st, ok := cw.GetAst().(*tree.Load); ok {
				if st.Local {
					loadLocalErrGroup = new(errgroup.Group)
					loadLocalErrGroup.Go(func() error {
						return mce.processLoadLocal(proc.Ctx, st.Param, loadLocalWriter)
					})
				}
			}

			if err = runner.Run(0); err != nil {
				if loadLocalErrGroup != nil { // release resources
					err2 := proc.LoadLocalReader.Close()
					if err2 != nil {
						logErrorf(ses.GetConciseProfile(), "processLoadLocal reader close failed: %s", err2.Error())
					}
					err2 = loadLocalErrGroup.Wait() // executor failed, but processLoadLocal is still running, wait for it
					if err2 != nil {
						logErrorf(ses.GetConciseProfile(), "processLoadLocal goroutine failed: %s", err2.Error())
					}
				}
				goto handleFailed
			}

			if loadLocalErrGroup != nil {
				if err = loadLocalErrGroup.Wait(); err != nil { //executor success, but processLoadLocal goroutine failed
					goto handleFailed
				}
			}

			logInfof(ses.GetConciseProfile(), "time of Exec.Run : %s", time.Since(runBegin).String())

			rspLen = cw.GetAffectedRows()
			echoTime := time.Now()

			logInfof(ses.GetConciseProfile(), "time of SendResponse %s", time.Since(echoTime).String())

			/*
				Step 4: Serialize the execution plan by json
			*/
			if cwft, ok := cw.(*TxnComputationWrapper); ok {
				_ = cwft.RecordExecPlan(requestCtx)
			}
		case *tree.ExplainAnalyze:
			explainColName := "QUERY PLAN"
			columns, err = GetExplainColumns(requestCtx, explainColName)
			if err != nil {
				logErrorf(ses.GetConciseProfile(), "GetColumns from ExplainColumns handler failed, error: %v", err)
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
			cmd := ses.GetCmd()
			for _, c := range columns {
				mysqlc := c.(Column)
				mrs.AddColumn(mysqlc)
				/*
					mysql COM_QUERY response: send the column definition per column
				*/
				err = proto.SendColumnDefinitionPacket(requestCtx, mysqlc, int(cmd))
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
				Step 1: Start
			*/
			if err = runner.Run(0); err != nil {
				goto handleFailed
			}

			logInfof(ses.GetConciseProfile(), "time of Exec.Run : %s", time.Since(runBegin).String())

			if cwft, ok := cw.(*TxnComputationWrapper); ok {
				queryPlan := cwft.plan
				// generator query explain
				explainQuery := explain.NewExplainQueryImpl(queryPlan.GetQuery())

				// build explain data buffer
				buffer := explain.NewExplainDataBuffer()
				var option *explain.ExplainOptions
				option, err = getExplainOption(requestCtx, statement.Options)
				if err != nil {
					goto handleFailed
				}

				err = explainQuery.ExplainPlan(requestCtx, buffer, option)
				if err != nil {
					goto handleFailed
				}

				err = buildMoExplainQuery(explainColName, buffer, ses, getDataFromPipeline)
				if err != nil {
					goto handleFailed
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
			}
		}
	handleSucceeded:
		//load data handle txn failure internally
		incStatementCounter(tenant, stmt)
		if !fromLoadData {
			txnErr = ses.TxnCommitSingleStatement(stmt)
			if txnErr != nil {
				logStatementStatus(requestCtx, ses, stmt, fail, txnErr)
				return txnErr
			}
		}
		switch stmt.(type) {
		case *tree.CreateTable, *tree.DropTable,
			*tree.CreateIndex, *tree.DropIndex, *tree.Insert, *tree.Update,
			*tree.CreateView, *tree.DropView, *tree.AlterView, *tree.Load, *tree.MoDump,
			*tree.CreateAccount, *tree.DropAccount, *tree.AlterAccount, *tree.AlterDataBaseConfig,
			*tree.CreateFunction, *tree.DropFunction,
			*tree.CreateUser, *tree.DropUser, *tree.AlterUser,
			*tree.CreateRole, *tree.DropRole, *tree.Revoke, *tree.Grant,
			*tree.SetDefaultRole, *tree.SetRole, *tree.SetPassword, *tree.Delete, *tree.TruncateTable, *tree.Use,
			*tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction:
			resp := mce.setResponse(i, len(cws), rspLen)
			if _, ok := stmt.(*tree.Insert); ok {
				resp.lastInsertId = proc.GetLastInsertID()
				if proc.GetLastInsertID() != 0 {
					ses.SetLastInsertID(proc.GetLastInsertID())
				}
			}
			if err2 = mce.GetSession().GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
				retErr = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
				logStatementStatus(requestCtx, ses, stmt, fail, retErr)
				return retErr
			}

		case *tree.CreateDatabase:
			insertRecordToMoMysqlCompatbilityMode(requestCtx, ses, stmt)
			resp := mce.setResponse(i, len(cws), rspLen)
			if err2 = mce.GetSession().GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
				retErr = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
				logStatementStatus(requestCtx, ses, stmt, fail, retErr)
				return retErr
			}

		case *tree.DropDatabase:
			deleteRecordToMoMysqlCompatbilityMode(requestCtx, ses, stmt)
			resp := mce.setResponse(i, len(cws), rspLen)
			if err2 = mce.GetSession().GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
				retErr = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
				logStatementStatus(requestCtx, ses, stmt, fail, retErr)
				return retErr
			}

		case *tree.PrepareStmt, *tree.PrepareString:
			if ses.GetCmd() == COM_STMT_PREPARE {
				if err2 = mce.GetSession().GetMysqlProtocol().SendPrepareResponse(requestCtx, prepareStmt); err2 != nil {
					retErr = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
					logStatementStatus(requestCtx, ses, stmt, fail, retErr)
					return retErr
				}
			} else {
				resp := mce.setResponse(i, len(cws), rspLen)
				if err2 = mce.GetSession().GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
					retErr = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
					logStatementStatus(requestCtx, ses, stmt, fail, retErr)
					return retErr
				}
			}

		case *tree.SetVar:
			resp := mce.setResponse(i, len(cws), rspLen)
			if err = proto.SendResponse(requestCtx, resp); err != nil {
				return moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err)
			}

		case *tree.Deallocate:
			//we will not send response in COM_STMT_CLOSE command
			if ses.GetCmd() != COM_STMT_CLOSE {
				resp := mce.setResponse(i, len(cws), rspLen)
				if err2 = mce.GetSession().GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
					retErr = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
					logStatementStatus(requestCtx, ses, stmt, fail, retErr)
					return retErr
				}
			}

		case *tree.Reset:
			resp := mce.setResponse(i, len(cws), rspLen)
			if err2 = mce.GetSession().GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
				retErr = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
				logStatementStatus(requestCtx, ses, stmt, fail, retErr)
				return retErr
			}
		}
		logStatementStatus(requestCtx, ses, stmt, success, nil)
		goto handleNext
	handleFailed:
		incStatementCounter(tenant, stmt)
		incStatementErrorsCounter(tenant, stmt)
		/*
			Cases    | set Autocommit = 1/0 | BEGIN statement |
			---------------------------------------------------
			Case1      1                       Yes
			Case2      1                       No
			Case3      0                       Yes
			Case4      0                       No
			---------------------------------------------------
			update error message in Case1,Case3,Case4.
		*/
		if ses.InMultiStmtTransactionMode() && ses.InActiveTransaction() {
			ses.cleanCache()
			ses.SetOptionBits(OPTION_ATTACH_ABORT_TRANSACTION_ERROR)
		}
		logError(ses.GetConciseProfile(), err.Error())
		if !fromLoadData {
			txnErr = ses.TxnRollbackSingleStatement(stmt)
			if txnErr != nil {
				logStatementStatus(requestCtx, ses, stmt, fail, txnErr)
				return txnErr
			}
		}
		logStatementStatus(requestCtx, ses, stmt, fail, err)
		return err
	handleNext:
	} // end of for

	if canCache && !ses.isCached(sql) {
		plans := make([]*plan.Plan, len(cws))
		stmts := make([]tree.Statement, len(cws))
		for i, cw := range cws {
			if cwft, ok := cw.(*TxnComputationWrapper); ok && checkNodeCanCache(cwft.plan) {
				plans[i] = cwft.plan
				stmts[i] = cwft.stmt
			} else {
				return nil
			}
		}
		ses.cachePlan(sql, stmts, plans)
	}

	return nil
}

func checkNodeCanCache(p *plan2.Plan) bool {
	if p == nil {
		return true
	}
	if q, ok := p.Plan.(*plan2.Plan_Query); ok {
		for _, node := range q.Query.Nodes {
			if node.NotCacheable {
				return false
			}
		}
	}
	return true
}

// execute query. Currently, it is developing. Finally, it will replace the doComQuery.
func (mce *MysqlCmdExecutor) doComQueryInProgress(requestCtx context.Context, sql string) (retErr error) {
	var stmtExecs []StmtExecutor
	var err error
	beginInstant := time.Now()
	ses := mce.GetSession()
	ses.SetShowStmtType(NotShowStatement)
	proto := ses.GetMysqlProtocol()
	ses.SetSql(sql)
	ses.GetExportParam().Outfile = false
	pu := ses.GetParameterUnit()
	proc := process.New(
		requestCtx,
		ses.GetMemPool(),
		pu.TxnClient,
		ses.GetTxnHandler().GetTxnOperator(),
		pu.FileService)
	proc.Id = mce.getNextProcessId()
	proc.Lim.Size = pu.SV.ProcessLimitationSize
	proc.Lim.BatchRows = pu.SV.ProcessLimitationBatchRows
	proc.Lim.PartitionRows = pu.SV.ProcessLimitationPartitionRows
	proc.SessionInfo = process.SessionInfo{
		User:              ses.GetUserName(),
		Host:              pu.SV.Host,
		ConnectionID:      uint64(proto.ConnectionID()),
		Database:          ses.GetDatabaseName(),
		Version:           pu.SV.ServerVersionPrefix + serverVersion.Load().(string),
		TimeZone:          ses.GetTimeZone(),
		StorageEngine:     pu.StorageEngine,
		AutoIncrCaches:    ses.GetAutoIncrCaches(),
		AutoIncrCacheSize: ses.pu.SV.AutoIncrCacheSize,
	}

	if ses.GetTenantInfo() != nil {
		proc.SessionInfo.Account = ses.GetTenantInfo().GetTenant()
		proc.SessionInfo.AccountId = ses.GetTenantInfo().GetTenantID()
		proc.SessionInfo.RoleId = ses.GetTenantInfo().GetDefaultRoleID()
		proc.SessionInfo.UserId = ses.GetTenantInfo().GetUserID()
	} else {
		proc.SessionInfo.Account = sysAccountName
		proc.SessionInfo.AccountId = sysAccountID
		proc.SessionInfo.RoleId = moAdminRoleID
		proc.SessionInfo.UserId = rootID
	}

	stmtExecs, err = GetStmtExecList(ses.GetDatabaseName(),
		sql,
		ses.GetUserName(),
		pu.StorageEngine,
		proc, ses)
	if err != nil {
		requestCtx = RecordParseErrorStatement(requestCtx, ses, proc, beginInstant, sql)
		retErr = moerr.NewParseError(requestCtx, err.Error())
		logStatementStringStatus(requestCtx, ses, sql, fail, retErr)
		return retErr
	}

	singleStatement := len(stmtExecs) == 1
	for _, exec := range stmtExecs {
		err = Execute(requestCtx, ses, proc, exec, beginInstant, sql, singleStatement)
		if err != nil {
			return err
		}
	}
	return err
}

func (mce *MysqlCmdExecutor) setResponse(cwIndex, cwsLen int, rspLen uint64) *Response {

	//if the stmt has next stmt, should set the server status equals to 10
	if cwIndex < cwsLen-1 {
		return NewOkResponse(rspLen, 0, 0, SERVER_MORE_RESULTS_EXISTS, int(COM_QUERY), "")
	} else {
		return NewOkResponse(rspLen, 0, 0, 0, int(COM_QUERY), "")
	}

}

// ExecRequest the server execute the commands from the client following the mysql's routine
func (mce *MysqlCmdExecutor) ExecRequest(requestCtx context.Context, ses *Session, req *Request) (resp *Response, err error) {
	defer func() {
		if e := recover(); e != nil {
			moe, ok := e.(*moerr.Error)
			if !ok {
				err = moerr.ConvertPanicError(requestCtx, e)
				resp = NewGeneralErrorResponse(COM_QUERY, err)
			} else {
				resp = NewGeneralErrorResponse(COM_QUERY, moe)
			}
		}
	}()

	var sql string
	logDebugf(ses.GetCompleteProfile(), "cmd %v", req.GetCmd())
	ses.SetCmd(req.GetCmd())
	doComQuery := mce.GetDoQueryFunc()
	switch req.GetCmd() {
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
		logInfo(ses.GetConciseProfile(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(SubStringFromBegin(query, int(ses.GetParameterUnit().SV.LengthOfQueryPrinted))))
		err = doComQuery(requestCtx, query)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_QUERY, err)
		}
		return resp, nil
	case COM_INIT_DB:
		var dbname = string(req.GetData().([]byte))
		mce.addSqlCount(1)
		query := "use `" + dbname + "`"
		err = doComQuery(requestCtx, query)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_INIT_DB, err)
		}

		return resp, nil
	case COM_FIELD_LIST:
		var payload = string(req.GetData().([]byte))
		mce.addSqlCount(1)
		query := makeCmdFieldListSql(payload)
		err = doComQuery(requestCtx, query)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_FIELD_LIST, err)
		}

		return resp, nil
	case COM_PING:
		resp = NewGeneralOkResponse(COM_PING)

		return resp, nil

	case COM_STMT_PREPARE:
		ses.SetCmd(COM_STMT_PREPARE)
		sql = string(req.GetData().([]byte))
		mce.addSqlCount(1)

		// rewrite to "Prepare stmt_name from 'xxx'"
		newLastStmtID := ses.GenNewStmtId()
		newStmtName := getPrepareStmtName(newLastStmtID)
		sql = fmt.Sprintf("prepare %s from %s", newStmtName, sql)
		logInfo(ses.GetConciseProfile(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))

		err = doComQuery(requestCtx, sql)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_PREPARE, err)
		}
		return resp, nil

	case COM_STMT_EXECUTE:
		ses.SetCmd(COM_STMT_EXECUTE)
		data := req.GetData().([]byte)
		sql, err = mce.parseStmtExecute(requestCtx, data)
		if err != nil {
			return NewGeneralErrorResponse(COM_STMT_EXECUTE, err), nil
		}
		err = doComQuery(requestCtx, sql)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_EXECUTE, err)
		}
		return resp, nil

	case COM_STMT_CLOSE:
		data := req.GetData().([]byte)

		// rewrite to "deallocate Prepare stmt_name"
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		stmtName := getPrepareStmtName(stmtID)
		sql = fmt.Sprintf("deallocate prepare %s", stmtName)
		logInfo(ses.GetConciseProfile(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))

		err = doComQuery(requestCtx, sql)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_CLOSE, err)
		}
		return resp, nil

	case COM_STMT_RESET:
		data := req.GetData().([]byte)

		//Payload of COM_STMT_RESET
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		stmtName := getPrepareStmtName(stmtID)
		sql = fmt.Sprintf("reset prepare %s", stmtName)
		logInfo(ses.GetConciseProfile(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))
		err = doComQuery(requestCtx, sql)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_RESET, err)
		}
		return resp, nil

	default:
		resp = NewGeneralErrorResponse(req.GetCmd(), moerr.NewInternalError(requestCtx, "unsupported command. 0x%x", req.GetCmd()))
	}
	return resp, nil
}

func (mce *MysqlCmdExecutor) parseStmtExecute(requestCtx context.Context, data []byte) (string, error) {
	// see https://dev.mysql.com/doc/internals/en/com-stmt-execute.html
	pos := 0
	if len(data) < 4 {
		return "", moerr.NewInvalidInput(requestCtx, "sql command contains malformed packet")
	}
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmtName := fmt.Sprintf("%s_%d", prefixPrepareStmtName, stmtID)
	ses := mce.GetSession()
	preStmt, err := ses.GetPrepareStmt(stmtName)
	if err != nil {
		return "", err
	}
	names, vars, err := ses.GetMysqlProtocol().ParseExecuteData(requestCtx, preStmt, data, pos)
	if err != nil {
		return "", err
	}
	sql := fmt.Sprintf("execute %s", stmtName)
	varStrings := make([]string, len(names))
	if len(names) > 0 {
		sql = sql + fmt.Sprintf(" using @%s", strings.Join(names, ",@"))
		for i := 0; i < len(names); i++ {
			varStrings[i] = fmt.Sprintf("%v", vars[i])
			err := ses.SetUserDefinedVar(names[i], vars[i])
			if err != nil {
				return "", err
			}
		}
	}
	logInfo(ses.GetConciseProfile(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql), logutil.VarsField(strings.Join(varStrings, " , ")))
	return sql, nil
}

func (mce *MysqlCmdExecutor) SetCancelFunc(cancelFunc context.CancelFunc) {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	mce.cancelRequestFunc = cancelFunc
}

func (mce *MysqlCmdExecutor) Close() {}

/*
StatementCanBeExecutedInUncommittedTransaction checks the statement can be executed in an active transaction.
*/
func StatementCanBeExecutedInUncommittedTransaction(ses *Session, stmt tree.Statement) (bool, error) {
	switch st := stmt.(type) {
	//ddl statement
	case *tree.CreateTable, *tree.CreateDatabase, *tree.CreateIndex, *tree.CreateView, *tree.AlterView:
		return true, nil
		//dml statement
	case *tree.Insert, *tree.Update, *tree.Delete, *tree.Select, *tree.Load, *tree.MoDump, *tree.ValuesStatement:
		return true, nil
		//transaction
	case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction:
		return true, nil
		//show
	case *tree.ShowCreateTable,
		*tree.ShowCreateView,
		*tree.ShowCreateDatabase,
		*tree.ShowColumns,
		*tree.ShowDatabases,
		*tree.ShowTarget,
		*tree.ShowTableStatus,
		*tree.ShowGrants,
		*tree.ShowTables,
		*tree.ShowProcessList,
		*tree.ShowErrors,
		*tree.ShowWarnings,
		*tree.ShowCollation,
		*tree.ShowVariables,
		*tree.ShowStatus,
		*tree.ShowIndex,
		*tree.ShowFunctionStatus,
		*tree.ShowNodeList,
		*tree.ShowLocks,
		*tree.ShowTableNumber,
		*tree.ShowColumnNumber,
		*tree.ShowTableValues,
		*tree.ShowAccounts:
		return true, nil
		//others
	case *tree.ExplainStmt, *tree.ExplainAnalyze, *tree.ExplainFor, *InternalCmdFieldList:
		return true, nil
	case *tree.PrepareStmt:
		return StatementCanBeExecutedInUncommittedTransaction(ses, st.Stmt)
	case *tree.PrepareString:
		preStmt, err := mysql.ParseOne(ses.requestCtx, st.Sql)
		if err != nil {
			return false, err
		}
		return StatementCanBeExecutedInUncommittedTransaction(ses, preStmt)
	case *tree.Execute:
		preName := string(st.Name)
		preStmt, err := ses.GetPrepareStmt(preName)
		if err != nil {
			return false, err
		}
		return StatementCanBeExecutedInUncommittedTransaction(ses, preStmt.PrepareStmt)
	case *tree.Deallocate, *tree.Reset:
		return true, nil
	case *tree.Use:
		/*
			These statements can not be executed in an uncommitted transaction:
				USE SECONDARY ROLE { ALL | NONE }
				USE ROLE role;
		*/
		return !st.IsUseRole(), nil
	case *tree.DropTable, *tree.DropDatabase, *tree.DropIndex, *tree.DropView:
		//background transaction can execute the DROPxxx in one transaction
		return ses.IsBackgroundSession(), nil
	}

	return false, nil
}

// IsDDL checks the statement is the DDL statement.
func IsDDL(stmt tree.Statement) bool {
	switch stmt.(type) {
	case *tree.CreateTable, *tree.DropTable,
		*tree.CreateView, *tree.DropView, *tree.AlterView,
		*tree.CreateDatabase, *tree.DropDatabase,
		*tree.CreateIndex, *tree.DropIndex, *tree.TruncateTable:
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

// IsPrepareStatement checks the statement is the Prepare statement.
func IsPrepareStatement(stmt tree.Statement) bool {
	switch stmt.(type) {
	case *tree.PrepareStmt, *tree.PrepareString:
		return true
	}
	return false
}

/*
NeedToBeCommittedInActiveTransaction checks the statement that need to be committed
in an active transaction.

Currently, it includes the drop statement, the administration statement ,

	the parameter modification statement.
*/
func NeedToBeCommittedInActiveTransaction(stmt tree.Statement) bool {
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
func convertEngineTypeToMysqlType(ctx context.Context, engineType types.T, col *MysqlColumn) error {
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
	case types.T_time:
		col.SetColumnType(defines.MYSQL_TYPE_TIME)
	case types.T_timestamp:
		col.SetColumnType(defines.MYSQL_TYPE_TIMESTAMP)
	case types.T_decimal64:
		col.SetColumnType(defines.MYSQL_TYPE_DECIMAL)
	case types.T_decimal128:
		col.SetColumnType(defines.MYSQL_TYPE_DECIMAL)
	case types.T_blob:
		col.SetColumnType(defines.MYSQL_TYPE_BLOB)
	case types.T_text:
		col.SetColumnType(defines.MYSQL_TYPE_TEXT)
	case types.T_uuid:
		col.SetColumnType(defines.MYSQL_TYPE_UUID)
	default:
		return moerr.NewInternalError(ctx, "RunWhileSend : unsupported type %d", engineType)
	}
	return nil
}

func convertMysqlTextTypeToBlobType(col *MysqlColumn) {
	if col.ColumnType() == defines.MYSQL_TYPE_TEXT {
		col.SetColumnType(defines.MYSQL_TYPE_BLOB)
	}
}

// build plan json when marhal plan error
func buildErrorJsonPlan(uuid uuid.UUID, errcode uint16, msg string) []byte {
	explainData := explain.ExplainData{
		Code:    errcode,
		Message: msg,
		Success: false,
		Uuid:    uuid.String(),
	}
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	encoder.Encode(explainData)
	return buffer.Bytes()
}

func serializePlanToJson(ctx context.Context, queryPlan *plan2.Plan, uuid uuid.UUID) (jsonBytes []byte, statsJonsBytes []byte, stats motrace.Statistic) {
	if queryPlan != nil && queryPlan.GetQuery() != nil {
		explainQuery := explain.NewExplainQueryImpl(queryPlan.GetQuery())
		options := &explain.ExplainOptions{
			Verbose: true,
			Analyze: true,
			Format:  explain.EXPLAIN_FORMAT_TEXT,
		}
		marshalPlan := explainQuery.BuildJsonPlan(ctx, uuid, options)
		stats.RowsRead, stats.BytesScan = marshalPlan.StatisticsRead()
		// data transform to json datastruct
		buffer := &bytes.Buffer{}
		encoder := json.NewEncoder(buffer)
		encoder.SetEscapeHTML(false)
		err := encoder.Encode(marshalPlan)
		if err != nil {
			moError := moerr.NewInternalError(ctx, "serialize plan to json error: %s", err.Error())
			jsonBytes = buildErrorJsonPlan(uuid, moError.ErrorCode(), moError.Error())
		} else {
			jsonBytes = buffer.Bytes()
		}
		// data transform Global to json
		if len(marshalPlan.Steps) > 0 {
			if len(marshalPlan.Steps) > 1 {
				logutil.Fatalf("need handle multi execPlan trees, cnt: %d", len(marshalPlan.Steps))
			}
			buffer := &bytes.Buffer{}
			encoder := json.NewEncoder(buffer)
			encoder.SetEscapeHTML(false)
			global := marshalPlan.Steps[0].GraphData.Global
			err = encoder.Encode(&global)
			if err != nil {
				statsJonsBytes = []byte(fmt.Sprintf(`{"code":200,"message":"%q"}`, err.Error()))
			} else {
				statsJonsBytes = buffer.Bytes()
			}
		}
	} else {
		jsonBytes = buildErrorJsonPlan(uuid, moerr.ErrWarn, "sql query no record execution plan")
	}
	return jsonBytes, statsJonsBytes, stats
}

// SerializeExecPlan Serialize the execution plan by json
var SerializeExecPlan = func(ctx context.Context, plan any, uuid uuid.UUID) ([]byte, []byte, motrace.Statistic) {
	if plan == nil {
		return serializePlanToJson(ctx, nil, uuid)
	} else if queryPlan, ok := plan.(*plan2.Plan); !ok {
		moError := moerr.NewInternalError(ctx, "execPlan not type of plan2.Plan: %s", reflect.ValueOf(plan).Type().Name())
		return buildErrorJsonPlan(uuid, moError.ErrorCode(), moError.Error()), []byte{}, motrace.Statistic{}
	} else {
		// data transform to json dataStruct
		return serializePlanToJson(ctx, queryPlan, uuid)
	}
}

func init() {
	motrace.SetDefaultSerializeExecPlan(SerializeExecPlan)
}

func getAccountId(ctx context.Context) uint32 {
	var accountId uint32

	if v := ctx.Value(defines.TenantIDKey{}); v != nil {
		accountId = v.(uint32)
	}
	return accountId
}

func changeVersion(ctx context.Context, ses *Session, db string) error {
	var err error
	version, _ := GetVersionCompatbility(ctx, ses, db)
	if ses.GetTenantInfo() != nil {
		ses.GetTenantInfo().SetVersion(version)
	}
	return err
}
