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
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/status"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var MaxPrepareNumberInOneSession int = 100000

// TODO: this variable should be configure by set variable
const MoDefaultErrorCount = 64

type ShowStatementType int

const (
	NotShowStatement ShowStatementType = 0
	ShowTableStatus  ShowStatementType = 1
)

type ConnType int

const (
	ConnTypeUnset    ConnType = 0
	ConnTypeInternal ConnType = 1
	ConnTypeExternal ConnType = 2
)

type TS string

const (
	// Created
	TSCreatedStart TS = "TSCreatedStart"
	TSCreatedEnd   TS = "TSCreatedEnd"

	// Handler
	TSEstablishStart  TS = "TSEstablishStart"
	TSEstablishEnd    TS = "TSEstablishEnd"
	TSUpgradeTLSStart TS = "TSUpgradeTLSStart"
	TSUpgradeTLSEnd   TS = "TSUpgradeTLSEnd"

	// mysql protocol
	TSAuthenticateStart  TS = "TSAuthenticateStart"
	TSAuthenticateEnd    TS = "TSAuthenticateEnd"
	TSSendErrPacketStart TS = "TSSendErrPacketStart"
	TSSendErrPacketEnd   TS = "TSSendErrPacketEnd"
	TSSendOKPacketStart  TS = "TSSendOKPacketStart"
	TSSendOKPacketEnd    TS = "TSSendOKPacketEnd"

	// session
	TSCheckTenantStart      TS = "TSCheckTenantStart"
	TSCheckTenantEnd        TS = "TSCheckTenantEnd"
	TSCheckUserStart        TS = "TSCheckUserStart"
	TSCheckUserEnd          TS = "TSCheckUserEnd"
	TSCheckRoleStart        TS = "TSCheckRoleStart"
	TSCheckRoleEnd          TS = "TSCheckRoleEnd"
	TSCheckDbNameStart      TS = "TSCheckDbNameStart"
	TSCheckDbNameEnd        TS = "TSCheckDbNameEnd"
	TSInitGlobalSysVarStart TS = "TSInitGlobalSysVarStart"
	TSInitGlobalSysVarEnd   TS = "TSInitGlobalSysVarEnd"
)

type Session struct {
	feSessionImpl

	//cmd from the client
	cmd CommandType

	// the process of the session
	proc *process.Process

	isInternal bool

	data            [][]interface{}
	ep              *ExportConfig
	showStmtType    ShowStatementType
	sysVars         map[string]interface{}
	userDefinedVars map[string]*UserDefinedVar

	prepareStmts map[string]*PrepareStmt
	lastStmtId   uint32

	priv *privilege

	errInfo *errInfo

	//fromRealUser distinguish the sql that the user inputs from the one
	//that the internal or background program executes
	fromRealUser bool

	cache *privilegeCache

	mu sync.Mutex

	isNotBackgroundSession bool
	lastInsertID           uint64
	tStmt                  *motrace.StatementInfo

	ast tree.Statement

	queryId []string

	blockIdx int

	p *plan.Plan

	limitResultSize float64 // MB

	curResultSize float64 // MB

	// sentRows used to record rows it sent to client for motrace.StatementInfo.
	// If there is NO exec_plan, sentRows will be 0.
	sentRows atomic.Int64
	// writeCsvBytes is used to record bytes sent by `select ... into 'file.csv'` for motrace.StatementInfo
	writeCsvBytes atomic.Int64
	// packetCounter count the packet communicated with client.
	packetCounter atomic.Int64
	// payloadCounter count the payload send by `load data`
	payloadCounter int64

	createdTime time.Time

	expiredTime time.Time

	planCache *planCache

	statsCache   *plan2.StatsCache
	seqCurValues map[uint64]string

	/*
		CORNER CASE:

		create sequence seq1;
		set @@a = (select nextval(seq1)); // a = 1
		select currval('seq1');// 1
		select lastval('seq1');// right value is 1

		We execute the expr of 'set var = expr' in a background session,
		the last value of the seq1 is saved in the background session.

		If we want to get the right value the lastval('seq1'), we need save
		the last value of the seq1 in the session that starts the background session.

		So, we define the type of seqLastValue as *string for updating its value conveniently.

		TODO: we need to reimplement the sequence in some extent traced by issue #9847.
	*/
	seqLastValue *string

	sqlHelper *SqlHelper

	rm *RoutineManager

	rt *Routine

	// requestLabel is the CN label info requested from client.
	requestLabel map[string]string
	// connTyp indicates the type of connection. Default is ConnTypeUnset.
	// If it is internal connection, the value will be ConnTypeInternal, otherwise,
	// the value will be ConnTypeExternal.
	connType ConnType

	// startedAt is the session start time.
	startedAt time.Time

	// queryEnd is the time when the query ends
	queryEnd time.Time
	// queryInProgress indicates whether the query is in progress
	queryInProgress atomic.Bool
	// queryInExecute indicates whether the query is in execute
	queryInExecute atomic.Bool

	// timestampMap record timestamp for statistical purposes
	timestampMap map[TS]time.Time

	// insert sql for create table as select stmt
	createAsSelectSql string

	// FromProxy denotes whether the session is dispatched from proxy
	fromProxy bool
	// If the connection is from proxy, client address is the real address of client.
	clientAddr string
	proxyAddr  string

	disableTrace bool
}

func (ses *Session) GetTxnHandler() *TxnHandler {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.txnHandler
}

func (ses *Session) GetTenantInfo() *TenantInfo {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.tenant
}

func (ses *Session) SendRows() int64 {
	return ses.sentRows.Load()
}

func (ses *Session) GetStmtInfo() *motrace.StatementInfo {
	return ses.tStmt
}

func (ses *Session) getNextProcessId() string {
	/*
		temporary method:
		routineId + sqlCount
	*/
	routineId := ses.GetMysqlProtocol().ConnectionID()
	return fmt.Sprintf("%d%d", routineId, ses.GetSqlCount())
}

func (ses *Session) SetPlan(plan *plan.Plan) {
	ses.p = plan
}

func (ses *Session) GetProc() *process.Process {
	return ses.proc
}

func (ses *Session) GetStatsCache() *plan2.StatsCache {
	return ses.statsCache
}

func (ses *Session) GetSessionStart() time.Time {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.startedAt
}

func (ses *Session) SetQueryEnd(t time.Time) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.queryEnd = t
}

func (ses *Session) GetQueryEnd() time.Time {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.queryEnd
}

func (ses *Session) SetQueryInProgress(b bool) {
	ses.queryInProgress.Store(b)
}

func (ses *Session) GetQueryInProgress() bool {
	return ses.queryInProgress.Load()
}

func (ses *Session) SetQueryInExecute(b bool) {
	ses.queryInExecute.Store(b)
}

func (ses *Session) GetQueryInExecute() bool {
	return ses.queryInExecute.Load()
}

func (ses *Session) setRoutineManager(rm *RoutineManager) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.rm = rm
}

func (ses *Session) getRoutineManager() *RoutineManager {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.rm
}

func (ses *Session) setRoutine(rt *Routine) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.rt = rt
}

func (ses *Session) getRoutine() *Routine {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.rt
}

func (ses *Session) SetSeqLastValue(proc *process.Process) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	*ses.seqLastValue = proc.SessionInfo.SeqLastValue[0]
}

func (ses *Session) DeleteSeqValues(proc *process.Process) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	for _, k := range proc.SessionInfo.SeqDeleteKeys {
		delete(ses.seqCurValues, k)
	}
}

func (ses *Session) AddSeqValues(proc *process.Process) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	for k, v := range proc.SessionInfo.SeqAddValues {
		ses.seqCurValues[k] = v
	}
}

func (ses *Session) GetSeqLastValue() string {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return *ses.seqLastValue
}

func (ses *Session) CopySeqToProc(proc *process.Process) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	for k, v := range ses.seqCurValues {
		proc.SessionInfo.SeqCurValues[k] = v
	}
	proc.SessionInfo.SeqLastValue[0] = *ses.seqLastValue
}

func (ses *Session) InheritSequenceData(other *Session) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.seqCurValues = other.seqCurValues
	ses.seqLastValue = other.seqLastValue
}

func (ses *Session) GetSqlHelper() *SqlHelper {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.sqlHelper
}

func (ses *Session) CountPayload(length int) {
	if ses == nil {
		return
	}
	ses.payloadCounter += int64(length)
}
func (ses *Session) CountPacket(delta int64) {
	if ses == nil {
		return
	}
	ses.packetCounter.Add(delta)
}
func (ses *Session) GetPacketCnt() int64 {
	if ses == nil {
		return 0
	}
	return ses.packetCounter.Load()
}
func (ses *Session) ResetPacketCounter() {
	if ses == nil {
		return
	}
	ses.packetCounter.Store(0)
	ses.payloadCounter = 0
}

// SetTStmt do set the Session.tStmt
// 1. init-set at RecordStatement, which means the statement is started.
// 2. reset at logStatementStringStatus, which means the statement is finished.
func (ses *Session) SetTStmt(stmt *motrace.StatementInfo) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.tStmt = stmt
}

const saveQueryIdCnt = 10

func (ses *Session) pushQueryId(uuid string) {
	if len(ses.queryId) > saveQueryIdCnt {
		ses.queryId = ses.queryId[1:]
	}
	ses.queryId = append(ses.queryId, uuid)
}

func (ses *Session) getQueryId(internalSql bool) []string {
	if internalSql {
		cnt := len(ses.queryId)
		//the last one is cnt-1
		if cnt > 0 {
			return ses.queryId[:cnt-1]
		} else {
			return ses.queryId[:cnt]
		}
	}
	return ses.queryId
}

type errInfo struct {
	codes  []uint16
	msgs   []string
	maxCnt int
}

func (e *errInfo) push(code uint16, msg string) {
	if e.maxCnt > 0 && len(e.codes) > e.maxCnt {
		e.codes = e.codes[1:]
		e.msgs = e.msgs[1:]
	}
	e.codes = append(e.codes, code)
	e.msgs = append(e.msgs, msg)
}

func (e *errInfo) length() int {
	return len(e.codes)
}

func NewSession(connCtx context.Context, proto MysqlProtocol, mp *mpool.MPool, gSysVars *GlobalSystemVariables, isNotBackgroundSession bool, sharedTxnHandler *TxnHandler) *Session {
	//if the sharedTxnHandler exists,we use its txnCtx and txnOperator in this session.
	//Currently, we only use the sharedTxnHandler in the background session.
	var txnOp TxnOperator
	var err error
	if sharedTxnHandler != nil {
		if !sharedTxnHandler.InActiveTxn() {
			panic("shared txn is invalid")
		}
		txnOp = sharedTxnHandler.GetTxn()
	}
	txnHandler := InitTxnHandler(getGlobalPu().StorageEngine, connCtx, txnOp)

	ses := &Session{
		feSessionImpl: feSessionImpl{
			proto:      proto,
			pool:       mp,
			txnHandler: txnHandler,
			//TODO:fix database name after the catalog is ready
			txnCompileCtx:  InitTxnCompilerContext(proto.GetDatabaseName()),
			gSysVars:       gSysVars,
			outputCallback: getDataFromPipeline,
			timeZone:       time.Local,
		},
		errInfo: &errInfo{
			codes:  make([]uint16, 0, MoDefaultErrorCount),
			msgs:   make([]string, 0, MoDefaultErrorCount),
			maxCnt: MoDefaultErrorCount,
		},
		cache:     &privilegeCache{},
		blockIdx:  0,
		planCache: newPlanCache(100),
		startedAt: time.Now(),
		connType:  ConnTypeUnset,

		timestampMap: map[TS]time.Time{},
		statsCache:   plan2.NewStatsCache(),
	}
	if isNotBackgroundSession {
		ses.sysVars = gSysVars.CopySysVarsToSession()
		ses.userDefinedVars = make(map[string]*UserDefinedVar)
		ses.prepareStmts = make(map[string]*PrepareStmt)
		// For seq init values.
		ses.seqCurValues = make(map[uint64]string)
		ses.seqLastValue = new(string)
	}

	ses.buf = buffer.New()
	ses.isNotBackgroundSession = isNotBackgroundSession
	ses.sqlHelper = &SqlHelper{ses: ses}
	ses.uuid, _ = uuid.NewV7()
	if ses.pool == nil {
		// If no mp, we create one for session.  Use GuestMmuLimitation as cap.
		// fixed pool size can be another param, or should be computed from cap,
		// but here, too lazy, just use Mid.
		//
		// XXX MPOOL
		// We don't have a way to close a session, so the only sane way of creating
		// a mpool is to use NoFixed
		ses.pool, err = mpool.NewMPool("pipeline-"+ses.GetUUIDString(), getGlobalPu().SV.GuestMmuLimitation, mpool.NoFixed)
		if err != nil {
			panic(err)
		}
	}
	ses.proc = process.New(
		context.TODO(),
		ses.pool,
		getGlobalPu().TxnClient,
		nil,
		getGlobalPu().FileService,
		getGlobalPu().LockService,
		getGlobalPu().QueryClient,
		getGlobalPu().HAKeeperClient,
		getGlobalPu().UdfService,
		getGlobalAic())

	ses.proc.Lim.Size = getGlobalPu().SV.ProcessLimitationSize
	ses.proc.Lim.BatchRows = getGlobalPu().SV.ProcessLimitationBatchRows
	ses.proc.Lim.MaxMsgSize = getGlobalPu().SV.MaxMessageSize
	ses.proc.Lim.PartitionRows = getGlobalPu().SV.ProcessLimitationPartitionRows

	ses.proc.SetStmtProfile(&ses.stmtProfile)
	// ses.proc.SetResolveVariableFunc(ses.txnCompileCtx.ResolveVariable)

	runtime.SetFinalizer(ses, func(ss *Session) {
		ss.Close()
	})
	return ses
}

func (ses *Session) Close() {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.feSessionImpl.Close()
	ses.feSessionImpl.Clear()
	ses.proto = nil
	ses.mrs = nil
	ses.data = nil
	ses.ep = nil
	if ses.txnHandler != nil {
		ses.txnHandler.Close()
		ses.txnHandler = nil
	}
	if ses.txnCompileCtx != nil {
		ses.txnCompileCtx.execCtx = nil
		ses.txnCompileCtx = nil
	}
	ses.sql = ""
	ses.sysVars = nil
	ses.userDefinedVars = nil
	ses.gSysVars = nil
	for _, stmt := range ses.prepareStmts {
		stmt.Close()
	}
	ses.prepareStmts = nil
	ses.allResultSet = nil
	ses.tenant = nil
	ses.priv = nil
	ses.errInfo = nil
	ses.cache = nil
	ses.debugStr = ""
	ses.tStmt = nil
	ses.ast = nil
	ses.rs = nil
	ses.queryId = nil
	ses.p = nil
	ses.planCache = nil
	ses.seqCurValues = nil
	ses.seqLastValue = nil
	if ses.sqlHelper != nil {
		ses.sqlHelper.ses = nil
		ses.sqlHelper = nil
	}
	ses.ClearStmtProfile()
	//  The mpool cleanup must be placed at the end,
	// and you must wait for all resources to be cleaned up before you can delete the mpool
	if ses.proc != nil {
		ses.proc.FreeVectors()
		bats := ses.proc.GetValueScanBatchs()
		for _, bat := range bats {
			bat.Clean(ses.proc.Mp())
		}
		ses.proc = nil
	}
	for _, bat := range ses.resultBatches {
		bat.Clean(ses.pool)
	}
	if ses.isNotBackgroundSession {
		pool := ses.GetMemPool()
		mpool.DeleteMPool(pool)
		ses.SetMemPool(nil)
	}
	if ses.buf != nil {
		ses.buf.Free()
		ses.buf = nil
	}

	ses.timestampMap = nil
	ses.upstream = nil
	ses.rm = nil
	ses.rt = nil
}

func (ses *Session) Clear() {
	ses.feSessionImpl.Clear()
}

func (ses *Session) GetIncBlockIdx() int {
	ses.blockIdx++
	return ses.blockIdx
}

func (ses *Session) ResetBlockIdx() {
	ses.blockIdx = 0
}

func (ses *Session) IsBackgroundSession() bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return !ses.isNotBackgroundSession
}

func (ses *Session) cachePlan(sql string, stmts []tree.Statement, plans []*plan.Plan) {
	if len(sql) == 0 {
		return
	}
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.planCache.cache(sql, stmts, plans)
}

func (ses *Session) getCachedPlan(sql string) *cachedPlan {
	if len(sql) == 0 {
		return nil
	}
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.planCache.get(sql)
}

func (ses *Session) isCached(sql string) bool {
	if len(sql) == 0 {
		return false
	}
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.planCache.isCached(sql)
}

func (ses *Session) cleanCache() {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.planCache.clean()
}

func (ses *Session) UpdateDebugString() {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	sb := bytes.Buffer{}
	//option connection id , ip
	if ses.proto != nil {
		sb.WriteString(fmt.Sprintf("connectionId %d", ses.proto.ConnectionID()))
		sb.WriteByte('|')
		sb.WriteString(ses.proto.Peer())
	}
	sb.WriteByte('|')
	//account info
	if ses.tenant != nil {
		sb.WriteString(ses.tenant.String())
	} else {
		acc := getDefaultAccount()
		sb.WriteString(acc.String())
	}
	sb.WriteByte('|')
	//go routine id
	if ses.rt != nil {
		sb.WriteString(fmt.Sprintf("goRoutineId %d", ses.rt.getGoroutineId()))
		sb.WriteByte('|')
	}
	//session id
	sb.WriteString(ses.uuid.String())
	//upstream sessionid
	if ses.upstream != nil {
		sb.WriteByte('|')
		sb.WriteString(ses.upstream.uuid.String())
	}

	ses.debugStr = sb.String()
}

func (ses *Session) GetPrivilegeCache() *privilegeCache {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.cache
}

func (ses *Session) InvalidatePrivilegeCache() {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.cache.invalidate()
}

// GetBackgroundExec generates a background executor
func (ses *Session) GetBackgroundExec(ctx context.Context) BackgroundExec {
	return NewBackgroundExec(
		ctx,
		ses,
		ses.GetMemPool())
}

// GetShareTxnBackgroundExec returns a background executor running the sql in a shared transaction.
// newRawBatch denotes we need the raw batch instead of mysql result set.
func (ses *Session) GetShareTxnBackgroundExec(ctx context.Context, newRawBatch bool) BackgroundExec {
	var txnOp TxnOperator
	if ses.GetTxnHandler() != nil {
		txnOp = ses.GetTxnHandler().GetTxn()
	}

	txnHandler := InitTxnHandler(getGlobalPu().StorageEngine, ses.GetTxnHandler().GetConnCtx(), txnOp)
	var callback outputCallBackFunc
	if newRawBatch {
		callback = batchFetcher2
	} else {
		callback = fakeDataSetFetcher2
	}
	backSes := &backSession{
		feSessionImpl: feSessionImpl{
			pool:           ses.pool,
			proto:          &FakeProtocol{},
			buf:            buffer.New(),
			stmtProfile:    process.StmtProfile{},
			tenant:         nil,
			txnHandler:     txnHandler,
			txnCompileCtx:  InitTxnCompilerContext(ses.proto.GetDatabaseName()),
			mrs:            nil,
			outputCallback: callback,
			allResultSet:   nil,
			resultBatches:  nil,
			derivedStmt:    false,
			gSysVars:       GSysVariables,
			label:          make(map[string]string),
			timeZone:       time.Local,
		},
	}
	backSes.uuid, _ = uuid.NewV7()
	bh := &backExec{
		backSes: backSes,
	}
	//the derived statement execute in a shared transaction in background session
	bh.backSes.ReplaceDerivedStmt(true)
	return bh
}

var GetRawBatchBackgroundExec = func(ctx context.Context, ses *Session) BackgroundExec {
	return ses.GetRawBatchBackgroundExec(ctx)
}

func (ses *Session) GetRawBatchBackgroundExec(ctx context.Context) BackgroundExec {
	txnHandler := InitTxnHandler(getGlobalPu().StorageEngine, ses.GetTxnHandler().GetConnCtx(), nil)
	backSes := &backSession{
		feSessionImpl: feSessionImpl{
			pool:           ses.GetMemPool(),
			proto:          &FakeProtocol{},
			buf:            buffer.New(),
			stmtProfile:    process.StmtProfile{},
			tenant:         nil,
			txnHandler:     txnHandler,
			txnCompileCtx:  InitTxnCompilerContext(""),
			mrs:            nil,
			outputCallback: batchFetcher2,
			allResultSet:   nil,
			resultBatches:  nil,
			derivedStmt:    false,
			gSysVars:       GSysVariables,
			label:          make(map[string]string),
			timeZone:       time.Local,
		},
	}
	backSes.uuid, _ = uuid.NewV7()
	bh := &backExec{
		backSes: backSes,
	}
	return bh
}

func (ses *Session) GetIsInternal() bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.isInternal
}

func (ses *Session) GetData() [][]interface{} {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.data
}

func (ses *Session) SetData(data [][]interface{}) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.data = data
}

func (ses *Session) AppendData(row []interface{}) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.data = append(ses.data, row)
}

func (ses *Session) InitExportConfig(ep *tree.ExportParam) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.ep = &ExportConfig{userConfig: ep}
}

func (ses *Session) GetExportConfig() *ExportConfig {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.ep
}

func (ses *Session) ClearExportParam() {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.ep = nil
}

func (ses *Session) SetShowStmtType(sst ShowStatementType) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.showStmtType = sst
}

func (ses *Session) GetShowStmtType() ShowStatementType {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.showStmtType
}

func (ses *Session) GetOutputCallback(execCtx *ExecCtx) func(*batch.Batch) error {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return func(bat *batch.Batch) error {
		return ses.outputCallback(ses, execCtx, bat)
	}
}

func (ses *Session) GetErrInfo() *errInfo {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.errInfo
}

func (ses *Session) GenNewStmtId() uint32 {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.lastStmtId = ses.lastStmtId + 1
	return ses.lastStmtId
}

func (ses *Session) SetLastStmtID(id uint32) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.lastStmtId = id
}

func (ses *Session) GetLastStmtId() uint32 {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.lastStmtId
}

func (ses *Session) SetLastInsertID(num uint64) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.lastInsertID = num
}

func (ses *Session) GetLastInsertID() uint64 {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.lastInsertID
}

func (ses *Session) SetCmd(cmd CommandType) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.cmd = cmd
}

func (ses *Session) GetCmd() CommandType {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.cmd
}

// GetTenantName return tenant name according to GetTenantInfo and stmt.
//
// With stmt = nil, should be only called in TxnHandler.NewTxn, TxnHandler.CommitTxn, TxnHandler.RollbackTxn
func (ses *Session) GetTenantNameWithStmt(stmt tree.Statement) string {
	tenant := sysAccountName
	if ses.GetTenantInfo() != nil && (stmt == nil || !IsPrepareStatement(stmt)) {
		tenant = ses.GetTenantInfo().GetTenant()
	}
	return tenant
}

func (ses *Session) GetTenantName() string {
	return ses.GetTenantNameWithStmt(nil)
}

func (ses *Session) SetPrepareStmt(ctx context.Context, name string, prepareStmt *PrepareStmt) error {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if stmt, ok := ses.prepareStmts[name]; !ok {
		if len(ses.prepareStmts) >= MaxPrepareNumberInOneSession {
			return moerr.NewInvalidState(ctx, "too many prepared statement, max %d", MaxPrepareNumberInOneSession)
		}
	} else {
		stmt.Close()
	}
	if prepareStmt != nil && prepareStmt.PreparePlan != nil {
		isInsertValues, exprList := checkPlanIsInsertValues(ses.proc,
			prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
		if isInsertValues {
			prepareStmt.proc = ses.proc
			prepareStmt.exprList = exprList
		}
	}
	ses.prepareStmts[name] = prepareStmt

	return nil
}

func (ses *Session) GetPrepareStmt(ctx context.Context, name string) (*PrepareStmt, error) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if prepareStmt, ok := ses.prepareStmts[name]; ok {
		return prepareStmt, nil
	}
	var connID uint32
	if ses.proto != nil {
		connID = ses.proto.ConnectionID()
	}
	logutil.Errorf("prepared statement '%s' does not exist on connection %d", name, connID)
	return nil, moerr.NewInvalidState(ctx, "prepared statement '%s' does not exist", name)
}

func (ses *Session) GetPrepareStmts() []*PrepareStmt {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ret := make([]*PrepareStmt, 0, len(ses.prepareStmts))
	for _, st := range ses.prepareStmts {
		ret = append(ret, st)
	}
	return ret
}

func (ses *Session) RemovePrepareStmt(name string) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if stmt, ok := ses.prepareStmts[name]; ok {
		stmt.Close()
	}
	delete(ses.prepareStmts, name)
}

func (ses *Session) SetSysVar(name string, value interface{}) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.sysVars[name] = value
}

func (ses *Session) GetSysVar(name string) interface{} {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.sysVars[name]
}

func (ses *Session) GetSysVars() map[string]interface{} {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.sysVars
}

// SetGlobalVar sets the value of system variable in global.
// used by SET GLOBAL
func (ses *Session) SetGlobalVar(ctx context.Context, name string, value interface{}) error {
	return ses.GetGlobalSysVars().SetGlobalSysVar(ctx, name, value)
}

// GetGlobalVar gets this value of the system variable in global
func (ses *Session) GetGlobalVar(ctx context.Context, name string) (interface{}, error) {
	gSysVars := ses.GetGlobalSysVars()
	if def, val, ok := gSysVars.GetGlobalSysVar(name); ok {
		if def.GetScope() == ScopeSession {
			//empty
			return nil, moerr.NewInternalError(ctx, errorSystemVariableSessionEmpty())
		}
		return val, nil
	}
	return nil, moerr.NewInternalError(ctx, errorSystemVariableDoesNotExist())
}

// SetSessionVar sets the value of system variable in session
func (ses *Session) SetSessionVar(ctx context.Context, name string, value interface{}) error {
	gSysVars := ses.GetGlobalSysVars()
	if def, _, ok := gSysVars.GetGlobalSysVar(name); ok {
		if def.GetScope() == ScopeGlobal {
			return moerr.NewInternalError(ctx, errorSystemVariableIsGlobal())
		}
		//scope session & both
		if !def.GetDynamic() {
			return moerr.NewInternalError(ctx, errorSystemVariableIsReadOnly())
		}

		cv, err := def.GetType().Convert(value)
		if err != nil {
			errutil.ReportError(ctx, err)
			return err
		}

		if def.UpdateSessVar == nil {
			ses.SetSysVar(def.GetName(), cv)
		} else {
			return def.UpdateSessVar(ctx, ses, ses.GetSysVars(), def.GetName(), cv)
		}
	} else {
		return moerr.NewInternalError(ctx, errorSystemVariableDoesNotExist())
	}
	return nil
}

// InitSetSessionVar sets the value of system variable in session when start a connection
func (ses *Session) InitSetSessionVar(ctx context.Context, name string, value interface{}) error {
	gSysVars := ses.GetGlobalSysVars()
	if def, _, ok := gSysVars.GetGlobalSysVar(name); ok {
		cv, err := def.GetType().Convert(value)
		if err != nil {
			errutil.ReportError(ctx, moerr.NewInternalError(context.Background(), "init variable fail: variable %s convert to the system variable type %s failed, bad value %v", name, def.GetType().String(), value))
		}

		if def.UpdateSessVar == nil {
			ses.SetSysVar(def.GetName(), cv)
		} else {
			return def.UpdateSessVar(ctx, ses, ses.GetSysVars(), def.GetName(), cv)
		}
	}
	return nil
}

// GetSessionVar gets this value of the system variable in session
func (ses *Session) GetSessionVar(ctx context.Context, name string) (interface{}, error) {
	gSysVars := ses.GetGlobalSysVars()
	if def, gVal, ok := gSysVars.GetGlobalSysVar(name); ok {
		ciname := strings.ToLower(name)
		if def.GetScope() == ScopeGlobal {
			return gVal, nil
		}
		return ses.GetSysVar(ciname), nil
	} else {
		return nil, moerr.NewInternalError(ctx, errorSystemVariableDoesNotExist())
	}
}

func (ses *Session) CopyAllSessionVars() map[string]interface{} {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	cp := make(map[string]interface{})
	for k, v := range ses.sysVars {
		cp[k] = v
	}
	return cp
}

// SetUserDefinedVar sets the user defined variable to the value in session
func (ses *Session) SetUserDefinedVar(name string, value interface{}, sql string) error {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.userDefinedVars[strings.ToLower(name)] = &UserDefinedVar{Value: value, Sql: sql}
	return nil
}

// GetUserDefinedVar gets value of the user defined variable
func (ses *Session) GetUserDefinedVar(name string) (SystemVariableType, *UserDefinedVar, error) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	val, ok := ses.userDefinedVars[strings.ToLower(name)]
	if !ok {
		return SystemVariableNullType{}, nil, nil
	}
	return InitSystemVariableStringType(name), val, nil
}

func (ses *Session) GetTxnInfo() string {
	txnH := ses.GetTxnHandler()
	if txnH == nil {
		return ""
	}
	txnOp := txnH.GetTxn()
	if txnOp == nil {
		return ""
	}
	meta := txnOp.Txn()
	return meta.DebugString()
}

func (ses *Session) GetDatabaseName() string {
	return ses.GetMysqlProtocol().GetDatabaseName()
}

func (ses *Session) SetDatabaseName(db string) {
	ses.GetMysqlProtocol().SetDatabaseName(db)
	ses.GetTxnCompileCtx().SetDatabase(db)
}

func (ses *Session) DatabaseNameIsEmpty() bool {
	return len(ses.GetDatabaseName()) == 0
}

func (ses *Session) SetUserName(uname string) {
	ses.GetMysqlProtocol().SetUserName(uname)
}

func (ses *Session) GetConnectionID() uint32 {
	protocol := ses.GetMysqlProtocol()
	if protocol != nil {
		return ses.GetMysqlProtocol().ConnectionID()
	}
	return 0
}

func (ses *Session) skipAuthForSpecialUser() bool {
	if ses.GetTenantInfo() != nil {
		ok, _, _ := isSpecialUser(ses.GetTenantInfo().GetUser())
		return ok
	}
	return false
}

// AuthenticateUser Verify the user's password, and if the login information contains the database name, verify if the database exists
func (ses *Session) AuthenticateUser(ctx context.Context, userInput string, dbName string, authResponse []byte, salt []byte, checkPassword func(pwd []byte, salt []byte, auth []byte) bool) ([]byte, error) {
	var defaultRoleID int64
	var defaultRole string
	var tenant *TenantInfo
	var err error
	var rsset []ExecResult
	var tenantID int64
	var userID int64
	var pwd, accountStatus string
	var accountVersion uint64
	//var createVersion string
	var pwdBytes []byte
	var isSpecial bool
	var specialAccount *TenantInfo

	//Get tenant info
	tenant, err = GetTenantInfo(ctx, userInput)
	if err != nil {
		return nil, err
	}

	ses.SetTenantInfo(tenant)
	ses.UpdateDebugString()
	sessionInfo := ses.GetDebugString()

	logDebugf(sessionInfo, "check special user")
	// check the special user for initilization
	isSpecial, pwdBytes, specialAccount = isSpecialUser(tenant.GetUser())
	if isSpecial && specialAccount.IsMoAdminRole() {
		ses.SetTenantInfo(specialAccount)
		if len(ses.requestLabel) == 0 {
			ses.requestLabel = db_holder.GetLabelSelector()
		}
		return GetPassWord(HashPassWordWithByte(pwdBytes))
	}

	ses.SetTenantInfo(tenant)

	//step1 : check tenant exists or not in SYS tenant context
	ses.timestampMap[TSCheckTenantStart] = time.Now()
	sysTenantCtx := defines.AttachAccount(ctx, uint32(sysAccountID), uint32(rootID), uint32(moAdminRoleID))
	sqlForCheckTenant, err := getSqlForCheckTenant(sysTenantCtx, tenant.GetTenant())
	if err != nil {
		return nil, err
	}
	mp := ses.GetMemPool()
	logDebugf(sessionInfo, "check tenant %s exists", tenant)
	rsset, err = executeSQLInBackgroundSession(sysTenantCtx, ses, mp, sqlForCheckTenant)
	if err != nil {
		return nil, err
	}
	if !execResultArrayHasData(rsset) {
		return nil, moerr.NewInternalError(sysTenantCtx, "there is no tenant %s", tenant.GetTenant())
	}

	//account id
	tenantID, err = rsset[0].GetInt64(sysTenantCtx, 0, 0)
	if err != nil {
		return nil, err
	}

	//account status
	accountStatus, err = rsset[0].GetString(sysTenantCtx, 0, 2)
	if err != nil {
		return nil, err
	}

	//account version
	accountVersion, err = rsset[0].GetUint64(sysTenantCtx, 0, 3)
	if err != nil {
		return nil, err
	}

	if strings.ToLower(accountStatus) == tree.AccountStatusSuspend.String() {
		return nil, moerr.NewInternalError(sysTenantCtx, "Account %s is suspended", tenant.GetTenant())
	}

	if strings.ToLower(accountStatus) == tree.AccountStatusRestricted.String() {
		ses.getRoutine().setResricted(true)
	} else {
		ses.getRoutine().setResricted(false)
	}

	tenant.SetTenantID(uint32(tenantID))
	ses.timestampMap[TSCheckTenantEnd] = time.Now()
	v2.CheckTenantDurationHistogram.Observe(ses.timestampMap[TSCheckTenantEnd].Sub(ses.timestampMap[TSCheckTenantStart]).Seconds())

	//step2 : check user exists or not in general tenant.
	//step3 : get the password of the user

	ses.timestampMap[TSCheckUserStart] = time.Now()
	tenantCtx := defines.AttachAccountId(ctx, uint32(tenantID))

	logDebugf(sessionInfo, "check user of %s exists", tenant)
	//Get the password of the user in an independent session
	sqlForPasswordOfUser, err := getSqlForPasswordOfUser(tenantCtx, tenant.GetUser())
	if err != nil {
		return nil, err
	}
	rsset, err = executeSQLInBackgroundSession(tenantCtx, ses, mp, sqlForPasswordOfUser)
	if err != nil {
		return nil, err
	}
	if !execResultArrayHasData(rsset) {
		return nil, moerr.NewInternalError(tenantCtx, "there is no user %s", tenant.GetUser())
	}

	userID, err = rsset[0].GetInt64(tenantCtx, 0, 0)
	if err != nil {
		return nil, err
	}

	pwd, err = rsset[0].GetString(tenantCtx, 0, 1)
	if err != nil {
		return nil, err
	}

	//the default_role in the mo_user table.
	//the default_role is always valid. public or other valid role.
	defaultRoleID, err = rsset[0].GetInt64(tenantCtx, 0, 2)
	if err != nil {
		return nil, err
	}

	tenant.SetUserID(uint32(userID))
	tenant.SetDefaultRoleID(uint32(defaultRoleID))
	ses.timestampMap[TSCheckUserEnd] = time.Now()
	v2.CheckUserDurationHistogram.Observe(ses.timestampMap[TSCheckUserEnd].Sub(ses.timestampMap[TSCheckUserStart]).Seconds())

	/*
		login case 1: tenant:user
		1.get the default_role of the user in mo_user

		login case 2: tenant:user:role
		1.check the role has been granted to the user
			-yes: go on
			-no: error

	*/
	//it denotes that there is no default role in the input
	if tenant.HasDefaultRole() {
		logDebugf(sessionInfo, "check default role of user %s.", tenant)
		//step4 : check role exists or not
		ses.timestampMap[TSCheckRoleStart] = time.Now()
		sqlForCheckRoleExists, err := getSqlForRoleIdOfRole(tenantCtx, tenant.GetDefaultRole())
		if err != nil {
			return nil, err
		}
		rsset, err = executeSQLInBackgroundSession(tenantCtx, ses, mp, sqlForCheckRoleExists)
		if err != nil {
			return nil, err
		}

		if !execResultArrayHasData(rsset) {
			return nil, moerr.NewInternalError(tenantCtx, "there is no role %s", tenant.GetDefaultRole())
		}

		logDebugf(sessionInfo, "check granted role of user %s.", tenant)
		//step4.2 : check the role has been granted to the user or not
		sqlForRoleOfUser, err := getSqlForRoleOfUser(tenantCtx, userID, tenant.GetDefaultRole())
		if err != nil {
			return nil, err
		}
		rsset, err = executeSQLInBackgroundSession(tenantCtx, ses, mp, sqlForRoleOfUser)
		if err != nil {
			return nil, err
		}
		if !execResultArrayHasData(rsset) {
			return nil, moerr.NewInternalError(tenantCtx, "the role %s has not been granted to the user %s",
				tenant.GetDefaultRole(), tenant.GetUser())
		}

		defaultRoleID, err = rsset[0].GetInt64(tenantCtx, 0, 0)
		if err != nil {
			return nil, err
		}
		tenant.SetDefaultRoleID(uint32(defaultRoleID))
		ses.timestampMap[TSCheckRoleEnd] = time.Now()
		v2.CheckRoleDurationHistogram.Observe(ses.timestampMap[TSCheckRoleEnd].Sub(ses.timestampMap[TSCheckRoleStart]).Seconds())
	} else {
		ses.timestampMap[TSCheckRoleStart] = time.Now()
		logDebugf(sessionInfo, "check designated role of user %s.", tenant)
		//the get name of default_role from mo_role
		sql := getSqlForRoleNameOfRoleId(defaultRoleID)
		rsset, err = executeSQLInBackgroundSession(tenantCtx, ses, mp, sql)
		if err != nil {
			return nil, err
		}
		if !execResultArrayHasData(rsset) {
			return nil, moerr.NewInternalError(tenantCtx, "get the default role of the user %s failed", tenant.GetUser())
		}

		defaultRole, err = rsset[0].GetString(tenantCtx, 0, 0)
		if err != nil {
			return nil, err
		}
		tenant.SetDefaultRole(defaultRole)
		ses.timestampMap[TSCheckRoleEnd] = time.Now()
		v2.CheckRoleDurationHistogram.Observe(ses.timestampMap[TSCheckRoleEnd].Sub(ses.timestampMap[TSCheckRoleStart]).Seconds())
	}
	//------------------------------------------------------------------------------------------------------------------
	psw, err := GetPassWord(pwd)
	if err != nil {
		return nil, err
	}

	// TO Check password
	if checkPassword(psw, salt, authResponse) {
		logDebugf(sessionInfo, "check password succeeded")
		ses.InitGlobalSystemVariables(tenantCtx)
	} else {
		return nil, moerr.NewInternalError(tenantCtx, "check password failed")
	}

	// If the login information contains the database name, verify if the database exists
	if dbName != "" {
		ses.timestampMap[TSCheckDbNameStart] = time.Now()
		_, err = executeSQLInBackgroundSession(tenantCtx, ses, mp, "use "+dbName)
		if err != nil {
			return nil, err
		}
		logDebugf(sessionInfo, "check database name succeeded")
		ses.timestampMap[TSCheckDbNameEnd] = time.Now()
		v2.CheckDbNameDurationHistogram.Observe(ses.timestampMap[TSCheckDbNameEnd].Sub(ses.timestampMap[TSCheckDbNameStart]).Seconds())
	}
	//------------------------------------------------------------------------------------------------------------------
	// record the id :routine pair in RoutineManager
	ses.getRoutineManager().accountRoutine.recordRountine(tenantID, ses.getRoutine(), accountVersion)
	logInfo(ses, sessionInfo, tenant.String())

	return GetPassWord(pwd)
}

func (ses *Session) MaybeUpgradeTenant(ctx context.Context, curVersion string, tenantID int64) error {
	// Get mo final version, which is based on the current code version
	finalVersion := ses.rm.baseService.GetFinalVersion()
	if versions.Compare(curVersion, finalVersion) <= 0 {
		return ses.rm.baseService.CheckTenantUpgrade(ctx, tenantID)
	}
	return nil
}

func (ses *Session) UpgradeTenant(ctx context.Context, tenantName string, retryCount uint32, isALLAccount bool) error {
	// Get mo final version, which is based on the current code version
	return ses.rm.baseService.UpgradeTenant(ctx, tenantName, retryCount, isALLAccount)
}

func (ses *Session) InitGlobalSystemVariables(ctx context.Context) error {
	var err error
	var rsset []ExecResult
	ses.timestampMap[TSInitGlobalSysVarStart] = time.Now()
	defer func() {
		ses.timestampMap[TSInitGlobalSysVarEnd] = time.Now()
		v2.InitGlobalSysVarDurationHistogram.Observe(ses.timestampMap[TSInitGlobalSysVarEnd].Sub(ses.timestampMap[TSInitGlobalSysVarStart]).Seconds())
	}()

	tenantInfo := ses.GetTenantInfo()
	// if is system account
	if tenantInfo.IsSysTenant() {
		sysTenantCtx := defines.AttachAccount(ctx, uint32(sysAccountID), uint32(rootID), uint32(moAdminRoleID))

		// get system variable from mo_mysql_compatibility mode
		sqlForGetVariables := getSystemVariablesWithAccount(sysAccountID)
		mp := ses.GetMemPool()

		rsset, err = executeSQLInBackgroundSession(
			sysTenantCtx,
			ses,
			mp,
			sqlForGetVariables)
		if err != nil {
			return err
		}
		if execResultArrayHasData(rsset) {
			for i := uint64(0); i < rsset[0].GetRowCount(); i++ {
				variable_name, err := rsset[0].GetString(sysTenantCtx, i, 0)
				if err != nil {
					return err
				}
				variable_value, err := rsset[0].GetString(sysTenantCtx, i, 1)
				if err != nil {
					return err
				}

				if sv, ok := gSysVarsDefs[variable_name]; ok {
					if !sv.GetDynamic() || (sv.Scope != ScopeGlobal && sv.Scope != ScopeBoth) {
						continue
					}
					val, err := sv.GetType().ConvertFromString(variable_value)
					if err != nil {
						errutil.ReportError(ctx, moerr.NewInternalError(context.Background(), "init variable fail: variable %s convert from string value to the system variable type %s failed, bad value %s", variable_name, sv.Type.String(), variable_value))
						return err
					}
					err = ses.InitSetSessionVar(sysTenantCtx, variable_name, val)
					if err != nil {
						errutil.ReportError(ctx, moerr.NewInternalError(context.Background(), "init variable fail: variable %s convert from string value to the system variable type %s failed, bad value %s", variable_name, sv.Type.String(), variable_value))
					}
				}
			}
		} else {
			return moerr.NewInternalError(sysTenantCtx, "there is no data in mo_mysql_compatibility_mode table for account %s", sysAccountName)
		}
	} else {
		tenantCtx := defines.AttachAccount(ctx, tenantInfo.GetTenantID(), tenantInfo.GetUserID(), uint32(accountAdminRoleID))

		// get system variable from mo_mysql_compatibility mode
		sqlForGetVariables := getSystemVariablesWithAccount(uint64(tenantInfo.GetTenantID()))
		mp := ses.GetMemPool()

		rsset, err = executeSQLInBackgroundSession(
			tenantCtx,
			ses,
			mp,
			sqlForGetVariables)
		if err != nil {
			return err
		}
		if execResultArrayHasData(rsset) {
			for i := uint64(0); i < rsset[0].GetRowCount(); i++ {
				variable_name, err := rsset[0].GetString(tenantCtx, i, 0)
				if err != nil {
					return err
				}
				variable_value, err := rsset[0].GetString(tenantCtx, i, 1)
				if err != nil {
					return err
				}

				if sv, ok := gSysVarsDefs[variable_name]; ok {
					if !sv.Dynamic || sv.GetScope() == ScopeSession {
						continue
					}
					val, err := sv.GetType().ConvertFromString(variable_value)
					if err != nil {
						return err
					}
					err = ses.InitSetSessionVar(tenantCtx, variable_name, val)
					if err != nil {
						return err
					}
				}
			}
		} else {
			return moerr.NewInternalError(tenantCtx, "there is no data in  mo_mysql_compatibility_mode table for account %s", tenantInfo.GetTenant())
		}
	}
	return err
}

func (ses *Session) GetPrivilege() *privilege {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.priv
}

func (ses *Session) SetPrivilege(priv *privilege) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.priv = priv
}

func (ses *Session) SetFromRealUser(b bool) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.fromRealUser = b
}

func (ses *Session) GetFromRealUser() bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.fromRealUser
}

func changeVersion(ctx context.Context, ses *Session, db string) error {
	var err error
	if _, ok := bannedCatalogDatabases[db]; ok {
		return err
	}
	version, _ := GetVersionCompatibility(ctx, ses, db)
	if ses.GetTenantInfo() != nil {
		ses.GetTenantInfo().SetVersion(version)
	}
	return err
}

// getCNLabels returns requested CN labels.
func (ses *Session) getCNLabels() map[string]string {
	return ses.requestLabel
}

// getSystemVariableValue get the system vaiables value from the mo_mysql_compatibility_mode table
func (ses *Session) GetGlobalSystemVariableValue(ctx context.Context, varName string) (val interface{}, err error) {
	var sql string
	//var err error
	var erArray []ExecResult
	var accountId uint32
	var variableValue string
	// check the variable name isValid or not
	_, err = ses.GetGlobalVar(ctx, varName)
	if err != nil {
		return nil, err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return nil, err
	}
	if tenantInfo := ses.GetTenantInfo(); tenantInfo != nil {
		accountId = tenantInfo.GetTenantID()
	}
	sql = getSqlForGetSystemVariableValueWithAccount(uint64(accountId), varName)

	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	if execResultArrayHasData(erArray) {
		variableValue, err = erArray[0].GetString(ctx, 0, 0)
		if err != nil {
			return nil, err
		}
		if sv, ok := gSysVarsDefs[varName]; ok {
			val, err = sv.GetType().ConvertFromString(variableValue)
			if err != nil {
				return nil, err
			}
			return val, nil
		}
	}

	return nil, moerr.NewInternalError(ctx, "can not resolve global system variable %s", varName)
}

func (ses *Session) SetNewResponse(category int, affectedRows uint64, cmd int, d interface{}, isLastStmt bool) *Response {
	// If the stmt has next stmt, should add SERVER_MORE_RESULTS_EXISTS to the server status.
	var resp *Response
	if !isLastStmt {
		resp = NewResponse(category, affectedRows, 0, 0,
			ses.GetTxnHandler().GetServerStatus()|SERVER_MORE_RESULTS_EXISTS, cmd, d)
	} else {
		resp = NewResponse(category, affectedRows, 0, 0, ses.GetTxnHandler().GetServerStatus(), cmd, d)
	}
	return resp
}

// StatusSession implements the queryservice.Session interface.
func (ses *Session) StatusSession() *status.Session {
	var (
		accountName string
		userName    string
		roleName    string
	)

	accountName, userName, roleName = getUserProfile(ses.GetTenantInfo())
	//if the query is processing, the end time is invalid.
	//we can not clear the session info under this condition.
	if !ses.GetQueryInProgress() {
		endAt := ses.GetQueryEnd()
		//if the current time is more than 3 second after the query end time, the session is timeout.
		//we clear the session statement info
		//for issue 11976
		if time.Since(endAt) > 3*time.Second {
			return &status.Session{
				NodeID:        ses.getRoutineManager().baseService.ID(),
				ConnID:        ses.GetConnectionID(),
				SessionID:     ses.GetUUIDString(),
				Account:       accountName,
				User:          userName,
				Host:          ses.getRoutineManager().baseService.SQLAddress(),
				DB:            ses.GetDatabaseName(),
				SessionStart:  ses.GetSessionStart(),
				Command:       "",
				Info:          "",
				TxnID:         uuid2Str(ses.GetTxnId()),
				StatementID:   "",
				StatementType: "",
				QueryType:     "",
				SQLSourceType: "",
				QueryStart:    time.Time{},
				ClientHost:    ses.clientAddr,
				Role:          roleName,
				FromProxy:     ses.fromProxy,
				ProxyHost:     ses.proxyAddr,
			}
		}
	}
	return &status.Session{
		NodeID:        ses.getRoutineManager().baseService.ID(),
		ConnID:        ses.GetConnectionID(),
		SessionID:     ses.GetUUIDString(),
		Account:       accountName,
		User:          userName,
		Host:          ses.getRoutineManager().baseService.SQLAddress(),
		DB:            ses.GetDatabaseName(),
		SessionStart:  ses.GetSessionStart(),
		Command:       ses.GetCmd().String(),
		Info:          ses.GetSqlOfStmt(),
		TxnID:         uuid2Str(ses.GetTxnId()),
		StatementID:   ses.GetStmtId().String(),
		StatementType: ses.GetStmtType(),
		QueryType:     ses.GetQueryType(),
		SQLSourceType: ses.GetSqlSourceType(),
		QueryStart:    ses.GetQueryStart(),
		ClientHost:    ses.clientAddr,
		Role:          roleName,
		FromProxy:     ses.fromProxy,
		ProxyHost:     ses.proxyAddr,
	}
}

// getStatusAfterTxnIsEnded
// !!! only used after the txn is ended.
// it may be called in the active txn. so, we
func (ses *Session) getStatusAfterTxnIsEnded(ctx context.Context) uint16 {
	return extendStatus(ses.GetTxnHandler().GetServerStatus())
}

func uuid2Str(uid uuid.UUID) string {
	if bytes.Equal(uid[:], dumpUUID[:]) {
		return ""
	}
	return strings.ReplaceAll(uid.String(), "-", "")
}

func (ses *Session) SetSessionRoutineStatus(status string) error {
	var err error
	if status == tree.AccountStatusRestricted.String() {
		ses.getRoutine().setResricted(true)
	} else if status == tree.AccountStatusSuspend.String() {
		ses.getRoutine().setResricted(false)
	} else if status == tree.AccountStatusOpen.String() {
		ses.getRoutine().setResricted(false)
	} else {
		err = moerr.NewInternalErrorNoCtx("SetSessionRoutineStatus have invalid status : %s", status)
	}
	return err
}

func checkPlanIsInsertValues(proc *process.Process,
	p *plan.Plan) (bool, [][]colexec.ExpressionExecutor) {
	qry := p.GetQuery()
	if qry != nil {
		for _, node := range qry.Nodes {
			if node.NodeType == plan.Node_VALUE_SCAN && node.RowsetData != nil {
				exprList := make([][]colexec.ExpressionExecutor, len(node.RowsetData.Cols))
				for i, col := range node.RowsetData.Cols {
					exprList[i] = make([]colexec.ExpressionExecutor, 0, len(col.Data))
					for _, data := range col.Data {
						if data.Pos >= 0 {
							continue
						}
						expr, err := colexec.NewExpressionExecutor(proc, data.Expr)
						if err != nil {
							return false, nil
						}
						exprList[i] = append(exprList[i], expr)
					}
				}
				return true, exprList
			}
		}
	}
	return false, nil
}

func commitAfterMigrate(ses *Session, err error) error {
	//if ses == nil {
	//	logutil.Error("session is nil")
	//	return moerr.NewInternalErrorNoCtx("session is nil")
	//}
	//txnHandler := ses.GetTxnHandler()
	//if txnHandler == nil {
	//	logutil.Error("txn handler is nil")
	//	return moerr.NewInternalErrorNoCtx("txn handler is nil")
	//}
	//if txnHandler.GetSession() == nil {
	//	logutil.Error("ses in txn handler is nil")
	//	return moerr.NewInternalErrorNoCtx("ses in txn handler is nil")
	//}
	//defer func() {
	//	txnHandler.ClearServerStatus(SERVER_STATUS_IN_TRANS)
	//	txnHandler.ClearOptionBits(OPTION_BEGIN)
	//}()
	//if err != nil {
	//	if rErr := txnHandler.RollbackTxn(); rErr != nil {
	//		logutil.Errorf("failed to rollback txn: %v", rErr)
	//	}
	//	return err
	//} else {
	//	if cErr := txnHandler.CommitTxn(); cErr != nil {
	//		logutil.Errorf("failed to commit txn: %v", cErr)
	//		return cErr
	//	}
	//}
	return nil
}

type dbMigration struct {
	db       string
	commitFn func(*Session, error) error
}

func newDBMigration(db string) *dbMigration {
	return &dbMigration{
		db:       db,
		commitFn: commitAfterMigrate,
	}
}

func (d *dbMigration) Migrate(ctx context.Context, ses *Session) error {
	if d.db == "" {
		return nil
	}
	tempExecCtx := &ExecCtx{
		reqCtx:         ctx,
		skipRespClient: true,
		ses:            ses,
	}
	return doComQuery(ses, tempExecCtx, &UserInput{sql: "use " + d.db})
}

type prepareStmtMigration struct {
	name       string
	sql        string
	paramTypes []byte
	commitFn   func(*Session, error) error
}

func newPrepareStmtMigration(name string, sql string, paramTypes []byte) *prepareStmtMigration {
	return &prepareStmtMigration{
		name:       name,
		sql:        sql,
		paramTypes: paramTypes,
		commitFn:   commitAfterMigrate,
	}
}

func (p *prepareStmtMigration) Migrate(ctx context.Context, ses *Session) error {
	if !strings.HasPrefix(strings.ToLower(p.sql), "prepare") {
		p.sql = fmt.Sprintf("prepare %s from %s", p.name, p.sql)
	}

	tempExecCtx := &ExecCtx{
		reqCtx:            ctx,
		skipRespClient:    true,
		ses:               ses,
		executeParamTypes: p.paramTypes,
	}
	return doComQuery(ses, tempExecCtx, &UserInput{sql: p.sql})
}

func Migrate(ses *Session, req *query.MigrateConnToRequest) error {
	parameters := getGlobalPu().SV

	//all offspring related to the request inherit the txnCtx
	cancelRequestCtx, cancelRequestFunc := context.WithTimeout(ses.GetTxnHandler().GetTxnCtx(), parameters.SessionTimeout.Duration)
	defer cancelRequestFunc()
	ses.UpdateDebugString()
	tenant := ses.GetTenantInfo()
	nodeCtx := cancelRequestCtx
	if ses.getRoutineManager() != nil && ses.getRoutineManager().baseService != nil {
		nodeCtx = context.WithValue(cancelRequestCtx, defines.NodeIDKey{}, ses.getRoutineManager().baseService.ID())
	}
	ctx := defines.AttachAccount(nodeCtx, tenant.GetTenantID(), tenant.GetUserID(), tenant.GetDefaultRoleID())

	accountID, err := defines.GetAccountId(ctx)

	if err != nil {
		logutil.Errorf("failed to get account ID: %v", err)
		return err
	}
	userID := defines.GetUserId(ctx)
	logutil.Infof("do migration on connection %d, db: %s, account id: %d, user id: %d",
		req.ConnID, req.DB, accountID, userID)

	dbm := newDBMigration(req.DB)
	if err := dbm.Migrate(ctx, ses); err != nil {
		return err
	}

	var maxStmtID uint32
	for _, p := range req.PrepareStmts {
		if p == nil {
			continue
		}
		pm := newPrepareStmtMigration(p.Name, p.SQL, p.ParamTypes)
		if err := pm.Migrate(ctx, ses); err != nil {
			return err
		}
		id := parsePrepareStmtID(p.Name)
		if id > maxStmtID {
			maxStmtID = id
		}
	}
	if maxStmtID > 0 {
		ses.SetLastStmtID(maxStmtID)
	}
	return nil
}
