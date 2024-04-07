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

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/status"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
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
	// account id
	accountId uint32

	//protocol layer
	protocol Protocol

	//cmd from the client
	cmd CommandType

	//for test
	mrs *MysqlResultSet

	// mpool
	mp *mpool.MPool

	// the process of the session
	proc *process.Process

	pu *config.ParameterUnit

	isInternal bool

	data         [][]interface{}
	ep           *ExportConfig
	showStmtType ShowStatementType

	txnHandler    *TxnHandler
	txnCompileCtx *TxnCompilerContext
	storage       engine.Engine
	sql           string

	sysVars         map[string]interface{}
	userDefinedVars map[string]*UserDefinedVar
	gSysVars        *GlobalSystemVariables

	//the server status
	serverStatus uint16

	//the option bits
	optionBits uint32

	prepareStmts map[string]*PrepareStmt
	lastStmtId   uint32

	requestCtx context.Context
	connectCtx context.Context

	//it gets the result set from the pipeline and send it to the client
	outputCallback func(interface{}, *batch.Batch) error

	//all the result set of executing the sql in background task
	allResultSet []*MysqlResultSet

	// result batches of executing the sql in background task
	// set by func batchFetcher
	resultBatches []*batch.Batch

	tenant *TenantInfo

	uuid uuid.UUID

	timeZone *time.Location

	priv *privilege

	errInfo *errInfo

	//fromRealUser distinguish the sql that the user inputs from the one
	//that the internal or background program executes
	fromRealUser bool

	cache *privilegeCache

	debugStr string

	mu sync.Mutex

	isNotBackgroundSession bool
	lastInsertID           uint64

	InitTempEngine bool

	tempTablestorage *memorystorage.Storage

	tStmt *motrace.StatementInfo

	ast tree.Statement

	rs *plan.ResultColDef

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

	statsCache *plan2.StatsCache

	autoIncrCacheManager *defines.AutoIncrCacheManager

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

	// when starting a transaction in session, the snapshot ts of the transaction
	// is to get a TN push to CN to get the maximum commitTS. but there is a problem,
	// when the last transaction ends and the next one starts, it is possible that the
	// log of the last transaction has not been pushed to CN, we need to wait until at
	// least the commit of the last transaction log of the previous transaction arrives.
	lastCommitTS timestamp.Timestamp
	upstream     *Session

	// requestLabel is the CN label info requested from client.
	requestLabel map[string]string
	// connTyp indicates the type of connection. Default is ConnTypeUnset.
	// If it is internal connection, the value will be ConnTypeInternal, otherwise,
	// the value will be ConnTypeExternal.
	connType ConnType

	// startedAt is the session start time.
	startedAt time.Time

	//derivedStmt denotes the sql or statement that derived from the user input statement.
	//a new internal statement derived from the statement the user input and executed during
	// the execution of it in the same transaction.
	//
	//For instance
	//	select nextval('seq_15')
	//  nextval internally will derive two sql (a select and an update). the two sql are executed
	//	in the same transaction.
	derivedStmt bool

	buf *buffer.Buffer

	stmtProfile process.StmtProfile
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

func (ses *Session) ClearStmtProfile() {
	ses.stmtProfile.Clear()
}

func (ses *Session) GetSessionStart() time.Time {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.startedAt
}

func (ses *Session) SetTxnId(id []byte) {
	ses.stmtProfile.SetTxnId(id)
}

func (ses *Session) GetTxnId() uuid.UUID {
	return ses.stmtProfile.GetTxnId()
}

func (ses *Session) SetStmtId(id uuid.UUID) {
	ses.stmtProfile.SetStmtId(id)
}

func (ses *Session) GetStmtId() uuid.UUID {
	return ses.stmtProfile.GetStmtId()
}

func (ses *Session) SetStmtType(st string) {
	ses.stmtProfile.SetStmtType(st)
}

func (ses *Session) GetStmtType() string {
	return ses.stmtProfile.GetStmtType()
}

func (ses *Session) SetQueryType(qt string) {
	ses.stmtProfile.SetQueryType(qt)
}

func (ses *Session) GetQueryType() string {
	return ses.stmtProfile.GetQueryType()
}

func (ses *Session) SetSqlSourceType(st string) {
	ses.stmtProfile.SetSqlSourceType(st)
}

func (ses *Session) GetSqlSourceType() string {
	return ses.stmtProfile.GetSqlSourceType()
}

func (ses *Session) SetQueryStart(t time.Time) {
	ses.stmtProfile.SetQueryStart(t)
}

func (ses *Session) GetQueryStart() time.Time {
	return ses.stmtProfile.GetQueryStart()
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

func (ses *Session) SetSqlOfStmt(sot string) {
	ses.stmtProfile.SetSqlOfStmt(sot)
}

func (ses *Session) GetSqlOfStmt() string {
	return ses.stmtProfile.GetSqlOfStmt()
}

func (ses *Session) IsDerivedStmt() bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.derivedStmt
}

// ReplaceDerivedStmt sets the derivedStmt and returns the previous value.
// if b is true, executing a derived statement.
func (ses *Session) ReplaceDerivedStmt(b bool) bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	prev := ses.derivedStmt
	ses.derivedStmt = b
	return prev
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

// The update version. Four function.
func (ses *Session) SetAutoIncrCacheManager(aicm *defines.AutoIncrCacheManager) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.autoIncrCacheManager = aicm
}

func (ses *Session) GetAutoIncrCacheManager() *defines.AutoIncrCacheManager {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.autoIncrCacheManager
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

func NewSession(proto Protocol, mp *mpool.MPool, pu *config.ParameterUnit,
	gSysVars *GlobalSystemVariables, isNotBackgroundSession bool,
	aicm *defines.AutoIncrCacheManager, sharedTxnHandler *TxnHandler) *Session {
	//if the sharedTxnHandler exists,we use its txnCtx and txnOperator in this session.
	//Currently, we only use the sharedTxnHandler in the background session.
	var txnCtx context.Context
	var txnOp TxnOperator
	var err error
	if sharedTxnHandler != nil {
		if !sharedTxnHandler.IsValidTxnOperator() {
			panic("shared txn is invalid")
		}
		txnCtx, txnOp, err = sharedTxnHandler.GetTxnOperator()
		if err != nil {
			panic(err)
		}
	}
	txnHandler := InitTxnHandler(pu.StorageEngine, pu.TxnClient, txnCtx, txnOp)

	ses := &Session{
		protocol:   proto,
		mp:         mp,
		pu:         pu,
		txnHandler: txnHandler,
		//TODO:fix database name after the catalog is ready
		txnCompileCtx: InitTxnCompilerContext(txnHandler, proto.GetDatabaseName()),
		storage:       &engine.EntireEngine{Engine: pu.StorageEngine},
		gSysVars:      gSysVars,

		serverStatus: 0,
		optionBits:   0,

		outputCallback: getDataFromPipeline,
		timeZone:       time.Local,
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
	ses.SetOptionBits(OPTION_AUTOCOMMIT)
	ses.GetTxnCompileCtx().SetSession(ses)
	ses.GetTxnHandler().SetSession(ses)
	ses.SetAutoIncrCacheManager(aicm)
	if ses.mp == nil {
		// If no mp, we create one for session.  Use GuestMmuLimitation as cap.
		// fixed pool size can be another param, or should be computed from cap,
		// but here, too lazy, just use Mid.
		//
		// XXX MPOOL
		// We don't have a way to close a session, so the only sane way of creating
		// a mpool is to use NoFixed
		ses.mp, err = mpool.NewMPool("pipeline-"+ses.GetUUIDString(), pu.SV.GuestMmuLimitation, mpool.NoFixed)
		if err != nil {
			panic(err)
		}
	}
	ses.proc = process.New(
		context.TODO(),
		ses.mp,
		ses.GetTxnHandler().GetTxnClient(),
		nil,
		pu.FileService,
		pu.LockService,
		pu.QueryClient,
		pu.HAKeeperClient,
		pu.UdfService,
		ses.GetAutoIncrCacheManager())
	ses.proc.SetStmtProfile(&ses.stmtProfile)

	runtime.SetFinalizer(ses, func(ss *Session) {
		ss.Close()
	})
	return ses
}

func (ses *Session) Close() {
	ses.protocol = nil
	ses.mrs = nil
	ses.data = nil
	ses.ep = nil
	if ses.txnHandler != nil {
		ses.txnHandler.ses = nil
		ses.txnHandler = nil
	}
	if ses.txnCompileCtx != nil {
		ses.txnCompileCtx.ses = nil
		ses.txnCompileCtx = nil
	}
	ses.storage = nil
	ses.sql = ""
	ses.sysVars = nil
	ses.userDefinedVars = nil
	ses.gSysVars = nil
	for _, stmt := range ses.prepareStmts {
		stmt.Close()
	}
	ses.prepareStmts = nil
	ses.requestCtx = nil
	ses.connectCtx = nil
	ses.allResultSet = nil
	ses.tenant = nil
	ses.priv = nil
	ses.errInfo = nil
	ses.cache = nil
	ses.debugStr = ""
	ses.tempTablestorage = nil
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
	if ses.isNotBackgroundSession {
		mp := ses.GetMemPool()
		mpool.DeleteMPool(mp)
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

// BackgroundSession executing the sql in background
type BackgroundSession struct {
	*Session
	cancel   context.CancelFunc
	shareTxn bool
}

// NewBackgroundSession generates an independent background session executing the sql
func NewBackgroundSession(reqCtx context.Context, upstream *Session, mp *mpool.MPool, PU *config.ParameterUnit, gSysVars *GlobalSystemVariables, shareTxn bool) *BackgroundSession {
	connCtx := upstream.GetConnectContext()
	aicm := upstream.GetAutoIncrCacheManager()
	var ses *Session
	var sharedTxnHandler *TxnHandler
	if shareTxn {
		sharedTxnHandler = upstream.GetTxnHandler()
		if sharedTxnHandler == nil || !sharedTxnHandler.IsValidTxnOperator() {
			panic("invalid shared txn handler")
		}
	}
	ses = NewSession(&FakeProtocol{}, mp, PU, gSysVars, false, aicm, sharedTxnHandler)
	ses.upstream = upstream
	ses.SetOutputCallback(fakeDataSetFetcher)
	if stmt := motrace.StatementFromContext(reqCtx); stmt != nil {
		// Reset background session id as frontend stmt id
		ses.uuid = stmt.StatementID
		logutil.Debugf("session uuid: %s -> background session uuid: %s", uuid.UUID(stmt.SessionID).String(), ses.uuid.String())
	}
	cancelBackgroundCtx, cancelBackgroundFunc := context.WithCancel(reqCtx)
	ses.SetRequestContext(cancelBackgroundCtx)
	ses.SetConnectContext(connCtx)
	ses.UpdateDebugString()
	ses.SetDatabaseName(upstream.GetDatabaseName())
	//TODO: For seq init values.
	ses.InheritSequenceData(upstream)
	backSes := &BackgroundSession{
		Session:  ses,
		cancel:   cancelBackgroundFunc,
		shareTxn: shareTxn,
	}
	return backSes
}

func (bgs *BackgroundSession) isShareTxn() bool {
	return bgs.shareTxn
}

func (bgs *BackgroundSession) Close() {
	if bgs.cancel != nil {
		bgs.cancel()
	}

	if bgs.Session != nil {
		bgs.Session.Close()
		bgs.Session = nil
	}
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
	if ses.protocol != nil {
		sb.WriteString(fmt.Sprintf("connectionId %d", ses.protocol.ConnectionID()))
		sb.WriteByte('|')
		sb.WriteString(ses.protocol.Peer())
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

func (ses *Session) GetDebugString() string {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.debugStr
}

func (ses *Session) EnableInitTempEngine() {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.InitTempEngine = true
}
func (ses *Session) IfInitedTempEngine() bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.InitTempEngine
}

func (ses *Session) GetTempTableStorage() *memorystorage.Storage {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if ses.tempTablestorage == nil {
		panic("temp table storage is not initialized")
	}
	return ses.tempTablestorage
}

func (ses *Session) SetTempTableStorage(ck clock.Clock) (*metadata.TNService, error) {
	// Without concurrency, there is no potential for data competition

	// Arbitrary value is OK since it's single sharded. Let's use 0xbeef
	// suggested by @reusee
	shards := []metadata.TNShard{
		{
			ReplicaID:     0xbeef,
			TNShardRecord: metadata.TNShardRecord{ShardID: 0xbeef},
		},
	}
	// Arbitrary value is OK, for more information about TEMPORARY_TABLE_DN_ADDR, please refer to the comment in defines/const.go
	tnAddr := defines.TEMPORARY_TABLE_TN_ADDR
	uid, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	tnStore := metadata.TNService{
		ServiceID:         uid.String(),
		TxnServiceAddress: tnAddr,
		Shards:            shards,
	}

	ms, err := memorystorage.NewMemoryStorage(
		mpool.MustNewZeroNoFixed(),
		ck,
		memoryengine.RandomIDGenerator,
	)
	if err != nil {
		return nil, err
	}
	ses.tempTablestorage = ms
	return &tnStore, nil
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
	return NewBackgroundHandler(
		ctx,
		ses,
		ses.GetMemPool(),
		ses.GetParameterUnit())
}

// GetShareTxnBackgroundExec returns a background executor running the sql in a shared transaction.
// newRawBatch denotes we need the raw batch instead of mysql result set.
func (ses *Session) GetShareTxnBackgroundExec(ctx context.Context, newRawBatch bool) BackgroundExec {
	bh := &BackgroundHandler{
		mce: NewMysqlCmdExecutor(),
		ses: NewBackgroundSession(ctx, ses, ses.GetMemPool(), ses.GetParameterUnit(), GSysVariables, true),
	}
	//the derived statement execute in a shared transaction in background session
	bh.ses.ReplaceDerivedStmt(true)
	if newRawBatch {
		bh.ses.SetOutputCallback(batchFetcher)
	}
	return bh
}

var GetRawBatchBackgroundExec = func(ctx context.Context, ses *Session) BackgroundExec {
	return ses.GetRawBatchBackgroundExec(ctx)
}

func (ses *Session) GetRawBatchBackgroundExec(ctx context.Context) *BackgroundHandler {
	bh := &BackgroundHandler{
		mce: NewMysqlCmdExecutor(),
		ses: NewBackgroundSession(ctx, ses, ses.GetMemPool(), ses.GetParameterUnit(), GSysVariables, false),
	}
	bh.ses.SetOutputCallback(batchFetcher)
	return bh
}

func (ses *Session) GetIsInternal() bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.isInternal
}

func (ses *Session) SetMemPool(mp *mpool.MPool) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.mp = mp
}

func (ses *Session) GetMemPool() *mpool.MPool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.mp
}

func (ses *Session) GetParameterUnit() *config.ParameterUnit {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.pu
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

func (ses *Session) GetOutputCallback() func(interface{}, *batch.Batch) error {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.outputCallback
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

func (ses *Session) SetRequestContext(reqCtx context.Context) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.requestCtx = reqCtx
}

func (ses *Session) GetRequestContext() context.Context {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.requestCtx
}

func (ses *Session) SetConnectContext(conn context.Context) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.connectCtx = conn
}

func (ses *Session) GetConnectContext() context.Context {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.connectCtx
}

func (ses *Session) SetTimeZone(loc *time.Location) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.timeZone = loc
}

func (ses *Session) GetTimeZone() *time.Location {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.timeZone
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

func (ses *Session) SetMysqlResultSet(mrs *MysqlResultSet) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.mrs = mrs
}

func (ses *Session) GetMysqlResultSet() *MysqlResultSet {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.mrs
}

func (ses *Session) ReplaceProtocol(proto Protocol) Protocol {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	old := ses.protocol
	ses.protocol = proto
	return old
}

func (ses *Session) SetMysqlResultSetOfBackgroundTask(mrs *MysqlResultSet) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if len(ses.allResultSet) == 0 {
		ses.allResultSet = append(ses.allResultSet, mrs)
	}
}

func (ses *Session) GetAllMysqlResultSet() []*MysqlResultSet {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.allResultSet
}

func (ses *Session) ClearAllMysqlResultSet() {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if ses.allResultSet != nil {
		ses.allResultSet = ses.allResultSet[:0]
	}
}

// ClearResultBatches does not call Batch.Clear().
func (ses *Session) ClearResultBatches() {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.resultBatches = nil
}

func (ses *Session) GetResultBatches() []*batch.Batch {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.resultBatches
}

func (ses *Session) SaveResultSet() {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if len(ses.allResultSet) == 0 && ses.mrs != nil {
		ses.allResultSet = []*MysqlResultSet{ses.mrs}
	}
}

func (ses *Session) AppendResultBatch(bat *batch.Batch) error {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	copied, err := bat.Dup(ses.mp)
	if err != nil {
		return err
	}
	ses.resultBatches = append(ses.resultBatches, copied)
	return nil
}

func (ses *Session) GetTenantInfo() *TenantInfo {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.tenant
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

func (ses *Session) GetUUID() []byte {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.uuid[:]
}

func (ses *Session) GetUUIDString() string {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.uuid.String()
}

func (ses *Session) SetTenantInfo(ti *TenantInfo) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.tenant = ti
}

func (ses *Session) SetPrepareStmt(name string, prepareStmt *PrepareStmt) error {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if stmt, ok := ses.prepareStmts[name]; !ok {
		if len(ses.prepareStmts) >= MaxPrepareNumberInOneSession {
			return moerr.NewInvalidState(ses.requestCtx, "too many prepared statement, max %d", MaxPrepareNumberInOneSession)
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

func (ses *Session) GetPrepareStmt(name string) (*PrepareStmt, error) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	if prepareStmt, ok := ses.prepareStmts[name]; ok {
		return prepareStmt, nil
	}
	return nil, moerr.NewInvalidState(ses.requestCtx, "prepared statement '%s' does not exist", name)
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

func (ses *Session) GetGlobalSysVars() *GlobalSystemVariables {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.gSysVars
}

// SetGlobalVar sets the value of system variable in global.
// used by SET GLOBAL
func (ses *Session) SetGlobalVar(name string, value interface{}) error {
	return ses.GetGlobalSysVars().SetGlobalSysVar(ses.GetRequestContext(), name, value)
}

// GetGlobalVar gets this value of the system variable in global
func (ses *Session) GetGlobalVar(name string) (interface{}, error) {
	gSysVars := ses.GetGlobalSysVars()
	if def, val, ok := gSysVars.GetGlobalSysVar(name); ok {
		if def.GetScope() == ScopeSession {
			//empty
			return nil, moerr.NewInternalError(ses.GetRequestContext(), errorSystemVariableSessionEmpty())
		}
		return val, nil
	}
	return nil, moerr.NewInternalError(ses.GetRequestContext(), errorSystemVariableDoesNotExist())
}

func (ses *Session) GetTxnCompileCtx() *TxnCompilerContext {
	var compCtx *TxnCompilerContext
	ses.mu.Lock()
	compCtx = ses.txnCompileCtx
	ses.mu.Unlock()
	return compCtx
}

func (ses *Session) GetBuffer() *buffer.Buffer {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.buf
}

// SetSessionVar sets the value of system variable in session
func (ses *Session) SetSessionVar(name string, value interface{}) error {
	gSysVars := ses.GetGlobalSysVars()
	if def, _, ok := gSysVars.GetGlobalSysVar(name); ok {
		if def.GetScope() == ScopeGlobal {
			return moerr.NewInternalError(ses.GetRequestContext(), errorSystemVariableIsGlobal())
		}
		//scope session & both
		if !def.GetDynamic() {
			return moerr.NewInternalError(ses.GetRequestContext(), errorSystemVariableIsReadOnly())
		}

		cv, err := def.GetType().Convert(value)
		if err != nil {
			errutil.ReportError(ses.GetRequestContext(), err)
			return err
		}

		if def.UpdateSessVar == nil {
			ses.SetSysVar(def.GetName(), cv)
		} else {
			return def.UpdateSessVar(ses, ses.GetSysVars(), def.GetName(), cv)
		}
	} else {
		return moerr.NewInternalError(ses.GetRequestContext(), errorSystemVariableDoesNotExist())
	}
	return nil
}

// InitSetSessionVar sets the value of system variable in session when start a connection
func (ses *Session) InitSetSessionVar(name string, value interface{}) error {
	gSysVars := ses.GetGlobalSysVars()
	if def, _, ok := gSysVars.GetGlobalSysVar(name); ok {
		cv, err := def.GetType().Convert(value)
		if err != nil {
			errutil.ReportError(ses.GetRequestContext(), moerr.NewInternalError(context.Background(), "init variable fail: variable %s convert to the system variable type %s failed, bad value %v", name, def.GetType().String(), value))
		}

		if def.UpdateSessVar == nil {
			ses.SetSysVar(def.GetName(), cv)
		} else {
			return def.UpdateSessVar(ses, ses.GetSysVars(), def.GetName(), cv)
		}
	}
	return nil
}

// GetSessionVar gets this value of the system variable in session
func (ses *Session) GetSessionVar(name string) (interface{}, error) {
	gSysVars := ses.GetGlobalSysVars()
	if def, gVal, ok := gSysVars.GetGlobalSysVar(name); ok {
		ciname := strings.ToLower(name)
		if def.GetScope() == ScopeGlobal {
			return gVal, nil
		}
		return ses.GetSysVar(ciname), nil
	} else {
		return nil, moerr.NewInternalError(ses.GetRequestContext(), errorSystemVariableDoesNotExist())
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
	_, txnOp, err := txnH.GetTxnOperator()
	if err != nil {
		return ""
	}
	if txnOp == nil {
		return ""
	}
	meta := txnOp.Txn()
	return meta.DebugString()
}

func (ses *Session) GetTxnHandler() *TxnHandler {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.txnHandler
}

func (ses *Session) SetSql(sql string) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.sql = sql
}

func (ses *Session) GetSql() string {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.sql
}

func (ses *Session) IsEntireEngine() bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	_, isEntire := ses.storage.(*engine.EntireEngine)
	if isEntire {
		return true
	} else {
		return false
	}
}

func (ses *Session) GetStorage() engine.Engine {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.storage
}

func (ses *Session) SetTempEngine(ctx context.Context, te engine.Engine) error {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ee := ses.storage.(*engine.EntireEngine)
	ee.TempEngine = te
	ses.requestCtx = ctx
	return nil
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

// GetUserName returns the user_ame and the account_name
func (ses *Session) GetUserName() string {
	return ses.GetMysqlProtocol().GetUserName()
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

func (ses *Session) SetOutputCallback(callback func(interface{}, *batch.Batch) error) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.outputCallback = callback
}

func (ses *Session) skipAuthForSpecialUser() bool {
	if ses.GetTenantInfo() != nil {
		ok, _, _ := isSpecialUser(ses.GetTenantInfo().GetUser())
		return ok
	}
	return false
}

// AuthenticateUser Verify the user's password, and if the login information contains the database name, verify if the database exists
func (ses *Session) AuthenticateUser(userInput string, dbName string, authResponse []byte, salt []byte, checkPassword func(pwd, salt, auth []byte) bool) ([]byte, error) {
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
	tenant, err = GetTenantInfo(ses.GetRequestContext(), userInput)
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
	sysTenantCtx := defines.AttachAccount(ses.GetRequestContext(), uint32(sysAccountID), uint32(rootID), uint32(moAdminRoleID))
	sqlForCheckTenant, err := getSqlForCheckTenant(sysTenantCtx, tenant.GetTenant())
	if err != nil {
		return nil, err
	}
	pu := ses.GetParameterUnit()
	mp := ses.GetMemPool()
	logDebugf(sessionInfo, "check tenant %s exists", tenant)
	rsset, err = executeSQLInBackgroundSession(
		sysTenantCtx,
		ses,
		mp,
		pu,
		sqlForCheckTenant)
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
	tenantCtx := defines.AttachAccountId(ses.GetRequestContext(), uint32(tenantID))

	logDebugf(sessionInfo, "check user of %s exists", tenant)
	//Get the password of the user in an independent session
	sqlForPasswordOfUser, err := getSqlForPasswordOfUser(tenantCtx, tenant.GetUser())
	if err != nil {
		return nil, err
	}
	rsset, err = executeSQLInBackgroundSession(
		tenantCtx,
		ses,
		mp,
		pu,
		sqlForPasswordOfUser)
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
		rsset, err = executeSQLInBackgroundSession(
			tenantCtx,
			ses,
			mp,
			pu,
			sqlForCheckRoleExists)
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
		rsset, err = executeSQLInBackgroundSession(
			tenantCtx,
			ses,
			mp,
			pu,
			sqlForRoleOfUser)
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
		rsset, err = executeSQLInBackgroundSession(
			tenantCtx,
			ses,
			mp,
			pu,
			sql)
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
		ses.InitGlobalSystemVariables()
	} else {
		return nil, moerr.NewInternalError(tenantCtx, "check password failed")
	}

	// If the login information contains the database name, verify if the database exists
	if dbName != "" {
		ses.timestampMap[TSCheckDbNameStart] = time.Now()
		_, err = executeSQLInBackgroundSession(tenantCtx, ses, mp, pu, "use "+dbName)
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

func (ses *Session) InitGlobalSystemVariables() error {
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
		sysTenantCtx := defines.AttachAccount(ses.GetRequestContext(), uint32(sysAccountID), uint32(rootID), uint32(moAdminRoleID))

		// get system variable from mo_mysql_compatibility mode
		sqlForGetVariables := getSystemVariablesWithAccount(sysAccountID)
		pu := ses.GetParameterUnit()
		mp := ses.GetMemPool()

		updateSqls := ses.getUpdateVariableSqlsByToml()
		for _, sql := range updateSqls {
			_, err = executeSQLInBackgroundSession(
				sysTenantCtx,
				ses,
				mp,
				pu,
				sql)
			if err != nil {
				return err
			}
		}

		rsset, err = executeSQLInBackgroundSession(
			sysTenantCtx,
			ses,
			mp,
			pu,
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
						errutil.ReportError(ses.GetRequestContext(), moerr.NewInternalError(context.Background(), "init variable fail: variable %s convert from string value to the system variable type %s failed, bad value %s", variable_name, sv.Type.String(), variable_value))
						return err
					}
					err = ses.InitSetSessionVar(variable_name, val)
					if err != nil {
						errutil.ReportError(ses.GetRequestContext(), moerr.NewInternalError(context.Background(), "init variable fail: variable %s convert from string value to the system variable type %s failed, bad value %s", variable_name, sv.Type.String(), variable_value))
					}
				}
			}
		} else {
			return moerr.NewInternalError(sysTenantCtx, "there is no data in mo_mysql_compatibility_mode table for account %s", sysAccountName)
		}
	} else {
		tenantCtx := defines.AttachAccount(ses.GetRequestContext(), tenantInfo.GetTenantID(), tenantInfo.GetUserID(), uint32(accountAdminRoleID))

		// get system variable from mo_mysql_compatibility mode
		sqlForGetVariables := getSystemVariablesWithAccount(uint64(tenantInfo.GetTenantID()))
		pu := ses.GetParameterUnit()
		mp := ses.GetMemPool()

		updateSqls := ses.getUpdateVariableSqlsByToml()
		for _, sql := range updateSqls {
			_, err = executeSQLInBackgroundSession(
				tenantCtx,
				ses,
				mp,
				pu,
				sql)
			if err != nil {
				return err
			}
		}

		rsset, err = executeSQLInBackgroundSession(
			tenantCtx,
			ses,
			mp,
			pu,
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
					err = ses.InitSetSessionVar(variable_name, val)
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

func (ses *Session) getUpdateVariableSqlsByToml() []string {
	updateSqls := make([]string, 0)
	tenantInfo := ses.GetTenantInfo()
	// sql_mode
	if getVariableValue(ses.pu.SV.SqlMode) != gSysVarsDefs["sql_mode"].Default {
		sqlForUpdate := getSqlForUpdateSystemVariableValue(ses.pu.SV.SqlMode, uint64(tenantInfo.GetTenantID()), "sql_mode")
		updateSqls = append(updateSqls, sqlForUpdate)
	}

	// lower_case_table_names
	if getVariableValue(ses.pu.SV.LowerCaseTableNames) != gSysVarsDefs["lower_case_table_names"].Default {
		sqlForUpdate := getSqlForUpdateSystemVariableValue(getVariableValue(ses.pu.SV.LowerCaseTableNames), uint64(tenantInfo.GetTenantID()), "lower_case_table_names")
		updateSqls = append(updateSqls, sqlForUpdate)
	}

	return updateSqls
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

// fakeDataSetFetcher gets the result set from the pipeline and save it in the session.
// It will not send the result to the client.
func fakeDataSetFetcher(handle interface{}, dataSet *batch.Batch) error {
	if handle == nil || dataSet == nil {
		return nil
	}

	ses := handle.(*Session)
	oq := newFakeOutputQueue(ses.GetMysqlResultSet())
	err := fillResultSet(oq, dataSet, ses)
	if err != nil {
		return err
	}
	ses.SetMysqlResultSetOfBackgroundTask(ses.GetMysqlResultSet())
	return nil
}

func fillResultSet(oq outputPool, dataSet *batch.Batch, ses *Session) error {
	n := dataSet.RowCount()
	for j := 0; j < n; j++ { //row index
		//needCopyBytes = true. we need to copy the bytes from the batch.Batch
		//to avoid the data being changed after the batch.Batch returned to the
		//pipeline.
		_, err := extractRowFromEveryVector(ses, dataSet, j, oq, true)
		if err != nil {
			return err
		}
	}
	return oq.flush()
}

// batchFetcher gets the result batches from the pipeline and save the origin batches in the session.
// It will not send the result to the client.
func batchFetcher(handle interface{}, dataSet *batch.Batch) error {
	if handle == nil {
		return nil
	}
	ses := handle.(*Session)
	ses.SaveResultSet()
	if dataSet == nil {
		return nil
	}
	return ses.AppendResultBatch(dataSet)
}

// getResultSet extracts the result set
func getResultSet(ctx context.Context, bh BackgroundExec) ([]ExecResult, error) {
	results := bh.GetExecResultSet()
	rsset := make([]ExecResult, len(results))
	for i, value := range results {
		if er, ok := value.(ExecResult); ok {
			rsset[i] = er
		} else {
			return nil, moerr.NewInternalError(ctx, "it is not the type of result set")
		}
	}
	return rsset, nil
}

// executeSQLInBackgroundSession executes the sql in an independent session and transaction.
// It sends nothing to the client.
func executeSQLInBackgroundSession(
	reqCtx context.Context,
	upstream *Session,
	mp *mpool.MPool,
	pu *config.ParameterUnit,
	sql string) ([]ExecResult, error) {
	bh := NewBackgroundHandler(reqCtx, upstream, mp, pu)
	defer bh.Close()
	logutil.Debugf("background exec sql:%v", sql)
	err := bh.Exec(reqCtx, sql)
	logutil.Debugf("background exec sql done")
	if err != nil {
		return nil, err
	}
	return getResultSet(reqCtx, bh)
}

// executeStmtInSameSession executes the statement in the same session.
// To be clear, only for the select statement derived from the set_var statement
// in an independent transaction
func executeStmtInSameSession(ctx context.Context, mce *MysqlCmdExecutor, ses *Session, stmt tree.Statement) error {
	switch stmt.(type) {
	case *tree.Select, *tree.ParenSelect:
	default:
		return moerr.NewInternalError(ctx, "executeStmtInSameSession can not run non select statement in the same session")
	}

	prevDB := ses.GetDatabaseName()
	prevOptionBits := ses.GetOptionBits()
	prevServerStatus := ses.GetServerStatus()
	//autocommit = on
	ses.setAutocommitOn()
	//1. replace output callback by batchFetcher.
	// the result batch will be saved in the session.
	// you can get the result batch by calling GetResultBatches()
	ses.SetOutputCallback(batchFetcher)
	//2. replace protocol by FakeProtocol.
	// Any response yielded during running query will be dropped by the FakeProtocol.
	// The client will not receive any response from the FakeProtocol.
	prevProto := ses.ReplaceProtocol(&FakeProtocol{})
	// inherit database
	ses.SetDatabaseName(prevDB)
	//restore normal protocol and output callback
	proc := ses.GetTxnCompileCtx().GetProcess()
	defer func() {
		//@todo we need to improve: make one session, one proc, one txnOperator
		p := ses.GetTxnCompileCtx().GetProcess()
		p.FreeVectors()
		ses.GetTxnCompileCtx().SetProcess(proc)
		ses.SetOptionBits(prevOptionBits)
		ses.SetServerStatus(prevServerStatus)
		ses.SetOutputCallback(getDataFromPipeline)
		ses.ReplaceProtocol(prevProto)
	}()
	logDebug(ses, ses.GetDebugString(), "query trace(ExecStmtInSameSession)",
		logutil.ConnectionIdField(ses.GetConnectionID()))
	//3. execute the statement
	return mce.GetDoQueryFunc()(ctx, &UserInput{stmt: stmt})
}

type BackgroundHandler struct {
	mce *MysqlCmdExecutor
	ses *BackgroundSession
}

// NewBackgroundHandler with first two parameters.
// connCtx as the parent of the txnCtx
var NewBackgroundHandler = func(
	reqCtx context.Context,
	upstream *Session,
	mp *mpool.MPool,
	pu *config.ParameterUnit) BackgroundExec {
	bh := &BackgroundHandler{
		mce: NewMysqlCmdExecutor(),
		ses: NewBackgroundSession(reqCtx, upstream, mp, pu, GSysVariables, false),
	}
	return bh
}

func (bh *BackgroundHandler) Close() {
	bh.mce.Close()
	bh.mce = nil
	bh.ses.Close()
}

func (bh *BackgroundHandler) Exec(ctx context.Context, sql string) error {
	bh.mce.SetSession(bh.ses.Session)
	if ctx == nil {
		ctx = bh.ses.GetRequestContext()
	} else {
		bh.ses.SetRequestContext(ctx)
	}
	_, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	bh.mce.ChooseDoQueryFunc(bh.ses.GetParameterUnit().SV.EnableDoComQueryInProgress)

	// For determine this is a background sql.
	ctx = context.WithValue(ctx, defines.BgKey{}, true)
	bh.mce.GetSession().SetRequestContext(ctx)

	//logutil.Debugf("-->bh:%s", sql)
	v, err := bh.ses.GetGlobalVar("lower_case_table_names")
	if err != nil {
		return err
	}
	statements, err := mysql.Parse(ctx, sql, v.(int64), 0)
	if err != nil {
		return err
	}
	defer func() {
		for _, stmt := range statements {
			stmt.Free()
		}
	}()
	if len(statements) > 1 {
		return moerr.NewInternalError(ctx, "Exec() can run one statement at one time. but get '%d' statements now, sql = %s", len(statements), sql)
	}
	//share txn can not run transaction statement
	if bh.ses.isShareTxn() {
		for _, stmt := range statements {
			switch stmt.(type) {
			case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction:
				return moerr.NewInternalError(ctx, "Exec() can not run transaction statement in share transaction, sql = %s", sql)
			}
		}
	}
	logDebug(bh.mce.GetSession(), bh.ses.GetDebugString(), "query trace(backgroundExecSql)",
		logutil.ConnectionIdField(bh.ses.GetConnectionID()),
		logutil.QueryField(SubStringFromBegin(sql, int(bh.ses.GetParameterUnit().SV.LengthOfQueryPrinted))))
	return bh.mce.GetDoQueryFunc()(ctx, &UserInput{sql: sql})
}

func (bh *BackgroundHandler) ExecStmt(ctx context.Context, stmt tree.Statement) error {
	bh.mce.SetSession(bh.ses.Session)
	if ctx == nil {
		ctx = bh.ses.GetRequestContext()
	} else {
		bh.ses.SetRequestContext(ctx)
	}
	_, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	bh.mce.ChooseDoQueryFunc(bh.ses.GetParameterUnit().SV.EnableDoComQueryInProgress)

	// For determine this is a background sql.
	ctx = context.WithValue(ctx, defines.BgKey{}, true)
	bh.mce.GetSession().SetRequestContext(ctx)

	//share txn can not run transaction statement
	if bh.ses.isShareTxn() {
		switch stmt.(type) {
		case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction:
			return moerr.NewInternalError(ctx, "Exec() can not run transaction statement in share transaction")
		}
	}
	logDebug(bh.ses.Session, bh.ses.GetDebugString(), "query trace(backgroundExecStmt)",
		logutil.ConnectionIdField(bh.ses.GetConnectionID()))
	return bh.mce.GetDoQueryFunc()(ctx, &UserInput{stmt: stmt})
}

func (bh *BackgroundHandler) GetExecResultSet() []interface{} {
	mrs := bh.ses.GetAllMysqlResultSet()
	ret := make([]interface{}, len(mrs))
	for i, mr := range mrs {
		ret[i] = mr
	}
	return ret
}

func (bh *BackgroundHandler) ClearExecResultSet() {
	bh.ses.ClearAllMysqlResultSet()
}

func (bh *BackgroundHandler) ClearExecResultBatches() {
	bh.ses.ClearResultBatches()
}

func (bh *BackgroundHandler) GetExecResultBatches() []*batch.Batch {
	return bh.ses.GetResultBatches()
}

type SqlHelper struct {
	ses *Session
}

func (sh *SqlHelper) GetCompilerContext() any {
	return sh.ses.txnCompileCtx
}

func (sh *SqlHelper) GetSubscriptionMeta(dbName string) (*plan.SubscriptionMeta, error) {
	return sh.ses.txnCompileCtx.GetSubscriptionMeta(dbName)
}

// Made for sequence func. nextval, setval.
func (sh *SqlHelper) ExecSql(sql string) (ret []interface{}, err error) {
	var erArray []ExecResult

	ctx := sh.ses.GetRequestContext()
	/*
		if we run the transaction statement (BEGIN, ect) here , it creates an independent transaction.
		if we do not run the transaction statement (BEGIN, ect) here, it runs the sql in the share transaction
		and committed outside this function.
		!!!NOTE: wen can not execute the transaction statement(BEGIN,COMMIT,ROLLBACK,START TRANSACTION ect) here.
	*/
	bh := sh.ses.GetShareTxnBackgroundExec(ctx, false)
	defer bh.Close()

	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	if len(erArray) == 0 {
		return nil, nil
	}

	return erArray[0].(*MysqlResultSet).Data[0], nil
}

func (ses *Session) updateLastCommitTS(lastCommitTS timestamp.Timestamp) {
	if lastCommitTS.Greater(ses.lastCommitTS) {
		ses.lastCommitTS = lastCommitTS
	}
	if ses.upstream != nil {
		ses.upstream.updateLastCommitTS(lastCommitTS)
	}
}

func (ses *Session) getLastCommitTS() timestamp.Timestamp {
	minTS := ses.lastCommitTS
	if ses.upstream != nil {
		v := ses.upstream.getLastCommitTS()
		if v.Greater(minTS) {
			minTS = v
		}
	}
	return minTS
}

// getCNLabels returns requested CN labels.
func (ses *Session) getCNLabels() map[string]string {
	return ses.requestLabel
}

// getSystemVariableValue get the system vaiables value from the mo_mysql_compatibility_mode table
func (ses *Session) getGlobalSystemVariableValue(varName string) (val interface{}, err error) {
	var sql string
	//var err error
	var erArray []ExecResult
	var accountId uint32
	var variableValue string
	//var val interface{}
	ctx := ses.GetRequestContext()

	// check the variable name isValid or not
	_, err = ses.GetGlobalVar(varName)
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

func (ses *Session) SetNewResponse(category int, affectedRows uint64, cmd int, d interface{}, cwIndex, cwsLen int) *Response {
	// If the stmt has next stmt, should add SERVER_MORE_RESULTS_EXISTS to the server status.
	var resp *Response
	if cwIndex < cwsLen-1 {
		resp = NewResponse(category, affectedRows, 0, 0,
			ses.GetServerStatus()|SERVER_MORE_RESULTS_EXISTS, cmd, d)
	} else {
		resp = NewResponse(category, affectedRows, 0, 0, ses.GetServerStatus(), cmd, d)
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

type dbMigration struct {
	db string
}

func (d *dbMigration) Migrate(ses *Session) error {
	if d.db == "" {
		return nil
	}
	return doUse(ses.requestCtx, ses, d.db)
}

type prepareStmtMigration struct {
	name       string
	sql        string
	paramTypes []byte
}

func (p *prepareStmtMigration) Migrate(ses *Session) error {
	v, err := ses.GetGlobalVar("lower_case_table_names")
	if err != nil {
		return err
	}
	if !strings.HasPrefix(strings.ToLower(p.sql), "prepare") {
		p.sql = fmt.Sprintf("prepare %s from %s", p.name, p.sql)
	}
	stmts, err := mysql.Parse(ses.requestCtx, p.sql, v.(int64), 0)
	if err != nil {
		return err
	}
	if _, err = doPrepareStmt(ses.requestCtx, ses, stmts[0].(*tree.PrepareStmt), p.sql, p.paramTypes); err != nil {
		return err
	}
	return nil
}

func (ses *Session) Migrate(req *query.MigrateConnToRequest) error {
	dbm := dbMigration{
		db: req.DB,
	}
	if err := dbm.Migrate(ses); err != nil {
		return err
	}

	var maxStmtID uint32
	for _, p := range req.PrepareStmts {
		if p == nil {
			continue
		}
		pm := prepareStmtMigration{
			name:       p.Name,
			sql:        p.SQL,
			paramTypes: p.ParamTypes,
		}
		if err := pm.Migrate(ses); err != nil {
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
