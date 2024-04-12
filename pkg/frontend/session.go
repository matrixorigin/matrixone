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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/status"
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
	feSessionImpl

	//cmd from the client
	cmd CommandType

	// the process of the session
	proc *process.Process

	isInternal bool

	data            [][]interface{}
	ep              *ExportConfig
	showStmtType    ShowStatementType
	storage         engine.Engine
	sysVars         map[string]interface{}
	userDefinedVars map[string]*UserDefinedVar

	prepareStmts map[string]*PrepareStmt
	lastStmtId   uint32

	requestCtx context.Context
	connectCtx context.Context

	priv *privilege

	errInfo *errInfo

	//fromRealUser distinguish the sql that the user inputs from the one
	//that the internal or background program executes
	fromRealUser bool

	cache *privilegeCache

	mu sync.Mutex

	isNotBackgroundSession bool
	lastInsertID           uint64

	InitTempEngine bool

	tempTablestorage *memorystorage.Storage

	tStmt *motrace.StatementInfo

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

func NewSession(proto MysqlProtocol, mp *mpool.MPool, gSysVars *GlobalSystemVariables, isNotBackgroundSession bool, sharedTxnHandler *TxnHandler) *Session {
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
	txnHandler := InitTxnHandler(globalPu.StorageEngine, txnCtx, txnOp)

	ses := &Session{
		feSessionImpl: feSessionImpl{
			proto:      proto,
			pool:       mp,
			txnHandler: txnHandler,
			//TODO:fix database name after the catalog is ready
			txnCompileCtx:  InitTxnCompilerContext(txnHandler, proto.GetDatabaseName()),
			gSysVars:       gSysVars,
			outputCallback: getDataFromPipeline,
			timeZone:       time.Local,
		},
		storage: &engine.EntireEngine{Engine: globalPu.StorageEngine},
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
	ses.GetTxnHandler().SetOptionBits(OPTION_AUTOCOMMIT)
	ses.GetTxnCompileCtx().SetSession(ses)
	ses.GetTxnHandler().SetSession(ses)
	if ses.pool == nil {
		// If no mp, we create one for session.  Use GuestMmuLimitation as cap.
		// fixed pool size can be another param, or should be computed from cap,
		// but here, too lazy, just use Mid.
		//
		// XXX MPOOL
		// We don't have a way to close a session, so the only sane way of creating
		// a mpool is to use NoFixed
		ses.pool, err = mpool.NewMPool("pipeline-"+ses.GetUUIDString(), globalPu.SV.GuestMmuLimitation, mpool.NoFixed)
		if err != nil {
			panic(err)
		}
	}
	ses.proc = process.New(
		context.TODO(),
		ses.pool,
		globalPu.TxnClient,
		nil,
		globalPu.FileService,
		globalPu.LockService,
		globalPu.QueryClient,
		globalPu.HAKeeperClient,
		globalPu.UdfService,
		globalAicm)
	ses.proc.SetStmtProfile(&ses.stmtProfile)

	runtime.SetFinalizer(ses, func(ss *Session) {
		ss.Close()
	})
	return ses
}

func (ses *Session) Close() {
	ses.proto = nil
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
	if ses == nil {
		return
	}
	ses.ClearAllMysqlResultSet()
	ses.ClearResultBatches()
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
	return NewBackgroundExec(
		ctx,
		ses,
		ses.GetMemPool())
}

// GetShareTxnBackgroundExec returns a background executor running the sql in a shared transaction.
// newRawBatch denotes we need the raw batch instead of mysql result set.
func (ses *Session) GetShareTxnBackgroundExec(ctx context.Context, newRawBatch bool) BackgroundExec {
	{
		var txnCtx context.Context
		var txnOp TxnOperator
		var err error
		if ses.GetTxnHandler() != nil {
			txnCtx, txnOp, err = ses.GetTxnHandler().GetTxnOperator()
			if err != nil {
				panic(err)
			}
		}

		txnHandler := InitTxnHandler(globalPu.StorageEngine, txnCtx, txnOp)
		var callback func(interface{}, *batch.Batch) error
		if newRawBatch {
			callback = batchFetcher2
		} else {
			callback = fakeDataSetFetcher2
		}
		backSes := &backSession{
			requestCtx: ctx,
			connectCtx: ses.connectCtx,
			feSessionImpl: feSessionImpl{
				pool:           ses.pool,
				proto:          &FakeProtocol{},
				buf:            buffer.New(),
				stmtProfile:    process.StmtProfile{},
				tenant:         nil,
				txnHandler:     txnHandler,
				txnCompileCtx:  InitTxnCompilerContext(txnHandler, ses.proto.GetDatabaseName()),
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
		backSes.GetTxnHandler().SetOptionBits(OPTION_AUTOCOMMIT)
		backSes.GetTxnCompileCtx().SetSession(backSes)
		backSes.GetTxnHandler().SetSession(backSes)
		bh := &backExec{
			backSes: backSes,
		}
		//the derived statement execute in a shared transaction in background session
		bh.backSes.ReplaceDerivedStmt(true)
		return bh
	}

	return nil
}

var GetRawBatchBackgroundExec = func(ctx context.Context, ses *Session) BackgroundExec {
	return ses.GetRawBatchBackgroundExec(ctx)
}

func (ses *Session) GetRawBatchBackgroundExec(ctx context.Context) BackgroundExec {
	txnHandler := InitTxnHandler(globalPu.StorageEngine, nil, nil)
	backSes := &backSession{
		requestCtx: ses.GetRequestContext(),
		connectCtx: ses.GetConnectContext(),
		feSessionImpl: feSessionImpl{
			pool:           ses.GetMemPool(),
			proto:          &FakeProtocol{},
			buf:            buffer.New(),
			stmtProfile:    process.StmtProfile{},
			tenant:         nil,
			txnHandler:     txnHandler,
			txnCompileCtx:  InitTxnCompilerContext(txnHandler, ""),
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
	backSes.GetTxnCompileCtx().SetSession(backSes)
	backSes.GetTxnHandler().SetSession(backSes)
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

func (ses *Session) GetOutputCallback() func(*batch.Batch) error {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return func(bat *batch.Batch) error {
		return ses.outputCallback(ses, bat)
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

func (ses *Session) SetRequestContext(reqCtx context.Context) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.requestCtx = reqCtx
}

func (ses *Session) GetRequestContext() context.Context {
	if ses == nil {
		panic("nil session")
	}
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
	tenantCtx := defines.AttachAccountId(ses.GetRequestContext(), uint32(tenantID))

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
		ses.InitGlobalSystemVariables()
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
		mp := ses.GetMemPool()

		updateSqls := ses.getUpdateVariableSqlsByToml()
		for _, sql := range updateSqls {
			_, err = executeSQLInBackgroundSession(
				sysTenantCtx,
				ses,
				mp,
				sql)
			if err != nil {
				return err
			}
		}

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
		mp := ses.GetMemPool()

		updateSqls := ses.getUpdateVariableSqlsByToml()
		for _, sql := range updateSqls {
			_, err = executeSQLInBackgroundSession(
				tenantCtx,
				ses,
				mp,
				sql)
			if err != nil {
				return err
			}
		}

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
	if getVariableValue(globalPu.SV.SqlMode) != gSysVarsDefs["sql_mode"].Default {
		sqlForUpdate := getSqlForUpdateSystemVariableValue(globalPu.SV.SqlMode, uint64(tenantInfo.GetTenantID()), "sql_mode")
		updateSqls = append(updateSqls, sqlForUpdate)
	}

	// lower_case_table_names
	if getVariableValue(globalPu.SV.LowerCaseTableNames) != gSysVarsDefs["lower_case_table_names"].Default {
		sqlForUpdate := getSqlForUpdateSystemVariableValue(getVariableValue(globalPu.SV.LowerCaseTableNames), uint64(tenantInfo.GetTenantID()), "lower_case_table_names")
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
