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

	"github.com/matrixorigin/matrixone/pkg/container/vector"

	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const MaxPrepareNumberInOneSession = 64

// TODO: this variable should be configure by set variable
const MoDefaultErrorCount = 64

type ShowStatementType int

const (
	NotShowStatement ShowStatementType = 0
	ShowTableStatus  ShowStatementType = 1
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

	pu *config.ParameterUnit

	isInternal bool

	data         [][]interface{}
	ep           *ExportParam
	showStmtType ShowStatementType

	txnHandler    *TxnHandler
	txnCompileCtx *TxnCompilerContext
	storage       engine.Engine
	sql           string

	sysVars         map[string]interface{}
	userDefinedVars map[string]interface{}
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

	flag         bool
	lastInsertID uint64

	skipAuth bool

	sqlSourceType []string

	InitTempEngine bool

	tempTablestorage *memorystorage.Storage

	isBackgroundSession bool

	tStmt *motrace.StatementInfo

	ast tree.Statement

	rs *plan.ResultColDef

	QueryId []string

	blockIdx int

	p *plan.Plan

	limitResultSize float64 // MB

	curResultSize float64 // MB

	sentRows atomic.Int64

	createdTime time.Time

	expiredTime time.Time

	planCache *planCache

	statsCache *plan2.StatsCache

	autoIncrCacheManager *defines.AutoIncrCacheManager

	seqCurValues map[uint64]string

	seqLastValue string

	sqlHelper *SqlHelper

	// when starting a transaction in session, the snapshot ts of the transaction
	// is to get a DN push to CN to get the maximum commitTS. but there is a problem,
	// when the last transaction ends and the next one starts, it is possible that the
	// log of the last transaction has not been pushed to CN, we need to wait until at
	// least the commit of the last transaction log of the previous transaction arrives.
	lastCommitTS timestamp.Timestamp
	upstream     *Session
}

func (ses *Session) SetSeqLastValue(proc *process.Process) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.seqLastValue = proc.SessionInfo.SeqLastValue[0]
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
	return ses.seqLastValue
}

func (ses *Session) CopySeqToProc(proc *process.Process) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	for k, v := range ses.seqCurValues {
		proc.SessionInfo.SeqCurValues[k] = v
	}
	proc.SessionInfo.SeqLastValue[0] = ses.seqLastValue
}

func (ses *Session) GetSqlHelper() *SqlHelper {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.sqlHelper
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

const saveQueryIdCnt = 10

func (ses *Session) pushQueryId(uuid string) {
	if len(ses.QueryId) > saveQueryIdCnt {
		ses.QueryId = ses.QueryId[1:]
	}
	ses.QueryId = append(ses.QueryId, uuid)
}

// Clean up all resources hold by the session.  As of now, the mpool
func (ses *Session) Dispose() {
	if ses.flag {
		mp := ses.GetMemPool()
		mpool.DeleteMPool(mp)
		ses.SetMemPool(mp)
	}
	ses.cleanCache()

	ses.statsCache = nil
	// Clean sequence record data.
	ses.seqCurValues = nil
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

func NewSession(proto Protocol, mp *mpool.MPool, pu *config.ParameterUnit, gSysVars *GlobalSystemVariables, flag bool, aicm *defines.AutoIncrCacheManager) *Session {
	txnHandler := InitTxnHandler(pu.StorageEngine, pu.TxnClient)
	ses := &Session{
		protocol: proto,
		mp:       mp,
		pu:       pu,
		ep: &ExportParam{
			ExportParam: &tree.ExportParam{
				Outfile: false,
				Fields:  &tree.Fields{},
				Lines:   &tree.Lines{},
			},
		},
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
	}
	if flag {
		ses.sysVars = gSysVars.CopySysVarsToSession()
		ses.userDefinedVars = make(map[string]interface{})
		ses.prepareStmts = make(map[string]*PrepareStmt)
		ses.statsCache = plan2.NewStatsCache()
		// For seq init values.
		ses.seqCurValues = make(map[uint64]string)
		ses.seqLastValue = ""
		ses.sqlHelper = &SqlHelper{ses: ses}
	}
	ses.flag = flag
	ses.uuid, _ = uuid.NewUUID()
	ses.SetOptionBits(OPTION_AUTOCOMMIT)
	ses.GetTxnCompileCtx().SetSession(ses)
	ses.GetTxnHandler().SetSession(ses)
	ses.SetAutoIncrCacheManager(aicm)

	var err error
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

	runtime.SetFinalizer(ses, func(ss *Session) {
		ss.Dispose()
	})
	return ses
}

// BackgroundSession executing the sql in background
type BackgroundSession struct {
	*Session
	cancel context.CancelFunc
}

// NewBackgroundSession generates an independent background session executing the sql
func NewBackgroundSession(
	reqCtx context.Context,
	upstream *Session,
	mp *mpool.MPool,
	PU *config.ParameterUnit,
	gSysVars *GlobalSystemVariables) *BackgroundSession {
	connCtx := upstream.GetConnectContext()
	aicm := upstream.GetAutoIncrCacheManager()

	ses := NewSession(&FakeProtocol{}, mp, PU, gSysVars, false, aicm)
	ses.upstream = upstream
	ses.SetOutputCallback(fakeDataSetFetcher)
	if stmt := motrace.StatementFromContext(reqCtx); stmt != nil {
		logutil.Infof("session uuid: %s -> background session uuid: %s", uuid.UUID(stmt.SessionID).String(), ses.uuid.String())
	}
	cancelBackgroundCtx, cancelBackgroundFunc := context.WithCancel(reqCtx)
	ses.SetRequestContext(cancelBackgroundCtx)
	ses.SetConnectContext(connCtx)
	ses.SetBackgroundSession(true)
	backSes := &BackgroundSession{
		Session: ses,
		cancel:  cancelBackgroundFunc,
	}
	return backSes
}

func (bgs *BackgroundSession) Close() {
	if bgs.cancel != nil {
		bgs.cancel()
	}

	if bgs.Session != nil {
		bgs.Session.ep = nil
		bgs.Session.errInfo.codes = nil
		bgs.Session.errInfo.msgs = nil
		bgs.Session.errInfo = nil
		bgs.Session.cache.invalidate()
		bgs.Session.cache = nil
		bgs.Session.txnCompileCtx = nil
		bgs.Session.txnHandler = nil
		bgs.Session.gSysVars = nil
		bgs.Session.statsCache = nil
	}
	bgs = nil
}

func (ses *Session) GetIncBlockIdx() int {
	ses.blockIdx++
	return ses.blockIdx
}

func (ses *Session) ResetBlockIdx() {
	ses.blockIdx = 0
}

func (ses *Session) SetBackgroundSession(b bool) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.isBackgroundSession = b
}

func (ses *Session) IsBackgroundSession() bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.isBackgroundSession
}

func (ses *Session) cachePlan(sql string, stmts []tree.Statement, plans []*plan.Plan) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.planCache.cache(sql, stmts, plans)
}

func (ses *Session) getCachedPlan(sql string) *cachedPlan {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.planCache.get(sql)
}

func (ses *Session) isCached(sql string) bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.planCache.isCached(sql)
}

func (ses *Session) cleanCache() {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.planCache.clean()
}

func (ses *Session) setSkipCheckPrivilege(b bool) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.skipAuth = b
}

func (ses *Session) skipCheckPrivilege() bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.skipAuth
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
	//session id
	sb.WriteString(ses.uuid.String())
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

func (ses *Session) SetTempTableStorage(ck clock.Clock) (*metadata.DNService, error) {
	// Without concurrency, there is no potential for data competition

	// Arbitrary value is OK since it's single sharded. Let's use 0xbeef
	// suggested by @reusee
	shards := []metadata.DNShard{
		{
			ReplicaID:     0xbeef,
			DNShardRecord: metadata.DNShardRecord{ShardID: 0xbeef},
		},
	}
	// Arbitrary value is OK, for more information about TEMPORARY_TABLE_DN_ADDR, please refer to the comment in defines/const.go
	dnAddr := defines.TEMPORARY_TABLE_DN_ADDR
	dnStore := metadata.DNService{
		ServiceID:         uuid.NewString(),
		TxnServiceAddress: dnAddr,
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
	return &dnStore, nil
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

func (ses *Session) GetBackgroundHandlerWithBatchFetcher(ctx context.Context) *BackgroundHandler {
	bh := &BackgroundHandler{
		mce: NewMysqlCmdExecutor(),
		ses: NewBackgroundSession(ctx, ses, ses.GetMemPool(), ses.GetParameterUnit(), GSysVariables),
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

func (ses *Session) SetExportParam(ep *tree.ExportParam) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.ep.ExportParam = ep
}

func (ses *Session) GetExportParam() *ExportParam {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.ep
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
	// just copy
	var err error
	copied := &batch.Batch{
		Cnt:   1,
		Attrs: make([]string, len(bat.Attrs)),
		Vecs:  make([]*vector.Vector, len(bat.Vecs)),
		Zs:    make([]int64, len(bat.Zs)),
	}
	copy(copied.Attrs, bat.Attrs)
	copy(copied.Zs, bat.Zs)
	for i := range copied.Vecs {
		copied.Vecs[i], err = bat.Vecs[i].Dup(ses.mp)
		if err != nil {
			return err
		}
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
func (ses *Session) GetTenantName(stmt tree.Statement) string {
	tenant := sysAccountName
	if ses.GetTenantInfo() != nil && (stmt == nil || !IsPrepareStatement(stmt)) {
		tenant = ses.GetTenantInfo().GetTenant()
	}
	return tenant
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
	if _, ok := ses.prepareStmts[name]; !ok {
		if len(ses.prepareStmts) >= MaxPrepareNumberInOneSession {
			return moerr.NewInvalidState(ses.requestCtx, "too many prepared statement, max %d", MaxPrepareNumberInOneSession)
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

func (ses *Session) RemovePrepareStmt(name string) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
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
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.txnCompileCtx
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
func (ses *Session) SetUserDefinedVar(name string, value interface{}) error {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.userDefinedVars[strings.ToLower(name)] = value
	return nil
}

// GetUserDefinedVar gets value of the user defined variable
func (ses *Session) GetUserDefinedVar(name string) (SystemVariableType, interface{}, error) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	val, ok := ses.userDefinedVars[strings.ToLower(name)]
	if !ok {
		return SystemVariableNullType{}, nil, nil
	}
	return InitSystemVariableStringType(name), val, nil
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

func (ses *Session) IsTaeEngine() bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	e, isEntire := ses.storage.(*engine.EntireEngine)
	if isEntire {
		_, ok := e.Engine.(moengine.TxnEngine)
		return ok
	} else {
		_, ok := ses.storage.(moengine.TxnEngine)
		return ok
	}
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

func (ses *Session) GetUserName() string {
	return ses.GetMysqlProtocol().GetUserName()
}

func (ses *Session) SetUserName(uname string) {
	ses.GetMysqlProtocol().SetUserName(uname)
}

func (ses *Session) GetConnectionID() uint32 {
	return ses.GetMysqlProtocol().ConnectionID()
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

// AuthenticateUser verifies the password of the user.
func (ses *Session) AuthenticateUser(userInput string) ([]byte, error) {
	var defaultRoleID int64
	var defaultRole string
	var tenant *TenantInfo
	var err error
	var rsset []ExecResult
	var tenantID int64
	var userID int64
	var pwd, accountStatus string
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
		return pwdBytes, nil
	}

	ses.SetTenantInfo(tenant)

	//step1 : check tenant exists or not in SYS tenant context
	sysTenantCtx := context.WithValue(ses.GetRequestContext(), defines.TenantIDKey{}, uint32(sysAccountID))
	sysTenantCtx = context.WithValue(sysTenantCtx, defines.UserIDKey{}, uint32(rootID))
	sysTenantCtx = context.WithValue(sysTenantCtx, defines.RoleIDKey{}, uint32(moAdminRoleID))
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

	if strings.ToLower(accountStatus) == tree.AccountStatusSuspend.String() {
		return nil, moerr.NewInternalError(sysTenantCtx, "Account %s is suspended", tenant.GetTenant())
	}

	tenant.SetTenantID(uint32(tenantID))
	//step2 : check user exists or not in general tenant.
	//step3 : get the password of the user

	tenantCtx := context.WithValue(ses.GetRequestContext(), defines.TenantIDKey{}, uint32(tenantID))

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
	} else {
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
	}

	logInfo(sessionInfo, tenant.String())

	return []byte(pwd), nil
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

func (ses *Session) getSqlType(sql string) {
	ses.sqlSourceType = nil
	tenant := ses.GetTenantInfo()
	if tenant == nil || strings.HasPrefix(sql, cmdFieldListSql) {
		ses.sqlSourceType = append(ses.sqlSourceType, intereSql)
		return
	}
	flag, _, _ := isSpecialUser(tenant.User)
	if flag {
		ses.sqlSourceType = append(ses.sqlSourceType, intereSql)
		return
	}
	for len(sql) > 0 {
		p1 := strings.Index(sql, "/*")
		p2 := strings.Index(sql, "*/")
		if p1 < 0 || p2 < 0 || p2 <= p1+1 {
			ses.sqlSourceType = append(ses.sqlSourceType, externSql)
			return
		}
		source := strings.TrimSpace(sql[p1+2 : p2])
		if source == cloudUserTag {
			ses.sqlSourceType = append(ses.sqlSourceType, cloudUserSql)
		} else if source == cloudNoUserTag {
			ses.sqlSourceType = append(ses.sqlSourceType, cloudNoUserSql)
		} else {
			ses.sqlSourceType = append(ses.sqlSourceType, externSql)
		}
		sql = sql[p2+2:]
	}
}

func changeVersion(ctx context.Context, ses *Session, db string) error {
	var err error
	if _, ok := bannedCatalogDatabases[db]; ok {
		return err
	}
	version, _ := GetVersionCompatbility(ctx, ses, db)
	if ses.GetTenantInfo() != nil {
		ses.GetTenantInfo().SetVersion(version)
	}
	return err
}

func fixColumnName(cols []*engine.Attribute, expr *plan.Expr) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			fixColumnName(cols, arg)
		}
	case *plan.Expr_Col:
		exprImpl.Col.Name = cols[exprImpl.Col.ColPos].Name
	}
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
	n := dataSet.Vecs[0].Length()
	for j := 0; j < n; j++ { //row index
		if dataSet.Zs[j] <= 0 {
			continue
		}
		_, err := extractRowFromEveryVector(ses, dataSet, j, oq)
		if err != nil {
			return err
		}
	}
	err := oq.flush()
	return err
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
		ses: NewBackgroundSession(reqCtx, upstream, mp, pu, GSysVariables),
	}
	return bh
}

func (bh *BackgroundHandler) Close() {
	bh.mce.Close()
	bh.ses.Close()
}

func (bh *BackgroundHandler) Exec(ctx context.Context, sql string) error {
	bh.mce.SetSession(bh.ses.Session)
	if ctx == nil {
		ctx = bh.ses.GetRequestContext()
	} else {
		bh.ses.SetRequestContext(ctx)
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
	statements, err := mysql.Parse(ctx, sql, v.(int64))
	if err != nil {
		return err
	}
	if len(statements) > 1 {
		return moerr.NewInternalError(ctx, "Exec() can run one statement at one time. but get '%d' statements now, sql = %s", len(statements), sql)
	}
	err = bh.mce.GetDoQueryFunc()(ctx, sql)
	if err != nil {
		return err
	}
	return err
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

// Made for sequence func. nextval, setval.
func (sh *SqlHelper) ExecSql(sql string) ([]interface{}, error) {
	var err error
	var erArray []ExecResult

	ctx := sh.ses.GetRequestContext()
	bh := sh.ses.GetBackgroundExec(ctx)
	defer bh.Close()

	err = bh.Exec(ctx, "begin;")
	if err != nil {
		goto handleFailed
	}

	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		goto handleFailed
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		goto handleFailed
	}

	// Success.
	err = bh.Exec(ctx, "commit;")
	if err != nil {
		goto handleFailed
	}

	if len(erArray) == 0 {
		return nil, nil
	}

	return erArray[0].(*MysqlResultSet).Data[0], nil
handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return nil, rbErr
	}
	return nil, err
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
