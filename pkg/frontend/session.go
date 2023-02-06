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
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
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
	ShowColumns      ShowStatementType = 1
	ShowTableStatus  ShowStatementType = 2
)

type TxnHandler struct {
	storage   engine.Engine
	txnClient TxnClient
	ses       *Session
	txn       TxnOperator
	mu        sync.Mutex
	entryMu   sync.Mutex
}

func InitTxnHandler(storage engine.Engine, txnClient TxnClient) *TxnHandler {
	h := &TxnHandler{
		storage:   &engine.EntireEngine{Engine: storage},
		txnClient: txnClient,
	}
	return h
}

// we don't need to lock. TxnHandler is holded by one session.
func (th *TxnHandler) SetTempEngine(te engine.Engine) {
	ee := th.storage.(*engine.EntireEngine)
	ee.TempEngine = te
}

type profileType uint8

const (
	profileTypeAccountWithName  profileType = 1 << 0
	profileTypeAccountWithId                = 1 << 1
	profileTypeSessionId                    = 1 << 2
	profileTypeConnectionWithId             = 1 << 3
	profileTypeConnectionWithIp             = 1 << 4

	profileTypeAll = profileTypeAccountWithName | profileTypeAccountWithId |
		profileTypeSessionId | profileTypeConnectionWithId | profileTypeConnectionWithIp

	profileTypeConcise = profileTypeConnectionWithId
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

	//it gets the result set from the pipeline and send it to the client
	outputCallback func(interface{}, *batch.Batch) error

	//all the result set of executing the sql in background task
	allResultSet []*MysqlResultSet

	tenant *TenantInfo

	uuid uuid.UUID

	timeZone *time.Location

	priv *privilege

	errInfo *errInfo

	//fromRealUser distinguish the sql that the user inputs from the one
	//that the internal or background program executes
	fromRealUser bool

	cache *privilegeCache

	profiles [8]string

	mu sync.Mutex

	flag         bool
	lastInsertID uint64

	skipAuth bool

	sqlSourceType string

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

	createdTime time.Time

	expiredTime time.Time

	planCache *planCache

	autoIncrCaches defines.AutoIncrCaches
}

// The update version. Four function.
func (ses *Session) SetAutoIncrCaches(autocaches defines.AutoIncrCaches) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.autoIncrCaches = autocaches
}

func (ses *Session) GetAutoIncrCaches() defines.AutoIncrCaches {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.autoIncrCaches
}

func (ses *Session) DeleteAutoIncrCache(name string) {
	ses.autoIncrCaches.Mu.Lock()
	defer ses.autoIncrCaches.Mu.Unlock()
	_, ok := ses.autoIncrCaches.AutoIncrCaches[name]
	if ok {
		delete(ses.autoIncrCaches.AutoIncrCaches, name)
	}
}

func (ses *Session) RenameAutoIncrCache(oldname, newname string) {
	ses.autoIncrCaches.Mu.Lock()
	defer ses.autoIncrCaches.Mu.Unlock()
	_, ok := ses.autoIncrCaches.AutoIncrCaches[oldname]
	if ok {
		ses.autoIncrCaches.AutoIncrCaches[newname] = defines.AutoIncrCache{CurNum: ses.autoIncrCaches.AutoIncrCaches[oldname].CurNum,
			MaxNum: ses.autoIncrCaches.AutoIncrCaches[oldname].MaxNum, Step: ses.autoIncrCaches.AutoIncrCaches[oldname].Step}
		delete(ses.autoIncrCaches.AutoIncrCaches, oldname)
	}
}

func (ses *Session) GetNextAutoIncrNum(colDefs []*plan.ColDef, ctx context.Context, incrParam *colexec.AutoIncrParam, bat *batch.Batch, tableID uint64) ([]uint64, []uint64, error) {
	ses.autoIncrCaches.Mu.Lock()
	defer ses.autoIncrCaches.Mu.Unlock()
	offset, step := make([]uint64, 0), make([]uint64, 0)
	for i, col := range colDefs {
		if !col.Typ.AutoIncr {
			continue
		}

		name := fmt.Sprintf("%d_%s", tableID, col.Name)
		autoincrcache, ok := ses.autoIncrCaches.AutoIncrCaches[name]
		// Not cached yet or the cache is ran out.
		// Need new txn for read from the table.
		if !ok || autoincrcache.CurNum >= autoincrcache.MaxNum {
			// Need return maxNum for correction.
			cur := uint64(0)
			if ok {
				cur = autoincrcache.CurNum
			}
			curNum, maxNum, stp, err := colexec.GetNextOneCache(ctx, incrParam, bat, tableID, i, name, uint64(cacheSize), cur)
			if err != nil {
				return nil, nil, err
			}
			ses.autoIncrCaches.AutoIncrCaches[name] = defines.AutoIncrCache{CurNum: curNum, MaxNum: maxNum, Step: stp}
		}

		offset = append(offset, ses.autoIncrCaches.AutoIncrCaches[name].CurNum)
		step = append(step, ses.autoIncrCaches.AutoIncrCaches[name].Step)

		// Here got the most recent id in the cache.
		// Need compare witch vec and get the maxNum.
		maxNum, err := colexec.GetMax(incrParam, bat, i, ses.autoIncrCaches.AutoIncrCaches[name].Step, 1, ses.autoIncrCaches.AutoIncrCaches[name].CurNum)
		if err != nil {
			return nil, nil, err
		}

		incrParam.SetLastInsertID(maxNum)

		// Update the caches.
		ses.autoIncrCaches.AutoIncrCaches[name] = defines.AutoIncrCache{CurNum: maxNum,
			MaxNum: ses.autoIncrCaches.AutoIncrCaches[name].MaxNum, Step: ses.autoIncrCaches.AutoIncrCaches[name].Step}
	}

	return offset, step, nil
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

func NewSession(proto Protocol, mp *mpool.MPool, pu *config.ParameterUnit, gSysVars *GlobalSystemVariables, flag bool) *Session {
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
	}
	ses.flag = flag
	ses.uuid, _ = uuid.NewUUID()
	ses.SetOptionBits(OPTION_AUTOCOMMIT)
	ses.GetTxnCompileCtx().SetSession(ses)
	ses.GetTxnHandler().SetSession(ses)

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
func NewBackgroundSession(ctx context.Context, mp *mpool.MPool, PU *config.ParameterUnit, gSysVars *GlobalSystemVariables, autoincrcaches defines.AutoIncrCaches) *BackgroundSession {
	ses := NewSession(&FakeProtocol{}, mp, PU, gSysVars, false)
	ses.SetOutputCallback(fakeDataSetFetcher)
	ses.SetAutoIncrCaches(autoincrcaches)
	if stmt := motrace.StatementFromContext(ctx); stmt != nil {
		logutil.Infof("session uuid: %s -> background session uuid: %s", uuid.UUID(stmt.SessionID).String(), ses.uuid.String())
	}
	cancelBackgroundCtx, cancelBackgroundFunc := context.WithCancel(ctx)
	ses.SetRequestContext(cancelBackgroundCtx)
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

func (ses *Session) makeProfile(profileTyp profileType) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	var mask profileType
	var profile string
	account := ses.tenant
	for i := uint8(0); i < 8; i++ {
		mask = 1 << i
		switch mask & profileTyp {
		case profileTypeAccountWithName:
			if account != nil {
				profile = fmt.Sprintf("account: %s user: %s role: %s", account.GetTenant(), account.GetUser(), account.GetDefaultRole())
			}
		case profileTypeAccountWithId:
			if account != nil {
				profile = fmt.Sprintf("accountId: %d userId: %d roleId: %d", account.GetTenantID(), account.GetUserID(), account.GetDefaultRoleID())
			}
		case profileTypeSessionId:
			profile = "sessionId " + ses.uuid.String()
		case profileTypeConnectionWithId:
			if ses.protocol != nil {
				profile = fmt.Sprintf("connectionId %d", ses.protocol.ConnectionID())
			}
		case profileTypeConnectionWithIp:
			if ses.protocol != nil {
				h, p, _, _ := ses.protocol.Peer()
				profile = "client " + h + ":" + p
			}
		default:
			profile = ""
		}
		ses.profiles[i] = profile
	}
}

func (ses *Session) MakeProfile() {
	ses.makeProfile(profileTypeAll)
}

func (ses *Session) getProfile(profileTyp profileType) string {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	var mask profileType
	sb := bytes.Buffer{}
	for i := uint8(0); i < 8; i++ {
		mask = 1 << i
		if mask&profileTyp != 0 {
			if sb.Len() != 0 {
				sb.WriteByte(' ')
			}
			sb.WriteString(ses.profiles[i])
		}
	}
	return sb.String()
}

func (ses *Session) GetConciseProfile() string {
	return ses.getProfile(profileTypeConcise)
}

func (ses *Session) GetCompleteProfile() string {
	return ses.getProfile(profileTypeAll)
}

func (ses *Session) IfInitedTempEngine() bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.InitTempEngine
}

func (ses *Session) GetTempTableStorage() *memorystorage.Storage {
	if ses.tempTablestorage == nil {
		panic("temp table storage is not initialized")
	}
	return ses.tempTablestorage
}

func (ses *Session) SetTempTableStorage(ck clock.Clock) (*logservicepb.DNStore, error) {
	// Without concurrency, there is no potential for data competition

	// Arbitrary value is OK since it's single sharded. Let's use 0xbeef
	// suggested by @reusee
	shard := logservicepb.DNShardInfo{
		ShardID:   0xbeef,
		ReplicaID: 0xbeef,
	}
	shards := []logservicepb.DNShardInfo{
		shard,
	}
	// Arbitrary value is OK, for more information about TEMPORARY_TABLE_DN_ADDR, please refer to the comment in defines/const.go
	dnAddr := defines.TEMPORARY_TABLE_DN_ADDR
	dnStore := logservicepb.DNStore{
		UUID:           uuid.NewString(),
		ServiceAddress: dnAddr,
		Shards:         shards,
	}

	ms, err := memorystorage.NewMemoryStorage(
		mpool.MustNewZero(),
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
	return NewBackgroundHandler(ctx, ses.GetMemPool(), ses.GetParameterUnit(), ses.autoIncrCaches)
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

func (ses *Session) AppendMysqlResultSetOfBackgroundTask(mrs *MysqlResultSet) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.allResultSet = append(ses.allResultSet, mrs)
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

func (ses *Session) GetPeer() (string, string) {
	rh, rp, _, _ := ses.GetMysqlProtocol().Peer()
	return rh, rp
}

func (ses *Session) SetOptionBits(bit uint32) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.optionBits |= bit
}

func (ses *Session) ClearOptionBits(bit uint32) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.optionBits &= ^bit
}

func (ses *Session) OptionBitsIsSet(bit uint32) bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.optionBits&bit != 0
}

func (ses *Session) SetServerStatus(bit uint16) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.serverStatus |= bit
}

func (ses *Session) ClearServerStatus(bit uint16) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.serverStatus &= ^bit
}

func (ses *Session) ServerStatusIsSet(bit uint16) bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.serverStatus&bit != 0
}

/*
InMultiStmtTransactionMode checks the session is in multi-statement transaction mode.
OPTION_NOT_AUTOCOMMIT: After the autocommit is off, the multi-statement transaction is
started implicitly by the first statement of the transaction.
OPTION_BEGIN: Whenever the autocommit is on or off, the multi-statement transaction is
started explicitly by the BEGIN statement.

But it does not denote the transaction is active or not.

Cases    | set Autocommit = 1/0 | BEGIN statement |
---------------------------------------------------
Case1      1                       Yes
Case2      1                       No
Case3      0                       Yes
Case4      0                       No
---------------------------------------------------

If it is Case1,Case3,Cass4, Then

	InMultiStmtTransactionMode returns true.
	Also, the bit SERVER_STATUS_IN_TRANS will be set.

If it is Case2, Then

	InMultiStmtTransactionMode returns false
*/
func (ses *Session) InMultiStmtTransactionMode() bool {
	return ses.OptionBitsIsSet(OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)
}

/*
InActiveMultiStmtTransaction checks the session is in multi-statement transaction mode
and there is an active transaction.

But sometimes, the session does not start an active transaction even if it is in multi-
statement transaction mode.

For example: there is no active transaction.
set autocommit = 0;
select 1;

For example: there is an active transaction.
begin;
select 1;

When the statement starts the multi-statement transaction(select * from table), this flag
won't be set until we access the tables.
*/
func (ses *Session) InActiveMultiStmtTransaction() bool {
	return ses.ServerStatusIsSet(SERVER_STATUS_IN_TRANS)
}

/*
TxnStart starts the transaction implicitly and idempotent

When it is in multi-statement transaction mode:

	Set SERVER_STATUS_IN_TRANS bit;
	Starts a new transaction if there is none. Reuse the current transaction if there is one.

When it is not in single statement transaction mode:

	Starts a new transaction if there is none. Reuse the current transaction if there is one.
*/
func (ses *Session) TxnStart() error {
	var err error
	if ses.InMultiStmtTransactionMode() {
		ses.SetServerStatus(SERVER_STATUS_IN_TRANS)
	}
	if !ses.GetTxnHandler().IsValidTxn() {
		err = ses.GetTxnHandler().NewTxn()
	}
	return err
}

/*
TxnCommitSingleStatement commits the single statement transaction.

Cases    | set Autocommit = 1/0 | BEGIN statement |
---------------------------------------------------
Case1      1                       Yes
Case2      1                       No
Case3      0                       Yes
Case4      0                       No
---------------------------------------------------

If it is Case1,Case3,Cass4, Then

	InMultiStmtTransactionMode returns true.
	Also, the bit SERVER_STATUS_IN_TRANS will be set.

If it is Case2, Then

	InMultiStmtTransactionMode returns false
*/
func (ses *Session) TxnCommitSingleStatement(stmt tree.Statement) error {
	var err error
	/*
		Commit Rules:
		1, if it is in single-statement mode:
			it commits.
		2, if it is in multi-statement mode:
			if the statement is the one can be executed in the active transaction,
				the transaction need to be committed at the end of the statement.
	*/
	if !ses.InMultiStmtTransactionMode() ||
		ses.InActiveTransaction() && NeedToBeCommittedInActiveTransaction(stmt) {
		err = ses.GetTxnHandler().CommitTxn()
		ses.ClearServerStatus(SERVER_STATUS_IN_TRANS)
		ses.ClearOptionBits(OPTION_BEGIN)
	}
	return err
}

/*
TxnRollbackSingleStatement rollbacks the single statement transaction.

Cases    | set Autocommit = 1/0 | BEGIN statement |
---------------------------------------------------
Case1      1                       Yes
Case2      1                       No
Case3      0                       Yes
Case4      0                       No
---------------------------------------------------

If it is Case1,Case3,Cass4, Then

	InMultiStmtTransactionMode returns true.
	Also, the bit SERVER_STATUS_IN_TRANS will be set.

If it is Case2, Then

	InMultiStmtTransactionMode returns false
*/
func (ses *Session) TxnRollbackSingleStatement(stmt tree.Statement) error {
	var err error
	/*
			Rollback Rules:
			1, if it is in single-statement mode (Case2):
				it rollbacks.
			2, if it is in multi-statement mode (Case1,Case3,Case4):
		        the transaction need to be rollback at the end of the statement.
				(every error will abort the transaction.)
	*/
	if !ses.InMultiStmtTransactionMode() ||
		ses.InActiveTransaction() {
		err = ses.GetTxnHandler().RollbackTxn()
		ses.ClearServerStatus(SERVER_STATUS_IN_TRANS)
		ses.ClearOptionBits(OPTION_BEGIN)
	}
	return err
}

/*
TxnBegin begins a new transaction.
It commits the current transaction implicitly.
*/
func (ses *Session) TxnBegin() error {
	var err error
	if ses.InMultiStmtTransactionMode() {
		ses.ClearServerStatus(SERVER_STATUS_IN_TRANS)
		err = ses.GetTxnHandler().CommitTxn()
	}
	ses.ClearOptionBits(OPTION_BEGIN)
	if err != nil {
		/*
			fix issue 6024.
			When we get a w-w conflict during commit the txn,
			we convert the error into a readable error.
		*/
		if moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
			return moerr.NewInternalError(ses.GetRequestContext(), writeWriteConflictsErrorInfo())
		}
		return err
	}
	ses.SetOptionBits(OPTION_BEGIN)
	ses.SetServerStatus(SERVER_STATUS_IN_TRANS)
	err = ses.GetTxnHandler().NewTxn()
	return err
}

// TxnCommit commits the current transaction.
func (ses *Session) TxnCommit() error {
	var err error
	ses.ClearServerStatus(SERVER_STATUS_IN_TRANS | SERVER_STATUS_IN_TRANS_READONLY)
	err = ses.GetTxnHandler().CommitTxn()
	ses.ClearServerStatus(SERVER_STATUS_IN_TRANS)
	ses.ClearOptionBits(OPTION_BEGIN)
	return err
}

// TxnRollback rollbacks the current transaction.
func (ses *Session) TxnRollback() error {
	var err error
	ses.ClearServerStatus(SERVER_STATUS_IN_TRANS | SERVER_STATUS_IN_TRANS_READONLY)
	err = ses.GetTxnHandler().RollbackTxn()
	ses.ClearOptionBits(OPTION_BEGIN)
	return err
}

/*
InActiveTransaction checks if it is in an active transaction.
*/
func (ses *Session) InActiveTransaction() bool {
	if ses.InActiveMultiStmtTransaction() {
		return true
	} else {
		return ses.GetTxnHandler().IsValidTxn()
	}
}

/*
SetAutocommit sets the value of the system variable 'autocommit'.

The rule is that we can not execute the statement 'set parameter = value' in
an active transaction whichever it is started by BEGIN or in 'set autocommit = 0;'.
*/
func (ses *Session) SetAutocommit(on bool) error {
	if ses.InActiveTransaction() {
		return moerr.NewInternalError(ses.requestCtx, parameterModificationInTxnErrorInfo())
	}
	if on {
		ses.ClearOptionBits(OPTION_BEGIN | OPTION_NOT_AUTOCOMMIT)
		ses.SetServerStatus(SERVER_STATUS_AUTOCOMMIT)
	} else {
		ses.ClearServerStatus(SERVER_STATUS_AUTOCOMMIT)
		ses.SetOptionBits(OPTION_NOT_AUTOCOMMIT)
	}
	return nil
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
	ses.MakeProfile()
	sessionProfile := ses.GetConciseProfile()

	logDebugf(sessionProfile, "check special user")
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
	sqlForCheckTenant := getSqlForCheckTenant(tenant.GetTenant())
	pu := ses.GetParameterUnit()
	mp := ses.GetMemPool()
	logDebugf(sessionProfile, "check tenant %s exists", tenant)
	rsset, err = executeSQLInBackgroundSession(sysTenantCtx, mp, pu, sqlForCheckTenant, ses.GetAutoIncrCaches())
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

	logDebugf(sessionProfile, "check user of %s exists", tenant)
	//Get the password of the user in an independent session
	sqlForPasswordOfUser := getSqlForPasswordOfUser(tenant.GetUser())
	rsset, err = executeSQLInBackgroundSession(tenantCtx, mp, pu, sqlForPasswordOfUser, ses.GetAutoIncrCaches())
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
		logDebugf(sessionProfile, "check default role of user %s.", tenant)
		//step4 : check role exists or not
		sqlForCheckRoleExists := getSqlForRoleIdOfRole(tenant.GetDefaultRole())
		rsset, err = executeSQLInBackgroundSession(tenantCtx, mp, pu, sqlForCheckRoleExists, ses.GetAutoIncrCaches())
		if err != nil {
			return nil, err
		}

		if !execResultArrayHasData(rsset) {
			return nil, moerr.NewInternalError(tenantCtx, "there is no role %s", tenant.GetDefaultRole())
		}

		logDebugf(sessionProfile, "check granted role of user %s.", tenant)
		//step4.2 : check the role has been granted to the user or not
		sqlForRoleOfUser := getSqlForRoleOfUser(userID, tenant.GetDefaultRole())
		rsset, err = executeSQLInBackgroundSession(tenantCtx, mp, pu, sqlForRoleOfUser, ses.GetAutoIncrCaches())
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
		logDebugf(sessionProfile, "check designated role of user %s.", tenant)
		//the get name of default_role from mo_role
		sql := getSqlForRoleNameOfRoleId(defaultRoleID)
		rsset, err = executeSQLInBackgroundSession(tenantCtx, mp, pu, sql, ses.GetAutoIncrCaches())
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

	logInfo(sessionProfile, tenant.String())

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

func (th *TxnHandler) SetSession(ses *Session) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.ses = ses
}

func (th *TxnHandler) GetTxnClient() TxnClient {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.txnClient
}

// TxnClientNew creates a new txn
func (th *TxnHandler) TxnClientNew() error {
	var err error
	th.mu.Lock()
	defer th.mu.Unlock()
	if th.txnClient == nil {
		panic("must set txn client")
	}

	var opts []client.TxnOption
	rt := moruntime.ProcessLevelRuntime()
	if rt != nil {
		if v, ok := rt.GetGlobalVariables(moruntime.TxnOptions); ok {
			opts = v.([]client.TxnOption)
		}
	}

	th.txn, err = th.txnClient.New(opts...)
	if err != nil {
		return err
	}
	if th.txn == nil {
		return moerr.NewInternalError(th.ses.GetRequestContext(), "TxnClientNew: txnClient new a null txn")
	}
	return err
}

// NewTxn commits the old transaction if it existed.
// Then it creates the new transaction.
func (th *TxnHandler) NewTxn() error {
	var err error
	ctx := th.GetSession().GetRequestContext()
	if th.IsValidTxn() {
		err = th.CommitTxn()
		if err != nil {
			/*
				fix issue 6024.
				When we get a w-w conflict during commit the txn,
				we convert the error into a readable error.
			*/
			if moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
				return moerr.NewInternalError(ctx, writeWriteConflictsErrorInfo())
			}
			return err
		}
	}
	th.SetInvalid()
	defer func() {
		if err != nil {
			tenant := th.ses.GetTenantName(nil)
			incTransactionErrorsCounter(tenant, metric.SQLTypeBegin)
		}
	}()
	err = th.TxnClientNew()
	if err != nil {
		return err
	}
	if ctx == nil {
		panic("context should not be nil")
	}
	storage := th.GetStorage()
	err = storage.New(ctx, th.GetTxnOperator())
	return err
}

// IsValidTxn checks the transaction is true or not.
func (th *TxnHandler) IsValidTxn() bool {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.txn != nil
}

func (th *TxnHandler) SetInvalid() {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.txn = nil
}

func (th *TxnHandler) GetTxnOperator() TxnOperator {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.txn
}

func (th *TxnHandler) GetSession() *Session {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.ses
}

func (th *TxnHandler) CommitTxn() error {
	th.entryMu.Lock()
	defer th.entryMu.Unlock()
	if !th.IsValidTxn() {
		return nil
	}
	ses := th.GetSession()
	sessionProfile := ses.GetConciseProfile()
	ctx := ses.GetRequestContext()
	if ctx == nil {
		panic("context should not be nil")
	}
	if ses.tempTablestorage != nil {
		ctx = context.WithValue(ctx, defines.TemporaryDN{}, ses.tempTablestorage)
	}
	storage := th.GetStorage()
	ctx, cancel := context.WithTimeout(
		ctx,
		storage.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	var err, err2 error
	defer func() {
		// metric count
		tenant := ses.GetTenantName(nil)
		incTransactionCounter(tenant)
		if err != nil {
			incTransactionErrorsCounter(tenant, metric.SQLTypeCommit)
		}
	}()
	txnOp := th.GetTxnOperator()
	if txnOp == nil {
		logErrorf(sessionProfile, "CommitTxn: txn operator is null")
	}

	txnId := txnOp.Txn().DebugString()
	logDebugf(sessionProfile, "CommitTxn txnId:%s", txnId)
	defer func() {
		logDebugf(sessionProfile, "CommitTxn exit txnId:%s", txnId)
	}()
	if err = storage.Commit(ctx, txnOp); err != nil {
		th.SetInvalid()
		logErrorf(sessionProfile, "CommitTxn: storage commit failed. txnId:%s error:%v", txnId, err)
		if txnOp != nil {
			err2 = txnOp.Rollback(ctx)
			if err2 != nil {
				logErrorf(sessionProfile, "CommitTxn: txn operator rollback failed. txnId:%s error:%v", txnId, err2)
			}
		}
		return err
	}
	if txnOp != nil {
		err = txnOp.Commit(ctx)
		if err != nil {
			th.SetInvalid()
			logErrorf(sessionProfile, "CommitTxn: txn operator commit failed. txnId:%s error:%v", txnId, err)
		}
	}
	th.SetInvalid()
	return err
}

func (th *TxnHandler) RollbackTxn() error {
	th.entryMu.Lock()
	defer th.entryMu.Unlock()
	if !th.IsValidTxn() {
		return nil
	}
	ses := th.GetSession()
	sessionProfile := ses.GetConciseProfile()
	ctx := ses.GetRequestContext()
	if ctx == nil {
		panic("context should not be nil")
	}
	if ses.tempTablestorage != nil {
		ctx = context.WithValue(ctx, defines.TemporaryDN{}, ses.tempTablestorage)
	}
	storage := th.GetStorage()
	ctx, cancel := context.WithTimeout(
		ctx,
		storage.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	var err, err2 error
	defer func() {
		// metric count
		tenant := ses.GetTenantName(nil)
		incTransactionCounter(tenant)
		incTransactionErrorsCounter(tenant, metric.SQLTypeOther) // exec rollback cnt
		if err != nil {
			incTransactionErrorsCounter(tenant, metric.SQLTypeRollback)
		}
	}()
	txnOp := th.GetTxnOperator()
	if txnOp == nil {
		logErrorf(sessionProfile, "RollbackTxn: txn operator is null")
	}
	txnId := txnOp.Txn().DebugString()
	logDebugf(sessionProfile, "RollbackTxn txnId:%s", txnId)
	defer func() {
		logDebugf(sessionProfile, "RollbackTxn exit txnId:%s", txnId)
	}()
	if err = storage.Rollback(ctx, txnOp); err != nil {
		th.SetInvalid()
		logErrorf(sessionProfile, "RollbackTxn: storage rollback failed. txnId:%s error:%v", txnId, err)
		if txnOp != nil {
			err2 = txnOp.Rollback(ctx)
			if err2 != nil {
				logErrorf(sessionProfile, "RollbackTxn: txn operator rollback failed. txnId:%s error:%v", txnId, err2)
			}
		}
		return err
	}
	if txnOp != nil {
		err = txnOp.Rollback(ctx)
		if err != nil {
			th.SetInvalid()
			logErrorf(sessionProfile, "RollbackTxn: txn operator commit failed. txnId:%s error:%v", txnId, err)
		}
	}
	th.SetInvalid()
	return err
}

func (th *TxnHandler) GetStorage() engine.Engine {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.storage
}

func (th *TxnHandler) GetTxn() (TxnOperator, error) {
	err := th.GetSession().TxnStart()
	if err != nil {
		logutil.Errorf("GetTxn. error:%v", err)
		return nil, err
	}
	return th.GetTxnOperator(), nil
}

func (th *TxnHandler) GetTxnOnly() TxnOperator {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.txn
}

var _ plan2.CompilerContext = &TxnCompilerContext{}

type QueryType int

const (
	TXN_DEFAULT QueryType = iota
	TXN_DELETE
	TXN_UPDATE
)

type TxnCompilerContext struct {
	dbName               string
	QryTyp               QueryType
	txnHandler           *TxnHandler
	ses                  *Session
	proc                 *process.Process
	buildAlterView       bool
	dbOfView, nameOfView string
	mu                   sync.Mutex
}

func (tcc *TxnCompilerContext) SetBuildingAlterView(yesOrNo bool, dbName, viewName string) {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	tcc.buildAlterView = yesOrNo
	tcc.dbOfView = dbName
	tcc.nameOfView = viewName
}

func (tcc *TxnCompilerContext) GetBuildingAlterView() (bool, string, string) {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.buildAlterView, tcc.dbOfView, tcc.nameOfView
}

func InitTxnCompilerContext(txn *TxnHandler, db string) *TxnCompilerContext {
	return &TxnCompilerContext{txnHandler: txn, dbName: db, QryTyp: TXN_DEFAULT}
}

func (tcc *TxnCompilerContext) GetQueryType() QueryType {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.QryTyp
}

func (tcc *TxnCompilerContext) SetSession(ses *Session) {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	tcc.ses = ses
}

func (tcc *TxnCompilerContext) GetSession() *Session {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.ses
}

func (tcc *TxnCompilerContext) GetTxnHandler() *TxnHandler {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.txnHandler
}

func (tcc *TxnCompilerContext) GetUserName() string {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.ses.GetUserName()
}

func (tcc *TxnCompilerContext) SetQueryType(qryTyp QueryType) {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	tcc.QryTyp = qryTyp
}

func (tcc *TxnCompilerContext) SetDatabase(db string) {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	tcc.dbName = db
}

func (tcc *TxnCompilerContext) DefaultDatabase() string {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.dbName
}

func (tcc *TxnCompilerContext) GetRootSql() string {
	return tcc.GetSession().GetSql()
}

func (tcc *TxnCompilerContext) GetAccountId() uint32 {
	return tcc.ses.accountId
}

func (tcc *TxnCompilerContext) GetContext() context.Context {
	return tcc.ses.requestCtx
}

func (tcc *TxnCompilerContext) DatabaseExists(name string) bool {
	var err error
	var txn TxnOperator
	txn, err = tcc.GetTxnHandler().GetTxn()
	if err != nil {
		return false
	}
	//open database
	ses := tcc.GetSession()
	_, err = tcc.GetTxnHandler().GetStorage().Database(ses.GetRequestContext(), name, txn)
	if err != nil {
		logErrorf(ses.GetConciseProfile(), "get database %v failed. error %v", name, err)
		return false
	}

	return true
}

// getRelation returns the context (maybe updated) and the relation
func (tcc *TxnCompilerContext) getRelation(dbName string, tableName string) (context.Context, engine.Relation, error) {
	dbName, err := tcc.ensureDatabaseIsNotEmpty(dbName)
	if err != nil {
		return nil, nil, err
	}

	ses := tcc.GetSession()
	ctx := ses.GetRequestContext()
	account := ses.GetTenantInfo()
	if isClusterTable(dbName, tableName) {
		//if it is the cluster table in the general account, switch into the sys account
		if account != nil && account.GetTenantID() != sysAccountID {
			ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))
		}
	}

	txn, err := tcc.GetTxnHandler().GetTxn()
	if err != nil {
		return nil, nil, err
	}

	//open database
	db, err := tcc.GetTxnHandler().GetStorage().Database(ctx, dbName, txn)
	if err != nil {
		logErrorf(ses.GetConciseProfile(), "get database %v error %v", dbName, err)
		return nil, nil, err
	}

	tableNames, err := db.Relations(ctx)
	if err != nil {
		return nil, nil, err
	}
	logDebugf(ses.GetConciseProfile(), "dbName %v tableNames %v", dbName, tableNames)

	//open table
	table, err := db.Relation(ctx, tableName)
	if err != nil {
		tmpTable, e := tcc.getTmpRelation(ctx, engine.GetTempTableName(dbName, tableName))
		if e != nil {
			logutil.Errorf("get table %v error %v", tableName, err)
			return nil, nil, err
		} else {
			table = tmpTable
		}
	}
	return ctx, table, nil
}

func (tcc *TxnCompilerContext) getTmpRelation(ctx context.Context, tableName string) (engine.Relation, error) {
	e := tcc.ses.storage
	txn, err := tcc.txnHandler.GetTxn()
	if err != nil {
		return nil, err
	}
	db, err := e.Database(ctx, defines.TEMPORARY_DBNAME, txn)
	if err != nil {
		logutil.Errorf("get temp database error %v", err)
		return nil, err
	}
	table, err := db.Relation(ctx, tableName)
	return table, err
}

func (tcc *TxnCompilerContext) ensureDatabaseIsNotEmpty(dbName string) (string, error) {
	if len(dbName) == 0 {
		dbName = tcc.DefaultDatabase()
	}
	if len(dbName) == 0 {
		return "", moerr.NewNoDB(tcc.GetContext())
	}
	return dbName, nil
}

func (tcc *TxnCompilerContext) ResolveById(tableId uint64) (*plan2.ObjectRef, *plan2.TableDef) {
	ses := tcc.GetSession()
	ctx := ses.GetRequestContext()
	txn, err := tcc.GetTxnHandler().GetTxn()
	if err != nil {
		return nil, nil
	}
	dbName, tableName, table, err := tcc.GetTxnHandler().GetStorage().GetRelationById(ctx, txn, tableId)
	if err != nil {
		return nil, nil
	}
	return tcc.getTableDef(ctx, table, dbName, tableName)
}

func (tcc *TxnCompilerContext) Resolve(dbName string, tableName string) (*plan2.ObjectRef, *plan2.TableDef) {
	dbName, err := tcc.ensureDatabaseIsNotEmpty(dbName)
	if err != nil {
		return nil, nil
	}
	ctx, table, err := tcc.getRelation(dbName, tableName)
	if err != nil {
		return nil, nil
	}
	return tcc.getTableDef(ctx, table, dbName, tableName)
}

func (tcc *TxnCompilerContext) getTableDef(ctx context.Context, table engine.Relation, dbName, tableName string) (*plan2.ObjectRef, *plan2.TableDef) {
	tableId := table.GetTableID(ctx)
	engineDefs, err := table.TableDefs(ctx)
	if err != nil {
		return nil, nil
	}

	var clusterByDef *plan2.ClusterByDef
	var cols []*plan2.ColDef
	var defs []*plan2.TableDefType
	var properties []*plan2.Property
	var TableType, Createsql string
	var CompositePkey *plan2.ColDef = nil
	var partitionInfo *plan2.PartitionByDef
	var viewSql *plan2.ViewDef
	var foreignKeys []*plan2.ForeignKeyDef
	var primarykey *plan2.PrimaryKeyDef
	var refChildTbls []uint64
	for _, def := range engineDefs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			isCPkey := util.JudgeIsCompositePrimaryKeyColumn(attr.Attr.Name)
			col := &plan2.ColDef{
				ColId: attr.Attr.ID,
				Name:  attr.Attr.Name,
				Typ: &plan2.Type{
					Id:          int32(attr.Attr.Type.Oid),
					Width:       attr.Attr.Type.Width,
					Precision:   attr.Attr.Type.Precision,
					Scale:       attr.Attr.Type.Scale,
					AutoIncr:    attr.Attr.AutoIncrement,
					Table:       tableName,
					NotNullable: attr.Attr.Default != nil && !attr.Attr.Default.NullAbility,
				},
				Primary:   attr.Attr.Primary,
				Default:   attr.Attr.Default,
				OnUpdate:  attr.Attr.OnUpdate,
				Comment:   attr.Attr.Comment,
				ClusterBy: attr.Attr.ClusterBy,
			}
			if isCPkey {
				CompositePkey = col
				continue
			}
			if attr.Attr.ClusterBy {
				clusterByDef = &plan.ClusterByDef{
					Name: attr.Attr.Name,
				}
				if util.JudgeIsCompositeClusterByColumn(attr.Attr.Name) {
					continue
				}
			}
			cols = append(cols, col)
		} else if pro, ok := def.(*engine.PropertiesDef); ok {
			for _, p := range pro.Properties {
				switch p.Key {
				case catalog.SystemRelAttr_Kind:
					TableType = p.Value
				case catalog.SystemRelAttr_CreateSQL:
					Createsql = p.Value
				default:
				}
				properties = append(properties, &plan2.Property{
					Key:   p.Key,
					Value: p.Value,
				})
			}
		} else if viewDef, ok := def.(*engine.ViewDef); ok {
			viewSql = &plan2.ViewDef{
				View: viewDef.View,
			}
		} else if c, ok := def.(*engine.ConstraintDef); ok {
			for _, ct := range c.Cts {
				switch k := ct.(type) {
				case *engine.UniqueIndexDef:
					u := &plan.UniqueIndexDef{}
					err = u.UnMarshalUniqueIndexDef(([]byte)(k.UniqueIndex))
					if err != nil {
						return nil, nil
					}
					defs = append(defs, &plan.TableDef_DefType{
						Def: &plan.TableDef_DefType_UIdx{
							UIdx: u,
						},
					})
				case *engine.SecondaryIndexDef:
					s := &plan.SecondaryIndexDef{}
					err = s.UnMarshalSecondaryIndexDef(([]byte)(k.SecondaryIndex))
					if err != nil {
						return nil, nil
					}
					defs = append(defs, &plan.TableDef_DefType{
						Def: &plan.TableDef_DefType_SIdx{
							SIdx: s,
						},
					})
				case *engine.ForeignKeyDef:
					foreignKeys = k.Fkeys
				case *engine.RefChildTableDef:
					refChildTbls = k.Tables
				case *engine.PrimaryKeyDef:
					primarykey = k.Pkey
				}
			}
		} else if commnetDef, ok := def.(*engine.CommentDef); ok {
			properties = append(properties, &plan2.Property{
				Key:   catalog.SystemRelAttr_Comment,
				Value: commnetDef.Comment,
			})
		} else if partitionDef, ok := def.(*engine.PartitionDef); ok {
			p := &plan2.PartitionByDef{}
			err = p.UnMarshalPartitionInfo(([]byte)(partitionDef.Partition))
			if err != nil {
				return nil, nil
			}
			partitionInfo = p
		}
	}
	if len(properties) > 0 {
		defs = append(defs, &plan2.TableDefType{
			Def: &plan2.TableDef_DefType_Properties{
				Properties: &plan2.PropertiesDef{
					Properties: properties,
				},
			},
		})
	}

	if tcc.GetQueryType() != TXN_DEFAULT {
		hideKeys, err := table.GetHideKeys(ctx)
		if err != nil {
			return nil, nil
		}
		hideKey := hideKeys[0]
		cols = append(cols, &plan2.ColDef{
			Name: hideKey.Name,
			Typ: &plan2.Type{
				Id:        int32(hideKey.Type.Oid),
				Width:     hideKey.Type.Width,
				Precision: hideKey.Type.Precision,
				Scale:     hideKey.Type.Scale,
			},
			Primary: hideKey.Primary,
		})
	}

	//convert
	obj := &plan2.ObjectRef{
		SchemaName: dbName,
		ObjName:    tableName,
	}

	tableDef := &plan2.TableDef{
		TblId:         tableId,
		Name:          tableName,
		Cols:          cols,
		Defs:          defs,
		TableType:     TableType,
		Createsql:     Createsql,
		Pkey:          primarykey,
		CompositePkey: CompositePkey,
		ViewSql:       viewSql,
		Partition:     partitionInfo,
		Fkeys:         foreignKeys,
		RefChildTbls:  refChildTbls,
		ClusterBy:     clusterByDef,
	}
	return obj, tableDef
}

func (tcc *TxnCompilerContext) ResolveVariable(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
	if isSystemVar {
		if isGlobalVar {
			return tcc.GetSession().GetGlobalVar(varName)
		} else {
			return tcc.GetSession().GetSessionVar(varName)
		}
	} else {
		_, val, err := tcc.GetSession().GetUserDefinedVar(varName)
		return val, err
	}
}

func (tcc *TxnCompilerContext) ResolveAccountIds(accountNames []string) ([]uint32, error) {
	var err error
	var sql string
	var accountIds []uint32
	var erArray []ExecResult
	var targetAccountId uint64
	if len(accountNames) == 0 {
		return []uint32{}, nil
	}

	dedup := make(map[string]int8)
	for _, name := range accountNames {
		dedup[name] = 1
	}

	ses := tcc.GetSession()
	ctx := ses.GetRequestContext()
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	err = bh.Exec(ctx, "begin;")
	if err != nil {
		goto handleFailed
	}

	for name := range dedup {
		sql = getSqlForCheckTenant(name)
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sql)
		if err != nil {
			goto handleFailed
		}

		erArray, err = getResultSet(ctx, bh)
		if err != nil {
			goto handleFailed
		}

		if execResultArrayHasData(erArray) {
			for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
				targetAccountId, err = erArray[0].GetUint64(ctx, i, 0)
				if err != nil {
					goto handleFailed
				}
			}
			accountIds = append(accountIds, uint32(targetAccountId))
		} else {
			return nil, moerr.NewInternalError(ctx, "there is no account %s", name)
		}
	}

	err = bh.Exec(ctx, "commit;")
	if err != nil {
		goto handleFailed
	}
	return accountIds, err
handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return nil, rbErr
	}
	return nil, err
}

func (tcc *TxnCompilerContext) GetPrimaryKeyDef(dbName string, tableName string) []*plan2.ColDef {
	dbName, err := tcc.ensureDatabaseIsNotEmpty(dbName)
	if err != nil {
		return nil
	}
	ctx, relation, err := tcc.getRelation(dbName, tableName)
	if err != nil {
		return nil
	}

	priKeys, err := relation.GetPrimaryKeys(ctx)
	if err != nil {
		return nil
	}
	if len(priKeys) == 0 {
		return nil
	}

	priDefs := make([]*plan2.ColDef, 0, len(priKeys))
	for _, key := range priKeys {
		priDefs = append(priDefs, &plan2.ColDef{
			Name: key.Name,
			Typ: &plan2.Type{
				Id:        int32(key.Type.Oid),
				Width:     key.Type.Width,
				Precision: key.Type.Precision,
				Scale:     key.Type.Scale,
				Size:      key.Type.Size,
			},
			Primary: key.Primary,
		})
	}
	return priDefs
}

func (tcc *TxnCompilerContext) GetHideKeyDef(dbName string, tableName string) *plan2.ColDef {
	dbName, err := tcc.ensureDatabaseIsNotEmpty(dbName)
	if err != nil {
		return nil
	}
	ctx, relation, err := tcc.getRelation(dbName, tableName)
	if err != nil {
		return nil
	}

	hideKeys, err := relation.GetHideKeys(ctx)
	if err != nil {
		return nil
	}
	if len(hideKeys) == 0 {
		return nil
	}
	hideKey := hideKeys[0]

	hideDef := &plan2.ColDef{
		Name: hideKey.Name,
		Typ: &plan2.Type{
			Id:        int32(hideKey.Type.Oid),
			Width:     hideKey.Type.Width,
			Precision: hideKey.Type.Precision,
			Scale:     hideKey.Type.Scale,
			Size:      hideKey.Type.Size,
		},
		Primary: hideKey.Primary,
	}
	return hideDef
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

func (tcc *TxnCompilerContext) Stats(obj *plan2.ObjectRef, e *plan2.Expr) (stats *plan2.Stats) {
	stats = new(plan2.Stats)
	dbName := obj.GetSchemaName()
	dbName, err := tcc.ensureDatabaseIsNotEmpty(dbName)
	if err != nil {
		return
	}
	tableName := obj.GetObjName()
	ctx, table, err := tcc.getRelation(dbName, tableName)
	if err != nil {
		return
	}
	if e != nil {
		cols, _ := table.TableColumns(ctx)
		fixColumnName(cols, e)
	}
	blockNum, cost, outcnt, err := table.Stats(ctx, e)
	if err != nil {
		return
	}
	stats.Cost = float64(cost)
	stats.Outcnt = float64(outcnt)
	stats.BlockNum = blockNum
	return
}

func (tcc *TxnCompilerContext) GetProcess() *process.Process {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	return tcc.proc
}

func (tcc *TxnCompilerContext) GetQueryResultMeta(uuid string) ([]*plan.ColDef, string, error) {
	proc := tcc.proc
	// get file size
	fs := objectio.NewObjectFS(proc.FileService, catalog.QueryResultMetaDir)
	dirs, err := fs.ListDir(catalog.QueryResultMetaDir)
	if err != nil {
		return nil, "", err
	}
	var size int64 = -1
	name := catalog.BuildQueryResultMetaName(proc.SessionInfo.Account, uuid)
	for _, d := range dirs {
		if d.Name == name {
			size = d.Size
		}
	}
	if size == -1 {
		return nil, "", moerr.NewQueryIdNotFound(proc.Ctx, uuid)
	}
	// read meta's meta
	path := catalog.BuildQueryResultMetaPath(proc.SessionInfo.Account, uuid)
	reader, err := objectio.NewObjectReader(path, proc.FileService)
	if err != nil {
		return nil, "", err
	}
	bs, err := reader.ReadAllMeta(proc.Ctx, size, proc.Mp())
	if err != nil {
		return nil, "", err
	}
	idxs := make([]uint16, 2)
	idxs[0] = catalog.COLUMNS_IDX
	idxs[1] = catalog.RESULT_PATH_IDX
	// read meta's data
	iov, err := reader.Read(proc.Ctx, bs[0].GetExtent(), idxs, proc.Mp())
	if err != nil {
		return nil, "", err
	}
	// cols
	vec := vector.New(catalog.MetaColTypes[catalog.COLUMNS_IDX])
	if err = vec.Read(iov.Entries[0].Object.([]byte)); err != nil {
		return nil, "", err
	}
	def := vector.MustStrCols(vec)[0]
	r := &plan.ResultColDef{}
	if err = r.Unmarshal([]byte(def)); err != nil {
		return nil, "", err
	}
	// paths
	vec = vector.New(catalog.MetaColTypes[catalog.RESULT_PATH_IDX])
	if err = vec.Read(iov.Entries[1].Object.([]byte)); err != nil {
		return nil, "", err
	}
	str := vector.MustStrCols(vec)[0]
	return r.ResultCols, str, nil
}

func (tcc *TxnCompilerContext) SetProcess(proc *process.Process) {
	tcc.mu.Lock()
	defer tcc.mu.Unlock()
	tcc.proc = proc
}

// fakeDataSetFetcher gets the result set from the pipeline and save it in the session.
// It will not send the result to the client.
func fakeDataSetFetcher(handle interface{}, dataSet *batch.Batch) error {
	if handle == nil || dataSet == nil {
		return nil
	}

	ses := handle.(*Session)
	oq := newFakeOutputQueue(ses.GetMysqlResultSet())
	n := vector.Length(dataSet.Vecs[0])
	for j := 0; j < n; j++ { //row index
		if dataSet.Zs[j] <= 0 {
			continue
		}
		_, err := extractRowFromEveryVector(ses, dataSet, int64(j), oq)
		if err != nil {
			return err
		}
	}
	err := oq.flush()
	if err != nil {
		return err
	}
	ses.AppendMysqlResultSetOfBackgroundTask(ses.GetMysqlResultSet())
	return nil
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
func executeSQLInBackgroundSession(ctx context.Context, mp *mpool.MPool, pu *config.ParameterUnit, sql string, autoIncrCaches defines.AutoIncrCaches) ([]ExecResult, error) {
	bh := NewBackgroundHandler(ctx, mp, pu, autoIncrCaches)
	defer bh.Close()
	logutil.Debugf("background exec sql:%v", sql)
	err := bh.Exec(ctx, sql)
	logutil.Debugf("background exec sql done")
	if err != nil {
		return nil, err
	}

	//get the result set
	//TODO: debug further
	//mrsArray := ses.GetAllMysqlResultSet()
	//for _, mrs := range mrsArray {
	//	for i := uint64(0); i < mrs.GetRowCount(); i++ {
	//		row, err := mrs.GetRow(i)
	//		if err != nil {
	//			return err
	//		}
	//		logutil.Info(row)
	//	}
	//}

	return getResultSet(ctx, bh)
}

type BackgroundHandler struct {
	mce *MysqlCmdExecutor
	ses *BackgroundSession
}

var NewBackgroundHandler = func(ctx context.Context, mp *mpool.MPool, pu *config.ParameterUnit, autoincrcaches defines.AutoIncrCaches) BackgroundExec {
	bh := &BackgroundHandler{
		mce: NewMysqlCmdExecutor(),
		ses: NewBackgroundSession(ctx, mp, pu, GSysVariables, autoincrcaches),
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
	//logutil.Debugf("-->bh:%s", sql)
	err := bh.mce.GetDoQueryFunc()(ctx, sql)
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
