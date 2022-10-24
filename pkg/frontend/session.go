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
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/util"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"

	"github.com/matrixorigin/matrixone/pkg/util/metric"
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
}

func InitTxnHandler(storage engine.Engine, txnClient TxnClient) *TxnHandler {
	h := &TxnHandler{
		storage:   storage,
		txnClient: txnClient,
	}
	return h
}

type Session struct {
	// account id
	accountId uint32

	//protocol layer
	protocol Protocol

	//cmd from the client
	Cmd int

	//for test
	Mrs *MysqlResultSet

	// mpool
	Mp *mpool.MPool

	Pu *config.ParameterUnit

	IsInternal bool

	Data         [][]interface{}
	ep           *tree.ExportParam
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

	mu sync.Mutex
}

// Clean up all resources hold by the session.  As of now, the mpool
func (ses *Session) Dispose() {
	mp := ses.GetMemPool()
	mpool.DeleteMPool(mp)
	ses.SetMemPool(mp)
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

func NewSession(proto Protocol, mp *mpool.MPool, PU *config.ParameterUnit, gSysVars *GlobalSystemVariables) *Session {
	var err error
	if mp == nil {
		// If no mp, we create one for session.  Use GuestMmuLimitation as cap.
		// fixed pool size can be another param, or should be computed from cap,
		// but here, too lazy, just use Mid.
		//
		// XXX MPOOL
		// We don't have a way to close a session, so the only sane way of creating
		// a mpool is to use NoFixed
		mp, err = mpool.NewMPool("session", PU.SV.GuestMmuLimitation, mpool.NoFixed)
		if err != nil {
			panic(err)
		}
	}

	txnHandler := InitTxnHandler(PU.StorageEngine, PU.TxnClient)
	ses := &Session{
		protocol: proto,
		Mp:       mp,
		Pu:       PU,
		ep: &tree.ExportParam{
			Outfile: false,
			Fields:  &tree.Fields{},
			Lines:   &tree.Lines{},
		},
		txnHandler: txnHandler,
		//TODO:fix database name after the catalog is ready
		txnCompileCtx:   InitTxnCompilerContext(txnHandler, proto.GetDatabaseName()),
		storage:         PU.StorageEngine,
		sysVars:         gSysVars.CopySysVarsToSession(),
		userDefinedVars: make(map[string]interface{}),
		gSysVars:        gSysVars,

		serverStatus: 0,
		optionBits:   0,

		prepareStmts:   make(map[string]*PrepareStmt),
		outputCallback: getDataFromPipeline,
		timeZone:       time.Local,
		errInfo: &errInfo{
			codes:  make([]uint16, 0, MoDefaultErrorCount),
			msgs:   make([]string, 0, MoDefaultErrorCount),
			maxCnt: MoDefaultErrorCount,
		},
	}
	ses.uuid, _ = uuid.NewUUID()
	ses.SetOptionBits(OPTION_AUTOCOMMIT)
	ses.txnCompileCtx.SetSession(ses)
	ses.txnHandler.SetSession(ses)

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
func NewBackgroundSession(ctx context.Context, mp *mpool.MPool, PU *config.ParameterUnit, gSysVars *GlobalSystemVariables) *BackgroundSession {
	ses := NewSession(&FakeProtocol{}, mp, PU, gSysVars)
	ses.SetOutputCallback(fakeDataSetFetcher)
	cancelBackgroundCtx, cancelBackgroundFunc := context.WithCancel(ctx)
	ses.SetRequestContext(cancelBackgroundCtx)
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
}

// GetBackgroundExec generates a background executor
func (ses *Session) GetBackgroundExec(ctx context.Context) BackgroundExec {
	return NewBackgroundHandler(ctx, ses.GetMemPool(), ses.GetParameterUnit())
}

func (ses *Session) GetIsInternal() bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.IsInternal
}

func (ses *Session) SetMemPool(mp *mpool.MPool) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.Mp = mp
}

func (ses *Session) GetMemPool() *mpool.MPool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.Mp
}

func (ses *Session) GetParameterUnit() *config.ParameterUnit {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.Pu
}

func (ses *Session) GetData() [][]interface{} {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.Data
}

func (ses *Session) SetData(data [][]interface{}) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.Data = data
}

func (ses *Session) AppendData(row []interface{}) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.Data = append(ses.Data, row)
}

func (ses *Session) SetExportParam(ep *tree.ExportParam) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.ep = ep
}

func (ses *Session) GetExportParam() *tree.ExportParam {
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

func (ses *Session) SetCmd(cmd int) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.Cmd = cmd
}

func (ses *Session) GetCmd() int {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.Cmd
}

func (ses *Session) SetMysqlResultSet(mrs *MysqlResultSet) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.Mrs = mrs
}

func (ses *Session) GetMysqlResultSet() *MysqlResultSet {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.Mrs
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
// With stmt = nil, should be only called in TxnBegin, TxnCommit, TxnRollback
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
			return moerr.NewInvalidState("too many prepared statement, max %d", MaxPrepareNumberInOneSession)
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
	return nil, moerr.NewInvalidState("prepared statement '%s' does not exist", name)
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
	return ses.GetGlobalSysVars().SetGlobalSysVar(name, value)
}

// GetGlobalVar gets this value of the system variable in global
func (ses *Session) GetGlobalVar(name string) (interface{}, error) {
	gSysVars := ses.GetGlobalSysVars()
	if def, val, ok := gSysVars.GetGlobalSysVar(name); ok {
		if def.GetScope() == ScopeSession {
			//empty
			return nil, errorSystemVariableSessionEmpty
		}
		return val, nil
	}
	return nil, errorSystemVariableDoesNotExist
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
			return errorSystemVariableIsGlobal
		}
		//scope session & both
		if !def.GetDynamic() {
			return errorSystemVariableIsReadOnly
		}

		cv, err := def.GetType().Convert(value)
		if err != nil {
			return err
		}

		if def.UpdateSessVar == nil {
			ses.SetSysVar(def.GetName(), cv)
		} else {
			return def.UpdateSessVar(ses, ses.GetSysVars(), def.GetName(), cv)
		}
	} else {
		return errorSystemVariableDoesNotExist
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
		return nil, errorSystemVariableDoesNotExist
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
	_, ok := ses.storage.(moengine.TxnEngine)
	return ok
}

func (ses *Session) GetStorage() engine.Engine {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	return ses.storage
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
OPTION_BEGAN: Whenever the autocommit is on or off, the multi-statement transaction is
started explicitly by the BEGIN statement.

But it does not denote the transaction is active or not.
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
		ses.InActiveTransaction() && IsStatementToBeCommittedInActiveTransaction(stmt) {
		err = ses.GetTxnHandler().CommitTxn()
		ses.ClearServerStatus(SERVER_STATUS_IN_TRANS)
		ses.ClearOptionBits(OPTION_BEGIN)
		// metric count
		tenant := ses.GetTenantName(stmt)
		incTransactionCounter(tenant)
		if err != nil {
			incTransactionErrorsCounter(tenant, metric.SQLTypeAutoCommit)
		}
	}
	return err
}

/*
TxnRollbackSingleStatement rollbacks the single statement transaction.
*/
func (ses *Session) TxnRollbackSingleStatement(stmt tree.Statement) error {
	var err error
	/*
		Rollback Rules:
		1, if it is in single-statement mode:
			it rollbacks.
		2, if it is in multi-statement mode:
			if the statement is the one can be executed in the active transaction,
				the transaction need to be rollback at the end of the statement.
	*/
	if !ses.InMultiStmtTransactionMode() ||
		ses.InActiveTransaction() && IsStatementToBeCommittedInActiveTransaction(stmt) {
		err = ses.GetTxnHandler().RollbackTxn()
		ses.ClearServerStatus(SERVER_STATUS_IN_TRANS)
		ses.ClearOptionBits(OPTION_BEGIN)
		// metric count
		tenant := ses.GetTenantName(stmt)
		incTransactionCounter(tenant)
		incTransactionErrorsCounter(tenant, metric.SQLTypeOther) // exec rollback cnt
		if err != nil {
			incTransactionErrorsCounter(tenant, metric.SQLTypeAutoRollback)
		}
	}
	return err
}

/*
TxnBegin begins a new transaction.
It commits the current transaction implicitly.
*/
func (ses *Session) TxnBegin() error {
	var err error
	tenant := ses.GetTenantName(nil)
	if ses.InMultiStmtTransactionMode() {
		ses.ClearServerStatus(SERVER_STATUS_IN_TRANS)
		err = ses.GetTxnHandler().CommitTxn()
		// metric count: last txn
		incTransactionCounter(tenant)
	}
	ses.ClearOptionBits(OPTION_BEGIN)
	if err != nil {
		// metric count: last txn commit failed.
		incTransactionErrorsCounter(tenant, metric.SQLTypeCommit)
		return err
	}
	ses.SetOptionBits(OPTION_BEGIN)
	ses.SetServerStatus(SERVER_STATUS_IN_TRANS)
	err = ses.GetTxnHandler().NewTxn()
	if err != nil {
		incTransactionErrorsCounter(tenant, metric.SQLTypeBegin)
	}
	return err
}

// TxnCommit commits the current transaction.
func (ses *Session) TxnCommit() error {
	var err error
	ses.ClearServerStatus(SERVER_STATUS_IN_TRANS | SERVER_STATUS_IN_TRANS_READONLY)
	err = ses.GetTxnHandler().CommitTxn()
	ses.ClearServerStatus(SERVER_STATUS_IN_TRANS)
	ses.ClearOptionBits(OPTION_BEGIN)
	// metric count
	tenant := ses.GetTenantName(nil)
	incTransactionCounter(tenant)
	if err != nil {
		incTransactionErrorsCounter(tenant, metric.SQLTypeCommit)
	}
	return err
}

// TxnRollback rollbacks the current transaction.
func (ses *Session) TxnRollback() error {
	var err error
	ses.ClearServerStatus(SERVER_STATUS_IN_TRANS | SERVER_STATUS_IN_TRANS_READONLY)
	err = ses.GetTxnHandler().RollbackTxn()
	ses.ClearOptionBits(OPTION_BEGIN)
	// metric count
	tenant := ses.GetTenantName(nil)
	incTransactionCounter(tenant)
	incTransactionErrorsCounter(tenant, metric.SQLTypeOther)
	if err != nil {
		incTransactionErrorsCounter(tenant, metric.SQLTypeRollback)
	}
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
		return errorParameterModificationInTxn
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

// AuthenticateUser verifies the password of the user.
func (ses *Session) AuthenticateUser(userInput string) ([]byte, error) {
	var defaultRoleID int64
	var defaultRole string
	var tenant *TenantInfo
	var err error
	var rsset []ExecResult
	var tenantID int64
	var userID int64
	var pwd string
	var pwdBytes []byte
	var isSpecial bool
	var specialAccount *TenantInfo

	//Get tenant info
	tenant, err = GetTenantInfo(userInput)
	if err != nil {
		return nil, err
	}

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
	rsset, err = executeSQLInBackgroundSession(sysTenantCtx, mp, pu, sqlForCheckTenant)
	if err != nil {
		return nil, err
	}
	if len(rsset) < 1 || rsset[0].GetRowCount() < 1 {
		return nil, moerr.NewInternalError("there is no tenant %s", tenant.GetTenant())
	}

	tenantID, err = rsset[0].GetInt64(0, 0)
	if err != nil {
		return nil, err
	}

	tenant.SetTenantID(uint32(tenantID))
	//step2 : check user exists or not in general tenant.
	//step3 : get the password of the user

	tenantCtx := context.WithValue(ses.GetRequestContext(), defines.TenantIDKey{}, uint32(tenantID))

	//Get the password of the user in an independent session
	sqlForPasswordOfUser := getSqlForPasswordOfUser(tenant.GetUser())
	rsset, err = executeSQLInBackgroundSession(tenantCtx, mp, pu, sqlForPasswordOfUser)
	if err != nil {
		return nil, err
	}
	if len(rsset) < 1 || rsset[0].GetRowCount() < 1 {
		return nil, moerr.NewInternalError("there is no user %s", tenant.GetUser())
	}

	userID, err = rsset[0].GetInt64(0, 0)
	if err != nil {
		return nil, err
	}

	pwd, err = rsset[0].GetString(0, 1)
	if err != nil {
		return nil, err
	}

	//the default_role in the mo_user table.
	//the default_role is always valid. public or other valid role.
	defaultRoleID, err = rsset[0].GetInt64(0, 2)
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
		//step4 : check role exists or not
		sqlForCheckRoleExists := getSqlForRoleIdOfRole(tenant.GetDefaultRole())
		rsset, err = executeSQLInBackgroundSession(tenantCtx, mp, pu, sqlForCheckRoleExists)
		if err != nil {
			return nil, err
		}

		if len(rsset) < 1 || rsset[0].GetRowCount() < 1 {
			return nil, moerr.NewInternalError("there is no role %s", tenant.GetDefaultRole())
		}

		//step4.2 : check the role has been granted to the user or not
		sqlForRoleOfUser := getSqlForRoleOfUser(userID, tenant.GetDefaultRole())
		rsset, err = executeSQLInBackgroundSession(tenantCtx, mp, pu, sqlForRoleOfUser)
		if err != nil {
			return nil, err
		}
		if len(rsset) < 1 || rsset[0].GetRowCount() < 1 {
			return nil, moerr.NewInternalError("the role %s has not been granted to the user %s",
				tenant.GetDefaultRole(), tenant.GetUser())
		}

		defaultRoleID, err = rsset[0].GetInt64(0, 0)
		if err != nil {
			return nil, err
		}
		tenant.SetDefaultRoleID(uint32(defaultRoleID))
	} else {
		//the get name of default_role from mo_role
		sql := getSqlForRoleNameOfRoleId(defaultRoleID)
		rsset, err = executeSQLInBackgroundSession(tenantCtx, mp, pu, sql)
		if err != nil {
			return nil, err
		}
		if len(rsset) < 1 || rsset[0].GetRowCount() < 1 {
			return nil, moerr.NewInternalError("get the default role of the user %s failed", tenant.GetUser())
		}

		defaultRole, err = rsset[0].GetString(0, 0)
		if err != nil {
			return nil, err
		}
		tenant.SetDefaultRole(defaultRole)
	}

	logutil.Info(tenant.String())

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
	th.txn, err = th.txnClient.New()
	if err != nil {
		return err
	}
	return err
}

// NewTxn commits the old transaction if it existed.
// Then it creates the new transaction.
func (th *TxnHandler) NewTxn() error {
	var err error
	if th.IsValidTxn() {
		err = th.CommitTxn()
		if err != nil {
			return err
		}
	}
	th.SetInvalid()
	err = th.TxnClientNew()
	if err != nil {
		return err
	}
	ctx := th.GetSession().GetRequestContext()
	if ctx == nil {
		panic("context should not be nil")
	}
	storage := th.GetStorage()
	ctx, cancel := context.WithTimeout(
		ctx,
		storage.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	return storage.New(ctx, th.GetTxnOperator())
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
	if !th.IsValidTxn() {
		return nil
	}
	ctx := th.GetSession().GetRequestContext()
	if ctx == nil {
		panic("context should not be nil")
	}
	storage := th.GetStorage()
	ctx, cancel := context.WithTimeout(
		ctx,
		storage.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	txnOp := th.GetTxnOperator()
	if err := storage.Commit(ctx, txnOp); err != nil {
		txnOp.Rollback(ctx)
		th.SetInvalid()
		return err
	}
	err := txnOp.Commit(ctx)
	th.SetInvalid()
	return err
}

func (th *TxnHandler) RollbackTxn() error {
	if !th.IsValidTxn() {
		return nil
	}
	ctx := th.GetSession().GetRequestContext()
	if ctx == nil {
		panic("context should not be nil")
	}
	storage := th.GetStorage()
	ctx, cancel := context.WithTimeout(
		ctx,
		storage.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	txnOp := th.GetTxnOperator()
	if err := storage.Rollback(ctx, txnOp); err != nil {
		th.SetInvalid()
		return err
	}
	err := txnOp.Rollback(ctx)
	th.SetInvalid()
	return err
}

func (th *TxnHandler) GetStorage() engine.Engine {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.storage
}

func (th *TxnHandler) GetTxn() TxnOperator {
	err := th.GetSession().TxnStart()
	if err != nil {
		panic(err)
	}
	return th.GetTxnOperator()
}

func (th *TxnHandler) GetTxnOnly() TxnOperator {
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
	dbName     string
	QryTyp     QueryType
	txnHandler *TxnHandler
	ses        *Session
	mu         sync.Mutex
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

func (tcc *TxnCompilerContext) DatabaseExists(name string) bool {
	var err error
	//open database
	_, err = tcc.GetTxnHandler().GetStorage().Database(tcc.GetSession().GetRequestContext(), name, tcc.GetTxnHandler().GetTxn())
	if err != nil {
		logutil.Errorf("get database %v failed. error %v", name, err)
		return false
	}

	return true
}

func (tcc *TxnCompilerContext) getRelation(dbName string, tableName string) (engine.Relation, error) {
	dbName, err := tcc.ensureDatabaseIsNotEmpty(dbName)
	if err != nil {
		return nil, err
	}

	ctx := tcc.GetSession().GetRequestContext()
	//open database
	db, err := tcc.GetTxnHandler().GetStorage().Database(ctx, dbName, tcc.GetTxnHandler().GetTxn())
	if err != nil {
		logutil.Errorf("get database %v error %v", dbName, err)
		return nil, err
	}

	tableNames, err := db.Relations(ctx)
	if err != nil {
		return nil, err
	}
	logutil.Infof("dbName %v tableNames %v", dbName, tableNames)

	//open table
	table, err := db.Relation(ctx, tableName)
	if err != nil {
		logutil.Errorf("get table %v error %v", tableName, err)
		return nil, err
	}
	table.Ranges(ctx, nil) // TODO
	return table, nil
}

func (tcc *TxnCompilerContext) ensureDatabaseIsNotEmpty(dbName string) (string, error) {
	if len(dbName) == 0 {
		dbName = tcc.DefaultDatabase()
	}
	if len(dbName) == 0 {
		return "", moerr.NewNoDB()
	}
	return dbName, nil
}

func (tcc *TxnCompilerContext) Resolve(dbName string, tableName string) (*plan2.ObjectRef, *plan2.TableDef) {
	dbName, err := tcc.ensureDatabaseIsNotEmpty(dbName)
	if err != nil {
		return nil, nil
	}
	table, err := tcc.getRelation(dbName, tableName)
	if err != nil {
		return nil, nil
	}
	ctx := tcc.GetSession().GetRequestContext()
	engineDefs, err := table.TableDefs(ctx)
	if err != nil {
		return nil, nil
	}

	var cols []*plan2.ColDef
	var defs []*plan2.TableDefType
	var properties []*plan2.Property
	var TableType, Createsql string
	var CompositePkey *plan2.ColDef = nil
	for _, def := range engineDefs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			isCPkey := util.JudgeIsCompositePrimaryKeyColumn(attr.Attr.Name)
			col := &plan2.ColDef{
				Name: attr.Attr.Name,
				Typ: &plan2.Type{
					Id:        int32(attr.Attr.Type.Oid),
					Width:     attr.Attr.Type.Width,
					Precision: attr.Attr.Type.Precision,
					Scale:     attr.Attr.Type.Scale,
					AutoIncr:  attr.Attr.AutoIncrement,
					Table:     tableName,
				},
				Primary:  attr.Attr.Primary,
				Default:  attr.Attr.Default,
				OnUpdate: attr.Attr.OnUpdate,
				Comment:  attr.Attr.Comment,
			}
			if isCPkey {
				col.IsCPkey = isCPkey
				CompositePkey = col
				continue
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
			defs = append(defs, &plan2.TableDefType{
				Def: &plan2.TableDef_DefType_View{
					View: &plan2.ViewDef{
						View: viewDef.View,
					},
				},
			})
		} else if commnetDef, ok := def.(*engine.CommentDef); ok {
			properties = append(properties, &plan2.Property{
				Key:   catalog.SystemRelAttr_Comment,
				Value: commnetDef.Comment,
			})
		} else if partitionDef, ok := def.(*engine.PartitionDef); ok {
			p := &plan2.PartitionInfo{}
			err = p.UnMarshalPartitionInfo(([]byte)(partitionDef.Partition))
			if err != nil {
				return nil, nil
			}
			defs = append(defs, &plan2.TableDefType{
				Def: &plan2.TableDef_DefType_Partition{
					Partition: p,
				},
			})
		} else if indexDef, ok := def.(*engine.ComputeIndexDef); ok {
			defs = append(defs, &plan2.TableDefType{
				Def: &plan2.TableDef_DefType_ComputeIndex{
					ComputeIndex: &plan2.ComputeIndexDef{
						Names:      indexDef.Names,
						TableNames: indexDef.TableNames,
						Uniques:    indexDef.Uniques,
					},
				},
			})
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
		Name:          tableName,
		Cols:          cols,
		Defs:          defs,
		TableType:     TableType,
		Createsql:     Createsql,
		CompositePkey: CompositePkey,
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

func (tcc *TxnCompilerContext) GetPrimaryKeyDef(dbName string, tableName string) []*plan2.ColDef {
	ctx := tcc.GetSession().GetRequestContext()
	dbName, err := tcc.ensureDatabaseIsNotEmpty(dbName)
	if err != nil {
		return nil
	}
	relation, err := tcc.getRelation(dbName, tableName)
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
		isCPkey := util.JudgeIsCompositePrimaryKeyColumn(key.Name)
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
			IsCPkey: isCPkey,
		})
	}
	return priDefs
}

func (tcc *TxnCompilerContext) GetHideKeyDef(dbName string, tableName string) *plan2.ColDef {
	ctx := tcc.GetSession().GetRequestContext()
	dbName, err := tcc.ensureDatabaseIsNotEmpty(dbName)
	if err != nil {
		return nil
	}
	relation, err := tcc.getRelation(dbName, tableName)
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

func (tcc *TxnCompilerContext) Cost(obj *plan2.ObjectRef, e *plan2.Expr) (cost *plan2.Cost) {
	cost = new(plan2.Cost)
	dbName := obj.GetSchemaName()
	dbName, err := tcc.ensureDatabaseIsNotEmpty(dbName)
	if err != nil {
		return
	}
	tableName := obj.GetObjName()
	table, err := tcc.getRelation(dbName, tableName)
	if err != nil {
		return
	}
	rows, err := table.Rows(tcc.GetSession().GetRequestContext())
	if err != nil {
		return
	}
	cost.Card = float64(rows)
	return
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
func getResultSet(bh BackgroundExec) ([]ExecResult, error) {
	results := bh.GetExecResultSet()
	rsset := make([]ExecResult, len(results))
	for i, value := range results {
		if er, ok := value.(ExecResult); ok {
			rsset[i] = er
		} else {
			return nil, moerr.NewInternalError("it is not the type of result set")
		}
	}
	return rsset, nil
}

// executeSQLInBackgroundSession executes the sql in an independent session and transaction.
// It sends nothing to the client.
func executeSQLInBackgroundSession(ctx context.Context, mp *mpool.MPool, pu *config.ParameterUnit, sql string) ([]ExecResult, error) {
	bh := NewBackgroundHandler(ctx, mp, pu)
	defer bh.Close()
	err := bh.Exec(ctx, sql)
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

	return getResultSet(bh)
}

type BackgroundHandler struct {
	mce *MysqlCmdExecutor
	ses *BackgroundSession
}

var NewBackgroundHandler = func(ctx context.Context, mp *mpool.MPool, pu *config.ParameterUnit) BackgroundExec {
	bh := &BackgroundHandler{
		mce: NewMysqlCmdExecutor(),
		ses: NewBackgroundSession(ctx, mp, pu, gSysVariables),
	}
	return bh
}

func (bh *BackgroundHandler) Close() {
	bh.mce.Close()
	bh.ses.Close()
}

func (bh *BackgroundHandler) Exec(ctx context.Context, sql string) error {
	bh.mce.PrepareSessionBeforeExecRequest(bh.ses.Session)
	if ctx == nil {
		ctx = bh.ses.GetRequestContext()
	}
	err := bh.mce.doComQuery(ctx, sql)
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
