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
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/matrixorigin/matrixone/pkg/vm/mempool"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
)

const MaxPrepareNumberInOneSession = 64

// TODO: this variable should be configure by set variable
const MoDefaultErrorCount = 64

type ShowStatementType int

const (
	NotShowStatement ShowStatementType = 0
	ShowColumns      ShowStatementType = 1
)

type TxnHandler struct {
	storage   engine.Engine
	txnClient TxnClient
	ses       *Session
	txn       TxnOperator
}

func InitTxnHandler(storage engine.Engine, txnClient TxnClient) *TxnHandler {
	h := &TxnHandler{
		storage:   storage,
		txnClient: txnClient,
	}
	return h
}

type Session struct {
	//protocol layer
	protocol Protocol

	//cmd from the client
	Cmd int

	//for test
	Mrs *MysqlResultSet

	GuestMmu *guest.Mmu
	Mempool  *mempool.Mempool

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

func NewSession(proto Protocol, gm *guest.Mmu, mp *mempool.Mempool, PU *config.ParameterUnit, gSysVars *GlobalSystemVariables) *Session {
	txnHandler := InitTxnHandler(PU.StorageEngine, PU.TxnClient)
	ses := &Session{
		protocol: proto,
		GuestMmu: gm,
		Mempool:  mp,
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
	return ses
}

// BackgroundSession executing the sql in background
type BackgroundSession struct {
	*Session
	cancel context.CancelFunc
}

// NewBackgroundSession generates an independent background session executing the sql
func NewBackgroundSession(ctx context.Context, gm *guest.Mmu, mp *mempool.Mempool, PU *config.ParameterUnit, gSysVars *GlobalSystemVariables) *BackgroundSession {
	ses := NewSession(&FakeProtocol{}, gm, mp, PU, gSysVars)
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
func (ses *Session) GenNewStmtId() uint32 {
	ses.lastStmtId = ses.lastStmtId + 1
	return ses.lastStmtId
}

func (ses *Session) GetLastStmtId() uint32 {
	return ses.lastStmtId
}

func (ses *Session) SetRequestContext(reqCtx context.Context) {
	ses.requestCtx = reqCtx
}

func (ses *Session) GetRequestContext() context.Context {
	return ses.requestCtx
}

func (ses *Session) SetTimeZone(loc *time.Location) {
	ses.timeZone = loc
}

func (ses *Session) GetTimeZone() *time.Location {
	return ses.timeZone
}

func (ses *Session) SetMysqlResultSet(mrs *MysqlResultSet) {
	ses.Mrs = mrs
}

func (ses *Session) GetMysqlResultSet() *MysqlResultSet {
	return ses.Mrs
}

func (ses *Session) AppendMysqlResultSetOfBackgroundTask(mrs *MysqlResultSet) {
	ses.allResultSet = append(ses.allResultSet, mrs)
}

func (ses *Session) GetAllMysqlResultSet() []*MysqlResultSet {
	return ses.allResultSet
}

func (ses *Session) ClearAllMysqlResultSet() {
	if ses.allResultSet != nil {
		ses.allResultSet = ses.allResultSet[:0]
	}
}

func (ses *Session) GetTenantInfo() *TenantInfo {
	return ses.tenant
}

func (ses *Session) GetUUID() []byte {
	return ses.uuid[:]
}

func (ses *Session) SetTenantInfo(ti *TenantInfo) {
	ses.tenant = ti
}

func (ses *Session) SetPrepareStmt(name string, prepareStmt *PrepareStmt) error {
	if _, ok := ses.prepareStmts[name]; !ok {
		if len(ses.prepareStmts) >= MaxPrepareNumberInOneSession {
			return moerr.NewInvalidState("too many prepared statement, max %d", MaxPrepareNumberInOneSession)
		}
	}
	ses.prepareStmts[name] = prepareStmt
	return nil
}

func (ses *Session) GetPrepareStmt(name string) (*PrepareStmt, error) {
	if prepareStmt, ok := ses.prepareStmts[name]; ok {
		return prepareStmt, nil
	}
	return nil, moerr.NewInvalidState("prepared statement '%s' does not exist", name)
}

func (ses *Session) RemovePrepareStmt(name string) {
	delete(ses.prepareStmts, name)
}

// SetGlobalVar sets the value of system variable in global.
// used by SET GLOBAL
func (ses *Session) SetGlobalVar(name string, value interface{}) error {
	return ses.gSysVars.SetGlobalSysVar(name, value)
}

// GetGlobalVar gets this value of the system variable in global
func (ses *Session) GetGlobalVar(name string) (interface{}, error) {
	if def, val, ok := ses.gSysVars.GetGlobalSysVar(name); ok {
		if def.GetScope() == ScopeSession {
			//empty
			return nil, errorSystemVariableSessionEmpty
		}
		return val, nil
	}
	return nil, errorSystemVariableDoesNotExist
}

func (ses *Session) GetTxnCompileCtx() *TxnCompilerContext {
	return ses.txnCompileCtx
}

// SetSessionVar sets the value of system variable in session
func (ses *Session) SetSessionVar(name string, value interface{}) error {
	if def, _, ok := ses.gSysVars.GetGlobalSysVar(name); ok {
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
			ses.sysVars[def.GetName()] = cv
		} else {
			return def.UpdateSessVar(ses, ses.sysVars, def.GetName(), cv)
		}
	} else {
		return errorSystemVariableDoesNotExist
	}
	return nil
}

// GetSessionVar gets this value of the system variable in session
func (ses *Session) GetSessionVar(name string) (interface{}, error) {
	if def, gVal, ok := ses.gSysVars.GetGlobalSysVar(name); ok {
		ciname := strings.ToLower(name)
		if def.GetScope() == ScopeGlobal {
			return gVal, nil
		}
		return ses.sysVars[ciname], nil
	} else {
		return nil, errorSystemVariableDoesNotExist
	}
}

func (ses *Session) CopyAllSessionVars() map[string]interface{} {
	cp := make(map[string]interface{})
	for k, v := range ses.sysVars {
		cp[k] = v
	}
	return cp
}

// SetUserDefinedVar sets the user defined variable to the value in session
func (ses *Session) SetUserDefinedVar(name string, value interface{}) error {
	ses.userDefinedVars[strings.ToLower(name)] = value
	return nil
}

// GetUserDefinedVar gets value of the user defined variable
func (ses *Session) GetUserDefinedVar(name string) (SystemVariableType, interface{}, error) {
	val, ok := ses.userDefinedVars[strings.ToLower(name)]
	if !ok {
		return SystemVariableNullType{}, nil, nil
	}
	return InitSystemVariableStringType(name), val, nil
}

func (ses *Session) GetTxnHandler() *TxnHandler {
	return ses.txnHandler
}

func (ses *Session) GetTxnCompilerContext() *TxnCompilerContext {
	return ses.txnCompileCtx
}

func (ses *Session) SetSql(sql string) {
	ses.sql = sql
}

func (ses *Session) GetSql() string {
	return ses.sql
}

func (ses *Session) IsTaeEngine() bool {
	_, ok := ses.storage.(moengine.TxnEngine)
	return ok
}

func (ses *Session) GetStorage() engine.Engine {
	return ses.storage
}

func (ses *Session) GetDatabaseName() string {
	return ses.protocol.GetDatabaseName()
}

func (ses *Session) SetDatabaseName(db string) {
	ses.protocol.SetDatabaseName(db)
	ses.txnCompileCtx.SetDatabase(db)
}

func (ses *Session) DatabaseNameIsEmpty() bool {
	return len(ses.GetDatabaseName()) == 0
}

func (ses *Session) GetUserName() string {
	return ses.protocol.GetUserName()
}

func (ses *Session) SetUserName(uname string) {
	ses.protocol.SetUserName(uname)
}

func (ses *Session) GetConnectionID() uint32 {
	return ses.protocol.ConnectionID()
}

func (ses *Session) SetOptionBits(bit uint32) {
	ses.optionBits |= bit
}

func (ses *Session) ClearOptionBits(bit uint32) {
	ses.optionBits &= ^bit
}

func (ses *Session) OptionBitsIsSet(bit uint32) bool {
	return ses.optionBits&bit != 0
}

func (ses *Session) SetServerStatus(bit uint16) {
	ses.serverStatus |= bit
}

func (ses *Session) ClearServerStatus(bit uint16) {
	ses.serverStatus &= ^bit
}

func (ses *Session) ServerStatusIsSet(bit uint16) bool {
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
	if !ses.txnHandler.IsValidTxn() {
		err = ses.txnHandler.NewTxn()
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
		err = ses.txnHandler.CommitTxn()
		ses.ClearServerStatus(SERVER_STATUS_IN_TRANS)
		ses.ClearOptionBits(OPTION_BEGIN)
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
		err = ses.txnHandler.RollbackTxn()
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
		err = ses.txnHandler.CommitTxn()
	}
	ses.ClearOptionBits(OPTION_BEGIN)
	if err != nil {
		return err
	}
	ses.SetOptionBits(OPTION_BEGIN)
	ses.SetServerStatus(SERVER_STATUS_IN_TRANS)
	err = ses.txnHandler.NewTxn()
	return err
}

// TxnCommit commits the current transaction.
func (ses *Session) TxnCommit() error {
	var err error
	ses.ClearServerStatus(SERVER_STATUS_IN_TRANS | SERVER_STATUS_IN_TRANS_READONLY)
	err = ses.txnHandler.CommitTxn()
	ses.ClearServerStatus(SERVER_STATUS_IN_TRANS)
	ses.ClearOptionBits(OPTION_BEGIN)
	return err
}

// TxnRollback rollbacks the current transaction.
func (ses *Session) TxnRollback() error {
	var err error
	ses.ClearServerStatus(SERVER_STATUS_IN_TRANS | SERVER_STATUS_IN_TRANS_READONLY)
	err = ses.txnHandler.RollbackTxn()
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
		return ses.txnHandler.IsValidTxn()
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
	//Get tenant info
	tenant, err = GetTenantInfo(userInput)
	if err != nil {
		return nil, err
	}

	ses.SetTenantInfo(tenant)

	//step1 : check tenant exists or not in SYS tenant context
	sysTenantCtx := context.WithValue(ses.requestCtx, defines.TenantIDKey{}, uint32(sysAccountID))
	sysTenantCtx = context.WithValue(sysTenantCtx, defines.UserIDKey{}, uint32(rootID))
	sysTenantCtx = context.WithValue(sysTenantCtx, defines.RoleIDKey{}, uint32(moAdminRoleID))
	sqlForCheckTenant := getSqlForCheckTenant(tenant.GetTenant())
	rsset, err = executeSQLInBackgroundSession(sysTenantCtx, ses.GuestMmu, ses.Mempool, ses.Pu, sqlForCheckTenant)
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

	tenantCtx := context.WithValue(ses.requestCtx, defines.TenantIDKey{}, uint32(tenantID))

	//Get the password of the user in an independent session
	sqlForPasswordOfUser := getSqlForPasswordOfUser(tenant.GetUser())
	rsset, err = executeSQLInBackgroundSession(tenantCtx, ses.GuestMmu, ses.Mempool, ses.Pu, sqlForPasswordOfUser)
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
		rsset, err = executeSQLInBackgroundSession(tenantCtx, ses.GuestMmu, ses.Mempool, ses.Pu, sqlForCheckRoleExists)
		if err != nil {
			return nil, err
		}

		if len(rsset) < 1 || rsset[0].GetRowCount() < 1 {
			return nil, moerr.NewInternalError("there is no role %s", tenant.GetDefaultRole())
		}

		//step4.2 : check the role has been granted to the user or not
		sqlForRoleOfUser := getSqlForRoleOfUser(userID, tenant.GetDefaultRole())
		rsset, err = executeSQLInBackgroundSession(tenantCtx, ses.GuestMmu, ses.Mempool, ses.Pu, sqlForRoleOfUser)
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
		rsset, err = executeSQLInBackgroundSession(tenantCtx, ses.GuestMmu, ses.Mempool, ses.Pu, sql)
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
	return ses.priv
}

func (ses *Session) SetPrivilege(priv *privilege) {
	ses.priv = priv
}

func (th *TxnHandler) SetSession(ses *Session) {
	th.ses = ses
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
	if th.txnClient == nil {
		panic("must set txn client")
	}
	th.txn, err = th.txnClient.New()
	if err != nil {
		return err
	}
	ctx := th.ses.GetRequestContext()
	if ctx == nil {
		panic("context should not be nil")
	}
	ctx, cancel := context.WithTimeout(
		ctx,
		th.storage.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	return th.storage.New(ctx, th.txn)
}

// IsValidTxn checks the transaction is true or not.
func (th *TxnHandler) IsValidTxn() bool {
	return th.txn != nil
}

func (th *TxnHandler) SetInvalid() {
	th.txn = nil
}

func (th *TxnHandler) CommitTxn() error {
	if !th.IsValidTxn() {
		return nil
	}
	ctx := th.ses.GetRequestContext()
	if ctx == nil {
		panic("context should not be nil")
	}
	ctx, cancel := context.WithTimeout(
		ctx,
		th.storage.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	if err := th.storage.Commit(ctx, th.txn); err != nil {
		return err
	}
	err := th.txn.Commit(ctx)
	th.SetInvalid()
	return err
}

func (th *TxnHandler) RollbackTxn() error {
	if !th.IsValidTxn() {
		return nil
	}
	ctx := th.ses.GetRequestContext()
	if ctx == nil {
		panic("context should not be nil")
	}
	ctx, cancel := context.WithTimeout(
		ctx,
		th.storage.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	if err := th.storage.Rollback(ctx, th.txn); err != nil {
		return err
	}
	err := th.txn.Rollback(ctx)
	th.SetInvalid()
	return err
}

func (th *TxnHandler) GetStorage() engine.Engine {
	return th.storage
}

func (th *TxnHandler) GetTxn() TxnOperator {
	err := th.ses.TxnStart()
	if err != nil {
		panic(err)
	}
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
}

func InitTxnCompilerContext(txn *TxnHandler, db string) *TxnCompilerContext {
	return &TxnCompilerContext{txnHandler: txn, dbName: db, QryTyp: TXN_DEFAULT}
}

func (tcc *TxnCompilerContext) SetSession(ses *Session) {
	tcc.ses = ses
}

func (tcc *TxnCompilerContext) SetQueryType(qryTyp QueryType) {
	tcc.QryTyp = qryTyp
}

func (tcc *TxnCompilerContext) SetDatabase(db string) {
	tcc.dbName = db
}

func (tcc *TxnCompilerContext) DefaultDatabase() string {
	return tcc.dbName
}

func (tcc *TxnCompilerContext) GetRootSql() string {
	return tcc.ses.GetSql()
}

func (tcc *TxnCompilerContext) DatabaseExists(name string) bool {
	var err error
	//open database
	_, err = tcc.txnHandler.GetStorage().Database(tcc.ses.GetRequestContext(), name, tcc.txnHandler.GetTxn())
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

	ctx := tcc.ses.GetRequestContext()
	//open database
	db, err := tcc.txnHandler.GetStorage().Database(ctx, dbName, tcc.txnHandler.GetTxn())
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
	ctx := tcc.ses.GetRequestContext()
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
				},
				Primary:       attr.Attr.Primary,
				Default:       attr.Attr.Default,
				OnUpdate:      attr.Attr.OnUpdate,
				Comment:       attr.Attr.Comment,
				AutoIncrement: attr.Attr.AutoIncrement,
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

	if tcc.QryTyp != TXN_DEFAULT {
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
			return tcc.ses.GetGlobalVar(varName)
		} else {
			return tcc.ses.GetSessionVar(varName)
		}
	} else {
		_, val, err := tcc.ses.GetUserDefinedVar(varName)
		return val, err
	}
}

func (tcc *TxnCompilerContext) GetPrimaryKeyDef(dbName string, tableName string) []*plan2.ColDef {
	ctx := tcc.ses.GetRequestContext()
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
	ctx := tcc.ses.GetRequestContext()
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
	rows, err := table.Rows(tcc.ses.GetRequestContext())
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

func convertIntoResultSet(values []interface{}) ([]ExecResult, error) {
	rsset := make([]ExecResult, len(values))
	for i, value := range values {
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
func executeSQLInBackgroundSession(ctx context.Context, gm *guest.Mmu, mp *mempool.Mempool, pu *config.ParameterUnit, sql string) ([]ExecResult, error) {
	bh := NewBackgroundHandler(ctx, gm, mp, pu)
	defer bh.Close()
	err := bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}
	rsset := bh.GetExecResultSet()
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

	return convertIntoResultSet(rsset)
}

type BackgroundHandler struct {
	mce *MysqlCmdExecutor
	ses *BackgroundSession
}

var NewBackgroundHandler = func(ctx context.Context, gm *guest.Mmu, mp *mempool.Mempool, pu *config.ParameterUnit) BackgroundExec {
	bh := &BackgroundHandler{
		mce: NewMysqlCmdExecutor(),
		ses: NewBackgroundSession(ctx, gm, mp, pu, gSysVariables),
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
