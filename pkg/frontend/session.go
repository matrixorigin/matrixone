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
	"fmt"

	"strings"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/matrixorigin/matrixone/pkg/vm/mempool"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
)

const MaxPrepareNumberInOneSession = 64

type ShowStatementType int

const (
	NotShowStatement   ShowStatementType = 0
	ShowCreateDatabase ShowStatementType = 1
	ShowCreateTable    ShowStatementType = 2
	ShowColumns        ShowStatementType = 3
)

type TxnHandler struct {
	storage engine.Engine
	txn     moengine.Txn
	ses     *Session
}

func InitTxnHandler(storage engine.Engine) *TxnHandler {
	return &TxnHandler{
		storage: storage,
	}
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

	closeRef      *CloseExportData
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
}

func NewSession(proto Protocol, gm *guest.Mmu, mp *mempool.Mempool, PU *config.ParameterUnit, gSysVars *GlobalSystemVariables) *Session {
	txnHandler := InitTxnHandler(config.StorageEngine)
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
		storage:         config.StorageEngine,
		sysVars:         gSysVars.CopySysVarsToSession(),
		userDefinedVars: make(map[string]interface{}),
		gSysVars:        gSysVars,

		serverStatus: 0,
		optionBits:   0,

		prepareStmts: make(map[string]*PrepareStmt),
	}
	ses.SetOptionBits(OPTION_AUTOCOMMIT)
	ses.txnCompileCtx.SetSession(ses)
	ses.txnHandler.SetSession(ses)
	return ses
}

func (ses *Session) SetPrepareStmt(name string, prepareStmt *PrepareStmt) error {
	if _, ok := ses.prepareStmts[name]; !ok {
		if len(ses.prepareStmts) >= MaxPrepareNumberInOneSession {
			return errors.New("", fmt.Sprintf("more than '%d' prepare statement in one session", MaxPrepareNumberInOneSession))
		}
	}
	ses.prepareStmts[name] = prepareStmt
	return nil
}

func (ses *Session) GetPrepareStmt(name string) (*PrepareStmt, error) {
	if prepareStmt, ok := ses.prepareStmts[name]; ok {
		return prepareStmt, nil
	}
	return nil, errors.New("", fmt.Sprintf("prepare statement '%s' does not exist", name))
}

func (ses *Session) RemovePrepareStmt(name string) {
	delete(ses.prepareStmts, name)
}

// SetGlobalVar sets the value of system variable in global.
//used by SET GLOBAL
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
		ses.sysVars[def.GetName()] = cv
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

//TxnCommit commits the current transaction.
func (ses *Session) TxnCommit() error {
	var err error
	ses.ClearServerStatus(SERVER_STATUS_IN_TRANS | SERVER_STATUS_IN_TRANS_READONLY)
	err = ses.txnHandler.CommitTxn()
	ses.ClearServerStatus(SERVER_STATUS_IN_TRANS)
	ses.ClearOptionBits(OPTION_BEGIN)
	return err
}

//TxnRollback rollbacks the current transaction.
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

func (th *TxnHandler) SetSession(ses *Session) {
	th.ses = ses
}

//NewTxn commits the old transaction if it existed.
//Then it creates the new transaction.
func (th *TxnHandler) NewTxn() error {
	var err error
	if th.IsValidTxn() {
		err = th.CommitTxn()
		if err != nil {
			return err
		}
	}
	th.SetInvalid()
	if taeEng, ok := th.storage.(moengine.TxnEngine); ok {
		//begin a transaction
		th.txn, err = taeEng.StartTxn(nil)
		if err != nil {
			logutil.Errorf("start tae txn error:%v", err)
			return err
		}
	}
	return err
}

//IsValidTxn checks the transaction is true or not.
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
	err := th.txn.Commit()
	th.SetInvalid()
	return err
}

func (th *TxnHandler) RollbackTxn() error {
	if !th.IsValidTxn() {
		return nil
	}
	err := th.txn.Rollback()
	th.SetInvalid()
	return err
}

func (th *TxnHandler) GetStorage() engine.Engine {
	return th.storage
}

func (th *TxnHandler) IsTaeEngine() bool {
	_, ok := th.storage.(moengine.TxnEngine)
	return ok
}

func (th *TxnHandler) GetTxn() moengine.Txn {
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

func (tcc *TxnCompilerContext) DatabaseExists(name string) bool {
	var err error
	//open database
	ctx := context.TODO()
	_, err = tcc.txnHandler.GetStorage().Database(ctx, name, engine.Snapshot(tcc.txnHandler.GetTxn().GetCtx()))
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

	ctx := context.TODO()
	//open database
	db, err := tcc.txnHandler.GetStorage().Database(ctx, dbName, engine.Snapshot(tcc.txnHandler.GetTxn().GetCtx()))
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
		return "", NewMysqlError(ER_NO_DB_ERROR)
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
	ctx := context.TODO()
	engineDefs, err := table.TableDefs(ctx)
	if err != nil {
		return nil, nil
	}

	var defs []*plan2.ColDef
	for _, def := range engineDefs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			defs = append(defs, &plan2.ColDef{
				Name: attr.Attr.Name,
				Typ: &plan2.Type{
					Id:        plan.Type_TypeId(attr.Attr.Type.Oid),
					Width:     attr.Attr.Type.Width,
					Precision: attr.Attr.Type.Precision,
					Scale:     attr.Attr.Type.Scale,
				},
				Primary: attr.Attr.Primary,
				Default: plan2.MakePlan2DefaultExpr(attr.Attr.Default),
			})
		}
	}
	if tcc.QryTyp != TXN_DEFAULT {
		hideKeys, err := table.GetHideKeys(ctx)
		if err != nil {
			return nil, nil
		}
		hideKey := hideKeys[0]
		defs = append(defs, &plan2.ColDef{
			Name: hideKey.Name,
			Typ: &plan2.Type{
				Id:        plan.Type_TypeId(hideKey.Type.Oid),
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
		Name: tableName,
		Cols: defs,
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
	ctx := context.TODO()
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
		priDefs = append(priDefs, &plan2.ColDef{
			Name: key.Name,
			Typ: &plan2.Type{
				Id:        plan.Type_TypeId(key.Type.Oid),
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
	ctx := context.TODO()
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
			Id:        plan.Type_TypeId(hideKey.Type.Oid),
			Width:     hideKey.Type.Width,
			Precision: hideKey.Type.Precision,
			Scale:     hideKey.Type.Scale,
			Size:      hideKey.Type.Size,
		},
		Primary: hideKey.Primary,
	}
	return hideDef
}

func (tcc *TxnCompilerContext) Cost(obj *plan2.ObjectRef, e *plan2.Expr) *plan2.Cost {
	dbName := obj.GetSchemaName()
	tableName := obj.GetObjName()
	dbName, err := tcc.ensureDatabaseIsNotEmpty(dbName)
	if err != nil {
		return nil
	}
	table, err := tcc.getRelation(dbName, tableName)
	if err != nil {
		return nil
	}
	rows := table.Rows()
	return &plan2.Cost{Card: float64(rows)}
}
