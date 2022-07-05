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
	goErrors "errors"
	"fmt"

	"strings"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/matrixorigin/matrixone/pkg/vm/mempool"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
)

var (
	errorTaeTxnBeginInBegan           = goErrors.New("begin txn in the began txn")
	errorTaeTxnHasNotBeenBegan        = goErrors.New("the txn has not been began")
	errorTaeTxnAutocommitInAutocommit = goErrors.New("start autocommit txn in the autocommit txn")
	errorTaeTxnBeginInAutocommit      = goErrors.New("begin txn in the autocommit txn")
	errorTaeTxnAutocommitInBegan      = goErrors.New("start autocommit txn in the txn has been began")
	errorIsNotAutocommitTxn           = goErrors.New("it is not autocommit txn")
	errorIsNotBeginCommitTxn          = goErrors.New("it is not the begin/commit txn ")
	errorTaeTxnInIllegalState         = goErrors.New("the txn is in the illegal state and needed to be cleaned before using again")
)

const (
	TxnInit       = iota // when the TxnState instance has just been created
	TxnBegan             // when the txn has been started by the BEGIN statement
	TxnAutocommit        // when the txn has been started by the automatic creation
	TxnEnd               // when the txn has been committed by the COMMIT statement or the automatic commit or the ROLLBACK statement
	TxnErr               // when the txn operation generates errors
	TxnNil               // placeholder
)

// TxnState represents for Transaction Machine
type TxnState struct {
	state     int
	fromState int
	err       error
}

type ShowStatementType int

const (
	NotShowStatement   ShowStatementType = 0
	ShowCreateDatabase ShowStatementType = 1
	ShowCreateTable    ShowStatementType = 2
	ShowColumns        ShowStatementType = 3
)

func InitTxnState() *TxnState {
	return &TxnState{
		state:     TxnInit,
		fromState: TxnNil,
		err:       nil,
	}
}

func (ts *TxnState) isState(s int) bool {
	return ts.state == s
}

func (ts *TxnState) switchToState(s int, err error) {
	logutil.Infof("switch from %d to %d", ts.state, s)
	ts.fromState = ts.state
	ts.state = s
	ts.err = err
}

func (ts *TxnState) getState() int {
	return ts.state
}

func (ts *TxnState) getFromState() int {
	return ts.fromState
}

func (ts *TxnState) getError() error {
	return ts.err
}

func (ts *TxnState) String() string {
	return fmt.Sprintf("state:%d fromState:%d err:%v", ts.state, ts.fromState, ts.err)
}

var _ moengine.Txn = &TaeTxnDumpImpl{}

//TaeTxnDumpImpl is just a placeholder and does nothing
type TaeTxnDumpImpl struct {
}

func InitTaeTxnDumpImpl() *TaeTxnDumpImpl {
	return &TaeTxnDumpImpl{}
}

func (tti *TaeTxnDumpImpl) GetCtx() []byte {
	return nil
}

func (tti *TaeTxnDumpImpl) GetID() uint64 {
	return 0
}

func (tti *TaeTxnDumpImpl) Commit() error {
	return nil
}

func (tti *TaeTxnDumpImpl) Rollback() error {
	return nil
}

func (tti *TaeTxnDumpImpl) String() string {
	return "TaeTxnDumpImpl"
}

func (tti *TaeTxnDumpImpl) Repr() string {
	return "TaeTxnDumpImpl.Repr"
}

func (tti *TaeTxnDumpImpl) GetError() error {
	return nil
}

type TxnHandler struct {
	storage  engine.Engine
	taeTxn   moengine.Txn
	txnState *TxnState
}

func InitTxnHandler(storage engine.Engine) *TxnHandler {
	return &TxnHandler{
		taeTxn:   InitTaeTxnDumpImpl(),
		txnState: InitTxnState(),
		storage:  storage,
	}
}

type Session struct {
	//protocol layer
	protocol Protocol

	//epoch gc handler
	pdHook *PDCallbackImpl

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
}

func NewSession(proto Protocol, pdHook *PDCallbackImpl, gm *guest.Mmu, mp *mempool.Mempool, PU *config.ParameterUnit, gSysVars *GlobalSystemVariables) *Session {
	txnHandler := InitTxnHandler(config.StorageEngine)
	ses := &Session{
		protocol: proto,
		pdHook:   pdHook,
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
	}
	ses.txnCompileCtx.SetSession(ses)
	return ses
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

func (ses *Session) GetEpochgc() *PDCallbackImpl {
	return ses.pdHook
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

func (th *TxnHandler) GetStorage() engine.Engine {
	return th.storage
}

func (th *TxnHandler) getTxnState() int {
	return th.txnState.getState()
}

func (th *TxnHandler) isTxnState(s int) bool {
	return th.txnState.isState(s)
}

func (th *TxnHandler) switchToTxnState(s int, err error) {
	th.txnState.switchToState(s, err)
}

func (th *TxnHandler) getFromTxnState() int {
	return th.txnState.getFromState()
}

func (th *TxnHandler) getTxnStateError() error {
	return th.txnState.getError()
}

func (th *TxnHandler) getTxnStateString() string {
	return th.txnState.String()
}

// IsInTaeTxn checks the session executes a txn
func (th *TxnHandler) IsInTaeTxn() bool {
	st := th.getTxnState()
	logutil.Infof("current txn state %d", st)
	if st == TxnAutocommit || st == TxnBegan {
		return true
	}
	return false
}

func (th *TxnHandler) IsTaeEngine() bool {
	_, ok := th.storage.(moengine.TxnEngine)
	return ok
}

func (th *TxnHandler) createTxn(beganErr, autocommitErr error) (moengine.Txn, error) {
	var err error
	var txn moengine.Txn
	if taeEng, ok := th.storage.(moengine.TxnEngine); ok {
		switch th.txnState.getState() {
		case TxnInit, TxnEnd:
			//begin a transaction
			txn, err = taeEng.StartTxn(nil)
		case TxnBegan:
			err = beganErr
		case TxnAutocommit:
			err = autocommitErr
		case TxnErr:
			err = errorTaeTxnInIllegalState
		}
		if txn == nil {
			txn = InitTaeTxnDumpImpl()
		}
	} else {
		txn = InitTaeTxnDumpImpl()
	}

	return txn, err
}

func (th *TxnHandler) StartByBegin() error {
	logutil.Infof("start txn by begin")
	var err error
	th.taeTxn, err = th.createTxn(errorTaeTxnBeginInBegan, errorTaeTxnBeginInAutocommit)
	if err == nil {
		th.txnState.switchToState(TxnBegan, err)
	} else {
		th.txnState.switchToState(TxnErr, err)
	}
	return err
}

func (th *TxnHandler) StartByAutocommit() error {
	logutil.Infof("start txn by autocommit")
	var err error
	th.taeTxn, err = th.createTxn(errorTaeTxnAutocommitInBegan, errorTaeTxnAutocommitInAutocommit)
	if err == nil {
		th.txnState.switchToState(TxnAutocommit, err)
	} else {
		th.txnState.switchToState(TxnErr, err)
	}
	return err
}

// StartByAutocommitIfNeeded starts a new txn or uses an existed txn
// true denotes a new txn
func (th *TxnHandler) StartByAutocommitIfNeeded() (bool, error) {
	logutil.Infof("start txn autocommit if needed")
	var err error
	if th.IsInTaeTxn() {
		return false, nil
	}
	logutil.Infof("need create new txn")
	err = th.StartByAutocommit()
	return true, err
}

func (th *TxnHandler) GetTxn() moengine.Txn {
	return th.taeTxn
}

const (
	TxnCommitAfterBegan = iota
	TxnCommitAfterAutocommit
	TxnCommitAfterAutocommitOnly
)

func (th *TxnHandler) commit(option int) error {
	var err error
	var switchTxnState bool = true
	switch th.getTxnState() {
	case TxnBegan:
		switch option {
		case TxnCommitAfterBegan:
			err = th.taeTxn.Commit()
		case TxnCommitAfterAutocommit:
			err = errorIsNotAutocommitTxn
		case TxnCommitAfterAutocommitOnly:
			//if it is the txn started by BEGIN statement,
			//we do not commit it.
			switchTxnState = false
		}
	case TxnAutocommit:
		switch option {
		case TxnCommitAfterBegan:
			err = errorIsNotBeginCommitTxn
		case TxnCommitAfterAutocommit, TxnCommitAfterAutocommitOnly:
			err = th.taeTxn.Commit()
		}
	case TxnInit, TxnEnd:
		err = errorTaeTxnHasNotBeenBegan
	case TxnErr:
		err = errorTaeTxnInIllegalState
	}

	if switchTxnState {
		if err == nil {
			th.txnState.switchToState(TxnEnd, err)
		} else {
			th.txnState.switchToState(TxnErr, err)
		}
	}
	return err
}

// CommitAfterBegin commits the tae txn started by the BEGIN statement
func (th *TxnHandler) CommitAfterBegin() error {
	logutil.Infof("commit began")
	var err error
	err = th.commit(TxnCommitAfterBegan)
	return err
}

// CommitAfterAutocommit commits the tae txn started by autocommit
func (th *TxnHandler) CommitAfterAutocommit() error {
	logutil.Infof("commit autocommit")
	var err error
	err = th.commit(TxnCommitAfterAutocommit)
	return err
}

// CommitAfterAutocommitOnly commits the tae txn started by autocommit
// Do not check TxnBegan
func (th *TxnHandler) CommitAfterAutocommitOnly() error {
	logutil.Infof("commit autocommit only")
	var err error
	err = th.commit(TxnCommitAfterAutocommitOnly)
	return err
}

const (
	TxnRollbackAfterBeganAndAutocommit = iota
	TxnRollbackAfterAutocommitOnly
)

func (th *TxnHandler) rollback(option int) error {
	var err error
	var switchTxnState bool = true
	switch th.getTxnState() {
	case TxnBegan:
		switch option {
		case TxnRollbackAfterBeganAndAutocommit:
			err = th.taeTxn.Rollback()
		case TxnRollbackAfterAutocommitOnly:
			//if it is the txn started by BEGIN statement,
			//we do not commit it.
			switchTxnState = false
		}
	case TxnAutocommit:
		switch option {
		case TxnRollbackAfterBeganAndAutocommit, TxnRollbackAfterAutocommitOnly:
			err = th.taeTxn.Rollback()
		}
	case TxnInit, TxnEnd:
		err = errorTaeTxnHasNotBeenBegan
	case TxnErr:
		err = errorTaeTxnInIllegalState
	}

	if switchTxnState {
		if err == nil {
			th.txnState.switchToState(TxnEnd, err)
		} else {
			th.txnState.switchToState(TxnErr, err)
		}
	}

	return err
}

func (th *TxnHandler) Rollback() error {
	logutil.Infof("rollback ")
	var err error
	err = th.rollback(TxnRollbackAfterBeganAndAutocommit)
	return err
}

func (th *TxnHandler) RollbackAfterAutocommitOnly() error {
	logutil.Infof("rollback autocommit only")
	var err error
	err = th.rollback(TxnRollbackAfterAutocommitOnly)
	return err
}

//CleanTxn just cleans the txn when the errors happen during the txn operations.
// It does not commit any txn.
func (th *TxnHandler) CleanTxn() error {
	logutil.Infof("clean tae txn")
	switch th.txnState.getState() {
	case TxnInit, TxnEnd:
		th.taeTxn = InitTaeTxnDumpImpl()
		th.txnState.switchToState(TxnInit, nil)
	case TxnErr:
		logutil.Errorf("clean txn. Get error:%v txnError:%v", th.txnState.getError(), th.taeTxn.GetError())
		th.taeTxn = InitTaeTxnDumpImpl()
		th.txnState.switchToState(TxnInit, nil)
	}
	return nil
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
	_, err = tcc.txnHandler.GetStorage().Database(name, tcc.txnHandler.GetTxn().GetCtx())
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

	//open database
	db, err := tcc.txnHandler.GetStorage().Database(dbName, tcc.txnHandler.GetTxn().GetCtx())
	if err != nil {
		logutil.Errorf("get database %v error %v", dbName, err)
		return nil, err
	}

	tableNames := db.Relations(tcc.txnHandler.GetTxn().GetCtx())
	logutil.Infof("dbName %v tableNames %v", dbName, tableNames)

	//open table
	table, err := db.Relation(tableName, tcc.txnHandler.GetTxn().GetCtx())
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
	engineDefs := table.TableDefs(tcc.txnHandler.GetTxn().GetCtx())

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
		hideKey := table.GetHideKey(tcc.txnHandler.GetTxn().GetCtx())
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
	dbName, err := tcc.ensureDatabaseIsNotEmpty(dbName)
	if err != nil {
		return nil
	}
	relation, err := tcc.getRelation(dbName, tableName)
	if err != nil {
		return nil
	}

	priKeys := relation.GetPrimaryKeys(tcc.txnHandler.GetTxn().GetCtx())
	if len(priKeys) == 0 {
		return nil
	}

	var priDefs []*plan2.ColDef = nil
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
	dbName, err := tcc.ensureDatabaseIsNotEmpty(dbName)
	if err != nil {
		return nil
	}
	relation, err := tcc.getRelation(dbName, tableName)
	if err != nil {
		return nil
	}

	hideKey := relation.GetHideKey(tcc.txnHandler.GetTxn().GetCtx())
	if hideKey == nil {
		return nil
	}

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
