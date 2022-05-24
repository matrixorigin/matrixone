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
)

const (
	TxnInit = iota
	TxnBegan
	TxnAutocommit
	TxnEnd
	TxnErr
	TxnNil
)

// TxnState represents for Transaction Machine
type TxnState struct {
	state     int
	fromState int
	err       error
}

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

func InitTaeTxnImpl() *TaeTxnDumpImpl {
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
		taeTxn:   InitTaeTxnImpl(),
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

	ep *tree.ExportParam

	closeRef      *CloseExportData
	txnHandler    *TxnHandler
	txnCompileCtx *TxnCompilerContext
	storage       engine.Engine
	sql           string
}

func NewSession(proto Protocol, pdHook *PDCallbackImpl, gm *guest.Mmu, mp *mempool.Mempool, PU *config.ParameterUnit) *Session {
	txnHandler := InitTxnHandler(config.StorageEngine)
	return &Session{
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
		txnCompileCtx: InitTxnCompilerContext(txnHandler, proto.GetDatabaseName()),
		storage:       config.StorageEngine,
	}
}

func (ses *Session) GetEpochgc() *PDCallbackImpl {
	return ses.pdHook
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

func (ses *Session) GetUserName() string {
	return ses.protocol.GetUserName()
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
	if st == TxnAutocommit || st == TxnBegan {
		return true
	}
	return false
}

func (th *TxnHandler) IsTaeEngine() bool {
	_, ok := th.storage.(moengine.TxnEngine)
	return ok
}

func (th *TxnHandler) StartByBegin() error {
	logutil.Infof("start txn by begin")
	var err error
	if taeEng, ok := th.storage.(moengine.TxnEngine); ok {
		switch th.txnState.getState() {
		case TxnInit, TxnEnd, TxnErr:
			//begin a transaction
			th.taeTxn, err = taeEng.StartTxn(nil)
		case TxnBegan:
			err = errorTaeTxnBeginInBegan
		case TxnAutocommit:
			err = errorTaeTxnBeginInAutocommit
		}
	} else {
		th.taeTxn = InitTaeTxnImpl()
	}

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
	if taeEng, ok := th.storage.(moengine.TxnEngine); ok {
		switch th.txnState.getState() {
		case TxnInit, TxnEnd, TxnErr:
			//begin a transaction
			th.taeTxn, err = taeEng.StartTxn(nil)
		case TxnAutocommit:
			err = errorTaeTxnAutocommitInAutocommit
		case TxnBegan:
			err = errorTaeTxnAutocommitInBegan
		}
	} else {
		th.taeTxn = InitTaeTxnImpl()
	}

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
	if taeEng, ok := th.storage.(moengine.TxnEngine); ok {
		switch th.txnState.getState() {
		case TxnInit, TxnEnd, TxnErr:
			//begin a transaction
			th.taeTxn, err = taeEng.StartTxn(nil)
		case TxnAutocommit:
			err = errorTaeTxnAutocommitInAutocommit
		case TxnBegan:
			err = errorTaeTxnAutocommitInBegan
		}
	} else {
		th.taeTxn = InitTaeTxnImpl()
	}

	if err == nil {
		th.txnState.switchToState(TxnAutocommit, err)
	} else {
		th.txnState.switchToState(TxnErr, err)
	}
	return true, err
}

func (th *TxnHandler) GetTxn() moengine.Txn {
	return th.taeTxn
}

// CommitAfterBegin commits the tae txn started by the BEGIN statement
func (th *TxnHandler) CommitAfterBegin() error {
	logutil.Infof("commit began")
	var err error
	switch th.getTxnState() {
	case TxnBegan:
		err = th.taeTxn.Commit()
	case TxnAutocommit:
		err = errorIsNotAutocommitTxn
	case TxnInit, TxnEnd, TxnErr:
		err = errorTaeTxnHasNotBeenBegan
	}

	if err == nil {
		th.txnState.switchToState(TxnEnd, err)
	} else {
		th.txnState.switchToState(TxnErr, err)
	}
	return err
}

// CommitAfterAutocommit commits the tae txn started by autocommit
func (th *TxnHandler) CommitAfterAutocommit() error {
	logutil.Infof("commit autocommit")
	var err error
	switch th.getTxnState() {
	case TxnAutocommit:
		err = th.taeTxn.Commit()
	case TxnBegan:
		err = errorIsNotBeginCommitTxn
	case TxnInit, TxnEnd, TxnErr:
		err = errorTaeTxnHasNotBeenBegan
	}

	if err == nil {
		th.txnState.switchToState(TxnEnd, err)
	} else {
		th.txnState.switchToState(TxnErr, err)
	}
	return err
}

// CommitAfterAutocommitOnly commits the tae txn started by autocommit
// Do not check TxnBegan
func (th *TxnHandler) CommitAfterAutocommitOnly() error {
	logutil.Infof("commit autocommit only")
	var err error
	switch th.getTxnState() {
	case TxnAutocommit:
		err = th.taeTxn.Commit()
	case TxnInit, TxnEnd, TxnErr:
		err = errorTaeTxnHasNotBeenBegan
	}

	if th.getTxnState() != TxnBegan {
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
	switch th.getTxnState() {
	case TxnBegan, TxnAutocommit:
		err = th.taeTxn.Rollback()
	case TxnInit, TxnEnd, TxnErr:
		return errorTaeTxnHasNotBeenBegan
	}

	if err == nil {
		th.txnState.switchToState(TxnEnd, err)
	} else {
		th.txnState.switchToState(TxnErr, err)
	}
	return err
}

func (th *TxnHandler) RollbackAfterAutocommitOnly() error {
	logutil.Infof("rollback autocommit only")
	var err error
	switch th.getTxnState() {
	case TxnAutocommit:
		err = th.taeTxn.Rollback()
	case TxnInit, TxnEnd, TxnErr:
		return errorTaeTxnHasNotBeenBegan
	}

	if th.txnState.getState() != TxnBegan {
		if err == nil {
			th.txnState.switchToState(TxnEnd, err)
		} else {
			th.txnState.switchToState(TxnErr, err)
		}
	}

	return err
}

//ClearTxn commits the tae txn when the errors happen during the txn
func (th *TxnHandler) ClearTxn() error {
	logutil.Infof("clear tae txn")
	var err error
	switch th.txnState.getState() {
	case TxnInit, TxnEnd, TxnErr:
		th.taeTxn = InitTaeTxnImpl()
	case TxnBegan:
		logutil.Infof("can not commit a began txn without obvious COMMIT or ROLLBACK")
	case TxnAutocommit:
		err = th.CommitAfterAutocommit()
		th.taeTxn = InitTaeTxnImpl()
		th.txnState.switchToState(TxnInit, err)
	}
	return err
}

var _ plan2.CompilerContext = &TxnCompilerContext{}

type TxnCompilerContext struct {
	dbName     string
	txnHandler *TxnHandler
}

func InitTxnCompilerContext(txn *TxnHandler, db string) *TxnCompilerContext {
	if len(db) == 0 {
		db = "mo_catalog"
	}
	return &TxnCompilerContext{txnHandler: txn, dbName: db}
}

func (tcc *TxnCompilerContext) SetDatabase(db string) {
	tcc.dbName = db
}

func (tcc *TxnCompilerContext) DefaultDatabase() string {
	return tcc.dbName
}

func (tcc *TxnCompilerContext) DatabaseExists(name string) bool {
	newTxn, err := tcc.txnHandler.StartByAutocommitIfNeeded()
	if err != nil {
		logutil.Errorf("error %v", err)
		return false
	}

	//open database
	_, err = tcc.txnHandler.GetStorage().Database(name, tcc.txnHandler.GetTxn().GetCtx())
	if err != nil {
		logutil.Errorf("error %v", err)
		err2 := tcc.txnHandler.RollbackAfterAutocommitOnly()
		if err2 != nil {
			return false
		}
		return false
	}

	if newTxn {
		err2 := tcc.txnHandler.CommitAfterAutocommitOnly()
		if err2 != nil {
			logutil.Errorf("error %v", err)
			return false
		}
	}
	return true
}

func (tcc *TxnCompilerContext) Resolve(dbName string, tableName string) (*plan2.ObjectRef, *plan2.TableDef) {
	newTxn, err := tcc.txnHandler.StartByAutocommitIfNeeded()
	if err != nil {
		logutil.Errorf("error %v", err)
		return nil, nil
	}

	if len(dbName) == 0 {
		dbName = tcc.DefaultDatabase()
	}

	//open database
	db, err := tcc.txnHandler.GetStorage().Database(dbName, tcc.txnHandler.GetTxn().GetCtx())
	if err != nil {
		logutil.Errorf("error %v", err)
		err2 := tcc.txnHandler.RollbackAfterAutocommitOnly()
		if err2 != nil {
			return nil, nil
		}
		return nil, nil
	}

	tableNames := db.Relations(tcc.txnHandler.GetTxn().GetCtx())
	logutil.Infof("tableNames %v", tableNames)

	//open table
	table, err := db.Relation(tableName, tcc.txnHandler.GetTxn().GetCtx())
	if err != nil {
		logutil.Errorf("error %v", err)
		err2 := tcc.txnHandler.RollbackAfterAutocommitOnly()
		if err2 != nil {
			return nil, nil
		}
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
				},
				Primary: attr.Attr.Primary,
			})
		}
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

	if newTxn {
		err2 := tcc.txnHandler.CommitAfterAutocommitOnly()
		if err2 != nil {
			logutil.Errorf("error %v", err)
			return nil, nil
		}
	}
	return obj, tableDef
}

func (tcc *TxnCompilerContext) Cost(obj *plan2.ObjectRef, e *plan2.Expr) *plan2.Cost {
	return &plan2.Cost{}
}
