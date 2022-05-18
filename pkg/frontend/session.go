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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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

	closeRef *CloseExportData

	//tae txn
	//TODO: add aoe dump impl of Txn interface for unifying the logic of txn
	taeTxn   moengine.Txn
	txnState *TxnState
}

func NewSession(proto Protocol, pdHook *PDCallbackImpl, gm *guest.Mmu, mp *mempool.Mempool, PU *config.ParameterUnit) *Session {
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
		taeTxn:   nil,
		txnState: InitTxnState(),
	}
}

func (ses *Session) GetEpochgc() *PDCallbackImpl {
	return ses.pdHook
}

func (ses *Session) getTxnState() int {
	return ses.txnState.getState()
}

func (ses *Session) isTxnState(s int) bool {
	return ses.txnState.isState(s)
}

func (ses *Session) switchToTxnState(s int, err error) {
	ses.txnState.switchToState(s, err)
}

func (ses *Session) getFromTxnState() int {
	return ses.txnState.getFromState()
}

func (ses *Session) getTxnStateError() error {
	return ses.txnState.getError()
}

func (ses *Session) getTxnStateString() string {
	return ses.txnState.String()
}

// IsInTaeTxn checks the session executes a txn
func (ses *Session) IsInTaeTxn() bool {
	st := ses.getTxnState()
	if st == TxnAutocommit || st == TxnBegan {
		return ses.GetTaeTxn() != nil
	}
	return false
}

func (ses *Session) BeginTaeTxn() error {
	logutil.Infof("begin begin")
	var err error
	if taeEng, ok := config.StorageEngine.(moengine.TxnEngine); ok {
		switch ses.txnState.getState() {
		case TxnInit, TxnEnd, TxnErr:
			//begin a transaction
			ses.taeTxn, err = taeEng.StartTxn(nil)
		case TxnBegan:
			err = errorTaeTxnBeginInBegan
		case TxnAutocommit:
			err = errorTaeTxnBeginInAutocommit
		}
	} else {
		ses.taeTxn = nil
	}

	if err == nil {
		ses.txnState.switchToState(TxnBegan, err)
	} else {
		ses.txnState.switchToState(TxnErr, err)
	}
	return err
}

func (ses *Session) BeginAutocommitTaeTxn() error {
	logutil.Infof("begin autocommit")
	var err error
	if taeEng, ok := config.StorageEngine.(moengine.TxnEngine); ok {
		switch ses.txnState.getState() {
		case TxnInit, TxnEnd, TxnErr:
			//begin a transaction
			ses.taeTxn, err = taeEng.StartTxn(nil)
		case TxnAutocommit:
			err = errorTaeTxnAutocommitInAutocommit
		case TxnBegan:
			err = errorTaeTxnAutocommitInBegan
		}
	} else {
		ses.taeTxn = nil
	}

	if err == nil {
		ses.txnState.switchToState(TxnAutocommit, err)
	} else {
		ses.txnState.switchToState(TxnErr, err)
	}
	return err
}

func (ses *Session) GetTaeTxn() moengine.Txn {
	return ses.taeTxn
}

// CommitTaeTxnBegan commits the tae txn started by the BEGIN statement
func (ses *Session) CommitTaeTxnBegan() error {
	logutil.Infof("commit began")
	var err error
	if ses.taeTxn != nil {
		switch ses.getTxnState() {
		case TxnBegan:
			err = ses.taeTxn.Commit()
		case TxnAutocommit:
			err = errorIsNotAutocommitTxn
		case TxnInit, TxnEnd, TxnErr:
			err = errorTaeTxnHasNotBeenBegan
		}
	}
	if err == nil {
		ses.txnState.switchToState(TxnEnd, err)
	} else {
		ses.txnState.switchToState(TxnErr, err)
	}
	return err
}

// CommitTaeTxnAutocommit commits the tae txn started by autocommit
func (ses *Session) CommitTaeTxnAutocommit() error {
	logutil.Infof("commit autocommit")
	var err error
	if ses.taeTxn != nil {
		switch ses.getTxnState() {
		case TxnAutocommit:
			err = ses.taeTxn.Commit()
		case TxnBegan:
			err = errorIsNotBeginCommitTxn
		case TxnInit, TxnEnd, TxnErr:
			err = errorTaeTxnHasNotBeenBegan
		}
	}
	if err == nil {
		ses.txnState.switchToState(TxnEnd, err)
	} else {
		ses.txnState.switchToState(TxnErr, err)
	}
	return err
}

// CommitTaeTxnAutocommitOnly commits the tae txn started by autocommit
// Do not check TxnBegan
func (ses *Session) CommitTaeTxnAutocommitOnly() error {
	logutil.Infof("commit autocommit only")
	var err error
	if ses.taeTxn != nil {
		switch ses.getTxnState() {
		case TxnAutocommit:
			err = ses.taeTxn.Commit()
		case TxnInit, TxnEnd, TxnErr:
			err = errorTaeTxnHasNotBeenBegan
		}
	}
	if ses.getTxnState() != TxnBegan {
		if err == nil {
			ses.txnState.switchToState(TxnEnd, err)
		} else {
			ses.txnState.switchToState(TxnErr, err)
		}
	}

	return err
}

func (ses *Session) RollbackTaeTxn() error {
	logutil.Infof("rollback ")
	var err error
	if ses.taeTxn != nil {
		switch ses.getTxnState() {
		case TxnBegan, TxnAutocommit:
			err = ses.taeTxn.Rollback()
		case TxnInit, TxnEnd, TxnErr:
			return errorTaeTxnHasNotBeenBegan
		}
	}
	if err == nil {
		ses.txnState.switchToState(TxnEnd, err)
	} else {
		ses.txnState.switchToState(TxnErr, err)
	}
	return err
}

//ClearTaeTxn commits the tae txn when the errors happen during the txn
func (ses *Session) ClearTaeTxn() error {
	logutil.Infof("clear tae txn")
	var err error
	switch ses.txnState.getState() {
	case TxnInit, TxnEnd, TxnErr:
		ses.taeTxn = nil
	case TxnBegan:
		logutil.Infof("can not commit a began txn without obvious COMMIT or ROLLBACK")
	case TxnAutocommit:
		err = ses.CommitTaeTxnAutocommit()
		ses.taeTxn = nil
		ses.txnState.switchToState(TxnInit, err)
	}
	return err
}
