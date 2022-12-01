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

package txnbase

import (
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var (
	ErrTransferTransactionState = moerr.NewInternalErrorNoCtx("tae: transfer transaction state error")
)

const (
	TSUncommitted int32 = iota
	TSCommitting
	TSCommitted
	TSRollbacking
	TSRollbacked
)

type TxnState struct {
	state int32
}

func (ts *TxnState) ToCommitting() error {
	if atomic.CompareAndSwapInt32(&ts.state, TSUncommitted, TSCommitting) {
		return nil
	}
	return ErrTransferTransactionState
}

func (ts *TxnState) ToCommitted() error {
	if atomic.CompareAndSwapInt32(&ts.state, TSCommitting, TSCommitted) {
		return nil
	}
	return ErrTransferTransactionState
}

func (ts *TxnState) ToRollbacking() error {
	if atomic.CompareAndSwapInt32(&ts.state, TSUncommitted, TSRollbacking) {
		return nil
	}
	return ErrTransferTransactionState
}

func (ts *TxnState) ToRollbacked() error {
	if atomic.CompareAndSwapInt32(&ts.state, TSRollbacking, TSRollbacked) {
		return nil
	}
	return ErrTransferTransactionState
}

func (ts *TxnState) IsUncommitted() bool {
	return atomic.LoadInt32(&ts.state) == TSUncommitted
}

func (ts *TxnState) IsCommitted() bool {
	return atomic.LoadInt32(&ts.state) == TSCommitted
}

func (ts *TxnState) IsRollbacked() bool {
	return atomic.LoadInt32(&ts.state) == TSRollbacked
}
