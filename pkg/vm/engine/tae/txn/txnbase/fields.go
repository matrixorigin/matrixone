// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnbase

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TxnField(txn txnif.AsyncTxn) zap.Field {
	return zap.Object("txn", txn.(zapcore.ObjectMarshaler))
}

func TxnMgrField(mgr *TxnManager) zap.Field {
	return zap.Object("txnmgr", mgr)
}

func (mgr *TxnManager) MarshalLogObject(enc zapcore.ObjectEncoder) (err error) {
	enc.AddUint64("currId", mgr.IdAlloc.Get())
	enc.AddUint64("currTs", mgr.TsAlloc.Get())
	enc.AddUint64("safeTs", mgr.StatSafeTS())
	return
}

func (txn *Txn) MarshalLogObject(enc zapcore.ObjectEncoder) (err error) {
	txn.RLock()
	defer txn.RUnlock()
	enc.AddUint64("id", txn.ID)
	enc.AddUint64("startTs", txn.StartTS)
	enc.AddString("state", txnif.TxnStrState(txn.State))
	if !txn.IsActiveLocked() {
		enc.AddUint64("commitTs", txn.CommitTS)
	}
	return
}
