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

package tables

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type mockTxn struct {
	*txnbase.TxnCtx
}

func newMockTxn() *mockTxn {
	return &mockTxn{
		TxnCtx: txnbase.NewTxnCtx(nil, common.NextGlobalSeqNum(),
			common.NextGlobalSeqNum(), nil),
	}
}

func (txn *mockTxn) GetError() error          { return nil }
func (txn *mockTxn) GetStore() txnif.TxnStore { return nil }
func (txn *mockTxn) GetTxnState(bool) int32   { return 0 }
func (txn *mockTxn) IsTerminated(bool) bool   { return false }
