// Copyright 2026 Matrix Origin
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

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// A query can scan several historical versions. The process cache represents
// only its first snapshot; a different scan needs its own immutable operator,
// not a mutation of the cached operator already held by another relation.
func txnOperatorForSnapshot(proc *process.Process, ts timestamp.Timestamp) client.TxnOperator {
	cached := proc.GetCloneTxnOperator()
	if cached != nil && cached.Txn().SnapshotTS.Equal(ts) {
		return cached
	}
	op := proc.GetTxnOperator().CloneSnapshotOp(ts)
	if cached == nil {
		proc.SetCloneTxnOperator(op)
	}
	return op
}
