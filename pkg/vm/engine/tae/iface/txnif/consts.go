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

package txnif

const (
	UncommitTS = ^uint64(0)
)

const (
	TxnStateActive int32 = iota
	TxnStateCommitting
	TxnStateRollbacking
	TxnStateCommitted
	TxnStateRollbacked
)

func TxnStrState(state int32) string {
	switch state {
	case TxnStateActive:
		return "Active"
	case TxnStateCommitting:
		return "Committing"
	case TxnStateRollbacking:
		return "Rollbacking"
	case TxnStateCommitted:
		return "Committed"
	case TxnStateRollbacked:
		return "Rollbacked"
	}
	panic("state not support")
}
