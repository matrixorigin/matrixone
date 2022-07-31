// Copyright 2022 Matrix Origin
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

package txnengine

import (
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

type Error struct {
	txnError *txn.TxnError
}

var _ error = Error{}

func (e Error) Error() string {
	if e.txnError != nil {
		return e.txnError.DebugString()
	}
	panic("impossible")
}

func errorFromTxnResponses(resps []txn.TxnResponse) error {
	for _, resp := range resps {
		if resp.TxnError != nil {
			return Error{
				txnError: resp.TxnError,
			}
		}
	}
	return nil
}
