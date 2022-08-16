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
	"math/rand"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var operators sync.Map

func OperatorToSnapshot(operator client.TxnOperator) engine.Snapshot {
	bs := make([]byte, 16)
	_, err := rand.Read(bs)
	if err != nil {
		panic(err)
	}
	operators.Store(*(*[16]byte)(bs), operator)
	return bs
}

func ToOperator(operator client.TxnOperator) client.TxnOperator {
	if snapshot, ok := operator.(engine.Snapshot); ok {
		key := *(*[16]byte)(snapshot)
		v, ok := operators.Load(key)
		if !ok {
			panic("not found")
		}
		return v.(client.TxnOperator)
	}
	return operator
}
