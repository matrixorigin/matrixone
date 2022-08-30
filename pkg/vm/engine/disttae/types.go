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

package disttae

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

type Engine struct {
	sync.Mutex
	getClusterDetails GetClusterDetailsFunc
	txns              map[string]*Transaction
}

// Transaction represents a transaction
type Transaction struct {
	// readOnly default value is true, once a write happen, then set to false
	readOnly    bool
	statementId [2]uint64
	meta        txn.TxnMeta
}
