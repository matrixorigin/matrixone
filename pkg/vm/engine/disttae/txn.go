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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

// detecting whether a transaction is a read-only transaction
func (txn *Transaction) ReadOnly() bool {
	return txn.readOnly
}

// use for solving halloween problem
func (txn *Transaction) SetStatementId(id [2]uint64) {
	txn.statementId = id
}

// Write used to write data to the transaction buffer
// insert/delete/update all use this api
func (txn *Transaction) WriteBatch(bat *batch.Batch) error {
	txn.readOnly = true
	return nil
}

// WriteFile used to add a s3 file information to the transaction buffer
// insert/delete/update all use this api
func (txn *Transaction) WriteFile(file string) error {
	txn.readOnly = true
	return nil
}
