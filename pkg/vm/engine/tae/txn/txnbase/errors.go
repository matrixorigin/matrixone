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

import "errors"

var (
	ErrTxnAlreadyCommitted  = errors.New("tae: txn already committed")
	ErrTxnNotCommitting     = errors.New("tae: txn not committing")
	ErrTxnNotRollbacking    = errors.New("tae: txn not rollbacking")
	ErrTxnNotActive         = errors.New("tae: txn not active")
	ErrTxnCannotRollback    = errors.New("tae: txn cannot txn rollback")
	ErrTxnDifferentDatabase = errors.New("tae: different database used")

	ErrNotFound   = errors.New("tae: not found")
	ErrDuplicated = errors.New("tae: duplicated ")

	ErrDDLDropCreated = errors.New("tae: DDL cannot drop created in a txn")
)
