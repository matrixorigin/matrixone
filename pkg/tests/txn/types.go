// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" 	,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txn

import (
	"github.com/matrixorigin/matrixone/pkg/tests/service"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// Cluster txn testing cluster
type Cluster interface {
	Start()
	Stop()
	Restart()
	Env() service.Cluster
	NewClient() Client
}

// Client used to execute read and write.
type Client interface {
	// Read read value of the specified key, execute in an auto-commit transaction
	Read(key string) (string, error)
	// Write write key-value pair to the cluster, execute in an auto-commit transaction
	Write(key, value string) error
	// ExecTxn exec multi read and write operations in a transaction. The handle func can
	// commit or abort the transaction.
	ExecTxn(func(txn Txn) error, ...client.TxnOption) error
}

// Txn txn operation handler
type Txn interface {
	// Commit commit the txn
	Commit() error
	// Rollback rollback the txn
	Rollback() error
	// Read read by key
	Read(key string) (string, error)
	// Write write key
	Write(key, value string) error
}
