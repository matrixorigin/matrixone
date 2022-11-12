// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txn

import (
	"database/sql"

	"github.com/matrixorigin/matrixone/pkg/tests/service"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"go.uber.org/zap"
)

// Cluster txn testing cluster
type Cluster interface {
	// Start start the cluster, block until all service started and all DNShard created
	Start()
	// Stop stop the cluster
	Stop()
	// Env return the test cluster env
	Env() service.Cluster
	// NewClient create a test txn client
	NewClient() Client
	// GetLogger returns the logger
	GetLogger() *zap.Logger
}

// Client used to execute read and write.
type Client interface {
	// NewTxn create a txn to execute read and write command
	NewTxn(options ...client.TxnOption) (Txn, error)
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

// SQLBasedTxn support exec raw sql Txn
type SQLBasedTxn interface {
	Txn
	// ExecSQL exec sql, ddl/insert/update/delete
	ExecSQL(sql string) (sql.Result, error)
	// ExecSQLQuery exec query
	ExecSQLQuery(sql string) (*sql.Rows, error)
}
