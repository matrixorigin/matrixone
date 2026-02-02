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

package publication

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// SyncLevel represents the level of synchronization
const (
	SyncLevelAccount  = "account"
	SyncLevelDatabase = "database"
	SyncLevelTable    = "table"
)

// DDL operation types
const (
	DDLOperationCreate int8 = 1
	DDLOperationAlter  int8 = 2
	DDLOperationDrop   int8 = 3
)

// Subscription state types
const (
	SubscriptionStateRunning int8 = 0 // running
	SubscriptionStateError   int8 = 1 // error
	SubscriptionStatePause   int8 = 2 // pause
	SubscriptionStateDropped int8 = 3 // dropped
)

// SrcInfo contains source information for subscription
// It can be account/database/table level
type SrcInfo struct {
	SyncLevel string // 'account', 'database', or 'table'
	DBName    string // Required for database/table level
	TableName string // Required for table level
	AccountID uint32 // Account ID for downstream operations
}

// ObjectStats represents object statistics
type ObjectStats struct {
	Stats       string   // Object stats information
	CreateAt    types.TS // Creation timestamp
	DeleteAt    types.TS // Deletion timestamp (0 if not deleted)
	IsTombstone bool     // Whether this is a tombstone (deleted object)
}

// TableKey represents a key for TableIDs map
type TableKey struct {
	DBName    string // Database name
	TableName string // Table name
}

// IterationContext contains context information for an iteration
type IterationContext struct {
	// Task identification
	TaskID                  string
	SubscriptionName        string
	SubscriptionAccountName string // The subscription account name for FROM clause
	SrcInfo                 SrcInfo

	// Upstream connection
	LocalTxn         client.TxnOperator
	UpstreamExecutor SQLExecutor
	LocalExecutor    SQLExecutor

	// Execution state
	IterationLSN      uint64
	SubscriptionState int8 // subscription state: 0=running, 1=error, 2=pause, 3=dropped

	// Context information
	PrevSnapshotName    string
	PrevSnapshotTS      types.TS
	CurrentSnapshotName string
	CurrentSnapshotTS   types.TS
	// AObjectMap stores the mapping from upstream aobj to downstream object stats
	// This map is used to track appendable object transformations during CCPR sync
	AObjectMap         AObjectMap
	TableIDs           map[TableKey]uint64
	IndexTableMappings map[string]string // Maps upstream_index_table_name to downstream_index_table_name
	ErrorMetadata      *ErrorMetadata    // Error metadata parsed from error_message
}
