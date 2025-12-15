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
	"github.com/matrixorigin/matrixone/pkg/objectio"
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
	DDLOperationCreate = "create"
	DDLOperationAlter  = "alter"
	DDLOperationDrop   = "drop"
)

// SrcInfo contains source information for subscription
// It can be account/database/table level
type SrcInfo struct {
	SyncLevel   string // 'account', 'database', or 'table'
	DBName      string // Required for database/table level
	TableName   string // Required for table level
	AccountName string // Optional, for account level
}

// ObjectStats represents object statistics
type ObjectStats struct {
	Stats       string   // Object stats information
	CreateAt    types.TS // Creation timestamp
	DeleteAt    types.TS // Deletion timestamp (0 if not deleted)
	IsTombstone bool     // Whether this is a tombstone (deleted object)
}

// AObjMapping contains both current and previous object stats for an upstream aobj
type AObjMapping struct {
	Current  objectio.ObjectStats // Newly written object stats in current iteration
	Previous objectio.ObjectStats // Object stats written in previous iteration (zero value if not exists)
}

// TableKey represents a key for TableIDs map
type TableKey struct {
	DBName    string // Database name
	TableName string // Table name
}

// IndexTableMapping represents the mapping between downstream and upstream index table names
type IndexTableMapping struct {
	IndexName                string // Index name
	AlgoTableType            string // Algorithm table type (e.g., "metadata", "centroids", "entries", "hnsw_meta", "hnsw_index")
	DownstreamIndexTableName string // Index table name in downstream (local)
	UpstreamIndexTableName   string // Index table name in upstream
}

// IndexKey represents a key for IndexTableMappings map
// Format: table_id + ":" + index_name + ":" + algo_table_type
type IndexKey struct {
	TableID       uint64
	IndexName     string
	AlgoTableType string
}

// IterationContext contains context information for an iteration
type IterationContext struct {
	// Task identification
	TaskID           uint64
	SubscriptionName string
	SrcInfo          SrcInfo

	// Upstream connection
	LocalTxn         client.TxnOperator
	UpstreamExecutor SQLExecutor
	LocalExecutor    SQLExecutor

	// Execution state
	IterationLSN uint64

	// Context information
	PrevSnapshotName    string
	PrevSnapshotTS      types.TS
	CurrentSnapshotName string
	CurrentSnapshotTS   types.TS
	// ActiveAObj maps upstream aobj UUID to both current and previous object stats
	// Current stats: the newly written object stats in this iteration
	// Previous stats: the object stats written in the previous iteration (if exists)
	ActiveAObj         map[string]AObjMapping
	TableIDs           map[TableKey]uint64
	IndexTableMappings map[IndexKey]IndexTableMapping // Maps index name and algo_table_type to index table mapping
}
