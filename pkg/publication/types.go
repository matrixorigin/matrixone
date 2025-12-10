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
)

// SyncLevel represents the level of synchronization
const (
	SyncLevelAccount  = "account"
	SyncLevelDatabase = "database"
	SyncLevelTable    = "table"
)

// SrcInfo contains source information for subscription
// It can be account/database/table level
type SrcInfo struct {
	SyncLevel string // 'account', 'database', or 'table'
	DBName    string // Required for database/table level
	TableName string // Required for table level
	AccountName string // Optional, for account level
}

// ObjectStats represents object statistics
type ObjectStats struct {
	Stats      string      // Object stats information
	CreateAt   types.TS    // Creation timestamp
	DeleteAt   types.TS    // Deletion timestamp (0 if not deleted)
	IsTombstone bool       // Whether this is a tombstone (deleted object)
}

// IterationContext contains context information for an iteration
type IterationContext struct {
	// Task identification
	TaskID           uint64
	SubscriptionName string
	SrcInfo          SrcInfo

	// Upstream connection
	UpstreamExecutor SQLExecutor

	// Sync configuration
	SyncConfig map[string]any

	// Execution state
	IterationLSN uint64
	CNUUID       string

	// Context information
	PrevSnapshotName    string
	PrevSnapshotTS      types.TS
	CurrentSnapshotName string
	CurrentSnapshotTS   types.TS
	ActiveAObj          map[string]objectio.ObjectStats // Map from upstream aobj UUID to new objectio.ObjectStats
	TableIDs            map[string]uint64
}
