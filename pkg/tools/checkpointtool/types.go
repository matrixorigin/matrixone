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

package checkpointtool

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
)

// EntryInfo is a simplified view of CheckpointEntry for display
type EntryInfo struct {
	Index            int
	Start            types.TS
	End              types.TS
	Type             checkpoint.EntryType
	State            checkpoint.State
	Version          uint32
	Location         string
	TNLocation       string
	TableIDLocations []string
	CKPLSN           uint64
	TruncLSN         uint64
}

// CheckpointInfo contains checkpoint metadata summary
type CheckpointInfo struct {
	Dir           string
	TotalEntries  int
	GlobalCount   int
	IncrCount     int
	CompactCount  int
	LatestTS      types.TS
	EarliestTS    types.TS
	TotalAccounts int
	TotalTables   int
}

// AccountInfo contains account summary within checkpoint
type AccountInfo struct {
	AccountID   uint32
	TableCount  int
	ObjectCount int
	DataRanges  int
	TombRanges  int
}

// TableInfo contains table data within checkpoint
type TableInfo struct {
	TableID    uint64
	AccountID  uint32
	DatabaseID uint64
	DataRanges []ckputil.TableRange
	TombRanges []ckputil.TableRange
}

// ObjectEntryInfo contains detailed object entry information with timestamps
type ObjectEntryInfo struct {
	Range      ckputil.TableRange
	CreateTime types.TS
	DeleteTime types.TS
}

// ComposedView represents logical checkpoint view at a timestamp
type ComposedView struct {
	Timestamp    types.TS
	BaseEntry    *EntryInfo
	Incrementals []*EntryInfo
	Tables       map[uint64]*TableInfo
}
