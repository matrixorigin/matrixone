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

package v2

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
)

const (
	PrefixGCMeta   = "gc"
	PrefixSnapMeta = "snap"
	PrefixAcctMeta = "acct"
	GCMetaDir      = "gc/"
	CKPMetaDir     = "ckp/"
)

type BatchType int8
type CleanerState int8

const CurrentVersion = uint16(3)

const (
	ObjectList BatchType = iota
	TombstoneList
)

const (
	CreateBlock BatchType = iota
	DeleteBlock
	DropTable
	DropDB
	DeleteFile
	Tombstone
)

const (
	Idle CleanerState = iota
	Running
)

const (
	GCAttrObjectName = "name"
	GCAttrBlockId    = "block_id"
	GCAttrTableId    = "table_id"
	GCAttrDBId       = "db_id"
	GCAttrCommitTS   = "commit_ts"
	GCCreateTS       = "create_time"
	GCDeleteTS       = "delete_time"
	GCAttrTombstone  = "tombstone"
	GCAttrVersion    = "version"
)

const (
	AddChecker    = "add_checker"
	RemoveChecker = "remove_checker"
)

const (
	CheckerKeyTTL   = "ttl"
	CheckerKeyMinTS = "min_ts"
)

var (
	BlockSchemaAttr = []string{
		GCAttrObjectName,
		GCCreateTS,
		GCDeleteTS,
		GCAttrCommitTS,
		GCAttrTableId,
	}
	BlockSchemaTypes = []types.Type{
		types.New(types.T_varchar, 5000, 0),
		types.New(types.T_TS, types.MaxVarcharLen, 0),
		types.New(types.T_TS, types.MaxVarcharLen, 0),
		types.New(types.T_TS, types.MaxVarcharLen, 0),
		types.New(types.T_uint64, 0, 0),
	}

	BlockSchemaAttrV1 = []string{
		GCAttrBlockId,
		GCAttrTableId,
		GCAttrDBId,
		GCAttrObjectName,
	}
	BlockSchemaTypesV1 = []types.Type{
		types.New(types.T_Blockid, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_varchar, 5000, 0),
	}

	TombstoneSchemaAttr = []string{
		GCAttrTombstone,
		GCAttrObjectName,
		GCAttrCommitTS,
	}

	TombstoneSchemaTypes = []types.Type{
		types.New(types.T_varchar, 5000, 0),
		types.New(types.T_varchar, 5000, 0),
		types.New(types.T_TS, types.MaxVarcharLen, 0),
	}

	VersionsSchemaAttr = []string{
		GCAttrVersion,
	}

	VersionsSchemaTypes = []types.Type{
		types.New(types.T_uint16, 0, 0),
	}

	DropTableSchemaAttr = []string{
		GCAttrTableId,
		GCAttrDBId,
	}
	DropTableSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
	}

	DropDBSchemaAtt = []string{
		GCAttrDBId,
	}
	DropDBSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
	}

	DeleteFileSchemaAtt = []string{
		GCAttrObjectName,
	}
	DeleteFileSchemaTypes = []types.Type{
		types.New(types.T_varchar, 5000, 0),
	}
)

type Cleaner interface {
	Replay() error
	Process()
	TryGC() error
	AddChecker(checker func(item any) bool, key string) int
	RemoveChecker(key string) error
	GetMaxConsumed() *checkpoint.CheckpointEntry
	Stop()
	// for test
	SetMinMergeCountForTest(count int)
	GetMinMerged() *checkpoint.CheckpointEntry
	CheckGC() error
	GetInputs() *GCTable
	SetTid(tid uint64)
	EnableGCForTest()
	DisableGCForTest()
	IsEnableGC() bool
	SetCheckGC(enable bool)
	GetMPool() *mpool.MPool
	GetSnapshots() (map[uint32]containers.Vector, error)
}
