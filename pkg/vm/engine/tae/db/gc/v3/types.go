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

package gc

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/engine_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
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
	GetScanWaterMark() *checkpoint.CheckpointEntry
	GetCheckpointGCWaterMark() *types.TS
	GetScannedWindow() *GCWindow
	Stop()
	GetMinMerged() *checkpoint.CheckpointEntry
	DoCheck() error
	GetPITRs() (*logtail.PitrInfo, error)
	SetTid(tid uint64)
	EnableGC()
	DisableGC()
	GCEnabled() bool
	GetMPool() *mpool.MPool
	GetSnapshots() (map[uint32]containers.Vector, error)
}

var ObjectTableAttrs []string
var ObjectTableTypes []types.Type
var ObjectTableSeqnums []uint16
var ObjectTableMetaAttrs []string
var ObjectTableMetaTypes []types.Type

var FSinkerFactory engine_util.FileSinkerFactory

const ObjectTablePrimaryKeyIdx = 0
const ObjectTableVersion = 0
const (
	DefaultInMemoryStagedSize = mpool.MB * 32
)

type GCMetaFile struct {
	name       string
	start, end types.TS
	ext        string
}

func (f *GCMetaFile) FullName(dir string) string {
	return dir + f.name
}

func (f *GCMetaFile) Start() *types.TS {
	return &f.start
}
func (f *GCMetaFile) End() *types.TS {
	return &f.end
}
func (f *GCMetaFile) Ext() string {
	return f.ext
}
func (f *GCMetaFile) Name() string {
	return f.name
}
func (f *GCMetaFile) EqualRange(start, end *types.TS) bool {
	return f.start == *start && f.end == *end
}

func init() {
	ObjectTableAttrs = []string{
		"stats",
		"created_ts",
		"deleted_ts",
		"db_id",
		"table_id",
	}
	ObjectTableTypes = []types.Type{
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		objectio.TSType,
		objectio.TSType,
		objectio.Uint64Type,
		objectio.Uint64Type,
	}
	ObjectTableSeqnums = []uint16{0, 1, 2, 3, 4}

	ObjectTableMetaAttrs = []string{
		"stats",
	}

	ObjectTableMetaTypes = []types.Type{
		objectio.VarcharType,
	}

	FSinkerFactory = engine_util.NewFSinkerImplFactory(
		ObjectTableSeqnums,
		ObjectTablePrimaryKeyIdx,
		true,
		false,
		ObjectTableVersion,
	)
}

func NewObjectTableBatch() *batch.Batch {
	ret := batch.New(false, ObjectTableAttrs)
	ret.SetVector(0, vector.NewVec(ObjectTableTypes[0]))
	ret.SetVector(1, vector.NewVec(ObjectTableTypes[1]))
	ret.SetVector(2, vector.NewVec(ObjectTableTypes[2]))
	ret.SetVector(3, vector.NewVec(ObjectTableTypes[3]))
	ret.SetVector(4, vector.NewVec(ObjectTableTypes[4]))
	return ret
}

func addObjectToBatch(
	bat *batch.Batch,
	stats *objectio.ObjectStats,
	object *ObjectEntry,
	mPool *mpool.MPool,
) error {
	err := vector.AppendBytes(bat.Vecs[0], stats[:], false, mPool)
	if err != nil {
		return err
	}
	err = vector.AppendFixed[types.TS](bat.Vecs[1], object.createTS, false, mPool)
	if err != nil {
		return err
	}
	err = vector.AppendFixed[types.TS](bat.Vecs[2], object.dropTS, false, mPool)
	if err != nil {
		return err
	}
	err = vector.AppendFixed[uint64](bat.Vecs[3], object.db, false, mPool)
	if err != nil {
		return err
	}
	err = vector.AppendFixed[uint64](bat.Vecs[4], object.table, false, mPool)
	if err != nil {
		return err
	}
	return nil
}
