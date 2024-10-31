// Copyright 2021 Matrix Origin
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

package logtail

import (
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

const (
	SnapshotAttr_TID                              = catalog.SnapshotAttr_TID
	SnapshotAttr_DBID                             = catalog.SnapshotAttr_DBID
	ObjectAttr_ObjectStats                        = catalog.ObjectAttr_ObjectStats
	EntryNode_CreateAt                            = catalog.EntryNode_CreateAt
	EntryNode_DeleteAt                            = catalog.EntryNode_DeleteAt
	SnapshotMetaAttr_BlockInsertBatchLocation     = "block_insert_batch_location"
	SnapshotMetaAttr_BlockDeleteBatchLocation     = "block_delete_batch_location"
	SnapshotMetaAttr_DataObjectBatchLocation      = "data_object_batch_location"
	SnapshotMetaAttr_TombstoneObjectBatchLocation = "tombstone_object_batch_location"
	CheckpointMetaAttr_BlockLocation              = "checkpoint_meta_block_location"
	CheckpointMetaAttr_SchemaType                 = "checkpoint_meta_schema_type"

	CheckpointMetaAttr_StorageUsageInsLocation = "checkpoint_meta_storage_usage_ins_location"
	CheckpointMetaAttr_StorageUsageDelLocation = "checkpoint_meta_storage_usage_del_location"

	// supporting `show accounts` in checkpoint
	CheckpointMetaAttr_ObjectSize = "checkpoint_meta_object_size"
	CheckpointMetaAttr_ObjectID   = "checkpoint_meta_object_id"
)

var (
	BaseAttr = []string{
		catalog.PhyAddrColumnName,
		objectio.TombstoneAttr_CommitTs_Attr,
	}

	BaseTypes = []types.Type{
		types.T_Rowid.ToType(),
		types.T_TS.ToType(),
	}

	MetaSchema         *catalog.Schema
	TNMetaSchema       *catalog.Schema
	ObjectInfoSchema   *catalog.Schema
	StorageUsageSchema *catalog.Schema

	MetaSchemaAttr = []string{
		SnapshotAttr_TID,
		SnapshotMetaAttr_BlockInsertBatchLocation,
		SnapshotMetaAttr_BlockDeleteBatchLocation,
		SnapshotMetaAttr_DataObjectBatchLocation,
		SnapshotMetaAttr_TombstoneObjectBatchLocation,
		CheckpointMetaAttr_StorageUsageInsLocation,
		CheckpointMetaAttr_StorageUsageDelLocation,
	}

	MetaShcemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
	}
	TNMetaSchemaAttr = []string{
		CheckpointMetaAttr_BlockLocation,
		CheckpointMetaAttr_SchemaType,
	}
	TNMetaShcemaTypes = []types.Type{
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_uint16, 0, 0),
	}

	ObjectInfoAttr = []string{
		ObjectAttr_ObjectStats,
		SnapshotAttr_DBID,
		SnapshotAttr_TID,
		EntryNode_CreateAt,
		EntryNode_DeleteAt,
		txnbase.SnapshotAttr_StartTS,
		txnbase.SnapshotAttr_PrepareTS,
		txnbase.SnapshotAttr_CommitTS,
	}
	ObjectInfoTypes = []types.Type{
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
	}

	StorageUsageSchemaAttrs = []string{
		pkgcatalog.SystemColAttr_AccID,
		SnapshotAttr_DBID,
		SnapshotAttr_TID,
		CheckpointMetaAttr_ObjectID,
		CheckpointMetaAttr_ObjectSize,
	}

	StorageUsageSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uuid, 0, 0),
		types.New(types.T_uint64, 0, 0),
	}
)

var (
	skippedDB = map[uint64]bool{
		pkgcatalog.MO_CATALOG_ID: true,
	}

	skippedTbl = map[uint64]bool{
		pkgcatalog.MO_DATABASE_ID: true,
		pkgcatalog.MO_TABLES_ID:   true,
		pkgcatalog.MO_COLUMNS_ID:  true,
	}
)

func shouldIgnoreDBInLogtail(id uint64) bool {
	return skippedDB[id]
}

func shouldIgnoreTblInLogtail(id uint64) bool {
	return skippedTbl[id]
}

func init() {

	MetaSchema = catalog.NewEmptySchema("meta")
	for i, colname := range MetaSchemaAttr {
		if i == 0 {
			if err := MetaSchema.AppendPKCol(colname, MetaShcemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := MetaSchema.AppendCol(colname, MetaShcemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	TNMetaSchema = catalog.NewEmptySchema("meta")
	for i, colname := range TNMetaSchemaAttr {
		if i == 0 {
			if err := TNMetaSchema.AppendPKCol(colname, TNMetaShcemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := TNMetaSchema.AppendCol(colname, TNMetaShcemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	ObjectInfoSchema = catalog.NewEmptySchema("object_info")
	for i, colname := range ObjectInfoAttr {
		if i == 0 {
			if err := ObjectInfoSchema.AppendPKCol(colname, ObjectInfoTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := ObjectInfoSchema.AppendCol(colname, ObjectInfoTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	StorageUsageSchema = catalog.NewEmptySchema("storage_usage")
	for i, colname := range StorageUsageSchemaAttrs {
		if i == 0 {
			if err := StorageUsageSchema.AppendPKCol(colname, StorageUsageSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := StorageUsageSchema.AppendCol(colname, StorageUsageSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}
}
