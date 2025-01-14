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

package ckputil

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// 1. All checkpoint files should be sorted by `cluster` key. And the
//    `cluster` key is `table`+ `object type`+ `id`.
// 2. For a specific table, we can save the checkpoint meta like this:
//    `table`: table id
//    `object type`: object type [Data|Tombstone]
//    `from`: the start rowid in the checkpoint files
//    `to`: the end rowid in the checkpoint files

const (
	TableObjectsAttr_Accout     = "account_id"
	TableObjectsAttr_DB         = "db_id"
	TableObjectsAttr_Table      = "table_id"
	TableObjectsAttr_ID         = "id"
	TableObjectsAttr_CreateTS   = "create_ts"
	TableObjectsAttr_DeleteTS   = "delete_ts"
	TableObjectsAttr_ObjectType = "object_type"

	// TableObjects should be clustered by `table`+`id`
	TableObjectsAttr_Cluster = "cluster"

	// TableObjectsAttr_PhysicalAddr = objectio.PhysicalAddr_Attr
)

const (
	TableObjectsAttr_Accout_Idx     = 0
	TableObjectsAttr_DB_Idx         = 1
	TableObjectsAttr_Table_Idx      = 2
	TableObjectsAttr_ObjectType_Idx = 3
	TableObjectsAttr_ID_Idx         = 4
	TableObjectsAttr_CreateTS_Idx   = 5
	TableObjectsAttr_DeleteTS_Idx   = 6
	TableObjectsAttr_Cluster_Idx    = 7
	// TableObjectsAttr_PhysicalAddr_Idx = 8
)

const (
	ObjectType_Invalid int8 = iota
	ObjectType_Data
	ObjectType_Tombstone
)

var TableObjectsAttrs = []string{
	TableObjectsAttr_Accout,
	TableObjectsAttr_DB,
	TableObjectsAttr_Table,
	TableObjectsAttr_ObjectType,
	TableObjectsAttr_ID,
	TableObjectsAttr_CreateTS,
	TableObjectsAttr_DeleteTS,
	TableObjectsAttr_Cluster,
	// TableObjectsAttr_PhysicalAddr,
}

var TableObjectsTypes = []types.Type{
	types.T_uint32.ToType(),
	types.T_uint64.ToType(),
	types.T_uint64.ToType(),
	types.T_int8.ToType(),
	types.T_char.ToType(),
	types.T_TS.ToType(),
	types.T_TS.ToType(),
	types.T_char.ToType(),
	// types.T_Rowid.ToType(),
}
var TableObjectsSeqnums = []uint16{0, 1, 2, 3, 4, 5, 6, 7}

// var ObjectEntryTypes = []types.Type{
// 	TableObjectsTypes[TableObjectsAttr_ID_Idx],
// 	TableObjectsTypes[TableObjectsAttr_CreateTS_Idx],
// 	TableObjectsTypes[TableObjectsAttr_DeleteTS_Idx],
// }

// var ObjectEntrySeqnums = []uint16{
// 	TableObjectsAttr_ID_Idx,
// 	TableObjectsAttr_CreateTS_Idx,
// 	TableObjectsAttr_DeleteTS_Idx,
// }

const (
	MetaAttr_Table    = "table_id"
	MetaAttr_Start    = "start_rowid"
	MetaAttr_End      = "end_rowid"
	MetaAttr_Location = "location"
)

const (
	MetaAttr_Table_Idx    = 0
	MetaAttr_Start_Idx    = 1
	MetaAttr_End_Idx      = 2
	MetaAttr_Location_Idx = 3
)

var MetaAttrs = []string{
	MetaAttr_Table,
	MetaAttr_Start,
	MetaAttr_End,
	MetaAttr_Location,
}

var MetaTypes = []types.Type{
	types.T_uint64.ToType(),
	types.T_Rowid.ToType(),
	types.T_Rowid.ToType(),
	types.T_char.ToType(),
}

var MetaSeqnums = []uint16{0, 1, 2, 3}

func NewObjectListBatch() *batch.Batch {
	return batch.NewWithSchema(false, TableObjectsAttrs, TableObjectsTypes)
}

func NewMetaBatch() *batch.Batch {
	return batch.NewWithSchema(false, MetaAttrs, MetaTypes)
}
