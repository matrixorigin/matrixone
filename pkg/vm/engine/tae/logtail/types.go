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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type Scope = int

const (
	// changes for mo_databases
	ScopeDatabases Scope = iota + 1
	// changes for mo_tables
	ScopeTables
	// changes for mo_columns
	ScopeColumns
	// changes for user tables
	ScopeUserTables
)

const (
	SnapshotAttr_TID     = catalog.SnapshotAttr_TID
	SnapshotAttr_DBID    = catalog.SnapshotAttr_DBID
	SegmentAttr_ID       = catalog.SegmentAttr_ID
	SegmentAttr_CreateAt = catalog.SegmentAttr_CreateAt
	SegmentAttr_State    = catalog.SegmentAttr_State
	SegmentAttr_Sorted   = catalog.SegmentAttr_Sorted
	// TODO(aptend): add SortHint and replay ckp correctly, onReplayCreateSegment. Use bytes as one column.
	// Low priority, because replay from ckp will keep the create order
	SnapshotAttr_BlockMaxRow               = catalog.SnapshotAttr_BlockMaxRow
	SnapshotAttr_SegmentMaxBlock           = catalog.SnapshotAttr_SegmentMaxBlock
	SnapshotMetaAttr_BlockInsertBatchStart = "block_insert_batch_start"
	SnapshotMetaAttr_BlockInsertBatchEnd   = "block_insert_batch_end"
	SnapshotMetaAttr_BlockDeleteBatchStart = "block_delete_batch_start"
	SnapshotMetaAttr_BlockDeleteBatchEnd   = "block_delete_batch_end"

	SnapshotAttr_SchemaExtra = catalog.SnapshotAttr_SchemaExtra
)

var (
	// for blk meta response
	BlkMetaSchema *catalog.Schema
	DelSchema     *catalog.Schema
	SegSchema     *catalog.Schema
	TxnNodeSchema *catalog.Schema
	DBDNSchema    *catalog.Schema
	TblDNSchema   *catalog.Schema
	SegDNSchema   *catalog.Schema
	BlkDNSchema   *catalog.Schema
	MetaSchema    *catalog.Schema
)

var (
	SegmentSchemaAttr = []string{
		SegmentAttr_ID,
		SegmentAttr_CreateAt,
		SegmentAttr_State,
		SegmentAttr_Sorted,
	}
	SegmentSchemaTypes = []types.Type{
		types.New(types.T_uuid, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_bool, 0, 0),
		types.New(types.T_bool, 0, 0),
	}
	TxnNodeSchemaAttr = []string{
		txnbase.SnapshotAttr_LogIndex_LSN,
		txnbase.SnapshotAttr_StartTS,
		txnbase.SnapshotAttr_PrepareTS,
		txnbase.SnapshotAttr_CommitTS,
		txnbase.SnapshotAttr_LogIndex_CSN,
		txnbase.SnapshotAttr_LogIndex_Size,
	}
	TxnNodeSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint32, 0, 0),
	}
	DBDNSchemaAttr = []string{
		txnbase.SnapshotAttr_LogIndex_LSN,
		txnbase.SnapshotAttr_StartTS,
		txnbase.SnapshotAttr_PrepareTS,
		txnbase.SnapshotAttr_CommitTS,
		txnbase.SnapshotAttr_LogIndex_CSN,
		txnbase.SnapshotAttr_LogIndex_Size,
		SnapshotAttr_DBID,
		SnapshotAttr_TID,
	}
	DBDNSchemaType = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
	}
	TblDNSchemaAttr = []string{
		txnbase.SnapshotAttr_LogIndex_LSN,
		txnbase.SnapshotAttr_StartTS,
		txnbase.SnapshotAttr_PrepareTS,
		txnbase.SnapshotAttr_CommitTS,
		txnbase.SnapshotAttr_LogIndex_CSN,
		txnbase.SnapshotAttr_LogIndex_Size,
		SnapshotAttr_DBID,
		SnapshotAttr_TID,
		SnapshotAttr_BlockMaxRow,
		SnapshotAttr_SegmentMaxBlock,
		SnapshotAttr_SchemaExtra,
	}
	TblDNSchemaType = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint16, 0, 0),
		types.New(types.T_varchar, 0, 0),
	}
	SegmentDNSchemaAttr = []string{
		txnbase.SnapshotAttr_LogIndex_LSN,
		txnbase.SnapshotAttr_StartTS,
		txnbase.SnapshotAttr_PrepareTS,
		txnbase.SnapshotAttr_CommitTS,
		txnbase.SnapshotAttr_LogIndex_CSN,
		txnbase.SnapshotAttr_LogIndex_Size,
		SnapshotAttr_DBID,
		SnapshotAttr_TID,
	}
	SegmentDNSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
	}
	BlockDNSchemaAttr = []string{
		txnbase.SnapshotAttr_LogIndex_LSN,
		txnbase.SnapshotAttr_StartTS,
		txnbase.SnapshotAttr_PrepareTS,
		txnbase.SnapshotAttr_CommitTS,
		txnbase.SnapshotAttr_LogIndex_CSN,
		txnbase.SnapshotAttr_LogIndex_Size,
		SnapshotAttr_DBID,
		SnapshotAttr_TID,
		pkgcatalog.BlockMeta_MetaLoc,
		pkgcatalog.BlockMeta_DeltaLoc,
	}
	BlockDNSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
	}
	MetaSchemaAttr = []string{
		SnapshotAttr_TID,
		SnapshotMetaAttr_BlockInsertBatchStart,
		SnapshotMetaAttr_BlockInsertBatchEnd,
		SnapshotMetaAttr_BlockDeleteBatchStart,
		SnapshotMetaAttr_BlockDeleteBatchEnd,
	}
	MetaShcemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_int32, 0, 0),
		types.New(types.T_int32, 0, 0),
		types.New(types.T_int32, 0, 0),
		types.New(types.T_int32, 0, 0),
	}
	BaseAttr = []string{
		catalog.AttrRowID,
		catalog.AttrCommitTs,
	}
	BaseTypes = []types.Type{
		types.T_Rowid.ToType(),
		types.T_TS.ToType(),
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
	BlkMetaSchema = catalog.NewEmptySchema("blkMeta")

	for i, colname := range pkgcatalog.MoTableMetaSchema {
		if i == 0 {
			if err := BlkMetaSchema.AppendPKCol(colname, pkgcatalog.MoTableMetaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := BlkMetaSchema.AppendCol(colname, pkgcatalog.MoTableMetaTypes[i]); err != nil {
				panic(err)
			}
		}
	}
	if err := BlkMetaSchema.Finalize(true); err != nil { // no phyaddr column
		panic(err)
	}

	// empty schema, no finalize, makeRespBatchFromSchema will add necessary colunms
	DelSchema = catalog.NewEmptySchema("del")

	SegSchema = catalog.NewEmptySchema("segment")
	for i, colname := range SegmentSchemaAttr {
		if i == 0 {
			if err := SegSchema.AppendPKCol(colname, SegmentSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := SegSchema.AppendCol(colname, SegmentSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	TxnNodeSchema = catalog.NewEmptySchema("txn_node")
	for i, colname := range TxnNodeSchemaAttr {
		if i == 0 {
			if err := TxnNodeSchema.AppendPKCol(colname, TxnNodeSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := TxnNodeSchema.AppendCol(colname, TxnNodeSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	DBDNSchema = catalog.NewEmptySchema("db_dn")
	for i, colname := range DBDNSchemaAttr {
		if i == 0 {
			if err := DBDNSchema.AppendPKCol(colname, DBDNSchemaType[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := DBDNSchema.AppendCol(colname, DBDNSchemaType[i]); err != nil {
				panic(err)
			}
		}
	}

	TblDNSchema = catalog.NewEmptySchema("table_dn")
	for i, colname := range TblDNSchemaAttr {
		if i == 0 {
			if err := TblDNSchema.AppendPKCol(colname, TblDNSchemaType[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := TblDNSchema.AppendCol(colname, TblDNSchemaType[i]); err != nil {
				panic(err)
			}
		}
	}

	SegDNSchema = catalog.NewEmptySchema("segment_dn")
	for i, colname := range SegmentDNSchemaAttr {
		if i == 0 {
			if err := SegDNSchema.AppendPKCol(colname, SegmentDNSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := SegDNSchema.AppendCol(colname, SegmentDNSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

	BlkDNSchema = catalog.NewEmptySchema("block_dn")
	for i, colname := range BlockDNSchemaAttr {
		if i == 0 {
			if err := BlkDNSchema.AppendPKCol(colname, BlockDNSchemaTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err := BlkDNSchema.AppendCol(colname, BlockDNSchemaTypes[i]); err != nil {
				panic(err)
			}
		}
	}

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
}
