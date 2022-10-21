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
	SegmentAttr_ID       = "id"
	SegmentAttr_CreateAt = "create_at"
	SegmentAttr_State    = "state"
)

var (
	// for blk meta response
	BlkMetaSchema *catalog.Schema
	DelSchema     *catalog.Schema
	SegSchema     *catalog.Schema
	TxnNodeSchema *catalog.Schema
)

var (
	SegmentSchemaAttr = []string{
		SegmentAttr_ID,
		SegmentAttr_CreateAt,
		SegmentAttr_State,
	}
	SegmentSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0, 0),
		types.New(types.T_TS, 0, 0, 0),
		types.New(types.T_bool, 0, 0, 0),
	}
	TxnNodeSchemaAttr = []string{
		txnbase.SnapshotAttr_LogIndex_LSN,
		txnbase.SnapshotAttr_StartTS,
		txnbase.SnapshotAttr_PrepareTS,
		txnbase.SnapshotAttr_LogIndex_CSN,
		txnbase.SnapshotAttr_LogIndex_Size,
	}
	TxnNodeSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0, 0),
		types.New(types.T_TS, 0, 0, 0),
		types.New(types.T_TS, 0, 0, 0),
		types.New(types.T_uint32, 0, 0, 0),
		types.New(types.T_uint32, 0, 0, 0),
	}
)

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
	TxnNodeSchema = catalog.NewEmptySchema("segment")

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
}
