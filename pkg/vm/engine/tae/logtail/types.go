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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
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
	blkMetaAttrBlockID    = "block_id"
	blkMetaAttrEntryState = "entry_state"
	blkMetaAttrCreateAt   = "create_at"
	blkMetaAttrDeleteAt   = "delete_at"
	blkMetaAttrMetaLoc    = "meta_loc"
	blkMetaAttrDeltaLoc   = "delta_loc"
)

var (
	// for blk meta response
	BlkMetaSchema *catalog.Schema
	DelSchema     *catalog.Schema
)

func init() {
	BlkMetaSchema = catalog.NewEmptySchema("blkMeta")
	BlkMetaSchema.AppendCol(blkMetaAttrBlockID, types.T_uint64.ToType())
	BlkMetaSchema.AppendCol(blkMetaAttrEntryState, types.T_bool.ToType()) // 0: Nonappendable 1: appendable
	BlkMetaSchema.AppendCol(blkMetaAttrCreateAt, types.T_TS.ToType())
	BlkMetaSchema.AppendCol(blkMetaAttrDeleteAt, types.T_TS.ToType())
	BlkMetaSchema.AppendCol(blkMetaAttrMetaLoc, types.T_varchar.ToType())
	BlkMetaSchema.AppendCol(blkMetaAttrDeltaLoc, types.T_varchar.ToType())
	BlkMetaSchema.Finalize(true) // no phyaddr column

	// empty schema, no finalize, makeRespBatchFromSchema will add necessary colunms
	DelSchema = catalog.NewEmptySchema("del")
}
