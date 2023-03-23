// Copyright 2022 Matrix Origin
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

package catalog

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	Row_ID               = "__mo_rowid"
	PrefixPriColName     = "__mo_cpkey_"
	PrefixCBColName      = "__mo_cbkey_"
	PrefixIndexTableName = "__mo_index_"
	// Compound primary key column name, which is a hidden column
	CPrimaryKeyColName = "__mo_cpkey_col"
	// IndexTable has two column at most, the first is idx col, the second is origin table primary col
	IndexTableIndexColName   = "__mo_index_idx_col"
	IndexTablePrimaryColName = "__mo_index_pri_col"
	ExternalFilePath         = "__mo_filepath"
	IndexTableNamePrefix     = "__mo_index_unique__"
	AutoIncrTableName        = "%!%mo_increment_columns"
)

var AutoIncrColumnNames = []string{Row_ID, "name", "offset", "step"}

func ContainExternalHidenCol(col string) bool {
	return col == ExternalFilePath
}

func IsHiddenTable(name string) bool {
	if strings.HasPrefix(name, IndexTableNamePrefix) {
		return true
	}
	return strings.EqualFold(name, AutoIncrTableName)
}

const (
	Meta_Length = 6
)

const (
	System_User    = uint32(0)
	System_Role    = uint32(0)
	System_Account = uint32(0)
)

const (
	// Non-hard-coded data dictionary table
	MO_INDEXES = "mo_indexes"
)

const (
	// default database name for catalog
	MO_CATALOG  = "mo_catalog"
	MO_DATABASE = "mo_database"
	MO_TABLES   = "mo_tables"
	MO_COLUMNS  = "mo_columns"

	// 'mo_database' table
	SystemDBAttr_ID          = "dat_id"
	SystemDBAttr_Name        = "datname"
	SystemDBAttr_CatalogName = "dat_catalog_name"
	SystemDBAttr_CreateSQL   = "dat_createsql"
	SystemDBAttr_Owner       = "owner"
	SystemDBAttr_Creator     = "creator"
	SystemDBAttr_CreateAt    = "created_time"
	SystemDBAttr_AccID       = "account_id"
	SystemDBAttr_Type        = "dat_type"

	// 'mo_tables' table
	SystemRelAttr_ID          = "rel_id"
	SystemRelAttr_Name        = "relname"
	SystemRelAttr_DBName      = "reldatabase"
	SystemRelAttr_DBID        = "reldatabase_id"
	SystemRelAttr_Persistence = "relpersistence"
	SystemRelAttr_Kind        = "relkind"
	SystemRelAttr_Comment     = "rel_comment"
	SystemRelAttr_CreateSQL   = "rel_createsql"
	SystemRelAttr_CreateAt    = "created_time"
	SystemRelAttr_Creator     = "creator"
	SystemRelAttr_Owner       = "owner"
	SystemRelAttr_AccID       = "account_id"
	SystemRelAttr_Partition   = "partitioned"
	SystemRelAttr_ViewDef     = "viewdef"
	SystemRelAttr_Constraint  = "constraint"

	// 'mo_columns' table
	SystemColAttr_UniqName        = "att_uniq_name"
	SystemColAttr_AccID           = "account_id"
	SystemColAttr_Name            = "attname"
	SystemColAttr_DBID            = "att_database_id"
	SystemColAttr_DBName          = "att_database"
	SystemColAttr_RelID           = "att_relname_id"
	SystemColAttr_RelName         = "att_relname"
	SystemColAttr_Type            = "atttyp"
	SystemColAttr_Num             = "attnum"
	SystemColAttr_Length          = "att_length"
	SystemColAttr_NullAbility     = "attnotnull"
	SystemColAttr_HasExpr         = "atthasdef"
	SystemColAttr_DefaultExpr     = "att_default"
	SystemColAttr_IsDropped       = "attisdropped"
	SystemColAttr_ConstraintType  = "att_constraint_type"
	SystemColAttr_IsUnsigned      = "att_is_unsigned"
	SystemColAttr_IsAutoIncrement = "att_is_auto_increment"
	SystemColAttr_Comment         = "att_comment"
	SystemColAttr_IsHidden        = "att_is_hidden"
	SystemColAttr_HasUpdate       = "attr_has_update"
	SystemColAttr_Update          = "attr_update"
	SystemColAttr_IsClusterBy     = "attr_is_clusterby"

	BlockMeta_ID              = "block_id"
	BlockMeta_EntryState      = "entry_state"
	BlockMeta_Sorted          = "sorted"
	BlockMeta_MetaLoc         = "%!%mo__meta_loc"
	BlockMeta_DeltaLoc        = "delta_loc"
	BlockMeta_CommitTs        = "committs"
	BlockMeta_SegmentID       = "segment_id"
	BlockMeta_TableIdx_Insert = "%!%mo__meta_tbl_index" // mark which table this metaLoc belongs to

	SystemCatalogName  = "def"
	SystemPersistRel   = "p"
	SystemTransientRel = "t"

	SystemOrdinaryRel     = "r"
	SystemIndexRel        = "i"
	SystemSequenceRel     = "S"
	SystemViewRel         = "v"
	SystemMaterializedRel = "m"
	SystemExternalRel     = "e"
	//the cluster table created by the sys account
	//and read only by the general account
	SystemClusterRel = "cluster"

	SystemColPKConstraint = "p"
	SystemColNoConstraint = "n"

	SystemDBTypeSubscription = "subscription"
)

const (
	// default database id for catalog
	MO_CATALOG_ID  = 1
	MO_DATABASE_ID = 1
	MO_TABLES_ID   = 2
	MO_COLUMNS_ID  = 3
)

// index use to update constraint
const (
	MO_TABLES_UPDATE_CONSTRAINT = 4
)

// column's index in catalog table
const (
	MO_DATABASE_DAT_ID_IDX           = 0
	MO_DATABASE_DAT_NAME_IDX         = 1
	MO_DATABASE_DAT_CATALOG_NAME_IDX = 2
	MO_DATABASE_CREATESQL_IDX        = 3
	MO_DATABASE_OWNER_IDX            = 4
	MO_DATABASE_CREATOR_IDX          = 5
	MO_DATABASE_CREATED_TIME_IDX     = 6
	MO_DATABASE_ACCOUNT_ID_IDX       = 7
	MO_DATABASE_DAT_TYPE_IDX         = 8

	MO_TABLES_REL_ID_IDX         = 0
	MO_TABLES_REL_NAME_IDX       = 1
	MO_TABLES_RELDATABASE_IDX    = 2
	MO_TABLES_RELDATABASE_ID_IDX = 3
	MO_TABLES_RELPERSISTENCE_IDX = 4
	MO_TABLES_RELKIND_IDX        = 5
	MO_TABLES_REL_COMMENT_IDX    = 6
	MO_TABLES_REL_CREATESQL_IDX  = 7
	MO_TABLES_CREATED_TIME_IDX   = 8
	MO_TABLES_CREATOR_IDX        = 9
	MO_TABLES_OWNER_IDX          = 10
	MO_TABLES_ACCOUNT_ID_IDX     = 11
	MO_TABLES_PARTITIONED_IDX    = 12
	MO_TABLES_VIEWDEF_IDX        = 13
	MO_TABLES_CONSTRAINT_IDX     = 14

	MO_COLUMNS_ATT_UNIQ_NAME_IDX         = 0
	MO_COLUMNS_ACCOUNT_ID_IDX            = 1
	MO_COLUMNS_ATT_DATABASE_ID_IDX       = 2
	MO_COLUMNS_ATT_DATABASE_IDX          = 3
	MO_COLUMNS_ATT_RELNAME_ID_IDX        = 4
	MO_COLUMNS_ATT_RELNAME_IDX           = 5
	MO_COLUMNS_ATTNAME_IDX               = 6
	MO_COLUMNS_ATTTYP_IDX                = 7
	MO_COLUMNS_ATTNUM_IDX                = 8
	MO_COLUMNS_ATT_LENGTH_IDX            = 9
	MO_COLUMNS_ATTNOTNULL_IDX            = 10
	MO_COLUMNS_ATTHASDEF_IDX             = 11
	MO_COLUMNS_ATT_DEFAULT_IDX           = 12
	MO_COLUMNS_ATTISDROPPED_IDX          = 13
	MO_COLUMNS_ATT_CONSTRAINT_TYPE_IDX   = 14
	MO_COLUMNS_ATT_IS_UNSIGNED_IDX       = 15
	MO_COLUMNS_ATT_IS_AUTO_INCREMENT_IDX = 16
	MO_COLUMNS_ATT_COMMENT_IDX           = 17
	MO_COLUMNS_ATT_IS_HIDDEN_IDX         = 18
	MO_COLUMNS_ATT_HAS_UPDATE_IDX        = 19
	MO_COLUMNS_ATT_UPDATE_IDX            = 20
	MO_COLUMNS_ATT_IS_CLUSTERBY          = 21

	BLOCKMETA_ID_IDX         = 0
	BLOCKMETA_ENTRYSTATE_IDX = 1
	BLOCKMETA_SORTED_IDX     = 2
	BLOCKMETA_METALOC_IDX    = 3
	BLOCKMETA_DELTALOC_IDX   = 4
	BLOCKMETA_COMMITTS_IDX   = 5
	BLOCKMETA_SEGID_IDX      = 6
)

type BlockInfo struct {
	BlockID    uint64
	EntryState bool
	Sorted     bool
	MetaLoc    string
	DeltaLoc   string
	CommitTs   types.TS
	SegmentID  uint64
}

// used for memengine and tae
// tae and memengine do not make the catalog into a table
// for its convenience, a conversion interface is provided to ensure easy use.
type CreateDatabase struct {
	DatabaseId  uint64
	Name        string
	CreateSql   string
	Owner       uint32
	Creator     uint32
	AccountId   uint32
	CreatedTime types.Timestamp
}

type DropDatabase struct {
	Id   uint64
	Name string
}

type CreateTable struct {
	TableId      uint64
	Name         string
	CreateSql    string
	Owner        uint32
	Creator      uint32
	AccountId    uint32
	DatabaseId   uint64
	DatabaseName string
	Comment      string
	Partition    string
	RelKind      string
	Viewdef      string
	Constraint   []byte
	Defs         []engine.TableDef
}

type UpdateConstraint struct {
	DatabaseId   uint64
	TableId      uint64
	TableName    string
	DatabaseName string
	Constraint   []byte
}

type DropOrTruncateTable struct {
	IsDrop       bool // true for Drop and false for Truncate
	Id           uint64
	NewId        uint64
	Name         string
	DatabaseId   uint64
	DatabaseName string
}

var (
	MoDatabaseSchema = []string{
		SystemDBAttr_ID,
		SystemDBAttr_Name,
		SystemDBAttr_CatalogName,
		SystemDBAttr_CreateSQL,
		SystemDBAttr_Owner,
		SystemDBAttr_Creator,
		SystemDBAttr_CreateAt,
		SystemDBAttr_AccID,
		SystemDBAttr_Type,
	}
	MoTablesSchema = []string{
		SystemRelAttr_ID,
		SystemRelAttr_Name,
		SystemRelAttr_DBName,
		SystemRelAttr_DBID,
		SystemRelAttr_Persistence,
		SystemRelAttr_Kind,
		SystemRelAttr_Comment,
		SystemRelAttr_CreateSQL,
		SystemRelAttr_CreateAt,
		SystemRelAttr_Creator,
		SystemRelAttr_Owner,
		SystemRelAttr_AccID,
		SystemRelAttr_Partition,
		SystemRelAttr_ViewDef,
		SystemRelAttr_Constraint,
	}
	MoColumnsSchema = []string{
		SystemColAttr_UniqName,
		SystemColAttr_AccID,
		SystemColAttr_DBID,
		SystemColAttr_DBName,
		SystemColAttr_RelID,
		SystemColAttr_RelName,
		SystemColAttr_Name,
		SystemColAttr_Type,
		SystemColAttr_Num,
		SystemColAttr_Length,
		SystemColAttr_NullAbility,
		SystemColAttr_HasExpr,
		SystemColAttr_DefaultExpr,
		SystemColAttr_IsDropped,
		SystemColAttr_ConstraintType,
		SystemColAttr_IsUnsigned,
		SystemColAttr_IsAutoIncrement,
		SystemColAttr_Comment,
		SystemColAttr_IsHidden,
		SystemColAttr_HasUpdate,
		SystemColAttr_Update,
		SystemColAttr_IsClusterBy,
	}
	MoTableMetaSchema = []string{
		BlockMeta_ID,
		BlockMeta_EntryState,
		BlockMeta_Sorted,
		BlockMeta_MetaLoc,
		BlockMeta_DeltaLoc,
		BlockMeta_CommitTs,
		BlockMeta_SegmentID,
	}
	MoDatabaseTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),     // dat_id
		types.New(types.T_varchar, 5000, 0), // datname
		types.New(types.T_varchar, 5000, 0), // dat_catalog_name
		types.New(types.T_varchar, 5000, 0), // dat_createsql
		types.New(types.T_uint32, 0, 0),     // owner
		types.New(types.T_uint32, 0, 0),     // creator
		types.New(types.T_timestamp, 0, 0),  // created_time
		types.New(types.T_uint32, 0, 0),     // account_id
		types.New(types.T_varchar, 32, 0),   // dat_type
	}
	MoTablesTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),     // rel_id
		types.New(types.T_varchar, 5000, 0), // relname
		types.New(types.T_varchar, 5000, 0), // reldatabase
		types.New(types.T_uint64, 0, 0),     // reldatabase_id
		types.New(types.T_varchar, 5000, 0), // relpersistence
		types.New(types.T_varchar, 5000, 0), // relkind
		types.New(types.T_varchar, 5000, 0), // rel_comment
		types.New(types.T_text, 0, 0),       // rel_createsql
		types.New(types.T_timestamp, 0, 0),  // created_time
		types.New(types.T_uint32, 0, 0),     // creator
		types.New(types.T_uint32, 0, 0),     // owner
		types.New(types.T_uint32, 0, 0),     // account_id
		types.New(types.T_blob, 0, 0),       // partition
		types.New(types.T_blob, 0, 0),       // viewdef
		types.New(types.T_varchar, 5000, 0), // constraint
	}
	MoColumnsTypes = []types.Type{
		types.New(types.T_varchar, 256, 0),  // att_uniq_name
		types.New(types.T_uint32, 0, 0),     // account_id
		types.New(types.T_uint64, 0, 0),     // att_database_id
		types.New(types.T_varchar, 256, 0),  // att_database
		types.New(types.T_uint64, 0, 0),     // att_relname_id
		types.New(types.T_varchar, 256, 0),  // att_relname
		types.New(types.T_varchar, 256, 0),  // attname
		types.New(types.T_varchar, 256, 0),  // atttyp
		types.New(types.T_int32, 0, 0),      // attnum
		types.New(types.T_int32, 0, 0),      // att_length
		types.New(types.T_int8, 0, 0),       // attnotnull
		types.New(types.T_int8, 0, 0),       // atthasdef
		types.New(types.T_varchar, 2048, 0), // att_default
		types.New(types.T_int8, 0, 0),       // attisdropped
		types.New(types.T_char, 1, 0),       // att_constraint_type
		types.New(types.T_int8, 0, 0),       // att_is_unsigned
		types.New(types.T_int8, 0, 0),       // att_is_auto_increment
		types.New(types.T_varchar, 2048, 0), // att_comment
		types.New(types.T_int8, 0, 0),       // att_is_hidden
		types.New(types.T_int8, 0, 0),       // att_has_update
		types.New(types.T_varchar, 2048, 0), // att_update
		types.New(types.T_int8, 0, 0),       // att_is_clusterby
	}
	MoTableMetaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),                    // block_id
		types.New(types.T_bool, 0, 0),                      // entry_state, true for appendable
		types.New(types.T_bool, 0, 0),                      // sorted, true for sorted by primary key
		types.New(types.T_varchar, types.MaxVarcharLen, 0), // meta_loc
		types.New(types.T_varchar, types.MaxVarcharLen, 0), // delta_loc
		types.New(types.T_TS, 0, 0),                        // committs
		types.New(types.T_uint64, 0, 0),                    // segment_id
	}
	// used by memengine or tae
	MoDatabaseTableDefs = []engine.TableDef{}
	// used by memengine or tae
	MoTablesTableDefs = []engine.TableDef{}
	// used by memengine or tae
	MoColumnsTableDefs = []engine.TableDef{}
	// used by memengine or tae or cn
	MoTableMetaDefs = []engine.TableDef{}
)

var (
	QueryResultPath     string
	QueryResultMetaPath string
	QueryResultMetaDir  string
)

func init() {
	QueryResultPath = fileservice.JoinPath(defines.SharedFileServiceName, "/query_result/%s_%s_%d.blk")
	QueryResultMetaPath = fileservice.JoinPath(defines.SharedFileServiceName, "/query_result_meta/%s_%s.blk")
	QueryResultMetaDir = fileservice.JoinPath(defines.SharedFileServiceName, "/query_result_meta")
}

const QueryResultName = "%s_%s_%d.blk"
const QueryResultMetaName = "%s_%s.blk"

type Meta struct {
	QueryId     [16]byte
	Statement   string
	AccountId   uint32
	RoleId      uint32
	ResultPath  string
	CreateTime  types.Timestamp
	ResultSize  float64
	Columns     string
	Tables      string
	UserId      uint32
	ExpiredTime types.Timestamp
	Plan        string
	Ast         string
	ColumnMap   string
}

var (
	MetaColTypes = []types.Type{
		types.New(types.T_uuid, 0, 0),      // query_id
		types.New(types.T_text, 0, 0),      // statement
		types.New(types.T_uint32, 0, 0),    // account_id
		types.New(types.T_uint32, 0, 0),    // role_id
		types.New(types.T_text, 0, 0),      // result_path
		types.New(types.T_timestamp, 0, 0), // create_time
		types.New(types.T_float64, 0, 0),   // result_size
		types.New(types.T_text, 0, 0),      // columns
		types.New(types.T_text, 0, 0),      // Tables
		types.New(types.T_uint32, 0, 0),    // user_id
		types.New(types.T_timestamp, 0, 0), // expired_time
		types.New(types.T_text, 0, 0),      // Plan
		types.New(types.T_text, 0, 0),      // Ast
		types.New(types.T_text, 0, 0),      // ColumnMap
	}

	MetaColNames = []string{
		"query_id",
		"statement",
		"account_id",
		"role_id",
		"result_path",
		"create_time",
		"result_size",
		"columns",
		"tables",
		"user_id",
		"expired_time",
		"plan",
		"Ast",
		"ColumnMap",
	}
)

const (
	QUERY_ID_IDX     = 0
	STATEMENT_IDX    = 1
	ACCOUNT_ID_IDX   = 2
	ROLE_ID_IDX      = 3
	RESULT_PATH_IDX  = 4
	CREATE_TIME_IDX  = 5
	RESULT_SIZE_IDX  = 6
	COLUMNS_IDX      = 7
	TABLES_IDX       = 8
	USER_ID_IDX      = 9
	EXPIRED_TIME_IDX = 10
	PLAN_IDX         = 11
	AST_IDX          = 12
	COLUMN_MAP_IDX   = 13
)
