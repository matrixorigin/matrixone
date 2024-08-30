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
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	Row_ID           = "__mo_rowid"
	PrefixPriColName = "__mo_cpkey_"
	PrefixCBColName  = "__mo_cbkey_"

	// Wildcard characters for partition subtable name
	PartitionSubTableWildcard = "\\%!\\%%\\%!\\%%"

	ExternalFilePath = "__mo_filepath"

	// MOAutoIncrTable mo auto increment table name
	MOAutoIncrTable = "mo_increment_columns"
	// TableTailAttr are attrs in table tail
	TableTailAttrDeleteRowID = "__mo_%1_delete_rowid"
	TableTailAttrCommitTs    = "__mo_%1_commit_time"
	TableTailAttrAborted     = "__mo_%1_aborted"
	TableTailAttrPKVal       = "__mo_%1_pk_val"

	MOAccountTable = "mo_account"
	// MOVersionTable mo version table. This table records information about the
	// versions of the MO cluster that have been upgraded. In other words, you can
	// query this table to find out all the versions of the MO cluster that have
	// been running.
	MOVersionTable = "mo_version"
	// MOUpgradeTable mo upgrade table. This table records the MO cluster version
	// upgrade paths, including upgrade paths for intermediate versions that are
	// upgraded across versions.
	MOUpgradeTable = "mo_upgrade"
	// MOUpgradeTenantTable MO is a cloud-native, multi-tenant database, and when
	// versions are upgraded, it may be necessary to upgrade all tenant-related metadata.
	// This table is used to record all the tenant records that need to be upgraded
	MOUpgradeTenantTable = "mo_upgrade_tenant"

	// MOForeignKeys saves the fk relationships
	MOForeignKeys = "mo_foreign_keys"

	// MOShardsMetadata is used to store the sharding information of the table. See detail
	// in shardservice.
	MOShardsMetadata = "mo_shards_metadata"
	// MOShards shards detail.
	MOShards = "mo_shards"
)

var InternalColumns = map[string]int8{
	Row_ID:                                     0,
	PrefixPriColName:                           0,
	PrefixCBColName:                            0,
	PrefixIndexTableName:                       0,
	CPrimaryKeyColName:                         0,
	FakePrimaryKeyColName:                      0,
	IndexTableIndexColName:                     0,
	IndexTablePrimaryColName:                   0,
	SystemSI_IVFFLAT_TblCol_Metadata_key:       0,
	SystemSI_IVFFLAT_TblCol_Metadata_val:       0,
	SystemSI_IVFFLAT_TblCol_Centroids_version:  0,
	SystemSI_IVFFLAT_TblCol_Centroids_id:       0,
	SystemSI_IVFFLAT_TblCol_Centroids_centroid: 0,
	SystemSI_IVFFLAT_TblCol_Entries_version:    0,
	SystemSI_IVFFLAT_TblCol_Entries_id:         0,
	SystemSI_IVFFLAT_TblCol_Entries_entry:      0,
}

var InternalTableNames = map[string]int8{
	IndexTableNamePrefix: 0,
	MOAutoIncrTable:      0,
}

func ContainExternalHidenCol(col string) bool {
	return col == ExternalFilePath
}

func IsHiddenTable(name string) bool {
	if strings.HasPrefix(name, IndexTableNamePrefix) {
		return true
	}
	return strings.EqualFold(name, MOAutoIncrTable)
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
	MO_COMMENT_NO_DEL_HINT = "[mo_no_del_hint]"
)

const (
	// Non-hard-coded data dictionary table
	MO_INDEXES = "mo_indexes"

	// MO_TABLE_PARTITIONS Data dictionary table of record table partition
	MO_TABLE_PARTITIONS = "mo_table_partitions"

	// MOTaskDB mo task db name
	MOTaskDB = "mo_task"

	// MOSysDaemonTask is the table name of daemon task table in mo_task.
	MOSysDaemonTask = "sys_daemon_task"

	// MOSysAsyncTask is the table name of async task table in mo_task.
	MOSysAsyncTask = "sys_async_task"

	// MOStages if the table name of mo_stages table in mo_cataglog.
	MO_STAGES = "mo_stages"

	// MO_PUBS publication meta table
	MO_PUBS = "mo_pubs"

	// MO_SUBS subscriptions meta table
	MO_SUBS = "mo_subs"

	// MO_SNAPSHOTS
	MO_SNAPSHOTS = "mo_snapshots"

	//MO_Pitr
	MO_PITR = "mo_pitr"
)

const (
	// Metrics and Trace related

	MO_SYSTEM    = "system"
	MO_STATEMENT = "statement_info"
	MO_RAWLOG    = "rawlog"

	MO_SYSTEM_METRICS = "system_metrics"
	MO_METRIC         = "metric"
	MO_SQL_STMT_CU    = "sql_statement_cu"

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
	SystemDBAttr_CPKey       = CPrimaryKeyColName

	// 'mo_tables' table
	SystemRelAttr_ID             = "rel_id"
	SystemRelAttr_Name           = "relname"
	SystemRelAttr_DBName         = "reldatabase"
	SystemRelAttr_DBID           = "reldatabase_id"
	SystemRelAttr_Persistence    = "relpersistence"
	SystemRelAttr_Kind           = "relkind"
	SystemRelAttr_Comment        = "rel_comment"
	SystemRelAttr_CreateSQL      = "rel_createsql"
	SystemRelAttr_CreateAt       = "created_time"
	SystemRelAttr_Creator        = "creator"
	SystemRelAttr_Owner          = "owner"
	SystemRelAttr_AccID          = "account_id"
	SystemRelAttr_Partitioned    = "partitioned"
	SystemRelAttr_Partition      = "partition_info"
	SystemRelAttr_ViewDef        = "viewdef"
	SystemRelAttr_Constraint     = "constraint"
	SystemRelAttr_Version        = "rel_version"
	SystemRelAttr_CatalogVersion = "catalog_version"
	SystemRelAttr_CPKey          = CPrimaryKeyColName

	// 'mo_indexes' table
	IndexAlgoName      = "algo"
	IndexAlgoTableType = "algo_table_type"
	IndexAlgoParams    = "algo_params"

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
	SystemColAttr_Seqnum          = "attr_seqnum"
	SystemColAttr_EnumValues      = "attr_enum"
	SystemColAttr_CPKey           = CPrimaryKeyColName

	BlockMeta_ID              = "block_id"
	BlockMeta_Delete_ID       = "block_delete_id"
	BlockMeta_EntryState      = "entry_state"
	BlockMeta_Sorted          = "sorted"
	BlockMeta_BlockInfo       = "%!%mo__block_info"
	BlockMeta_MetaLoc         = "%!%mo__meta_loc"
	BlockMeta_DeltaLoc        = "delta_loc"
	BlockMeta_CommitTs        = "committs"
	BlockMeta_SegmentID       = "segment_id"
	BlockMeta_MemTruncPoint   = "trunc_pointt"
	BlockMeta_TableIdx_Insert = "%!%mo__meta_tbl_index" // mark which table this metaLoc belongs to
	BlockMeta_Type            = "%!%mo__meta_type"
	BlockMeta_Deletes_Length  = "%!%mo__meta_deletes_length"
	BlockMeta_Partition       = "%!%mo__meta_partition"

	ObjectMeta_ObjectStats = "object_stats"

	// BlockMetaOffset_Min       = "%!%mo__meta_offset_min"
	// BlockMetaOffset_Max       = "%!%mo__meta_offset_max"
	BlockMetaOffset    = "%!%mo__meta_offset"
	SystemCatalogName  = "def"
	SystemPersistRel   = "p"
	SystemTransientRel = "t"

	SystemOrdinaryRel     = "r"
	SystemIndexRel        = "i"
	SystemSequenceRel     = "S"
	SystemViewRel         = "v"
	SystemMaterializedRel = "m"
	SystemExternalRel     = plan.SystemExternalRel
	SystemSourceRel       = "s"
	//the cluster table created by the sys account
	//and read only by the general account
	SystemClusterRel = "cluster"
	/*
		the partition table contains the data of the partition.
		the table partitioned has multiple partition tables
	*/
	SystemPartitionRel    = "partition"
	SystemColPKConstraint = "p"
	SystemColNoConstraint = "n"

	SystemDBTypeSubscription = "subscription"
)

// Key/Index related constants
const (
	UniqueIndexSuffix             = "unique_"
	SecondaryIndexSuffix          = "secondary_"
	PrefixIndexTableName          = "__mo_index_"
	IndexTableNamePrefix          = PrefixIndexTableName
	UniqueIndexTableNamePrefix    = PrefixIndexTableName + UniqueIndexSuffix
	SecondaryIndexTableNamePrefix = PrefixIndexTableName + SecondaryIndexSuffix

	/************ 0. Regular Secondary Index ************/

	// Regualar secondary index table columns
	IndexTableIndexColName   = "__mo_index_idx_col"
	IndexTablePrimaryColName = "__mo_index_pri_col"

	CPrimaryKeyColName = "__mo_cpkey_col" // Compound primary key column name, which is a hidden column
	// FakePrimaryKeyColName for tables without a primary key, a new hidden primary key column
	// is added, which will not be sorted or used for any other purpose, but will only be used to add
	// locks to the Lock operator in pessimistic transaction mode.
	FakePrimaryKeyColName = "__mo_fake_pk_col"

	/************ 1. Master Index  ************/

	MasterIndexTableIndexColName   = IndexTableIndexColName
	MasterIndexTablePrimaryColName = IndexTablePrimaryColName

	/************ 2. IVF_FLAT Secondary Index ************/

	// IVF_FLAT Table Types
	SystemSI_IVFFLAT_TblType_Metadata  = "metadata"
	SystemSI_IVFFLAT_TblType_Centroids = "centroids"
	SystemSI_IVFFLAT_TblType_Entries   = "entries"

	// IVF_FLAT MetadataTable - Column names
	SystemSI_IVFFLAT_TblCol_Metadata_key = "__mo_index_key"
	SystemSI_IVFFLAT_TblCol_Metadata_val = "__mo_index_val"

	// IVF_FLAT Centroids - Column names
	SystemSI_IVFFLAT_TblCol_Centroids_version  = "__mo_index_centroid_version"
	SystemSI_IVFFLAT_TblCol_Centroids_id       = "__mo_index_centroid_id"
	SystemSI_IVFFLAT_TblCol_Centroids_centroid = "__mo_index_centroid"

	// IVF_FLAT Entries - Column names
	SystemSI_IVFFLAT_TblCol_Entries_version = "__mo_index_centroid_fk_version"
	SystemSI_IVFFLAT_TblCol_Entries_id      = "__mo_index_centroid_fk_id"
	SystemSI_IVFFLAT_TblCol_Entries_pk      = IndexTablePrimaryColName
	SystemSI_IVFFLAT_TblCol_Entries_entry   = "__mo_index_centroid_fk_entry"
)

const (
	// default database id for catalog
	MO_CATALOG_ID  = 1
	MO_DATABASE_ID = 1
	MO_TABLES_ID   = 2
	MO_COLUMNS_ID  = 3

	// MO_RESERVED_MAX is the max reserved table ID.
	MO_RESERVED_MAX = 100
)

// index use to update constraint
const (
	MO_TABLES_ALTER_TABLE       = 0
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
	MO_DATABASE_CPKEY_IDX            = 9

	MO_TABLES_REL_ID_IDX          = 0
	MO_TABLES_REL_NAME_IDX        = 1
	MO_TABLES_RELDATABASE_IDX     = 2
	MO_TABLES_RELDATABASE_ID_IDX  = 3
	MO_TABLES_RELPERSISTENCE_IDX  = 4
	MO_TABLES_RELKIND_IDX         = 5
	MO_TABLES_REL_COMMENT_IDX     = 6
	MO_TABLES_REL_CREATESQL_IDX   = 7
	MO_TABLES_CREATED_TIME_IDX    = 8
	MO_TABLES_CREATOR_IDX         = 9
	MO_TABLES_OWNER_IDX           = 10
	MO_TABLES_ACCOUNT_ID_IDX      = 11
	MO_TABLES_PARTITIONED_IDX     = 12
	MO_TABLES_PARTITION_INFO_IDX  = 13
	MO_TABLES_VIEWDEF_IDX         = 14
	MO_TABLES_CONSTRAINT_IDX      = 15
	MO_TABLES_VERSION_IDX         = 16
	MO_TABLES_CATALOG_VERSION_IDX = 17
	MO_TABLES_CPKEY_IDX           = 18

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
	MO_COLUMNS_ATT_SEQNUM_IDX            = 22
	MO_COLUMNS_ATT_ENUM_IDX              = 23
	MO_COLUMNS_ATT_CPKEY_IDX             = 24
	MO_COLUMNS_MAXIDX                    = MO_COLUMNS_ATT_CPKEY_IDX

	BLOCKMETA_ID_IDX            = 0
	BLOCKMETA_ENTRYSTATE_IDX    = 1
	BLOCKMETA_SORTED_IDX        = 2
	BLOCKMETA_METALOC_IDX       = 3
	BLOCKMETA_DELTALOC_IDX      = 4
	BLOCKMETA_COMMITTS_IDX      = 5
	BLOCKMETA_SEGID_IDX         = 6
	BLOCKMETA_MemTruncPoint_IDX = 7

	SKIP_ROWID_OFFSET = 2 //rowid and cpk occupied the first coluns in delete batch
)

// used for memengine and tae
// tae and memengine do not make the catalog into a table
// for its convenience, a conversion interface is provided to ensure easy use.
type CreateDatabase struct {
	DatabaseId  uint64
	Name        string
	CreateSql   string
	DatTyp      string
	Owner       uint32 // roleid
	Creator     uint32 // userid
	AccountId   uint32 // tenantid
	CreatedTime types.Timestamp
}

type CreateDatabaseReq struct {
	Bat  *batch.Batch
	Cmds []CreateDatabase
}

type DropDatabase struct {
	Id   uint64
	Name string
}

type DropDatabaseReq struct {
	Bat  *batch.Batch
	Cmds []DropDatabase
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
	Partitioned  int8
	Partition    string
	RelKind      string
	Viewdef      string
	Constraint   []byte
	Defs         []engine.TableDef
}

type CreateTableReq struct {
	TableBat  *batch.Batch
	ColumnBat []*batch.Batch
	Cmds      []CreateTable
}

func (t CreateTable) String() string {
	return fmt.Sprintf("{aid-%v,uid-%v,rid-%v}: %d-%s:%d-%s",
		t.AccountId, t.Creator, t.Owner, t.DatabaseId, t.DatabaseName, t.TableId, t.Name)
}

type DropTable struct {
	IsDrop       bool // true for Drop and false for Truncate
	Id           uint64
	NewId        uint64
	Name         string
	DatabaseId   uint64
	DatabaseName string
}

type DropTableReq struct {
	TableBat  *batch.Batch
	ColumnBat []*batch.Batch
	Cmds      []DropTable
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
		SystemDBAttr_CPKey,
	}
	MoDatabaseAllColsString  = strings.Join(append([]string{Row_ID}, MoDatabaseSchema...), ",")
	MoDatabaseAllQueryFormat = fmt.Sprintf(
		"select %s from `%s`.`%s` where %s = %%d and %s = %%q",
		MoDatabaseAllColsString, MO_CATALOG, MO_DATABASE,
		SystemDBAttr_AccID, SystemDBAttr_Name)

	MoDatabaseBatchQuery = fmt.Sprintf(
		"select %s from `%s`.`%s`",
		MoDatabaseAllColsString, MO_CATALOG, MO_DATABASE)

	MoDatabasesInEngineQueryFormat = fmt.Sprintf(
		"select %s from `%s`.`%s` where %s = %%d",
		SystemDBAttr_Name, MO_CATALOG, MO_DATABASE,
		SystemDBAttr_AccID)

	MoDatabaseRowidQueryFormat = fmt.Sprintf(
		"select %s from `%s`.`%s` where %s = %%d and %s = %%q",
		Row_ID, MO_CATALOG, MO_DATABASE,
		SystemDBAttr_AccID, SystemDBAttr_Name)

	MoDatabaseSchema_V1 = []string{
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
		SystemRelAttr_Partitioned,
		SystemRelAttr_Partition,
		SystemRelAttr_ViewDef,
		SystemRelAttr_Constraint,
		SystemRelAttr_Version,
		SystemRelAttr_CatalogVersion,
		SystemRelAttr_CPKey,
	}
	MoTablesAllColsString = strings.Replace(
		strings.Join(append([]string{Row_ID}, MoTablesSchema...), ","),
		SystemRelAttr_Constraint,
		fmt.Sprintf("`%s`", SystemRelAttr_Constraint),
		-1)
	MoTablesAllQueryFormat = fmt.Sprintf(
		"select %s from `%s`.`%s` where %s = %%d and %s = %%q and %s = %%q",
		MoTablesAllColsString, MO_CATALOG, MO_TABLES,
		SystemRelAttr_AccID, SystemRelAttr_DBName, SystemRelAttr_Name)

	MoTablesBatchQuery = fmt.Sprintf(
		"select %s from `%s`.`%s`",
		MoTablesAllColsString, MO_CATALOG, MO_TABLES)

	MoTablesInDBQueryFormat = fmt.Sprintf(
		"select %s from `%s`.`%s` where %s = %%d and %s = %%q",
		SystemRelAttr_Name, MO_CATALOG, MO_TABLES,
		SystemRelAttr_AccID, SystemRelAttr_DBName)

	MoTablesRowidQueryFormat = fmt.Sprintf(
		"select %s from `%s`.`%s` where %s = %%d and %s = %%q and %s = %%q",
		Row_ID, MO_CATALOG, MO_TABLES,
		SystemRelAttr_AccID, SystemRelAttr_DBName, SystemRelAttr_Name)

	MoTablesQueryNameById = fmt.Sprintf(
		"select %s,%s from `%s`.`%s` where %s = %%d and %s = %%d",
		SystemRelAttr_Name, SystemRelAttr_DBName, MO_CATALOG, MO_TABLES,
		SystemRelAttr_AccID, SystemRelAttr_ID)

	MoTablesSchema_V1 = []string{
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
		SystemRelAttr_Partitioned,
		SystemRelAttr_Partition,
		SystemRelAttr_ViewDef,
		SystemRelAttr_Constraint,
		SystemRelAttr_Version,
	}
	MoTablesSchema_V2 = []string{
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
		SystemRelAttr_Partitioned,
		SystemRelAttr_Partition,
		SystemRelAttr_ViewDef,
		SystemRelAttr_Constraint,
		SystemRelAttr_Version,
		SystemRelAttr_CatalogVersion,
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
		SystemColAttr_Seqnum,
		SystemColAttr_EnumValues,
		SystemColAttr_CPKey,
	}
	MoColumnsAllColsString  = strings.Join(append([]string{Row_ID}, MoColumnsSchema...), ",")
	MoColumnsAllQueryFormat = fmt.Sprintf(
		"select %s from `%s`.`%s` where %s = %%d and %s = %%q and %s = %%q and %s = %%d",
		MoColumnsAllColsString, MO_CATALOG, MO_COLUMNS,
		SystemColAttr_AccID, SystemColAttr_DBName, SystemColAttr_RelName, SystemColAttr_RelID)

	MoColumnsBatchQuery = fmt.Sprintf(
		"select %s from `%s`.`%s` order by %s, %s, %s",
		MoColumnsAllColsString, MO_CATALOG, MO_COLUMNS,
		SystemColAttr_AccID, SystemColAttr_DBName, SystemColAttr_RelName)

	MoColumnsRowidsQueryFormat = fmt.Sprintf(
		"select %s from `%s`.`%s` where %s = %%d and %s = %%q and %s = %%q and %s = %%d order by %s",
		Row_ID, MO_CATALOG, MO_COLUMNS,
		SystemColAttr_AccID, SystemColAttr_DBName, SystemColAttr_RelName, SystemColAttr_RelID, SystemColAttr_Num)

	MoColumnsSchema_V1 = []string{
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
		SystemColAttr_Seqnum,
	}
	MoColumnsSchema_V2 = []string{
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
		SystemColAttr_Seqnum,
		SystemColAttr_EnumValues,
	}
	MoTableMetaSchema = []string{
		BlockMeta_ID,
		BlockMeta_EntryState,
		BlockMeta_Sorted,
		BlockMeta_MetaLoc,
		BlockMeta_DeltaLoc,
		BlockMeta_CommitTs,
		BlockMeta_SegmentID,
		BlockMeta_MemTruncPoint,
	}
	MoTableMetaSchemaV1 = []string{
		BlockMeta_ID,
		BlockMeta_EntryState,
		BlockMeta_Sorted,
		BlockMeta_MetaLoc,
		BlockMeta_DeltaLoc,
		BlockMeta_CommitTs,
		BlockMeta_SegmentID,
	}
	MoDatabaseTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),      // dat_id
		types.New(types.T_varchar, 5000, 0),  // datname
		types.New(types.T_varchar, 5000, 0),  // dat_catalog_name
		types.New(types.T_varchar, 5000, 0),  // dat_createsql
		types.New(types.T_uint32, 0, 0),      // owner
		types.New(types.T_uint32, 0, 0),      // creator
		types.New(types.T_timestamp, 0, 0),   // created_time
		types.New(types.T_uint32, 0, 0),      // account_id
		types.New(types.T_varchar, 32, 0),    // dat_type
		types.New(types.T_varchar, 65535, 0), // cpkey
	}
	MoDatabaseTypes_V1 = []types.Type{
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
		types.New(types.T_uint64, 0, 0),      // rel_id
		types.New(types.T_varchar, 5000, 0),  // relname
		types.New(types.T_varchar, 5000, 0),  // reldatabase
		types.New(types.T_uint64, 0, 0),      // reldatabase_id
		types.New(types.T_varchar, 5000, 0),  // relpersistence
		types.New(types.T_varchar, 5000, 0),  // relkind
		types.New(types.T_varchar, 5000, 0),  // rel_comment
		types.New(types.T_text, 0, 0),        // rel_createsql
		types.New(types.T_timestamp, 0, 0),   // created_time
		types.New(types.T_uint32, 0, 0),      // creator
		types.New(types.T_uint32, 0, 0),      // owner
		types.New(types.T_uint32, 0, 0),      // account_id
		types.New(types.T_int8, 0, 0),        // partitioned
		types.New(types.T_blob, 0, 0),        // partition_info
		types.New(types.T_varchar, 5000, 0),  // viewdef
		types.New(types.T_varchar, 5000, 0),  // constraint
		types.New(types.T_uint32, 0, 0),      // schema_version
		types.New(types.T_uint32, 0, 0),      // schema_catalog_version
		types.New(types.T_varchar, 65535, 0), // cpkey
	}
	MoTablesTypes_V1 = []types.Type{
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
		types.New(types.T_int8, 0, 0),       // partitioned
		types.New(types.T_blob, 0, 0),       // partition_info
		types.New(types.T_varchar, 5000, 0), // viewdef
		types.New(types.T_varchar, 5000, 0), // constraint
		types.New(types.T_uint32, 0, 0),     // schema_version
	}
	MoTablesTypes_V2 = []types.Type{
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
		types.New(types.T_int8, 0, 0),       // partitioned
		types.New(types.T_blob, 0, 0),       // partition_info
		types.New(types.T_varchar, 5000, 0), // viewdef
		types.New(types.T_varchar, 5000, 0), // constraint
		types.New(types.T_uint32, 0, 0),     // schema_version
		types.New(types.T_uint32, 0, 0),     // schema_catalog_version
	}
	MoColumnsTypes = []types.Type{
		types.New(types.T_varchar, 256, 0),                 // att_uniq_name
		types.New(types.T_uint32, 0, 0),                    // account_id
		types.New(types.T_uint64, 0, 0),                    // att_database_id
		types.New(types.T_varchar, 256, 0),                 // att_database
		types.New(types.T_uint64, 0, 0),                    // att_relname_id
		types.New(types.T_varchar, 256, 0),                 // att_relname
		types.New(types.T_varchar, 256, 0),                 // attname
		types.New(types.T_varchar, 256, 0),                 // atttyp
		types.New(types.T_int32, 0, 0),                     // attnum
		types.New(types.T_int32, 0, 0),                     // att_length
		types.New(types.T_int8, 0, 0),                      // attnotnull
		types.New(types.T_int8, 0, 0),                      // atthasdef
		types.New(types.T_varchar, 2048, 0),                // att_default
		types.New(types.T_int8, 0, 0),                      // attisdropped
		types.New(types.T_char, 1, 0),                      // att_constraint_type
		types.New(types.T_int8, 0, 0),                      // att_is_unsigned
		types.New(types.T_int8, 0, 0),                      // att_is_auto_increment
		types.New(types.T_varchar, 2048, 0),                // att_comment
		types.New(types.T_int8, 0, 0),                      // att_is_hidden
		types.New(types.T_int8, 0, 0),                      // att_has_update
		types.New(types.T_varchar, 2048, 0),                // att_update
		types.New(types.T_int8, 0, 0),                      // att_is_clusterby
		types.New(types.T_uint16, 0, 0),                    // att_seqnum
		types.New(types.T_varchar, types.MaxVarcharLen, 0), // att_enum
		types.New(types.T_varchar, 65535, 0),               // cpkey
	}
	MoColumnsTypes_V1 = []types.Type{
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
		types.New(types.T_uint16, 0, 0),     // att_seqnum
	}
	MoColumnsTypes_V2 = []types.Type{
		types.New(types.T_varchar, 256, 0),                 // att_uniq_name
		types.New(types.T_uint32, 0, 0),                    // account_id
		types.New(types.T_uint64, 0, 0),                    // att_database_id
		types.New(types.T_varchar, 256, 0),                 // att_database
		types.New(types.T_uint64, 0, 0),                    // att_relname_id
		types.New(types.T_varchar, 256, 0),                 // att_relname
		types.New(types.T_varchar, 256, 0),                 // attname
		types.New(types.T_varchar, 256, 0),                 // atttyp
		types.New(types.T_int32, 0, 0),                     // attnum
		types.New(types.T_int32, 0, 0),                     // att_length
		types.New(types.T_int8, 0, 0),                      // attnotnull
		types.New(types.T_int8, 0, 0),                      // atthasdef
		types.New(types.T_varchar, 2048, 0),                // att_default
		types.New(types.T_int8, 0, 0),                      // attisdropped
		types.New(types.T_char, 1, 0),                      // att_constraint_type
		types.New(types.T_int8, 0, 0),                      // att_is_unsigned
		types.New(types.T_int8, 0, 0),                      // att_is_auto_increment
		types.New(types.T_varchar, 2048, 0),                // att_comment
		types.New(types.T_int8, 0, 0),                      // att_is_hidden
		types.New(types.T_int8, 0, 0),                      // att_has_update
		types.New(types.T_varchar, 2048, 0),                // att_update
		types.New(types.T_int8, 0, 0),                      // att_is_clusterby
		types.New(types.T_uint16, 0, 0),                    // att_seqnum
		types.New(types.T_varchar, types.MaxVarcharLen, 0), // att_enum
	}
	MoTableMetaTypes = []types.Type{
		types.New(types.T_Blockid, 0, 0),                   // block_id
		types.New(types.T_bool, 0, 0),                      // entry_state, true for appendable
		types.New(types.T_bool, 0, 0),                      // sorted, true for sorted by primary key
		types.New(types.T_varchar, types.MaxVarcharLen, 0), // meta_loc
		types.New(types.T_varchar, types.MaxVarcharLen, 0), // delta_loc
		types.New(types.T_TS, 0, 0),                        // committs
		types.New(types.T_uuid, 0, 0),                      // segment_id
		types.New(types.T_TS, 0, 0),                        // flush_point
	}
	MoTableMetaTypesV1 = []types.Type{
		types.New(types.T_Blockid, 0, 0),                   // block_id
		types.New(types.T_bool, 0, 0),                      // entry_state, true for appendable
		types.New(types.T_bool, 0, 0),                      // sorted, true for sorted by primary key
		types.New(types.T_varchar, types.MaxVarcharLen, 0), // meta_loc
		types.New(types.T_varchar, types.MaxVarcharLen, 0), // delta_loc
		types.New(types.T_TS, 0, 0),                        // committs
		types.New(types.T_uuid, 0, 0),                      // segment_id
	}
	MoTablesIdxs = []uint16{
		MO_TABLES_REL_ID_IDX,
		MO_TABLES_REL_NAME_IDX,
		MO_TABLES_RELDATABASE_IDX,
		MO_TABLES_RELDATABASE_ID_IDX,
		MO_TABLES_RELPERSISTENCE_IDX,
		MO_TABLES_RELKIND_IDX,
		MO_TABLES_REL_COMMENT_IDX,
		MO_TABLES_REL_CREATESQL_IDX,
		MO_TABLES_CREATED_TIME_IDX,
		MO_TABLES_CREATOR_IDX,
		MO_TABLES_OWNER_IDX,
		MO_TABLES_ACCOUNT_ID_IDX,
		MO_TABLES_PARTITIONED_IDX,
		MO_TABLES_PARTITION_INFO_IDX,
		MO_TABLES_VIEWDEF_IDX,
		MO_TABLES_CONSTRAINT_IDX,
		MO_TABLES_VERSION_IDX,
		MO_TABLES_CATALOG_VERSION_IDX,
	}
	MoColumnsIdxs = []uint16{
		MO_COLUMNS_ATT_UNIQ_NAME_IDX,
		MO_COLUMNS_ACCOUNT_ID_IDX,
		MO_COLUMNS_ATT_DATABASE_ID_IDX,
		MO_COLUMNS_ATT_DATABASE_IDX,
		MO_COLUMNS_ATT_RELNAME_ID_IDX,
		MO_COLUMNS_ATT_RELNAME_IDX,
		MO_COLUMNS_ATTNAME_IDX,
		MO_COLUMNS_ATTTYP_IDX,
		MO_COLUMNS_ATTNUM_IDX,
		MO_COLUMNS_ATT_LENGTH_IDX,
		MO_COLUMNS_ATTNOTNULL_IDX,
		MO_COLUMNS_ATTHASDEF_IDX,
		MO_COLUMNS_ATT_DEFAULT_IDX,
		MO_COLUMNS_ATTISDROPPED_IDX,
		MO_COLUMNS_ATT_CONSTRAINT_TYPE_IDX,
		MO_COLUMNS_ATT_IS_UNSIGNED_IDX,
		MO_COLUMNS_ATT_IS_AUTO_INCREMENT_IDX,
		MO_COLUMNS_ATT_COMMENT_IDX,
		MO_COLUMNS_ATT_IS_HIDDEN_IDX,
		MO_COLUMNS_ATT_HAS_UPDATE_IDX,
		MO_COLUMNS_ATT_UPDATE_IDX,
		MO_COLUMNS_ATT_IS_CLUSTERBY,
		MO_COLUMNS_ATT_SEQNUM_IDX,
		MO_COLUMNS_ATT_ENUM_IDX,
	}
)

var (
	QueryResultPath     string
	QueryResultMetaPath string
	QueryResultMetaDir  string
	//ProfileDir holds all profiles dumped by the runtime/pprof
	ProfileDir string
	TraceDir   string
)

func init() {
	QueryResultPath = fileservice.JoinPath(defines.SharedFileServiceName, "/query_result/%s_%s_%d.blk")
	QueryResultMetaPath = fileservice.JoinPath(defines.SharedFileServiceName, "/query_result_meta/%s_%s.blk")
	QueryResultMetaDir = fileservice.JoinPath(defines.SharedFileServiceName, "/query_result_meta")
	ProfileDir = fileservice.JoinPath(defines.ETLFileServiceName, "/profile")
	TraceDir = fileservice.JoinPath(defines.ETLFileServiceName, "/trace")
}

type Meta struct {
	QueryId       [16]byte
	Statement     string
	AccountId     uint32
	RoleId        uint32
	ResultPath    string
	CreateTime    types.Timestamp
	ResultSize    float64
	Columns       string
	Tables        string
	UserId        uint32
	ExpiredTime   types.Timestamp
	Plan          string
	Ast           string
	ColumnMap     string
	SaveRowCount  uint64
	QueryRowCount uint64
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
		types.New(types.T_uint64, 0, 0),    // SavedRowCount
		types.New(types.T_uint64, 0, 0),    // QueryRowCount
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
		"savedRowCount", //count of rows saved in the query result
		"queryRowCount", //count of rows generated by the query
	}
)

const (
	QUERY_ID_IDX        = 0
	STATEMENT_IDX       = 1
	ACCOUNT_ID_IDX      = 2
	ROLE_ID_IDX         = 3
	RESULT_PATH_IDX     = 4
	CREATE_TIME_IDX     = 5
	RESULT_SIZE_IDX     = 6
	COLUMNS_IDX         = 7
	TABLES_IDX          = 8
	USER_ID_IDX         = 9
	EXPIRED_TIME_IDX    = 10
	PLAN_IDX            = 11
	AST_IDX             = 12
	COLUMN_MAP_IDX      = 13
	SAVED_ROW_COUNT_IDX = 14
	QUERY_ROW_COUNT_IDX = 15
)
