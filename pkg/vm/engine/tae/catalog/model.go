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

package catalog

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type EntryType uint8

const (
	ETDatabase EntryType = iota
	ETTable
	ETSegment
	ETBlock
	ETColDef
)

var (
	PhyAddrColumnType types.Type
)

const (
	PhyAddrColumnName    = "PADDR"
	PhyAddrColumnComment = "Physical address"
	SortKeyNamePrefx     = "_SORT_"

	AttrCommitTs = "commit_ts"
	AttrAborted  = "aborted"

	TenantSysID = uint32(0)

	SystemDBID               = uint64(1)
	SystemDBName             = "mo_catalog"
	CatalogName              = "taec"
	SystemTable_DB_Name      = "mo_database"
	SystemTable_Table_Name   = "mo_tables"
	SystemTable_Columns_Name = "mo_columns"
	SystemTable_DB_ID        = uint64(1)
	SystemTable_Table_ID     = uint64(2)
	SystemTable_Columns_ID   = uint64(3)
	SystemSegment_DB_ID      = uint64(101)
	SystemSegment_Table_ID   = uint64(102)
	SystemSegment_Columns_ID = uint64(103)
	SystemBlock_DB_ID        = uint64(201)
	SystemBlock_Table_ID     = uint64(202)
	SystemBlock_Columns_ID   = uint64(203)

	SystemCatalogName  = "def"
	SystemPersistRel   = "p"
	SystemTransientRel = "t"

	SystemOrdinaryRel     = "r"
	SystemIndexRel        = "i"
	SystemSequenceRel     = "S"
	SystemViewRel         = "v"
	SystemMaterializedRel = "m"
	SystemExternalRel     = "e"

	SystemColPKConstraint = "p"
	SystemColNoConstraint = "n"
)

const (
	SystemDBAttr_ID          = "dat_id"
	SystemDBAttr_Name        = "datname"
	SystemDBAttr_CatalogName = "dat_catalog_name"
	SystemDBAttr_CreateSQL   = "dat_createsql"
	SystemDBAttr_Owner       = "owner"
	SystemDBAttr_Creator     = "creator"
	SystemDBAttr_CreateAt    = "created_time"
	SystemDBAttr_AccID       = "account_id"

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
	SystemColAttr_IsHidden        = "att_is_hidden"
	SystemColAttr_Comment         = "att_comment"
)

var SystemDBSchema *Schema
var SystemTableSchema *Schema
var SystemColumnSchema *Schema

const (
	ModelSchemaName   = "_ModelSchema"
	ModelAttrET       = "ET"
	ModelAttrID       = "ID"
	ModelAttrName     = "NAME"
	ModelAttrTS       = "TS"
	ModelAttrOpT      = "OPT"
	ModelAttrLogIdx   = "LOGIDX"
	ModelAttrInfo     = "INFO"
	ModelAttrParentID = "PARENTID"
)

func init() {
	var err error
	PhyAddrColumnType = types.T_Rowid.ToType()

	ti8 := types.T_int8.ToType()
	ti32 := types.T_int32.ToType()
	tu32 := types.T_uint32.ToType()
	tu64 := types.T_uint64.ToType()
	ttimestamp := types.T_timestamp.ToType()
	tvarchar := types.T_varchar.ToType()
	tsinglechar := types.T_char.ToType()
	ttext := types.T_blob.ToType()

	/*

		SystemDBSchema

	*/

	SystemDBSchema = NewEmptySchema(SystemTable_DB_Name)
	if err = SystemDBSchema.AppendPKCol(SystemDBAttr_ID, tu64, 0); err != nil {
		panic(err)
	}
	if err = SystemDBSchema.AppendCol(SystemDBAttr_Name, tvarchar); err != nil {
		panic(err)
	}
	if err = SystemDBSchema.AppendCol(SystemDBAttr_CatalogName, tvarchar); err != nil {
		panic(err)
	}
	if err = SystemDBSchema.AppendCol(SystemDBAttr_CreateSQL, tvarchar); err != nil {
		panic(err)
	}
	if err = SystemDBSchema.AppendCol(SystemDBAttr_Owner, tu32); err != nil {
		panic(err)
	}
	if err = SystemDBSchema.AppendCol(SystemDBAttr_Creator, tu32); err != nil {
		panic(err)
	}
	if err = SystemDBSchema.AppendCol(SystemDBAttr_CreateAt, ttimestamp); err != nil {
		panic(err)
	}
	if err = SystemDBSchema.AppendCol(SystemDBAttr_AccID, tu32); err != nil {
		panic(err)
	}

	if err = SystemDBSchema.Finalize(true); err != nil {
		panic(err)
	}

	/*

		SystemTableSchema

	*/

	SystemTableSchema = NewEmptySchema(SystemTable_Table_Name)
	if err = SystemTableSchema.AppendPKCol(SystemRelAttr_ID, tu64, 0); err != nil {
		panic(err)
	}
	if err = SystemTableSchema.AppendCol(SystemRelAttr_Name, tvarchar); err != nil {
		panic(err)
	}
	if err = SystemTableSchema.AppendCol(SystemRelAttr_DBName, tvarchar); err != nil {
		panic(err)
	}
	if err = SystemTableSchema.AppendCol(SystemRelAttr_DBID, tu64); err != nil {
		panic(err)
	}
	if err = SystemTableSchema.AppendCol(SystemRelAttr_Persistence, tsinglechar); err != nil {
		panic(err)
	}
	if err = SystemTableSchema.AppendCol(SystemRelAttr_Kind, tsinglechar); err != nil {
		panic(err)
	}
	if err = SystemTableSchema.AppendCol(SystemRelAttr_Comment, tvarchar); err != nil {
		panic(err)
	}
	if err = SystemTableSchema.AppendCol(SystemRelAttr_CreateSQL, tvarchar); err != nil {
		panic(err)
	}
	if err = SystemTableSchema.AppendCol(SystemRelAttr_CreateAt, ttimestamp); err != nil {
		panic(err)
	}
	if err = SystemTableSchema.AppendCol(SystemRelAttr_Creator, tu32); err != nil {
		panic(err)
	}
	if err = SystemTableSchema.AppendCol(SystemRelAttr_Owner, tu32); err != nil {
		panic(err)
	}
	if err = SystemTableSchema.AppendCol(SystemRelAttr_AccID, tu32); err != nil {
		panic(err)
	}
	if err = SystemTableSchema.AppendCol(SystemRelAttr_Partition, ttext); err != nil {
		panic(err)
	}
	if err = SystemTableSchema.Finalize(true); err != nil {
		panic(err)
	}

	/*

		SystemColumnSchema

	*/

	SystemColumnSchema = NewEmptySchema(SystemTable_Columns_Name)
	if err = SystemColumnSchema.AppendColDef(&ColDef{
		Name:    SystemColAttr_UniqName,
		Type:    tvarchar,
		Hidden:  true,
		SortIdx: 0,
		SortKey: true,
		Primary: true,
	}); err != nil {
		panic(err)
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_AccID, tu32); err != nil {
		panic(err)
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_DBID, tu64); err != nil {
		panic(err)
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_DBName, tvarchar); err != nil {
		panic(err)
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_RelID, tu64); err != nil {
		panic(err)
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_RelName, tvarchar); err != nil {
		panic(err)
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_Name, tvarchar); err != nil {
		panic(err)
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_Type, ti32); err != nil {
		panic(err)
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_Num, ti32); err != nil {
		panic(err)
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_Length, ti32); err != nil {
		panic(err)
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_NullAbility, ti8); err != nil {
		panic(err)
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_HasExpr, ti8); err != nil {
		panic(err)
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_DefaultExpr, tvarchar); err != nil {
		panic(err)
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_IsDropped, ti8); err != nil {
		panic(err)
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_ConstraintType, tsinglechar); err != nil {
		panic(err)
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_IsUnsigned, ti8); err != nil {
		panic(err)
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_IsAutoIncrement, ti8); err != nil {
		panic(err)
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_Comment, tvarchar); err != nil {
		panic(err)
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_IsHidden, ti8); err != nil {
		panic(err)
	}
	if err = SystemColumnSchema.Finalize(true); err != nil {
		panic(err)
	}
}
