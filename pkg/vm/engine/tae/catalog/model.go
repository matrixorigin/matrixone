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

import "github.com/matrixorigin/matrixone/pkg/container/types"

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

	SystemColPKConstraint = "p"
	SystemColNoConstraint = "n"
)

const (
	SystemDBAttr_Name        = "datname"
	SystemDBAttr_CatalogName = "dat_catalog_name"
	SystemDBAttr_CreateSQL   = "dat_createsql"

	SystemRelAttr_Name        = "relname"
	SystemRelAttr_DBName      = "reldatabase"
	SystemRelAttr_Persistence = "relpersistence"
	SystemRelAttr_Kind        = "relkind"
	SystemRelAttr_Comment     = "rel_comment"
	SystemRelAttr_CreateSQL   = "rel_createsql"

	SystemColAttr_Name            = "attname"
	SystemColAttr_DBName          = "att_database"
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
	PhyAddrColumnType = types.Type{
		Oid:   types.T_decimal128,
		Size:  16,
		Width: 128,
	}

	SystemDBSchema = NewEmptySchema(SystemTable_DB_Name)
	t := types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	if err = SystemDBSchema.AppendPKCol(SystemDBAttr_Name, t, 0); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	if err = SystemDBSchema.AppendCol(SystemDBAttr_CatalogName, t); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	if err = SystemDBSchema.AppendCol(SystemDBAttr_CreateSQL, t); err != nil {
		panic(err)
	}
	if err = SystemDBSchema.Finalize(true); err != nil {
		panic(err)
	}

	SystemTableSchema = NewEmptySchema(SystemTable_Table_Name)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	if err = SystemTableSchema.AppendPKCol(SystemRelAttr_Name, t, 0); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	if err = SystemTableSchema.AppendCol(SystemRelAttr_DBName, t); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_char,
		Size:  1,
		Width: 8,
	}
	if err = SystemTableSchema.AppendCol(SystemRelAttr_Persistence, t); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_char,
		Size:  1,
		Width: 8,
	}
	if err = SystemTableSchema.AppendCol(SystemRelAttr_Kind, t); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	if err = SystemTableSchema.AppendCol(SystemRelAttr_Comment, t); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	if err = SystemTableSchema.AppendCol(SystemRelAttr_CreateSQL, t); err != nil {
		panic(err)
	}
	if err = SystemTableSchema.Finalize(true); err != nil {
		panic(err)
	}

	SystemColumnSchema = NewEmptySchema(SystemTable_Columns_Name)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_DBName, t); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_RelName, t); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	if err = SystemColumnSchema.AppendPKCol(SystemColAttr_Name, t, 0); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_int32,
		Size:  4,
		Width: 32,
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_Type, t); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_int32,
		Size:  4,
		Width: 32,
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_Num, t); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_int32,
		Size:  4,
		Width: 32,
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_Length, t); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_int8,
		Size:  1,
		Width: 8,
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_NullAbility, t); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_int8,
		Size:  1,
		Width: 8,
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_HasExpr, t); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_DefaultExpr, t); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_int8,
		Size:  1,
		Width: 8,
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_IsDropped, t); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_char,
		Size:  1,
		Width: 8,
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_ConstraintType, t); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_int8,
		Size:  1,
		Width: 8,
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_IsUnsigned, t); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_int8,
		Size:  1,
		Width: 8,
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_IsAutoIncrement, t); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_Comment, t); err != nil {
		panic(err)
	}
	t = types.Type{
		Oid:   types.T_int8,
		Size:  1,
		Width: 8,
	}
	if err = SystemColumnSchema.AppendCol(SystemColAttr_IsHidden, t); err != nil {
		panic(err)
	}
	if err = SystemColumnSchema.Finalize(true); err != nil {
		panic(err)
	}
}
