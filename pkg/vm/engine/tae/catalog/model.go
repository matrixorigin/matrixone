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

const (
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

	SystemColAttr_Name            = "att_name"
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

// UINT8 UINT64  VARCHAR UINT64  INT8   CHAR    VARCHAR    UINT64
//  ET  |  ID  |  NAME  |  TS  | OPT | LOGIDX |  INFO   | PARENTID |
var ModelSchema *Schema
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
	SystemDBSchema = NewEmptySchema(SystemTable_DB_Name)
	t := types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemDBSchema.AppendCol(SystemDBAttr_Name, t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemDBSchema.AppendCol(SystemDBAttr_CatalogName, t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemDBSchema.AppendCol(SystemDBAttr_CreateSQL, t)

	SystemTableSchema = NewEmptySchema(SystemTable_Table_Name)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemTableSchema.AppendCol(SystemRelAttr_Name, t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemTableSchema.AppendCol(SystemRelAttr_DBName, t)
	t = types.Type{
		Oid:   types.T_char,
		Size:  1,
		Width: 8,
	}
	SystemTableSchema.AppendCol(SystemRelAttr_Persistence, t)
	t = types.Type{
		Oid:   types.T_char,
		Size:  1,
		Width: 8,
	}
	SystemTableSchema.AppendCol(SystemRelAttr_Kind, t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemTableSchema.AppendCol(SystemRelAttr_Comment, t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemTableSchema.AppendCol(SystemDBAttr_CreateSQL, t)

	SystemColumnSchema = NewEmptySchema(SystemTable_Columns_Name)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemColumnSchema.AppendCol(SystemColAttr_DBName, t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemColumnSchema.AppendCol(SystemColAttr_RelName, t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemColumnSchema.AppendCol(SystemColAttr_Name, t)
	t = types.Type{
		Oid:   types.T_uint32,
		Size:  4,
		Width: 32,
	}
	SystemColumnSchema.AppendCol(SystemColAttr_Type, t)
	t = types.Type{
		Oid:   types.T_uint32,
		Size:  4,
		Width: 32,
	}
	SystemColumnSchema.AppendCol(SystemColAttr_Num, t)
	t = types.Type{
		Oid:   types.T_uint32,
		Size:  4,
		Width: 32,
	}
	SystemColumnSchema.AppendCol(SystemColAttr_Length, t)
	t = types.Type{
		Oid:   types.T_int8,
		Size:  1,
		Width: 8,
	}
	SystemColumnSchema.AppendCol(SystemColAttr_NullAbility, t)
	t = types.Type{
		Oid:   types.T_int8,
		Size:  1,
		Width: 8,
	}
	SystemColumnSchema.AppendCol(SystemColAttr_HasExpr, t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemColumnSchema.AppendCol(SystemColAttr_DefaultExpr, t)
	t = types.Type{
		Oid:   types.T_int8,
		Size:  1,
		Width: 8,
	}
	SystemColumnSchema.AppendCol(SystemColAttr_IsDropped, t)
	t = types.Type{
		Oid:   types.T_char,
		Size:  1,
		Width: 8,
	}
	SystemColumnSchema.AppendCol(SystemColAttr_ConstraintType, t)
	t = types.Type{
		Oid:   types.T_int8,
		Size:  1,
		Width: 8,
	}
	SystemColumnSchema.AppendCol(SystemColAttr_IsUnsigned, t)
	t = types.Type{
		Oid:   types.T_int8,
		Size:  1,
		Width: 8,
	}
	SystemColumnSchema.AppendCol(SystemColAttr_IsAutoIncrement, t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemColumnSchema.AppendCol(SystemColAttr_Comment, t)
	t = types.Type{
		Oid:   types.T_int8,
		Size:  1,
		Width: 8,
	}
	SystemColumnSchema.AppendCol(SystemColAttr_IsHidden, t)

	ModelSchema = NewEmptySchema(ModelSchemaName)
	t = types.Type{
		Oid:   types.T_uint8,
		Size:  1,
		Width: 8,
	}
	ModelSchema.AppendCol(ModelAttrET, t)
	t = types.Type{
		Oid:   types.T_uint64,
		Size:  8,
		Width: 64,
	}
	ModelSchema.AppendCol(ModelAttrID, t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	ModelSchema.AppendCol(ModelAttrName, t)
	t = types.Type{
		Oid:   types.T_uint64,
		Size:  8,
		Width: 64,
	}
	ModelSchema.AppendCol(ModelAttrTS, t)
	t = types.Type{
		Oid:   types.T_int8,
		Size:  1,
		Width: 8,
	}
	ModelSchema.AppendCol(ModelAttrOpT, t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	ModelSchema.AppendCol(ModelAttrLogIdx, t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	ModelSchema.AppendCol(ModelAttrInfo, t)
	t = types.Type{
		Oid:   types.T_uint64,
		Size:  8,
		Width: 64,
	}
	ModelSchema.AppendCol(ModelAttrParentID, t)
}
