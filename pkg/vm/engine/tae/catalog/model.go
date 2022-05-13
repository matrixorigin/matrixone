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
	SystemTable_DB_Name      = "mo_databases"
	SystemTable_Table_Name   = "mo_tables"
	SystemTable_Columns_Name = "mo_columns"
	SystemTable_DB_ID        = uint64(1)
	SystemTable_Table_ID     = uint64(1)
	SystemTable_Columns_ID   = uint64(1)
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
	SystemDBSchema.AppendCol("datName", t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemDBSchema.AppendCol("dat_catalog_name", t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemDBSchema.AppendCol("dat_createsql", t)

	SystemTableSchema = NewEmptySchema(SystemTable_Table_Name)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemTableSchema.AppendCol("relname", t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemTableSchema.AppendCol("reldatabase", t)
	t = types.Type{
		Oid:   types.T_char,
		Size:  1,
		Width: 8,
	}
	SystemTableSchema.AppendCol("relpersistence", t)
	t = types.Type{
		Oid:   types.T_char,
		Size:  1,
		Width: 8,
	}
	SystemTableSchema.AppendCol("relkind", t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemTableSchema.AppendCol("rel_comment", t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemTableSchema.AppendCol("rel_createsql", t)

	SystemColumnSchema = NewEmptySchema(SystemTable_Columns_Name)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemColumnSchema.AppendCol("att_database", t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemColumnSchema.AppendCol("att_relname", t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemColumnSchema.AppendCol("att_name", t)
	t = types.Type{
		Oid:   types.T_uint32,
		Size:  4,
		Width: 32,
	}
	SystemColumnSchema.AppendCol("attyp", t)
	t = types.Type{
		Oid:   types.T_uint32,
		Size:  4,
		Width: 32,
	}
	SystemColumnSchema.AppendCol("attrnum", t)
	t = types.Type{
		Oid:   types.T_uint32,
		Size:  4,
		Width: 32,
	}
	SystemColumnSchema.AppendCol("att_length", t)
	t = types.Type{
		Oid:   types.T_int8,
		Size:  1,
		Width: 8,
	}
	SystemColumnSchema.AppendCol("attnotnull", t)
	t = types.Type{
		Oid:   types.T_int8,
		Size:  1,
		Width: 8,
	}
	SystemColumnSchema.AppendCol("atthasdef", t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemColumnSchema.AppendCol("att_default", t)
	t = types.Type{
		Oid:   types.T_int8,
		Size:  1,
		Width: 8,
	}
	SystemColumnSchema.AppendCol("attisdropped", t)
	t = types.Type{
		Oid:   types.T_char,
		Size:  1,
		Width: 8,
	}
	SystemColumnSchema.AppendCol("att_constraint_type", t)
	t = types.Type{
		Oid:   types.T_int8,
		Size:  1,
		Width: 8,
	}
	SystemColumnSchema.AppendCol("att_is_unsigned", t)
	t = types.Type{
		Oid:   types.T_int8,
		Size:  1,
		Width: 8,
	}
	SystemColumnSchema.AppendCol("att_is_auto_increment", t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	SystemColumnSchema.AppendCol("att_comment", t)
	t = types.Type{
		Oid:   types.T_int8,
		Size:  1,
		Width: 8,
	}
	SystemColumnSchema.AppendCol("att_is_hidden", t)

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
