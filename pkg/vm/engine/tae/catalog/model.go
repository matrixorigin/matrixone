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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const (
	PhyAddrColumnName    = catalog.Row_ID
	PhyAddrColumnComment = "Physical address"
	SortKeyNamePrefx     = "_SORT_"

	AttrRowID    = PhyAddrColumnName
	AttrCommitTs = "commit_time"
	AttrAborted  = "aborted"

	TenantSysID = uint32(0)
)

var SystemDBSchema *Schema
var SystemTableSchema *Schema
var SystemTableSchema_V1 *Schema
var SystemColumnSchema *Schema

var SystemSegment_DB_ID types.Uuid
var SystemSegment_Table_ID types.Uuid
var SystemSegment_Columns_ID types.Uuid
var SystemBlock_DB_ID types.Blockid
var SystemBlock_Table_ID types.Blockid
var SystemBlock_Columns_ID types.Blockid

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

	SystemSegment_DB_ID = types.Uuid{101}
	SystemSegment_Table_ID = types.Uuid{102}
	SystemSegment_Columns_ID = types.Uuid{103}
	SystemBlock_DB_ID = types.Blockid{101}
	SystemBlock_Table_ID = types.Blockid{102}
	SystemBlock_Columns_ID = types.Blockid{103}

	var err error

	SystemDBSchema = NewEmptySchema(catalog.MO_DATABASE)
	for i, colname := range catalog.MoDatabaseSchema {
		if i == 0 {
			if err = SystemDBSchema.AppendPKCol(colname, catalog.MoDatabaseTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err = SystemDBSchema.AppendCol(colname, catalog.MoDatabaseTypes[i]); err != nil {
				panic(err)
			}
		}
	}
	if err = SystemDBSchema.Finalize(true); err != nil {
		panic(err)
	}

	SystemTableSchema = NewEmptySchema(catalog.MO_TABLES)
	for i, colname := range catalog.MoTablesSchema {
		if i == 0 {
			if err = SystemTableSchema.AppendPKCol(colname, catalog.MoTablesTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err = SystemTableSchema.AppendCol(colname, catalog.MoTablesTypes[i]); err != nil {
				panic(err)
			}
		}
	}
	if err = SystemTableSchema.Finalize(true); err != nil {
		panic(err)
	}

	SystemTableSchema_V1 = NewEmptySchema(catalog.MO_TABLES + "_v1")
	for i, colname := range catalog.MoTablesSchema_V1 {
		if i == 0 {
			if err = SystemTableSchema.AppendPKCol(colname, catalog.MoTablesTypes_V1[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err = SystemTableSchema.AppendCol(colname, catalog.MoTablesTypes_V1[i]); err != nil {
				panic(err)
			}
		}
	}
	if err = SystemTableSchema.Finalize(true); err != nil {
		panic(err)
	}

	SystemColumnSchema = NewEmptySchema(catalog.MO_COLUMNS)
	for i, colname := range catalog.MoColumnsSchema {
		if i == 0 {
			if err = SystemColumnSchema.AppendPKCol(colname, catalog.MoColumnsTypes[i], 0); err != nil {
				panic(err)
			}
		} else {
			if err = SystemColumnSchema.AppendCol(colname, catalog.MoColumnsTypes[i]); err != nil {
				panic(err)
			}
		}
	}
	if err = SystemColumnSchema.Finalize(true); err != nil {
		panic(err)
	}
}
