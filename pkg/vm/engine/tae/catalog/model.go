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

	TenantSysID              = uint32(0)
	SystemSegment_DB_ID      = uint64(101)
	SystemSegment_Table_ID   = uint64(102)
	SystemSegment_Columns_ID = uint64(103)
	SystemBlock_DB_ID        = uint64(201)
	SystemBlock_Table_ID     = uint64(202)
	SystemBlock_Columns_ID   = uint64(203)
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
