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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	// default database name for catalog
	MO_CATALOG  = "mo_catalog"
	MO_DATABASE = "mo_database"
	MO_TABLES   = "mo_tables"
	MO_COLUMNS  = "mo_columns"
)

const (
	// default database id for catalog
	MO_CATALOG_ID  = 1
	MO_DATABASE_ID = 1
	MO_TABLES_ID   = 2
	MO_COLUMNS_ID  = 3
)

// column's index in catalog table
const (
	MO_DATABASE_DAT_ID_IDX   = 0
	MO_DATABASE_DAT_NAME_IDX = 1
	MO_TABLES_REL_ID_IDX     = 0
	MO_TABLES_REL_NAME_IDX   = 1
)

var (
	MoDatabaseSchema = []string{
		"dat_id",
		"datname",
		"dat_catalog_name",
		"dat_createsql",
		"owner",
		"creator",
		"created_time",
		"account_id",
	}
	MoTablesSchema = []string{
		"rel_id",
		"relname",
		"reldatabase",
		"reldatabase_id",
		"relpersistence",
		"relkind",
		"rel_comment",
		"rel_createsql",
		"created_time",
		"creator",
		"owner",
		"account_id",
	}
	MoColumnsSchema = []string{
		"att_uniq_name",
		"account_id",
		"att_database_id",
		"att_database",
		"att_relname_id",
		"att_relname",
		"attname",
		"atttyp",
		"attnum",
		"att_length",
		"attnotnull",
		"atthasdef",
		"att_default",
		"attisdropped",
		"att_constraint_type",
		"att_is_unsigned",
		"att_is_auto_increment",
		"att_comment",
		"att_is_hidden",
	}
	MoDatabaseTypes = []types.Type{
		types.New(types.T_uint64, 0, 0, 0),    // dat_id
		types.New(types.T_varchar, 100, 0, 0), // datname
		types.New(types.T_varchar, 100, 0, 0), // dat_catalog_name
		types.New(types.T_varchar, 100, 0, 0), // dat_createsql
		types.New(types.T_uint32, 0, 0, 0),    // owner
		types.New(types.T_uint32, 0, 0, 0),    // creator
		types.New(types.T_timestamp, 0, 0, 0), // created_time
		types.New(types.T_uint32, 0, 0, 0),    // account_id
	}
	MoTablesTypes = []types.Type{
		types.New(types.T_uint64, 0, 0, 0),    // rel_id
		types.New(types.T_varchar, 100, 0, 0), // relname
		types.New(types.T_varchar, 100, 0, 0), // reldatabase
		types.New(types.T_uint32, 0, 0, 0),    // reldatabase_id
		types.New(types.T_varchar, 100, 0, 0), // relpersistence
		types.New(types.T_varchar, 100, 0, 0), // relkind
		types.New(types.T_varchar, 100, 0, 0), // rel_comment
		types.New(types.T_varchar, 100, 0, 0), // rel_createsql
		types.New(types.T_timestamp, 0, 0, 0), // created_time
		types.New(types.T_uint32, 0, 0, 0),    // creator
		types.New(types.T_uint32, 0, 0, 0),    // owner
		types.New(types.T_uint32, 0, 0, 0),    // account_id
	}
	MoColumnsTypes = []types.Type{
		types.New(types.T_varchar, 256, 0, 0),  // att_uniq_name
		types.New(types.T_uint32, 0, 0, 0),     // account_id
		types.New(types.T_uint64, 0, 0, 0),     // att_database_id
		types.New(types.T_varchar, 256, 0, 0),  // att_database
		types.New(types.T_uint64, 0, 0, 0),     // att_relname_id
		types.New(types.T_varchar, 256, 0, 0),  // att_relname
		types.New(types.T_varchar, 256, 0, 0),  // attname
		types.New(types.T_int32, 0, 0, 0),      // atttyp
		types.New(types.T_int32, 0, 0, 0),      // attnum
		types.New(types.T_int32, 0, 0, 0),      // att_length
		types.New(types.T_int8, 0, 0, 0),       // attnotnull
		types.New(types.T_int8, 0, 0, 0),       // atthasdef
		types.New(types.T_varchar, 1024, 0, 0), // att_default
		types.New(types.T_int8, 0, 0, 0),       // attisdropped
		types.New(types.T_char, 1, 0, 0),       // att_constraint_type
		types.New(types.T_int8, 0, 0, 0),       // att_is_unsigned
		types.New(types.T_int8, 0, 0, 0),       // att_is_auto_increment
		types.New(types.T_varchar, 1024, 0, 0), // att_comment
		types.New(types.T_int8, 0, 0, 0),       // att_is_hidden

	}
	// used by memengine or tae
	MoDatabaseTableDefs = []engine.TableDef{}
	// used by memengine or tae
	MoTablesTableDefs = []engine.TableDef{}
	// used by memengine or tae
	MoColumnsTableDefs = []engine.TableDef{}
)
