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
	MoColumnsSchema = []string{}
)
