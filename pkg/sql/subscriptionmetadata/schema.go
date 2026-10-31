// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package subscriptionmetadata

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// TablesSchema returns the ordered columns and types of the subscription metadata table function.
func TablesSchema() ([]string, []types.Type) {
	return []string{
		"account_id",
		"rel_id",
		"relname",
		"reldatabase",
		"reldatabase_id",
		"relkind",
		"rel_createsql",
		"created_time",
		"partitioned",
		"rel_comment",
		"extra_info",
		"rel_logical_id",
		"owner",
		"publisher_account_id",
	}, []types.Type{
		catalog.MoTablesTypes[catalog.MO_TABLES_ACCOUNT_ID_IDX],
		catalog.MoTablesTypes[catalog.MO_TABLES_REL_ID_IDX],
		catalog.MoTablesTypes[catalog.MO_TABLES_REL_NAME_IDX],
		catalog.MoTablesTypes[catalog.MO_TABLES_RELDATABASE_IDX],
		catalog.MoTablesTypes[catalog.MO_TABLES_RELDATABASE_ID_IDX],
		catalog.MoTablesTypes[catalog.MO_TABLES_RELKIND_IDX],
		catalog.MoTablesTypes[catalog.MO_TABLES_REL_CREATESQL_IDX],
		catalog.MoTablesTypes[catalog.MO_TABLES_CREATED_TIME_IDX],
		catalog.MoTablesTypes[catalog.MO_TABLES_PARTITIONED_IDX],
		catalog.MoTablesTypes[catalog.MO_TABLES_REL_COMMENT_IDX],
		catalog.MoTablesTypes[catalog.MO_TABLES_EXTRA_INFO_IDX],
		catalog.MoTablesTypes[catalog.MO_TABLES_LOGICAL_ID_IDX],
		catalog.MoTablesTypes[catalog.MO_TABLES_OWNER_IDX],
		types.New(types.T_uint32, 0, 0),
	}
}

// ColumnsSchema returns the ordered columns and types of the subscription metadata table function.
func ColumnsSchema() ([]string, []types.Type) {
	return []string{
		"account_id",
		"att_database_id",
		"att_database",
		"att_relname_id",
		"att_relname",
		"attname",
		"atttyp",
		"attnum",
		"attnotnull",
		"att_default",
		"att_constraint_type",
		"att_is_auto_increment",
		"att_comment",
		"att_is_hidden",
		"attr_enum",
		"attr_has_generated",
		"attr_generated",
		"key_priority",
		"rel_id",
		"relkind",
		"rel_createsql",
		"partitioned",
		"extra_info",
		"rel_logical_id",
		"table_owner",
		"att_is_unsigned",
	}, []types.Type{
		catalog.MoColumnsTypes[catalog.MO_COLUMNS_ACCOUNT_ID_IDX],
		catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_DATABASE_ID_IDX],
		catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_DATABASE_IDX],
		catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_RELNAME_ID_IDX],
		catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_RELNAME_IDX],
		catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATTNAME_IDX],
		catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATTTYP_IDX],
		catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATTNUM_IDX],
		catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATTNOTNULL_IDX],
		catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_DEFAULT_IDX],
		catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_CONSTRAINT_TYPE_IDX],
		catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_IS_AUTO_INCREMENT_IDX],
		catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_COMMENT_IDX],
		catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_IS_HIDDEN_IDX],
		catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_ENUM_IDX],
		catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_HAS_GENERATED_IDX],
		catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_GENERATED_IDX],
		types.New(types.T_int64, 0, 0),
		catalog.MoTablesTypes[catalog.MO_TABLES_REL_ID_IDX],
		catalog.MoTablesTypes[catalog.MO_TABLES_RELKIND_IDX],
		catalog.MoTablesTypes[catalog.MO_TABLES_REL_CREATESQL_IDX],
		catalog.MoTablesTypes[catalog.MO_TABLES_PARTITIONED_IDX],
		catalog.MoTablesTypes[catalog.MO_TABLES_EXTRA_INFO_IDX],
		catalog.MoTablesTypes[catalog.MO_TABLES_LOGICAL_ID_IDX],
		catalog.MoTablesTypes[catalog.MO_TABLES_OWNER_IDX],
		catalog.MoColumnsTypes[catalog.MO_COLUMNS_ATT_IS_UNSIGNED_IDX],
	}
}
