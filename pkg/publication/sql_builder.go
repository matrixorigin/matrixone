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

package publication

import (
	"fmt"
	"strings"
)

var PublicationSQLBuilder = publicationSQLBuilder{}

const (
	// Create snapshot SQL templates
	PublicationCreateSnapshotForAccountSqlTemplate  = `CREATE SNAPSHOT %s FOR ACCOUNT%s`
	PublicationCreateSnapshotForDatabaseSqlTemplate = `CREATE SNAPSHOT %s FOR DATABASE %s`
	PublicationCreateSnapshotForTableSqlTemplate    = `CREATE SNAPSHOT %s FOR TABLE %s %s`

	// Query mo_catalog tables SQL templates
	PublicationQueryMoTablesSqlTemplate = `SELECT ` +
		`rel_id, ` +
		`relname, ` +
		`reldatabase_id, ` +
		`reldatabase, ` +
		`rel_createsql, ` +
		`account_id ` +
		`FROM mo_catalog.mo_tables ` +
		`WHERE 1=1%s`

	PublicationQueryMoDatabasesSqlTemplate = `SELECT ` +
		`dat_id, ` +
		`datname, ` +
		`dat_createsql, ` +
		`account_id ` +
		`FROM mo_catalog.mo_databases ` +
		`WHERE 1=1%s`

	PublicationQueryMoColumnsSqlTemplate = `SELECT ` +
		`account_id, ` +
		`att_database_id, ` +
		`att_database, ` +
		`att_relname_id, ` +
		`att_relname, ` +
		`attname, ` +
		`atttyp, ` +
		`attnum, ` +
		`att_length, ` +
		`attnotnull, ` +
		`atthasdef, ` +
		`att_default, ` +
		`attisdropped, ` +
		`att_constraint_type, ` +
		`att_is_unsigned, ` +
		`att_is_auto_increment, ` +
		`att_comment, ` +
		`att_is_hidden, ` +
		`att_has_update, ` +
		`att_update, ` +
		`att_has_cluster_by, ` +
		`att_cluster_by, ` +
		`att_seqnum, ` +
		`att_enum_values ` +
		`FROM mo_catalog.mo_columns ` +
		`WHERE 1=1%s`

	// Object list SQL template
	PublicationObjectListSqlTemplate = `OBJECTLIST%s SNAPSHOT %s%s`

	// Get object SQL template
	PublicationGetObjectSqlTemplate = `GET OBJECT %s`

	// Drop snapshot SQL templates
	PublicationDropSnapshotSqlTemplate         = `DROP SNAPSHOT %s`
	PublicationDropSnapshotIfExistsSqlTemplate = `DROP SNAPSHOT IF EXISTS %s`

	// Query mo_ccpr_log SQL template
	PublicationQueryMoCcprLogSqlTemplate = `SELECT ` +
		`cn_uuid, ` +
		`iteration_state, ` +
		`iteration_lsn ` +
		`FROM mo_catalog.mo_ccpr_log ` +
		`WHERE task_id = %d`

	// Query snapshot TS SQL template
	PublicationQuerySnapshotTsSqlTemplate = `SELECT ` +
		`ts ` +
		`FROM mo_catalog.mo_snapshots ` +
		`WHERE sname = '%s'`

	// Update mo_ccpr_log SQL template
	PublicationUpdateMoCcprLogSqlTemplate = `UPDATE mo_catalog.mo_ccpr_log ` +
		`SET iteration_state = %d, ` +
		`iteration_lsn = %d, ` +
		`context = '%s' ` +
		`WHERE task_id = %d`
)

const (
	PublicationCreateSnapshotForAccountSqlTemplate_Idx = iota
	PublicationCreateSnapshotForDatabaseSqlTemplate_Idx
	PublicationCreateSnapshotForTableSqlTemplate_Idx
	PublicationQueryMoTablesSqlTemplate_Idx
	PublicationQueryMoDatabasesSqlTemplate_Idx
	PublicationQueryMoColumnsSqlTemplate_Idx
	PublicationObjectListSqlTemplate_Idx
	PublicationGetObjectSqlTemplate_Idx
	PublicationDropSnapshotSqlTemplate_Idx
	PublicationDropSnapshotIfExistsSqlTemplate_Idx
	PublicationQueryMoCcprLogSqlTemplate_Idx
	PublicationQuerySnapshotTsSqlTemplate_Idx
	PublicationUpdateMoCcprLogSqlTemplate_Idx

	PublicationSqlTemplateCount
)

var PublicationSQLTemplates = [PublicationSqlTemplateCount]struct {
	SQL         string
	OutputAttrs []string
}{
	PublicationCreateSnapshotForAccountSqlTemplate_Idx: {
		SQL: PublicationCreateSnapshotForAccountSqlTemplate,
	},
	PublicationCreateSnapshotForDatabaseSqlTemplate_Idx: {
		SQL: PublicationCreateSnapshotForDatabaseSqlTemplate,
	},
	PublicationCreateSnapshotForTableSqlTemplate_Idx: {
		SQL: PublicationCreateSnapshotForTableSqlTemplate,
	},
	PublicationQueryMoTablesSqlTemplate_Idx: {
		SQL: PublicationQueryMoTablesSqlTemplate,
		OutputAttrs: []string{
			"rel_id",
			"relname",
			"reldatabase_id",
			"reldatabase",
			"rel_createsql",
			"account_id",
		},
	},
	PublicationQueryMoDatabasesSqlTemplate_Idx: {
		SQL: PublicationQueryMoDatabasesSqlTemplate,
		OutputAttrs: []string{
			"dat_id",
			"datname",
			"dat_createsql",
			"account_id",
		},
	},
	PublicationQueryMoColumnsSqlTemplate_Idx: {
		SQL: PublicationQueryMoColumnsSqlTemplate,
		OutputAttrs: []string{
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
			"att_has_update",
			"att_update",
			"att_has_cluster_by",
			"att_cluster_by",
			"att_seqnum",
			"att_enum_values",
		},
	},
	PublicationObjectListSqlTemplate_Idx: {
		SQL: PublicationObjectListSqlTemplate,
	},
	PublicationGetObjectSqlTemplate_Idx: {
		SQL: PublicationGetObjectSqlTemplate,
	},
	PublicationDropSnapshotSqlTemplate_Idx: {
		SQL: PublicationDropSnapshotSqlTemplate,
	},
	PublicationDropSnapshotIfExistsSqlTemplate_Idx: {
		SQL: PublicationDropSnapshotIfExistsSqlTemplate,
	},
	PublicationQueryMoCcprLogSqlTemplate_Idx: {
		SQL: PublicationQueryMoCcprLogSqlTemplate,
		OutputAttrs: []string{
			"cn_uuid",
			"iteration_state",
			"iteration_lsn",
		},
	},
	PublicationQuerySnapshotTsSqlTemplate_Idx: {
		SQL: PublicationQuerySnapshotTsSqlTemplate,
		OutputAttrs: []string{
			"ts",
		},
	},
	PublicationUpdateMoCcprLogSqlTemplate_Idx: {
		SQL: PublicationUpdateMoCcprLogSqlTemplate,
	},
}

type publicationSQLBuilder struct{}

// ------------------------------------------------------------------------------------------------
// Snapshot SQL
// ------------------------------------------------------------------------------------------------

// CreateSnapshotForAccountSQL creates SQL for creating snapshot for an account
// If accountName is empty, creates snapshot for the current account
// Example: CREATE SNAPSHOT sp1 FOR ACCOUNT
// Example: CREATE SNAPSHOT sp1 FOR ACCOUNT acc01
func (b publicationSQLBuilder) CreateSnapshotForAccountSQL(
	snapshotName string,
	accountName string,
) string {
	var accountPart string
	if accountName != "" {
		accountPart = " " + escapeSQLIdentifier(accountName)
	}
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationCreateSnapshotForAccountSqlTemplate_Idx].SQL,
		escapeSQLIdentifier(snapshotName),
		accountPart,
	)
}

// CreateSnapshotForDatabaseSQL creates SQL for creating snapshot for a database
// Example: CREATE SNAPSHOT sp1 FOR DATABASE db1
func (b publicationSQLBuilder) CreateSnapshotForDatabaseSQL(
	snapshotName string,
	dbName string,
) string {
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationCreateSnapshotForDatabaseSqlTemplate_Idx].SQL,
		escapeSQLIdentifier(snapshotName),
		escapeSQLIdentifier(dbName),
	)
}

// CreateSnapshotForTableSQL creates SQL for creating snapshot for a table
// Example: CREATE SNAPSHOT sp1 FOR TABLE db1 t1
func (b publicationSQLBuilder) CreateSnapshotForTableSQL(
	snapshotName string,
	dbName string,
	tableName string,
) string {
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationCreateSnapshotForTableSqlTemplate_Idx].SQL,
		escapeSQLIdentifier(snapshotName),
		escapeSQLIdentifier(dbName),
		escapeSQLIdentifier(tableName),
	)
}

// DropSnapshotSQL creates SQL for dropping a snapshot
// Example: DROP SNAPSHOT sp1
func (b publicationSQLBuilder) DropSnapshotSQL(
	snapshotName string,
) string {
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationDropSnapshotSqlTemplate_Idx].SQL,
		escapeSQLIdentifier(snapshotName),
	)
}

// DropSnapshotIfExistsSQL creates SQL for dropping a snapshot if it exists
// Example: DROP SNAPSHOT IF EXISTS sp1
func (b publicationSQLBuilder) DropSnapshotIfExistsSQL(
	snapshotName string,
) string {
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationDropSnapshotIfExistsSqlTemplate_Idx].SQL,
		escapeSQLIdentifier(snapshotName),
	)
}

// ------------------------------------------------------------------------------------------------
// Query mo_catalog tables SQL
// ------------------------------------------------------------------------------------------------

// QueryMoTablesSQL creates SQL for querying mo_tables
// Supports filtering by db_name or db_name+table_name
func (b publicationSQLBuilder) QueryMoTablesSQL(
	accountID uint32,
	dbName string,
	tableName string,
) string {
	var conditions []string

	if accountID > 0 {
		conditions = append(conditions, fmt.Sprintf(" AND account_id = %d", accountID))
	}

	if dbName != "" {
		conditions = append(conditions, fmt.Sprintf(" AND reldatabase = '%s'", escapeSQLString(dbName)))
	}

	if tableName != "" {
		conditions = append(conditions, fmt.Sprintf(" AND relname = '%s'", escapeSQLString(tableName)))
	}

	whereClause := strings.Join(conditions, "")
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationQueryMoTablesSqlTemplate_Idx].SQL,
		whereClause,
	)
}

// QueryMoDatabasesSQL creates SQL for querying mo_databases
// Supports filtering by db_name
func (b publicationSQLBuilder) QueryMoDatabasesSQL(
	accountID uint32,
	dbName string,
) string {
	var conditions []string

	if accountID > 0 {
		conditions = append(conditions, fmt.Sprintf(" AND account_id = %d", accountID))
	}

	if dbName != "" {
		conditions = append(conditions, fmt.Sprintf(" AND datname = '%s'", escapeSQLString(dbName)))
	}

	whereClause := strings.Join(conditions, "")
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationQueryMoDatabasesSqlTemplate_Idx].SQL,
		whereClause,
	)
}

// QueryMoColumnsSQL creates SQL for querying mo_columns
// Supports filtering by account_id, db_name, and table_name
func (b publicationSQLBuilder) QueryMoColumnsSQL(
	accountID uint32,
	dbName string,
	tableName string,
) string {
	var conditions []string

	if accountID > 0 {
		conditions = append(conditions, fmt.Sprintf(" AND account_id = %d", accountID))
	}

	if dbName != "" {
		conditions = append(conditions, fmt.Sprintf(" AND att_database = '%s'", escapeSQLString(dbName)))
	}

	if tableName != "" {
		conditions = append(conditions, fmt.Sprintf(" AND att_relname = '%s'", escapeSQLString(tableName)))
	}

	whereClause := strings.Join(conditions, "")
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationQueryMoColumnsSqlTemplate_Idx].SQL,
		whereClause,
	)
}

// QueryMoColumnsWithJoinSQL creates SQL for querying mo_columns with JOIN to mo_tables
// This allows filtering by db_name and table_name directly using mo_tables fields
// Useful when you need to ensure consistency between mo_columns and mo_tables
func (b publicationSQLBuilder) QueryMoColumnsWithJoinSQL(
	accountID uint32,
	dbName string,
	tableName string,
) string {
	var conditions []string

	if accountID > 0 {
		conditions = append(conditions, fmt.Sprintf(" AND c.account_id = %d", accountID))
		conditions = append(conditions, fmt.Sprintf(" AND t.account_id = %d", accountID))
	}

	if dbName != "" {
		conditions = append(conditions, fmt.Sprintf(" AND t.reldatabase = '%s'", escapeSQLString(dbName)))
	}

	if tableName != "" {
		conditions = append(conditions, fmt.Sprintf(" AND t.relname = '%s'", escapeSQLString(tableName)))
	}

	whereClause := strings.Join(conditions, "")
	return `SELECT ` +
		`c.account_id, ` +
		`c.att_database_id, ` +
		`c.att_database, ` +
		`c.att_relname_id, ` +
		`c.att_relname, ` +
		`c.attname, ` +
		`c.atttyp, ` +
		`c.attnum, ` +
		`c.att_length, ` +
		`c.attnotnull, ` +
		`c.atthasdef, ` +
		`c.att_default, ` +
		`c.attisdropped, ` +
		`c.att_constraint_type, ` +
		`c.att_is_unsigned, ` +
		`c.att_is_auto_increment, ` +
		`c.att_comment, ` +
		`c.att_is_hidden, ` +
		`c.att_has_update, ` +
		`c.att_update, ` +
		`c.att_has_cluster_by, ` +
		`c.att_cluster_by, ` +
		`c.att_seqnum, ` +
		`c.att_enum_values ` +
		`FROM mo_catalog.mo_columns AS c ` +
		`INNER JOIN mo_catalog.mo_tables AS t ON c.att_relname_id = t.rel_id ` +
		`WHERE 1=1` + whereClause
}

// ------------------------------------------------------------------------------------------------
// Object List SQL
// ------------------------------------------------------------------------------------------------

// ObjectListSQL creates SQL for object list statement
// Example: OBJECTLIST DATABASE db1 TABLE t1 SNAPSHOT sp2 AGAINST SNAPSHOT sp1
// Example: OBJECTLIST DATABASE db1 SNAPSHOT sp2
// Example: OBJECTLIST SNAPSHOT sp2
func (b publicationSQLBuilder) ObjectListSQL(
	dbName string,
	tableName string,
	snapshotName string,
	againstSnapshotName string,
) string {
	var parts []string

	if dbName != "" {
		parts = append(parts, fmt.Sprintf(" DATABASE %s", escapeSQLIdentifier(dbName)))
	}

	if tableName != "" {
		parts = append(parts, fmt.Sprintf(" TABLE %s", escapeSQLIdentifier(tableName)))
	}

	dbTablePart := strings.Join(parts, "")

	var againstPart string
	if againstSnapshotName != "" {
		againstPart = fmt.Sprintf(" AGAINST SNAPSHOT %s", escapeSQLIdentifier(againstSnapshotName))
	}

	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationObjectListSqlTemplate_Idx].SQL,
		dbTablePart,
		escapeSQLIdentifier(snapshotName),
		againstPart,
	)
}

// ------------------------------------------------------------------------------------------------
// Get Object SQL
// ------------------------------------------------------------------------------------------------

// GetObjectSQL creates SQL for get object statement
// Example: GET OBJECT object_name
func (b publicationSQLBuilder) GetObjectSQL(
	objectName string,
) string {
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationGetObjectSqlTemplate_Idx].SQL,
		escapeSQLString(objectName),
	)
}

// ------------------------------------------------------------------------------------------------
// Query mo_ccpr_log SQL
// ------------------------------------------------------------------------------------------------

// QueryMoCcprLogSQL creates SQL for querying mo_ccpr_log by task_id
// Returns cn_uuid, iteration_state, iteration_lsn
// Example: SELECT cn_uuid, iteration_state, iteration_lsn FROM mo_catalog.mo_ccpr_log WHERE task_id = 1
func (b publicationSQLBuilder) QueryMoCcprLogSQL(
	taskID uint64,
) string {
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationQueryMoCcprLogSqlTemplate_Idx].SQL,
		taskID,
	)
}

// QuerySnapshotTsSQL creates SQL for querying snapshot TS by snapshot name
// Returns ts (bigint)
// Example: SELECT ts FROM mo_catalog.mo_snapshots WHERE sname = 'sp1'
func (b publicationSQLBuilder) QuerySnapshotTsSQL(
	snapshotName string,
) string {
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationQuerySnapshotTsSqlTemplate_Idx].SQL,
		escapeSQLString(snapshotName),
	)
}

// UpdateMoCcprLogSQL creates SQL for updating mo_ccpr_log by task_id
// Updates iteration_state, iteration_lsn, and context
// Example: UPDATE mo_catalog.mo_ccpr_log SET iteration_state = 1, iteration_lsn = 1000, context = '{"key":"value"}' WHERE task_id = 1
func (b publicationSQLBuilder) UpdateMoCcprLogSQL(
	taskID uint64,
	iterationState int8,
	iterationLSN uint64,
	contextJSON string,
) string {
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationUpdateMoCcprLogSqlTemplate_Idx].SQL,
		iterationState,
		iterationLSN,
		escapeSQLString(contextJSON),
		taskID,
	)
}

// ------------------------------------------------------------------------------------------------
// Helper functions
// ------------------------------------------------------------------------------------------------

// escapeSQLString escapes special characters in SQL string literals to prevent SQL injection.
// It follows the SQL standard escaping rules:
//  1. Single quotes (') are escaped as double single quotes (”)
//  2. Backslashes (\) are escaped as double backslashes (\\)
func escapeSQLString(s string) string {
	// Replace backslash first (before replacing quotes) to avoid double-escaping
	s = strings.ReplaceAll(s, `\`, `\\`)
	// Replace single quotes with double single quotes (SQL standard escaping)
	s = strings.ReplaceAll(s, "'", "''")
	return s
}

// escapeSQLIdentifier escapes SQL identifiers (table names, column names, etc.)
// For identifiers that contain special characters or are reserved words, wrap them in backticks
func escapeSQLIdentifier(s string) string {
	// If identifier contains special characters or spaces, wrap in backticks
	if strings.ContainsAny(s, " `\"'()[]{},.;:+-*/=<>!@#$%^&|\\") {
		// Escape backticks inside the identifier
		s = strings.ReplaceAll(s, "`", "``")
		return "`" + s + "`"
	}
	return s
}
