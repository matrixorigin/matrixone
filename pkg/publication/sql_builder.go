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
	// Create snapshot SQL templates with publication support (for CCPR subscription)
	// These templates use the publisher's account (授权账户) for snapshot creation
	PublicationCreateSnapshotForAccountSqlTemplate  = `CREATE SNAPSHOT%s %s FOR ACCOUNT FROM %s PUBLICATION %s`
	PublicationCreateSnapshotForDatabaseSqlTemplate = `CREATE SNAPSHOT%s %s FOR DATABASE %s FROM %s PUBLICATION %s`
	PublicationCreateSnapshotForTableSqlTemplate    = `CREATE SNAPSHOT%s %s FOR TABLE %s %s FROM %s PUBLICATION %s`

	// Query mo_catalog tables SQL templates using internal command with publication permission check
	// Format: __++__internal_get_mo_indexes <tableId> <subscriptionAccountName> <publicationName> <snapshotName>
	PublicationQueryMoIndexesSqlTemplate = `__++__internal_get_mo_indexes %d %s %s %s`

	// Object list SQL template
	PublicationObjectListSqlTemplate = `OBJECTLIST%s SNAPSHOT %s%s%s`

	// Get object SQL template using internal command with publication permission check
	// Format: __++__internal_get_object <subscriptionAccountName> <publicationName> <objectName> <chunkIndex>
	PublicationGetObjectSqlTemplate = `__++__internal_get_object %s %s %s %d`

	// Get DDL SQL template using internal command with publication permission check
	// Format: __++__internal_get_ddl <snapshotName> <subscriptionAccountName> <publicationName>
	PublicationGetDdlSqlTemplate = `__++__internal_get_ddl %s %s %s`

	// Drop snapshot SQL templates
	PublicationDropSnapshotIfExistsSqlTemplate = `DROP SNAPSHOT IF EXISTS %s`

	// Query mo_ccpr_log SQL template
	PublicationQueryMoCcprLogSqlTemplate = `SELECT ` +
		`cn_uuid, ` +
		`iteration_state, ` +
		`iteration_lsn, ` +
		`state ` +
		`FROM mo_catalog.mo_ccpr_log ` +
		`WHERE task_id = '%s'`

	// Query mo_ccpr_log full SQL template (includes subscription_name, subscription_account_name, sync_level, account_id, db_name, table_name, upstream_conn, context, error_message, state)
	PublicationQueryMoCcprLogFullSqlTemplate = `SELECT ` +
		`subscription_name, ` +
		`subscription_account_name, ` +
		`sync_level, ` +
		`account_id, ` +
		`db_name, ` +
		`table_name, ` +
		`upstream_conn, ` +
		`context, ` +
		`error_message, ` +
		`state ` +
		`FROM mo_catalog.mo_ccpr_log ` +
		`WHERE task_id = '%s'`

	// Query snapshot TS SQL template using internal command with publication permission check
	// Format: __++__internal_get_snapshot_ts <snapshotName> <accountName> <publicationName>
	PublicationQuerySnapshotTsSqlTemplate = `__++__internal_get_snapshot_ts %s %s %s`

	// Query databases covered by snapshot using internal command with publication permission check
	// Format: __++__internal_get_databases <snapshotName> <accountName> <publicationName>
	PublicationGetDatabasesSqlTemplate = `__++__internal_get_databases %s %s %s`

	// Check snapshot flushed SQL template
	// Parameters: snapshot_name, account_name, publication_name
	PublicationCheckSnapshotFlushedSqlTemplate = `CHECKSNAPSHOTFLUSHED %s ACCOUNT %s PUBLICATION %s`

	// Update mo_ccpr_log SQL template
	PublicationUpdateMoCcprLogSqlTemplate = `UPDATE mo_catalog.mo_ccpr_log ` +
		`SET iteration_state = %d, ` +
		`iteration_lsn = %d, ` +
		`context = '%s', ` +
		`error_message = '%s', ` +
		`state = %d ` +
		`WHERE task_id = '%s'`

	// Update mo_ccpr_log iteration_state (and lsn) only
	PublicationUpdateMoCcprLogStateSqlTemplate = `UPDATE mo_catalog.mo_ccpr_log ` +
		`SET iteration_state = %d, ` +
		`iteration_lsn = %d, ` +
		`cn_uuid = '%s' ` +
		`WHERE task_id = '%s'`

	// Query mo_ccpr_log state before update SQL template
	PublicationQueryMoCcprLogStateBeforeUpdateSqlTemplate = `SELECT ` +
		`state, ` +
		`iteration_state, ` +
		`iteration_lsn ` +
		`FROM mo_catalog.mo_ccpr_log ` +
		`WHERE task_id = '%s'`

	// Update mo_ccpr_log without state SQL template (for successful iterations)
	PublicationUpdateMoCcprLogNoStateSqlTemplate = `UPDATE mo_catalog.mo_ccpr_log ` +
		`SET iteration_state = %d, ` +
		`iteration_lsn = %d, ` +
		`context = '%s', ` +
		`error_message = '%s' ` +
		`WHERE task_id = '%s'`

	// Update mo_ccpr_log iteration_state only
	PublicationUpdateMoCcprLogIterationStateOnlySqlTemplate = `UPDATE mo_catalog.mo_ccpr_log ` +
		`SET iteration_state = %d ` +
		`WHERE task_id = '%s'`

	// Update mo_ccpr_log iteration_state and cn_uuid (without lsn)
	PublicationUpdateMoCcprLogIterationStateAndCnUuidSqlTemplate = `UPDATE mo_catalog.mo_ccpr_log ` +
		`SET iteration_state = %d, ` +
		`cn_uuid = '%s' ` +
		`WHERE task_id = '%s'`
)

const (
	PublicationCreateSnapshotForAccountSqlTemplate_Idx = iota
	PublicationCreateSnapshotForDatabaseSqlTemplate_Idx
	PublicationCreateSnapshotForTableSqlTemplate_Idx
	PublicationQueryMoIndexesSqlTemplate_Idx
	PublicationObjectListSqlTemplate_Idx
	PublicationGetObjectSqlTemplate_Idx
	PublicationGetDdlSqlTemplate_Idx
	PublicationDropSnapshotIfExistsSqlTemplate_Idx
	PublicationQueryMoCcprLogSqlTemplate_Idx
	PublicationQueryMoCcprLogFullSqlTemplate_Idx
	PublicationQuerySnapshotTsSqlTemplate_Idx
	PublicationGetDatabasesSqlTemplate_Idx
	PublicationUpdateMoCcprLogSqlTemplate_Idx
	PublicationUpdateMoCcprLogStateSqlTemplate_Idx
	PublicationCheckSnapshotFlushedSqlTemplate_Idx
	PublicationQueryMoCcprLogStateBeforeUpdateSqlTemplate_Idx
	PublicationUpdateMoCcprLogNoStateSqlTemplate_Idx
	PublicationUpdateMoCcprLogIterationStateOnlySqlTemplate_Idx
	PublicationUpdateMoCcprLogIterationStateAndCnUuidSqlTemplate_Idx

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
	PublicationQueryMoIndexesSqlTemplate_Idx: {
		SQL: PublicationQueryMoIndexesSqlTemplate,
		OutputAttrs: []string{
			"table_id",
			"name",
			"algo_table_type",
			"index_table_name",
		},
	},
	PublicationObjectListSqlTemplate_Idx: {
		SQL: PublicationObjectListSqlTemplate,
	},
	PublicationGetObjectSqlTemplate_Idx: {
		SQL: PublicationGetObjectSqlTemplate,
	},
	PublicationGetDdlSqlTemplate_Idx: {
		SQL: PublicationGetDdlSqlTemplate,
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
			"state",
		},
	},
	PublicationQueryMoCcprLogFullSqlTemplate_Idx: {
		SQL: PublicationQueryMoCcprLogFullSqlTemplate,
		OutputAttrs: []string{
			"subscription_name",
			"subscription_account_name",
			"sync_level",
			"account_id",
			"db_name",
			"table_name",
			"upstream_conn",
			"context",
			"error_message",
		},
	},
	PublicationQuerySnapshotTsSqlTemplate_Idx: {
		SQL: PublicationQuerySnapshotTsSqlTemplate,
		OutputAttrs: []string{
			"ts",
		},
	},
	PublicationGetDatabasesSqlTemplate_Idx: {
		SQL: PublicationGetDatabasesSqlTemplate,
		OutputAttrs: []string{
			"datname",
		},
	},
	PublicationUpdateMoCcprLogSqlTemplate_Idx: {
		SQL: PublicationUpdateMoCcprLogSqlTemplate,
	},
	PublicationUpdateMoCcprLogStateSqlTemplate_Idx: {
		SQL: PublicationUpdateMoCcprLogStateSqlTemplate,
	},
	PublicationCheckSnapshotFlushedSqlTemplate_Idx: {
		SQL: PublicationCheckSnapshotFlushedSqlTemplate,
	},
	PublicationQueryMoCcprLogStateBeforeUpdateSqlTemplate_Idx: {
		SQL: PublicationQueryMoCcprLogStateBeforeUpdateSqlTemplate,
		OutputAttrs: []string{
			"state",
			"iteration_state",
			"iteration_lsn",
		},
	},
	PublicationUpdateMoCcprLogNoStateSqlTemplate_Idx: {
		SQL: PublicationUpdateMoCcprLogNoStateSqlTemplate,
	},
	PublicationUpdateMoCcprLogIterationStateOnlySqlTemplate_Idx: {
		SQL: PublicationUpdateMoCcprLogIterationStateOnlySqlTemplate,
	},
	PublicationUpdateMoCcprLogIterationStateAndCnUuidSqlTemplate_Idx: {
		SQL: PublicationUpdateMoCcprLogIterationStateAndCnUuidSqlTemplate,
	},
}

type publicationSQLBuilder struct{}

// ------------------------------------------------------------------------------------------------
// Snapshot SQL
// ------------------------------------------------------------------------------------------------

// CreateSnapshotForAccountSQL creates SQL for creating account-level snapshot with publication
// This is used for CCPR subscription to create snapshot using the publisher's account (授权账户)
// Example: CREATE SNAPSHOT sp1 FOR ACCOUNT FROM acc01 PUBLICATION pub01
// Example: CREATE SNAPSHOT IF NOT EXISTS sp1 FOR ACCOUNT FROM acc01 PUBLICATION pub01
func (b publicationSQLBuilder) CreateSnapshotForAccountSQL(
	snapshotName string,
	accountName string,
	pubName string,
	ifNotExists bool,
) string {
	var ifNotExistsPart string
	if ifNotExists {
		ifNotExistsPart = " IF NOT EXISTS"
	}
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationCreateSnapshotForAccountSqlTemplate_Idx].SQL,
		ifNotExistsPart,
		escapeSQLIdentifier(snapshotName),
		escapeSQLIdentifier(accountName),
		escapeSQLIdentifier(pubName),
	)
}

// CreateSnapshotForDatabaseSQL creates SQL for creating database-level snapshot with publication
// This is used for CCPR subscription to create snapshot using the publisher's account (授权账户)
// Example: CREATE SNAPSHOT sp1 FOR DATABASE db1 FROM acc01 PUBLICATION pub01
// Example: CREATE SNAPSHOT IF NOT EXISTS sp1 FOR DATABASE db1 FROM acc01 PUBLICATION pub01
func (b publicationSQLBuilder) CreateSnapshotForDatabaseSQL(
	snapshotName string,
	dbName string,
	accountName string,
	pubName string,
	ifNotExists bool,
) string {
	var ifNotExistsPart string
	if ifNotExists {
		ifNotExistsPart = " IF NOT EXISTS"
	}
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationCreateSnapshotForDatabaseSqlTemplate_Idx].SQL,
		ifNotExistsPart,
		escapeSQLIdentifier(snapshotName),
		escapeSQLIdentifier(dbName),
		escapeSQLIdentifier(accountName),
		escapeSQLIdentifier(pubName),
	)
}

// CreateSnapshotForTableSQL creates SQL for creating table-level snapshot with publication
// This is used for CCPR subscription to create snapshot using the publisher's account (授权账户)
// Example: CREATE SNAPSHOT sp1 FOR TABLE db1 t1 FROM acc01 PUBLICATION pub01
// Example: CREATE SNAPSHOT IF NOT EXISTS sp1 FOR TABLE db1 t1 FROM acc01 PUBLICATION pub01
func (b publicationSQLBuilder) CreateSnapshotForTableSQL(
	snapshotName string,
	dbName string,
	tableName string,
	accountName string,
	pubName string,
	ifNotExists bool,
) string {
	var ifNotExistsPart string
	if ifNotExists {
		ifNotExistsPart = " IF NOT EXISTS"
	}
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationCreateSnapshotForTableSqlTemplate_Idx].SQL,
		ifNotExistsPart,
		escapeSQLIdentifier(snapshotName),
		escapeSQLIdentifier(dbName),
		escapeSQLIdentifier(tableName),
		escapeSQLIdentifier(accountName),
		escapeSQLIdentifier(pubName),
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

// QueryMoIndexesSQL creates SQL for querying mo_indexes using internal command with publication permission check
// Uses internal command: __++__internal_get_mo_indexes <tableId> <subscriptionAccountName> <publicationName> <snapshotName>
// This command checks if the current account has permission to access the publication,
// then uses the authorized account to query mo_indexes table at the snapshot timestamp
// Returns table_id, name, algo_table_type, index_table_name
func (b publicationSQLBuilder) QueryMoIndexesSQL(
	tableID uint64,
	subscriptionAccountName string,
	publicationName string,
	snapshotName string,
) string {
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationQueryMoIndexesSqlTemplate_Idx].SQL,
		tableID,
		escapeSQLString(subscriptionAccountName),
		escapeSQLString(publicationName),
		escapeSQLString(snapshotName),
	)
}

// ------------------------------------------------------------------------------------------------
// Object List SQL
// ------------------------------------------------------------------------------------------------

// ObjectListSQL creates SQL for object list statement
// Example: OBJECTLIST DATABASE db1 TABLE t1 SNAPSHOT sp2 AGAINST SNAPSHOT sp1 FROM acc1 PUBLICATION pub1
// Example: OBJECTLIST DATABASE db1 SNAPSHOT sp2 FROM acc1 PUBLICATION pub1
// Example: OBJECTLIST SNAPSHOT sp2 FROM acc1 PUBLICATION pub1
func (b publicationSQLBuilder) ObjectListSQL(
	dbName string,
	tableName string,
	snapshotName string,
	againstSnapshotName string,
	subscriptionAccountName string,
	pubName string,
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

	var fromPart string
	if subscriptionAccountName != "" && pubName != "" {
		fromPart = fmt.Sprintf(" FROM %s PUBLICATION %s", escapeSQLIdentifier(subscriptionAccountName), escapeSQLIdentifier(pubName))
	}

	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationObjectListSqlTemplate_Idx].SQL,
		dbTablePart,
		escapeSQLIdentifier(snapshotName),
		againstPart,
		fromPart,
	)
}

// ------------------------------------------------------------------------------------------------
// Get Object SQL
// ------------------------------------------------------------------------------------------------

// GetObjectSQL creates SQL for get object statement
// Example: GETOBJECT object_name OFFSET 0 FROM acc1 PUBLICATION pub1
// GetObjectSQL creates SQL for get object statement using internal command with publication permission check
// Uses internal command: __++__internal_get_object <subscriptionAccountName> <publicationName> <objectName> <chunkIndex>
// This command checks if the current account has permission to access the publication,
// then reads the object data chunk from fileservice
// Returns data, total_size, chunk_index, total_chunks, is_complete
func (b publicationSQLBuilder) GetObjectSQL(
	subscriptionAccountName string,
	pubName string,
	objectName string,
	chunkIndex int64,
) string {
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationGetObjectSqlTemplate_Idx].SQL,
		escapeSQLString(subscriptionAccountName),
		escapeSQLString(pubName),
		escapeSQLString(objectName),
		chunkIndex,
	)
}

// GetDdlSQL creates SQL for get DDL statement using internal command with publication permission check
// Uses internal command: __++__internal_get_ddl <snapshotName> <subscriptionAccountName> <publicationName>
// This command checks if the current account has permission to access the publication,
// then uses the snapshot's level to determine dbName and tableName scope
// Returns dbname, tablename, tableid, tablesql
func (b publicationSQLBuilder) GetDdlSQL(
	snapshotName string,
	subscriptionAccountName string,
	pubName string,
) string {
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationGetDdlSqlTemplate_Idx].SQL,
		escapeSQLString(snapshotName),
		escapeSQLString(subscriptionAccountName),
		escapeSQLString(pubName),
	)
}

// ------------------------------------------------------------------------------------------------
// Query mo_ccpr_log SQL
// ------------------------------------------------------------------------------------------------

// QueryMoCcprLogSQL creates SQL for querying mo_ccpr_log by task_id
// Returns cn_uuid, iteration_state, iteration_lsn, state
// Example: SELECT cn_uuid, iteration_state, iteration_lsn, state FROM mo_catalog.mo_ccpr_log WHERE task_id = 'uuid'
func (b publicationSQLBuilder) QueryMoCcprLogSQL(
	taskID string,
) string {
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationQueryMoCcprLogSqlTemplate_Idx].SQL,
		taskID,
	)
}

// QueryMoCcprLogFullSQL creates SQL for querying full mo_ccpr_log record by task_id
// Returns subscription_name, sync_level, db_name, table_name, upstream_conn, context
// Example: SELECT subscription_name, sync_level, db_name, table_name, upstream_conn, context FROM mo_catalog.mo_ccpr_log WHERE task_id = 'uuid'
func (b publicationSQLBuilder) QueryMoCcprLogFullSQL(
	taskID string,
) string {
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationQueryMoCcprLogFullSqlTemplate_Idx].SQL,
		taskID,
	)
}

// QuerySnapshotTsSQL creates SQL for querying snapshot TS by snapshot name with publication permission check
// Uses internal command: __++__internal_get_snapshot_ts <snapshotName> <accountName> <publicationName>
// This command checks if the current account has permission to access the publication,
// then uses the authorized account to query mo_snapshots table
// Returns ts (bigint)
func (b publicationSQLBuilder) QuerySnapshotTsSQL(
	snapshotName string,
	accountName string,
	publicationName string,
) string {
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationQuerySnapshotTsSqlTemplate_Idx].SQL,
		escapeSQLString(snapshotName),
		escapeSQLString(accountName),
		escapeSQLString(publicationName),
	)
}

// GetDatabasesSQL creates SQL for querying databases covered by snapshot with publication permission check
// Uses internal command: __++__internal_get_databases <snapshotName> <accountName> <publicationName>
// This command checks if the current account has permission to access the publication,
// then uses the authorized account to query mo_database at the snapshot timestamp
// Returns datname (varchar)
func (b publicationSQLBuilder) GetDatabasesSQL(
	snapshotName string,
	accountName string,
	publicationName string,
) string {
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationGetDatabasesSqlTemplate_Idx].SQL,
		escapeSQLString(snapshotName),
		escapeSQLString(accountName),
		escapeSQLString(publicationName),
	)
}

// CheckSnapshotFlushedSQL creates SQL for checking if snapshot is flushed
// Returns result (bool)
// Example: CHECKSNAPSHOTFLUSHED sp1 ACCOUNT account1 PUBLICATION pub1
func (b publicationSQLBuilder) CheckSnapshotFlushedSQL(
	snapshotName string,
	accountName string,
	publicationName string,
) string {
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationCheckSnapshotFlushedSqlTemplate_Idx].SQL,
		escapeSQLIdentifier(snapshotName),
		escapeSQLIdentifier(accountName),
		escapeSQLIdentifier(publicationName),
	)
}

// UpdateMoCcprLogSQL creates SQL for updating mo_ccpr_log by task_id
// Updates iteration_state, iteration_lsn, context, error_message, and state
// Example: UPDATE mo_catalog.mo_ccpr_log SET iteration_state = 1, iteration_lsn = 1000, context = '{"key":"value"}', error_message = 'error msg', state = 0 WHERE task_id = 'uuid'
func (b publicationSQLBuilder) UpdateMoCcprLogSQL(
	taskID string,
	iterationState int8,
	iterationLSN uint64,
	contextJSON string,
	errorMessage string,
	subscriptionState int8,
) string {
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationUpdateMoCcprLogSqlTemplate_Idx].SQL,
		iterationState,
		iterationLSN,
		escapeSQLString(contextJSON),
		escapeSQLString(errorMessage),
		subscriptionState,
		taskID,
	)
}

// QueryMoCcprLogStateBeforeUpdateSQL creates SQL for querying state, iteration_state, iteration_lsn before update
// Example: SELECT state, iteration_state, iteration_lsn FROM mo_catalog.mo_ccpr_log WHERE task_id = 'uuid'
func (b publicationSQLBuilder) QueryMoCcprLogStateBeforeUpdateSQL(taskID string) string {
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationQueryMoCcprLogStateBeforeUpdateSqlTemplate_Idx].SQL,
		taskID,
	)
}

// UpdateMoCcprLogNoStateSQL creates SQL for updating mo_ccpr_log without state field
// Used for successful iterations where we don't need to change the subscription state
// Example: UPDATE mo_catalog.mo_ccpr_log SET iteration_state = 2, iteration_lsn = 1001, context = '...', error_message = " WHERE task_id = 'uuid'
func (b publicationSQLBuilder) UpdateMoCcprLogNoStateSQL(
	taskID string,
	iterationState int8,
	iterationLSN uint64,
	contextJSON string,
	errorMessage string,
) string {
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationUpdateMoCcprLogNoStateSqlTemplate_Idx].SQL,
		iterationState,
		iterationLSN,
		escapeSQLString(contextJSON),
		escapeSQLString(errorMessage),
		taskID,
	)
}

// UpdateMoCcprLogIterationStateAndCnUuidSQL creates SQL for updating iteration_state and cn_uuid in mo_ccpr_log (without lsn)
// Example: UPDATE mo_catalog.mo_ccpr_log SET iteration_state = 1, cn_uuid = 'uuid' WHERE task_id = 'uuid'
func (b publicationSQLBuilder) UpdateMoCcprLogIterationStateAndCnUuidSQL(
	taskID string,
	iterationState int8,
	cnUUID string,
) string {
	return fmt.Sprintf(
		PublicationSQLTemplates[PublicationUpdateMoCcprLogIterationStateAndCnUuidSqlTemplate_Idx].SQL,
		iterationState,
		escapeSQLString(cnUUID),
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
