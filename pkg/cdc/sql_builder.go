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

package cdc

import (
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
)

const (
	CDCWatermarkErrMsgMaxLen = 256

	CDCState_Running = "running"
	CDCState_Paused  = "paused"
	CDCState_Failed  = "failed"
)

var CDCSQLBuilder = cdcSQLBuilder{}

const (
	CDCInsertTaskSqlTemplate = `INSERT INTO mo_catalog.mo_cdc_task VALUES(` +
		`%d,` + //account id
		`"%s",` + //task id
		`"%s",` + //task name
		`"%s",` + //source uri
		`"%s",` + //source password
		`"%s",` + //sink uri
		`"%s",` + //sink type
		`"%s",` + //sink password
		`"%s",` + //sink ssl ca path
		`"%s",` + //sink ssl cert path
		`"%s",` + //sink ssl key path
		`"%s",` + //tables
		`"%s",` + //filters
		`"%s",` + //opfilters
		`"%s",` + //source state
		`"%s",` + //sink state
		`"%s",` + //start ts
		`"%s",` + //end ts
		`"%s",` + //config file
		`"%s",` + //task create time
		`"%s",` + //state
		`%d,` + //checkpoint
		`"%d",` + //checkpoint str
		`"%t",` + //no full
		`"%s",` + //incr config
		`'%s',` + //additional config
		`"",` + //err msg
		`"",` + //reserved2
		`"",` + //reserved3
		`""` + //reserved4
		`)`

	CDCGetTaskSqlTemplate = `SELECT ` +
		`sink_uri, ` +
		`sink_type, ` +
		`sink_password, ` +
		`tables, ` +
		`filters, ` +
		`start_ts, ` +
		`end_ts, ` +
		`no_full, ` +
		`additional_config ` +
		`FROM ` +
		`mo_catalog.mo_cdc_task ` +
		`WHERE ` +
		`account_id = %d AND ` +
		`task_id = "%s"`

	CDCShowCdcTaskSqlTemplate = `SELECT ` +
		`task_id, ` +
		`task_name, ` +
		`source_uri, ` +
		`sink_uri, ` +
		`state, ` +
		`err_msg ` +
		`FROM ` +
		`mo_catalog.mo_cdc_task ` +
		`WHERE ` +
		`account_id = %d`

	CDCGetCdcTaskIdSqlTemplate = "SELECT " +
		"task_id " +
		"FROM `mo_catalog`.`mo_cdc_task` " +
		"WHERE 1=1 AND account_id = %d"

	CDCDeleteTaskSqlTemplate = "DELETE " +
		"FROM " +
		"`mo_catalog`.`mo_cdc_task` " +
		"WHERE " +
		"1=1 AND account_id = %d"

	CDCUpdateTaskStateSqlTemplate = "UPDATE " +
		"`mo_catalog`.`mo_cdc_task` " +
		"SET state = ? " +
		"WHERE " +
		"1=1 AND account_id = %d"

	CDCUpdateTaskStateAndErrMsgSqlTemplate = "UPDATE " +
		"`mo_catalog`.`mo_cdc_task` " +
		"SET state = '%s', err_msg = '%s' " +
		"WHERE " +
		"1=1 AND account_id = %d AND task_id = '%s'"

	CDCGetDataKeySqlTemplate = "SELECT " +
		"encrypted_key " +
		"FROM mo_catalog.mo_data_key " +
		"WHERE account_id = %d and key_id = '%s'"

	// Watermark Related SQL
	CDCInsertWatermarkSqlTemplate = "INSERT INTO " +
		"`mo_catalog`.`mo_cdc_watermark` " +
		"VALUES (%d, '%s', '%s', '%s', '%s', '%s')"

	CDCDeleteWatermarkSqlTemplate = "DELETE FROM " +
		"`mo_catalog`.`mo_cdc_watermark` " +
		"WHERE " +
		"account_id = %d AND " +
		"task_id = '%s'"

	CDCGetTableWatermarkSqlTemplate = "SELECT " +
		"watermark " +
		"FROM " +
		"`mo_catalog`.`mo_cdc_watermark` " +
		"WHERE " +
		"account_id = %d AND " +
		"task_id = '%s' AND " +
		"db_name = '%s' AND " +
		"table_name = '%s'"

	CDCGetWatermarkSqlTemplate = "SELECT " +
		"db_name, " +
		"table_name, " +
		"watermark, " +
		"err_msg " +
		"FROM " +
		"`mo_catalog`.`mo_cdc_watermark` " +
		"WHERE " +
		"account_id = %d AND " +
		"task_id = '%s'"

	CDCUpdateWatermarkSqlTemplate = "UPDATE " +
		"`mo_catalog`.`mo_cdc_watermark` " +
		"SET watermark='%s' " +
		"WHERE " +
		"account_id = %d AND " +
		"task_id = '%s' AND " +
		"db_name = '%s' AND " +
		"table_name = '%s'"

	CDCDeleteWatermarkByTableSqlTemplate = "DELETE " +
		"FROM " +
		"`mo_catalog`.`mo_cdc_watermark` " +
		"WHERE " +
		"account_id = %d AND " +
		"task_id = '%s' AND " +
		"db_name = '%s' AND " +
		"table_name = '%s'"

	CDCUpdateWatermarkErrMsgSqlTemplate = "UPDATE " +
		"`mo_catalog`.`mo_cdc_watermark` " +
		"SET err_msg='%s' " +
		"WHERE " +
		"account_id = %d AND " +
		"task_id = '%s' AND " +
		"db_name = '%s' AND " +
		"table_name = '%s'"

	CDCCollectTableInfoSqlTemplate = "SELECT " +
		" rel_id, " +
		" relname, " +
		" reldatabase_id, " +
		" reldatabase, " +
		" rel_createsql, " +
		" account_id " +
		"FROM `mo_catalog`.`mo_tables` " +
		"WHERE " +
		" relkind = '%s' " +
		" AND reldatabase NOT IN (%s)"
)

const (
	CDCInsertTaskSqlTemplate_Idx             = 0
	CDCGetTaskSqlTemplate_Idx                = 1
	CDCShowTaskSqlTemplate_Idx               = 2
	CDCGetTaskIdSqlTemplate_Idx              = 3
	CDCDeleteTaskSqlTemplate_Idx             = 4
	CDCUpdateTaskStateSQL_Idx                = 5
	CDCUpdateTaskStateAndErrMsgSQL_Idx       = 6
	CDCInsertWatermarkSqlTemplate_Idx        = 7
	CDCGetWatermarkSqlTemplate_Idx           = 8
	CDCGetTableWatermarkSQL_Idx              = 9
	CDCUpdateWatermarkSQL_Idx                = 10
	CDCUpdateWatermarkErrMsgSQL_Idx          = 11
	CDCDeleteWatermarkSqlTemplate_Idx        = 12
	CDCDeleteWatermarkByTableSqlTemplate_Idx = 13
	CDCGetDataKeySQL_Idx                     = 14
	CDCCollectTableInfoSqlTemplate_Idx       = 15

	CDCSqlTemplateCount = 16
)

var CDCSQLTemplates = [CDCSqlTemplateCount]struct {
	SQL         string
	OutputAttrs []string
}{
	CDCInsertTaskSqlTemplate_Idx: {
		SQL: CDCInsertTaskSqlTemplate,
	},
	CDCGetTaskSqlTemplate_Idx: {
		SQL: CDCGetTaskSqlTemplate,
		OutputAttrs: []string{
			"sink_uri",
			"sink_type",
			"sink_password",
			"tables",
			"filters",
			"start_ts",
			"end_ts",
			"no_full",
			"additional_config",
		},
	},
	CDCShowTaskSqlTemplate_Idx: {
		SQL: CDCShowCdcTaskSqlTemplate,
		OutputAttrs: []string{
			"task_id",
			"task_name",
			"source_uri",
			"sink_uri",
			"state",
			"err_msg",
		},
	},
	CDCGetTaskIdSqlTemplate_Idx: {
		SQL:         CDCGetCdcTaskIdSqlTemplate,
		OutputAttrs: []string{"task_id"},
	},
	CDCDeleteTaskSqlTemplate_Idx: {
		SQL: CDCDeleteTaskSqlTemplate,
	},
	CDCUpdateTaskStateSQL_Idx: {
		SQL: CDCUpdateTaskStateSqlTemplate,
	},
	CDCUpdateTaskStateAndErrMsgSQL_Idx: {
		SQL: CDCUpdateTaskStateAndErrMsgSqlTemplate,
	},
	CDCInsertWatermarkSqlTemplate_Idx: {
		SQL: CDCInsertWatermarkSqlTemplate,
	},
	CDCGetWatermarkSqlTemplate_Idx: {
		SQL: CDCGetWatermarkSqlTemplate,
		OutputAttrs: []string{
			"db_name",
			"table_name",
			"watermark",
			"err_msg",
		},
	},
	CDCGetTableWatermarkSQL_Idx: {
		SQL: CDCGetTableWatermarkSqlTemplate,
		OutputAttrs: []string{
			"watermark",
		},
	},
	CDCUpdateWatermarkSQL_Idx: {
		SQL: CDCUpdateWatermarkSqlTemplate,
	},
	CDCUpdateWatermarkErrMsgSQL_Idx: {
		SQL: CDCUpdateWatermarkErrMsgSqlTemplate,
	},
	CDCDeleteWatermarkSqlTemplate_Idx: {
		SQL: CDCDeleteWatermarkSqlTemplate,
	},
	CDCDeleteWatermarkByTableSqlTemplate_Idx: {
		SQL: CDCDeleteWatermarkByTableSqlTemplate,
	},
	CDCGetDataKeySQL_Idx: {
		SQL:         CDCGetDataKeySqlTemplate,
		OutputAttrs: []string{"encrypted_key"},
	},
	CDCCollectTableInfoSqlTemplate_Idx: {
		SQL: fmt.Sprintf(
			CDCCollectTableInfoSqlTemplate,
			catalog.SystemOrdinaryRel,
			AddSingleQuotesJoin(catalog.SystemDatabases),
		),
		OutputAttrs: []string{
			"rel_id",
			"relname",
			"reldatabase_id",
			"reldatabase",
			"rel_createsql",
			"account_id",
		},
	},
}

type cdcSQLBuilder struct{}

// ------------------------------------------------------------------------------------------------
// Task SQL
// ------------------------------------------------------------------------------------------------
func (b cdcSQLBuilder) InsertTaskSQL(
	accountId uint64,
	taskId string,
	taskName string,
	sourceUri string,
	sourcePwd string,
	sinkUri string,
	sinkTyp string,
	sinkPwd string,
	sinkCaPath string,
	sinkCertPath string,
	sinkKeyPath string,
	tables string,
	filters string,
	opfilters string,
	sourceState string,
	sinkState string,
	startTs string,
	endTs string,
	configFile string,
	taskCreateTime time.Time,
	state string,
	checkpoint uint64,
	noFull bool,
	incrConfig string,
	additionalConfigStr string,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCInsertTaskSqlTemplate_Idx].SQL,
		accountId,
		taskId,
		taskName,
		sourceUri,
		sourcePwd,
		sinkUri,
		sinkTyp,
		sinkPwd,
		sinkCaPath,
		sinkCertPath,
		sinkKeyPath,
		tables,
		filters,
		opfilters,
		sourceState,
		sinkState,
		startTs,
		endTs,
		configFile,
		taskCreateTime.Format(time.DateTime),
		state,
		checkpoint,
		checkpoint,
		noFull,
		incrConfig,
		additionalConfigStr,
	)
}

func (b cdcSQLBuilder) GetTaskSQL(
	accountId uint64,
	taskId string,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCGetTaskSqlTemplate_Idx].SQL,
		accountId,
		taskId,
	)
}

func (b cdcSQLBuilder) ShowTaskSQL(
	accountId uint64,
	showAll bool,
	taskName string,
) string {
	sql := fmt.Sprintf(
		CDCSQLTemplates[CDCShowTaskSqlTemplate_Idx].SQL,
		accountId,
	)
	if !showAll {
		sql += fmt.Sprintf(
			` AND task_name = '%s'`,
			taskName,
		)
	}
	return sql
}

func (b cdcSQLBuilder) DeleteTaskSQL(
	accountId uint64,
	taskName string,
) string {
	sql := fmt.Sprintf(
		CDCSQLTemplates[CDCDeleteTaskSqlTemplate_Idx].SQL,
		accountId,
	)
	if taskName != "" {
		sql += fmt.Sprintf(
			` AND task_name = '%s'`,
			taskName,
		)
	}
	return sql
}

func (b cdcSQLBuilder) UpdateTaskStateSQL(
	accountId uint64,
	taskName string,
) string {
	sql := fmt.Sprintf(
		CDCSQLTemplates[CDCUpdateTaskStateSQL_Idx].SQL,
		accountId,
	)
	if taskName != "" {
		sql += fmt.Sprintf(
			` AND task_name = '%s'`,
			taskName,
		)
	}
	return sql
}

func (b cdcSQLBuilder) UpdateTaskStateAndErrMsgSQL(
	accountId uint64,
	taskId string,
	state string,
	errMsg string,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCUpdateTaskStateAndErrMsgSQL_Idx].SQL,
		state,
		errMsg,
		accountId,
		taskId,
	)
}

func (b cdcSQLBuilder) GetTaskIdSQL(
	accountId uint64,
	taskName string,
) string {
	sql := fmt.Sprintf(
		CDCSQLTemplates[CDCGetTaskIdSqlTemplate_Idx].SQL,
		accountId,
	)
	if taskName != "" {
		sql += fmt.Sprintf(
			` AND task_name = '%s'`,
			taskName,
		)
	}
	return sql
}

// ------------------------------------------------------------------------------------------------
// Watermark SQL
// ------------------------------------------------------------------------------------------------
func (b cdcSQLBuilder) InsertWatermarkSQL(
	accountId uint64,
	taskId string,
	dbName string,
	tableName string,
	watermark string,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCInsertWatermarkSqlTemplate_Idx].SQL,
		accountId,
		taskId,
		dbName,
		tableName,
		watermark,
		"",
	)
}

func (b cdcSQLBuilder) DeleteWatermarkSQL(
	accountId uint64,
	taskId string,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCDeleteWatermarkSqlTemplate_Idx].SQL,
		accountId,
		taskId,
	)
}

func (b cdcSQLBuilder) GetWatermarkSQL(
	accountId uint64,
	taskId string,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCGetWatermarkSqlTemplate_Idx].SQL,
		accountId,
		taskId,
	)
}

func (b cdcSQLBuilder) GetTableWatermarkSQL(
	accountId uint64,
	taskId string,
	dbName string,
	tableName string,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCGetTableWatermarkSQL_Idx].SQL,
		accountId,
		taskId,
		dbName,
		tableName,
	)
}

func (b cdcSQLBuilder) GetDataKeySQL(
	accountId uint64,
	keyId string,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCGetDataKeySQL_Idx].SQL,
		accountId,
		keyId,
	)
}

func (b cdcSQLBuilder) UpdateWatermarkErrMsgSQL(
	accountId uint64,
	taskId string,
	dbName string,
	tableName string,
	errMsg string,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCUpdateWatermarkErrMsgSQL_Idx].SQL,
		errMsg,
		accountId,
		taskId,
		dbName,
		tableName,
	)
}

func (b cdcSQLBuilder) DeleteWatermarkByTableSQL(
	accountId uint64,
	taskId string,
	dbName string,
	tableName string,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCDeleteWatermarkByTableSqlTemplate_Idx].SQL,
		accountId,
		taskId,
		dbName,
		tableName,
	)
}

func (b cdcSQLBuilder) UpdateWatermarkSQL(
	accountId uint64,
	taskId string,
	dbName string,
	tableName string,
	watermark string,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCUpdateWatermarkSQL_Idx].SQL,
		watermark,
		accountId,
		taskId,
		dbName,
		tableName,
	)
}

// ------------------------------------------------------------------------------------------------
// Table Info SQL
// ------------------------------------------------------------------------------------------------
func (b cdcSQLBuilder) CollectTableInfoSQL() string {
	return CDCSQLTemplates[CDCCollectTableInfoSqlTemplate_Idx].SQL
}
