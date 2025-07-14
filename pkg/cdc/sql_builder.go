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
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
		"VALUES %s"

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

	CDCGetWatermarkWhereSqlTemplate = "SELECT " +
		"%s " +
		"FROM " +
		"`mo_catalog`.`mo_cdc_watermark` " +
		"WHERE %s"

	CDCOnDuplicateUpdateWatermarkTemplate = "INSERT INTO " +
		"`mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, watermark) " +
		"VALUES %s " +
		"ON DUPLICATE KEY UPDATE watermark = VALUES(watermark)"

	CDCOnDuplicateUpdateWatermarkErrMsgTemplate = "INSERT INTO " +
		"`mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, err_msg) " +
		"VALUES %s " +
		"ON DUPLICATE KEY UPDATE err_msg = VALUES(err_msg)"

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
		" account_id IN (%s) " +
		"%s" +
		"%s" +
		" AND relkind = '%s' " +
		" AND reldatabase NOT IN (%s)"
		/*
			CREATE TABLE mo_async_index_log (
				account_id INT UNSIGNED NOT NULL,//0
				table_id BIGINT UNSIGNED NOT NULL,//1
				index_name VARCHAR NOT NULL,//2
				last_sync_txn_ts VARCHAR(32)  NOT NULL,//3
				err_code INT NOT NULL,//4
				error_msg VARCHAR(255) NOT NULL,//5
				info VARCHAR(255) NOT NULL,//6
				drop_at DATETIME NULL,//7
				consumer_config VARCHAR(255) NULL,//8
			);
		*/
	CDCInsertMOAsyncIndexLogSqlTemplate = `REPLACE INTO mo_catalog.mo_async_index_log (` +
		`account_id,` +
		`table_id,` +
		`index_name,` +
		`last_sync_txn_ts,` +
		`err_code,` +
		`error_msg,` +
		`info,` +
		`consumer_config,` +
		`drop_at` +
		`) VALUES (` +
		`%d,` + // account_id
		`%d,` + // table_id
		`'%s',` + // index_name
		`'%s',` + // last_sync_txn_ts
		`%d,` + // err_code
		`'%s',` + // error_msg
		`'%s',` + // info
		`'%s',` + // consumer_config
		`null` + // drop_at
		`)`
	CDCUpdateMOAsyncIndexLogSqlTemplate = `UPDATE mo_catalog.mo_async_index_log SET ` +
		`err_code = %d,` +
		`error_msg = '%s',` +
		`last_sync_txn_ts = '%s'` +
		`WHERE` +
		` account_id = %d ` +
		`AND table_id = %d ` +
		`AND index_name = '%s'`
	CDCUpdateMOAsyncIndexLogDropAtSqlTemplate = `UPDATE mo_catalog.mo_async_index_log SET ` +
		`drop_at = now()` +
		`WHERE` +
		` account_id = %d ` +
		`AND table_id = %d ` +
		`AND index_name = '%s'`
	CDCDeleteMOAsyncIndexLogSqlTemplate = `DELETE FROM mo_catalog.mo_async_index_log WHERE ` +
		`drop_at < '%s'`
	CDCSelectMOAsyncIndexLogSqlTemplate        = `SELECT * from mo_catalog.mo_async_index_log`
	CDCSelectMOAsyncIndexLogByTableSqlTemplate = `SELECT drop_at from mo_catalog.mo_async_index_log WHERE ` +
		`account_id = %d ` +
		`AND table_id = %d ` +
		`AND index_name = '%s'`
	/*
		CREATE TABLE mo_async_index_iterations (
			account_id INT UNSIGNED NOT NULL,
			table_id BIGINT UNSIGNED NOT NULL,
			index_names VARCHAR(255),--multiple indexes
			from_ts VARCHAR(32) NOT NULL,
			to_ts VARCHAR(32) NOT NULL,
			error_json VARCHAR(255) NOT NULL,--Multiple errors are stored. Different consumers may have different errors.
			start_at DATETIME NULL,
			end_at DATETIME NULL,
		);
	*/
	CDCInsertMOAsyncIndexIterationsTemplate = `INSERT INTO mo_catalog.mo_async_index_iterations(` +
		`account_id,` +
		`table_id,` +
		`index_names,` +
		`from_ts,` +
		`to_ts,` +
		`error_json,` +
		`start_at,` +
		`end_at` +
		`) VALUES (` +
		`%d,` + // account_id
		`%d,` + // table_id
		`'%s',` + // index_names
		`'%s',` + // from_ts
		`'%s',` + // to_ts
		`'%s',` + // error_json
		`'%s',` + // start_at
		`'%s'` + // end_at
		`)`
	CDCDeleteMOAsyncIndexIterationsTemplate = `DELETE FROM mo_catalog.mo_async_index_iterations WHERE ` +
		`end_at < '%s'`
	/*  CREATE TABLE `mo_tables` (
		`rel_id` bigint unsigned DEFAULT NULL,
		`relname` varchar(5000) DEFAULT NULL,
		`reldatabase` varchar(5000) DEFAULT NULL,
		`reldatabase_id` bigint unsigned DEFAULT NULL,
		`relpersistence` varchar(5000) DEFAULT NULL,
		`relkind` varchar(5000) DEFAULT NULL,
		`rel_comment` varchar(5000) DEFAULT NULL,
		`rel_createsql` text DEFAULT NULL,
		`created_time` timestamp NULL DEFAULT NULL,
		`creator` int unsigned DEFAULT NULL,
		`owner` int unsigned DEFAULT NULL,
		`account_id` int unsigned DEFAULT NULL,
		`partitioned` tinyint DEFAULT NULL,
		`partition_info` blob DEFAULT NULL,
		`viewdef` varchar(5000) DEFAULT NULL,
		`constraint` varchar(5000) DEFAULT NULL,
		`rel_version` int unsigned DEFAULT NULL,
		`catalog_version` int unsigned DEFAULT NULL,
		PRIMARY KEY (`account_id`,`reldatabase`,`relname`)
	  )
	*/
	CDCGetTableIDTemplate = "SELECT " +
		"rel_id, " +
		"reldatabase_id " +
		"FROM `mo_catalog`.`mo_tables` " +
		"WHERE " +
		" account_id = %d " +
		" AND reldatabase = '%s' " +
		" AND relname = '%s' "
)

const (
	CDCInsertTaskSqlTemplate_Idx                    = 0
	CDCGetTaskSqlTemplate_Idx                       = 1
	CDCShowTaskSqlTemplate_Idx                      = 2
	CDCGetTaskIdSqlTemplate_Idx                     = 3
	CDCDeleteTaskSqlTemplate_Idx                    = 4
	CDCUpdateTaskStateSQL_Idx                       = 5
	CDCUpdateTaskStateAndErrMsgSQL_Idx              = 6
	CDCInsertWatermarkSqlTemplate_Idx               = 7
	CDCGetWatermarkSqlTemplate_Idx                  = 8
	CDCGetTableWatermarkSQL_Idx                     = 9
	CDCUpdateWatermarkSQL_Idx                       = 10
	CDCUpdateWatermarkErrMsgSQL_Idx                 = 11
	CDCDeleteWatermarkSqlTemplate_Idx               = 12
	CDCDeleteWatermarkByTableSqlTemplate_Idx        = 13
	CDCGetDataKeySQL_Idx                            = 14
	CDCCollectTableInfoSqlTemplate_Idx              = 15
	CDCGetWatermarkWhereSqlTemplate_Idx             = 16
	CDCOnDuplicateUpdateWatermarkTemplate_Idx       = 17
	CDCOnDuplicateUpdateWatermarkErrMsgTemplate_Idx = 18

	CDCInsertMOAsyncIndexLogSqlTemplate_Idx        = 19
	CDCUpdateMOAsyncIndexLogSqlTemplate_Idx        = 20
	CDCUpdateMOAsyncIndexLogDropAtSqlTemplate_Idx  = 21
	CDCDeleteMOAsyncIndexLogSqlTemplate_Idx        = 22
	CDCSelectMOAsyncIndexLogSqlTemplate_Idx        = 23
	CDCSelectMOAsyncIndexLogByTableSqlTemplate_Idx = 24

	CDCInsertMOAsyncIndexIterationsTemplate_Idx = 25
	CDCDeleteMOAsyncIndexIterationsTemplate_Idx = 26

	CDCGetTableIDTemplate_Idx = 27

	CDCSqlTemplateCount = 28
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
		SQL: CDCCollectTableInfoSqlTemplate,
		OutputAttrs: []string{
			"rel_id",
			"relname",
			"reldatabase_id",
			"reldatabase",
			"rel_createsql",
			"account_id",
		},
	},

	CDCInsertMOAsyncIndexLogSqlTemplate_Idx: {
		SQL: CDCInsertMOAsyncIndexLogSqlTemplate,
	},
	CDCUpdateMOAsyncIndexLogSqlTemplate_Idx: {
		SQL: CDCUpdateMOAsyncIndexLogSqlTemplate,
	},
	CDCUpdateMOAsyncIndexLogDropAtSqlTemplate_Idx: {
		SQL: CDCUpdateMOAsyncIndexLogDropAtSqlTemplate,
	},
	CDCDeleteMOAsyncIndexLogSqlTemplate_Idx: {
		SQL: CDCDeleteMOAsyncIndexLogSqlTemplate,
	},
	CDCSelectMOAsyncIndexLogSqlTemplate_Idx: {
		SQL: CDCSelectMOAsyncIndexLogSqlTemplate,
		OutputAttrs: []string{
			"id",
			"account_id",
			"table_id",
			"db_id",
			"index_name",
			"last_sync_txn_ts",
			"err_code",
			"error_msg",
			"info",
			"drop_at",
			"consumer_config",
		},
	},
	CDCSelectMOAsyncIndexLogByTableSqlTemplate_Idx: {
		SQL: CDCSelectMOAsyncIndexLogByTableSqlTemplate,
		OutputAttrs: []string{
			"drop_at",
		},
	},
	CDCInsertMOAsyncIndexIterationsTemplate_Idx: {
		SQL: CDCInsertMOAsyncIndexIterationsTemplate,
	},
	CDCDeleteMOAsyncIndexIterationsTemplate_Idx: {
		SQL: CDCDeleteMOAsyncIndexIterationsTemplate,
	},
	CDCGetWatermarkWhereSqlTemplate_Idx: {
		SQL: CDCGetWatermarkWhereSqlTemplate,
	},
	CDCOnDuplicateUpdateWatermarkTemplate_Idx: {
		SQL: CDCOnDuplicateUpdateWatermarkTemplate,
	},
	CDCOnDuplicateUpdateWatermarkErrMsgTemplate_Idx: {
		SQL: CDCOnDuplicateUpdateWatermarkErrMsgTemplate,
	},
	CDCGetTableIDTemplate_Idx: {
		SQL: CDCGetTableIDTemplate,
		OutputAttrs: []string{
			"rel_id",
			"reldatabase_id",
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
	values := fmt.Sprintf(
		"(%d, '%s', '%s', '%s', '%s', '%s')",
		accountId,
		taskId,
		dbName,
		tableName,
		watermark,
		"",
	)
	return fmt.Sprintf(
		CDCSQLTemplates[CDCInsertWatermarkSqlTemplate_Idx].SQL,
		values,
	)
}

func (b cdcSQLBuilder) InsertWatermarkWithValuesSQL(
	values string,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCInsertWatermarkSqlTemplate_Idx].SQL,
		values,
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

func (b cdcSQLBuilder) GetWatermarkWhereSQL(
	projectionStr string,
	whereStr string,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCGetWatermarkWhereSqlTemplate_Idx].SQL,
		projectionStr,
		whereStr,
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

func (b cdcSQLBuilder) OnDuplicateUpdateWatermarkSQL(
	values string,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCOnDuplicateUpdateWatermarkTemplate_Idx].SQL,
		values,
	)
}

func (b cdcSQLBuilder) OnDuplicateUpdateWatermarkErrMsgSQL(
	values string,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCOnDuplicateUpdateWatermarkErrMsgTemplate_Idx].SQL,
		values,
	)
}

// ------------------------------------------------------------------------------------------------
// Async Index Log SQL
// ------------------------------------------------------------------------------------------------

func (b cdcSQLBuilder) AsyncIndexLogInsertSQL(
	accountID uint32,
	tableID uint64,
	indexName string,
	info string,
	consumerConfig string,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCInsertMOAsyncIndexLogSqlTemplate_Idx].SQL,
		accountID,
		tableID,
		indexName,
		types.TS{}.ToString(),
		0,
		"",
		info,
		consumerConfig,
	)
}

func (b cdcSQLBuilder) AsyncIndexLogUpdateResultSQL(
	accountID uint32,
	tableID uint64,
	indexName string,
	newWatermark types.TS,
	errorCode int,
	errorMsg string,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCUpdateMOAsyncIndexLogSqlTemplate_Idx].SQL,
		errorCode,
		errorMsg,
		newWatermark.ToString(),
		accountID,
		tableID,
		indexName,
	)
}

func (b cdcSQLBuilder) AsyncIndexLogUpdateDropAtSQL(
	accountID uint32,
	tableID uint64,
	indexName string,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCUpdateMOAsyncIndexLogDropAtSqlTemplate_Idx].SQL,
		accountID,
		tableID,
		indexName,
	)
}

func (b cdcSQLBuilder) AsyncIndexLogGCSQL(t time.Time) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCDeleteMOAsyncIndexLogSqlTemplate_Idx].SQL,
		t.Format(time.DateTime),
	)
}

func (b cdcSQLBuilder) AsyncIndexLogSelectSQL() string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCSelectMOAsyncIndexLogSqlTemplate_Idx].SQL,
	)
}

func (b cdcSQLBuilder) AsyncIndexLogSelectByTableSQL(
	accountID uint32,
	tableID uint64,
	indexName string,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCSelectMOAsyncIndexLogByTableSqlTemplate_Idx].SQL,
		accountID,
		tableID,
		indexName,
	)
}

// ------------------------------------------------------------------------------------------------
// Async Index Iterations SQL
// ------------------------------------------------------------------------------------------------

func (b cdcSQLBuilder) AsyncIndexIterationsInsertSQL(
	accountID uint32,
	tableID uint64,
	indexNames string,
	fromTs types.TS,
	toTs types.TS,
	errorJson string,
	startAt time.Time,
	endAt time.Time,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCInsertMOAsyncIndexIterationsTemplate_Idx].SQL,
		accountID,
		tableID,
		indexNames,
		fromTs.ToString(),
		toTs.ToString(),
		errorJson,
		startAt.Format(time.DateTime),
		endAt.Format(time.DateTime),
	)
}
func (b cdcSQLBuilder) AsyncIndexIterationsGCSQL(t time.Time) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCDeleteMOAsyncIndexIterationsTemplate_Idx].SQL,
		t.Format(time.DateTime),
	)
}

// ------------------------------------------------------------------------------------------------
// Table Info SQL
// ------------------------------------------------------------------------------------------------
func (b cdcSQLBuilder) CollectTableInfoSQL(accountIDs string, dbNames string, tableNames string) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCCollectTableInfoSqlTemplate_Idx].SQL,
		accountIDs,
		func() string {
			if dbNames == "*" {
				return ""
			}
			return " AND reldatabase IN (" + dbNames + ") "
		}(),
		func() string {
			if tableNames == "*" {
				return ""
			}
			return " AND relname IN (" + tableNames + ") "
		}(),
		catalog.SystemOrdinaryRel,
		AddSingleQuotesJoin(catalog.SystemDatabases),
	)
}

func (b cdcSQLBuilder) GetTableIDSQL(
	accountID uint32,
	dbName string,
	tableName string,
) string {
	return fmt.Sprintf(
		CDCSQLTemplates[CDCGetTableIDTemplate_Idx].SQL,
		accountID,
		dbName,
		tableName,
	)
}
