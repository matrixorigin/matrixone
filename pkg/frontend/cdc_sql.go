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

package frontend

import (
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
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
		`FROM mo_catalog.mo_cdc_task ` +
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
		`FROM mo_catalog.mo_cdc_task ` +
		`WHERE ` +
		`account_id = %d`

	CDCGetCdcTaskIdSqlTemplate = `SELECT ` +
		`task_id ` +
		`FROM mo_catalog.mo_cdc_task ` +
		`WHERE ` +
		`account_id = %d`

	CDCDeleteCdcMetaSqlTemplate = "DELETE FROM `mo_catalog`.mo_cdc_task WHERE 1=1"

	CDCUpdateCdcMetaSqlTemplate = "UPDATE `mo_catalog`.mo_cdc_task SET state = ? WHERE 1=1"

	CDCDeleteWatermarkSqlTemplate = "DELETE FROM `mo_catalog`.`mo_cdc_watermark` WHERE account_id = %d AND task_id = '%s'"

	CDCGetWatermarkSqlTemplate = "SELECT db_name, table_name, watermark, err_msg FROM mo_catalog.mo_cdc_watermark WHERE account_id = %d and task_id = '%s'"

	CDCUpdateErrMsgSqlTemplate = "UPDATE `mo_catalog`.mo_cdc_task SET state = ?, err_msg = ? WHERE account_id = %d and task_id = '%s'"

	CDCGetDataKeySqlTemplate = "SELECT encrypted_key FROM mo_catalog.mo_data_key WHERE account_id = %d and key_id = '%s'"

	CDCGetDbIdAndTableIdSqlTemplate = `SELECT ` +
		`reldatabase_id, ` +
		`rel_id ` +
		`FROM mo_catalog.mo_tables ` +
		`WHERE ` +
		`account_id = %d AND ` +
		`reldatabase = '%s' AND ` +
		`relname = '%s'`

	CDCGetTableSqlTemplate = `SELECT ` +
		`rel_id ` +
		`FROM mo_catalog.mo_tables ` +
		`WHERE ` +
		`account_id = %d AND ` +
		`reldatabase = '%s' AND ` +
		`relname = '%s'`

	CDCGetAccountIdSqlTemplate = `SELECT ` +
		`account_id ` +
		`FROM mo_catalog.mo_account ` +
		`WHERE ` +
		`account_name = '%s'`

	CDCGetPKCountSqlTemplate = `SELECT ` +
		`COUNT(att_constraint_type) ` +
		`FROM mo_catalog.mo_columns ` +
		`WHERE ` +
		`account_id = %d AND ` +
		`att_database = '%s' AND ` +
		`att_relname = '%s' AND ` +
		`att_constraint_type = '%s'`
)

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
		CDCInsertTaskSqlTemplate,
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
		CDCGetTaskSqlTemplate,
		accountId,
		taskId,
	)
}

func (b cdcSQLBuilder) ShowCdcTaskSQL(
	accountId uint64,
) string {
	return fmt.Sprintf(
		CDCShowCdcTaskSqlTemplate,
		accountId,
	)
}

// ------------------------------------------------------------------------------------------------
// Table SQL
// ------------------------------------------------------------------------------------------------
func (b cdcSQLBuilder) GetDbIdAndTableIdSQL(
	accountId uint64,
	db string,
	table string,
) string {
	return fmt.Sprintf(
		CDCGetDbIdAndTableIdSqlTemplate,
		accountId,
		db,
		table,
	)
}

func (b cdcSQLBuilder) GetTableSQL(
	accountId uint64,
	db string,
	table string,
) string {
	return fmt.Sprintf(
		CDCGetTableSqlTemplate,
		accountId,
		db,
		table,
	)
}

func (b cdcSQLBuilder) GetAccountIdSQL(
	account string,
) string {
	return fmt.Sprintf(
		CDCGetAccountIdSqlTemplate,
		account,
	)
}

func (b cdcSQLBuilder) GetPkCountSQL(
	accountId uint64,
	db string,
	table string,
) string {
	return fmt.Sprintf(
		CDCGetPKCountSqlTemplate,
		accountId,
		db,
		table,
		catalog.SystemColPKConstraint,
	)
}

// ------------------------------------------------------------------------------------------------
// Watermark SQL
// ------------------------------------------------------------------------------------------------
func (b cdcSQLBuilder) DeleteWatermarkSQL(
	accountId uint64,
	taskId string,
) string {
	return fmt.Sprintf(
		CDCDeleteWatermarkSqlTemplate,
		accountId,
		taskId,
	)
}

func (b cdcSQLBuilder) GetWatermarkSQL(
	accountId uint64,
	taskId string,
) string {
	return fmt.Sprintf(
		CDCGetWatermarkSqlTemplate,
		accountId,
		taskId,
	)
}
