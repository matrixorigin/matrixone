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
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	cdc2 "github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	insertNewCdcTaskFormat = `insert into mo_catalog.mo_cdc_task values(` +
		`%d,` + //account id
		`"%s",` + //task id
		`"%s",` + //task name
		`"%s",` + //source_uri
		`"%s",` + //source_password
		`"%s",` + //sink_uri
		`"%s",` + //sink_type
		`"%s",` + //sink_password
		`"%s",` + //sink_ssl_ca_path
		`"%s",` + //sink_ssl_cert_path
		`"%s",` + //sink_ssl_key_path
		`"%s",` + //tables
		`"%s",` + //filters
		`"%s",` + //opfilters
		`"%s",` + //source_state
		`"%s",` + //sink_state
		`"%s",` + //start_ts
		`"%s",` + //end_ts
		`"%s",` + //config_file
		`"%s",` + //task_create_time
		`"%s",` + //state
		`%d,` + //checkpoint
		`"%d",` + //checkpoint_str
		`"%t",` + //no_full
		`"%s",` + //incr_config
		`"",` + //reserved0
		`"",` + //reserved1
		`"",` + //reserved2
		`"",` + //reserved3
		`""` + //reserved4
		`)`

	getCdcTaskFormat = `select ` +
		`sink_uri, ` +
		`sink_type, ` +
		`sink_password, ` +
		`tables, ` +
		`filters, ` +
		`no_full ` +
		`from ` +
		`mo_catalog.mo_cdc_task ` +
		`where ` +
		`account_id = %d and ` +
		`task_id = "%s"`

	getShowCdcTaskFormat = "select task_id, task_name, source_uri, sink_uri, state from mo_catalog.mo_cdc_task where account_id = %d"

	getDbIdAndTableIdFormat = "select reldatabase_id,rel_id from mo_catalog.mo_tables where account_id = %d and reldatabase = '%s' and relname = '%s'"

	getTableFormat = "select rel_id from `mo_catalog`.`mo_tables` where account_id = %d and reldatabase ='%s' and relname = '%s'"

	getAccountIdFormat = "select account_id from `mo_catalog`.`mo_account` where account_name='%s'"

	getPkCountFormat = "select count(att_constraint_type) from `mo_catalog`.`mo_columns` where account_id = %d and att_database = '%s' and att_relname = '%s' and att_constraint_type = '%s'"

	getCdcTaskIdFormat = "select task_id from `mo_catalog`.`mo_cdc_task` where 1=1"

	deleteCdcMetaFormat = "delete from `mo_catalog`.`mo_cdc_task` where 1=1"

	updateCdcMetaFormat = "update `mo_catalog`.`mo_cdc_task` set state = ? where 1=1"

	deleteWatermarkFormat = "delete from `mo_catalog`.`mo_cdc_watermark` where account_id = %d and task_id = '%s'"

	getWatermarkFormat = `
		select 
			t.reldatabase, t.relname, w.watermark
		from 
			mo_catalog.mo_cdc_watermark w 
		join 
			mo_catalog.mo_tables t 
		on w.table_id = t.rel_id 
		where w.account_id = %d and w.task_id = '%s'`
)

var showCdcOutputColumns = [7]Column{
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "task_id",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "task_name",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "source_uri",
			columnType: defines.MYSQL_TYPE_TEXT,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "sink_uri",
			columnType: defines.MYSQL_TYPE_TEXT,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "state",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "checkpoint",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
	&MysqlColumn{
		ColumnImpl: ColumnImpl{
			name:       "timestamp",
			columnType: defines.MYSQL_TYPE_VARCHAR,
		},
	},
}

func getSqlForNewCdcTask(
	accId uint64,
	taskId uuid.UUID,
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
) string {
	return fmt.Sprintf(insertNewCdcTaskFormat,
		accId,
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
	)
}

func getSqlForRetrievingCdcTask(
	accId uint64,
	taskId uuid.UUID,
) string {
	return fmt.Sprintf(getCdcTaskFormat, accId, taskId)
}

func getSqlForDbIdAndTableId(accId uint64, db, table string) string {
	return fmt.Sprintf(getDbIdAndTableIdFormat, accId, db, table)
}

func getSqlForGetTable(accountId uint64, db, table string) string {
	return fmt.Sprintf(getTableFormat, accountId, db, table)
}

func getSqlForCheckAccount(account string) string {
	return fmt.Sprintf(getAccountIdFormat, account)
}

func getSqlForGetPkCount(accountId uint64, db, table string) string {
	return fmt.Sprintf(getPkCountFormat, accountId, db, table, catalog.SystemColPKConstraint)
}

func getSqlForDeleteWatermark(accountId uint64, taskId string) string {
	return fmt.Sprintf(deleteWatermarkFormat, accountId, taskId)
}

func getSqlForGetWatermark(accountId uint64, taskId string) string {
	return fmt.Sprintf(getWatermarkFormat, accountId, taskId)
}

func getSqlForGetTask(accountId uint64, all bool, taskName string) string {
	s := fmt.Sprintf(getShowCdcTaskFormat, accountId)
	if !all {
		s += fmt.Sprintf(" and task_name = '%s'", taskName)
	}
	return s
}

const (
	CdcRunning = "running"
	CdcStopped = "stopped"
)

func handleCreateCdc(ses *Session, execCtx *ExecCtx, create *tree.CreateCDC) error {
	return doCreateCdc(execCtx.reqCtx, ses, create)
}

func doCreateCdc(ctx context.Context, ses *Session, create *tree.CreateCDC) (err error) {
	ts := getGlobalPu().TaskService
	if ts == nil {
		return moerr.NewInternalError(ctx, "no task service is found")
	}

	cdcTaskOptionsMap := make(map[string]string)
	for i := 0; i < len(create.Option); i += 2 {
		cdcTaskOptionsMap[create.Option[i]] = create.Option[i+1]
	}

	cdcLevel := cdcTaskOptionsMap["Level"]
	cdcAccount := cdcTaskOptionsMap["Account"]

	if cdcLevel != cdc2.AccountLevel {
		return moerr.NewInternalError(ctx, "invalid level. only support account level in 1.3")
	}

	if cdcAccount != ses.GetTenantInfo().GetTenant() {
		return moerr.NewInternalErrorf(ctx, "invalid account. account must be %s", ses.GetTenantInfo().GetTenant())
	}

	////////////
	//!!!NOTE!!!
	//1.3
	//	level: account level
	//	account: must be designated.
	///////////

	//step 1 : handle tables
	tablePts, err := preprocessTables(
		ctx,
		ses,
		cdcLevel,
		cdcAccount,
		create.Tables,
	)
	if err != nil {
		return err
	}

	//step 2: handle filters
	//There must be no special characters (',' '.' ':' '`') in the single rule.
	filters := cdcTaskOptionsMap["Rules"]

	jsonFilters, filterPts, err := preprocessRules(ctx, filters)
	if err != nil {
		return err
	}

	err = attachAccountToFilters(ctx, ses, cdcLevel, cdcAccount, filterPts)
	if err != nil {
		return err
	}

	//TODO: refine it after 1.3
	//check table be filtered or not
	if filterTable(tablePts, filterPts) == 0 {
		return moerr.NewInternalError(ctx, "all tables has been excluded by filters. create cdc failed.")
	}

	dat := time.Now().UTC()

	creatorAccInfo := ses.GetTenantInfo()
	cdcId, _ := uuid.NewV7()

	//step 4: check uri format and strip password
	jsonSrcUri, _, err := extractUriInfo(ctx, create.SourceUri, cdc2.SourceUriPrefix)
	if err != nil {
		return err
	}

	sinkType := strings.ToLower(create.SinkType)
	useConsole := false
	if cdc2.EnableConsoleSink && sinkType == cdc2.ConsoleSink {
		useConsole = true
	}

	if !useConsole && sinkType != cdc2.MysqlSink && sinkType != cdc2.MatrixoneSink {
		return moerr.NewInternalErrorf(ctx, "unsupported sink type: %s", create.SinkType)
	}

	var jsonSinkUri string
	var encodedSinkPwd string
	var sinkUriInfo cdc2.UriInfo
	if !useConsole {
		jsonSinkUri, sinkUriInfo, err = extractUriInfo(ctx, create.SinkUri, cdc2.SinkUriPrefix)
		if err != nil {
			return err
		}

		encodedSinkPwd, err = sinkUriInfo.GetEncodedPassword()
		if err != nil {
			return err
		}
	}

	noFull := false
	if cdcTaskOptionsMap["NoFull"] == "true" {
		noFull = true
	}

	details := &task.Details{
		//account info that create cdc
		AccountID: creatorAccInfo.GetTenantID(),
		Account:   creatorAccInfo.GetTenant(),
		Username:  creatorAccInfo.GetUser(),
		Details: &task.Details_CreateCdc{
			CreateCdc: &task.CreateCdcDetails{
				TaskName: create.TaskName.String(),
				TaskId:   cdcId.String(),
				Accounts: []*task.Account{
					{
						Id:   uint64(creatorAccInfo.GetTenantID()),
						Name: cdcAccount,
					},
				},
			},
		},
	}

	addCdcTaskCallback := func(ctx context.Context, tx taskservice.SqlExecutor) (ret int, err error) {
		err = checkAccounts(ctx, tx, tablePts, filterPts)
		if err != nil {
			return 0, err
		}

		ret, err = checkTables(ctx, tx, tablePts, filterPts)
		if err != nil {
			return
		}

		jsonTables, err := cdc2.JsonEncode(tablePts)
		if err != nil {
			return 0, err
		}

		//step 5: create daemon task
		insertSql := getSqlForNewCdcTask(
			uint64(creatorAccInfo.GetTenantID()), //the account_id of cdc creator
			cdcId,
			create.TaskName.String(),
			jsonSrcUri, //json bytes
			"",
			jsonSinkUri, //json bytes
			sinkType,
			encodedSinkPwd, //encrypted password
			"",
			"",
			"",
			jsonTables,
			jsonFilters,
			"",
			cdc2.SASCommon,
			cdc2.SASCommon,
			"", //1.3 does not support startTs
			"", //1.3 does not support endTs
			cdcTaskOptionsMap["ConfigFile"],
			dat,
			CdcRunning,
			0,
			noFull,
			"",
		)

		//insert cdc record into the mo_cdc_task
		exec, err := tx.ExecContext(ctx, insertSql)
		if err != nil {
			return 0, err
		}

		cdcTaskRowsAffected, err := exec.RowsAffected()
		if err != nil {
			return 0, err
		}
		return int(cdcTaskRowsAffected), nil
	}

	if _, err = ts.AddCdcTask(ctx, cdcTaskMetadata(cdcId.String()), details, addCdcTaskCallback); err != nil {
		return err
	}
	return
}

func cdcTaskMetadata(cdcId string) task.TaskMetadata {
	return task.TaskMetadata{
		ID:       cdcId,
		Executor: task.TaskCode_InitCdc,
		Options: task.TaskOptions{
			MaxRetryTimes: defaultConnectorTaskMaxRetryTimes,
			RetryInterval: defaultConnectorTaskRetryInterval,
			DelayDuration: 0,
			Concurrency:   0,
		},
	}
}

// checkAccounts checks the accounts exists or not
func checkAccounts(ctx context.Context, tx taskservice.SqlExecutor, tablePts, filterPts *cdc2.PatternTuples) error {
	//step1 : collect accounts
	accounts := make(map[string]uint64)
	for _, pt := range tablePts.Pts {
		if pt == nil || pt.Source.Account == "" {
			continue
		}
		accounts[pt.Source.Account] = math.MaxUint64
	}

	for _, pt := range filterPts.Pts {
		if pt == nil || pt.Source.Account == "" {
			continue
		}
		accounts[pt.Source.Account] = math.MaxUint64
	}

	//step2 : collect account id
	//after this step, all account has accountid
	res := make(map[string]uint64)
	for acc := range accounts {
		exists, accId, err := checkAccountExists(ctx, tx, acc)
		if err != nil {
			return err
		}
		if !exists {
			return moerr.NewInternalErrorf(ctx, "account %s does not exist", acc)
		}
		res[acc] = accId
	}

	//step3: attach accountId
	for _, pt := range tablePts.Pts {
		if pt == nil || pt.Source.Account == "" {
			continue
		}
		pt.Source.AccountId = res[pt.Source.Account]
	}

	for _, pt := range filterPts.Pts {
		if pt == nil || pt.Source.Account == "" {
			continue
		}
		pt.Source.AccountId = res[pt.Source.Account]
	}

	return nil
}

// checkTables
// checks the table existed or not
// checks the table having the primary key
// filters the table
func checkTables(ctx context.Context, tx taskservice.SqlExecutor, tablePts, filterPts *cdc2.PatternTuples) (int, error) {
	var err error
	var found bool
	var hasPrimaryKey bool
	for _, pt := range tablePts.Pts {
		if pt == nil {
			continue
		}

		//skip tables that is filtered
		if needSkipThisTable(pt.Source.Account, pt.Source.Database, pt.Source.Table, filterPts) {
			continue
		}

		//check tables exists or not and filter the table
		found, err = checkTableExists(ctx, tx, pt.Source.AccountId, pt.Source.Database, pt.Source.Table)
		if err != nil {
			return 0, err
		}
		if !found {
			return 0, moerr.NewInternalErrorf(ctx, "no table %s:%s", pt.Source.Database, pt.Source.Table)
		}

		//check table has primary key
		hasPrimaryKey, err = checkPrimaryKey(ctx, tx, pt.Source.AccountId, pt.Source.Database, pt.Source.Table)
		if err != nil {
			return 0, err
		}
		if !hasPrimaryKey {
			return 0, moerr.NewInternalErrorf(ctx, "table %s:%s does not have primary key", pt.Source.Database, pt.Source.Table)
		}
	}
	return 0, err
}

// filterTable checks the table filtered or not
// returns the count of tables that not be filtered
func filterTable(tablePts, filterPts *cdc2.PatternTuples) int {
	//check table be filtered or not
	leftCount := 0
	for _, pt := range tablePts.Pts {
		if pt == nil {
			continue
		}

		//skip tables that is filtered
		if needSkipThisTable(pt.Source.Account, pt.Source.Database, pt.Source.Table, filterPts) {
			continue
		}
		leftCount++
	}
	return leftCount
}

func queryTable(
	ctx context.Context,
	tx taskservice.SqlExecutor,
	query string,
	callback func(ctx context.Context, rows *sql.Rows) (bool, error)) (bool, error) {
	var rows *sql.Rows
	var err error
	rows, err = tx.QueryContext(ctx, query)
	if err != nil {
		return false, err
	}
	if rows.Err() != nil {
		return false, rows.Err()
	}
	defer func() {
		_ = rows.Close()
	}()

	var ret bool
	for rows.Next() {
		ret, err = callback(ctx, rows)
		if err != nil {
			return false, err
		}
		if ret {
			return true, nil
		}
	}
	return false, nil
}

func checkAccountExists(ctx context.Context, tx taskservice.SqlExecutor, account string) (bool, uint64, error) {
	checkSql := getSqlForCheckAccount(account)
	var err error
	var ret bool
	var accountId uint64
	ret, err = queryTable(ctx, tx, checkSql, func(ctx context.Context, rows *sql.Rows) (bool, error) {
		accountId = 0
		if err = rows.Scan(&accountId); err != nil {
			return false, err
		}
		return true, nil
	})

	return ret, accountId, err
}

func checkTableExists(ctx context.Context, tx taskservice.SqlExecutor, accountId uint64, db, table string) (bool, error) {
	//select from mo_tables
	checkSql := getSqlForGetTable(accountId, db, table)
	var err error
	var ret bool
	var tableId uint64
	ret, err = queryTable(ctx, tx, checkSql, func(ctx context.Context, rows *sql.Rows) (bool, error) {
		tableId = 0
		if err = rows.Scan(&tableId); err != nil {
			return false, err
		}
		return true, nil
	})

	return ret, err
}

func checkPrimaryKey(ctx context.Context, tx taskservice.SqlExecutor, accountId uint64, db, table string) (bool, error) {
	checkSql := getSqlForGetPkCount(accountId, db, table)
	var ret bool
	var err error
	var pkCount uint64

	ret, err = queryTable(ctx, tx, checkSql, func(ctx context.Context, rows *sql.Rows) (bool, error) {
		pkCount = 0

		if err = rows.Scan(
			&pkCount,
		); err != nil {
			return false, err
		}
		if pkCount > 0 {
			return true, nil
		}
		return false, nil
	})

	return ret, err
}

// extractTablePair
// extract source:sink pair from the pattern
//
//	There must be no special characters (','  '.'  ':' '`') in account name & database name & table name.
func extractTablePair(ctx context.Context, pattern string, defaultAcc string) (*cdc2.PatternTuple, error) {
	var err error
	pattern = strings.TrimSpace(pattern)
	//step1 : split table pair by ':' => table0 table1
	//step2 : split table0/1 by '.' => account database table
	//step3 : check table accord with regular expression
	pt := &cdc2.PatternTuple{OriginString: pattern}
	if strings.Contains(pattern, ":") {
		//Format: account.db.table:db.table
		splitRes := strings.Split(pattern, ":")
		if len(splitRes) != 2 {
			return nil, moerr.NewInternalErrorf(ctx, "must be source : sink. invalid format")
		}

		//handle source part
		pt.Source.Account, pt.Source.Database, pt.Source.Table, pt.Source.TableIsRegexp, err = extractTableInfo(ctx, splitRes[0], false)
		if err != nil {
			return nil, err
		}
		if pt.Source.Account == "" {
			pt.Source.Account = defaultAcc
		}

		//handle sink part
		pt.Sink.Account, pt.Sink.Database, pt.Sink.Table, pt.Sink.TableIsRegexp, err = extractTableInfo(ctx, splitRes[1], false)
		if err != nil {
			return nil, err
		}
		if pt.Sink.Account == "" {
			pt.Sink.Account = defaultAcc
		}
		return pt, nil
	}

	//Format: account.db.table
	//handle source part only
	pt.Source.Account, pt.Source.Database, pt.Source.Table, pt.Source.TableIsRegexp, err = extractTableInfo(ctx, pattern, false)
	if err != nil {
		return nil, err
	}
	if pt.Source.Account == "" {
		pt.Source.Account = defaultAcc
	}

	pt.Sink.AccountId = pt.Source.AccountId
	pt.Sink.Account = pt.Source.Account
	pt.Sink.Database = pt.Source.Database
	pt.Sink.Table = pt.Source.Table
	pt.Sink.TableIsRegexp = pt.Source.TableIsRegexp
	return pt, nil
}

// extractTableInfo
// get account,database,table info from string
//
// account: may be empty
// database: must be concrete name instead of pattern.
// table: must be concrete name or pattern in the source part. must be concrete name in the destination part
// isRegexpTable: table name is regular expression
// !!!NOTE!!!
//
//	There must be no special characters (','  '.'  ':' '`') in account name & database name & table name.
func extractTableInfo(ctx context.Context, input string, mustBeConcreteTable bool) (account string, db string, table string, isRegexpTable bool, err error) {
	parts := strings.Split(strings.TrimSpace(input), ".")
	if len(parts) != 2 && len(parts) != 3 {
		err = moerr.NewInternalErrorf(ctx, "needs account.database.table or database.table. invalid format.")
		return
	}

	if len(parts) == 2 {
		db, table = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	} else {
		account, db, table = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), strings.TrimSpace(parts[2])

		if !accountNameIsLegal(account) {
			err = moerr.NewInternalErrorf(ctx, "invalid account name")
			return
		}
	}

	if !dbNameIsLegal(db) {
		err = moerr.NewInternalErrorf(ctx, "invalid database name")
		return
	}

	if !tableNameIsLegal(table) {
		err = moerr.NewInternalErrorf(ctx, "invalid table name")
		return
	}

	return
}

// preprocessTables extract tables and serialize them
func preprocessTables(
	ctx context.Context,
	ses *Session,
	level string,
	account string,
	tables string,
) (*cdc2.PatternTuples, error) {
	tablesPts, err := extractTablePairs(ctx, tables, account)
	if err != nil {
		return nil, err
	}

	//step 2: check privilege
	if err = canCreateCdcTask(ctx, ses, level, account, tablesPts); err != nil {
		return nil, err
	}

	return tablesPts, nil
}

/*
extractTablePairs extracts all source:sink pairs from the pattern
There must be no special characters (','  '.'  ':' '`') in account name & database name & table name.
*/
func extractTablePairs(ctx context.Context, pattern string, defaultAcc string) (*cdc2.PatternTuples, error) {
	pattern = strings.TrimSpace(pattern)
	pts := &cdc2.PatternTuples{}

	tablePairs := strings.Split(pattern, ",")
	if len(tablePairs) == 0 {
		return nil, moerr.NewInternalError(ctx, "invalid pattern format")
	}

	//step1 : split pattern by ',' => table pair
	for _, pair := range tablePairs {
		pt, err := extractTablePair(ctx, pair, defaultAcc)
		if err != nil {
			return nil, err
		}
		pts.Append(pt)
	}

	return pts, nil
}

func preprocessRules(ctx context.Context, rules string) (string, *cdc2.PatternTuples, error) {
	pts, err := extractRules(ctx, rules)
	if err != nil {
		return "", nil, err
	}

	jsonPts, err := cdc2.JsonEncode(pts)
	if err != nil {
		return "", nil, err
	}
	return jsonPts, pts, nil
}

/*
extractRules extracts filters
pattern maybe empty string. then, it returns empty PatternTuples
There must be no special characters (','  '.'  ':' '`') in account name & database name & table name.
*/
func extractRules(ctx context.Context, pattern string) (*cdc2.PatternTuples, error) {
	pattern = strings.TrimSpace(pattern)
	pts := &cdc2.PatternTuples{}
	if len(pattern) == 0 {
		return pts, nil
	}

	tablePairs := strings.Split(pattern, ",")
	if len(tablePairs) == 0 {
		return nil, moerr.NewInternalError(ctx, "invalid pattern format")
	}
	var err error
	//step1 : split pattern by ',' => table pair
	for _, pair := range tablePairs {
		pt := &cdc2.PatternTuple{}
		pt.Source.Account, pt.Source.Database, pt.Source.Table, pt.Source.TableIsRegexp, err = extractTableInfo(ctx, pair, false)
		if err != nil {
			return nil, err
		}
		pts.Append(pt)
	}

	return pts, nil
}

func canCreateCdcTask(ctx context.Context, ses *Session, level string, account string, pts *cdc2.PatternTuples) error {
	if strings.EqualFold(level, cdc2.ClusterLevel) {
		if !ses.tenant.IsMoAdminRole() {
			return moerr.NewInternalError(ctx, "Only sys account administrator are allowed to create cluster level task")
		}
		for _, pt := range pts.Pts {
			if isBannedDatabase(pt.Source.Database) {
				return moerr.NewInternalError(ctx, "The system database cannot be subscribed to")
			}
		}
	} else if strings.EqualFold(level, cdc2.AccountLevel) {
		if !ses.tenant.IsMoAdminRole() && ses.GetTenantName() != account {
			return moerr.NewInternalErrorf(ctx, "No privilege to create task on %s", account)
		}
		for _, pt := range pts.Pts {
			if account != pt.Source.Account {
				return moerr.NewInternalErrorf(ctx, "No privilege to create task on table %s", pt.OriginString)
			}
			if isBannedDatabase(pt.Source.Database) {
				return moerr.NewInternalError(ctx, "The system database cannot be subscribed to")
			}
		}
	} else {
		return moerr.NewInternalErrorf(ctx, "Incorrect level %s", level)
	}
	return nil
}

func attachAccountToFilters(ctx context.Context, ses *Session, level string, account string, pts *cdc2.PatternTuples) error {
	if strings.EqualFold(level, cdc2.ClusterLevel) {
		if !ses.tenant.IsMoAdminRole() {
			return moerr.NewInternalError(ctx, "Only sys account administrator are allowed to create cluster level task")
		}
		for _, pt := range pts.Pts {
			if pt.Source.Account == "" {
				pt.Source.Account = ses.GetTenantName()
			}
		}
	} else if strings.EqualFold(level, cdc2.AccountLevel) {
		if !ses.tenant.IsMoAdminRole() && ses.GetTenantName() != account {
			return moerr.NewInternalErrorf(ctx, "No privilege to create task on %s", account)
		}
		for _, pt := range pts.Pts {
			if pt.Source.Account == "" {
				pt.Source.Account = account
			}
			if account != pt.Source.Account {
				return moerr.NewInternalErrorf(ctx, "No privilege to create task on table %s", pt.OriginString)
			}
		}
	} else {
		return moerr.NewInternalErrorf(ctx, "Incorrect level %s", level)
	}
	return nil
}

func RegisterCdcExecutor(
	logger *zap.Logger,
	ts taskservice.TaskService,
	ieFactory func() ie.InternalExecutor,
	attachToTask func(context.Context, uint64, taskservice.ActiveRoutine) error,
	cnUUID string,
	fileService fileservice.FileService,
	cnTxnClient client.TxnClient,
	cnEngine engine.Engine,
	cnEngMp *mpool.MPool,
) func(ctx context.Context, task task.Task) error {
	return func(ctx context.Context, T task.Task) error {
		ctx1, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		tasks, err := ts.QueryDaemonTask(ctx1,
			taskservice.WithTaskIDCond(taskservice.EQ, T.GetID()),
		)
		if err != nil {
			return err
		}
		if len(tasks) != 1 {
			return moerr.NewInternalErrorf(ctx, "invalid tasks count %d", len(tasks))
		}
		details, ok := tasks[0].Details.Details.(*task.Details_CreateCdc)
		if !ok {
			return moerr.NewInternalError(ctx, "invalid details type")
		}
		cdc := NewCdcTask(
			logger,
			ieFactory(),
			details.CreateCdc,
			cnUUID,
			fileService,
			cnTxnClient,
			cnEngine,
			cnEngMp,
		)
		cdc.activeRoutine = cdc2.NewCdcActiveRoutine()
		if err = attachToTask(ctx, T.GetID(), cdc); err != nil {
			return err
		}
		return cdc.Start(ctx, true)
	}
}

type CdcTask struct {
	logger *zap.Logger
	ie     ie.InternalExecutor

	cnUUID      string
	cnTxnClient client.TxnClient
	cnEngine    engine.Engine
	fileService fileservice.FileService

	cdcTask *task.CreateCdcDetails

	mp         *mpool.MPool
	packerPool *fileservice.Pool[*types.Packer]

	sinkUri cdc2.UriInfo
	tables  cdc2.PatternTuples
	filters cdc2.PatternTuples
	startTs types.TS
	noFull  string

	activeRoutine *cdc2.ActiveRoutine
	// sunkWatermarkUpdater update the watermark of the items that has been sunk to downstream
	sunkWatermarkUpdater *cdc2.WatermarkUpdater
}

func NewCdcTask(
	logger *zap.Logger,
	ie ie.InternalExecutor,
	cdcTask *task.CreateCdcDetails,
	cnUUID string,
	fileService fileservice.FileService,
	cnTxnClient client.TxnClient,
	cnEngine engine.Engine,
	cdcMp *mpool.MPool,
) *CdcTask {
	return &CdcTask{
		logger:      logger,
		ie:          ie,
		cdcTask:     cdcTask,
		cnUUID:      cnUUID,
		fileService: fileService,
		cnTxnClient: cnTxnClient,
		cnEngine:    cnEngine,
		mp:          cdcMp,
		packerPool: fileservice.NewPool(
			128,
			func() *types.Packer {
				return types.NewPacker()
			},
			func(packer *types.Packer) {
				packer.Reset()
			},
			func(packer *types.Packer) {
				packer.Close()
			},
		),
	}
}

func (cdc *CdcTask) Start(rootCtx context.Context, firstTime bool) (err error) {
	defer func() {
		if err != nil {
			// if Start failed, there will be some dangle goroutines(watermarkUpdater, reader, sinker...)
			// need to close them to avoid goroutine leak
			close(cdc.activeRoutine.Cancel)
		}
	}()

	ctx := defines.AttachAccountId(rootCtx, uint32(cdc.cdcTask.Accounts[0].GetId()))

	//step1 : get cdc task definition
	if err = cdc.retrieveCdcTask(ctx); err != nil {
		return err
	}

	//check table be filtered or not
	if filterTable(&cdc.tables, &cdc.filters) == 0 {
		return moerr.NewInternalError(ctx, "all tables has been excluded by filters. start cdc failed.")
	}

	//step2 : get source tableid
	var info *cdc2.DbTableInfo
	dbTableInfos := make([]*cdc2.DbTableInfo, 0, len(cdc.tables.Pts))
	for _, tuple := range cdc.tables.Pts {
		accId, accName, dbName, tblName := tuple.Source.AccountId, tuple.Source.Account, tuple.Source.Database, tuple.Source.Table
		if needSkipThisTable(accName, dbName, tblName, &cdc.filters) {
			logutil.Infof("cdc skip table %s:%s by filter", dbName, tblName)
			continue
		}
		//get dbid tableid for the source table
		info, err = cdc.retrieveTable(ctx, accId, accName, dbName, tblName)
		if err != nil {
			return err
		}

		info.SinkAccountName = tuple.Sink.Account
		info.SinkDbName = tuple.Sink.Database
		info.SinkTblName = tuple.Sink.Table

		dbTableInfos = append(dbTableInfos, info)
	}

	err = cdc.startWatermarkAndPipeline(ctx, dbTableInfos)
	if err != nil {
		return err
	}

	if firstTime {
		// hold
		ch := make(chan int, 1)
		select {
		case <-ctx.Done():
			break
		case <-ch:
			break
		}
	}
	return
}

func (cdc *CdcTask) startWatermarkAndPipeline(ctx context.Context, dbTableInfos []*cdc2.DbTableInfo) (err error) {
	var info *cdc2.DbTableInfo
	txnOp, err := cdc2.GetTxnOp(ctx, cdc.cnEngine, cdc.cnTxnClient, "cdc-startWatermarkAndPipeline")
	if err != nil {
		return err
	}
	defer func() {
		cdc2.FinishTxnOp(ctx, err, txnOp, cdc.cnEngine)
	}()
	if err = cdc.cnEngine.New(ctx, txnOp); err != nil {
		return err
	}

	if cdc.noFull == "true" {
		cdc.startTs = types.TimestampToTS(txnOp.SnapshotTS())
	}

	// start watermark updater
	cdc.sunkWatermarkUpdater = cdc2.NewWatermarkUpdater(cdc.cdcTask.Accounts[0].GetId(), cdc.cdcTask.TaskId, cdc.ie)

	count, err := cdc.sunkWatermarkUpdater.GetCountFromDb()
	if err != nil {
		return err
	} else if count == 0 {
		for _, info = range dbTableInfos {
			// use startTs as watermark
			if err = cdc.sunkWatermarkUpdater.InsertIntoDb(info.SourceTblId, cdc.startTs); err != nil {
				return err
			}
		}
	}
	go cdc.sunkWatermarkUpdater.Run(ctx, cdc.activeRoutine)

	// create exec pipelines
	for _, info = range dbTableInfos {
		if err = cdc.addExecPipelineForTable(info, txnOp); err != nil {
			return
		}
	}
	return
}

func (cdc *CdcTask) retrieveCdcTask(ctx context.Context) error {
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)

	cdcTaskId, _ := uuid.Parse(cdc.cdcTask.TaskId)
	sql := getSqlForRetrievingCdcTask(cdc.cdcTask.Accounts[0].GetId(), cdcTaskId)
	res := cdc.ie.Query(ctx, sql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		return res.Error()
	}

	if res.RowCount() < 1 {
		return moerr.NewInternalErrorf(ctx, "none cdc task for %d %s", cdc.cdcTask.Accounts[0].GetId(), cdc.cdcTask.TaskId)
	} else if res.RowCount() > 1 {
		return moerr.NewInternalErrorf(ctx, "duplicate cdc task for %d %s", cdc.cdcTask.Accounts[0].GetId(), cdc.cdcTask.TaskId)
	}

	//sink_type
	sinkTyp, err := res.GetString(ctx, 0, 1)
	if err != nil {
		return err
	}

	var sinkPwd string
	if sinkTyp != cdc2.ConsoleSink {
		//sink uri
		jsonSinkUri, err := res.GetString(ctx, 0, 0)
		if err != nil {
			return err
		}

		err = cdc2.JsonDecode(jsonSinkUri, &cdc.sinkUri)
		if err != nil {
			return err
		}

		//sink_password
		sinkPwd, err = res.GetString(ctx, 0, 2)
		if err != nil {
			return err
		}

		cdc.sinkUri.Password, err = cdc2.AesCFBDecode(ctx, sinkPwd)
		if err != nil {
			return err
		}
	}

	//update sink type after deserialize
	cdc.sinkUri.SinkTyp = sinkTyp

	//tables
	jsonTables, err := res.GetString(ctx, 0, 3)
	if err != nil {
		return err
	}

	err = cdc2.JsonDecode(jsonTables, &cdc.tables)
	if err != nil {
		return err
	}

	//filters
	jsonFilters, err := res.GetString(ctx, 0, 4)
	if err != nil {
		return err
	}

	err = cdc2.JsonDecode(jsonFilters, &cdc.filters)
	if err != nil {
		return err
	}

	// noFull
	noFull, err := res.GetString(ctx, 0, 5)
	if err != nil {
		return err
	}

	cdc.startTs = types.TS{}
	cdc.noFull = noFull

	return nil
}

func (cdc *CdcTask) retrieveTable(ctx context.Context, accId uint64, accName, dbName, tblName string) (*cdc2.DbTableInfo, error) {
	var dbId, tblId uint64
	var err error
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	sql := getSqlForDbIdAndTableId(accId, dbName, tblName)
	res := cdc.ie.Query(ctx, sql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		return nil, res.Error()
	}

	/*
		missing table will be handled in the future.
	*/
	if res.RowCount() < 1 {
		return nil, moerr.NewInternalErrorf(ctx, "no table %s:%s", dbName, tblName)
	} else if res.RowCount() > 1 {
		return nil, moerr.NewInternalErrorf(ctx, "duplicate table %s:%s", dbName, tblName)
	}

	if dbId, err = res.GetUint64(ctx, 0, 0); err != nil {
		return nil, err
	}

	if tblId, err = res.GetUint64(ctx, 0, 1); err != nil {
		return nil, err
	}

	return &cdc2.DbTableInfo{
		SourceAccountName: accName,
		SourceDbName:      dbName,
		SourceTblName:     tblName,
		SourceAccountId:   accId,
		SourceDbId:        dbId,
		SourceTblId:       tblId,
	}, err
}

func needSkipThisTable(accountName, dbName, tblName string, filters *cdc2.PatternTuples) bool {
	if len(filters.Pts) == 0 {
		return false
	}
	for _, filter := range filters.Pts {
		if filter == nil {
			continue
		}
		if filter.Source.Account == accountName &&
			filter.Source.Database == dbName &&
			filter.Source.Table == tblName {
			return true
		}
	}
	return false
}

var Start = func(ctx context.Context, cdc *CdcTask, firstTime bool) error {
	return cdc.Start(ctx, firstTime)
}

// Resume cdc task from last recorded watermark
func (cdc *CdcTask) Resume() (err error) {
	for {
		// closed in Pause, need renew
		cdc.activeRoutine.Cancel = make(chan struct{})
		if err = Start(context.Background(), cdc, false); err == nil {
			return
		}
		time.Sleep(time.Second)
	}
}

// Restart cdc task from init watermark
func (cdc *CdcTask) Restart() (err error) {
	for {
		// closed in Pause, need renew
		cdc.activeRoutine.Cancel = make(chan struct{})
		// delete previous records
		if err = cdc.sunkWatermarkUpdater.DeleteAllFromDb(); err == nil {
			if err = Start(context.Background(), cdc, false); err == nil {
				return
			}
		}
		time.Sleep(time.Second)
	}
}

// Pause cdc task
func (cdc *CdcTask) Pause() error {
	close(cdc.activeRoutine.Cancel)
	cdc.activeRoutine.Cancel = nil
	return nil
}

// Cancel cdc task
func (cdc *CdcTask) Cancel() error {
	if cdc.activeRoutine.Cancel != nil {
		close(cdc.activeRoutine.Cancel)
	}
	return cdc.sunkWatermarkUpdater.DeleteAllFromDb()
}

func (cdc *CdcTask) addExecPipelineForTable(info *cdc2.DbTableInfo, txnOp client.TxnOperator) error {
	// reader --call--> sinker ----> remote db
	ctx := defines.AttachAccountId(context.Background(), uint32(cdc.cdcTask.Accounts[0].GetId()))

	// add watermark to updater
	watermark, err := cdc.sunkWatermarkUpdater.GetFromDb(info.SourceTblId)
	if err != nil {
		return err
	}
	cdc.sunkWatermarkUpdater.UpdateMem(info.SourceTblId, watermark)

	tableDef, err := cdc2.GetTableDef(ctx, txnOp, cdc.cnEngine, info.SourceTblId)
	if err != nil {
		return err
	}

	// make sinker for table
	sinker, err := cdc2.NewSinker(
		cdc.sinkUri,
		info,
		cdc.sunkWatermarkUpdater,
		tableDef,
		cdc2.DefaultRetryTimes,
		cdc2.DefaultRetryDuration,
	)
	if err != nil {
		return err
	}

	// make reader
	reader := cdc2.NewTableReader(
		cdc.cnTxnClient,
		cdc.cnEngine,
		cdc.mp,
		cdc.packerPool,
		info,
		sinker,
		cdc.sunkWatermarkUpdater,
		tableDef,
		cdc.ResetWatermarkForTable,
	)
	go reader.Run(ctx, cdc.activeRoutine)

	return nil
}

func (cdc *CdcTask) ResetWatermarkForTable(info *cdc2.DbTableInfo) (err error) {
	tblId := info.SourceTblId
	// delete old watermark of table
	cdc.sunkWatermarkUpdater.DeleteFromMem(tblId)
	if err = cdc.sunkWatermarkUpdater.DeleteFromDb(tblId); err != nil {
		return
	}

	// use start_ts as init watermark
	if err = cdc.sunkWatermarkUpdater.InsertIntoDb(tblId, cdc.startTs); err != nil {
		return
	}
	cdc.sunkWatermarkUpdater.UpdateMem(tblId, cdc.startTs)
	return
}

func handleDropCdc(ses *Session, execCtx *ExecCtx, st *tree.DropCDC) error {
	return updateCdc(execCtx.reqCtx, ses, st)
}

func handlePauseCdc(ses *Session, execCtx *ExecCtx, st *tree.PauseCDC) error {
	return updateCdc(execCtx.reqCtx, ses, st)
}

func handleResumeCdc(ses *Session, execCtx *ExecCtx, st *tree.ResumeCDC) error {
	return updateCdc(execCtx.reqCtx, ses, st)
}

func handleRestartCdc(ses *Session, execCtx *ExecCtx, st *tree.RestartCDC) error {
	return updateCdc(execCtx.reqCtx, ses, st)
}

func updateCdc(ctx context.Context, ses *Session, st tree.Statement) (err error) {
	var targetTaskStatus task.TaskStatus
	var taskName string

	conds := make([]taskservice.Condition, 0)
	appendCond := func(cond ...taskservice.Condition) {
		conds = append(conds, cond...)
	}
	accountId := ses.GetTenantInfo().GetTenantID()

	switch stmt := st.(type) {
	case *tree.DropCDC:
		targetTaskStatus = task.TaskStatus_CancelRequested
		if stmt.Option == nil {
			return moerr.NewInternalErrorf(ctx, "invalid cdc option")
		}
		if stmt.Option.All {
			appendCond(
				taskservice.WithAccountID(taskservice.EQ, accountId),
				taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			)
		} else {
			taskName = stmt.Option.TaskName.String()
			appendCond(
				taskservice.WithAccountID(taskservice.EQ, accountId),
				taskservice.WithTaskName(taskservice.EQ, stmt.Option.TaskName.String()),
				taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			)
		}
	case *tree.PauseCDC:
		targetTaskStatus = task.TaskStatus_PauseRequested
		if stmt.Option == nil {
			return moerr.NewInternalErrorf(ctx, "invalid cdc option")
		}
		if stmt.Option.All {
			appendCond(
				taskservice.WithAccountID(taskservice.EQ, accountId),
				taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			)
		} else {
			taskName = stmt.Option.TaskName.String()
			appendCond(taskservice.WithAccountID(taskservice.EQ, accountId),
				taskservice.WithTaskName(taskservice.EQ, stmt.Option.TaskName.String()),
				taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			)
		}
	case *tree.RestartCDC:
		targetTaskStatus = task.TaskStatus_RestartRequested
		taskName = stmt.TaskName.String()
		if len(taskName) == 0 {
			return moerr.NewInternalErrorf(ctx, "invalid task name")
		}
		appendCond(
			taskservice.WithAccountID(taskservice.EQ, accountId),
			taskservice.WithTaskName(taskservice.EQ, stmt.TaskName.String()),
			taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
		)
	case *tree.ResumeCDC:
		targetTaskStatus = task.TaskStatus_ResumeRequested
		taskName = stmt.TaskName.String()
		if len(taskName) == 0 {
			return moerr.NewInternalErrorf(ctx, "invalid task name")
		}
		appendCond(
			taskservice.WithAccountID(taskservice.EQ, accountId),
			taskservice.WithTaskName(taskservice.EQ, stmt.TaskName.String()),
			taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
		)
	}

	return runUpdateCdcTask(ctx, targetTaskStatus, uint64(accountId), taskName, conds...)
}

func runUpdateCdcTask(
	ctx context.Context,
	targetTaskStatus task.TaskStatus,
	accountId uint64,
	taskName string,
	conds ...taskservice.Condition,
) (err error) {
	ts := getGlobalPu().TaskService
	if ts == nil {
		return nil
	}
	updateCdcTaskFunc := func(
		ctx context.Context,
		targetStatus task.TaskStatus,
		taskKeyMap map[taskservice.CdcTaskKey]struct{},
		tx taskservice.SqlExecutor,
	) (int, error) {
		return updateCdcTask(
			ctx,
			targetStatus,
			taskKeyMap,
			tx,
			accountId,
			taskName,
		)
	}
	_, err = ts.UpdateCdcTask(ctx,
		targetTaskStatus,
		updateCdcTaskFunc,
		conds...,
	)
	if err != nil {
		return err
	}
	return
}

func updateCdcTask(
	ctx context.Context,
	targetStatus task.TaskStatus,
	taskKeyMap map[taskservice.CdcTaskKey]struct{},
	tx taskservice.SqlExecutor,
	accountId uint64,
	taskName string,
) (int, error) {
	var taskId string
	var affectedCdcRow int
	var cnt int64

	//where : account_id = xxx and task_name = yyy
	whereClauses := buildCdcTaskWhereClause(accountId, taskName)
	//step1: query all account id & task id that we need from mo_cdc_task
	query := getCdcTaskIdFormat + whereClauses

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}
	if rows.Err() != nil {
		return 0, rows.Err()
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		if err = rows.Scan(&taskId); err != nil {
			return 0, err
		}
		tInfo := taskservice.CdcTaskKey{AccountId: accountId, TaskId: taskId}
		taskKeyMap[tInfo] = struct{}{}
	}

	var prepare *sql.Stmt
	//step2: update or cancel cdc task
	if targetStatus != task.TaskStatus_CancelRequested {
		//Update cdc task
		//updating mo_cdc_task table
		updateSql := updateCdcMetaFormat + whereClauses
		prepare, err = tx.PrepareContext(ctx, updateSql)
		if err != nil {
			return 0, err
		}
		defer func() {
			_ = prepare.Close()
		}()

		if prepare != nil {
			//execute update cdc status in mo_cdc_task
			var targetCdcStatus string
			if targetStatus == task.TaskStatus_PauseRequested {
				targetCdcStatus = CdcStopped
			} else {
				targetCdcStatus = CdcRunning
			}
			res, err := prepare.ExecContext(ctx, targetCdcStatus)
			if err != nil {
				return 0, err
			}
			affected, err := res.RowsAffected()
			if err != nil {
				return 0, err
			}
			affectedCdcRow += int(affected)
		}

		if targetStatus == task.TaskStatus_RestartRequested {
			//delete mo_cdc_watermark
			cnt, err = deleteWatermark(ctx, tx, taskKeyMap)
			if err != nil {
				return 0, err
			}
			affectedCdcRow += int(cnt)
		}
	} else {
		//Cancel cdc task
		//deleting mo_cdc_task
		deleteSql := deleteCdcMetaFormat + whereClauses
		cnt, err = executeSql(ctx, tx, deleteSql)
		if err != nil {
			return 0, err
		}
		affectedCdcRow += int(cnt)

		//delete mo_cdc_watermark
		cnt, err = deleteWatermark(ctx, tx, taskKeyMap)
		if err != nil {
			return 0, err
		}
		affectedCdcRow += int(cnt)
	}
	return affectedCdcRow, nil
}

func buildCdcTaskWhereClause(accountId uint64, taskName string) string {
	if len(taskName) == 0 {
		return fmt.Sprintf(" and account_id = %d", accountId)
	} else {
		return fmt.Sprintf(" and account_id = %d and task_name = '%s'", accountId, taskName)
	}
}

func deleteWatermark(ctx context.Context, tx taskservice.SqlExecutor, taskKeyMap map[taskservice.CdcTaskKey]struct{}) (int64, error) {
	tCount := int64(0)
	cnt := int64(0)
	var err error
	//deleting mo_cdc_watermark belongs to cancelled cdc task
	for tInfo := range taskKeyMap {
		deleteSql2 := getSqlForDeleteWatermark(tInfo.AccountId, tInfo.TaskId)
		cnt, err = executeSql(ctx, tx, deleteSql2)
		if err != nil {
			return 0, err
		}
		tCount += cnt
	}
	return tCount, nil
}

func executeSql(ctx context.Context, tx taskservice.SqlExecutor, query string, args ...interface{}) (int64, error) {
	exec, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	rows, err := exec.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rows, nil
}

func handleShowCdc(ses *Session, execCtx *ExecCtx, st *tree.ShowCDC) (err error) {
	var (
		taskId        string
		taskName      string
		sourceUri     string
		sinkUri       string
		state         string
		ckpStr        string
		sourceUriInfo cdc2.UriInfo
		sinkUriInfo   cdc2.UriInfo
	)

	ctx := defines.AttachAccountId(execCtx.reqCtx, catalog.System_Account)
	pu := getGlobalPu()
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	rs := &MysqlResultSet{}
	ses.SetMysqlResultSet(rs)
	for _, column := range showCdcOutputColumns {
		rs.AddColumn(column)
	}

	// current timestamp
	txnOp, err := cdc2.GetTxnOp(ctx, pu.StorageEngine, pu.TxnClient, "cdc-handleShowCdc")
	if err != nil {
		return err
	}
	defer func() {
		cdc2.FinishTxnOp(ctx, err, txnOp, pu.StorageEngine)
	}()
	timestamp := txnOp.SnapshotTS().ToStdTime().String()

	// get from task table
	sql := getSqlForGetTask(uint64(ses.GetTenantInfo().GetTenantID()), st.Option.All, string(st.Option.TaskName))

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	for _, result := range erArray {
		for i := uint64(0); i < result.GetRowCount(); i++ {
			if taskId, err = result.GetString(ctx, i, 0); err != nil {
				return
			}
			if taskName, err = result.GetString(ctx, i, 1); err != nil {
				return
			}
			if sourceUri, err = result.GetString(ctx, i, 2); err != nil {
				return
			}
			if sinkUri, err = result.GetString(ctx, i, 3); err != nil {
				return
			}
			if state, err = result.GetString(ctx, i, 4); err != nil {
				return
			}

			// decode uriInfo
			if err = cdc2.JsonDecode(sourceUri, &sourceUriInfo); err != nil {
				return
			}
			if err = cdc2.JsonDecode(sinkUri, &sinkUriInfo); err != nil {
				return
			}

			// get watermarks
			if ckpStr, err = getTaskCkp(ctx, bh, ses.GetTenantInfo().TenantID, taskId); err != nil {
				return
			}

			rs.AddRow([]interface{}{
				taskId,
				taskName,
				sourceUriInfo.String(),
				sinkUriInfo.String(),
				state,
				ckpStr,
				timestamp,
			})
		}
	}
	return
}

func getTaskCkp(ctx context.Context, bh BackgroundExec, accountId uint32, taskId string) (s string, err error) {
	var (
		dbName    string
		tblName   string
		watermark string
	)

	s = "{\n"

	sql := getSqlForGetWatermark(uint64(accountId), taskId)
	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	for _, result := range erArray {
		for i := uint64(0); i < result.GetRowCount(); i++ {
			if dbName, err = result.GetString(ctx, i, 0); err != nil {
				return
			}
			if tblName, err = result.GetString(ctx, i, 1); err != nil {
				return
			}
			if watermark, err = result.GetString(ctx, i, 2); err != nil {
				return
			}

			s += fmt.Sprintf("  \"%s.%s\": %s,\n", dbName, tblName, watermark)
		}
	}

	s += "}"
	return
}
