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
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
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
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
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
		`'%s',` + //additional_config
		`"",` + //err_msg
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
		`no_full, ` +
		`additional_config ` +
		`from ` +
		`mo_catalog.mo_cdc_task ` +
		`where ` +
		`account_id = %d and ` +
		`task_id = "%s"`

	getShowCdcTaskFormat = "select task_id, task_name, source_uri, sink_uri, state, err_msg from mo_catalog.mo_cdc_task where account_id = %d"

	getDbIdAndTableIdFormat = "select reldatabase_id,rel_id from mo_catalog.mo_tables where account_id = %d and reldatabase = '%s' and relname = '%s'"

	getTableFormat = "select rel_id from `mo_catalog`.`mo_tables` where account_id = %d and reldatabase ='%s' and relname = '%s'"

	getAccountIdFormat = "select account_id from `mo_catalog`.`mo_account` where account_name='%s'"

	getPkCountFormat = "select count(att_constraint_type) from `mo_catalog`.`mo_columns` where account_id = %d and att_database = '%s' and att_relname = '%s' and att_constraint_type = '%s'"

	getCdcTaskIdFormat = "select task_id from `mo_catalog`.`mo_cdc_task` where 1=1"

	deleteCdcMetaFormat = "delete from `mo_catalog`.`mo_cdc_task` where 1=1"

	updateCdcMetaFormat = "update `mo_catalog`.`mo_cdc_task` set state = ? where 1=1"

	deleteWatermarkFormat = "delete from `mo_catalog`.`mo_cdc_watermark` where account_id = %d and task_id = '%s'"

	getWatermarkFormat = "select db_name, table_name, watermark, err_msg from mo_catalog.mo_cdc_watermark where account_id = %d and task_id = '%s'"

	getDataKeyFormat = "select encrypted_key from mo_catalog.mo_data_key where account_id = %d and key_id = '%s'"

	updateErrMsgFormat = "update `mo_catalog`.`mo_cdc_task` set state = '%s', err_msg = '%s' where account_id = %d and task_id = '%s'"
)

const (
	CdcRunning   = "running"
	CdcPaused    = "paused"
	CdcFailed    = "failed"
	maxErrMsgLen = 256
)

var showCdcOutputColumns = [8]Column{
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
			name:       "err_msg",
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
	additionalConfigStr string,
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
		additionalConfigStr,
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

func handleCreateCdc(ses *Session, execCtx *ExecCtx, create *tree.CreateCDC) error {
	return doCreateCdc(execCtx.reqCtx, ses, create)
}

func doCreateCdc(ctx context.Context, ses *Session, create *tree.CreateCDC) (err error) {
	service := ses.GetService()
	ts := getPu(service).TaskService
	if ts == nil {
		return moerr.NewInternalError(ctx, "no task service is found")
	}

	cdcTaskOptionsMap := make(map[string]string)
	for i := 0; i < len(create.Option); i += 2 {
		cdcTaskOptionsMap[create.Option[i]] = create.Option[i+1]
	}

	// step 1: handle tables
	level := cdcTaskOptionsMap["Level"]
	if level != cdc2.AccountLevel && level != cdc2.DbLevel && level != cdc2.TableLevel {
		return moerr.NewInternalErrorf(ctx, "invalid level: %s", level)
	}
	tablePts, err := getPatternTuples(ctx, level, create.Tables)
	if err != nil {
		return err
	}
	jsonTables, err := cdc2.JsonEncode(tablePts)
	if err != nil {
		return
	}

	// step 2: handle exclude (regular expression)
	exclude := cdcTaskOptionsMap["Exclude"]
	if _, err = regexp.Compile(exclude); err != nil {
		return moerr.NewInternalErrorf(ctx, "invalid exclude expression: %s, err: %v", exclude, err)
	}

	//step 4: check source uri format and strip password
	jsonSrcUri, _, err := extractUriInfo(ctx, create.SourceUri, cdc2.SourceUriPrefix)
	if err != nil {
		return err
	}

	//step 5: check sink uri format, strip password and check connection
	sinkType := strings.ToLower(create.SinkType)
	useConsole := false
	if cdc2.EnableConsoleSink && sinkType == cdc2.ConsoleSink {
		useConsole = true
	}

	if !useConsole && sinkType != cdc2.MysqlSink && sinkType != cdc2.MatrixoneSink {
		return moerr.NewInternalErrorf(ctx, "unsupported sink type: %s", create.SinkType)
	}

	jsonSinkUri, sinkUriInfo, err := extractUriInfo(ctx, create.SinkUri, cdc2.SinkUriPrefix)
	if err != nil {
		return
	}
	if _, err = cdc2.OpenDbConn(sinkUriInfo.User, sinkUriInfo.Password, sinkUriInfo.Ip, sinkUriInfo.Port, cdc2.DefaultSendSqlTimeout); err != nil {
		err = moerr.NewInternalErrorf(ctx, "failed to connect to sink, please check the connection, err: %v", err)
		return
	}

	//step 6: no full
	noFull, _ := strconv.ParseBool(cdcTaskOptionsMap["NoFull"])

	//step 7: additionalConfig
	additionalConfig := make(map[string]any)

	// InitSnapshotSplitTxn
	additionalConfig[cdc2.InitSnapshotSplitTxn] = cdc2.DefaultInitSnapshotSplitTxn
	if cdcTaskOptionsMap[cdc2.InitSnapshotSplitTxn] == "false" {
		additionalConfig[cdc2.InitSnapshotSplitTxn] = false
	}

	// MaxSqlLength
	additionalConfig[cdc2.MaxSqlLength] = cdc2.DefaultMaxSqlLength
	if val, ok := cdcTaskOptionsMap[cdc2.MaxSqlLength]; ok {
		if additionalConfig[cdc2.MaxSqlLength], err = strconv.ParseUint(val, 10, 64); err != nil {
			return
		}
	}

	// SendSqlTimeout
	additionalConfig[cdc2.SendSqlTimeout] = cdc2.DefaultSendSqlTimeout
	if val, ok := cdcTaskOptionsMap[cdc2.SendSqlTimeout]; ok {
		// check duration format
		if _, err = time.ParseDuration(val); err != nil {
			return
		}
		additionalConfig[cdc2.SendSqlTimeout] = val
	}

	// marshal
	additionalConfigBytes, err := json.Marshal(additionalConfig)
	if err != nil {
		return err
	}

	//step 8: details
	accountInfo := ses.GetTenantInfo()
	accountId := accountInfo.GetTenantID()
	accountName := accountInfo.GetTenant()
	cdcId, _ := uuid.NewV7()
	details := &task.Details{
		//account info that create cdc
		AccountID: accountId,
		Account:   accountName,
		Username:  accountInfo.GetUser(),
		Details: &task.Details_CreateCdc{
			CreateCdc: &task.CreateCdcDetails{
				TaskName: create.TaskName.String(),
				TaskId:   cdcId.String(),
				Accounts: []*task.Account{
					{
						Id:   uint64(accountId),
						Name: accountName,
					},
				},
			},
		},
	}

	addCdcTaskCallback := func(ctx context.Context, tx taskservice.SqlExecutor) (ret int, err error) {
		var encodedSinkPwd string
		if !useConsole {
			// TODO replace with creatorAccountId
			if err = initAesKeyWrapper(ctx, tx, catalog.System_Account, service); err != nil {
				return
			}

			if encodedSinkPwd, err = sinkUriInfo.GetEncodedPassword(); err != nil {
				return
			}
		}

		exclude = strings.ReplaceAll(exclude, "\\", "\\\\")

		//step 5: create daemon task
		insertSql := getSqlForNewCdcTask(
			uint64(accountId), //the account_id of cdc creator
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
			exclude,
			"",
			cdc2.SASCommon,
			cdc2.SASCommon,
			"", //1.3 does not support startTs
			"", //1.3 does not support endTs
			cdcTaskOptionsMap["ConfigFile"],
			time.Now().UTC(),
			CdcRunning,
			0,
			noFull,
			"",
			string(additionalConfigBytes),
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

// getPatternTuple pattern example:
//
//	db1
//	db1:db2
//	db1.t1
//	db1.t1:db2.t2
//
// There must be no special characters (','  '.'  ':' '`') in database name & table name.
func getPatternTuple(ctx context.Context, level string, pattern string, dup map[string]struct{}) (pt *cdc2.PatternTuple, err error) {
	splitRes := strings.Split(strings.TrimSpace(pattern), ":")
	if len(splitRes) > 2 {
		err = moerr.NewInternalErrorf(ctx, "invalid pattern format: %s, must be `source` or `source:sink`.", pattern)
		return
	}

	pt = &cdc2.PatternTuple{OriginString: pattern}

	// handle source part
	if pt.Source.Database, pt.Source.Table, err = extractTableInfo(ctx, splitRes[0], level); err != nil {
		return
	}
	key := cdc2.GenDbTblKey(pt.Source.Database, pt.Source.Table)
	if _, ok := dup[key]; ok {
		err = moerr.NewInternalErrorf(ctx, "one db/table: %s can't be used as multi sources in a cdc task", key)
		return
	}
	dup[key] = struct{}{}

	// handle sink part
	if len(splitRes) > 1 {
		if pt.Sink.Database, pt.Sink.Table, err = extractTableInfo(ctx, splitRes[1], level); err != nil {
			return
		}
	} else {
		// if not specify sink, then sink = source
		pt.Sink.Database = pt.Source.Database
		pt.Sink.Table = pt.Source.Table
	}
	return
}

// extractTableInfo get account,database,table info from string
//
// input format:
//
//	DbLevel: database
//	TableLevel: database.table
//
// There must be no special characters (','  '.'  ':' '`') in database name & table name.
func extractTableInfo(ctx context.Context, input string, level string) (db string, table string, err error) {
	parts := strings.Split(strings.TrimSpace(input), ".")
	if level == cdc2.DbLevel && len(parts) != 1 {
		err = moerr.NewInternalErrorf(ctx, "invalid databases format: %s", input)
		return
	} else if level == cdc2.TableLevel && len(parts) != 2 {
		err = moerr.NewInternalErrorf(ctx, "invalid tables format: %s", input)
		return
	}

	db = strings.TrimSpace(parts[0])
	if !dbNameIsLegal(db) {
		err = moerr.NewInternalErrorf(ctx, "invalid database name: %s", db)
		return
	}

	if level == cdc2.TableLevel {
		table = strings.TrimSpace(parts[1])
		if !tableNameIsLegal(table) {
			err = moerr.NewInternalErrorf(ctx, "invalid table name: %s", table)
			return
		}
	} else {
		table = cdc2.MatchAll
	}
	return
}

func getPatternTuples(ctx context.Context, level string, tables string) (pts *cdc2.PatternTuples, err error) {
	pts = &cdc2.PatternTuples{}

	if level == cdc2.AccountLevel {
		pts.Append(&cdc2.PatternTuple{
			Source: cdc2.PatternTable{Database: cdc2.MatchAll, Table: cdc2.MatchAll},
			Sink:   cdc2.PatternTable{Database: cdc2.MatchAll, Table: cdc2.MatchAll},
		})
		return
	}

	// split tables by ',' => table pair
	var pt *cdc2.PatternTuple
	tablePairs := strings.Split(strings.TrimSpace(tables), ",")
	dup := make(map[string]struct{})
	for _, pair := range tablePairs {
		if pt, err = getPatternTuple(ctx, level, pair, dup); err != nil {
			return
		}
		pts.Append(pt)
	}
	return
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
		ctx1, cancel := context.WithTimeoutCause(context.Background(), time.Second*3, moerr.CauseRegisterCdc)
		defer cancel()
		tasks, err := ts.QueryDaemonTask(ctx1,
			taskservice.WithTaskIDCond(taskservice.EQ, T.GetID()),
		)
		if err != nil {
			return moerr.AttachCause(ctx1, err)
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
		return cdc.Start(ctx)
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

	sinkUri          cdc2.UriInfo
	tables           cdc2.PatternTuples
	exclude          *regexp.Regexp
	startTs          types.TS
	noFull           bool
	additionalConfig map[string]interface{}

	activeRoutine *cdc2.ActiveRoutine
	// watermarkUpdater update the watermark of the items that has been sunk to downstream
	watermarkUpdater cdc2.IWatermarkUpdater
	// runningReaders store the running execute pipelines, map key pattern: db.table
	runningReaders *sync.Map

	isRunning bool
	holdCh    chan int

	// start wrapper, for ut
	startFunc func(ctx context.Context) error
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
	task := &CdcTask{
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
	task.startFunc = task.Start
	return task
}

func (cdc *CdcTask) Start(rootCtx context.Context) (err error) {
	taskId := cdc.cdcTask.TaskId
	taskName := cdc.cdcTask.TaskName
	cnUUID := cdc.cnUUID
	accountId := uint32(cdc.cdcTask.Accounts[0].GetId())
	logutil.Infof("cdc task %s start on cn %s", taskName, cnUUID)

	defer func() {
		if err != nil {
			// if Start failed, there will be some dangle goroutines(watermarkUpdater, reader, sinker...)
			// need to close them to avoid goroutine leak
			cdc.activeRoutine.ClosePause()
			cdc.activeRoutine.CloseCancel()

			if updateErrMsgErr := cdc.updateErrMsg(rootCtx, err.Error()); updateErrMsgErr != nil {
				logutil.Errorf("cdc task %s update err msg failed, err: %v", taskName, updateErrMsgErr)
			}
		}

		cdc2.GetTableScanner(cnUUID).UnRegister(taskId)
	}()

	ctx := defines.AttachAccountId(rootCtx, accountId)

	// get cdc task definition
	if err = cdc.retrieveCdcTask(ctx); err != nil {
		return err
	}

	// reset runningReaders
	cdc.runningReaders = &sync.Map{}

	// start watermarkUpdater
	cdc.watermarkUpdater = cdc2.NewWatermarkUpdater(accountId, taskId, cdc.ie)
	go cdc.watermarkUpdater.Run(ctx, cdc.activeRoutine)

	// register to table scanner
	cdc2.GetTableScanner(cnUUID).Register(taskId, cdc.handleNewTables)

	cdc.isRunning = true
	logutil.Infof("cdc task %s start on cn %s success", taskName, cnUUID)
	// start success, clear err msg
	if err = cdc.updateErrMsg(ctx, ""); err != nil {
		logutil.Errorf("cdc task %s update err msg failed, err: %v", taskName, err)
	}

	// hold
	cdc.holdCh = make(chan int, 1)
	select {
	case <-ctx.Done():
		break
	case <-cdc.holdCh:
		break
	}
	return
}

// Resume cdc task from last recorded watermark
func (cdc *CdcTask) Resume() (err error) {
	logutil.Infof("cdc task %s resume", cdc.cdcTask.TaskName)
	defer func() {
		if err == nil {
			logutil.Infof("cdc task %s resume success", cdc.cdcTask.TaskName)
		} else {
			logutil.Infof("cdc task %s resume failed, err: %v", cdc.cdcTask.TaskName, err)
		}
	}()

	go func() {
		// closed in Pause, need renew
		cdc.activeRoutine = cdc2.NewCdcActiveRoutine()
		_ = cdc.startFunc(context.Background())
	}()
	return
}

// Restart cdc task from init watermark
func (cdc *CdcTask) Restart() (err error) {
	logutil.Infof("cdc task %s restart", cdc.cdcTask.TaskName)
	defer func() {
		if err == nil {
			logutil.Infof("cdc task %s restart success", cdc.cdcTask.TaskName)
		} else {
			logutil.Infof("cdc task %s restart failed, err: %v", cdc.cdcTask.TaskName, err)
		}
	}()

	// delete previous records
	if err = cdc.watermarkUpdater.DeleteAllFromDb(); err != nil {
		return
	}
	go func() {
		// closed in Pause, need renew
		cdc.activeRoutine = cdc2.NewCdcActiveRoutine()
		_ = cdc.startFunc(context.Background())
	}()
	return
}

// Pause cdc task
func (cdc *CdcTask) Pause() error {
	logutil.Infof("cdc task %s pause", cdc.cdcTask.TaskName)
	defer func() {
		logutil.Infof("cdc task %s pause success", cdc.cdcTask.TaskName)
	}()

	if cdc.isRunning {
		cdc.activeRoutine.ClosePause()
		cdc.isRunning = false
	}
	// let Start() go
	cdc.holdCh <- 1
	return nil
}

// Cancel cdc task
func (cdc *CdcTask) Cancel() (err error) {
	logutil.Infof("cdc task %s cancel", cdc.cdcTask.TaskName)
	defer func() {
		if err == nil {
			logutil.Infof("cdc task %s cancel success", cdc.cdcTask.TaskName)
		} else {
			logutil.Infof("cdc task %s cancel failed, err: %v", cdc.cdcTask.TaskName, err)
		}
	}()

	if cdc.isRunning {
		cdc.activeRoutine.CloseCancel()
		cdc.isRunning = false
	}
	if err = cdc.watermarkUpdater.DeleteAllFromDb(); err != nil {
		return err
	}
	// let Start() go
	cdc.holdCh <- 1
	return
}

func (cdc *CdcTask) resetWatermarkForTable(info *cdc2.DbTableInfo) (err error) {
	dbName, tblName := info.SourceDbName, info.SourceTblName
	// delete old watermark of table
	cdc.watermarkUpdater.DeleteFromMem(dbName, tblName)
	if err = cdc.watermarkUpdater.DeleteFromDb(dbName, tblName); err != nil {
		return
	}

	// use start_ts as init watermark
	// TODO handle no_full
	if err = cdc.watermarkUpdater.InsertIntoDb(info, cdc.startTs); err != nil {
		return
	}
	cdc.watermarkUpdater.UpdateMem(dbName, tblName, cdc.startTs)
	return
}

func (cdc *CdcTask) initAesKeyByInternalExecutor(ctx context.Context, accountId uint32) (err error) {
	if len(cdc2.AesKey) > 0 {
		return nil
	}

	querySql := fmt.Sprintf(getDataKeyFormat, accountId, cdc2.InitKeyId)
	res := cdc.ie.Query(ctx, querySql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		return res.Error()
	} else if res.RowCount() < 1 {
		return moerr.NewInternalErrorf(ctx, "no data key record for account %d", accountId)
	}

	encryptedKey, err := res.GetString(ctx, 0, 0)
	if err != nil {
		return err
	}

	cdc2.AesKey, err = decrypt(ctx, encryptedKey, []byte(getGlobalPuWrapper(cdc.cnUUID).SV.KeyEncryptionKey))
	return
}

func (cdc *CdcTask) updateErrMsg(ctx context.Context, errMsg string) (err error) {
	accId := cdc.cdcTask.Accounts[0].GetId()
	cdcTaskId, _ := uuid.Parse(cdc.cdcTask.TaskId)
	state := CdcRunning
	if errMsg != "" {
		state = CdcFailed
	}
	if len(errMsg) > maxErrMsgLen {
		errMsg = errMsg[:maxErrMsgLen]
	}

	sql := fmt.Sprintf(updateErrMsgFormat, state, errMsg, accId, cdcTaskId)
	return cdc.ie.Exec(defines.AttachAccountId(ctx, catalog.System_Account), sql, ie.SessionOverrideOptions{})
}

func (cdc *CdcTask) handleNewTables(allAccountTbls map[uint32]cdc2.TblMap) {
	accountId := uint32(cdc.cdcTask.Accounts[0].GetId())
	ctx := defines.AttachAccountId(context.Background(), accountId)

	txnOp, err := cdc2.GetTxnOp(ctx, cdc.cnEngine, cdc.cnTxnClient, "cdc-handleNewTables")
	if err != nil {
		logutil.Errorf("cdc task %s get txn op failed, err: %v", cdc.cdcTask.TaskName, err)
	}
	defer func() {
		cdc2.FinishTxnOp(ctx, err, txnOp, cdc.cnEngine)
	}()
	if err = cdc.cnEngine.New(ctx, txnOp); err != nil {
		logutil.Errorf("cdc task %s new engine failed, err: %v", cdc.cdcTask.TaskName, err)
	}

	for key, info := range allAccountTbls[accountId] {
		// already running
		if _, ok := cdc.runningReaders.Load(key); ok {
			continue
		}

		if !cdc.matchAnyPattern(key, info) {
			continue
		}

		if cdc.exclude != nil && cdc.exclude.MatchString(key) {
			continue
		}

		logutil.Infof("cdc task find new table: %s", info)
		if err = cdc.addExecPipelineForTable(ctx, info, txnOp); err != nil {
			logutil.Errorf("cdc task %s add exec pipeline for table %s failed, err: %v", cdc.cdcTask.TaskName, key, err)
		} else {
			logutil.Infof("cdc task %s add exec pipeline for table %s successfully", cdc.cdcTask.TaskName, key)
		}
	}
}

func (cdc *CdcTask) matchAnyPattern(key string, info *cdc2.DbTableInfo) bool {
	match := func(s, p string) bool {
		if p == cdc2.MatchAll {
			return true
		}
		return s == p
	}

	db, table := cdc2.SplitDbTblKey(key)
	for _, pt := range cdc.tables.Pts {
		if match(db, pt.Source.Database) && match(table, pt.Source.Table) {
			// complete sink info
			info.SinkDbName = pt.Sink.Database
			if info.SinkDbName == cdc2.MatchAll {
				info.SinkDbName = db
			}
			info.SinkTblName = pt.Sink.Table
			if info.SinkTblName == cdc2.MatchAll {
				info.SinkTblName = table
			}
			return true
		}
	}
	return false
}

// reader ----> sinker ----> remote db
func (cdc *CdcTask) addExecPipelineForTable(ctx context.Context, info *cdc2.DbTableInfo, txnOp client.TxnOperator) (err error) {
	// step 1. init watermarkUpdater
	// get watermark from db
	watermark, err := cdc.watermarkUpdater.GetFromDb(info.SourceDbName, info.SourceTblName)
	if moerr.IsMoErrCode(err, moerr.ErrNoWatermarkFound) {
		// add watermark into db if not exists
		watermark = cdc.startTs
		if cdc.noFull {
			watermark = types.TimestampToTS(txnOp.SnapshotTS())
		}

		if err = cdc.watermarkUpdater.InsertIntoDb(info, watermark); err != nil {
			return
		}
	} else if err != nil {
		return
	}
	// clear err msg
	if err = cdc.watermarkUpdater.SaveErrMsg(info.SourceDbName, info.SourceTblName, ""); err != nil {
		return
	}
	// add watermark into memory
	cdc.watermarkUpdater.UpdateMem(info.SourceDbName, info.SourceTblName, watermark)

	tableDef, err := cdc2.GetTableDef(ctx, txnOp, cdc.cnEngine, info.SourceTblId)
	if err != nil {
		return
	}

	// step 2. new sinker
	sinker, err := cdc2.NewSinker(
		cdc.sinkUri,
		info,
		cdc.watermarkUpdater,
		tableDef,
		cdc2.DefaultRetryTimes,
		cdc2.DefaultRetryDuration,
		cdc.activeRoutine,
		uint64(cdc.additionalConfig[cdc2.MaxSqlLength].(float64)),
		cdc.additionalConfig[cdc2.SendSqlTimeout].(string),
	)
	if err != nil {
		return err
	}
	go sinker.Run(ctx, cdc.activeRoutine)

	// step 3. new reader
	reader := cdc2.NewTableReader(
		cdc.cnTxnClient,
		cdc.cnEngine,
		cdc.mp,
		cdc.packerPool,
		info,
		sinker,
		cdc.watermarkUpdater,
		tableDef,
		cdc.resetWatermarkForTable,
		cdc.additionalConfig[cdc2.InitSnapshotSplitTxn].(bool),
		cdc.runningReaders,
	)
	go reader.Run(ctx, cdc.activeRoutine)

	return
}

func (cdc *CdcTask) retrieveCdcTask(ctx context.Context) error {
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)

	accId := cdc.cdcTask.Accounts[0].GetId()
	cdcTaskId, _ := uuid.Parse(cdc.cdcTask.TaskId)
	sql := getSqlForRetrievingCdcTask(accId, cdcTaskId)
	res := cdc.ie.Query(ctx, sql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		return res.Error()
	}

	if res.RowCount() < 1 {
		return moerr.NewInternalErrorf(ctx, "none cdc task for %d %s", accId, cdc.cdcTask.TaskId)
	} else if res.RowCount() > 1 {
		return moerr.NewInternalErrorf(ctx, "duplicate cdc task for %d %s", accId, cdc.cdcTask.TaskId)
	}

	//sink_type
	sinkTyp, err := res.GetString(ctx, 0, 1)
	if err != nil {
		return err
	}

	if sinkTyp != cdc2.ConsoleSink {
		//sink uri
		jsonSinkUri, err := res.GetString(ctx, 0, 0)
		if err != nil {
			return err
		}

		if err = cdc2.JsonDecode(jsonSinkUri, &cdc.sinkUri); err != nil {
			return err
		}

		//sink_password
		sinkPwd, err := res.GetString(ctx, 0, 2)
		if err != nil {
			return err
		}

		// TODO replace with creatorAccountId
		if err = cdc.initAesKeyByInternalExecutor(ctx, catalog.System_Account); err != nil {
			return err
		}

		if cdc.sinkUri.Password, err = cdc2.AesCFBDecode(ctx, sinkPwd); err != nil {
			return err
		}
	}

	//update sink type after deserialize
	cdc.sinkUri.SinkTyp = sinkTyp

	// tables
	jsonTables, err := res.GetString(ctx, 0, 3)
	if err != nil {
		return err
	}

	if err = cdc2.JsonDecode(jsonTables, &cdc.tables); err != nil {
		return err
	}

	// exclude
	exclude, err := res.GetString(ctx, 0, 4)
	if err != nil {
		return err
	}
	if exclude != "" {
		if cdc.exclude, err = regexp.Compile(exclude); err != nil {
			return err
		}
	}

	// noFull
	noFull, err := res.GetString(ctx, 0, 5)
	if err != nil {
		return err
	}
	cdc.noFull, _ = strconv.ParseBool(noFull)

	// startTs
	cdc.startTs = types.TS{}

	// additionalConfig
	additionalConfigStr, err := res.GetString(ctx, 0, 6)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(additionalConfigStr), &cdc.additionalConfig)
}

var initAesKeyByInternalExecutor = func(ctx context.Context, cdc *CdcTask, accountId uint32) error {
	return cdc.initAesKeyByInternalExecutor(ctx, accountId)
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

	return runUpdateCdcTask(ctx, targetTaskStatus, uint64(accountId), taskName, ses.GetService(), conds...)
}

func runUpdateCdcTask(
	ctx context.Context,
	targetTaskStatus task.TaskStatus,
	accountId uint64,
	taskName string,
	service string,
	conds ...taskservice.Condition) (err error) {
	ts := getPu(service).TaskService
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
	return err
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

	empty := true
	for rows.Next() {
		empty = false
		if err = rows.Scan(&taskId); err != nil {
			return 0, err
		}
		tInfo := taskservice.CdcTaskKey{AccountId: accountId, TaskId: taskId}
		taskKeyMap[tInfo] = struct{}{}
	}

	if taskName != "" && empty {
		return 0, moerr.NewInternalErrorf(ctx, "no cdc task found, task name: %s", taskName)
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
				targetCdcStatus = CdcPaused
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
		errMsg        string
		ckpStr        string
		sourceUriInfo cdc2.UriInfo
		sinkUriInfo   cdc2.UriInfo
	)

	ctx := defines.AttachAccountId(execCtx.reqCtx, catalog.System_Account)
	pu := getPu(ses.GetService())
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
	timestamp := txnOp.SnapshotTS().ToStdTime().In(time.Local).String()

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
			if errMsg, err = result.GetString(ctx, i, 5); err != nil {
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
				errMsg,
				ckpStr,
				timestamp,
			})
		}
	}
	return
}

func getTaskCkp(ctx context.Context, bh BackgroundExec, accountId uint32, taskId string) (s string, err error) {
	var (
		dbName           string
		tblName          string
		watermarkStdTime string
		errMsg           string
	)

	getWatermarkStdTime := func(result ExecResult, i uint64) (watermarkStr string, err error) {
		var watermarkTs timestamp.Timestamp
		if watermarkStr, err = result.GetString(ctx, i, 2); err != nil {
			return
		}
		if watermarkTs, err = timestamp.ParseTimestamp(watermarkStr); err != nil {
			return
		}
		watermarkStr = watermarkTs.ToStdTime().In(time.Local).String()
		return
	}

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
			if watermarkStdTime, err = getWatermarkStdTime(result, i); err != nil {
				return
			}
			if errMsg, err = result.GetString(ctx, i, 3); err != nil {
				return
			}

			if len(errMsg) == 0 {
				s += fmt.Sprintf("  \"%s.%s\": %s,\n", dbName, tblName, watermarkStdTime)
			} else {
				s += fmt.Sprintf("  \"%s.%s\": %s(Failed, error: %s),\n", dbName, tblName, watermarkStdTime, errMsg)
			}
		}
	}

	s += "}"
	return
}

var (
	queryTableWrapper  = queryTable
	decrypt            = cdc2.AesCFBDecodeWithKey
	getGlobalPuWrapper = getPu
	initAesKeyWrapper  = initAesKeyBySqlExecutor
)

func initAesKeyBySqlExecutor(ctx context.Context, executor taskservice.SqlExecutor, accountId uint32, service string) (err error) {
	if len(cdc2.AesKey) > 0 {
		return nil
	}

	var encryptedKey string
	var ret bool
	querySql := fmt.Sprintf(getDataKeyFormat, accountId, cdc2.InitKeyId)

	ret, err = queryTableWrapper(ctx, executor, querySql, func(ctx context.Context, rows *sql.Rows) (bool, error) {
		if err = rows.Scan(&encryptedKey); err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		return
	} else if !ret {
		return moerr.NewInternalError(ctx, "no data key")
	}

	cdc2.AesKey, err = decrypt(ctx, encryptedKey, []byte(getGlobalPuWrapper(service).SV.KeyEncryptionKey))
	return
}
