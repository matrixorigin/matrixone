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
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	cdc2 "github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	pb "github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/tools"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
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
		`%d,` + //start_ts
		`"%d",` + //start_ts_str
		`%d,` + //end_ts
		`"%d",` + //end_ts_str
		`"%s",` + //config_file
		`"%s",` + //task_create_time
		`"%s",` + //state
		`%d,` + //checkpoint
		`"%d",` + //checkpoint_str
		`"%s",` + //full_config
		`"%s",` + //incr_config
		`"",` + //reserved0
		`"",` + //reserved1
		`"",` + //reserved2
		`"",` + //reserved3
		`""` + //reserved4
		`)`

	getNewCdcTaskFormat = `select ` +
		`sink_uri, ` +
		`sink_type, ` +
		`sink_password, ` +
		`tables ` +
		`from ` +
		`mo_catalog.mo_cdc_task ` +
		`where ` +
		`account_id = %d and ` +
		`task_id = "%s"`

	getDbIdAndTableIdFormat = "select reldatabase_id,rel_id from mo_catalog.mo_tables where account_id = %d and reldatabase = '%s' and relname = '%s'"

	getTables = "select account_name, reldatabase, relname from mo_catalog.mo_tables join mo_catalog.mo_account on mo_catalog.mo_tables.account_id = mo_catalog.mo_account.account_id where REGEXP_LIKE(account_name, '^%s$') and REGEXP_LIKE(reldatabase, '^%s$') and REGEXP_LIKE(relname, '^%s$')"

	getCdcTaskId = "select task_id from mo_catalog.mo_cdc_task where account_id = %d"

	getCdcTaskIdWhere = "select task_id from mo_catalog.mo_cdc_task where account_id = %d and task_name = '%s'"

	dropCdcMeta = "delete from mo_catalog.mo_cdc_task where account_id = %d and task_id = '%s'"

	updateCdcMeta = "update mo_catalog.mo_cdc_task set state = '%s' where account_id = %d and task_id = '%s'"
)

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
	startTs uint64,
	endTs uint64,
	configFile string,
	taskCreateTime time.Time,
	state string,
	checkpoint uint64,
	fullConfig string,
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
		startTs,
		endTs,
		endTs,
		configFile,
		taskCreateTime.Format(time.DateTime),
		state,
		checkpoint,
		checkpoint,
		fullConfig,
		incrConfig,
	)
}

func getSqlForRetrivingNewCdcTask(
	accId uint64,
	taskId uuid.UUID,
) string {
	return fmt.Sprintf(getNewCdcTaskFormat, accId, taskId)
}

func getSqlForDbIdAndTableId(accId uint64, db, table string) string {
	return fmt.Sprintf(getDbIdAndTableIdFormat, accId, db, table)
}

func getSqlForTables(
	pt *PatternTuple,
) string {
	return fmt.Sprintf(getTables, pt.SourceAccount, pt.SourceDatabase, pt.SourceTable)
}

func getSqlForTaskIdAndName(ses *Session, all bool, taskName string) string {
	if all {
		return fmt.Sprintf(getCdcTaskId, ses.GetAccountId())
	} else {
		return fmt.Sprintf(getCdcTaskIdWhere, ses.GetAccountId(), taskName)
	}
}

func getSqlForDropCdcMeta(ses *Session, taskId string) string {
	return fmt.Sprintf(dropCdcMeta, ses.GetAccountId(), taskId)
}

func getSqlForUpdateCdcMeta(ses *Session, taskId string, status string) string {
	return fmt.Sprintf(updateCdcMeta, status, ses.GetAccountId(), taskId)
}

const (
	MysqlSink     = "mysql"
	MatrixoneSink = "matrixone"

	SASCommon = "common"
	SASError  = "error"

	SyncLoading = "loading"
	SyncRunning = "running"
	SyncStopped = "stopped"
)

func handleCreateCdc(ses *Session, execCtx *ExecCtx, create *tree.CreateCDC) error {
	return doCreateCdc(execCtx.reqCtx, ses, create)
}

func doCreateCdc(ctx context.Context, ses *Session, create *tree.CreateCDC) (err error) {
	fmt.Fprintln(os.Stderr, "===>create cdc", create.Tables)
	ts := getGlobalPu().TaskService
	if ts == nil {
		return moerr.NewInternalError(ctx, "no task service is found")
	}

	err = createCdc(
		ctx,
		ses,
		ts,
		create,
	)
	return err
}

func createCdc(
	ctx context.Context,
	ses *Session,
	ts taskservice.TaskService,
	create *tree.CreateCDC,
) error {
	accInfo := ses.GetTenantInfo()
	tasks, err := ts.QueryDaemonTask(ctx,
		taskservice.WithTaskType(taskservice.EQ,
			pb.TaskType_CreateCdc.String()),
		taskservice.WithAccountID(taskservice.EQ,
			accInfo.GetTenantID()),
	)
	if err != nil {
		return err
	}
	for _, t := range tasks {
		_, ok := t.Details.Details.(*pb.Details_CreateCdc)
		if !ok {
			return moerr.NewInternalError(ctx, fmt.Sprintf("invalid task type %s",
				t.TaskType.String()))
		}
		//if dc.Connector.TableName == tableName && duplicate(t, options) {
		//	// do not return error if ifNotExists is true since the table is not actually created
		//	if ifNotExists {
		//		return nil
		//	}
		//	return moerr.NewErrDuplicateConnector(ctx, tableName)
		//}
	}
	cdcId, _ := uuid.NewV7()
	err = saveCdcTask(ctx, ses, cdcId, create)
	if err != nil {
		return err
	}

	details := &pb.Details{
		AccountID: accInfo.GetTenantID(),
		Account:   accInfo.GetTenant(),
		Username:  accInfo.GetUser(),
		Details: &pb.Details_CreateCdc{
			CreateCdc: &pb.CreateCdcDetails{
				TaskName:  create.TaskName,
				AccountId: uint64(accInfo.GetTenantID()),
				TaskId:    cdcId.String(),
			},
		},
	}
	if err = ts.CreateDaemonTask(ctx, cdcTaskMetadata(cdcId.String()), details); err != nil {
		return err
	}
	return nil
}

func cdcTaskMetadata(cdcId string) pb.TaskMetadata {
	return pb.TaskMetadata{
		ID:       cdcId,
		Executor: pb.TaskCode_InitCdc,
		Options: pb.TaskOptions{
			MaxRetryTimes: defaultConnectorTaskMaxRetryTimes,
			RetryInterval: defaultConnectorTaskRetryInterval,
			DelayDuration: 0,
			Concurrency:   0,
		},
	}
}

func string2uint64(str string) (res uint64, err error) {
	if str != "" {
		res, err = strconv.ParseUint(str, 10, 64)
		if err != nil {
			return 0, err
		}
	} else {
		res = uint64(0)
	}
	return res, nil
}

type PatternTuple struct {
	SourceAccount  string
	SourceDatabase string
	SourceTable    string
	SinkAccount    string
	SinkDatabase   string
	SinkTable      string
}

func splitPattern(pattern string) (*PatternTuple, error) {
	pt := &PatternTuple{}
	if strings.Contains(pattern, ":") {
		splitRes := strings.Split(pattern, ":")
		if len(splitRes) != 2 {
			return nil, fmt.Errorf("invalid pattern format")
		}

		source := strings.Split(splitRes[0], ".")
		if len(source) != 2 && len(source) != 3 {
			return nil, fmt.Errorf("invalid pattern format")
		}
		if len(source) == 2 {
			pt.SourceDatabase, pt.SourceTable = source[0], source[1]
		} else {
			pt.SourceAccount, pt.SourceDatabase, pt.SourceTable = source[0], source[1], source[2]
		}

		sink := strings.Split(splitRes[1], ".")
		if len(sink) != 2 && len(sink) != 3 {
			return nil, fmt.Errorf("invalid pattern format")
		}
		if len(sink) == 2 {
			pt.SinkDatabase, pt.SinkTable = sink[0], sink[1]
		} else {
			pt.SinkAccount, pt.SinkDatabase, pt.SinkTable = sink[0], sink[1], sink[2]
		}
		return pt, nil
	}

	source := make([]string, 0)
	current := strings.Builder{}
	inRegex := false
	isRegex := false
	for i := 0; i < len(pattern); i++ {
		char := pattern[i]
		if char == '/' {
			isRegex = true
			inRegex = !inRegex
		} else if char == '.' && !inRegex {
			res := current.String()
			if !isRegex {
				res = strings.ReplaceAll(res, ".", "?")
				res = strings.ReplaceAll(res, "*", ".*")

			}
			isRegex = false
			source = append(source, res)
			current.Reset()
		} else {
			current.WriteByte(char)
		}
	}
	if current.Len() > 0 {
		res := current.String()
		if !isRegex {
			res = strings.ReplaceAll(res, ".", "?")
			res = strings.ReplaceAll(res, "*", ".*")

		}
		isRegex = false
		source = append(source, res)
		current.Reset()
	}
	if (len(source) != 2 && len(source) != 3) || inRegex {
		return nil, fmt.Errorf("invalid pattern format")
	}
	if len(source) == 2 {
		pt.SourceDatabase, pt.SourceTable = source[0], source[1]
	} else {
		pt.SourceAccount, pt.SourceDatabase, pt.SourceTable = source[0], source[1], source[2]
	}
	return pt, nil
}

func string2patterns(pattern string) ([]*PatternTuple, error) {
	pts := make([]*PatternTuple, 0)
	current := strings.Builder{}
	inRegex := false
	for i := 0; i < len(pattern); i++ {
		char := pattern[i]
		if char == '/' {
			inRegex = !inRegex
		}
		if char == ',' && !inRegex {
			pt, err := splitPattern(current.String())
			if err != nil {
				return nil, err
			}
			pts = append(pts, pt)
			current.Reset()
		} else {
			current.WriteByte(char)
		}
	}

	if current.Len() != 0 {
		pt, err := splitPattern(current.String())
		if err != nil {
			return nil, err
		}
		pts = append(pts, pt)
	}
	return pts, nil
}

func patterns2tables(ctx context.Context, ses *Session, pts []*PatternTuple, bh BackgroundExec) (map[string]string, error) {
	resMap := make(map[string]string)
	for _, pt := range pts {
		if pt.SourceAccount == "" {
			pt.SourceAccount = ses.GetTenantName()
		}
		sql := getSqlForTables(pt)
		bh.ClearExecResultSet()
		err := bh.Exec(ctx, sql)
		if err != nil {
			return nil, err
		}
		erArray, err := getResultSet(ctx, bh)
		if err != nil {
			return nil, err
		}
		if execResultArrayHasData(erArray) {
			res := erArray[0]
			for rowIdx := range erArray[0].GetRowCount() {
				sourceString := strings.Builder{}
				sinkString := strings.Builder{}
				acc, err := res.GetString(ctx, rowIdx, 0)
				if err != nil {
					return nil, err
				}
				db, err := res.GetString(ctx, rowIdx, 1)
				if err != nil {
					return nil, err
				}
				tbl, err := res.GetString(ctx, rowIdx, 2)
				if err != nil {
					return nil, err
				}
				sourceString.WriteString(acc)
				sourceString.WriteString(".")
				sourceString.WriteString(db)
				sourceString.WriteString(".")
				sourceString.WriteString(tbl)
				if pt.SinkTable != "" {
					if pt.SinkAccount != "" {
						sinkString.WriteString(pt.SinkAccount)
						sinkString.WriteString(".")
					}
					sinkString.WriteString(pt.SinkDatabase)
					sinkString.WriteString(".")
					sinkString.WriteString(pt.SinkTable)
				} else {
					sinkString.WriteString(sourceString.String())
				}
				resMap[sourceString.String()] = sinkString.String()
			}
		}
	}
	return resMap, nil
}

func saveCdcTask(
	ctx context.Context,
	ses *Session,
	cdcId uuid.UUID,
	create *tree.CreateCDC,
) error {
	var startTs, endTs uint64
	var err error
	accInfo := ses.GetTenantInfo()

	fmt.Fprintln(os.Stderr, "====>save cdc task",
		accInfo.GetTenantID(),
		cdcId,
		create.TaskName,
		create.SourceUri,
		create.SinkUri,
		create.SinkType,
	)
	cdcTaskOptionsMap := make(map[string]string)
	for i := 0; i < len(create.Option); i += 2 {
		cdcTaskOptionsMap[create.Option[i]] = create.Option[i+1]
	}

	startTs, err = string2uint64(cdcTaskOptionsMap["StartTS"])
	if err != nil {
		return err
	}
	endTs, err = string2uint64(cdcTaskOptionsMap["EndTS"])
	if err != nil {
		return err
	}
	pts, err := string2patterns(create.Tables)
	if err != nil {
		return err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	tablesMap, err := patterns2tables(ctx, ses, pts, bh)
	if err != nil {
		return err
	}
	fmt.Println("=============>", tablesMap)
	dat := time.Now().UTC()

	//TODO: make it better
	//Currently just for test
	sql := getSqlForNewCdcTask(
		uint64(accInfo.GetTenantID()),
		cdcId,
		create.TaskName,
		create.SourceUri,
		"FIXME",
		create.SinkUri,
		create.SinkType,
		"FIXME",
		"",
		"",
		"",
		create.Tables,
		"FIXME",
		"FIXME",
		SASCommon,
		SASCommon,
		startTs,
		endTs,
		cdcTaskOptionsMap["ConfigFile"],
		dat,
		SyncLoading,
		0, //FIXME
		"FIXME",
		"FIXME",
	)

	err = bh.Exec(ctx, sql)
	if err != nil {
		return err
	}

	return nil
}

func RegisterCdcExecutor(
	logger *zap.Logger,
	ts taskservice.TaskService,
	ieFactory func() ie.InternalExecutor,
	attachToTask func(context.Context, uint64, taskservice.ActiveRoutine) error,
	createTxnClient func(bool) (client.TxnClient, client.TimestampWaiter, error),
	cnUUID string,
	fileService fileservice.FileService,
	cnTxnClient client.TxnClient,
	cnEngine engine.Engine,
) func(ctx context.Context, task task.Task) error {
	return func(ctx context.Context, T task.Task) error {
		fmt.Fprintln(os.Stderr, "====>", "cdc task executor")
		ctx1, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		tasks, err := ts.QueryDaemonTask(ctx1,
			taskservice.WithTaskIDCond(taskservice.EQ, T.GetID()),
		)
		if err != nil {
			return err
		}
		if len(tasks) != 1 {
			return moerr.NewInternalError(ctx, "invalid tasks count %d", len(tasks))
		}
		details, ok := tasks[0].Details.Details.(*task.Details_CreateCdc)
		if !ok {
			return moerr.NewInternalError(ctx, "invalid details type")
		}

		fmt.Fprintln(os.Stderr, "====>", "cdc task info 1", tasks[0].String())
		accId := details.CreateCdc.GetAccountId()
		taskId := details.CreateCdc.GetTaskId()
		taskName := details.CreateCdc.GetTaskName()

		fmt.Fprintln(os.Stderr, "====>", "cdc task info 2", accId, taskId, taskName)

		cdc := NewCdcTask(
			logger,
			ieFactory(),
			details.CreateCdc,
			createTxnClient,
			cnUUID,
			fileService,
			cnTxnClient,
			cnEngine,
		)
		cdc.activeRoutine = cdc2.NewCdcActiveRoutine()
		if err := attachToTask(ctx, T.GetID(), cdc); err != nil {
			return err
		}
		err = cdc.Start(ctx)

		return err
	}
}

type CdcTask struct {
	logger *zap.Logger
	ie     ie.InternalExecutor

	cnUUID          string
	cnTxnClient     client.TxnClient
	cnEngine        engine.Engine
	fileService     fileservice.FileService
	createTxnClient func(bool) (client.TxnClient, client.TimestampWaiter, error)

	cdcTask       *task.CreateCdcDetails
	cdcTxnClient  client.TxnClient
	cdcTsWaiter   client.TimestampWaiter
	cdcEngMp      *mpool.MPool
	cdcEngine     engine.Engine
	cdcTable      *disttae.CdcRelation
	activeRoutine *cdc2.ActiveRoutine
}

func NewCdcTask(
	logger *zap.Logger,
	ie ie.InternalExecutor,
	cdcTask *task.CreateCdcDetails,
	createTxnClient func(bool) (client.TxnClient, client.TimestampWaiter, error),
	cnUUID string,
	fileService fileservice.FileService,
	cnTxnClient client.TxnClient,
	cnEngine engine.Engine,
) *CdcTask {
	return &CdcTask{
		logger:          logger,
		ie:              ie,
		cdcTask:         cdcTask,
		createTxnClient: createTxnClient,
		cnUUID:          cnUUID,
		fileService:     fileService,
		cnTxnClient:     cnTxnClient,
		cnEngine:        cnEngine,
	}
}

func (cdc *CdcTask) Start(rootCtx context.Context) (err error) {
	fmt.Fprintln(os.Stderr, "====>cdc start")
	ctx := defines.AttachAccountId(rootCtx, uint32(cdc.cdcTask.AccountId))

	cdcTaskId, _ := uuid.Parse(cdc.cdcTask.TaskId)

	//step1 : get cdc task definition
	sql := getSqlForRetrivingNewCdcTask(cdc.cdcTask.AccountId, cdcTaskId)
	res := cdc.ie.Query(ctx, sql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		return res.Error()
	}

	if res.RowCount() < 1 {
		return moerr.NewInternalError(ctx, "none cdc task for %d %s", cdc.cdcTask.AccountId, cdc.cdcTask.TaskId)
	} else if res.RowCount() > 1 {
		return moerr.NewInternalError(ctx, "duplicate cdc task for %d %s", cdc.cdcTask.AccountId, cdc.cdcTask.TaskId)
	}

	//sink uri
	sinkUri, err := res.StrValue(ctx, 0, 0)
	if err != nil {
		return err
	}

	//sink_type
	sinkTyp, err := res.StrValue(ctx, 0, 1)
	if err != nil {
		return err
	}

	//sink_password
	sinkPwd, err := res.StrValue(ctx, 0, 2)
	if err != nil {
		return err
	}

	//tables
	tables, err := res.StrValue(ctx, 0, 3)
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "====>", "cdc task row", sinkUri, sinkTyp, sinkPwd, tables)

	//step2 : create cdc engine.

	fs, err := fileservice.Get[fileservice.FileService](cdc.fileService, defines.SharedFileServiceName)
	if err != nil {
		return err
	}

	etlFs, err := fileservice.Get[fileservice.FileService](cdc.fileService, defines.ETLFileServiceName)
	if err != nil {
		return err
	}

	cdc.cdcTxnClient, cdc.cdcTsWaiter, err = cdc.createTxnClient(false)
	if err != nil {
		return err
	}

	cdc.cdcEngMp, err = mpool.NewMPool("cdc", 0, mpool.NoFixed)
	if err != nil {
		return err
	}

	inQueue := cdc2.NewMemQ[tools.Pair[*disttae.TableCtx, *disttae.DecoderInput]]()
	cdc.cdcEngine = disttae.NewCdcEngine(
		ctx,
		cdc.cnUUID,
		cdc.cdcEngMp,
		fs,
		etlFs,
		cdc.cdcTxnClient,
		cdc.cdcTask.GetTaskId(),
		inQueue,
		cdc.cnEngine,
		cdc.cnTxnClient,
	)
	cdcEngine := cdc.cdcEngine.(*disttae.CdcEngine)

	if sinkTyp != MysqlSink && sinkTyp != MatrixoneSink {
		return moerr.NewInternalError(ctx, "unsupported sink type: %s", sinkTyp)
	}

	err = disttae.InitLogTailPushModel(ctx, cdcEngine, cdc.cdcTsWaiter)
	if err != nil {
		return err
	}

	//TODO: refine it
	start := time.Now()
	for !cdcEngine.PushClient().IsSubscriberReady() {
		time.Sleep(30 * time.Millisecond)
		if time.Since(start) > time.Second*10 {
			break
		}
	}

	if !cdcEngine.PushClient().IsSubscriberReady() {
		return moerr.NewInternalError(ctx, "cdc pushClient is not ready")
	}

	//step3 : subscribe the table
	//TODO:add multiple tables
	var dbId, tableId uint64
	dbTables := make([]tools.Pair[string, string], 0)
	dbTableIds := make([]tools.Pair[uint64, uint64], 0)
	tableList := strings.Split(tables, ",")
	for _, table := range tableList {
		//get dbid tableid for the table
		seps := strings.Split(table, ".")
		if len(seps) != 2 {
			return moerr.NewInternalError(ctx, "invalid tables format")
		}
		dbTablePair := tools.Pair[string, string]{
			Key:   seps[0],
			Value: seps[1],
		}
		dbTables = append(dbTables, dbTablePair)
		sql = getSqlForDbIdAndTableId(cdc.cdcTask.AccountId, dbTablePair.Key, dbTablePair.Value)
		res = cdc.ie.Query(ctx, sql, ie.SessionOverrideOptions{})
		if res.Error() != nil {
			return res.Error()
		}

		if res.RowCount() < 1 {
			return moerr.NewInternalError(ctx, "no table %s:%s", dbTablePair.Key, dbTablePair.Value)
		} else if res.RowCount() > 1 {
			return moerr.NewInternalError(ctx, "duplicate table %s:%s", dbTablePair.Key, dbTablePair.Value)
		}

		dbId, err = res.U64Value(ctx, 0, 0)
		if err != nil {
			return err
		}

		tableId, err = res.U64Value(ctx, 0, 1)
		if err != nil {
			return err
		}

		dbTableIds = append(dbTableIds, tools.Pair[uint64, uint64]{
			Key:   dbId,
			Value: tableId,
		})
	}
	//dbid 0, tableid 0 reserved for heartbeat
	dbTableIds = append(dbTableIds, tools.Pair[uint64, uint64]{
		Key:   0,
		Value: 0,
	})

	//                         + == inputCh == > decoder == interCh == > sinker -> remote db    // for table 1
	// 	                       |
	// inQueue -> partitioner -+ == inputCh == > decoder == interCh == > sinker -> remote db    // for table 2
	//	                       |                                                                   ...
	//	                       |                                                                   ...
	// 						   + == inputCh == > decoder == interCh == > sinker -> remote db	// for table n

	// make channels between partitioner and decoder
	inputChs := make(map[uint64]chan tools.Pair[*disttae.TableCtx, *disttae.DecoderInput], len(dbTableIds))
	for _, dbTableId := range dbTableIds {
		tableId = dbTableId.Value
		inputChs[tableId] = make(chan tools.Pair[*disttae.TableCtx, *disttae.DecoderInput])
	}

	// make channels between decoder and sinker
	interChs := make(map[uint64]chan tools.Pair[*disttae.TableCtx, *cdc2.DecoderOutput], len(dbTableIds))
	for _, dbTableId := range dbTableIds {
		tableId = dbTableId.Value
		interChs[tableId] = make(chan tools.Pair[*disttae.TableCtx, *cdc2.DecoderOutput])
	}

	// make partitioner
	partitioner := cdc2.NewPartitioner(inQueue, inputChs)
	go partitioner.Run(ctx, cdc.activeRoutine)

	// make decoders
	decoders := make(map[uint64]cdc2.Decoder)
	for _, dbTableId := range dbTableIds {
		tableId = dbTableId.Value
		decoder := cdc2.NewDecoder(cdc.cdcEngMp, fs, tableId, inputChs[tableId], interChs[tableId])
		decoders[tableId] = decoder

		go decoder.Run(ctx, cdc.activeRoutine)
	}

	// make sinkers
	sinkers := make(map[uint64]cdc2.Sinker)
	for _, dbTableId := range dbTableIds {
		tableId = dbTableId.Value
		sinker, err := cdc2.NewSinker(ctx, sinkUri, interChs[tableId])
		if err != nil {
			return err
		}
		sinkers[tableId] = sinker

		go sinker.Run(ctx, cdc.activeRoutine)
	}

	ch := make(chan int, 1)
	cdcTables := make([]*disttae.CdcRelation, 0)
	for i, pair := range dbTableIds {
		//skip heartbeat
		if pair.Key == 0 || pair.Value == 0 {
			continue
		}
		cdcTbl := disttae.NewCdcRelation(
			dbTables[i].Key,
			dbTables[i].Value,
			cdc.cdcTask.AccountId,
			pair.Key,
			pair.Value,
			cdcEngine)
		fmt.Fprintln(os.Stderr, "====>", "cdc SubscribeTable",
			dbId, tableId, "before")
		err = disttae.SubscribeCdcTable(ctx, cdcTbl, pair.Key, pair.Value)
		if err != nil {
			return err
		}

		cdcTables = append(cdcTables, cdcTbl)

		//step4 : process partition state
		fmt.Fprintf(os.Stderr, "====> cdc SubscribeTable %v:%v after\n",
			dbId, tableId,
		)
	}

	//TODO:

	<-ch

	return nil
}

func (cdc *CdcTask) Resume() error {
	//TODO: DO SOMETHING
	return nil
}

func (cdc *CdcTask) Pause() error {
	close(cdc.activeRoutine.Pause)
	return nil
}

func (cdc *CdcTask) Cancel() error {
	close(cdc.activeRoutine.Cancel)
	return nil
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

func updateCdc(ctx context.Context, ses *Session, st tree.Statement) error {
	var err error
	var sql string
	var targetTaskStatus, targetTaskRequest task.TaskStatus
	var targetCdcStatus string
	var allowedTaskStatus task.TaskStatus

	ts := getGlobalPu().TaskService
	if ts == nil {
		return moerr.NewInternalError(ctx,
			"task service not ready yet, please try again later.")
	}
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	err = bh.Exec(ctx, "begin;")
	if err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	switch stmt := st.(type) {
	case *tree.DropCDC:
		sql = getSqlForTaskIdAndName(ses, stmt.Option.All, stmt.Option.TaskName)
		targetTaskStatus, targetTaskRequest = task.TaskStatus_Canceled, task.TaskStatus_CancelRequested
		targetCdcStatus = SyncStopped
	case *tree.PauseCDC:
		sql = getSqlForTaskIdAndName(ses, stmt.Option.All, stmt.Option.TaskName)
		targetTaskStatus, targetTaskRequest = task.TaskStatus_Paused, task.TaskStatus_PauseRequested
		allowedTaskStatus = task.TaskStatus_Running
		targetCdcStatus = SyncStopped
	case *tree.ResumeCDC:
		sql = getSqlForTaskIdAndName(ses, false, stmt.TaskName)
		targetTaskStatus, targetTaskRequest = task.TaskStatus_Running, task.TaskStatus_ResumeRequested
		allowedTaskStatus = task.TaskStatus_Paused
		targetCdcStatus = SyncRunning
	}
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return err
	}
	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return err
	}
	var taskIds []string
	if execResultArrayHasData(erArray) {
		rowCount := erArray[0].GetRowCount()
		taskIds = make([]string, 0, rowCount)
		for i := uint64(0); i < rowCount; i++ {
			taskId, err := erArray[0].GetString(ctx, i, 0)
			if err != nil {
				return err
			}
			taskIds = append(taskIds, taskId)
		}
	} else {
		return moerr.NewInternalError(ctx, "There is no any cdc task.")
	}

	daemonTasks, err := ts.QueryDaemonTask(ctx,
		taskservice.WithAccountID(taskservice.EQ, ses.accountId),
	)
	if err != nil {
		return err
	}

	for _, taskId := range taskIds {
		for idx := range daemonTasks {
			daemonTask := &daemonTasks[idx]
			if taskId == daemonTask.Details.Details.(*pb.Details_CreateCdc).CreateCdc.TaskId {
				if daemonTask.TaskStatus == targetTaskStatus || daemonTask.TaskStatus == targetTaskRequest {
					return nil
				} else if targetTaskStatus != task.TaskStatus_Canceled && daemonTask.TaskStatus != allowedTaskStatus {
					return moerr.NewInternalError(ctx,
						"task can be resumed only if it is in %s state, now it is %s",
						allowedTaskStatus,
						daemonTask.TaskStatus)
				}
				daemonTask.TaskStatus = targetTaskRequest
				if targetTaskStatus == task.TaskStatus_Canceled {
					sql = getSqlForDropCdcMeta(ses, taskId)
				} else {
					sql = getSqlForUpdateCdcMeta(ses, taskId, targetCdcStatus)
				}
				bh.ClearExecResultSet()
				err = bh.Exec(ctx, sql)
				if err != nil {
					return err
				}
			}
		}
	}
	c, err := ts.UpdateDaemonTask(ctx, daemonTasks)
	if err != nil {
		return err
	}
	if c < 1 {
		return moerr.NewInternalError(ctx, "update daemon task status failed.")
	}
	return nil
}
