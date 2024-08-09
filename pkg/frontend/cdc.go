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

	if err := createCdc(
		ctx,
		ses,
		ts,
		create,
	); err != nil {
		return err
	}
	return nil
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
	if err = ts.CreateDaemonTask(ctx, cdcTaskMetadata(), details); err != nil {
		return err
	}
	return nil
}

func cdcTaskMetadata() pb.TaskMetadata {
	return pb.TaskMetadata{
		ID:       "-",
		Executor: pb.TaskCode_InitCdc,
		Options: pb.TaskOptions{
			MaxRetryTimes: defaultConnectorTaskMaxRetryTimes,
			RetryInterval: defaultConnectorTaskRetryInterval,
			DelayDuration: 0,
			Concurrency:   0,
		},
	}
}

func saveCdcTask(
	ctx context.Context,
	ses *Session,
	cdcId uuid.UUID,
	create *tree.CreateCDC,
) error {
	accInfo := ses.GetTenantInfo()

	fmt.Fprintln(os.Stderr, "====>save cdc task",
		accInfo.GetTenantID(),
		cdcId,
		create.TaskName,
		create.SourceUri,
		create.SinkUri,
		create.SinkType,
	)

	dat := time.Now().UTC()

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

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
		0, //FIXME
		0, //FIXME
		"FIXME",
		dat,
		SyncLoading,
		0, //FIXME
		"FIXME",
		"FIXME",
	)

	err := bh.Exec(ctx, sql)
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
	createTxnClient func() (client.TxnClient, client.TimestampWaiter, error),
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
	createTxnClient func() (client.TxnClient, client.TimestampWaiter, error)

	cdcTask      *task.CreateCdcDetails
	cdcTxnClient client.TxnClient
	cdcTsWaiter  client.TimestampWaiter
	cdcEngMp     *mpool.MPool
	cdcEngine    engine.Engine
	cdcTable     *disttae.CdcRelation
}

func NewCdcTask(
	logger *zap.Logger,
	ie ie.InternalExecutor,
	cdcTask *task.CreateCdcDetails,
	createTxnClient func() (client.TxnClient, client.TimestampWaiter, error),
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

	//
	val1, err := res.StrValue(ctx, 0, 0)
	if err != nil {
		return err
	}

	val2, err := res.StrValue(ctx, 0, 1)
	if err != nil {
		return err
	}

	val3, err := res.StrValue(ctx, 0, 2)
	if err != nil {
		return err
	}

	tables, err := res.StrValue(ctx, 0, 3)
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "====>", "cdc task row", val1, val2, val3, tables)

	//step2 : create cdc engine.

	fs, err := fileservice.Get[fileservice.FileService](cdc.fileService, defines.SharedFileServiceName)
	if err != nil {
		return err
	}

	etlFs, err := fileservice.Get[fileservice.FileService](cdc.fileService, defines.ETLFileServiceName)
	if err != nil {
		return err
	}

	cdc.cdcTxnClient, cdc.cdcTsWaiter, err = cdc.createTxnClient()
	if err != nil {
		return err
	}

	cdc.cdcEngMp, err = mpool.NewMPool("cdc", 0, mpool.NoFixed)
	if err != nil {
		return err
	}

	inQueue := cdc2.NewMemQ[tools.Pair[*disttae.TableCtx, *disttae.DecoderInput]]()
	outQueue := cdc2.NewMemQ[tools.Pair[*disttae.TableCtx, *cdc2.DecoderOutput]]()
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

	//init cdc decoder or sinker
	go cdc2.RunDecoder(ctx, inQueue, outQueue, cdc2.NewDecoder())

	go cdc2.RunSinker(ctx, outQueue, cdc2.NewConsoleSinker())

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
	seps := strings.Split(tables, ":")
	if len(seps) != 2 {
		return moerr.NewInternalError(ctx, "invalid tables format")
	}
	db := seps[0]
	table := seps[1]
	var dbId, tableId uint64

	//get dbid tableid for the table
	sql = getSqlForDbIdAndTableId(cdc.cdcTask.AccountId, db, table)
	res = cdc.ie.Query(ctx, sql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		return res.Error()
	}

	if res.RowCount() < 1 {
		return moerr.NewInternalError(ctx, "no table %s:%s", db, table)
	} else if res.RowCount() > 1 {
		return moerr.NewInternalError(ctx, "duplicate table %s:%s", db, table)
	}

	//
	dbId, err = res.U64Value(ctx, 0, 0)
	if err != nil {
		return err
	}

	tableId, err = res.U64Value(ctx, 0, 1)
	if err != nil {
		return err
	}

	ch := make(chan int, 1)
	cdcTbl := disttae.NewCdcRelation(db, table, cdc.cdcTask.AccountId, dbId, tableId, cdcEngine)
	fmt.Fprintln(os.Stderr, "====>", "cdc SubscribeTable",
		dbId, tableId, "before")
	err = disttae.SubscribeCdcTable(ctx, cdcTbl, dbId, tableId)
	if err != nil {
		return err
	}

	//step4 : process partition state
	fmt.Fprintf(os.Stderr, "====> cdc SubscribeTable %v:%v after\n",
		dbId, tableId,
	)
	//TODO:

	<-ch

	return nil
}
