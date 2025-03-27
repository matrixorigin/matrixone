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
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
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

var queryTable = func(
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
		cdcTask := NewCdcTask(
			logger,
			ieFactory(),
			details.CreateCdc,
			cnUUID,
			fileService,
			cnTxnClient,
			cnEngine,
			cnEngMp,
		)
		cdcTask.activeRoutine = cdc.NewCdcActiveRoutine()
		if err = attachToTask(ctx, T.GetID(), cdcTask); err != nil {
			return err
		}
		return cdcTask.Start(ctx)
	}
}

type CdcTask struct {
	sync.Mutex

	logger *zap.Logger
	ie     ie.InternalExecutor

	cnUUID      string
	cnTxnClient client.TxnClient
	cnEngine    engine.Engine
	fileService fileservice.FileService

	cdcTask *task.CreateCdcDetails

	mp         *mpool.MPool
	packerPool *fileservice.Pool[*types.Packer]

	sinkUri          cdc.UriInfo
	tables           cdc.PatternTuples
	exclude          *regexp.Regexp
	startTs, endTs   types.TS
	noFull           bool
	additionalConfig map[string]interface{}

	activeRoutine *cdc.ActiveRoutine
	// watermarkUpdater update the watermark of the items that has been sunk to downstream
	watermarkUpdater cdc.IWatermarkUpdater
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

func (cdcTask *CdcTask) Start(rootCtx context.Context) (err error) {
	taskId := cdcTask.cdcTask.TaskId
	taskName := cdcTask.cdcTask.TaskName
	cnUUID := cdcTask.cnUUID
	accountId := uint32(cdcTask.cdcTask.Accounts[0].GetId())
	logutil.Infof("cdc task %s start on cn %s", taskName, cnUUID)

	defer func() {
		if err != nil {
			logutil.Errorf("cdc task %s start failed, err: %v", taskName, err)

			// if Start failed, there will be some dangle goroutines(watermarkUpdater, reader, sinker...)
			// need to close them to avoid goroutine leak
			cdcTask.activeRoutine.ClosePause()
			cdcTask.activeRoutine.CloseCancel()

			if updateErrMsgErr := cdcTask.updateErrMsg(rootCtx, err.Error()); updateErrMsgErr != nil {
				logutil.Errorf("cdc task %s update err msg failed, err: %v", taskName, updateErrMsgErr)
			}
		}
	}()

	ctx := defines.AttachAccountId(rootCtx, accountId)

	// get cdc task definition
	if err = cdcTask.retrieveCdcTask(ctx); err != nil {
		return err
	}

	// reset runningReaders
	cdcTask.runningReaders = &sync.Map{}

	// start watermarkUpdater
	cdcTask.watermarkUpdater = cdc.NewWatermarkUpdater(accountId, taskId, cdcTask.ie)
	go cdcTask.watermarkUpdater.Run(ctx, cdcTask.activeRoutine)

	// register to table scanner
	cdc.GetTableScanner(cnUUID).Register(taskId, cdcTask.handleNewTables)

	cdcTask.isRunning = true
	logutil.Infof("cdc task %s start on cn %s success", taskName, cnUUID)
	// start success, clear err msg
	if err = cdcTask.updateErrMsg(ctx, ""); err != nil {
		logutil.Errorf("cdc task %s update err msg failed, err: %v", taskName, err)
		err = nil
	}

	// hold
	cdcTask.holdCh = make(chan int, 1)
	select {
	case <-ctx.Done():
		break
	case <-cdcTask.holdCh:
		break
	}
	return
}

// Resume cdc task from last recorded watermark
func (cdcTask *CdcTask) Resume() error {
	logutil.Infof("cdc task %s resume", cdcTask.cdcTask.TaskName)
	defer func() {
		logutil.Infof("cdc task %s resume success", cdcTask.cdcTask.TaskName)
	}()

	go func() {
		// closed in Pause, need renew
		cdcTask.activeRoutine = cdc.NewCdcActiveRoutine()
		_ = cdcTask.startFunc(context.Background())
	}()
	return nil
}

// Restart cdc task from init watermark
func (cdcTask *CdcTask) Restart() error {
	logutil.Infof("cdc task %s restart", cdcTask.cdcTask.TaskName)
	defer func() {
		logutil.Infof("cdc task %s restart success", cdcTask.cdcTask.TaskName)
	}()

	if cdcTask.isRunning {
		cdc.GetTableScanner(cdcTask.cnUUID).UnRegister(cdcTask.cdcTask.TaskId)
		cdcTask.activeRoutine.CloseCancel()
		cdcTask.isRunning = false
		// let Start() go
		cdcTask.holdCh <- 1
	}

	go func() {
		cdcTask.activeRoutine = cdc.NewCdcActiveRoutine()
		_ = cdcTask.startFunc(context.Background())
	}()
	return nil
}

// Pause cdc task
func (cdcTask *CdcTask) Pause() error {
	logutil.Infof("cdc task %s pause", cdcTask.cdcTask.TaskName)
	defer func() {
		logutil.Infof("cdc task %s pause success", cdcTask.cdcTask.TaskName)
	}()

	if cdcTask.isRunning {
		cdc.GetTableScanner(cdcTask.cnUUID).UnRegister(cdcTask.cdcTask.TaskId)
		cdcTask.activeRoutine.ClosePause()
		cdcTask.isRunning = false
		// let Start() go
		cdcTask.holdCh <- 1
	}
	return nil
}

// Cancel cdc task
func (cdcTask *CdcTask) Cancel() error {
	logutil.Infof("cdc task %s cancel", cdcTask.cdcTask.TaskName)
	defer func() {
		logutil.Infof("cdc task %s cancel success", cdcTask.cdcTask.TaskName)
	}()

	if cdcTask.isRunning {
		cdc.GetTableScanner(cdcTask.cnUUID).UnRegister(cdcTask.cdcTask.TaskId)
		cdcTask.activeRoutine.CloseCancel()
		cdcTask.isRunning = false
		// let Start() go
		cdcTask.holdCh <- 1
	}
	return nil
}

func (cdcTask *CdcTask) initAesKeyByInternalExecutor(ctx context.Context, accountId uint32) (err error) {
	if len(cdc.AesKey) > 0 {
		return nil
	}

	querySql := cdc.CDCSQLBuilder.GetDataKeySQL(uint64(accountId), cdc.InitKeyId)
	res := cdcTask.ie.Query(ctx, querySql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		return res.Error()
	} else if res.RowCount() < 1 {
		return moerr.NewInternalErrorf(ctx, "no data key record for account %d", accountId)
	}

	encryptedKey, err := res.GetString(ctx, 0, 0)
	if err != nil {
		return err
	}

	cdc.AesKey, err = cdc.AesCFBDecodeWithKey(ctx, encryptedKey, []byte(getGlobalPuWrapper(cdcTask.cnUUID).SV.KeyEncryptionKey))
	return
}

func (cdcTask *CdcTask) updateErrMsg(ctx context.Context, errMsg string) (err error) {
	accId := cdcTask.cdcTask.Accounts[0].GetId()
	cdcTaskId, _ := uuid.Parse(cdcTask.cdcTask.TaskId)
	state := CdcRunning
	if errMsg != "" {
		state = CdcFailed
	}
	if len(errMsg) > maxErrMsgLen {
		errMsg = errMsg[:maxErrMsgLen]
	}

	sql := cdc.CDCSQLBuilder.UpdateTaskStateAndErrMsgSQL(uint64(accId), cdcTaskId.String(), state, errMsg)
	return cdcTask.ie.Exec(defines.AttachAccountId(ctx, catalog.System_Account), sql, ie.SessionOverrideOptions{})
}

func (cdcTask *CdcTask) handleNewTables(allAccountTbls map[uint32]cdc.TblMap) {
	// lock to avoid create pipelines for the same table
	cdcTask.Lock()
	defer cdcTask.Unlock()

	accountId := uint32(cdcTask.cdcTask.Accounts[0].GetId())
	ctx := defines.AttachAccountId(context.Background(), accountId)

	txnOp, err := cdc.GetTxnOp(ctx, cdcTask.cnEngine, cdcTask.cnTxnClient, "cdc-handleNewTables")
	if err != nil {
		logutil.Errorf("cdc task %s get txn op failed, err: %v", cdcTask.cdcTask.TaskName, err)
		return
	}
	defer func() {
		cdc.FinishTxnOp(ctx, err, txnOp, cdcTask.cnEngine)
	}()
	if err = cdcTask.cnEngine.New(ctx, txnOp); err != nil {
		logutil.Errorf("cdc task %s new engine failed, err: %v", cdcTask.cdcTask.TaskName, err)
		return
	}

	for key, info := range allAccountTbls[accountId] {
		// already running
		if _, ok := cdcTask.runningReaders.Load(key); ok {
			continue
		}

		if cdcTask.exclude != nil && cdcTask.exclude.MatchString(key) {
			continue
		}

		newTableInfo := info.Clone()
		if !cdcTask.matchAnyPattern(key, newTableInfo) {
			continue
		}

		logutil.Infof("cdc task find new table: %s", newTableInfo)
		if err = cdcTask.addExecPipelineForTable(ctx, newTableInfo, txnOp); err != nil {
			logutil.Errorf("cdc task %s add exec pipeline for table %s failed, err: %v", cdcTask.cdcTask.TaskName, key, err)
		} else {
			logutil.Infof("cdc task %s add exec pipeline for table %s successfully", cdcTask.cdcTask.TaskName, key)
		}
	}
}

func (cdcTask *CdcTask) matchAnyPattern(key string, info *cdc.DbTableInfo) bool {
	match := func(s, p string) bool {
		if p == cdc.CDCPitrGranularity_All {
			return true
		}
		return s == p
	}

	db, table := cdc.SplitDbTblKey(key)
	for _, pt := range cdcTask.tables.Pts {
		if match(db, pt.Source.Database) && match(table, pt.Source.Table) {
			// complete sink info
			info.SinkDbName = pt.Sink.Database
			if info.SinkDbName == cdc.CDCPitrGranularity_All {
				info.SinkDbName = db
			}
			info.SinkTblName = pt.Sink.Table
			if info.SinkTblName == cdc.CDCPitrGranularity_All {
				info.SinkTblName = table
			}
			return true
		}
	}
	return false
}

// reader ----> sinker ----> remote db
func (cdcTask *CdcTask) addExecPipelineForTable(ctx context.Context, info *cdc.DbTableInfo, txnOp client.TxnOperator) (err error) {
	// step 1. init watermarkUpdater
	// get watermark from db
	watermark, err := cdcTask.watermarkUpdater.GetFromDb(info.SourceDbName, info.SourceTblName)
	if moerr.IsMoErrCode(err, moerr.ErrNoWatermarkFound) {
		// add watermark into db if not exists
		watermark = cdcTask.startTs
		if cdcTask.noFull {
			watermark = types.TimestampToTS(txnOp.SnapshotTS())
		}
		if err = cdcTask.watermarkUpdater.InsertIntoDb(info, watermark); err != nil {
			return
		}
	} else if err != nil {
		return
	}
	// clear err msg
	if err = cdcTask.watermarkUpdater.SaveErrMsg(info.SourceDbName, info.SourceTblName, ""); err != nil {
		return
	}
	// add watermark into memory
	cdcTask.watermarkUpdater.UpdateMem(info.SourceDbName, info.SourceTblName, watermark)

	tableDef, err := cdc.GetTableDef(ctx, txnOp, cdcTask.cnEngine, info.SourceTblId)
	if err != nil {
		return
	}

	// step 2. new sinker
	sinker, err := cdc.NewSinker(
		cdcTask.sinkUri,
		info,
		cdcTask.watermarkUpdater,
		tableDef,
		cdc.CDCDefaultRetryTimes,
		cdc.CDCDefaultRetryDuration,
		cdcTask.activeRoutine,
		uint64(cdcTask.additionalConfig[cdc.CDCTaskExtraOptions_MaxSqlLength].(float64)),
		cdcTask.additionalConfig[cdc.CDCTaskExtraOptions_SendSqlTimeout].(string),
	)
	if err != nil {
		return err
	}
	go sinker.Run(ctx, cdcTask.activeRoutine)

	// step 3. new reader
	reader := cdc.NewTableReader(
		cdcTask.cnTxnClient,
		cdcTask.cnEngine,
		cdcTask.mp,
		cdcTask.packerPool,
		info,
		sinker,
		cdcTask.watermarkUpdater,
		tableDef,
		cdcTask.additionalConfig[cdc.CDCTaskExtraOptions_InitSnapshotSplitTxn].(bool),
		cdcTask.runningReaders,
		cdcTask.startTs,
		cdcTask.endTs,
		cdcTask.noFull,
	)
	go reader.Run(ctx, cdcTask.activeRoutine)

	return
}

func (cdcTask *CdcTask) retrieveCdcTask(ctx context.Context) error {
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)

	accId := cdcTask.cdcTask.Accounts[0].GetId()
	cdcTaskId, _ := uuid.Parse(cdcTask.cdcTask.TaskId)
	sql := cdc.CDCSQLBuilder.GetTaskSQL(accId, cdcTaskId.String())
	res := cdcTask.ie.Query(ctx, sql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		return res.Error()
	}

	if res.RowCount() < 1 {
		return moerr.NewInternalErrorf(ctx, "none cdc task for %d %s", accId, cdcTask.cdcTask.TaskId)
	} else if res.RowCount() > 1 {
		return moerr.NewInternalErrorf(ctx, "duplicate cdc task for %d %s", accId, cdcTask.cdcTask.TaskId)
	}

	//sink_type
	sinkTyp, err := res.GetString(ctx, 0, 1)
	if err != nil {
		return err
	}

	if sinkTyp != cdc.CDCSinkType_Console {
		//sink uri
		jsonSinkUri, err := res.GetString(ctx, 0, 0)
		if err != nil {
			return err
		}

		if err = cdc.JsonDecode(jsonSinkUri, &cdcTask.sinkUri); err != nil {
			return err
		}

		//sink_password
		sinkPwd, err := res.GetString(ctx, 0, 2)
		if err != nil {
			return err
		}

		// TODO replace with creatorAccountId
		if err = cdcTask.initAesKeyByInternalExecutor(ctx, catalog.System_Account); err != nil {
			return err
		}

		if cdcTask.sinkUri.Password, err = cdc.AesCFBDecode(ctx, sinkPwd); err != nil {
			return err
		}
	}

	//update sink type after deserialize
	cdcTask.sinkUri.SinkTyp = sinkTyp

	// tables
	jsonTables, err := res.GetString(ctx, 0, 3)
	if err != nil {
		return err
	}

	if err = cdc.JsonDecode(jsonTables, &cdcTask.tables); err != nil {
		return err
	}

	// exclude
	exclude, err := res.GetString(ctx, 0, 4)
	if err != nil {
		return err
	}
	if exclude != "" {
		if cdcTask.exclude, err = regexp.Compile(exclude); err != nil {
			return err
		}
	}

	// startTs
	startTs, err := res.GetString(ctx, 0, 5)
	if err != nil {
		return err
	}
	if cdcTask.startTs, err = CDCStrToTS(startTs); err != nil {
		return err
	}

	// endTs
	endTs, err := res.GetString(ctx, 0, 6)
	if err != nil {
		return err
	}
	if cdcTask.endTs, err = CDCStrToTS(endTs); err != nil {
		return err
	}

	// noFull
	noFull, err := res.GetString(ctx, 0, 7)
	if err != nil {
		return err
	}
	cdcTask.noFull, _ = strconv.ParseBool(noFull)

	// additionalConfig
	additionalConfigStr, err := res.GetString(ctx, 0, 8)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(additionalConfigStr), &cdcTask.additionalConfig)
}

var initAesKeyByInternalExecutor = func(ctx context.Context, cdcTask *CdcTask, accountId uint32) error {
	return cdcTask.initAesKeyByInternalExecutor(ctx, accountId)
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

	query := cdc.CDCSQLBuilder.GetTaskIdSQL(accountId, taskName)

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
		updateSql := cdc.CDCSQLBuilder.UpdateTaskStateSQL(accountId, taskName)
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
		deleteSql := cdc.CDCSQLBuilder.DeleteTaskSQL(accountId, taskName)
		cnt, err = ExecuteAndGetRowsAffected(ctx, tx, deleteSql)
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

func deleteWatermark(ctx context.Context, tx taskservice.SqlExecutor, taskKeyMap map[taskservice.CdcTaskKey]struct{}) (int64, error) {
	tCount := int64(0)
	cnt := int64(0)
	var err error
	//deleting mo_cdc_watermark belongs to cancelled cdc task
	for tInfo := range taskKeyMap {
		deleteSql2 := cdc.CDCSQLBuilder.DeleteWatermarkSQL(tInfo.AccountId, tInfo.TaskId)
		cnt, err = ExecuteAndGetRowsAffected(ctx, tx, deleteSql2)
		if err != nil {
			return 0, err
		}
		tCount += cnt
	}
	return tCount, nil
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
		sourceUriInfo cdc.UriInfo
		sinkUriInfo   cdc.UriInfo
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
	txnOp, err := cdc.GetTxnOp(ctx, pu.StorageEngine, pu.TxnClient, "cdc-handleShowCdc")
	if err != nil {
		return err
	}
	defer func() {
		cdc.FinishTxnOp(ctx, err, txnOp, pu.StorageEngine)
	}()
	timestamp := txnOp.SnapshotTS().ToStdTime().In(time.Local).String()

	// get from task table
	sql := cdc.CDCSQLBuilder.ShowTaskSQL(uint64(ses.GetTenantInfo().GetTenantID()), st.Option.All, string(st.Option.TaskName))

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
			if err = cdc.JsonDecode(sourceUri, &sourceUriInfo); err != nil {
				return
			}
			if err = cdc.JsonDecode(sinkUri, &sinkUriInfo); err != nil {
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

	sql := cdc.CDCSQLBuilder.GetWatermarkSQL(uint64(accountId), taskId)
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
