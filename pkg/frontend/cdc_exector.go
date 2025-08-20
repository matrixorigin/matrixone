// Copyright 2022 Matrix Origin
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
	"encoding/json"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

var CDCExectorError_QueryDaemonTaskTimeout = moerr.NewInternalErrorNoCtx("query daemon task timeout")

var CDCExeutorAllocator *mpool.MPool

func init() {
	var err error
	mpool.DeleteMPool(CDCExeutorAllocator)
	if CDCExeutorAllocator, err = mpool.NewMPool("cdc_executor", 0, mpool.NoFixed); err != nil {
		panic(err)
	}
}

func CDCTaskExecutorFactory(
	logger *zap.Logger,
	sqlExecutorFactory func() ie.InternalExecutor,
	attachToTask func(context.Context, uint64, taskservice.ActiveRoutine) error,
	cnUUID string,
	ts taskservice.TaskService,
	fs fileservice.FileService,
	txnClient client.TxnClient,
	txnEngine engine.Engine,
) taskservice.TaskExecutor {
	return func(ctx context.Context, spec task.Task) error {
		ctx1, cancel := context.WithTimeoutCause(
			ctx, time.Second*5, CDCExectorError_QueryDaemonTaskTimeout,
		)
		defer cancel()
		tasks, err := ts.QueryDaemonTask(
			ctx1,
			taskservice.WithTaskIDCond(taskservice.EQ, spec.GetID()),
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

		exec := NewCDCTaskExecutor(
			logger,
			sqlExecutorFactory(),
			details.CreateCdc,
			cnUUID,
			fs,
			txnClient,
			txnEngine,
			CDCExeutorAllocator,
		)
		exec.activeRoutine = cdc.NewCdcActiveRoutine()
		if err = attachToTask(ctx, spec.GetID(), exec); err != nil {
			return err
		}
		return exec.Start(ctx)
	}
}

type CDCTaskExecutor struct {
	sync.Mutex

	logger *zap.Logger
	ie     ie.InternalExecutor

	cnUUID      string
	cnTxnClient client.TxnClient
	cnEngine    engine.Engine
	fileService fileservice.FileService

	spec *task.CreateCdcDetails

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
	watermarkUpdater *cdc.CDCWatermarkUpdater
	// runningReaders store the running execute pipelines, map key pattern: db.table
	runningReaders *sync.Map

	isRunning bool
	holdCh    chan int

	// start wrapper, for ut
	startFunc func(ctx context.Context) error
}

func NewCDCTaskExecutor(
	logger *zap.Logger,
	ie ie.InternalExecutor,
	spec *task.CreateCdcDetails,
	cnUUID string,
	fileService fileservice.FileService,
	cnTxnClient client.TxnClient,
	cnEngine engine.Engine,
	cdcMp *mpool.MPool,
) *CDCTaskExecutor {
	task := &CDCTaskExecutor{
		logger:      logger,
		ie:          ie,
		spec:        spec,
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

func (exec *CDCTaskExecutor) Start(rootCtx context.Context) (err error) {
	taskId := exec.spec.TaskId
	taskName := exec.spec.TaskName
	cnUUID := exec.cnUUID
	accountId := uint32(exec.spec.Accounts[0].GetId())
	logutil.Info(
		"CDC-Task-Start",
		zap.String("task-id", taskId),
		zap.String("task-name", taskName),
		zap.String("cn-uuid", cnUUID),
		zap.Uint32("account-id", accountId),
	)

	defer func() {
		if err != nil {
			// if Start failed, there will be some dangle goroutines(watermarkUpdater, reader, sinker...)
			// need to close them to avoid goroutine leak
			exec.activeRoutine.ClosePause()
			exec.activeRoutine.CloseCancel()

			updateErrMsgErr := exec.updateErrMsg(rootCtx, err.Error())
			logutil.Error(
				"CDC-Task-Start-Failed",
				zap.String("task-id", taskId),
				zap.String("task-name", taskName),
				zap.Error(err),
				zap.NamedError("update-err-msg-err", updateErrMsgErr),
			)
		}
	}()

	ctx := defines.AttachAccountId(rootCtx, accountId)

	// get cdc task definition
	if err = exec.retrieveCdcTask(ctx); err != nil {
		return err
	}

	dbs := make([]string, 0, len(exec.tables.Pts))
	tables := make([]string, 0, len(exec.tables.Pts))
	for _, pt := range exec.tables.Pts {
		dbs = append(dbs, pt.Source.Database)
		tables = append(tables, pt.Source.Table)
	}

	// reset runningReaders
	exec.runningReaders = &sync.Map{}

	// start watermarkUpdater
	exec.watermarkUpdater = cdc.GetCDCWatermarkUpdater(exec.cnUUID, exec.ie)

	// register to table scanner
	cdc.GetTableDetector(cnUUID).Register(taskId, accountId, dbs, tables, exec.handleNewTables)

	exec.isRunning = true
	// start success, clear err msg
	clearErrMsgErr := exec.updateErrMsg(ctx, "")

	logutil.Info(
		"CDC-Task-Start-Success",
		zap.String("task-id", taskId),
		zap.String("task-name", taskName),
		zap.NamedError("clear-err-msg-err", clearErrMsgErr),
	)

	// hold
	exec.holdCh = make(chan int, 1)
	select {
	case <-ctx.Done():
		break
	case <-exec.holdCh:
		break
	}
	return
}

// Resume cdc task from last recorded watermark
func (exec *CDCTaskExecutor) Resume() error {
	logutil.Info(
		"CDC-Task-Resume-Start",
		zap.String("task-id", exec.spec.TaskId),
		zap.String("task-name", exec.spec.TaskName),
	)
	defer func() {
		logutil.Info(
			"CDC-Task-Resume-Success",
			zap.String("task-id", exec.spec.TaskId),
			zap.String("task-name", exec.spec.TaskName),
		)
	}()

	go func() {
		// closed in Pause, need renew
		exec.activeRoutine = cdc.NewCdcActiveRoutine()
		_ = exec.startFunc(context.Background())
	}()
	return nil
}

// Restart cdc task from init watermark
func (exec *CDCTaskExecutor) Restart() error {
	logutil.Info(
		"CDC-Task-Restart-Start",
		zap.String("task-id", exec.spec.TaskId),
		zap.String("task-name", exec.spec.TaskName),
	)
	defer func() {
		logutil.Info(
			"CDC-Task-Restart-Success",
			zap.String("task-id", exec.spec.TaskId),
			zap.String("task-name", exec.spec.TaskName),
		)
	}()

	if exec.isRunning {
		cdc.GetTableDetector(exec.cnUUID).UnRegister(exec.spec.TaskId)
		exec.activeRoutine.CloseCancel()
		exec.isRunning = false
		// let Start() go
		exec.holdCh <- 1
	}

	go func() {
		exec.activeRoutine = cdc.NewCdcActiveRoutine()
		_ = exec.startFunc(context.Background())
	}()
	return nil
}

// Pause cdc task
func (exec *CDCTaskExecutor) Pause() error {
	logutil.Info(
		"CDC-Task-Pause-Start",
		zap.String("task-id", exec.spec.TaskId),
		zap.String("task-name", exec.spec.TaskName),
	)
	defer func() {
		logutil.Info(
			"CDC-Task-Pause-Success",
			zap.String("task-id", exec.spec.TaskId),
			zap.String("task-name", exec.spec.TaskName),
		)
	}()

	if exec.isRunning {
		cdc.GetTableDetector(exec.cnUUID).UnRegister(exec.spec.TaskId)
		exec.activeRoutine.ClosePause()
		exec.isRunning = false
		// let Start() go
		exec.holdCh <- 1
	}
	return nil
}

// Cancel cdc task
func (exec *CDCTaskExecutor) Cancel() error {
	logutil.Info(
		"CDC-Task-Cancel-Start",
		zap.String("task-id", exec.spec.TaskId),
		zap.String("task-name", exec.spec.TaskName),
	)
	defer func() {
		logutil.Info(
			"CDC-Task-Cancel-Success",
			zap.String("task-id", exec.spec.TaskId),
			zap.String("task-name", exec.spec.TaskName),
		)
	}()

	if exec.isRunning {
		cdc.GetTableDetector(exec.cnUUID).UnRegister(exec.spec.TaskId)
		exec.activeRoutine.CloseCancel()
		exec.isRunning = false
		// let Start() go
		exec.holdCh <- 1
	}
	return nil
}

func (exec *CDCTaskExecutor) initAesKeyByInternalExecutor(ctx context.Context, accountId uint32) (err error) {
	if len(cdc.AesKey) > 0 {
		return nil
	}

	querySql := cdc.CDCSQLBuilder.GetDataKeySQL(uint64(accountId), cdc.InitKeyId)
	res := exec.ie.Query(ctx, querySql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		return res.Error()
	} else if res.RowCount() < 1 {
		return moerr.NewInternalErrorf(ctx, "no data key record for account %d", accountId)
	}

	encryptedKey, err := res.GetString(ctx, 0, 0)
	if err != nil {
		return err
	}

	cdc.AesKey, err = cdc.AesCFBDecodeWithKey(
		ctx, encryptedKey,
		[]byte(getGlobalPuWrapper(exec.cnUUID).SV.KeyEncryptionKey),
	)
	return
}

func (exec *CDCTaskExecutor) updateErrMsg(ctx context.Context, errMsg string) (err error) {
	accId := exec.spec.Accounts[0].GetId()
	state := cdc.CDCState_Running
	if errMsg != "" {
		state = cdc.CDCState_Failed
	}
	if len(errMsg) > cdc.CDCWatermarkErrMsgMaxLen {
		errMsg = errMsg[:cdc.CDCWatermarkErrMsgMaxLen]
	}

	sql := cdc.CDCSQLBuilder.UpdateTaskStateAndErrMsgSQL(
		uint64(accId),
		exec.spec.TaskId,
		state,
		errMsg,
	)
	return exec.ie.Exec(
		defines.AttachAccountId(ctx, catalog.System_Account),
		sql,
		ie.SessionOverrideOptions{},
	)
}

func (exec *CDCTaskExecutor) handleNewTables(allAccountTbls map[uint32]cdc.TblMap) error {
	// lock to avoid create pipelines for the same table
	// 2025.7, this lock might be needless now
	exec.Lock()
	defer exec.Unlock()

	// if injected, we expect nothing
	if sleepSeconds, injected := objectio.CDCHandleSlowInjected(); injected {
		time.Sleep(time.Duration(sleepSeconds) * time.Second)
	}

	accountId := uint32(exec.spec.Accounts[0].GetId())
	ctx := defines.AttachAccountId(context.Background(), accountId)

	txnOp, err := cdc.GetTxnOp(ctx, exec.cnEngine, exec.cnTxnClient, "cdc-handleNewTables")
	if err != nil {
		logutil.Error(
			"CDC-Task-HandleNewTables-GetTxnOpFailed",
			zap.String("task-id", exec.spec.TaskId),
			zap.String("task-name", exec.spec.TaskName),
			zap.Error(err),
		)
		return err
	}
	defer func() {
		cdc.FinishTxnOp(ctx, err, txnOp, exec.cnEngine)
	}()
	err = exec.cnEngine.New(ctx, txnOp)

	// if injected, we expect the handleNewTables to keep retrying
	if objectio.CDCHandleErrInjected() {
		err = moerr.NewInternalError(context.Background(), "CDC_HANDLENEWTABLES_ERR")
	}

	if err != nil {
		logutil.Error(
			"CDC-Task-HandleNewTables-NewEngineFailed",
			zap.String("task-id", exec.spec.TaskId),
			zap.String("task-name", exec.spec.TaskName),
			zap.Error(err),
		)
		return err
	}

	for key, info := range allAccountTbls[accountId] {
		// already running
		if val, ok := exec.runningReaders.Load(key); ok {
			if reader, ok := val.(cdc.TableReader); ok {
				readerInfo := reader.Info()
				// wait the old reader to stop
				if info.OnlyDiffinTblId(readerInfo) {
					logutil.Infof("cdc task wait old reader to stop %s %d->%d",
						key, readerInfo.SourceTblId, info.SourceTblId)
					waitChan := make(chan struct{})
					go func() {
						defer close(waitChan)
						reader.GetWg().Wait()
					}()
					<-waitChan
				} else {
					continue
				}
			}
		}

		if exec.exclude != nil && exec.exclude.MatchString(key) {
			continue
		}

		newTableInfo := info.Clone()
		if !exec.matchAnyPattern(key, newTableInfo) {
			continue
		}

		logutil.Infof("cdc task find new table: %s", newTableInfo)
		if err = exec.addExecPipelineForTable(ctx, newTableInfo, txnOp); err != nil {
			logutil.Errorf("cdc task %s add exec pipeline for table %s failed, err: %v", exec.spec.TaskName, key, err)
			return err
		} else {
			info.IdChanged = newTableInfo.IdChanged
			logutil.Infof("cdc task %s add exec pipeline for table %s successfully", exec.spec.TaskName, key)
		}
	}
	return nil
}

func (exec *CDCTaskExecutor) matchAnyPattern(key string, info *cdc.DbTableInfo) bool {
	match := func(s, p string) bool {
		if p == cdc.CDCPitrGranularity_All {
			return true
		}
		return s == p
	}

	db, table := cdc.SplitDbTblKey(key)
	for _, pt := range exec.tables.Pts {
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
func (exec *CDCTaskExecutor) addExecPipelineForTable(
	ctx context.Context,
	info *cdc.DbTableInfo,
	txnOp client.TxnOperator,
) (err error) {
	// for ut
	if objectio.CDCAddExecConsumeTruncateInjected() {
		info.IdChanged = false
		return nil
	}

	if objectio.CDCAddExecErrInjected() {
		return moerr.NewInternalErrorNoCtx("CDC_AddExecPipelineForTable_ERR")
	}

	// step 1. init watermarkUpdater
	// get watermark from db
	watermark := exec.startTs
	if exec.noFull {
		watermark = types.TimestampToTS(txnOp.SnapshotTS())
	}
	watermarkKey := cdc.WatermarkKey{
		AccountId: uint64(exec.spec.Accounts[0].GetId()),
		TaskId:    exec.spec.TaskId,
		DBName:    info.SourceDbName,
		TableName: info.SourceTblName,
	}
	if watermark, err = exec.watermarkUpdater.GetOrAddCommitted(
		ctx,
		&watermarkKey,
		&watermark,
	); err != nil {
		return err
	}

	// clear err msg
	if err = exec.watermarkUpdater.UpdateWatermarkErrMsg(
		ctx,
		&watermarkKey,
		"",
	); err != nil {
		return
	}

	tableDef, err := cdc.GetTableDef(ctx, txnOp, exec.cnEngine, info.SourceTblId)
	if err != nil {
		return
	}

	// step 2. new sinker
	sinker, err := cdc.NewSinker(
		exec.sinkUri,
		uint64(exec.spec.Accounts[0].GetId()),
		exec.spec.TaskId,
		info,
		exec.watermarkUpdater,
		tableDef,
		cdc.CDCDefaultRetryTimes,
		cdc.CDCDefaultRetryDuration,
		exec.activeRoutine,
		uint64(exec.additionalConfig[cdc.CDCTaskExtraOptions_MaxSqlLength].(float64)),
		exec.additionalConfig[cdc.CDCTaskExtraOptions_SendSqlTimeout].(string),
	)
	if err != nil {
		return err
	}
	go sinker.Run(ctx, exec.activeRoutine)

	// step 3. new reader
	reader := cdc.NewTableReader(
		exec.cnTxnClient,
		exec.cnEngine,
		exec.mp,
		exec.packerPool,
		uint64(exec.spec.Accounts[0].GetId()),
		exec.spec.TaskId,
		info,
		sinker,
		exec.watermarkUpdater,
		tableDef,
		exec.additionalConfig[cdc.CDCTaskExtraOptions_InitSnapshotSplitTxn].(bool),
		exec.runningReaders,
		exec.startTs,
		exec.endTs,
		exec.noFull,
		exec.additionalConfig[cdc.CDCTaskExtraOptions_Frequency].(string),
	)
	go reader.Run(ctx, exec.activeRoutine)

	return
}

func (exec *CDCTaskExecutor) retrieveCdcTask(ctx context.Context) error {
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)

	accId := exec.spec.Accounts[0].GetId()
	sql := cdc.CDCSQLBuilder.GetTaskSQL(accId, exec.spec.TaskId)
	res := exec.ie.Query(ctx, sql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		return res.Error()
	}

	if res.RowCount() < 1 {
		return moerr.NewInternalErrorf(ctx, "none cdc task for %d %s", accId, exec.spec.TaskId)
	} else if res.RowCount() > 1 {
		return moerr.NewInternalErrorf(ctx, "duplicate cdc task for %d %s", accId, exec.spec.TaskId)
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

		if err = cdc.JsonDecode(jsonSinkUri, &exec.sinkUri); err != nil {
			return err
		}

		//sink_password
		sinkPwd, err := res.GetString(ctx, 0, 2)
		if err != nil {
			return err
		}

		// TODO replace with creatorAccountId
		if err = exec.initAesKeyByInternalExecutor(ctx, catalog.System_Account); err != nil {
			return err
		}

		if exec.sinkUri.Password, err = cdc.AesCFBDecode(ctx, sinkPwd); err != nil {
			return err
		}
	}

	//update sink type after deserialize
	exec.sinkUri.SinkTyp = sinkTyp

	// tables
	jsonTables, err := res.GetString(ctx, 0, 3)
	if err != nil {
		return err
	}

	if err = cdc.JsonDecode(jsonTables, &exec.tables); err != nil {
		return err
	}

	// exclude
	exclude, err := res.GetString(ctx, 0, 4)
	if err != nil {
		return err
	}
	if exclude != "" {
		if exec.exclude, err = regexp.Compile(exclude); err != nil {
			return err
		}
	}

	// startTs
	startTs, err := res.GetString(ctx, 0, 5)
	if err != nil {
		return err
	}
	if exec.startTs, err = CDCStrToTS(startTs); err != nil {
		return err
	}
	// endTs
	endTs, err := res.GetString(ctx, 0, 6)
	if err != nil {
		return err
	}
	if exec.endTs, err = CDCStrToTS(endTs); err != nil {
		return err
	}

	// noFull
	noFull, err := res.GetString(ctx, 0, 7)
	if err != nil {
		return err
	}
	exec.noFull, _ = strconv.ParseBool(noFull)

	// additionalConfig
	additionalConfigStr, err := res.GetString(ctx, 0, 8)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(additionalConfigStr), &exec.additionalConfig)
}
