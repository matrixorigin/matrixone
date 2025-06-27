// Copyright 2024 Matrix Origin
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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"go.uber.org/zap"
)

var ErrSetAlreadyPersisted = moerr.NewInternalErrorNoCtx("set already persisted")
var ErrNoWatermarkFound = moerr.NewInternalErrorNoCtx("no watermark found")

const (
	WatermarkUpdateInterval          = time.Second * 3
	ReadWatermarkProjectionList      = "account_id, task_id, db_name, table_name, watermark"
	UpdateWatermarkCronJobNamePrefix = "CDCWatermarkUpdater-CronJob"
)

var cdcWatermarkUpdater atomic.Pointer[CDCWatermarkUpdater]

const (
	JT_CDC_GetOrAddCommittedWM tasks.JobType = 400 + iota
	JT_CDC_CommittingWM
	JT_CDC_UpdateWMErrMsg
	JT_CDC_RemoveCachedWM
)

func init() {
	tasks.RegisterJobType(JT_CDC_GetOrAddCommittedWM, "CDC_GetOrAddCommittedWM")
	tasks.RegisterJobType(JT_CDC_CommittingWM, "CDC_CommittingWM")
	tasks.RegisterJobType(JT_CDC_UpdateWMErrMsg, "CDC_UpdateWMErrMsg")
	tasks.RegisterJobType(JT_CDC_RemoveCachedWM, "CDC_RemoveCachedWM")
}

func GetCDCWatermarkUpdater(
	cnUUID string,
	executor ie.InternalExecutor,
) *CDCWatermarkUpdater {
	updater := cdcWatermarkUpdater.Load()
	for updater == nil {
		newUpdater := NewCDCWatermarkUpdater(
			fmt.Sprintf("cdc_watermark_updater_%s", cnUUID),
			executor,
		)
		newUpdater.Start()
		if cdcWatermarkUpdater.CompareAndSwap(nil, newUpdater) {
			updater = newUpdater
		} else {
			newUpdater.Stop()
			updater = cdcWatermarkUpdater.Load()
		}
	}
	return updater
}

type WatermarkKey struct {
	AccountId uint64
	TaskId    string
	DBName    string
	TableName string
}

func (k *WatermarkKey) String() string {
	return fmt.Sprintf("%d.%s.%s.%s", k.AccountId, k.TaskId, k.DBName, k.TableName)
}

type WatermarkResult struct {
	Watermark types.TS
	Ok        bool
}

type UpdaterJob struct {
	tasks.Job
	Key       *WatermarkKey
	Watermark types.TS
	ErrMsg    string
}

type UpdateOption func(*CDCWatermarkUpdater)

func WithExportStatsInterval(interval time.Duration) UpdateOption {
	return func(u *CDCWatermarkUpdater) {
		u.opts.exportStatsInterval = interval
	}
}

func WithCronJobErrorSupressTimes(times uint64) UpdateOption {
	return func(u *CDCWatermarkUpdater) {
		u.opts.cronJobErrorSupressTimes = times
	}
}

func WithCronJobInterval(interval time.Duration) UpdateOption {
	return func(u *CDCWatermarkUpdater) {
		u.opts.cronJobInterval = interval
	}
}

func WithCustomizedCronJob(fn func(ctx context.Context)) UpdateOption {
	return func(u *CDCWatermarkUpdater) {
		u.customized.cronJob = fn
	}
}

func WithCustomizedScheduleJob(fn func(job *UpdaterJob) (err error)) UpdateOption {
	return func(u *CDCWatermarkUpdater) {
		u.customized.scheduleJob = fn
	}
}

func NewGetOrAddCommittedWMJob(
	ctx context.Context,
	key *WatermarkKey,
	watermark *types.TS,
) *UpdaterJob {
	job := new(UpdaterJob)
	job.Init(
		ctx,
		uuid.Must(uuid.NewV7()).String(),
		JT_CDC_GetOrAddCommittedWM,
		nil,
	)
	job.Key = key
	job.Watermark = *watermark
	return job
}

func NewCommittingWMJob(
	ctx context.Context,
) *UpdaterJob {
	job := new(UpdaterJob)
	job.Init(
		ctx,
		uuid.Must(uuid.NewV7()).String(),
		JT_CDC_CommittingWM,
		nil,
	)
	return job
}

func NewUpdateWMErrMsgJob(
	ctx context.Context,
	key *WatermarkKey,
	errMsg string,
) *UpdaterJob {
	job := new(UpdaterJob)
	job.Init(
		ctx,
		uuid.Must(uuid.NewV7()).String(),
		JT_CDC_UpdateWMErrMsg,
		nil,
	)
	job.Key = key
	job.ErrMsg = errMsg
	return job
}

func NewRemoveCachedWMJob(
	ctx context.Context,
	key *WatermarkKey,
) *UpdaterJob {
	job := new(UpdaterJob)
	job.Init(
		ctx,
		uuid.Must(uuid.NewV7()).String(),
		JT_CDC_RemoveCachedWM,
		nil,
	)
	job.Key = key
	return job
}

type CDCWatermarkUpdater struct {
	sync.RWMutex

	opts struct {
		exportStatsInterval      time.Duration
		cronJobInterval          time.Duration
		cronJobErrorSupressTimes uint64
	}

	// sql executor
	ie ie.InternalExecutor
	// watermarkMap saves the watermark of each table
	cacheUncommitted map[WatermarkKey]types.TS
	cacheCommitting  map[WatermarkKey]types.TS
	cacheCommitted   map[WatermarkKey]types.TS

	queue        sm.Queue
	cronExecutor *tasks.CancelableJob

	customized struct {
		cronJob     func(ctx context.Context)
		scheduleJob func(job *UpdaterJob) (err error)
	}

	getOrAddCommittedBuffer []*UpdaterJob
	addCommittedBuffer      []*UpdaterJob
	committingBuffer        []*UpdaterJob
	committingErrMsgBuffer  []*UpdaterJob
	readKeysBuffer          map[WatermarkKey]WatermarkResult

	stats struct {
		runTimes       atomic.Uint64
		skipTimes      atomic.Uint64
		errorTimes     atomic.Uint64
		lastExportTime time.Time
	}
}

func NewCDCWatermarkUpdater(
	name string,
	ie ie.InternalExecutor,
	opts ...UpdateOption,
) *CDCWatermarkUpdater {
	u := &CDCWatermarkUpdater{
		ie:               ie,
		cacheUncommitted: make(map[WatermarkKey]types.TS),
		cacheCommitting:  make(map[WatermarkKey]types.TS),
		cacheCommitted:   make(map[WatermarkKey]types.TS),

		getOrAddCommittedBuffer: make([]*UpdaterJob, 0, 100),
		addCommittedBuffer:      make([]*UpdaterJob, 0, 100),
		committingBuffer:        make([]*UpdaterJob, 0, 100),
		readKeysBuffer:          make(map[WatermarkKey]WatermarkResult, 100),
	}
	for _, opt := range opts {
		opt(u)
	}
	u.fillDefaults()
	u.queue = sm.NewSafeQueue(5000, 200, u.onJobs)
	u.cronExecutor = tasks.NewCancelableCronJob(
		fmt.Sprintf("%s-%s", UpdateWatermarkCronJobNamePrefix, name),
		u.opts.cronJobInterval,
		u.wrapCronJob(u.customized.cronJob),
		true,
		1,
	)
	return u
}

func (u *CDCWatermarkUpdater) fillDefaults() {
	if u.opts.exportStatsInterval == 0 {
		u.opts.exportStatsInterval = time.Minute * 10
	}
	if u.opts.cronJobInterval == 0 {
		u.opts.cronJobInterval = WatermarkUpdateInterval
	}
	if u.customized.cronJob == nil {
		u.customized.cronJob = u.cronRun
	}
	if u.customized.scheduleJob == nil {
		u.customized.scheduleJob = u.scheduleJob
	}
	if u.opts.cronJobErrorSupressTimes == 0 {
		u.opts.cronJobErrorSupressTimes = 500
	}
}

func (u *CDCWatermarkUpdater) resetJobs(err error) {
	for i := range u.addCommittedBuffer {
		if err != nil && u.addCommittedBuffer[i] != nil {
			u.addCommittedBuffer[i].DoneWithErr(err)
		}
		u.addCommittedBuffer[i] = nil
	}
	u.addCommittedBuffer = u.addCommittedBuffer[:0]
	for i := range u.getOrAddCommittedBuffer {
		if err != nil && u.getOrAddCommittedBuffer[i] != nil {
			u.getOrAddCommittedBuffer[i].DoneWithErr(err)
		}
		u.getOrAddCommittedBuffer[i] = nil
	}
	u.getOrAddCommittedBuffer = u.getOrAddCommittedBuffer[:0]
	for i := range u.committingBuffer {
		if err != nil && u.committingBuffer[i] != nil {
			u.committingBuffer[i].DoneWithErr(err)
		}
		u.committingBuffer[i] = nil
	}
	u.committingBuffer = u.committingBuffer[:0]
	for i := range u.committingErrMsgBuffer {
		if err != nil && u.committingErrMsgBuffer[i] != nil {
			u.committingErrMsgBuffer[i].DoneWithErr(err)
		}
		u.committingErrMsgBuffer[i] = nil
	}
	u.committingErrMsgBuffer = u.committingErrMsgBuffer[:0]
	for key := range u.readKeysBuffer {
		delete(u.readKeysBuffer, key)
	}
}

func (u *CDCWatermarkUpdater) onJobs(jobs ...any) {
	var (
		err    error
		errMsg string
	)
	defer func() {
		u.resetJobs(err)
		if err != nil {
			logutil.Error(
				"CDC-Watermark-Read-Error",
				zap.Error(err),
				zap.String("err-msg", errMsg),
			)
		}
	}()

	for _, j := range jobs {
		job := j.(*UpdaterJob)
		switch job.Type() {
		case JT_CDC_GetOrAddCommittedWM:
			u.getOrAddCommittedBuffer = append(u.getOrAddCommittedBuffer, job)
			u.readKeysBuffer[*job.Key] = WatermarkResult{
				Watermark: types.TS{},
				Ok:        false,
			}
		case JT_CDC_CommittingWM:
			u.committingBuffer = append(u.committingBuffer, job)
		case JT_CDC_UpdateWMErrMsg:
			if _, err := u.GetFromCache(context.Background(), job.Key); err != nil {
				job.DoneWithErr(err)
				continue
			}
			u.committingErrMsgBuffer = append(u.committingErrMsgBuffer, job)
		case JT_CDC_RemoveCachedWM:
			u.Lock()
			if _, ok := u.cacheCommitted[*job.Key]; ok {
				delete(u.cacheCommitted, *job.Key)
				job.DoneWithErr(nil)
			}
			u.Unlock()
			logutil.Info(
				"CDC-Remove-Cached-WM-Success",
				zap.String("key", job.Key.String()),
			)
		default:
			logutil.Fatal("unknown job type", zap.Int("job-type", int(job.Type())))
		}
	}

	// read watermarks from the `mo_cdc_watermark` table
	// it collect all keys in the `getOrAddCommittedBuffer` and
	// read the watermarks from the `mo_cdc_watermark` table. if
	// the watermark is found, notify the job with the watermark, otherwise,
	// add the job to the `addCommittedBuffer`.
	if errMsg, err = u.execReadWM(); err != nil {
		return
	}

	// it collect all keys in the `addCommittedBuffer` and
	// add the watermarks records to the `mo_cdc_watermark` table.
	if errMsg, err = u.execAddWM(); err != nil {
		return
	}

	// batch update watermarks records in the `mo_cdc_watermark` table
	if errMsg, err = u.execBatchUpdateWM(); err != nil {
		return
	}
	errMsg, err = u.execBatchUpdateWMErrMsg()
}

func (u *CDCWatermarkUpdater) execReadWM() (errMsg string, err error) {
	if len(u.readKeysBuffer) == 0 {
		return "", nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	readSql := u.constructReadWMSQL(u.readKeysBuffer)
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	res := u.ie.Query(ctx, readSql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		err = res.Error()
		errMsg = fmt.Sprintf("read sql \"%s\" failed", readSql)
		return
	}

	var (
		key          WatermarkKey
		watermarkStr string
		watermark    types.TS
	)
	for i, rows := uint64(0), res.RowCount(); i < rows; i++ {
		if key.AccountId, err = res.GetUint64(ctx, i, 0); err != nil {
			errMsg = fmt.Sprintf("read sql \"%s\" bad account_id", readSql)
			return
		}
		if key.TaskId, err = res.GetString(ctx, i, 1); err != nil {
			errMsg = fmt.Sprintf("read sql \"%s\" bad task_id", readSql)
			return
		}
		if key.DBName, err = res.GetString(ctx, i, 2); err != nil {
			errMsg = fmt.Sprintf("read sql \"%s\" bad db_name", readSql)
			return
		}
		if key.TableName, err = res.GetString(ctx, i, 3); err != nil {
			errMsg = fmt.Sprintf("read sql \"%s\" bad tbl_name", readSql)
			return
		}
		if watermarkStr, err = res.GetString(ctx, i, 4); err != nil {
			errMsg = fmt.Sprintf("read sql \"%s\" bad watermark", readSql)
			return
		}
		watermark = types.StringToTS(watermarkStr)

		// update the readKeysBuffer
		u.readKeysBuffer[key] = WatermarkResult{
			Watermark: watermark,
			Ok:        true,
		}
	}

	// for each job in the getOrAddCommittedBuffer, if the watermark is found,
	// notify the job with the watermark, otherwise, add the job to the addCommittedBuffer
	// and clear the getOrAddCommittedBuffer
	// the jobs in the addCommittedBuffer will be processed in the `execAddWM`
	u.Lock()
	defer u.Unlock()
	for i, job := range u.getOrAddCommittedBuffer {
		if u.readKeysBuffer[*job.Key].Ok {
			u.cacheCommitted[*job.Key] = u.readKeysBuffer[*job.Key].Watermark
			job.DoneWithResult(u.readKeysBuffer[*job.Key].Watermark)
		} else {
			u.addCommittedBuffer = append(u.addCommittedBuffer, job)
		}
		u.getOrAddCommittedBuffer[i] = nil
	}
	u.getOrAddCommittedBuffer = u.getOrAddCommittedBuffer[:0]
	return
}

func (u *CDCWatermarkUpdater) execBatchUpdateWM() (errMsg string, err error) {
	if len(u.committingBuffer) == 0 {
		return "", nil
	}
	u.Lock()
	// no committing jobs and no uncommitted watermarks, skip
	if len(u.committingBuffer)+len(u.cacheUncommitted) == 0 {
		u.Unlock()
		return "", nil
	}
	// move uncommitted watermarks to committing
	for key, watermark := range u.cacheUncommitted {
		u.cacheCommitting[key] = watermark
	}
	// clear uncommitted watermarks
	for key := range u.cacheUncommitted {
		delete(u.cacheUncommitted, key)
	}
	commitSql := u.constructBatchUpdateWMSQL(u.cacheCommitting)
	u.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	err = u.ie.Exec(ctx, commitSql, ie.SessionOverrideOptions{})
	u.Lock()
	defer u.Unlock()

	if err != nil {
		errMsg = fmt.Sprintf("commit sql \"%s\" failed", commitSql)
	} else {
		// commit watermarks from committing to committed
		for key, watermark := range u.cacheCommitting {
			u.cacheCommitted[key] = watermark
		}
	}

	// notify committing jobs that the watermarks are committed and
	// clear the committing buffer
	for i, job := range u.committingBuffer {
		job.DoneWithErr(err)
		u.committingBuffer[i] = nil
	}
	u.committingBuffer = u.committingBuffer[:0]

	// clear the committing cache
	for key := range u.cacheCommitting {
		delete(u.cacheCommitting, key)
	}
	return
}

func (u *CDCWatermarkUpdater) execBatchUpdateWMErrMsg() (errMsg string, err error) {
	if len(u.committingErrMsgBuffer) == 0 {
		return "", nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	errMsgSql := u.constructBatchUpdateWMErrMsgSQL(u.committingErrMsgBuffer)
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	err = u.ie.Exec(ctx, errMsgSql, ie.SessionOverrideOptions{})
	if err != nil {
		errMsg = fmt.Sprintf("update err_msg sql \"%s\" failed", errMsgSql)
	}
	u.Lock()
	defer u.Unlock()
	for i, job := range u.committingErrMsgBuffer {
		job.DoneWithErr(err)
		u.committingErrMsgBuffer[i] = nil
	}
	u.committingErrMsgBuffer = u.committingErrMsgBuffer[:0]
	return
}

func (u *CDCWatermarkUpdater) constructBatchUpdateWMSQL(
	keys map[WatermarkKey]types.TS,
) (commitSql string) {
	var values string
	i := 0
	for key, wm := range keys {
		if i > 0 {
			values += ","
		}
		values += fmt.Sprintf(
			"(%d, '%s', '%s', '%s', '%s')",
			key.AccountId,
			key.TaskId,
			key.DBName,
			key.TableName,
			wm.ToString(),
		)
		i++
	}
	commitSql = CDCSQLBuilder.OnDuplicateUpdateWatermarkSQL(values)
	return
}

func (u *CDCWatermarkUpdater) constructBatchUpdateWMErrMsgSQL(
	jobs []*UpdaterJob,
) (commitSql string) {
	var values string
	for i, job := range jobs {
		if i > 0 {
			values += ","
		}
		values += fmt.Sprintf(
			"(%d, '%s', '%s', '%s', '%s')",
			job.Key.AccountId,
			job.Key.TaskId,
			job.Key.DBName,
			job.Key.TableName,
			job.ErrMsg, // only update the err_msg
		)
	}
	commitSql = CDCSQLBuilder.OnDuplicateUpdateWatermarkErrMsgSQL(values)
	return
}

func (u *CDCWatermarkUpdater) execAddWM() (errMsg string, err error) {
	if len(u.addCommittedBuffer) == 0 {
		return "", nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	addSql := u.constructAddWMSQL(u.addCommittedBuffer)
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	err = u.ie.Exec(ctx, addSql, ie.SessionOverrideOptions{})
	if err != nil {
		errMsg = fmt.Sprintf("add sql \"%s\" failed", addSql)
	}
	u.Lock()
	defer u.Unlock()
	for i, job := range u.addCommittedBuffer {
		// add the watermark to the cacheCommitted
		u.cacheCommitted[*job.Key] = job.Watermark
		// notify the job with the watermark
		job.DoneWithResult(job.Watermark)
		// clear the addCommittedBuffer
		u.addCommittedBuffer[i] = nil
	}
	// clear the addCommittedBuffer
	u.addCommittedBuffer = u.addCommittedBuffer[:0]
	return
}

func (u *CDCWatermarkUpdater) constructAddWMSQL(
	jobs []*UpdaterJob,
) (addSql string) {
	var values string
	for i, job := range jobs {
		if i > 0 {
			values += ","
		}
		values += fmt.Sprintf(
			"(%d, '%s', '%s', '%s', '%s', '%s')",
			job.Key.AccountId,
			job.Key.TaskId,
			job.Key.DBName,
			job.Key.TableName,
			job.Watermark.ToString(),
			"",
		)
	}
	addSql = CDCSQLBuilder.InsertWatermarkWithValuesSQL(values)
	return
}

func (u *CDCWatermarkUpdater) constructReadWMSQL(
	keys map[WatermarkKey]WatermarkResult,
) (readSql string) {
	var (
		idx       int
		filterStr string
	)
	// "(xxx AND yyy) OR (xxx AND yyy)"
	for key := range keys {
		if idx > 0 {
			filterStr += " OR "
		}
		filterStr += fmt.Sprintf(
			"(account_id = %d AND task_id = '%s' AND db_name = '%s' AND table_name = '%s')",
			key.AccountId,
			key.TaskId,
			key.DBName,
			key.TableName,
		)
		idx++
	}
	readSql = CDCSQLBuilder.GetWatermarkWhereSQL(ReadWatermarkProjectionList, filterStr)
	return
}

func (u *CDCWatermarkUpdater) Start() {
	u.queue.Start()
	u.cronExecutor.Start()
}

func (u *CDCWatermarkUpdater) Stop() {
	u.cronExecutor.Stop()
	u.queue.Stop()
}

func (u *CDCWatermarkUpdater) getFromCache(
	key *WatermarkKey,
) (watermark types.TS, ok bool) {
	u.RLock()
	defer u.RUnlock()
	if watermark, ok = u.cacheUncommitted[*key]; ok {
		return
	}
	if watermark, ok = u.cacheCommitting[*key]; ok {
		return
	}
	watermark, ok = u.cacheCommitted[*key]
	return
}

func (u *CDCWatermarkUpdater) GetFromCache(
	ctx context.Context,
	key *WatermarkKey,
) (watermark types.TS, err error) {
	var ok bool
	if watermark, ok = u.getFromCache(key); ok {
		return
	}
	err = ErrNoWatermarkFound
	return
}

func (u *CDCWatermarkUpdater) UpdateWatermarkErrMsg(
	ctx context.Context,
	key *WatermarkKey,
	errMsg string,
) (err error) {
	job := NewUpdateWMErrMsgJob(ctx, key, errMsg)
	if _, err = u.queue.Enqueue(job); err != nil {
		return
	}
	job.WaitDone()
	err = job.GetResult().Err
	return
}

func (u *CDCWatermarkUpdater) UpdateWatermarkOnly(
	ctx context.Context,
	key *WatermarkKey,
	watermark *types.TS,
) (err error) {
	u.Lock()
	defer u.Unlock()
	u.cacheUncommitted[*key] = *watermark
	return nil
}

func (u *CDCWatermarkUpdater) RemoveCachedWM(
	ctx context.Context,
	key *WatermarkKey,
) (err error) {
	if err = u.ForceFlush(ctx); err != nil {
		logutil.Error(
			"CDCWatermarkUpdater-RemoveCachedWM-ForceFlushFailed",
			zap.String("key", key.String()),
			zap.Error(err),
		)
		return
	}
	job := NewRemoveCachedWMJob(ctx, key)
	if _, err = u.queue.Enqueue(job); err != nil {
		return
	}
	job.WaitDone()
	err = job.GetResult().Err
	return
}

func (u *CDCWatermarkUpdater) ForceFlush(ctx context.Context) (err error) {
	job := NewCommittingWMJob(ctx)
	if err = u.customized.scheduleJob(job); err != nil {
		return
	}
	job.WaitDone()
	err = job.GetResult().Err
	return
}

// Note: suppose there is no concurrent write to the same key
func (u *CDCWatermarkUpdater) GetOrAddCommitted(
	ctx context.Context,
	key *WatermarkKey,
	watermark *types.TS,
) (ret types.TS, err error) {
	u.RLock()
	persisted, ok := u.cacheCommitted[*key]
	u.RUnlock()
	if ok {
		if persisted.GE(watermark) {
			ret = persisted
			return
		}
	}

	job := NewGetOrAddCommittedWMJob(ctx, key, watermark)
	if _, err = u.queue.Enqueue(job); err != nil {
		return
	}
	job.WaitDone()
	res := job.GetResult()
	if res.Err != nil {
		err = res.Err
	} else {
		ret = res.Res.(types.TS)
	}
	return
}

// cron job to move the watermark from uncommitted to
// committing
func (u *CDCWatermarkUpdater) wrapCronJob(job func(ctx context.Context)) func(ctx context.Context) {
	return func(ctx context.Context) {
		if time.Since(u.stats.lastExportTime) > u.opts.exportStatsInterval {
			u.stats.lastExportTime = time.Now()
			logutil.Info(
				"CDCWatermarkUpdater-Stats",
				zap.Uint64("run-times", u.stats.runTimes.Load()),
				zap.Uint64("skip-times", u.stats.skipTimes.Load()),
			)
		}
		u.stats.runTimes.Add(1)
		job(ctx)
	}
}

func (u *CDCWatermarkUpdater) scheduleJob(job *UpdaterJob) (err error) {
	if _, err = u.queue.Enqueue(job); err != nil {
		job.DoneWithErr(err)
		return
	}
	return
}

func (u *CDCWatermarkUpdater) cronRun(ctx context.Context) {
	u.Lock()
	// if there is any watermark in committing, skip the current run
	if len(u.cacheCommitting) > 0 || len(u.cacheUncommitted) == 0 {
		u.stats.skipTimes.Add(1)
		u.Unlock()
		return
	}
	// move all watermarks from uncommitted to committing
	for key, watermark := range u.cacheUncommitted {
		u.cacheCommitting[key] = watermark
		delete(u.cacheUncommitted, key)
	}
	u.Unlock()

	var err error
	defer func() {
		if err != nil {
			u.stats.errorTimes.Add(1)
			times := u.stats.errorTimes.Load()
			if times%u.opts.cronJobErrorSupressTimes == 0 {
				logutil.Error(
					"CDCWatermarkUpdater-Error",
					zap.Error(err),
					zap.Uint64("error-times", times),
				)
			}
		}
	}()

	err = u.ForceFlush(ctx)
}
