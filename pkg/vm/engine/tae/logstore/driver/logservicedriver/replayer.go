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

package logservicedriver

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"go.uber.org/zap"
)

type ReadState int32

const (
	ReadState_InLoop = iota
	ReadState_InLoopDone
	ReadState_AllDone
)

const (
	DefaultPollTruncateInterval = time.Second * 5
)

type ReplayOption func(*replayer)

func WithReplayerWaitMore(wait func() bool) ReplayOption {
	return func(r *replayer) {
		r.waitMoreRecords = wait
	}
}

func WithReplayerOnWriteSkip(f func(map[uint64]uint64)) ReplayOption {
	return func(r *replayer) {
		if r.onWriteSkip != nil {
			r.onWriteSkip = func(skipMap map[uint64]uint64) {
				f(skipMap)
				r.onWriteSkip(skipMap)
			}
		} else {
			r.onWriteSkip = f
		}
	}
}

func WithReplayerNeedWriteSkip(f func() bool) ReplayOption {
	return func(r *replayer) {
		r.needWriteSkip = f
	}
}

func WithReplayerOnLogRecord(f func(logservice.LogRecord)) ReplayOption {
	return func(r *replayer) {
		if r.onLogRecord != nil {
			r.onLogRecord = func(record logservice.LogRecord) {
				f(record)
				r.onLogRecord(record)
			}
		} else {
			r.onLogRecord = f
		}
	}
}

func WithReplayerOnUserLogEntry(f func(uint64, LogEntry)) ReplayOption {
	return func(r *replayer) {
		if r.onUserLogEntry != nil {
			r.onUserLogEntry = func(psn uint64, entry LogEntry) {
				f(psn, entry)
				r.onUserLogEntry(psn, entry)
			}
		} else {
			r.onUserLogEntry = f
		}
	}
}

func WithReplayerOnReplayDone(f func(error, DSNStats)) ReplayOption {
	return func(r *replayer) {
		if r.onReplayDone != nil {
			r.onReplayDone = func(resErr error, stats DSNStats) {
				f(resErr, stats)
				r.onReplayDone(resErr, stats)
			}
		} else {
			r.onReplayDone = f
		}
	}
}

func WithReplayerOnTruncate(f func(uint64)) ReplayOption {
	return func(r *replayer) {
		if r.onTruncate != nil {
			r.onTruncate = func(psn uint64) {
				f(psn)
				r.onTruncate(psn)
			}
		} else {
			r.onTruncate = f
		}
	}
}

func WithReplayerOnScheduled(f func(uint64, LogEntry)) ReplayOption {
	return func(r *replayer) {
		if r.onScheduled != nil {
			r.onScheduled = func(psn uint64, entry LogEntry) {
				f(psn, entry)
				r.onScheduled(psn, entry)
			}
		} else {
			r.onScheduled = f
		}
	}
}

func WithReplayerOnApplied(f func(*entry.Entry)) ReplayOption {
	return func(r *replayer) {
		if r.onApplied != nil {
			r.onApplied = func(e *entry.Entry) {
				f(e)
				r.onApplied(e)
			}
		} else {
			r.onApplied = f
		}
	}
}

func WithReplayerPollTruncateInterval(interval time.Duration) ReplayOption {
	return func(r *replayer) {
		r.pollTruncateInterval = interval
	}
}

func WithReplayerUnmarshalLogRecord(f func(logservice.LogRecord) LogEntry) ReplayOption {
	return func(r *replayer) {
		r.logRecordToLogEntry = f
	}
}

func WithReplayerAppendSkipCmd(f func(ctx context.Context, skipMap map[uint64]uint64) error) ReplayOption {
	return func(r *replayer) {
		r.appendSkipCmd = f
	}
}

type replayerDriver interface {
	readFromBackend(
		ctx context.Context, firstPSN uint64, maxSize int,
	) (nextPSN uint64, records []logservice.LogRecord, err error)
	getTruncatedPSNFromBackend(ctx context.Context) (uint64, error)
	getClientForWrite() (*wrappedClient, error)
	GetMaxClient() int
}

type replayer struct {
	readBatchSize        int
	pollTruncateInterval time.Duration

	driver replayerDriver
	handle driver.ApplyHandle

	logRecordToLogEntry func(logservice.LogRecord) LogEntry
	appendSkipCmd       func(ctx context.Context, skipMap map[uint64]uint64) error
	onLogRecord         func(logservice.LogRecord)
	onUserLogEntry      func(uint64, LogEntry)
	onScheduled         func(uint64, LogEntry)
	onApplied           func(*entry.Entry)

	needWriteSkip func() bool
	onWriteSkip   func(map[uint64]uint64)
	onTruncate    func(uint64)
	onReplayDone  func(resErr error, stats DSNStats)

	// true means wait for more records after all existing records have been read
	waitMoreRecords func() bool

	replayedState struct {
		// DSN->PSN mapping
		dsn2PSNMap map[uint64]uint64
		readCache  readCache

		// the DSN is monotonically continuously increasing and the corresponding
		// PSN may not be monotonically increasing due to the concurrent write.
		// So when writing a record, we logs the visible safe DSN in the record,
		// which is the maximum DSN that has been safely written to the backend
		// continuously without any gaps. It means that all records with DSN <=
		// safeDSN have been safely written to the backend. When replaying, we
		// can make sure that all records with DSN <= safeDSN have been replayed
		safeDSN uint64

		firstAppliedDSN uint64
		firstAppliedLSN uint64

		lastAppliedDSN uint64
		lastAppliedLSN uint64
	}

	waterMarks struct {
		// psn to read for the next batch
		psnToRead uint64

		truncatedPSN uint64

		// the DSN watermark has been scheduled for apply
		dsnScheduled uint64

		minDSN uint64
		maxDSN uint64
	}

	stats struct {
		applyDuration time.Duration
		readDuration  time.Duration

		appliedLSNCount  atomic.Int64
		readPSNCount     int
		readDSNCount     int
		schedulePSNCount int
		scheduleLSNCount int
	}
}

func newReplayer(
	handle driver.ApplyHandle,
	driver replayerDriver,
	readBatchSize int,
	opts ...ReplayOption,
) *replayer {
	r := &replayer{
		handle:        handle,
		driver:        driver,
		readBatchSize: readBatchSize,
	}
	r.replayedState.readCache = newReadCache()
	r.replayedState.dsn2PSNMap = make(map[uint64]uint64)
	r.waterMarks.dsnScheduled = math.MaxUint64
	r.waterMarks.minDSN = math.MaxUint64
	for _, opt := range opts {
		opt(r)
	}

	if r.pollTruncateInterval <= 0 {
		r.pollTruncateInterval = DefaultPollTruncateInterval
	}

	if r.logRecordToLogEntry == nil {
		r.logRecordToLogEntry = func(record logservice.LogRecord) LogEntry {
			e, err := DecodeLogEntry(record.Payload(), nil)
			if err != nil {
				logutil.Fatal(
					"Wal-Replay",
					zap.Error(err),
					zap.Uint64("psn", record.Lsn),
					zap.Any("type", record.Type),
				)
			}
			return e
		}
	}

	if r.appendSkipCmd == nil {
		r.appendSkipCmd = r.AppendSkipCmd
	}

	if r.waitMoreRecords == nil {
		r.waitMoreRecords = func() bool {
			return false
		}
	}

	return r
}

func (r *replayer) ExportDSNStats() DSNStats {
	return DSNStats{
		Min:       r.waterMarks.minDSN,
		Max:       r.waterMarks.maxDSN,
		Truncated: r.waterMarks.truncatedPSN,
	}
}

func (r *replayer) exportFields(level int) []zap.Field {
	ret := []zap.Field{
		zap.Duration("read-duration", r.stats.readDuration),
		zap.Int("read-psn-count", r.stats.readPSNCount),
		zap.Int("read-dsn-count", r.stats.readDSNCount),
		zap.Int64("apply-lsn-count", r.stats.appliedLSNCount.Load()),
		zap.Int("schedule-psn-count", r.stats.schedulePSNCount),
		zap.Int("schedule-lsn-count", r.stats.scheduleLSNCount),
		zap.Duration("apply-duration", r.stats.applyDuration),
		zap.Uint64("first-apply-dsn", r.replayedState.firstAppliedDSN),
		zap.Uint64("first-apply-lsn", r.replayedState.firstAppliedLSN),
		zap.Uint64("last-apply-dsn", r.replayedState.lastAppliedDSN),
		zap.Uint64("last-apply-lsn", r.replayedState.lastAppliedLSN),
	}
	if level > 0 {
		ret = append(ret,
			zap.Uint64("safe-dsn", r.replayedState.safeDSN),
			zap.Uint64("min-dsn", r.waterMarks.minDSN),
			zap.Uint64("max-dsn", r.waterMarks.maxDSN),
			zap.Uint64("dsn-scheduled", r.waterMarks.dsnScheduled),
		)
	}
	if level > 1 {
		ret = append(ret,
			zap.Any("dsn-psn-map", r.replayedState.dsn2PSNMap),
		)
	}
	return ret
}

func (r *replayer) initReadWatermarks(ctx context.Context) (err error) {
	var psn uint64
	if psn, err = r.driver.getTruncatedPSNFromBackend(ctx); err != nil {
		return
	}
	r.waterMarks.truncatedPSN = psn
	r.waterMarks.psnToRead = psn + 1
	return
}

func (r *replayer) Replay(
	ctx context.Context,
) (err error) {
	var (
		readDone      bool
		resultC       = make(chan error, 1)
		applyC        = make(chan *entry.Entry, 20)
		lastScheduled *entry.Entry
		errMsg        string
	)
	defer func() {
		fields := r.exportFields(0)
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
			fields = append(fields, zap.String("err-msg", errMsg))
			fields = append(fields, zap.Error(err))
		}
		logger(
			"Wal-Replay-Info",
			fields...,
		)
		if r.onReplayDone != nil {
			r.onReplayDone(err, r.ExportDSNStats())
		}
	}()

	// init the read watermarks to use the next sequnce number of the
	// truncated PSN as the start PSN to read
	if err = r.initReadWatermarks(ctx); err != nil {
		return
	}

	var (
		wg       sync.WaitGroup
		waitTime = time.Millisecond
	)

	wg.Add(2)
	// a dedicated goroutine to replay entries from the applyC
	go r.streamApplying(ctx, applyC, resultC, &wg)
	ctx2, cancel := context.WithCancel(ctx)
	go r.pollTruncateLoop(ctx2, &wg)

	defer func() {
		cancel()
		wg.Wait()
	}()

	// read log records batch by batch and schedule the records for apply
	for {
		select {
		case <-ctx.Done():
			err = context.Cause(ctx)
			close(applyC)
			logutil.Error(
				"Wal-Replay-Context-Done",
				zap.Error(err),
			)
			return
		default:
		}

		if readDone, err = r.readNextBatch(ctx); err != nil || (readDone && !r.waitMoreRecords()) {
			break
		}

		if readDone && r.waitMoreRecords() {
			for err == nil || err != ErrAllRecordsRead {
				lastScheduled, err = r.tryScheduleApply(
					ctx, applyC, lastScheduled, ReadState_InLoopDone,
				)
			}
			if err == ErrAllRecordsRead {
				err = nil
			}
			if err != nil {
				errMsg = fmt.Sprintf("read loop schedule apply error: %v", err)
				close(applyC)
				return
			}
			time.Sleep(waitTime)
			waitTime *= 2
			if waitTime > time.Millisecond*128 {
				waitTime = time.Millisecond * 128
			}
			continue
		}
		waitTime = time.Millisecond

		// when the schedule DSN is less than the safe DSN, we try to
		// schedule some entries for apply
		for r.waterMarks.dsnScheduled < r.replayedState.safeDSN {
			if lastScheduled, err = r.tryScheduleApply(
				ctx, applyC, lastScheduled, ReadState_InLoop,
			); err != nil {
				break
			}
		}

		if err != nil {
			break
		}
	}

	if err != nil {
		errMsg = fmt.Sprintf("read and schedule error in loop: %v", err)
		close(applyC)
		return
	}

	for err == nil || err != ErrAllRecordsRead {
		lastScheduled, err = r.tryScheduleApply(
			ctx, applyC, lastScheduled, ReadState_AllDone,
		)
	}

	if err == ErrAllRecordsRead {
		err = nil
	}

	if err != nil {
		errMsg = fmt.Sprintf("schedule apply error: %v", err)
		close(applyC)
		return
	}

	r.replayedState.readCache.clear()

	if lastScheduled != nil {
		lastScheduled.WaitDone()
	}

	close(applyC)

	// wait for the replay to finish
	if applyErr := <-resultC; applyErr != nil {
		err = applyErr
		errMsg = fmt.Sprintf("apply error: %v", applyErr)
	}

	return
}

func (r *replayer) tryScheduleApply(
	ctx context.Context,
	applyC chan<- *entry.Entry,
	lastScheduled *entry.Entry,
	readState ReadState,
) (scheduled *entry.Entry, err error) {
	scheduled = lastScheduled

	// readCache isEmpty means all readed records have been scheduled for apply
	if r.replayedState.readCache.isEmpty() {
		err = ErrAllRecordsRead
		return
	}

	dsn := r.waterMarks.dsnScheduled + 1
	psn, ok := r.replayedState.dsn2PSNMap[dsn]
	// logutil.Infof("DEBUG-1: dsn %d, psn %d, ok %v, %v", dsn, psn, ok, r.waterMarks.dsnScheduled)

	// Senario 1 [dsn not found]:
	// dsn is not found in the dsn2PSNMap, which means the record
	// with the dsn has not been read from the backend
	if !ok {
		appliedLSNCount := r.stats.appliedLSNCount.Load()
		if appliedLSNCount == 0 && lastScheduled != nil {
			lastScheduled.WaitDone()
			appliedLSNCount = r.stats.appliedLSNCount.Load()
		}

		if dsn <= r.replayedState.safeDSN {
			// [dsn not found && dsn <= safeDSN]:
			// the record should already be read from the backend

			if appliedLSNCount == 0 {
				//[dsn not found && dsn <= safeDSN && appliedLSNCount == 0]
				// there is no record applied or scheduled for apply
				// maybe there are some old non-contiguous records
				// Example:
				// PSN: 10,     11,     12,     13,     14,     15      16
				// DSN: [26,27],[24,25],[30,31],[28,29],[32,33],[36,37],[34,35]
				// Truncate: want to truncate DSN 32 and it will truncate PSN 13, remmaining: PSN 14, 15, 16
				// PSN: 14,     15,     16
				// DSN: [32,33],[36,37],[34,35]

				logutil.Info(
					"Wal-Replay-Skip-Entry",
					zap.Uint64("dsn", dsn),
					zap.Uint64("safe-dsn", r.replayedState.safeDSN),
				)
				r.waterMarks.minDSN = dsn + 1
				r.waterMarks.dsnScheduled = dsn
				return
			} else {
				// [dsn not found && dsn <= safeDSN && appliedLSNCount > 0]
				err = moerr.NewInternalErrorNoCtxf(
					"dsn %d not found", dsn,
				)
				logutil.Error(
					"Wal-Schedule-Apply-Error",
					zap.Error(err),
					zap.Uint64("dsn", dsn),
					zap.Uint64("safe-dsn", r.replayedState.safeDSN),
					zap.Any("dsn-psn", r.replayedState.dsn2PSNMap),
				)
				return
			}
		} else {
			// [dsn not found && dsn > safeDSN && allReaded]
			// it can only get in here when allReaded. it means even all records have been read,
			// there are some big DSNs not found in the dsn2PSNMap.
			// Truncated: PSN 11
			// PSN: 10,     11,     12,     13,     14,	    15
			// DSN: [37,37],[35,35],[40,40],[36,36],[39,39],[38,38]
			// For DSN 36, it will get in here after all records have been read

			logutil.Debug(
				"Wal-Replay-Info",
				zap.Uint64("dsn", dsn),
				zap.Uint64("safe-dsn", r.replayedState.safeDSN),
				zap.Any("dsn-psn", r.replayedState.dsn2PSNMap),
				zap.Uint64("dsn-scheduled", r.waterMarks.dsnScheduled),
			)

			if readState == ReadState_InLoop {
				panic(fmt.Sprintf("logic error, safe dsn %d, dsn %d", r.replayedState.safeDSN, dsn))
			} else if readState == ReadState_InLoopDone {
				err = ErrAllRecordsRead
				return
			}

			if appliedLSNCount == 0 {
				logutil.Info(
					"Wal-Replay-Skip-Entry",
					zap.Uint64("dsn", dsn),
					zap.Uint64("safe-dsn", r.replayedState.safeDSN),
				)
				r.waterMarks.minDSN = dsn + 1
				r.waterMarks.dsnScheduled = dsn
				if len(r.replayedState.dsn2PSNMap) == 0 {
					err = ErrAllRecordsRead
					return
				}
				return
			}

			// [dsn not found && dsn > safeDSN]
			if len(r.replayedState.dsn2PSNMap) != 0 && (r.needWriteSkip == nil || r.needWriteSkip()) {
				if r.onWriteSkip != nil {
					r.onWriteSkip(r.replayedState.dsn2PSNMap)
				}
				if err = r.appendSkipCmd(
					ctx, r.replayedState.dsn2PSNMap,
				); err != nil {
					return
				}
			}
			err = ErrAllRecordsRead
			return
		}
	}

	if lastScheduled == nil {
		logutil.Info(
			"Wal-Replay-First-Entry",
			zap.Uint64("dsn", dsn),
			zap.Uint64("psn", psn),
			zap.Any("read-state", readState),
		)
	}

	var record LogEntry
	if record, err = r.replayedState.readCache.getRecord(psn); err != nil {
		return
	}

	scheduleApply := func(e *entry.Entry) {
		scheduled = e
		applyC <- e
	}

	if err = record.ForEachEntry(scheduleApply); err != nil {
		return
	}

	dsnRange := record.DSNRange()
	r.updateDSN(dsnRange.End)
	r.updateDSN(dsnRange.Start)
	r.waterMarks.dsnScheduled = dsnRange.End
	r.stats.schedulePSNCount++

	r.replayedState.readCache.removeRecord(psn)
	// dsn2PSNMap is produced by the readNextBatch and consumed if it is scheduled apply
	delete(r.replayedState.dsn2PSNMap, dsn)

	if r.onScheduled != nil {
		r.onScheduled(psn, record)
	}

	return
}

// updateDSN updates the minDSN and maxDSN
func (r *replayer) updateDSN(dsn uint64) {
	if dsn == 0 {
		return
	}
	if dsn < r.waterMarks.minDSN {
		r.waterMarks.minDSN = dsn
	}
	if dsn > r.waterMarks.maxDSN {
		r.waterMarks.maxDSN = dsn
	}
}

func (r *replayer) pollTruncateLoop(
	ctx context.Context,
	wg *sync.WaitGroup,
) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			logutil.Error(
				"Wal-Replay-Truncate-Loop",
				zap.Error(err),
			)
		}
		wg.Done()
	}()
	ticker := time.NewTicker(r.pollTruncateInterval)
	for {
		select {
		case <-ctx.Done():
			err = context.Cause(ctx)
			return
		case <-ticker.C:
			var psn uint64
			if psn, err = r.driver.getTruncatedPSNFromBackend(ctx); err != nil {
				return
			}
			r.waterMarks.truncatedPSN = psn
			if r.onTruncate != nil {
				r.onTruncate(psn)
			}
		}
	}
}

func (r *replayer) streamApplying(
	ctx context.Context,
	sourceC <-chan *entry.Entry,
	resultC chan error,
	wg *sync.WaitGroup,
) (err error) {
	var (
		e  *entry.Entry
		ok bool
	)
	defer func() {
		resultC <- err
		wg.Done()
	}()
	for {
		select {
		case <-ctx.Done():
			err = context.Cause(ctx)
			return
		case e, ok = <-sourceC:
			if !ok {
				return
			}
			r.stats.scheduleLSNCount++

			t0 := time.Now()

			// state == driver.RE_Nomal means the entry is applied successfully
			// here log the first applied entry
			if state := r.handle(e); state == driver.RE_Nomal {
				_, lsn := e.Entry.GetLsn()
				dsn := e.DSN
				if r.replayedState.lastAppliedDSN < dsn {
					r.replayedState.lastAppliedDSN = dsn
					r.replayedState.lastAppliedLSN = lsn
				}

				if r.stats.appliedLSNCount.Load() == 0 {
					r.replayedState.firstAppliedDSN = dsn
					r.replayedState.firstAppliedLSN = lsn
				}
				r.stats.appliedLSNCount.Add(1)
				if r.onApplied != nil {
					r.onApplied(e)
				}
			}
			e.DoneWithErr(nil)
			e.Entry.Free()

			r.stats.applyDuration += time.Since(t0)
		}
	}
}

// this function reads the next batch of records from the backend
func (r *replayer) readNextBatch(
	ctx context.Context,
) (done bool, err error) {
	t0 := time.Now()
	nextPSN, records, err := r.driver.readFromBackend(
		ctx, r.waterMarks.psnToRead, r.readBatchSize,
	)
	if err != nil {
		return
	}

	logutil.Debug(
		"Wal-Debug-Read",
		zap.Uint64("psn-to-read", r.waterMarks.psnToRead),
		zap.Uint64("next-to-read", nextPSN),
		zap.Int("readed-cnt", len(records)),
	)

	r.stats.readDuration += time.Since(t0)
	for i, record := range records {
		if r.onLogRecord != nil {
			r.onLogRecord(record)
		}
		// skip non-user records
		if record.GetType() != pb.UserRecord {
			continue
		}
		psn := r.waterMarks.psnToRead + uint64(i)
		entry := r.logRecordToLogEntry(record)
		if updated := r.replayedState.readCache.addRecord(
			psn, entry,
		); updated {
			if r.onUserLogEntry != nil {
				r.onUserLogEntry(psn, entry)
			}

			logutil.Debug(
				"Wal-Trace-Read",
				zap.Uint64("psn", psn),
				zap.Uint64("safe-dsn", entry.GetSafeDSN()),
				zap.Uint64("start-dsn", entry.GetStartDSN()),
				zap.Int("entry-count", int(entry.GetEntryCount())),
			)

			// 1. update the safe DSN
			if r.replayedState.safeDSN < entry.GetSafeDSN() {
				r.replayedState.safeDSN = entry.GetSafeDSN()
			}

			// 2. update the stats
			r.stats.readPSNCount++
			r.stats.readDSNCount += int(entry.GetEntryCount())

			// 3. remove the skipped records if the entry is a skip entry
			if entry.GetCmdType() == uint16(Cmd_SkipDSN) {
				cmd := SkipCmd(entry.GetEntry(0))
				skipDSNs := cmd.GetDSNSlice()
				logutil.Info(
					"Wal-Read-Skip-Entry",
					zap.Any("skip-dsns", skipDSNs),
					zap.Any("skip-psns", cmd.GetPSNSlice()),
					zap.Uint64("psn", psn),
					zap.Uint64("safe-dsn", entry.GetSafeDSN()),
					// zap.Any("dsn-psn", r.replayedState.dsn2PSNMap),
				)

				for _, dsn := range skipDSNs {
					if _, ok := r.replayedState.dsn2PSNMap[dsn]; !ok {
						panic(fmt.Sprintf("dsn %d not found in the dsn2PSNMap", dsn))
					}
					delete(r.replayedState.dsn2PSNMap, dsn)
				}

				continue
			}

			// 4. update the DSN->PSN mapping
			dsn := entry.GetStartDSN()
			r.replayedState.dsn2PSNMap[dsn] = psn
			safe := entry.GetSafeDSN()

			// 5. init the scheduled DSN watermark
			// it only happens there is no record scheduled for apply
			if dsn-1 < r.waterMarks.dsnScheduled {
				if r.stats.schedulePSNCount != 0 {
					// it means a bigger DSN has been scheduled for apply and then there is
					// a smaller DSN with bigger PSN. it should not happen
					logutil.Errorf(
						"safe: %d, dsn: %d, psn: %d, scheduled: %d",
						safe, dsn, psn, r.waterMarks.dsnScheduled,
					)
					panic("logic error")
				}
				// logutil.Infof("DEBUG-3: dsn %d, psn %d, scheduled %d, safe %d", dsn, psn, r.waterMarks.dsnScheduled, safe)
				if safe == 0 {
					r.waterMarks.dsnScheduled = 0
				} else {
					r.waterMarks.dsnScheduled = safe - 1
				}
			}
		}
	}

	if nextPSN <= r.waterMarks.psnToRead {
		done = true
	} else {
		r.waterMarks.psnToRead = nextPSN
	}
	return
}

// PXU TODO: make sure there is no concurrent write
func (r *replayer) AppendSkipCmd(
	ctx context.Context,
	skipMap map[uint64]uint64,
) (err error) {
	var (
		now = time.Now()
	)
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"Wal-Replay-Append-Skip-Entries",
			zap.Any("gsn-psn-map", skipMap),
			zap.Duration("duration", time.Since(now)),
			zap.Error(err),
		)
	}()

	// the skip map should not be larger than the max client count
	// FIXME: the ClientMaxCount is configurable and it may be different
	// from the last time it writes logs
	if len(skipMap) > r.driver.GetMaxClient() {
		err = moerr.NewInternalErrorNoCtxf(
			"too many skip entries, count %d", len(skipMap),
		)
		return
	}

	var client *wrappedClient
	if client, err = r.driver.getClientForWrite(); err != nil {
		return
	}
	defer client.Putback()

	entry := SkipMapToLogEntry(skipMap)

	_, err = client.Append(
		ctx, entry, moerr.CauseAppendSkipCmd,
	)
	return
}
