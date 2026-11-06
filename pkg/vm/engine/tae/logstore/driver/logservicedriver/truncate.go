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
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	// "time"
)

// PXU TODO: Truncate(ctx context.Context, dsn uint64, sync bool) error
// driver lsn -> entry lsn
func (d *LogServiceDriver) Truncate(dsn uint64) error {
	for {
		old := d.truncateDSNIntent.Load()
		if dsn <= old {
			break
		}
		if d.truncateDSNIntent.CompareAndSwap(old, dsn) {
			logutil.Info(
				"Wal-Set-Truncate-Intent",
				zap.Uint64("dsn-intent", dsn),
				zap.Uint64("old-intent", old),
			)
			break
		}
	}
	_, err := d.truncateQueue.Enqueue(struct{}{})
	if err != nil {
		panic(err)
	}
	return nil
}

// PXU TODO: ???
func (d *LogServiceDriver) GetTruncated() (dsn uint64, err error) {
	return d.truncateDSNIntent.Load(), nil
}

func (d *LogServiceDriver) onTruncateRequests(items ...any) {
	if !d.canWrite() {
		logutil.Warn(
			"Wal-Readonly-Skip-Truncate",
			zap.Uint64("dsn-intent", d.truncateDSNIntent.Load()),
			zap.Uint64("truncated-psn", d.truncatedPSN),
		)
		return
	}
	d.doTruncate()
}

// this is always called by one goroutine
func (d *LogServiceDriver) doTruncate() {
	t0 := time.Now()

	dsnIntent := d.truncateDSNIntent.Load()
	truncatedPSN := d.truncatedPSN
	psnIntent := truncatedPSN

	nextValidPSN := d.getNextValidPSN(psnIntent)
	loopCount := 0
	for d.isToTruncate(nextValidPSN, dsnIntent) {
		loopCount++
		psnIntent = nextValidPSN
		nextValidPSN = d.getNextValidPSN(psnIntent)
		if nextValidPSN <= psnIntent {
			break
		}
	}
	d.sequence.mu.RLock()
	minPSN := d.sequence.psns.Minimum()
	maxPSN := d.sequence.psns.Maximum()
	d.sequence.mu.RUnlock()
	logutil.Info(
		"Wal-Truncate-Prepare",
		zap.Int("loop-count", loopCount),
		zap.Duration("duration", time.Since(t0)),
		zap.Uint64("dsn-intent", dsnIntent),
		zap.Uint64("psn-intent", psnIntent),
		zap.Uint64("truncated-psn", truncatedPSN),
		zap.Uint64("valid-min-psn", minPSN),
		zap.Uint64("valid-max-psn", maxPSN),
		zap.Bool("do-truncate", psnIntent > truncatedPSN),
	)
	if psnIntent <= truncatedPSN {
		return
	}
	var (
		err          error
		psnTruncated uint64
	)
	if psnTruncated, err = d.truncateFromRemote(
		d.truncateCtx,
		psnIntent,
		dsnIntent,
		10,
	); err != nil {
		// PXU TODO: metrics
		logutil.Error(
			"Wal-Truncate-Error",
			zap.Uint64("psn-intent", psnIntent),
			zap.Error(err),
		)
		if err != ErrClientPoolClosed && d.truncateCtx.Err() == nil {
			d.scheduleTruncateRetry()
		}
		return
	}
	if psnTruncated > d.truncatedPSN {
		d.truncatedPSN = psnTruncated
	}
	d.truncateByPSN(psnTruncated)
}

func (d *LogServiceDriver) scheduleTruncateRetry() {
	if !d.truncateRetryScheduled.CompareAndSwap(false, true) {
		return
	}
	go func() {
		timer := time.NewTimer(max(5*time.Second, d.config.RetryInterval(1)))
		defer timer.Stop()
		select {
		case <-d.closeC:
			d.truncateRetryScheduled.Store(false)
			return
		case <-timer.C:
		}
		d.truncateRetryScheduled.Store(false)
		if _, err := d.truncateQueue.Enqueue(struct{}{}); err != nil {
			select {
			case <-d.closeC:
			default:
				logutil.Error("Wal-Truncate-Retry-Enqueue", zap.Error(err))
			}
		}
	}()
}

// it is only called by the replayer and no concurrent scenario
// 1. update the truncatedPSN and the truncateDSNIntent
func (d *LogServiceDriver) commitTruncateInfo(psn uint64) {
	if psn < d.truncatedPSN {
		panic(fmt.Sprintf("invalid psn %d < %d", psn, d.truncatedPSN))
	}
	d.truncatedPSN = psn
	d.truncateByPSN(psn)
}

func (d *LogServiceDriver) truncateFromRemote(
	ctx context.Context,
	psnIntent uint64,
	dsnIntent uint64,
	maxRetry int,
) (psnTruncated uint64, err error) {
	var (
		t0         = time.Now()
		client     *wrappedClient
		retryTimes int
	)
	psnTruncated = psnIntent
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"Wal-Truncate-Execute",
			zap.Uint64("psn-intent", psnIntent),
			zap.Uint64("psn-truncated", psnTruncated),
			zap.Duration("duration", time.Since(t0)),
			zap.Int("retry-times", retryTimes),
			zap.Error(err),
		)
	}()

	if client, err = d.clientPool.GetOnFlyContext(ctx); err != nil {
		return
	}
	defer client.Putback()

	for ; retryTimes < maxRetry+1; retryTimes++ {
		if ctx.Err() != nil {
			err = ctx.Err()
			return
		}
		var (
			cancel  context.CancelFunc
			loopCtx context.Context
		)
		loopCtx, cancel = context.WithTimeoutCause(
			ctx, d.config.MaxTimeout, moerr.CauseTruncateLogservice,
		)
		err = client.wrapped.TruncateWAL(loopCtx, psnIntent, dsnIntent)
		err = moerr.AttachCause(loopCtx, err)
		cancel()

		// the psnIntent is already truncated
		if moerr.IsMoErrCode(err, moerr.ErrInvalidTruncateLsn) {
			loopCtx, cancel = context.WithTimeoutCause(
				ctx, d.config.MaxTimeout, moerr.CauseGetLogserviceTruncate,
			)
			var checkpointedDSN uint64
			psnTruncated, checkpointedDSN, err = d.getWALTruncateFromBackend(loopCtx)
			cancel()
			if err != nil {
				return
			}
			if psnTruncated >= psnIntent && checkpointedDSN >= dsnIntent {
				break
			}
			err = moerr.NewInternalErrorf(ctx,
				"WAL truncate has PSN %d but checkpointed DSN %d, expected at least %d",
				psnTruncated, checkpointedDSN, dsnIntent)
			return
		}
		if err == nil {
			break
		}
	}
	return
}

func (d *LogServiceDriver) getWALTruncateFromBackend(
	ctx context.Context,
) (psn, checkpointedDSN uint64, err error) {
	var (
		client     *wrappedClient
		retryTimes int
		start      = time.Now()
	)
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger("Wal-Get-Truncate", zap.Uint64("psn", psn),
			zap.Uint64("checkpointed-dsn", checkpointedDSN),
			zap.Int("retry-times", retryTimes), zap.Error(err),
			zap.Duration("duration", time.Since(start)))
	}()
	if client, err = d.clientPool.GetContext(ctx); err != nil {
		return
	}
	defer client.Putback()
	budgetCtx, cancel := context.WithTimeoutCause(
		ctx, d.config.MaxTimeout, moerr.CauseGetLogserviceTruncate)
	defer cancel()
	for {
		if budgetCtx.Err() != nil {
			err = moerr.AttachCause(budgetCtx, budgetCtx.Err())
			return
		}
		psn, checkpointedDSN, err = client.wrapped.GetWALTruncate(budgetCtx)
		if err == nil {
			return
		}
		retryTimes++
		if budgetCtx.Err() != nil {
			err = moerr.AttachCause(budgetCtx, budgetCtx.Err())
			return
		}
		timer := time.NewTimer(d.config.RetryInterval(retryTimes))
		select {
		case <-timer.C:
		case <-budgetCtx.Done():
			timer.Stop()
			err = moerr.AttachCause(budgetCtx, budgetCtx.Err())
			return
		}
		timer.Stop()
	}
}

func (d *LogServiceDriver) getTruncatedPSNFromBackend(
	ctx context.Context,
) (psn uint64, err error) {
	var (
		client     *wrappedClient
		retryTimes int
		start      = time.Now()
	)
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"Wal-Get-Truncate",
			zap.Uint64("psn", psn),
			zap.Int("retry-times", retryTimes),
			zap.Error(err),
			zap.Duration("duration", time.Since(start)),
		)
	}()
	if client, err = d.clientPool.Get(); err != nil {
		return
	}
	defer client.Putback()
	budgetCtx, cancel := context.WithTimeoutCause(
		ctx, d.config.MaxTimeout, moerr.CauseGetLogserviceTruncate)
	defer cancel()

	for {
		if budgetCtx.Err() != nil {
			err = moerr.AttachCause(budgetCtx, budgetCtx.Err())
			return
		}
		psn, err = client.wrapped.GetTruncatedLsn(budgetCtx)
		if err == nil {
			break
		}
		retryTimes++
		if budgetCtx.Err() != nil {
			err = moerr.AttachCause(budgetCtx, budgetCtx.Err())
			return
		}
		timer := time.NewTimer(d.config.RetryInterval(retryTimes))
		select {
		case <-timer.C:
		case <-budgetCtx.Done():
			timer.Stop()
			err = moerr.AttachCause(budgetCtx, budgetCtx.Err())
			return
		}
		timer.Stop()
	}
	return
}
