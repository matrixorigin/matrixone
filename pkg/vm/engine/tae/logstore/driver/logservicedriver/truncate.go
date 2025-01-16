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
	d.psn.mu.RLock()
	minPSN := d.psn.records.Minimum()
	maxPSN := d.psn.records.Maximum()
	d.psn.mu.RUnlock()
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
		context.Background(),
		psnIntent,
		10,
	); err != nil {
		// PXU TODO: metrics
		logutil.Error(
			"Wal-Truncate-Error",
			zap.Uint64("psn-intent", psnIntent),
			zap.Error(err),
		)
		return
	}
	if psnTruncated > d.truncatedPSN {
		d.truncatedPSN = psnTruncated
	}
	d.gcPSN(psnTruncated)
}

func (d *LogServiceDriver) truncateFromRemote(
	ctx context.Context,
	psnIntent uint64,
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

	if client, err = d.clientPool.Get(); err == ErrClientPoolClosed {
		return
	}
	defer d.clientPool.Put(client)

	for ; retryTimes < maxRetry+1; retryTimes++ {
		var (
			cancel  context.CancelFunc
			loopCtx context.Context
		)
		loopCtx, cancel = context.WithTimeoutCause(
			ctx, d.config.TruncateDuration, moerr.CauseTruncateLogservice,
		)
		err = client.c.Truncate(loopCtx, psnIntent)
		err = moerr.AttachCause(loopCtx, err)
		cancel()

		// the psnIntent is already truncated
		if moerr.IsMoErrCode(err, moerr.ErrInvalidTruncateLsn) {
			loopCtx, cancel = context.WithTimeoutCause(
				ctx, d.config.GetTruncateDuration, moerr.CauseGetLogserviceTruncate,
			)
			psnTruncated, err = d.getTruncatedPSNFromBackend(loopCtx)
			cancel()
			if err != nil {
				return
			}
			if psnTruncated >= psnIntent {
				break
			}
		}
		if err == nil {
			break
		}
	}
	return
}

func (d *LogServiceDriver) getTruncatedPSNFromBackend(
	ctx context.Context,
) (psn uint64, err error) {
	var (
		client     *wrappedClient
		retryTimes int
		maxRetry   = 20
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
	defer d.clientPool.Put(client)

	for ; retryTimes < maxRetry; retryTimes++ {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeoutCause(
			ctx,
			d.config.GetTruncateDuration,
			moerr.CauseGetLogserviceTruncate,
		)
		psn, err = client.c.GetTruncatedLsn(ctx)
		err = moerr.AttachCause(ctx, err)
		cancel()
		if err == nil {
			break
		}
	}
	return
}
