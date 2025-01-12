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

// driver lsn -> entry lsn
func (d *LogServiceDriver) Truncate(lsn uint64) error {
	logutil.Info("TRACE-WAL-TRUNCATE", zap.Uint64(" driver start truncate", lsn))
	if lsn > d.truncateDSNIntent.Load() {
		d.truncateDSNIntent.Store(lsn)
	}
	_, err := d.truncateQueue.Enqueue(struct{}{})
	if err != nil {
		panic(err)
	}
	return nil
}

// PXU TODO: ???
func (d *LogServiceDriver) GetTruncated() (lsn uint64, err error) {
	lsn = d.truncateDSNIntent.Load()
	return
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
		"Wal-Truncate",
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
	d.truncateFromRemote(psnIntent)
	d.truncatedPSN = psnIntent
	d.gcPSN(psnIntent)
}

func (d *LogServiceDriver) truncateFromRemote(psn uint64) {
	var (
		t0           = time.Now()
		truncatedPSN uint64
		client       *clientWithRecord
		retryTimes   int
		err          error
	)
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"Wal-Truncate-PSN",
			zap.Uint64("psn", psn),
			zap.Duration("duration", time.Since(t0)),
			zap.Int("retry-times", retryTimes),
			zap.Error(err),
		)
	}()

	if client, err = d.clientPool.Get(); err == ErrClientPoolClosed {
		return
	} else if err != nil {
		panic(err)
	}
	defer d.clientPool.Put(client)

	ctx, cancel := context.WithTimeoutCause(
		context.Background(), d.config.TruncateDuration, moerr.CauseTruncateLogservice,
	)
	err = client.c.Truncate(ctx, psn)
	err = moerr.AttachCause(ctx, err)
	cancel()

	if moerr.IsMoErrCode(err, moerr.ErrInvalidTruncateLsn) {
		if truncatedPSN, err = d.getTruncatedPSNFromBackend(context.TODO()); err != nil {
			panic(err)
		}
		if truncatedPSN == psn {
			err = nil
		}
	}
	if err != nil {
		err = RetryWithTimeout(
			d.config.RetryTimeout,
			func() (shouldReturn bool) {
				ctx, cancel := context.WithTimeoutCause(
					context.Background(),
					d.config.TruncateDuration,
					moerr.CauseTruncateLogservice2,
				)
				err = client.c.Truncate(ctx, psn)
				err = moerr.AttachCause(ctx, err)
				cancel()
				if moerr.IsMoErrCode(err, moerr.ErrInvalidTruncateLsn) {
					if truncatedPSN, err = d.getTruncatedPSNFromBackend(context.TODO()); err != nil {
						panic(err)
					}
					if truncatedPSN == psn {
						err = nil
					}
				}
				retryTimes++
				return err == nil
			},
		)
		if err != nil {
			panic(err)
		}
	}
}
func (d *LogServiceDriver) getTruncatedPSNFromBackend(
	ctx context.Context,
) (psn uint64, err error) {
	var (
		client     *clientWithRecord
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
