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
	if lsn > d.truncating.Load() {
		d.truncating.Store(lsn)
	}
	_, err := d.truncateQueue.Enqueue(struct{}{})
	if err != nil {
		panic(err)
	}
	return nil
}

func (d *LogServiceDriver) GetTruncated() (lsn uint64, err error) {
	lsn = d.truncating.Load()
	return
}

func (d *LogServiceDriver) onTruncate(items ...any) {
	d.doTruncate()
}

func (d *LogServiceDriver) doTruncate() {
	t0 := time.Now()

	target := d.truncating.Load()
	truncatedPSN := d.truncatedPSN
	psn := truncatedPSN

	//TODO use valid lsn
	next := d.getNextValidPSN(psn)
	loopCount := 0
	for d.isToTruncate(next, target) {
		loopCount++
		psn = next
		next = d.getNextValidPSN(psn)
		if next <= psn {
			break
		}
	}
	d.psnmu.RLock()
	minPSN := d.validPSN.Minimum()
	maxPSN := d.validPSN.Maximum()
	d.psnmu.RUnlock()
	logutil.Info(
		"Wal-Truncate",
		zap.Int("loop-count", loopCount),
		zap.Duration("duration", time.Since(t0)),
		zap.Uint64("target-psn", target),
		zap.Uint64("to-truncate-psn", psn),
		zap.Uint64("truncated-psn", truncatedPSN),
		zap.Uint64("valid-min-psn", minPSN),
		zap.Uint64("valid-max-psn", maxPSN),
		zap.Bool("do-truncate", psn != truncatedPSN),
	)
	if psn == truncatedPSN {
		return
	}
	d.doTruncatePSN(psn)
	d.truncatedPSN = psn
	d.gcPSN(psn)
}

func (d *LogServiceDriver) doTruncatePSN(psn uint64) {
	var (
		t0         = time.Now()
		client     *clientWithRecord
		retryTimes int
		err        error
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
		truncatedLsn := d.getTruncatedPSNFromRemote()
		if truncatedLsn == psn {
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
					truncatedLsn := d.getTruncatedPSNFromRemote()
					if truncatedLsn == psn {
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
func (d *LogServiceDriver) getTruncatedPSNFromRemote() (lsn uint64) {
	client, err := d.clientPool.Get()
	if err == ErrClientPoolClosed {
		return
	}
	if err != nil {
		panic(err)
	}
	defer d.clientPool.Put(client)
	ctx, cancel := context.WithTimeoutCause(context.Background(), d.config.GetTruncateDuration, moerr.CauseGetLogserviceTruncate)
	lsn, err = client.c.GetTruncatedLsn(ctx)
	err = moerr.AttachCause(ctx, err)
	cancel()
	if err != nil {
		err = RetryWithTimeout(d.config.RetryTimeout, func() (shouldReturn bool) {
			logutil.Infof("LogService Driver: retry gettruncate, err is %v", err)
			ctx, cancel := context.WithTimeoutCause(context.Background(), d.config.GetTruncateDuration, moerr.CauseGetLogserviceTruncate2)
			lsn, err = client.c.GetTruncatedLsn(ctx)
			err = moerr.AttachCause(ctx, err)
			cancel()
			return err == nil
		})
		if err != nil {
			panic(err)
		}
	}
	logutil.Infof("TRACE-WAL-TRUNCATE-Get Truncate %d", lsn)
	return
}
