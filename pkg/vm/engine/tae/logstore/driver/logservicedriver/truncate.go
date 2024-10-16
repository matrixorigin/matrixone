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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
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
	lastServiceLsn := d.truncatedLogserviceLsn
	lsn := lastServiceLsn
	//TODO use valid lsn
	next := d.getNextValidLogserviceLsn(lsn)
	loopCount := 0
	for d.isToTruncate(next, target) {
		loopCount++
		lsn = next
		next = d.getNextValidLogserviceLsn(lsn)
		if next <= lsn {
			break
		}
	}
	d.addrMu.RLock()
	min := d.validLsn.Minimum()
	max := d.validLsn.Maximum()
	d.addrMu.RUnlock()
	logutil.Info("TRACE-WAL-TRUNCATE-Get LogService lsn",
		zap.Int("loop count", loopCount),
		zap.Uint64("driver lsn", target),
		zap.Uint64("min", min),
		zap.Uint64("max", max),
		zap.String("duration", time.Since(t0).String()))
	if lsn == lastServiceLsn {
		logutil.Info("LogService Driver: retrun because logservice is small")
		return
	}
	d.truncateLogservice(lsn)
	d.truncatedLogserviceLsn = lsn
	d.gcAddr(lsn)
}

func (d *LogServiceDriver) truncateLogservice(lsn uint64) {
	logutil.Info("TRACE-WAL-TRUNCATE-Start Truncate", zap.Uint64("lsn", lsn))
	t0 := time.Now()
	client, err := d.clientPool.Get()
	if err == ErrClientPoolClosed {
		return
	}
	if err != nil {
		panic(err)
	}
	defer d.clientPool.Put(client)
	ctx, cancel := context.WithTimeout(context.Background(), d.config.TruncateDuration)
	err = client.c.Truncate(ctx, lsn)
	cancel()
	if moerr.IsMoErrCode(err, moerr.ErrInvalidTruncateLsn) {
		truncatedLsn := d.getLogserviceTruncate()
		if truncatedLsn == lsn {
			err = nil
		}
	}
	if err != nil {
		err = RetryWithTimeout(d.config.RetryTimeout, func() (shouldReturn bool) {
			logutil.Infof("LogService Driver: retry truncate, lsn %d err is %v", lsn, err)
			ctx, cancel := context.WithTimeout(context.Background(), d.config.TruncateDuration)
			err = client.c.Truncate(ctx, lsn)
			cancel()
			if moerr.IsMoErrCode(err, moerr.ErrInvalidTruncateLsn) {
				truncatedLsn := d.getLogserviceTruncate()
				if truncatedLsn == lsn {
					err = nil
				}
			}
			return err == nil
		})
		if err != nil {
			panic(err)
		}
	}
	logutil.Info("TRACE-WAL-TRUNCATE-Truncate successfully",
		zap.Uint64("lsn", lsn),
		zap.String("duration",
			time.Since(t0).String()))
}
func (d *LogServiceDriver) getLogserviceTruncate() (lsn uint64) {
	client, err := d.clientPool.Get()
	if err == ErrClientPoolClosed {
		return
	}
	if err != nil {
		panic(err)
	}
	defer d.clientPool.Put(client)
	ctx, cancel := context.WithTimeout(context.Background(), d.config.GetTruncateDuration)
	lsn, err = client.c.GetTruncatedLsn(ctx)
	cancel()
	if err != nil {
		err = RetryWithTimeout(d.config.RetryTimeout, func() (shouldReturn bool) {
			logutil.Infof("LogService Driver: retry gettruncate, err is %v", err)
			ctx, cancel := context.WithTimeout(context.Background(), d.config.GetTruncateDuration)
			lsn, err = client.c.GetTruncatedLsn(ctx)
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
