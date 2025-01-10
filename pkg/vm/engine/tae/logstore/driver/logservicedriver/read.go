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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"go.uber.org/zap"
)

var ErrRecordNotFound = moerr.NewInternalErrorNoCtx("driver read cache: lsn not found")
var ErrAllRecordsRead = moerr.NewInternalErrorNoCtx("driver read cache: all records are read")

type readCache struct {
	lsns    []uint64
	records map[uint64]*recordEntry
	readMu  sync.RWMutex
}

func newReadCache() *readCache {
	return &readCache{
		lsns:    make([]uint64, 0),
		records: make(map[uint64]*recordEntry),
		readMu:  sync.RWMutex{},
	}
}

func (d *LogServiceDriver) Read(dsn uint64) (*entry.Entry, error) {
	psn, err := d.getPSNByDSNWithRetry(dsn, 10)
	if err != nil {
		panic(err)
	}
	d.readMu.RLock()
	r, err := d.readFromCache(psn)
	d.readMu.RUnlock()
	if err != nil {
		d.readMu.Lock()
		r, err = d.readFromCache(psn)
		if err == nil {
			d.readMu.Unlock()
			return r.readEntry(dsn), nil
		}
		d.readSmallBatchFromLogService(psn)
		r, err = d.readFromCache(psn)
		if err != nil {
			logutil.Debugf("try read %d", psn)
			panic(err)
		}
		d.readMu.Unlock()
	}
	return r.readEntry(dsn), nil
}

func (d *LogServiceDriver) readFromCache(lsn uint64) (*recordEntry, error) {
	if len(d.records) == 0 {
		return nil, ErrAllRecordsRead
	}
	record, ok := d.records[lsn]
	if !ok {
		return nil, ErrRecordNotFound
	}
	return record, nil
}

func (d *LogServiceDriver) appendRecords(records []logservice.LogRecord, firstlsn uint64, fn func(uint64, *recordEntry), maxsize int) {
	lsns := make([]uint64, 0)
	cnt := 0
	for i, record := range records {
		if record.GetType() != pb.UserRecord {
			continue
		}
		lsn := firstlsn + uint64(i)
		cnt++
		if maxsize != 0 {
			if cnt > maxsize {
				break
			}
		}
		_, ok := d.records[lsn]
		if ok {
			continue
		}
		d.records[lsn] = newEmptyRecordEntry(record)
		lsns = append(lsns, lsn)
		if fn != nil {
			fn(lsn, d.records[lsn])
		}
	}
	d.lsns = append(d.lsns, lsns...)
}

func (d *LogServiceDriver) dropRecords() {
	drop := len(d.lsns) - d.config.ReadCacheSize
	lsns := d.lsns[:drop]
	for _, lsn := range lsns {
		delete(d.records, lsn)
	}
	d.lsns = d.lsns[drop:]
}
func (d *LogServiceDriver) dropRecordByLsn(lsn uint64) {
	delete(d.records, lsn)
}

func (d *LogServiceDriver) resetReadCache() {
	for lsn := range d.records {
		delete(d.records, lsn)
	}
}

func (d *LogServiceDriver) readSmallBatchFromLogService(lsn uint64) {
	_, records := d.doReadMany(lsn, int(d.config.RecordSize))
	if len(records) == 0 {
		_, records = d.doReadMany(lsn, MaxReadSize)
	}
	d.appendRecords(records, lsn, nil, 1)
	if !d.IsReplaying() && len(d.lsns) > d.config.ReadCacheSize {
		d.dropRecords()
	}
}

func (d *LogServiceDriver) readFromLogServiceInReplay(
	lsn uint64,
	size int,
	fn func(lsn uint64, r *recordEntry),
) (uint64, uint64) {
	nextLsn, records := d.doReadMany(lsn, size)
	safeLsn := uint64(0)
	d.appendRecords(
		records,
		lsn,
		func(lsn uint64, r *recordEntry) {
			r.unmarshal()
			if safeLsn < r.appended {
				safeLsn = r.appended
			}
			fn(lsn, r)
		},
		0,
	)
	return nextLsn, safeLsn
}

func (d *LogServiceDriver) doReadMany(
	lsn uint64, size int,
) (nextLsn uint64, records []logservice.LogRecord) {
	client, err := d.clientPool.Get()
	defer d.clientPool.Put(client)
	if err != nil {
		panic(err)
	}
	t0 := time.Now()
	ctx, cancel := context.WithTimeoutCause(
		context.Background(),
		d.config.ReadDuration,
		moerr.CauseReadFromLogService,
	)

	records, nextLsn, err = client.c.Read(
		ctx, lsn, uint64(size),
	)
	err = moerr.AttachCause(ctx, err)

	cancel()

	if err != nil {
		err = RetryWithTimeout(
			d.config.RetryTimeout,
			func() (shouldReturn bool) {
				var (
					dueErr = err
					now    = time.Now()
				)
				ctx, cancel := context.WithTimeoutCause(
					context.Background(),
					d.config.ReadDuration,
					moerr.CauseReadFromLogService2,
				)
				defer cancel()
				if records, nextLsn, err = client.c.Read(
					ctx, lsn, uint64(size),
				); err != nil {
					err = moerr.AttachCause(ctx, err)
				}
				logger := logutil.Info
				if err != nil {
					logger = logutil.Error
				}
				logger(
					"Wal-Retry-Read-In-Driver",
					zap.Any("due-error", dueErr),
					zap.Uint64("from-lsn", lsn),
					zap.Int("size", size),
					zap.Int("num-records", len(records)),
					zap.Uint64("next-lsn", nextLsn),
					zap.Duration("duration", time.Since(now)),
					zap.Error(err),
				)
				return err == nil
			},
		)
		if err != nil {
			panic(err)
		}
	}
	d.readDuration += time.Since(t0)
	return
}
