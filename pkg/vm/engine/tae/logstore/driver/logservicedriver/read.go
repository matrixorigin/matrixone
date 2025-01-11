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
	psns []uint64
	// PSN -> Record mapping
	records map[uint64]*recordEntry
	readMu  sync.RWMutex
}

func newReadCache() readCache {
	return readCache{
		psns:    make([]uint64, 0),
		records: make(map[uint64]*recordEntry),
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

func (d *LogServiceDriver) readFromCache(psn uint64) (*recordEntry, error) {
	if len(d.records) == 0 {
		return nil, ErrAllRecordsRead
	}
	record, ok := d.records[psn]
	if !ok {
		return nil, ErrRecordNotFound
	}
	return record, nil
}

func (d *LogServiceDriver) appendRecords(
	records []logservice.LogRecord,
	firstPSN uint64,
	fn func(uint64, *recordEntry),
	maxsize int,
) {
	cnt := 0
	for i, record := range records {
		if record.GetType() != pb.UserRecord {
			continue
		}
		psn := firstPSN + uint64(i)
		cnt++
		if maxsize != 0 {
			if cnt > maxsize {
				break
			}
		}
		if _, ok := d.records[psn]; ok {
			continue
		}
		d.records[psn] = newEmptyRecordEntry(record)
		d.psns = append(d.psns, psn)
		if fn != nil {
			fn(psn, d.records[psn])
		}
	}
}

func (d *LogServiceDriver) dropRecords() {
	dropCnt := len(d.psns) - d.config.ReadCacheSize
	psns := d.psns[:dropCnt]
	for _, psn := range psns {
		delete(d.records, psn)
	}
	d.psns = d.psns[dropCnt:]
}

func (d *LogServiceDriver) dropRecordFromCache(psn uint64) {
	delete(d.records, psn)
}

func (d *LogServiceDriver) resetReadCache() {
	for lsn := range d.records {
		delete(d.records, lsn)
	}
}

func (d *LogServiceDriver) readSmallBatchFromLogService(lsn uint64) {
	_, records := d.readFromBackend(lsn, int(d.config.RecordSize))
	if len(records) == 0 {
		_, records = d.readFromBackend(lsn, MaxReadSize)
	}
	d.appendRecords(records, lsn, nil, 1)
	if !d.IsReplaying() && len(d.psns) > d.config.ReadCacheSize {
		d.dropRecords()
	}
}

func (d *LogServiceDriver) readFromLogServiceInReplay(
	psn uint64,
	size int,
	fn func(uint64, *recordEntry),
) (uint64, uint64) {
	nextPSN, records := d.readFromBackend(psn, size)
	safePSN := uint64(0)
	d.appendRecords(
		records,
		psn,
		func(psn uint64, r *recordEntry) {
			r.unmarshal()
			if safePSN < r.appended {
				safePSN = r.appended
			}
			fn(psn, r)
		},
		0,
	)
	return nextPSN, safePSN
}

func (d *LogServiceDriver) readFromBackend(
	firstPSN uint64, maxSize int,
) (nextPSN uint64, records []logservice.LogRecord) {
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

	records, nextPSN, err = client.c.Read(
		ctx, firstPSN, uint64(maxSize),
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
				if records, nextPSN, err = client.c.Read(
					ctx, firstPSN, uint64(maxSize),
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
					zap.Uint64("from-psn", firstPSN),
					zap.Int("max-size", maxSize),
					zap.Int("num-records", len(records)),
					zap.Uint64("next-psn", nextPSN),
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
