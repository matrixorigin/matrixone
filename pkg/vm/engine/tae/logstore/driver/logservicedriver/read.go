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

func (d *LogServiceDriver) Read(drlsn uint64) (*entry.Entry, error) {
	lsn, err := d.tryGetLogServiceLsnByDriverLsn(drlsn)
	if err != nil {
		panic(err)
	}
	d.readMu.RLock()
	r, err := d.readFromCache(lsn)
	d.readMu.RUnlock()
	if err != nil {
		d.readMu.Lock()
		r, err = d.readFromCache(lsn)
		if err == nil {
			d.readMu.Unlock()
			return r.readEntry(drlsn), nil
		}
		d.readSmallBatchFromLogService(lsn)
		r, err = d.readFromCache(lsn)
		if err != nil {
			logutil.Infof("try read %d", lsn)
			panic(err)
		}
		d.readMu.Unlock()
	}
	return r.readEntry(drlsn), nil
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
	_, records := d.readFromLogService(lsn, int(d.config.ReadMaxSize))
	d.appendRecords(records, lsn, nil, 1)
	if !d.IsReplaying() && len(d.lsns) > d.config.ReadCacheSize {
		d.dropRecords()
	}
}

func (d *LogServiceDriver) readFromLogServiceInReplay(lsn uint64, size int, fn func(lsn uint64, r *recordEntry)) (uint64, uint64) {
	nextLsn, records := d.readFromLogService(lsn, size)
	safeLsn := uint64(0)
	d.appendRecords(records, lsn, func(lsn uint64, r *recordEntry) {
		r.unmarshal()
		if safeLsn < r.appended {
			safeLsn = r.appended
		}
		fn(lsn, r)
	}, 0)
	return nextLsn, safeLsn
}

func (d *LogServiceDriver) readFromLogService(lsn uint64, size int) (nextLsn uint64, records []logservice.LogRecord) {
	client, err := d.clientPool.Get()
	defer d.clientPool.Put(client)
	if err != nil {
		panic(err)
	}
	t0 := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), d.config.ReadDuration)
	records, nextLsn, err = client.c.Read(ctx, lsn, uint64(size))
	cancel()
	if err != nil {
		err = RetryWithTimeout(d.config.RetryTimeout, func() (shouldReturn bool) {
			logutil.Infof("LogService Driver: retry read err is %v", err)
			ctx, cancel := context.WithTimeout(context.Background(), d.config.ReadDuration)
			records, nextLsn, err = client.c.Read(ctx, lsn, uint64(size))
			cancel()
			return err == nil
		})
		if err != nil {
			panic(err)
		}
	}
	d.readDuration += time.Since(t0)
	return
}
