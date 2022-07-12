package logservicedriver

import (
	"context"
	"errors"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

var ErrRecordNotFound = errors.New("driver read cache: lsn not found")

type readCache struct {
	lsns    []uint64
	records map[uint64]*recordEntry
	readMu  sync.RWMutex
}

func (d *LogServiceDriver) Read(drlsn uint64) entry.Entry {
	lsn, err := d.getLogServiceLsnByDriverLsn(drlsn)
	if err != nil {
		panic(err)
	}
	r, err := d.tryRead(lsn)
	if err != nil {
		d.readFromLogService(lsn)
		r, err = d.tryRead(lsn)
		if err != nil {
			panic(err)
		}
	}
	return *r.readEntry(drlsn)
}
func (d *LogServiceDriver) tryRead(lsn uint64) (*recordEntry, error) {
	record, ok := d.records[lsn]
	if !ok {
		return nil, ErrRecordNotFound
	}
	return record, nil
}
func (d *LogServiceDriver) readFromLogService(lsn uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), d.config.ReadDuration)
	defer cancel()
	client := d.clientPool.Get().(logservice.Client)
	records, lsn, err := client.Read(ctx, lsn, d.config.ReadMaxSize)
	if err != nil { //TODO
		panic(err)
	}
	d.appendRecords(records, lsn)
	client.Close() //TODO reuse clients without close
	d.clientPool.Put(client)
}

func (d *LogServiceDriver) appendRecords(records []logservice.LogRecord, firstlsn uint64) {
	d.readMu.Lock()
	d.readMu.Unlock()
	lsns := make([]uint64, 0)
	for i, record := range records {
		lsn := firstlsn + uint64(i)
		_, ok := d.records[lsn]
		if ok {
			continue
		}
		d.records[lsn] = &recordEntry{record: &record}
		lsns = append(lsns, lsn)
	}
	d.lsns = append(d.lsns, lsns...)
	if len(d.lsns) > d.config.ReadCacheSize {
		d.dropRecords()
	}
}

func (d *LogServiceDriver) dropRecords() {
	drop := len(d.lsns) - d.config.ReadCacheSize
	lsns := d.lsns[:drop]
	for _, lsn := range lsns {
		delete(d.records, lsn)
	}
	d.lsns = d.lsns[drop:]
}
