package driver

import (
	"bytes"
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

var DefaultReadMaxSize = uint64(10)

type LogServiceDriver struct {
	logServiceClient logservice.Client
}

func NewLogServiceDriver(shardID, replicaID uint64,addr string) *LogServiceDriver {
	scfg := logservice.LogServiceClientConfig{
		ReadOnly:         false,
		ShardID:          shardID,
		ReplicaID:        replicaID,
		ServiceAddresses: []string{addr},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := logservice.CreateClient(ctx, "shard1", scfg) // TODO retry
	logutil.Infof("err is %v", err)
	return &LogServiceDriver{logServiceClient: c}
}

// Heartbeat?
// ShardID,ReplicaID?
// Shard对应client?
// Context用法
func (d *LogServiceDriver) makeRecord(e entry.Entry) logservice.LogRecord {
	buf := make([]byte, e.TotalSize())
	copy(buf, e.GetMetaBuf())
	copy(buf[len(e.GetMetaBuf()):], e.GetInfoBuf())
	copy(buf[e.TotalSize()-e.GetPayloadSize():], e.GetPayload())
	return logservice.LogRecord{Data: e.GetPayload()}
}

func (d *LogServiceDriver) makeEntry(r logservice.LogRecord) entry.Entry {
	bbuf := bytes.NewBuffer(r.Data)
	e := entry.GetBase()
	e.ReadMeta(bbuf)
	e.SetInfoBuf(r.Data[e.GetMetaSize() : e.GetMetaSize()+e.GetInfoSize()])
	e.Unmarshal(r.Data[e.TotalSize()-e.GetPayloadSize() : e.TotalSize()])
	return e
}

// TODO many entries in one record
func (d *LogServiceDriver) Append(e entry.Entry) (lsn uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	record := d.makeRecord(e)
	lsn, err := d.logServiceClient.Append(ctx, record)
	if err != nil {
		// TODO retry
		d.logServiceClient.Append(ctx, record)
	}
	return lsn
}
func (d *LogServiceDriver) Truncate(lsn uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	d.logServiceClient.Truncate(ctx, lsn) // TODO retry
}
func (d *LogServiceDriver) GetTruncated() (lsn uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	lsn, _ = d.logServiceClient.GetTruncatedIndex(ctx) //TODO retry
	return
}
func (d *LogServiceDriver) Read(lsn uint64) entry.Entry {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	records, _, _ := d.logServiceClient.Read(ctx, lsn, DefaultReadMaxSize) //TODO retry and check lsn
	record := records[0]                                                   //TODO save in cache and read from cache
	e := d.makeEntry(record)
	return e
}
func (d *LogServiceDriver) Close() error {
	return d.logServiceClient.Close()
}
