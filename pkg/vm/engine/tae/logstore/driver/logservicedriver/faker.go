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
	"sort"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"golang.org/x/exp/rand"
)

var _ BackendClient = (*mockBackendClient)(nil)
var _ logservice.Client = (*mockBackendClient)(nil)

func MockTruncateLogRecord(psn uint64) logservice.LogRecord {
	ret := logservice.LogRecord{
		Type: pb.Internal,
		Lsn:  psn,
		Data: make([]byte, 8),
	}
	buf := types.EncodeUint64(&psn)
	copy(ret.Data, buf)

	return ret
}

func GetTruncatedPSNFromMockTruncateRecord(r logservice.LogRecord) uint64 {
	return types.DecodeUint64(r.Data)
}

func makeMockLogRecord(pos int) logservice.LogRecord {
	return logservice.LogRecord{
		Type: pb.UserRecord,
		Lsn:  uint64(pos),
	}
}

func noopAppendSkipCmd(_ context.Context, skipMap map[uint64]uint64) error {
	return nil
}

func mockUnmarshalLogRecordFactor(d *mockDriver) func(r logservice.LogRecord) LogEntry {
	return func(r logservice.LogRecord) (ret LogEntry) {
		var err error
		pos := int(r.Lsn)
		// [CmdType,PSN,DSN-S,DSN-E,SAFE]
		spec := d.recordSpecs[pos]
		switch CmdType(spec[0]) {
		case Cmd_Normal:
			cnt := int(spec[3] - spec[2] + 1)
			startDSN := spec[2]
			writer := NewLogEntryWriter()
			writer.SetSafeDSN(spec[4])
			for i := 0; i < cnt; i++ {
				e := entry.MockEntryWithPayload(nil)
				e.DSN = startDSN + uint64(i)
				if err = writer.AppendEntry(e); err != nil {
					panic(err)
				}
			}

			if ret, err = writer.Finish(); err != nil {
				panic(err)
			}
		case Cmd_SkipDSN:
			ret = SkipMapToLogEntry(d.skipMap[spec[1]])
		}
		return
	}
}

func mockHandleFactory(fromDSN uint64, onApplyCB func(*entry.Entry)) func(*entry.Entry) (replayEntryState driver.ReplayEntryState) {
	return func(e *entry.Entry) (replayEntryState driver.ReplayEntryState) {
		if e.DSN < fromDSN {
			return driver.RE_Truncate
		}
		if onApplyCB != nil {
			onApplyCB(e)
		}
		return driver.RE_Nomal
	}
}

type mockDriver struct {
	truncatedPSN uint64
	recordSpecs  [][5]uint64
	skipMap      map[uint64]map[uint64]uint64
	maxClient    int
}

func newMockDriver(
	truncatedPSN uint64,
	recordSpecs [][5]uint64,
	maxClient int,
) *mockDriver {
	return &mockDriver{
		truncatedPSN: truncatedPSN,
		recordSpecs:  recordSpecs,
		skipMap:      make(map[uint64]map[uint64]uint64),
		maxClient:    maxClient,
	}
}

func (d *mockDriver) GetMaxClient() int {
	return d.maxClient
}

func (d *mockDriver) addSkipMap(psn, skipDSN, skipPSN uint64) {
	if _, ok := d.skipMap[psn]; !ok {
		d.skipMap[psn] = make(map[uint64]uint64)
	}
	d.skipMap[psn][skipDSN] = skipPSN
}

func (d *mockDriver) getClientForWrite() (*wrappedClient, error) {
	return nil, nil
}

func (d *mockDriver) getTruncatedPSNFromBackend(ctx context.Context) (uint64, error) {
	return d.truncatedPSN, nil
}

func (d *mockDriver) readFromBackend(
	ctx context.Context, firstPSN uint64, maxSize int,
) (nextPSN uint64, records []logservice.LogRecord, err error) {
	for i, spec := range d.recordSpecs {
		if spec[1] >= firstPSN {
			records = append(records, makeMockLogRecord(i))
		}
		if len(records) >= maxSize {
			return spec[1] + 1, records, nil
		}
	}
	if len(d.recordSpecs) == 0 {
		nextPSN = firstPSN
	} else {
		nextPSN = d.recordSpecs[len(d.recordSpecs)-1][1] + 1
	}

	return
}

type mockBackend struct {
	sync.RWMutex
	store     []logservice.LogRecord
	nextPSN   uint64
	truncated uint64
	shardId   uint64
}

func NewMockBackend() *mockBackend {
	return &mockBackend{nextPSN: 1, shardId: 1}
}

type mockBackendClient struct {
	store MockBackend
}

func newMockBackendClient(backend MockBackend) *mockBackendClient {
	return &mockBackendClient{
		store: backend,
	}
}

func (c *mockBackend) Append(
	ctx context.Context, record logservice.LogRecord,
) (psn uint64, err error) {
	time.Sleep(time.Duration(rand.Intn(2000)+500) * time.Microsecond)
	cloned := record.Clone()
	c.Lock()
	defer c.Unlock()
	// {
	// 	e := LogEntry(record.Payload())
	// 	logutil.Infof("DEBUg-Append: PSN=%d,StartDSN=%d,Cnt=%d,Safe=%d", c.nextPSN, e.GetStartDSN(), e.GetEntryCount(), e.GetSafeDSN())
	// }
	cloned.Lsn = c.nextPSN
	cloned.Type = pb.UserRecord
	c.store = append(c.store, cloned)
	c.nextPSN++
	return cloned.Lsn, nil
}

func (c *mockBackend) GetTruncatedLsn(
	ctx context.Context,
) (psn uint64, err error) {
	c.RLock()
	defer c.RUnlock()
	return c.truncated, nil
}

func (c *mockBackend) Truncate(
	ctx context.Context, psnIntent uint64,
) (err error) {
	c.Lock()
	defer c.Unlock()
	if psnIntent <= c.truncated {
		err = moerr.NewInvalidTruncateLsn(ctx, c.shardId, psnIntent)
		return
	}
	var offset int
	for _, record := range c.store {
		if record.Lsn >= psnIntent {
			break
		}
		offset++
	}
	truncateRecord := MockTruncateLogRecord(psnIntent)
	truncateRecord.Lsn = c.nextPSN
	c.store = append(c.store, truncateRecord)
	c.store = c.store[offset:]
	c.truncated = psnIntent
	c.nextPSN++
	return
}

func (c *mockBackend) Read(
	ctx context.Context, firstPSN, maxSize uint64,
) (records []logservice.LogRecord, nextPSN uint64, err error) {
	c.RLock()
	defer c.RUnlock()
	idx := sort.Search(len(c.store), func(i int) bool {
		return c.store[i].Lsn >= firstPSN
	})
	// logutil.Infof("idx: %d, firstPSN=%d, total=%d", idx, firstPSN, len(c.store))
	if idx >= len(c.store) {
		nextPSN = firstPSN
		return
	}
	var (
		i    = idx
		size = 0
	)
	for ; i < len(c.store); i++ {
		records = append(records, c.store[i].Clone())
		size += len(c.store[i].Data) + 20 //
		if size >= int(maxSize) {
			break
		}
	}
	if i < len(c.store)-1 {
		nextPSN = c.store[i+1].Lsn
	} else {
		nextPSN = c.nextPSN
	}
	return
}

func (c *mockBackendClient) Close() (err error) {
	return
}

func (c *mockBackendClient) UpdateLeaseholderID(ctx context.Context, id uint64) error {
	return nil
}

func (c *mockBackendClient) GetLogRecord(size int) logservice.LogRecord {
	return logservice.NewUserLogRecord(0, size)
}

func (c *mockBackendClient) Append(
	ctx context.Context, record logservice.LogRecord,
) (psn uint64, err error) {
	return c.store.Append(ctx, record)
}

func (c *mockBackendClient) Truncate(
	ctx context.Context, psnIntent uint64,
) (err error) {
	return c.store.Truncate(ctx, psnIntent)
}

func (c *mockBackendClient) Read(
	ctx context.Context, firstPSN, maxSize uint64,
) (records []logservice.LogRecord, nextPSN uint64, err error) {
	return c.store.Read(ctx, firstPSN, maxSize)
}

func (c *mockBackendClient) GetTruncatedLsn(
	ctx context.Context,
) (psn uint64, err error) {
	return c.store.GetTruncatedLsn(ctx)
}

func (c *mockBackendClient) SetRequiredLsn(ctx context.Context, lsn logservice.Lsn) error {
	panic("not implemented")
}
func (c *mockBackendClient) GetRequiredLsn(_ context.Context) (logservice.Lsn, error) {
	panic("not implemented")
}
func (c *mockBackendClient) GetLogRecordSize() int {
	panic("not implemented")
}
func (c *mockBackendClient) GetTSOTimestamp(_ context.Context, _ uint64) (uint64, error) {
	panic("not implemented")
}
func (c *mockBackendClient) GetLatestLsn(_ context.Context) (uint64, error) {
	panic("not implemented")
}

// ReadLsn
func (c *mockBackendClient) ReadLsn(_ context.Context, _ time.Time) (logservice.Lsn, error) {
	panic("not implemented")
}
func (c *mockBackendClient) Config() logservice.ClientConfig {
	panic("not implemented")
}
