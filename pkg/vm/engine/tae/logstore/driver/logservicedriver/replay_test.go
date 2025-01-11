// Copyright 2024 Matrix Origin
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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	storeDriver "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/stretchr/testify/assert"
)

func Test_AppendSkipCmd(t *testing.T) {
	service, ccfg := initTest(t)
	defer service.Close()

	cfg := NewTestConfig("", ccfg)
	cfg.RecordSize = 100
	driver := NewLogServiceDriver(cfg)
	defer driver.Close()

	r := newReplayer(nil, driver, ReplayReadSize)
	r.AppendSkipCmd(nil)
}

func TestAppendSkipCmd2(t *testing.T) {
	service, ccfg := initTest(t)
	defer service.Close()

	cfg := NewTestConfig("", ccfg)
	driver := NewLogServiceDriver(cfg)

	entryCount := 10
	entries := make([]*entry.Entry, entryCount)

	for i := 0; i < entryCount; i++ {
		payload := []byte(fmt.Sprintf("payload %d", i))
		e := entry.MockEntryWithPayload(payload)
		entries[i] = e
	}

	// truncated: 7
	// LSN:      8, 1, 2, 3, 6, 9, 7, 10, 13, 12
	// appended: 0, 1, 2, 5, 6, 6, 9, 10, 10, 10
	// replay: 6-10

	lsns := []uint64{8, 1, 2, 3, 6, 9, 7, 10, 13, 12}
	appended := []uint64{0, 1, 2, 5, 6, 6, 9, 10, 10, 10}

	client, _ := driver.getClientForWrite()
	for i := 0; i < entryCount; i++ {
		entries[i].Lsn = lsns[i]

		entry := newRecordEntry()
		entry.appended = appended[i]
		entry.append(entries[i])
		size := entry.prepareRecord()
		client.TryResize(size)
		record := client.record
		copy(record.Payload(), entry.payload)
		record.ResizePayload(size)
		ctx, cancel := context.WithTimeoutCause(context.Background(), time.Second, moerr.CauseDriverAppender1)
		_, err := client.c.Append(ctx, record)
		cancel()
		assert.NoError(t, err)
		entries[i].DoneWithErr(nil)
	}

	for _, e := range entries {
		e.WaitDone()
	}

	{
		entryCount := 0
		assert.NoError(t, driver.Close())
		driver = NewLogServiceDriver(driver.config)
		err := driver.Replay(func(e *entry.Entry) storeDriver.ReplayEntryState {
			assert.Less(t, e.Lsn, uint64(11))
			if e.Lsn > 7 {
				entryCount++
				return storeDriver.RE_Nomal
			} else {
				return storeDriver.RE_Truncate
			}
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, entryCount)
	}

	for _, e := range entries {
		e.Entry.Free()
	}

	driver.Close()
}

func TestAppendSkipCmd3(t *testing.T) {
	service, ccfg := initTest(t)
	defer service.Close()

	cfg := NewTestConfig("", ccfg)
	driver := NewLogServiceDriver(cfg)
	// truncated: 7
	// empty log service

	{
		entryCount := 0
		assert.NoError(t, driver.Close())
		driver = NewLogServiceDriver(driver.config)
		err := driver.Replay(func(e *entry.Entry) storeDriver.ReplayEntryState {
			assert.Less(t, e.Lsn, uint64(11))
			if e.Lsn > 7 {
				entryCount++
				return storeDriver.RE_Nomal
			} else {
				return storeDriver.RE_Truncate
			}
		})
		assert.NoError(t, err)
		assert.Equal(t, 0, entryCount)
	}

	driver.Close()
}

func TestAppendSkipCmd4(t *testing.T) {
	service, ccfg := initTest(t)
	defer service.Close()

	cfg := NewTestConfig("", ccfg)
	driver := NewLogServiceDriver(cfg)

	entryCount := 4
	entries := make([]*entry.Entry, entryCount)

	for i := 0; i < entryCount; i++ {
		payload := []byte(fmt.Sprintf("payload %d", i))
		e := entry.MockEntryWithPayload(payload)
		entries[i] = e
	}

	// truncated: 0
	// LSN:      1, 2, 3, 5
	// appended: 1, 2, 3, 3
	// replay: 1-3

	lsns := []uint64{1, 2, 3, 5}
	appended := []uint64{1, 2, 3, 3}

	client, _ := driver.getClientForWrite()
	for i := 0; i < entryCount; i++ {
		entries[i].Lsn = lsns[i]

		entry := newRecordEntry()
		entry.appended = appended[i]
		entry.append(entries[i])
		size := entry.prepareRecord()
		client.TryResize(size)
		record := client.record
		copy(record.Payload(), entry.payload)
		record.ResizePayload(size)
		ctx, cancel := context.WithTimeoutCause(context.Background(), time.Second, moerr.CauseDriverAppender1)
		_, err := client.c.Append(ctx, record)
		cancel()
		assert.NoError(t, err)
		entries[i].Entry.Free()
	}

	{
		entryCount := 0
		assert.NoError(t, driver.Close())
		driver = NewLogServiceDriver(driver.config)
		err := driver.Replay(func(e *entry.Entry) storeDriver.ReplayEntryState {
			assert.Less(t, e.Lsn, uint64(4))
			if e.Lsn > 0 {
				entryCount++
				return storeDriver.RE_Nomal
			} else {
				return storeDriver.RE_Truncate
			}
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, entryCount)
	}

	for i := 0; i < entryCount; i++ {
		payload := []byte(fmt.Sprintf("payload %d", i))
		e := entry.MockEntryWithPayload(payload)
		entries[i] = e
	}

	// truncated: 0
	// LSN:      1, 2, 3, 5
	// appended: 1, 2, 3, 3
	// replay: 1-3

	entryCount = 1
	entries = make([]*entry.Entry, entryCount)
	lsns = []uint64{4}
	appended = []uint64{4}

	client, _ = driver.getClientForWrite()
	for i := 0; i < entryCount; i++ {
		payload := []byte(fmt.Sprintf("payload %d", i))
		e := entry.MockEntryWithPayload(payload)
		entries[i] = e
		entries[i].Lsn = lsns[i]

		entry := newRecordEntry()
		entry.appended = appended[i]
		entry.append(entries[i])
		size := entry.prepareRecord()
		client.TryResize(size)
		record := client.record
		copy(record.Payload(), entry.payload)
		record.ResizePayload(size)
		ctx, cancel := context.WithTimeoutCause(context.Background(), time.Second, moerr.CauseDriverAppender1)
		_, err := client.c.Append(ctx, record)
		cancel()
		assert.NoError(t, err)
		entries[i].Entry.Free()
	}
	{
		entryCount := 0
		assert.NoError(t, driver.Close())
		driver = NewLogServiceDriver(driver.config)
		err := driver.Replay(func(e *entry.Entry) storeDriver.ReplayEntryState {
			assert.Less(t, e.Lsn, uint64(5))
			if e.Lsn > 0 {
				entryCount++
				return storeDriver.RE_Nomal
			} else {
				return storeDriver.RE_Truncate
			}
		})
		assert.NoError(t, err)
		assert.Equal(t, 4, entryCount)
	}

	assert.NoError(t, driver.Close())
}

func TestAppendSkipCmd5(t *testing.T) {
	service, ccfg := initTest(t)
	defer service.Close()

	cfg := NewTestConfig("", ccfg)
	driver := NewLogServiceDriver(cfg)

	entryCount := 4
	entries := make([]*entry.Entry, entryCount)

	for i := 0; i < entryCount; i++ {
		payload := []byte(fmt.Sprintf("payload %d", i))
		e := entry.MockEntryWithPayload(payload)
		entries[i] = e
	}

	// truncated: 8
	// LSN:      10, 9, 12, 11
	// appended: 8, 10, 10, 12
	// replay: 9-12

	lsns := []uint64{10, 9, 12, 11}
	appended := []uint64{8, 10, 10, 12}

	client, _ := driver.getClientForWrite()
	for i := 0; i < entryCount; i++ {
		entries[i].Lsn = lsns[i]

		entry := newRecordEntry()
		entry.appended = appended[i]
		entry.append(entries[i])
		size := entry.prepareRecord()
		client.TryResize(size)
		record := client.record
		copy(record.Payload(), entry.payload)
		record.ResizePayload(size)
		ctx, cancel := context.WithTimeoutCause(context.Background(), time.Second, moerr.CauseDriverAppender1)
		_, err := client.c.Append(ctx, record)
		cancel()
		assert.NoError(t, err)
		entries[i].DoneWithErr(nil)
	}

	for _, e := range entries {
		e.WaitDone()
	}

	{
		entryCount := 0
		assert.NoError(t, driver.Close())
		driver = NewLogServiceDriver(driver.config)
		err := driver.Replay(func(e *entry.Entry) storeDriver.ReplayEntryState {
			assert.Less(t, e.Lsn, uint64(13))
			if e.Lsn > 8 {
				entryCount++
				return storeDriver.RE_Nomal
			} else {
				return storeDriver.RE_Truncate
			}
		})
		assert.NoError(t, err)
		assert.Equal(t, 4, entryCount)
	}

	for _, e := range entries {
		e.Entry.Free()
	}

	driver.Close()
}

// case 1: normal
func Test_Replayer1(t *testing.T) {
	ctx := context.Background()
	mockDriver := newMockDriver(
		13,
		// MetaType,PSN,DSN-S,DSN-E,Safe
		[][5]uint64{
			{uint64(TNormal), 12, 30, 31, 0},
			{uint64(TNormal), 13, 28, 29, 1},
			{uint64(TNormal), 14, 32, 33, 27},
			{uint64(TNormal), 15, 36, 37, 28},
			{uint64(TNormal), 16, 41, 43, 32},
			{uint64(TNormal), 17, 38, 40, 32},
			{uint64(TNormal), 18, 34, 35, 32},
		},
		2,
	)
	psn, err := mockDriver.getTruncatedPSNFromBackend(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(13), psn)

	nextPSN, records := mockDriver.readFromBackend(psn+1, 2)
	assert.Equal(t, uint64(16), nextPSN)
	assert.Equal(t, 2, len(records))
	assert.Equal(t, pb.UserRecord, records[0].Type)
	assert.Equal(t, pb.UserRecord, records[1].Type)
	nextPSN, records = mockDriver.readFromBackend(nextPSN, 3)
	assert.Equal(t, uint64(19), nextPSN)
	assert.Equal(t, 3, len(records))
	assert.Equal(t, pb.UserRecord, records[0].Type)
	assert.Equal(t, pb.UserRecord, records[1].Type)
	assert.Equal(t, pb.UserRecord, records[2].Type)
	nextPSN, records = mockDriver.readFromBackend(nextPSN, 1)
	assert.Equal(t, uint64(19), nextPSN)
	assert.Equal(t, 0, len(records))

	mockHandle := mockHandleFactory(39)

	r := newReplayer2(
		mockHandle,
		mockDriver,
		2,
		WithReplayerAppendSkipCmd(noopAppendSkipCmd),
		WithReplayerUnmarshalLogRecord(mockUnmarshalLogRecordFactor(mockDriver)),
	)

	err = r.Replay(ctx)
	assert.NoError(t, err)
	logutil.Info("DEBUG", r.exportFields(2)...)

}
