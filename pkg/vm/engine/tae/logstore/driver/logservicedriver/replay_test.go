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
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	storeDriver "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

func Test_MockBackend1(t *testing.T) {
	ctx := context.Background()
	backend := NewMockBackend()
	client := newMockBackendClient(backend)
	defer client.Close()
	var (
		psnSize    = make(map[uint64]int)
		lastPSN    uint64
		psn        uint64
		toTruncate uint64
		truncated  uint64
		err        error
	)

	for i := 0; i < 14; i++ {
		if i == 4 || i == 8 {
			truncated, err = client.GetTruncatedLsn(ctx)
			assert.NoError(t, err)
			assert.Equal(t, toTruncate, truncated)
			toTruncate = uint64(i - 2)
			err = client.Truncate(ctx, toTruncate)
			assert.NoError(t, err)
		}
		record := client.GetLogRecord(400 * (i + 1))
		assert.Equal(t, 400*(i+1), len(record.Payload()))
		psn, err = client.Append(ctx, record)
		assert.NoError(t, err)
		psnSize[psn] = len(record.Payload())
		assert.Greater(t, psn, lastPSN)
		lastPSN = psn
	}

	truncated, err = client.GetTruncatedLsn(ctx)
	assert.NoError(t, err)
	assert.Equal(t, toTruncate, truncated)

	first := toTruncate + 1
	loaded, next, err := client.Read(ctx, first, 1000000)
	t.Log(first, len(loaded), next, err, psnSize)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(loaded))

	var userRecordCount int
	for _, r := range loaded {
		if r.Type == pb.UserRecord {
			userRecordCount++
		}
	}
	assert.Equal(t, 9, userRecordCount)
	assert.Equal(t, lastPSN+1, next)

	var next2 uint64
	loaded, next2, err = client.Read(ctx, next, 1000000)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(loaded))
	assert.Equal(t, next, next2)

	loaded, next, err = client.Read(ctx, first, 8000)
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), next)
	assert.Equal(t, 3, len(loaded))
}

func Test_AppendSkipCmd(t *testing.T) {
	service, ccfg := initTest(t)
	defer service.Close()

	cfg := NewConfig(
		"",
		WithConfigOptMaxClient(10),
		WithConfigOptClientBufSize(100),
		WithConfigOptClientConfig("", ccfg),
	)
	driver := NewLogServiceDriver(&cfg)
	defer driver.Close()

	r := newReplayer(nil, driver, MaxReadBatchSize)
	r.AppendSkipCmd(context.Background(), nil)
}

func TestAppendSkipCmd2(t *testing.T) {
	service, ccfg := initTest(t)
	defer service.Close()

	cfg := NewConfig(
		"",
		WithConfigOptMaxClient(10),
		WithConfigOptClientConfig("", ccfg),
	)
	driver := NewLogServiceDriver(&cfg)

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

	dsns := []uint64{8, 1, 2, 3, 6, 9, 7, 10, 13, 12}
	appended := []uint64{0, 1, 2, 5, 6, 6, 9, 10, 10, 10}

	writer := NewLogEntryWriter()
	client, _ := driver.getClientForWrite()
	defer client.Putback()
	for i := 0; i < entryCount; i++ {
		entries[i].DSN = dsns[i]
		err := writer.AppendEntry(entries[i])
		assert.NoError(t, err)
		writer.SetSafeDSN(appended[i])
		entry := writer.Finish()
		_, err = client.Append(
			context.Background(),
			entry,
			moerr.CauseDriverAppender1,
		)
		assert.NoError(t, err)
		writer.Reset()
	}

	for _, e := range entries {
		e.WaitDone()
	}

	{
		var list1 []uint64
		var list2 []uint64
		entryCount := 0
		assert.NoError(t, driver.Close())
		driver = NewLogServiceDriver(driver.GetCfg())
		err := driver.Replay(
			context.Background(),
			func(e *entry.Entry) storeDriver.ReplayEntryState {
				assert.Less(t, e.DSN, uint64(11))
				list1 = append(list1, e.DSN)
				if e.DSN > 7 {
					entryCount++
					list2 = append(list2, e.DSN)
					return storeDriver.RE_Nomal
				} else {
					return storeDriver.RE_Truncate
				}
			},
			func() storeDriver.ReplayMode {
				return storeDriver.ReplayMode_ReplayForWrite
			},
			nil,
		)
		t.Logf("list1: %v", list1)
		t.Logf("list2: %v", list2)
		assert.NoError(t, err)
		assert.Equal(t, 3, entryCount)
	}

	for _, e := range entries {
		e.Entry.Free()
	}

	driver.Close()
}

func TestAppendSkipCmd3(t *testing.T) {
	ctx := context.Background()
	service, ccfg := initTest(t)
	defer service.Close()

	cfg := NewConfig(
		"",
		WithConfigOptMaxClient(10),
		WithConfigOptClientConfig("", ccfg),
	)
	driver := NewLogServiceDriver(&cfg)
	// truncated: 7
	// empty log service

	{
		entryCount := 0
		assert.NoError(t, driver.Close())
		driver = NewLogServiceDriver(driver.GetCfg())
		err := driver.Replay(
			ctx,
			func(e *entry.Entry) storeDriver.ReplayEntryState {
				assert.Less(t, e.DSN, uint64(11))
				if e.DSN > 7 {
					entryCount++
					return storeDriver.RE_Nomal
				} else {
					return storeDriver.RE_Truncate
				}
			},
			func() storeDriver.ReplayMode {
				return storeDriver.ReplayMode_ReplayForWrite
			},
			nil,
		)
		assert.NoError(t, err)
		assert.Equal(t, 0, entryCount)
	}

	driver.Close()
}

func TestAppendSkipCmd4(t *testing.T) {
	service, ccfg := initTest(t)
	defer service.Close()

	cfg := NewConfig(
		"",
		WithConfigOptMaxClient(10),
		WithConfigOptClientConfig("", ccfg),
	)
	driver := NewLogServiceDriver(&cfg)

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

	dsns := []uint64{1, 2, 3, 5}
	appended := []uint64{1, 2, 3, 3}

	writer := NewLogEntryWriter()

	client, _ := driver.getClientForWrite()
	defer client.Putback()
	for i := 0; i < entryCount; i++ {
		entries[i].DSN = dsns[i]

		writer.SetSafeDSN(appended[i])
		err := writer.AppendEntry(entries[i])
		assert.NoError(t, err)
		entry := writer.Finish()
		_, err = client.Append(
			context.Background(),
			entry,
			moerr.CauseDriverAppender1,
		)
		assert.NoError(t, err)
		writer.Reset()
	}

	{
		entryCount := 0
		assert.NoError(t, driver.Close())
		driver = NewLogServiceDriver(driver.GetCfg())
		err := driver.Replay(
			context.Background(),
			func(e *entry.Entry) storeDriver.ReplayEntryState {
				assert.Less(t, e.DSN, uint64(4))
				if e.DSN > 0 {
					entryCount++
					return storeDriver.RE_Nomal
				} else {
					return storeDriver.RE_Truncate
				}
			},
			func() storeDriver.ReplayMode {
				return storeDriver.ReplayMode_ReplayForWrite
			},
			nil,
		)
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
	dsns = []uint64{4}
	appended = []uint64{4}

	writer = NewLogEntryWriter()

	client, _ = driver.getClientForWrite()
	defer client.Putback()
	for i := 0; i < entryCount; i++ {
		payload := []byte(fmt.Sprintf("payload %d", i))
		e := entry.MockEntryWithPayload(payload)
		entries[i] = e
		entries[i].DSN = dsns[i]

		writer.SetSafeDSN(appended[i])
		err := writer.AppendEntry(entries[i])
		assert.NoError(t, err)
		entry := writer.Finish()
		_, err = client.Append(
			context.Background(),
			entry,
			moerr.CauseDriverAppender1,
		)
		assert.NoError(t, err)
		writer.Reset()
		entries[i].Entry.Free()
	}
	{
		entryCount := 0
		assert.NoError(t, driver.Close())
		driver = NewLogServiceDriver(driver.GetCfg())
		err := driver.Replay(
			context.Background(),
			func(e *entry.Entry) storeDriver.ReplayEntryState {
				assert.Less(t, e.DSN, uint64(5))
				if e.DSN > 0 {
					entryCount++
					return storeDriver.RE_Nomal
				} else {
					return storeDriver.RE_Truncate
				}
			},
			func() storeDriver.ReplayMode {
				return storeDriver.ReplayMode_ReplayForWrite
			},
			nil,
		)
		assert.NoError(t, err)
		assert.Equal(t, 4, entryCount)
	}

	assert.NoError(t, driver.Close())
}

func TestAppendSkipCmd5(t *testing.T) {
	service, ccfg := initTest(t)
	defer service.Close()

	cfg := NewConfig(
		"",
		WithConfigOptMaxClient(10),
		WithConfigOptClientConfig("", ccfg),
	)
	driver := NewLogServiceDriver(&cfg)

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

	dsns := []uint64{10, 9, 12, 11}
	appended := []uint64{8, 10, 10, 12}
	writer := NewLogEntryWriter()

	client, _ := driver.getClientForWrite()
	defer client.Putback()
	for i := 0; i < entryCount; i++ {
		entries[i].DSN = dsns[i]
		writer.SetSafeDSN(appended[i])
		err := writer.AppendEntry(entries[i])
		assert.NoError(t, err)
		entry := writer.Finish()
		_, err = client.Append(
			context.Background(),
			entry,
			moerr.CauseDriverAppender1,
		)
		assert.NoError(t, err)
		writer.Reset()
	}

	for _, e := range entries {
		e.WaitDone()
	}

	{
		entryCount := 0
		assert.NoError(t, driver.Close())
		driver = NewLogServiceDriver(driver.GetCfg())
		err := driver.Replay(
			context.Background(),
			func(e *entry.Entry) storeDriver.ReplayEntryState {
				assert.Less(t, e.DSN, uint64(13))
				if e.DSN > 8 {
					entryCount++
					return storeDriver.RE_Nomal
				} else {
					return storeDriver.RE_Truncate
				}
			},
			func() storeDriver.ReplayMode {
				return storeDriver.ReplayMode_ReplayForWrite
			},
			nil,
		)
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
			{uint64(Cmd_Normal), 12, 30, 31, 0},
			{uint64(Cmd_Normal), 13, 28, 29, 1},
			{uint64(Cmd_Normal), 14, 32, 33, 27},
			{uint64(Cmd_Normal), 15, 36, 37, 28},
			{uint64(Cmd_Normal), 16, 41, 43, 32},
			{uint64(Cmd_Normal), 17, 38, 40, 32},
			{uint64(Cmd_Normal), 18, 34, 35, 32},
		},
		2,
	)
	psn, err := mockDriver.getTruncatedPSNFromBackend(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint64(13), psn)

	nextPSN, records, _ := mockDriver.readFromBackend(ctx, psn+1, 2)
	assert.Equal(t, uint64(16), nextPSN)
	assert.Equal(t, 2, len(records))
	assert.Equal(t, pb.UserRecord, records[0].Type)
	assert.Equal(t, pb.UserRecord, records[1].Type)
	nextPSN, records, _ = mockDriver.readFromBackend(ctx, nextPSN, 3)
	assert.Equal(t, uint64(19), nextPSN)
	assert.Equal(t, 3, len(records))
	assert.Equal(t, pb.UserRecord, records[0].Type)
	assert.Equal(t, pb.UserRecord, records[1].Type)
	assert.Equal(t, pb.UserRecord, records[2].Type)
	nextPSN, records, _ = mockDriver.readFromBackend(ctx, nextPSN, 1)
	assert.Equal(t, uint64(19), nextPSN)
	assert.Equal(t, 0, len(records))

	var appliedDSNs []uint64
	mockHandle := mockHandleFactory(39, func(e *entry.Entry) {
		appliedDSNs = append(appliedDSNs, e.DSN)
	})

	r := newReplayer(
		mockHandle,
		mockDriver,
		2,
		WithReplayerAppendSkipCmd(noopAppendSkipCmd),
		WithReplayerUnmarshalLogRecord(mockUnmarshalLogRecordFactor(mockDriver)),
	)

	err = r.Replay(ctx)
	assert.NoError(t, err)
	logutil.Info("DEBUG", r.exportFields(2)...)
	assert.Equal(t, []uint64{39, 40, 41, 42, 43}, appliedDSNs)
}

func Test_Replayer2(t *testing.T) {
	ctx := context.Background()
	mockDriver := newMockDriver(
		0,
		// MetaType,PSN,DSN-S,DSN-E,Safe
		[][5]uint64{
			{uint64(Cmd_Normal), 1, 30, 31, 0},
			{uint64(Cmd_Normal), 2, 28, 29, 0},
			{uint64(Cmd_Normal), 3, 32, 33, 0},
			{uint64(Cmd_Normal), 4, 36, 37, 0},
			{uint64(Cmd_Normal), 5, 41, 43, 0},
			{uint64(Cmd_Normal), 6, 38, 40, 0},
			{uint64(Cmd_Normal), 7, 34, 35, 0},
		},
		2,
	)
	var appliedDSNs []uint64
	mockHandle := mockHandleFactory(1, func(e *entry.Entry) {
		appliedDSNs = append(appliedDSNs, e.DSN)
	})

	r := newReplayer(
		mockHandle,
		mockDriver,
		2,
		WithReplayerAppendSkipCmd(noopAppendSkipCmd),
		WithReplayerUnmarshalLogRecord(mockUnmarshalLogRecordFactor(mockDriver)),
	)

	err := r.Replay(ctx)
	assert.NoError(t, err)
	logutil.Info("DEBUG", r.exportFields(2)...)
	assert.Equalf(t, []uint64{28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43}, appliedDSNs, "appliedDSNs: %v", appliedDSNs)
}

func Test_Replayer3(t *testing.T) {
	ctx := context.Background()
	mockDriver := newMockDriver(
		12,
		// MetaType,PSN,DSN-S,DSN-E,Safe
		[][5]uint64{
			{uint64(Cmd_Normal), 11, 30, 31, 0},
			{uint64(Cmd_Normal), 12, 28, 29, 0},
			{uint64(Cmd_Normal), 13, 32, 33, 0},
			{uint64(Cmd_Normal), 14, 36, 37, 0},
			{uint64(Cmd_Normal), 15, 41, 43, 0},
			{uint64(Cmd_Normal), 16, 38, 40, 0},
			{uint64(Cmd_Normal), 17, 34, 35, 0},
		},
		20,
	)
	var appliedDSNs []uint64
	mockHandle := mockHandleFactory(40, func(e *entry.Entry) {
		appliedDSNs = append(appliedDSNs, e.DSN)
	})

	r := newReplayer(
		mockHandle,
		mockDriver,
		20,
		WithReplayerAppendSkipCmd(noopAppendSkipCmd),
		WithReplayerUnmarshalLogRecord(mockUnmarshalLogRecordFactor(mockDriver)),
	)

	err := r.Replay(ctx)
	assert.NoError(t, err)
	logutil.Info("DEBUG", r.exportFields(2)...)
	t.Logf("appliedDSNs: %v", appliedDSNs)
	assert.Equal(t, []uint64{40, 41, 42, 43}, appliedDSNs)
}

func Test_Replayer4(t *testing.T) {
	ctx := context.Background()
	mockDriver := newMockDriver(
		12,
		// MetaType,PSN,DSN-S,DSN-E,Safe
		[][5]uint64{
			{uint64(Cmd_Normal), 11, 37, 37, 0},
			{uint64(Cmd_Normal), 12, 35, 35, 0},
			{uint64(Cmd_Normal), 13, 40, 40, 33},
			{uint64(Cmd_Normal), 14, 36, 36, 34},
			{uint64(Cmd_Normal), 15, 39, 39, 34},
			{uint64(Cmd_Normal), 16, 38, 38, 34},
		},
		30,
	)
	var appliedDSNs []uint64
	mockHandle := mockHandleFactory(38, func(e *entry.Entry) {
		appliedDSNs = append(appliedDSNs, e.DSN)
	})

	r := newReplayer(
		mockHandle,
		mockDriver,
		30,
		WithReplayerAppendSkipCmd(noopAppendSkipCmd),
		WithReplayerUnmarshalLogRecord(mockUnmarshalLogRecordFactor(mockDriver)),
	)

	err := r.Replay(ctx)
	assert.NoError(t, err)
	t.Logf("appliedDSNs: %v", appliedDSNs)
	assert.Equal(t, []uint64{38, 39, 40}, appliedDSNs)
}

func Test_Replayer5(t *testing.T) {
	ctx := context.Background()
	mockDriver := newMockDriver(
		12,
		// MetaType,PSN,DSN-S,DSN-E,Safe
		[][5]uint64{
			{uint64(Cmd_Normal), 11, 37, 37, 0},
			{uint64(Cmd_Normal), 12, 35, 35, 0},
			{uint64(Cmd_Normal), 13, 40, 40, 33},
			{uint64(Cmd_Normal), 14, 36, 36, 34},
			{uint64(Cmd_Normal), 15, 39, 39, 34},
			{uint64(Cmd_Normal), 16, 42, 42, 35},
			{uint64(Cmd_Normal), 17, 38, 38, 36},
			{uint64(Cmd_Normal), 18, 41, 41, 37},
			{uint64(Cmd_Normal), 19, 45, 45, 38},
			{uint64(Cmd_Normal), 20, 43, 43, 39},
			{uint64(Cmd_Normal), 21, 44, 44, 40},
		},
		30,
	)
	var appliedDSNs []uint64
	mockHandle := mockHandleFactory(38, func(e *entry.Entry) {
		appliedDSNs = append(appliedDSNs, e.DSN)
	})

	var psnReaded []uint64
	onRead := func(psn uint64, _ LogEntry) {
		if len(psnReaded) > 0 {
			assert.Equal(t, psnReaded[len(psnReaded)-1]+1, psn)
		}
		psnReaded = append(psnReaded, psn)
	}

	var dsnScheduled []uint64
	onScheduled := func(_ uint64, r LogEntry) {
		r.ForEachEntry(func(e *entry.Entry) {
			if len(dsnScheduled) > 0 {
				assert.True(t, dsnScheduled[len(dsnScheduled)-1] < e.DSN)
			}
			dsnScheduled = append(dsnScheduled, e.DSN)
		})
	}

	r := newReplayer(
		mockHandle,
		mockDriver,
		30,
		WithReplayerAppendSkipCmd(noopAppendSkipCmd),
		WithReplayerUnmarshalLogRecord(mockUnmarshalLogRecordFactor(mockDriver)),
		WithReplayerOnUserLogEntry(onRead),
		WithReplayerOnScheduled(onScheduled),
	)

	err := r.Replay(ctx)
	assert.NoError(t, err)
	t.Logf("psnReaded: %v", psnReaded)
	t.Logf("dsnScheduled: %v", dsnScheduled)
	t.Logf("appliedDSNs: %v", appliedDSNs)
	assert.Equal(t, []uint64{38, 39, 40, 41, 42, 43, 44, 45}, appliedDSNs)
}

func Test_Replayer6(t *testing.T) {
	ctx := context.Background()
	mockDriver := newMockDriver(
		0,
		// MetaType,PSN,DSN-S,DSN-E,Safe
		[][5]uint64{
			{uint64(Cmd_Normal), 1, 8, 8, 7},
			{uint64(Cmd_Normal), 2, 1, 1, 0},
			{uint64(Cmd_Normal), 3, 2, 2, 1},
			{uint64(Cmd_Normal), 4, 3, 3, 2},
			{uint64(Cmd_Normal), 5, 6, 6, 5},
			{uint64(Cmd_Normal), 6, 9, 9, 7},
			{uint64(Cmd_Normal), 7, 7, 7, 6},
			{uint64(Cmd_Normal), 8, 10, 10, 8},
			{uint64(Cmd_Normal), 9, 12, 12, 10},
		},
		30,
	)
	var appliedDSNs []uint64
	mockHandle := mockHandleFactory(8, func(e *entry.Entry) {
		appliedDSNs = append(appliedDSNs, e.DSN)
	})

	var psnReaded []uint64
	onRead := func(psn uint64, e LogEntry) {
		if len(psnReaded) > 0 {
			assert.Equal(t, psnReaded[len(psnReaded)-1]+1, psn)
		}
		psnReaded = append(psnReaded, psn)
	}

	var dsnScheduled []uint64
	onScheduled := func(_ uint64, r LogEntry) {
		r.ForEachEntry(func(e *entry.Entry) {
			if len(dsnScheduled) > 0 {
				assert.True(t, dsnScheduled[len(dsnScheduled)-1] < e.DSN)
			}
			dsnScheduled = append(dsnScheduled, e.DSN)
		})
	}

	writeSkip := make(map[uint64]uint64)
	onWriteSkip := func(m map[uint64]uint64) {
		for k, v := range m {
			writeSkip[k] = v
		}
	}

	r := newReplayer(
		mockHandle,
		mockDriver,
		30,
		WithReplayerOnWriteSkip(onWriteSkip),
		WithReplayerAppendSkipCmd(noopAppendSkipCmd),
		WithReplayerUnmarshalLogRecord(mockUnmarshalLogRecordFactor(mockDriver)),
		WithReplayerOnUserLogEntry(onRead),
		WithReplayerOnScheduled(onScheduled),
	)

	err := r.Replay(ctx)
	assert.NoError(t, err)
	t.Logf("psnReaded: %v", psnReaded)
	t.Logf("dsnScheduled: %v", dsnScheduled)
	t.Logf("appliedDSNs: %v", appliedDSNs)
	t.Logf("writeSkip: %v", writeSkip)
	assert.Equal(t, []uint64{8, 9, 10}, appliedDSNs)
	assert.Equal(t, map[uint64]uint64{12: 9}, writeSkip)
}

func Test_Replayer7(t *testing.T) {
	ctx := context.Background()
	mockDriver := newMockDriver(
		0,
		// MetaType,PSN,DSN-S,DSN-E,Safe
		[][5]uint64{
			{uint64(Cmd_Normal), 1, 8, 8, 7},
			{uint64(Cmd_Normal), 2, 1, 1, 0},
			{uint64(Cmd_Normal), 3, 2, 2, 1},
			{uint64(Cmd_Normal), 4, 3, 3, 2},
			{uint64(Cmd_Normal), 5, 6, 6, 5},
			{uint64(Cmd_Normal), 6, 9, 9, 7},
			{uint64(Cmd_Normal), 7, 7, 7, 6},
			{uint64(Cmd_Normal), 8, 10, 10, 8},
			// here is the problem
			// the safe DSN is 12 but there is no DSN 11 found
			// it should not happen and the replayer should return an error
			{uint64(Cmd_Normal), 9, 13, 13, 12},
			{uint64(Cmd_Normal), 10, 12, 12, 10},
		},
		30,
	)
	var appliedDSNs []uint64
	mockHandle := mockHandleFactory(8, func(e *entry.Entry) {
		appliedDSNs = append(appliedDSNs, e.DSN)
	})

	var psnReaded []uint64
	onRead := func(psn uint64, _ LogEntry) {
		if len(psnReaded) > 0 {
			assert.Equal(t, psnReaded[len(psnReaded)-1]+1, psn)
		}
		psnReaded = append(psnReaded, psn)
	}

	var dsnScheduled []uint64
	onScheduled := func(_ uint64, r LogEntry) {
		r.ForEachEntry(func(e *entry.Entry) {
			if len(dsnScheduled) > 0 {
				assert.True(t, dsnScheduled[len(dsnScheduled)-1] < e.DSN)
			}
			dsnScheduled = append(dsnScheduled, e.DSN)
		})
	}

	writeSkip := make(map[uint64]uint64)
	onWriteSkip := func(m map[uint64]uint64) {
		for k, v := range m {
			writeSkip[k] = v
		}
	}

	r := newReplayer(
		mockHandle,
		mockDriver,
		30,
		WithReplayerOnWriteSkip(onWriteSkip),
		WithReplayerAppendSkipCmd(noopAppendSkipCmd),
		WithReplayerUnmarshalLogRecord(mockUnmarshalLogRecordFactor(mockDriver)),
		WithReplayerOnUserLogEntry(onRead),
		WithReplayerOnScheduled(onScheduled),
	)

	err := r.Replay(ctx)
	assert.Error(t, err)
	t.Logf("psnReaded: %v", psnReaded)
	t.Logf("dsnScheduled: %v", dsnScheduled)
	t.Logf("appliedDSNs: %v", appliedDSNs)
	t.Logf("writeSkip: %v", writeSkip)
	assert.Equal(t, []uint64{8, 9, 10}, appliedDSNs)
	assert.Equal(t, 0, len(writeSkip))
}

func Test_Replayer8(t *testing.T) {
	ctx := context.Background()
	mockDriver := newMockDriver(
		0,
		// MetaType,PSN,DSN-S,DSN-E,Safe
		[][5]uint64{
			{uint64(Cmd_Normal), 1, 1, 1, 1},
			{uint64(Cmd_Normal), 2, 2, 2, 2},
			{uint64(Cmd_Normal), 3, 3, 3, 3},
			{uint64(Cmd_Normal), 4, 5, 5, 3},
			{uint64(Cmd_SkipDSN), 5, 0, 0, 0},
			{uint64(Cmd_Normal), 6, 6, 6, 4},
			{uint64(Cmd_Normal), 7, 4, 4, 3},
			{uint64(Cmd_Normal), 8, 5, 5, 4},
			// PXU TODO: add UT to test after replaying the skip, the DSN can be reused later
		},
		30,
	)
	mockDriver.addSkipMap(5, 5, 4)

	var appliedDSNs []uint64
	mockHandle := mockHandleFactory(3, func(e *entry.Entry) {
		appliedDSNs = append(appliedDSNs, e.DSN)
	})

	var psnReaded []uint64
	onRead := func(psn uint64, _ LogEntry) {
		// if len(psnReaded) > 0 {
		// 	assert.Equal(t, psnReaded[len(psnReaded)-1]+1, psn)
		// }
		t.Logf("Read PSN: %d", psn)
		psnReaded = append(psnReaded, psn)
	}

	var dsnScheduled []uint64
	onScheduled := func(_ uint64, r LogEntry) {
		r.ForEachEntry(func(e *entry.Entry) {
			if len(dsnScheduled) > 0 {
				assert.True(t, dsnScheduled[len(dsnScheduled)-1] < e.DSN)
			}
			t.Logf("Scheduled DSN: %d", e.DSN)
			dsnScheduled = append(dsnScheduled, e.DSN)
		})
	}

	writeSkip := make(map[uint64]uint64)
	onWriteSkip := func(m map[uint64]uint64) {
		for k, v := range m {
			writeSkip[k] = v
		}
	}

	r := newReplayer(
		mockHandle,
		mockDriver,
		30,
		WithReplayerOnWriteSkip(onWriteSkip),
		WithReplayerAppendSkipCmd(noopAppendSkipCmd),
		WithReplayerUnmarshalLogRecord(mockUnmarshalLogRecordFactor(mockDriver)),
		WithReplayerOnUserLogEntry(onRead),
		WithReplayerOnScheduled(onScheduled),
	)

	err := r.Replay(ctx)
	assert.NoError(t, err)
	t.Logf("psnReaded: %v", psnReaded)
	t.Logf("dsnScheduled: %v", dsnScheduled)
	t.Logf("appliedDSNs: %v", appliedDSNs)
	t.Logf("writeSkip: %v", writeSkip)
	assert.Equal(t, []uint64{3, 4, 5, 6}, appliedDSNs)
	assert.Equal(t, 0, len(writeSkip))
}

func Test_Replayer9(t *testing.T) {
	store := NewMockBackend()
	cfg := NewConfig(
		"",
		WithConfigOptMaxClient(10),
		WithConfigMockClient(store),
	)

	producer := NewLogServiceDriver(&cfg)
	defer producer.Close()
	consumer := NewLogServiceDriver(&cfg)
	defer consumer.Close()

	var maxReadDSN atomic.Uint64
	applyCnt := 0

	readCtx, readCancel := context.WithCancelCause(context.Background())
	go func() {
		consumer.Replay(
			readCtx,
			func(e *entry.Entry) storeDriver.ReplayEntryState {
				applyCnt++
				assert.Equalf(t, e.DSN-1, maxReadDSN.Load(), fmt.Sprintf("%d, %d", e.DSN-1, maxReadDSN.Load()))
				if e.DSN > maxReadDSN.Load() {
					maxReadDSN.Store(e.DSN)
				}
				return storeDriver.RE_Nomal
			},
			func() storeDriver.ReplayMode {
				return storeDriver.ReplayMode_ReplayForever
			},
			&storeDriver.ReplayOption{
				PollTruncateInterval: 10 * time.Millisecond,
			},
		)
	}()

	ctx := context.Background()
	err := producer.Replay(
		ctx,
		func(e *entry.Entry) storeDriver.ReplayEntryState {
			return storeDriver.RE_Nomal
		},
		func() storeDriver.ReplayMode {
			return storeDriver.ReplayMode_ReplayForWrite
		},
		nil,
	)
	assert.NoError(t, err)

	entryCnt := 10000

	entries := make([]*entry.Entry, 0, entryCnt)
	for i := 0; i < entryCnt; i++ {
		v := uint64(i)
		e := entry.MockEntryWithPayload(types.EncodeUint64(&v))
		err := producer.Append(e)
		assert.NoError(t, err)
		entries = append(entries, e)
	}
	for _, e := range entries {
		err := e.WaitDone()
		assert.NoError(t, err)
	}

	testutils.WaitExpect(5000, func() bool {
		return maxReadDSN.Load() == uint64(entryCnt)
	})
	assert.Equal(t, uint64(entryCnt), maxReadDSN.Load())
	assert.Equal(t, entryCnt, applyCnt)

	toTruncates := []uint64{entries[entryCnt/2].DSN, entries[entryCnt/4].DSN, entries[entryCnt/8].DSN}
	maxTrucateIntent := toTruncates[0]

	entries = entries[:0]
	for i := 0; i < entryCnt; i++ {
		v := uint64(entryCnt + i)
		e := entry.MockEntryWithPayload(types.EncodeUint64(&v))
		err := producer.Append(e)
		assert.NoError(t, err)
		entries = append(entries, e)
		if i == entryCnt/16 || i == entryCnt/4 || i == entryCnt/8 {
			if len(toTruncates) > 1 {
				dsn := toTruncates[len(toTruncates)-1]
				toTruncates = toTruncates[:len(toTruncates)-1]
				err := producer.Truncate(dsn)
				assert.NoError(t, err)
				t.Logf("Truncate DSN: %d", dsn)
			}
		}
	}

	for _, e := range entries {
		err = e.WaitDone()
		assert.NoError(t, err)
	}

	testutils.WaitExpect(10000, func() bool {
		return maxReadDSN.Load() == uint64(entryCnt*2)
	})
	assert.Equalf(t, uint64(entryCnt*2), maxReadDSN.Load(), fmt.Sprintf("%d, %d", entryCnt*2, maxReadDSN.Load()))

	cancelErr := moerr.NewInternalErrorNoCtx("cancel")
	readCancel(cancelErr)
	<-consumer.ReplayWaitC()
	readErr := consumer.GetReplayState().Err()
	t.Logf("Read error: %v", readErr)
	assert.ErrorIs(t, readErr, cancelErr)

	consumer = NewLogServiceDriver(&cfg)
	readCtx, readCancel = context.WithCancelCause(context.Background())
	maxReadDSN.Store(0)
	go func() {
		consumer.Replay(
			readCtx,
			func(e *entry.Entry) storeDriver.ReplayEntryState {
				if e.DSN <= maxTrucateIntent {
					return storeDriver.RE_Truncate
				}
				applyCnt++
				if maxReadDSN.Load() == 0 {
					maxReadDSN.Store(e.DSN)
				} else {
					assert.Equalf(t, e.DSN-1, maxReadDSN.Load(), fmt.Sprintf("%d, %d", e.DSN-1, maxReadDSN.Load()))
					if e.DSN > maxReadDSN.Load() {
						maxReadDSN.Store(e.DSN)
					}
				}
				return storeDriver.RE_Nomal
			},
			func() storeDriver.ReplayMode {
				return storeDriver.ReplayMode_ReplayForever
			},
			&storeDriver.ReplayOption{
				PollTruncateInterval: 10 * time.Millisecond,
			},
		)
	}()

	// entries = entries[:0]
	// for i := 0; i < entryCnt; i++ {
	// 	v := uint64(entryCnt*2 + i)
	// 	e := entry.MockEntryWithPayload(types.EncodeUint64(&v))
	// 	err := producer.Append(e)
	// 	assert.NoError(t, err)
	// 	entries = append(entries, e)
	// }
	// for _, e := range entries {
	// 	err := e.WaitDone()
	// 	assert.NoError(t, err)
	// }

	testutils.WaitExpect(10000, func() bool {
		return maxReadDSN.Load() == uint64(entryCnt*2)
	})
	t.Logf(
		"maxReadDSN: %d, maxTruncateIntent: %d",
		maxReadDSN.Load(), maxTrucateIntent,
	)
	assert.Equalf(t, uint64(entryCnt*2), maxReadDSN.Load(), fmt.Sprintf("%d, %d", entryCnt*2, maxReadDSN.Load()))

	readCancel(cancelErr)
	select {
	case <-consumer.ReplayWaitC():
	case <-time.After(5 * time.Second):
	}
	readErr = consumer.GetReplayState().Err()
	t.Logf("Read error: %v", readErr)
	assert.ErrorIs(t, readErr, cancelErr)
}

func Test_Replayer10(t *testing.T) {
	ctx := context.Background()
	mockDriver := newMockDriver(
		11,
		// MetaType,PSN,DSN-S,DSN-E,Safe
		[][5]uint64{
			// {uint64(Cmd_Normal), 12, 37, 37, 0},
			{uint64(Cmd_Normal), 13, 3025, 3064, 106},
			{uint64(Cmd_Normal), 14, 3070, 3070, 106},
			{uint64(Cmd_Normal), 15, 3068, 3069, 106},
			{uint64(Cmd_Normal), 16, 2484, 3024, 106},
			{uint64(Cmd_Normal), 17, 111, 2061, 59},
			{uint64(Cmd_Normal), 18, 2464, 2483, 106},
			{uint64(Cmd_Normal), 19, 3065, 3067, 106},
			{uint64(Cmd_Normal), 20, 3071, 3071, 106},
		},
		30,
	)
	var appliedDSNs []uint64
	mockHandle := mockHandleFactory(2502, func(e *entry.Entry) {
		// mockHandle := mockHandleFactory(2000, func(e *entry.Entry) {
		appliedDSNs = append(appliedDSNs, e.DSN)
	})

	var psnReaded []uint64
	onRead := func(psn uint64, _ LogEntry) {
		if len(psnReaded) > 0 {
			assert.Equal(t, psnReaded[len(psnReaded)-1]+1, psn)
		}
		psnReaded = append(psnReaded, psn)
	}

	var dsnScheduled []uint64
	onScheduled := func(_ uint64, r LogEntry) {
		r.ForEachEntry(func(e *entry.Entry) {
			// t.Logf("Scheduled DSN: %d", e.DSN)
			if len(dsnScheduled) > 0 {
				assert.True(t, dsnScheduled[len(dsnScheduled)-1] < e.DSN)
			}
			dsnScheduled = append(dsnScheduled, e.DSN)
		})
	}

	r := newReplayer(
		mockHandle,
		mockDriver,
		30,
		WithReplayerAppendSkipCmd(noopAppendSkipCmd),
		WithReplayerUnmarshalLogRecord(mockUnmarshalLogRecordFactor(mockDriver)),
		WithReplayerOnUserLogEntry(onRead),
		WithReplayerOnScheduled(onScheduled),
	)

	err := r.Replay(ctx)
	assert.NoError(t, err)
	t.Logf("psnReaded: %v", psnReaded)
	t.Logf("dsnScheduled: %d=>%d", dsnScheduled[0], dsnScheduled[len(dsnScheduled)-1])
	t.Logf("appliedDSNs: %d=>%d", appliedDSNs[0], appliedDSNs[len(appliedDSNs)-1])
	assert.Equal(t, uint64(2502), appliedDSNs[0])
	assert.Equal(t, uint64(3071), appliedDSNs[len(appliedDSNs)-1])
}

// start producer and consumer at the same time
// producer append 1000 entries while consumer replay forever
// change the consumer replay mode to replay for write
// and append 1000 entries again and open a new consumer to
// replay forever
func Test_Replayer11(t *testing.T) {
	store := NewMockBackend()
	cfg := NewConfig(
		"",
		WithConfigOptMaxClient(10),
		WithConfigMockClient(store),
	)

	producer := NewLogServiceDriver(&cfg)
	defer producer.Close()

	consumer := NewLogServiceDriver(&cfg)
	defer consumer.Close()

	appendCnt := 5000

	applyCnt := 0

	var mode atomic.Int32
	mode.Store(int32(storeDriver.ReplayMode_ReplayForever))

	go func() {
		err := consumer.Replay(
			context.Background(),
			func(e *entry.Entry) storeDriver.ReplayEntryState {
				applyCnt++
				return storeDriver.RE_Nomal
			},
			func() storeDriver.ReplayMode {
				return storeDriver.ReplayMode(mode.Load())
			},
			nil,
		)
		assert.NoError(t, err)
	}()

	var lastEntry *entry.Entry

	err := producer.Replay(
		context.Background(),
		func(e *entry.Entry) storeDriver.ReplayEntryState {
			return storeDriver.RE_Nomal
		},
		func() storeDriver.ReplayMode {
			return storeDriver.ReplayMode_ReplayForWrite
		},
		nil,
	)
	assert.NoError(t, err)

	for i := 0; i < appendCnt; i++ {
		v := uint64(i)
		e := entry.MockEntryWithPayload(types.EncodeUint64(&v))
		err = producer.Append(e)
		assert.NoError(t, err)
		lastEntry = e
	}
	err = lastEntry.WaitDone()
	assert.NoError(t, err)

	mode.Store(int32(storeDriver.ReplayMode_ReplayForWrite))

	<-consumer.ReplayWaitC()
	state := consumer.GetReplayState()
	assert.NoError(t, state.Err())
	assert.Equal(t, storeDriver.ReplayMode_ReplayForWrite, state.mode)
	assert.Equal(t, appendCnt, applyCnt)

	producer = consumer

	consumer = NewLogServiceDriver(&cfg)
	defer consumer.Close()

	applyCnt = 0
	mode.Store(int32(storeDriver.ReplayMode_ReplayForever))

	go func() {
		err := consumer.Replay(
			context.Background(),
			func(e *entry.Entry) storeDriver.ReplayEntryState {
				applyCnt++
				return storeDriver.RE_Nomal
			},
			func() storeDriver.ReplayMode {
				return storeDriver.ReplayMode(mode.Load())
			},
			nil,
		)
		assert.NoError(t, err)
	}()

	for i := 0; i < appendCnt; i++ {
		v := uint64(appendCnt + i)
		e := entry.MockEntryWithPayload(types.EncodeUint64(&v))
		err = producer.Append(e)
		assert.NoError(t, err)
		lastEntry = e
	}
	err = lastEntry.WaitDone()
	assert.NoError(t, err)

	mode.Store(int32(storeDriver.ReplayMode_ReplayForRead))

	<-consumer.ReplayWaitC()
	state = consumer.GetReplayState()
	assert.NoError(t, state.Err())
	assert.Equal(t, storeDriver.ReplayMode_ReplayForRead, state.mode)
	assert.Equal(t, appendCnt*2, applyCnt)
}
