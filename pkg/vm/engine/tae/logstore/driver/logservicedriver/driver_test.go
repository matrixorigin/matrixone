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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"

	// "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/stretchr/testify/assert"
)

func initTest(t *testing.T) (*logservice.Service, *logservice.ClientConfig) {
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	fs := vfs.NewStrictMem()
	service, ccfg, err := logservice.NewTestService(fs)
	assert.NoError(t, err)
	return service, &ccfg
}

func restartDriver(t *testing.T, d *LogServiceDriver, h func(*entry.Entry)) *LogServiceDriver {
	assert.NoError(t, d.Close())
	t.Log("Addr:")
	// preAddr:=d.addr
	for lsn, intervals := range d.addr {
		t.Logf("%d %v", lsn, intervals)
	}
	// preLsns:=d.validLsn
	t.Logf("Valid lsn: %v", d.validLsn)
	t.Logf("Driver Lsn %d, Syncing %d, Synced %d", d.driverLsn, d.syncing, d.synced)
	t.Logf("Truncated %d", d.truncating.Load())
	t.Logf("LSTruncated %d", d.truncatedLogserviceLsn)
	d = NewLogServiceDriver(context.TODO(), d.config)
	tempLsn := uint64(0)
	err := d.Replay(func(e *entry.Entry) {
		if e.Lsn <= tempLsn {
			panic("logic err")
		}
		tempLsn = e.Lsn
		if h != nil {
			h(e)
		}
	})
	assert.NoError(t, err)
	t.Log("Addr:")
	for lsn, intervals := range d.addr {
		t.Logf("%d %v", lsn, intervals)
	}
	// assert.Equal(t,len(preAddr),len(d.addr))
	// for lsn,intervals := range preAddr{
	// 	replayedInterval,ok:=d.addr[lsn]
	// 	assert.True(t,ok)
	// 	assert.Equal(t,intervals.Intervals[0].Start,replayedInterval.Intervals[0].Start)
	// 	assert.Equal(t,intervals.Intervals[0].End,replayedInterval.Intervals[0].End)
	// }
	t.Logf("Valid lsn: %v", d.validLsn)
	// assert.Equal(t,preLsns.GetCardinality(),d.validLsn.GetCardinality())
	t.Logf("Driver Lsn %d, Syncing %d, Synced %d", d.driverLsn, d.syncing, d.synced)
	t.Logf("Truncated %d", d.truncating.Load())
	t.Logf("LSTruncated %d", d.truncatedLogserviceLsn)
	return d
}

func TestReplay1(t *testing.T) {
	// t.Skip("debug")
	service, ccfg := initTest(t)
	defer service.Close()

	cfg := NewTestConfig(ccfg)
	driver := NewLogServiceDriver(context.TODO(), cfg)

	entryCount := 10000
	entries := make([]*entry.Entry, entryCount)

	for i := 0; i < entryCount; i++ {
		payload := []byte(fmt.Sprintf("payload %d", i))
		e := entry.MockEntryWithPayload(payload)
		driver.Append(e)
		entries[i] = e
	}

	for _, e := range entries {
		e.WaitDone()
	}

	// i := 0
	// h := func(e *entry.Entry) {
	// 	payload := []byte(fmt.Sprintf("payload %d", i))
	// 	assert.Equal(t, payload, e.Entry.GetPayload())
	// 	i++
	// }

	driver = restartDriver(t, driver, nil)

	for _, e := range entries {
		e.Entry.Free()
	}

	driver.Close()
}

func TestReplay2(t *testing.T) {
	t.Skip("debug")

	service, ccfg := initTest(t)
	defer service.Close()

	cfg := NewTestConfig(ccfg)
	cfg.NewRecordSize = 100
	driver := NewLogServiceDriver(context.TODO(), cfg)

	entryCount := 10000
	entries := make([]*entry.Entry, entryCount)

	for i := 0; i < entryCount; i++ {
		payload := []byte(fmt.Sprintf("payload %d", i))
		e := entry.MockEntryWithPayload(payload)
		driver.Append(e)
		entries[i] = e
	}

	synced := driver.getSynced()
	driver.Truncate(synced)

	for i, e := range entries {
		e.WaitDone()
		assert.Equal(t, uint64(i+1), e.Lsn)
	}

	truncated, err := driver.GetTruncated()
	i := truncated
	t.Logf("truncate %d", i)
	assert.NoError(t, err)
	h := func(e *entry.Entry) {
		entryPayload := e.Entry.GetPayload()
		strs := strings.Split(string(entryPayload), " ")
		id, err := strconv.Atoi(strs[1])
		assert.NoError(t, err)
		if id <= int(truncated) {
			return
		}

		payload := []byte(fmt.Sprintf("payload %d", i))
		assert.Equal(t, payload, entryPayload)
		i++
	}

	driver = restartDriver(t, driver, h)

	for _, e := range entries {
		e.Entry.Free()
	}

	driver.Close()
}
