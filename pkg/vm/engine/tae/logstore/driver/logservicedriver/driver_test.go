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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lni/vfs"
	"github.com/panjf2000/ants/v2"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func initTest(t *testing.T) (*logservice.Service, *logservice.ClientConfig) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	fs := vfs.NewStrictMem()
	service, ccfg, err := logservice.NewTestService(fs)
	require.NoError(t, err)
	require.NotNil(t, service)
	return service, &ccfg
}

func TestClientAcquisitionFailureCompletesWaiterBeforePanic(t *testing.T) {
	service, ccfg := initTest(t)
	defer service.Close()
	cfg := NewConfig(
		"",
		WithConfigOptClientConfig("", ccfg),
		WithConfigOptMaxClient(1),
	)
	d := NewLogServiceDriver(&cfg)
	d.clientPool.Close()

	committer := getCommitter()
	e := entry.MockEntryWithPayload([]byte("client acquisition failure"))
	e.DSN = 1
	committer.AddIntent(e)
	require.Panics(t, func() { d.asyncCommit(committer) })
	require.ErrorIs(t, e.WaitDone(), ErrClientPoolClosed)
	require.NoError(t, d.Close())
}

type blockingBackendClient struct {
	*mockBackendClient
	appendStarted chan struct{}
	release       chan struct{}
	startOnce     sync.Once
}

type deadlineBlockingBackendClient struct {
	*mockBackendClient
	appendStarted chan struct{}
	release       chan struct{}
	startOnce     sync.Once
}

func (c *deadlineBlockingBackendClient) Append(
	_ context.Context,
	record logservice.LogRecord,
) (uint64, error) {
	c.startOnce.Do(func() { close(c.appendStarted) })
	<-c.release
	return c.mockBackendClient.Append(context.Background(), record)
}

func (c *blockingBackendClient) Append(ctx context.Context, record logservice.LogRecord) (uint64, error) {
	c.startOnce.Do(func() { close(c.appendStarted) })
	select {
	case <-c.release:
		return c.mockBackendClient.Append(ctx, record)
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func TestCommitSubmissionWaitsForWorkerHandoff(t *testing.T) {
	backend := NewMockBackend()
	started := make(chan struct{})
	release := make(chan struct{})
	factory := func() (logservice.Client, error) {
		return &blockingBackendClient{
			mockBackendClient: newMockBackendClient(backend),
			appendStarted:     started,
			release:           release,
		}, nil
	}
	cfg := NewConfig("", WithConfigOptClientFactory(factory), WithConfigOptMaxClient(2))
	d := NewLogServiceDriver(&cfg)
	// Keep two clients available while constraining the append worker capacity
	// to one, so the second accepted committer reaches the worker handoff while
	// the first task is still running.
	d.workers.Release()
	d.workers, _ = ants.NewPool(1, ants.WithNonblocking(true))

	first := entry.MockEntryWithPayload([]byte("first"))
	second := entry.MockEntryWithPayload([]byte("second"))
	d.onCommitIntents(first)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first append did not enter the production worker")
	}
	secondSubmitted := make(chan struct{})
	go func() {
		d.onCommitIntents(second)
		close(secondSubmitted)
	}()

	require.Eventually(t, func() bool { return d.pendingWait.Load() == 2 }, time.Second, time.Millisecond)
	select {
	case <-secondSubmitted:
		t.Fatal("second committer was submitted before the first worker returned")
	default:
	}

	close(release)
	require.NoError(t, first.WaitDone())
	require.NoError(t, second.WaitDone())
	select {
	case <-secondSubmitted:
	case <-time.After(time.Second):
		t.Fatal("second committer did not finish worker handoff")
	}
	require.Eventually(t, func() bool { return d.pendingWait.Load() == 0 }, time.Second, time.Millisecond)
	require.NoError(t, d.Close())
}

func TestSubmitCommitWaitsAfterCommitterCompletion(t *testing.T) {
	service, ccfg := initTest(t)
	defer service.Close()
	cfg := NewConfig("", WithConfigOptClientConfig("", ccfg), WithConfigOptMaxClient(1))
	d := NewLogServiceDriver(&cfg)
	d.workers.Release()
	d.workers, _ = ants.NewPool(1, ants.WithNonblocking(true))

	committer := getCommitter()
	committer.startCommit()
	donePublished := make(chan struct{})
	releaseWorker := make(chan struct{})
	require.NoError(t, d.workers.Submit(func() {
		committer.finishCommit()
		close(donePublished)
		<-releaseWorker
	}))
	<-donePublished

	taskRan := make(chan struct{})
	submitDone := make(chan error, 1)
	go func() {
		submitDone <- d.submitCommit(func() { close(taskRan) })
	}()
	select {
	case err := <-submitDone:
		t.Fatalf("submit returned before the worker was reusable: %v", err)
	default:
	}

	close(releaseWorker)
	require.NoError(t, <-submitDone)
	select {
	case <-taskRan:
	case <-time.After(time.Second):
		t.Fatal("submitted task did not run after worker handoff")
	}
	putCommitter(committer)
	require.NoError(t, d.Close())
}

func TestCloseDeadlineIncludesIntakeAndWorkerDrain(t *testing.T) {
	backend := NewMockBackend()
	started := make(chan struct{})
	release := make(chan struct{})
	factory := func() (logservice.Client, error) {
		return &deadlineBlockingBackendClient{
			mockBackendClient: newMockBackendClient(backend),
			appendStarted:     started,
			release:           release,
		}, nil
	}
	cfg := NewConfig("", WithConfigOptClientFactory(factory), WithConfigOptMaxClient(2))
	d := NewLogServiceDriver(&cfg)
	d.closeTimeout = 50 * time.Millisecond
	d.workers.Release()
	d.workers, _ = ants.NewPool(1, ants.WithNonblocking(true))

	first := entry.MockEntryWithPayload([]byte("first"))
	second := entry.MockEntryWithPayload([]byte("second"))
	d.onCommitIntents(first)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first append did not enter the production worker")
	}
	secondSubmitted := make(chan struct{})
	go func() {
		d.onCommitIntents(second)
		close(secondSubmitted)
	}()
	require.Eventually(t, func() bool { return d.pendingWait.Load() == 2 }, time.Second, time.Millisecond)

	start := time.Now()
	err := d.Close()
	require.Error(t, err)
	require.Less(t, time.Since(start), time.Second)
	select {
	case <-secondSubmitted:
	case <-time.After(time.Second):
		t.Fatal("close did not unblock worker submission")
	}
	require.Error(t, second.WaitDone())

	firstDone := make(chan error, 1)
	go func() { firstDone <- first.WaitDone() }()
	select {
	case <-firstDone:
		t.Fatal("in-flight append acquired a synthetic terminal result")
	default:
	}

	close(release)
	require.NoError(t, <-firstDone)
	require.Eventually(t, func() bool { return d.pendingWait.Load() == 0 }, time.Second, time.Millisecond)

	// A production caller fail-stops after the deadline error. The test keeps
	// the process alive, so release the remaining internal loops explicitly.
	d.waitCommitLoop.Stop()
	d.truncateQueue.Stop()
	d.clientPool.Close()
	d.cancel()
	close(d.commitWaitQueue)
	close(d.postCommitQueue)
}

type failingBackendClient struct {
	*mockBackendClient
	err error
}

func (c *failingBackendClient) Append(context.Context, logservice.LogRecord) (uint64, error) {
	return 0, c.err
}

func TestAppendFailureCompletesWaiterBeforeFailStop(t *testing.T) {
	backend := NewMockBackend()
	errExpected := errors.New("append failed")
	factory := func() (logservice.Client, error) {
		return &failingBackendClient{
			mockBackendClient: newMockBackendClient(backend),
			err:               errExpected,
		}, nil
	}
	cfg := NewConfig(
		"",
		WithConfigOptClientFactory(factory),
		WithConfigOptMaxClient(1),
		WithConfigOptMaxTimeout(time.Millisecond),
	)
	d := NewLogServiceDriver(&cfg)
	failStop := make(chan error, 1)
	d.onAppendFailure = func(err error) { failStop <- err }

	e := entry.MockEntryWithPayload([]byte("append failure"))
	d.onCommitIntents(e)
	require.ErrorIs(t, e.WaitDone(), errExpected)
	require.ErrorIs(t, <-failStop, errExpected)
	require.Eventually(t, func() bool { return d.pendingWait.Load() == 0 }, time.Second, time.Millisecond)
	require.Zero(t, d.getCommittedDSNWatermark())
	require.NoError(t, d.Close())
}

func restartDriver(t *testing.T, d *LogServiceDriver, h func(*entry.Entry)) *LogServiceDriver {
	assert.NoError(t, d.Close())
	t.Log("Addr:")
	// preAddr:=d.addr
	for lsn, intervals := range d.sequence.psn2DSNMap {
		t.Logf("%d %v", lsn, intervals)
	}
	// preLsns:=d.validPSN
	t.Logf("Valid lsn: %v", d.sequence.psns)
	t.Logf("Driver DSN %d, Synced %d", d.watermark.nextDSN.Load(), d.watermark.committedDSN)
	t.Logf("Truncated %d", d.truncateDSNIntent.Load())
	t.Logf("LSTruncated %d", d.truncatedPSN)
	d = NewLogServiceDriver(d.GetCfg())
	tempLsn := uint64(0)
	err := d.Replay(
		context.Background(),
		func(e *entry.Entry) driver.ReplayEntryState {
			if e.DSN <= tempLsn {
				panic("logic err")
			}
			tempLsn = e.DSN
			if h != nil {
				h(e)
			}
			return driver.RE_Nomal
		},
		func() driver.ReplayMode {
			return driver.ReplayMode_ReplayForWrite
		},
		nil,
	)
	assert.NoError(t, err)
	t.Log("Addr:")
	for lsn, intervals := range d.sequence.psn2DSNMap {
		t.Logf("%d %v", lsn, intervals)
	}
	// assert.Equal(t,len(preAddr),len(d.addr))
	// for lsn,intervals := range preAddr{
	// 	replayedInterval,ok:=d.addr[lsn]
	// 	assert.True(t,ok)
	// 	assert.Equal(t,intervals.Intervals[0].Start,replayedInterval.Intervals[0].Start)
	// 	assert.Equal(t,intervals.Intervals[0].End,replayedInterval.Intervals[0].End)
	// }
	t.Logf("Valid lsn: %v", d.sequence.psns)
	// assert.Equal(t,preLsns.GetCardinality(),d.validPSN.GetCardinality())
	t.Logf("Truncated %d", d.truncateDSNIntent.Load())
	t.Logf("LSTruncated %d", d.truncatedPSN)
	return d
}

func TestReplay1(t *testing.T) {
	// t.Skip("debug")
	service, ccfg := initTest(t)
	defer service.Close()

	cfg := NewConfig(
		"",
		WithConfigOptClientConfig("", ccfg),
		WithConfigOptClientBufSize(10*mpool.MB),
		WithConfigOptMaxClient(10),
	)
	d := NewLogServiceDriver(&cfg)

	err := d.Replay(
		context.Background(),
		func(e *entry.Entry) driver.ReplayEntryState {
			return driver.RE_Nomal
		},
		func() driver.ReplayMode {
			return driver.ReplayMode_ReplayForWrite
		},
		nil,
	)
	assert.NoError(t, err)

	entryCount := 10000
	entries := make([]*entry.Entry, entryCount)

	for i := 0; i < entryCount; i++ {
		payload := []byte(fmt.Sprintf("payload %d", i))
		e := entry.MockEntryWithPayload(payload)
		d.Append(e)
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

	d = restartDriver(t, d, nil)

	for _, e := range entries {
		e.Entry.Free()
	}

	d.Close()
}

func TestReplay2(t *testing.T) {
	t.Skip("debug")

	service, ccfg := initTest(t)
	defer service.Close()

	cfg := NewConfig(
		"",
		WithConfigOptClientConfig("", ccfg),
		WithConfigOptClientBufSize(100),
	)
	driver := NewLogServiceDriver(&cfg)

	entryCount := 10000
	entries := make([]*entry.Entry, entryCount)

	for i := 0; i < entryCount; i++ {
		payload := []byte(fmt.Sprintf("payload %d", i))
		e := entry.MockEntryWithPayload(payload)
		driver.Append(e)
		entries[i] = e
	}

	synced := driver.getCommittedDSNWatermark()
	driver.Truncate(synced)

	for i, e := range entries {
		e.WaitDone()
		assert.Equal(t, uint64(i+1), e.DSN)
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

// func Test_TokenController(t *testing.T) {
// 	c := newTokenController(100)
// 	var wg sync.WaitGroup

// 	pool, _ := ants.NewPool(64)
// 	defer pool.Release()

// 	now := time.Now()

// 	for i := 0; i < 1000; i++ {
// 		wg.Add(1)
// 		pool.Submit(func() {
// 			defer wg.Done()
// 			token := c.Apply()
// 			time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)+1))
// 			c.Putback(token)
// 		})
// 	}
// 	wg.Wait()
// 	t.Logf("time cost: %v", time.Since(now))
// }
