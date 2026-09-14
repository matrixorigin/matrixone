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
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/vfs"
	"github.com/panjf2000/ants/v2"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	walentry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"

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
	startOnce     *sync.Once
}

type stopBarrierQueue struct {
	sm.Queue
	stopStarted chan struct{}
	releaseStop chan struct{}
	stopDone    chan struct{}
	startOnce   sync.Once
	releaseOnce sync.Once
	doneOnce    sync.Once
}

func newStopBarrierQueue(queue sm.Queue) *stopBarrierQueue {
	return &stopBarrierQueue{
		Queue:       queue,
		stopStarted: make(chan struct{}),
		releaseStop: make(chan struct{}),
		stopDone:    make(chan struct{}),
	}
}

func (q *stopBarrierQueue) Stop() {
	q.startOnce.Do(func() { close(q.stopStarted) })
	<-q.releaseStop
	q.Queue.Stop()
	q.doneOnce.Do(func() { close(q.stopDone) })
}

func (q *stopBarrierQueue) Release() {
	q.releaseOnce.Do(func() { close(q.releaseStop) })
}

type deadlineBlockingBackendClient struct {
	*mockBackendClient
	appendStarted chan struct{}
	release       chan struct{}
	startOnce     *sync.Once
}

// completionObservedEntry exposes the terminal notification point used by
// driver.Entry.DoneWithErr. The release barrier keeps the underlying waitgroup
// incomplete until the test has inspected the durable notification event.
type completionObservedEntry struct {
	*walentry.Base
	doneStarted chan struct{}
	doneRelease <-chan struct{}
	doneOnce    sync.Once
}

func (e *completionObservedEntry) GetInfo() any {
	e.doneOnce.Do(func() { close(e.doneStarted) })
	<-e.doneRelease
	return e.Base.GetInfo()
}

func newCompletionObservedEntry(
	payload []byte,
	doneStarted chan struct{},
	doneRelease <-chan struct{},
) *entry.Entry {
	base := walentry.GetBase()
	base.SetType(walentry.IOET_WALEntry_Test)
	base.SetInfo(&walentry.Info{})
	if err := base.SetPayload(payload); err != nil {
		panic(err)
	}
	base.PrepareWrite()
	return entry.NewEntry(&completionObservedEntry{
		Base:        base,
		doneStarted: doneStarted,
		doneRelease: doneRelease,
	})
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
	var startedOnce sync.Once
	factory := func() (logservice.Client, error) {
		return &blockingBackendClient{
			mockBackendClient: newMockBackendClient(backend),
			appendStarted:     started,
			release:           release,
			startOnce:         &startedOnce,
		}, nil
	}
	cfg := NewConfig("", WithConfigOptClientFactory(factory), WithConfigOptMaxClient(2))
	d := NewLogServiceDriver(&cfg)
	var releaseOnce sync.Once
	releaseWorkers := func() { releaseOnce.Do(func() { close(release) }) }
	var first, second *entry.Entry
	var secondSubmitted, secondDone chan struct{}
	t.Cleanup(func() {
		releaseWorkers()
		closeErr := d.Close()
		if closeErr != nil {
			t.Errorf("driver close failed during cleanup: %v", closeErr)
		}
		joined := true
		if secondDone != nil {
			select {
			case <-secondDone:
			case <-time.After(time.Second):
				t.Error("second intent goroutine did not terminate during cleanup")
				joined = false
			}
		}
		if closeErr != nil || !joined {
			return
		}
		if first != nil {
			first.Entry.Free()
		}
		if second != nil {
			second.Entry.Free()
		}
	})
	// Keep two clients available while constraining the append worker capacity
	// to one, so the second accepted committer reaches the worker handoff while
	// the first task is still running.
	d.workers.Release()
	d.workers, _ = ants.NewPool(1, ants.WithNonblocking(true))

	first = entry.MockEntryWithPayload([]byte("first"))
	second = entry.MockEntryWithPayload([]byte("second"))
	d.onCommitIntents(first)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first append did not enter the production worker")
	}
	secondSubmitted = make(chan struct{})
	secondDone = make(chan struct{})
	go func() {
		defer close(secondDone)
		d.onCommitIntents(second)
		close(secondSubmitted)
	}()

	require.Eventually(t, func() bool { return d.pendingWait.Load() == 2 }, time.Second, time.Millisecond)
	select {
	case <-secondSubmitted:
		t.Fatal("second committer was submitted before the first worker returned")
	default:
	}

	releaseWorkers()
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
	t.Cleanup(func() { service.Close() })
	cfg := NewConfig("", WithConfigOptClientConfig("", ccfg), WithConfigOptMaxClient(1))
	d := NewLogServiceDriver(&cfg)
	releaseWorkerC := make(chan struct{})
	var releaseOnce sync.Once
	releaseWorker := func() { releaseOnce.Do(func() { close(releaseWorkerC) }) }
	d.workers.Release()
	d.workers, _ = ants.NewPool(1, ants.WithNonblocking(true))

	committer := getCommitter()
	var submitDone chan error
	var submitExited chan struct{}
	t.Cleanup(func() {
		releaseWorker()
		_ = d.Close()
		if submitExited != nil {
			select {
			case <-submitExited:
			case <-time.After(time.Second):
				t.Error("submit goroutine did not terminate during cleanup")
				return
			}
		}
		putCommitter(committer)
	})
	committer.startCommit()
	donePublished := make(chan struct{})
	require.NoError(t, d.workers.Submit(func() {
		committer.finishCommit()
		close(donePublished)
		<-releaseWorkerC
	}))
	<-donePublished

	taskRan := make(chan struct{})
	submitDone = make(chan error, 1)
	submitExited = make(chan struct{})
	go func() {
		defer close(submitExited)
		submitDone <- d.submitCommit(func() { close(taskRan) })
	}()
	select {
	case err := <-submitDone:
		t.Fatalf("submit returned before the worker was reusable: %v", err)
	default:
	}

	releaseWorker()
	require.NoError(t, <-submitDone)
	select {
	case <-taskRan:
	case <-time.After(time.Second):
		t.Fatal("submitted task did not run after worker handoff")
	}
	require.NoError(t, d.Close())
}

func TestCloseDeadlineIncludesIntakeAndWorkerDrain(t *testing.T) {
	const (
		closeTimeout = 250 * time.Millisecond
		workerBudget = 100 * time.Millisecond
		closeSlack   = 100 * time.Millisecond
	)
	backend := NewMockBackend()
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	factory := func() (logservice.Client, error) {
		return &deadlineBlockingBackendClient{
			mockBackendClient: newMockBackendClient(backend),
			appendStarted:     started,
			release:           release,
			startOnce:         &startedOnce,
		}, nil
	}
	cfg := NewConfig("", WithConfigOptClientFactory(factory), WithConfigOptMaxClient(2))
	d := NewLogServiceDriver(&cfg)
	d.closeTimeout = closeTimeout
	intakeQueue := newStopBarrierQueue(d.commitLoop)
	d.commitLoop = intakeQueue
	var releaseOnce sync.Once
	releaseAppend := func() { releaseOnce.Do(func() { close(release) }) }
	var remainingCleanupOnce sync.Once
	cleanupRemaining := func() {
		remainingCleanupOnce.Do(func() {
			d.waitCommitLoop.Stop()
			d.truncateQueue.Stop()
			d.clientPool.Close()
			d.cancel()
			close(d.commitWaitQueue)
			close(d.postCommitQueue)
		})
	}
	var first, second *entry.Entry
	var secondSubmitted chan struct{}
	var firstDoneStarted chan struct{}
	var firstDoneRelease chan struct{}
	var firstDoneReleaseOnce sync.Once
	releaseFirstDone := func() {
		if firstDoneRelease != nil {
			firstDoneReleaseOnce.Do(func() { close(firstDoneRelease) })
		}
	}
	t.Cleanup(func() {
		releaseAppend()
		releaseFirstDone()
		intakeQueue.Release()
		if err := d.Close(); err != nil {
			// Close may have returned at the shared deadline before the worker
			// and wait loop finished. Release the blocker first, then clean up
			// only after accepted work has drained.
			deadline := time.NewTimer(time.Second)
			timedOut := false
			for d.pendingWait.Load() != 0 && !timedOut {
				select {
				case <-deadline.C:
					timedOut = true
				case <-time.After(time.Millisecond):
				}
			}
			if !deadline.Stop() {
				select {
				case <-deadline.C:
				default:
				}
			}
			if d.pendingWait.Load() == 0 {
				cleanupRemaining()
			}
		}
		if secondSubmitted != nil {
			select {
			case <-secondSubmitted:
			case <-time.After(time.Second):
				t.Error("second intent goroutine did not terminate during cleanup")
			}
		}
		select {
		case <-intakeQueue.stopDone:
		case <-time.After(time.Second):
			t.Error("intake stop barrier did not finish during cleanup")
		}
		if pending := d.pendingWait.Load(); pending != 0 {
			t.Errorf("%d accepted WAL committers remained pending during cleanup", pending)
			return
		}
		if first != nil {
			first.Entry.Free()
		}
		if second != nil {
			second.Entry.Free()
		}
	})
	d.workers.Release()
	d.workers, _ = ants.NewPool(1, ants.WithNonblocking(true))

	firstDoneStarted = make(chan struct{})
	firstDoneRelease = make(chan struct{})
	first = newCompletionObservedEntry(
		[]byte("first"),
		firstDoneStarted,
		firstDoneRelease,
	)
	second = entry.MockEntryWithPayload([]byte("second"))
	d.onCommitIntents(first)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first append did not enter the production worker")
	}
	secondSubmitted = make(chan struct{})
	go func() {
		d.onCommitIntents(second)
		close(secondSubmitted)
	}()
	require.Eventually(t, func() bool { return d.pendingWait.Load() == 2 }, time.Second, time.Millisecond)

	closeRequested := time.Now()
	closeDone := make(chan error, 1)
	go func() { closeDone <- d.Close() }()
	select {
	case <-intakeQueue.stopStarted:
	case <-time.After(time.Second):
		t.Fatal("close did not enter the intake-stop barrier")
	}
	closeDeadline := closeRequested.Add(closeTimeout)
	releaseIntakeTimer := time.NewTimer(time.Until(closeDeadline.Add(-workerBudget)))
	select {
	case <-releaseIntakeTimer.C:
		intakeQueue.Release()
	case err := <-closeDone:
		t.Fatalf("close returned before the intake barrier was released: %v", err)
	}
	closeGuard := time.NewTimer(time.Until(closeDeadline.Add(closeSlack)))
	var err error
	select {
	case err = <-closeDone:
		require.Error(t, err)
	case <-closeGuard.C:
		t.Fatal("close exceeded the shared intake and worker deadline")
	}
	select {
	case <-secondSubmitted:
	case <-time.After(time.Second):
		t.Fatal("close did not unblock worker submission")
	}
	require.Error(t, second.WaitDone())

	// DoneWithErr closes firstDoneStarted before it can complete the entry's
	// waitgroup. A closed channel is a durable terminal event, so this check is
	// independent of whether a waiter goroutine has been scheduled yet.
	select {
	case <-firstDoneStarted:
		t.Fatal("in-flight append acquired a synthetic terminal result")
	default:
	}
	require.Equal(t, int64(2), d.pendingWait.Load())

	releaseAppend()
	select {
	case <-firstDoneStarted:
	case <-time.After(time.Second):
		t.Fatal("in-flight append did not reach terminal notification")
	}
	releaseFirstDone()
	require.NoError(t, first.WaitDone())
	require.Eventually(t, func() bool { return d.pendingWait.Load() == 0 }, time.Second, time.Millisecond)

	// A production caller fail-stops after the deadline error. The test keeps
	// the process alive, so release the remaining internal loops explicitly.
	cleanupRemaining()
}

type countingBackendClient struct {
	*mockBackendClient
	appendCalls atomic.Int32
}

func (c *countingBackendClient) Append(ctx context.Context, record logservice.LogRecord) (uint64, error) {
	c.appendCalls.Add(1)
	return c.mockBackendClient.Append(ctx, record)
}

func TestPreCallbackFailureCompletesAllGroupWaitersBeforeFailStop(t *testing.T) {
	backend := NewMockBackend()
	errExpected := errors.New("pre-callback failed")
	var client *countingBackendClient
	factory := func() (logservice.Client, error) {
		client = &countingBackendClient{mockBackendClient: newMockBackendClient(backend)}
		return client, nil
	}
	cfg := NewConfig("", WithConfigOptClientFactory(factory), WithConfigOptMaxClient(1))
	d := NewLogServiceDriver(&cfg)
	failStop := make(chan error, 1)
	failStopEntered := make(chan struct{})
	releaseFailStop := make(chan struct{})
	var failStopOnce sync.Once
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFailStop) }) }
	var first, second *entry.Entry
	var firstWaitDone, secondWaitDone chan struct{}
	t.Cleanup(func() {
		release()
		closeErr := d.Close()
		if closeErr != nil {
			t.Errorf("driver close failed during cleanup: %v", closeErr)
		}
		joined := true
		if firstWaitDone != nil {
			select {
			case <-firstWaitDone:
			case <-time.After(time.Second):
				t.Error("first waiter did not terminate during cleanup")
				joined = false
			}
		}
		if secondWaitDone != nil {
			select {
			case <-secondWaitDone:
			case <-time.After(time.Second):
				t.Error("second waiter did not terminate during cleanup")
				joined = false
			}
		}
		if closeErr != nil || !joined {
			return
		}
		if first != nil {
			first.Entry.Free()
		}
		if second != nil {
			second.Entry.Free()
		}
	})
	d.onAppendFailure = func(err error) {
		failStopOnce.Do(func() { close(failStopEntered) })
		failStop <- err
		<-releaseFailStop
	}

	var callbacks atomic.Int32
	first = entry.MockEntryWithPayload([]byte("first pre-callback"))
	first.Entry.RegisterGroupWalPreCallbacks(func() error {
		callbacks.Add(1)
		return nil
	})
	second = entry.MockEntryWithPayload([]byte("second pre-callback"))
	second.Entry.RegisterGroupWalPreCallbacks(func() error {
		callbacks.Add(1)
		return errExpected
	})
	firstWait := make(chan error, 1)
	firstWaitDone = make(chan struct{})
	go func() {
		defer close(firstWaitDone)
		firstWait <- first.WaitDone()
	}()
	secondWait := make(chan error, 1)
	secondWaitDone = make(chan struct{})
	go func() {
		defer close(secondWaitDone)
		secondWait <- second.WaitDone()
	}()

	// Invoke the production group-intent callback with two accepted entries so
	// LogEntryWriter.Finish executes the pre-callbacks before BackendClient.Append.
	d.onCommitIntents(first, second)
	select {
	case <-failStopEntered:
	case <-time.After(time.Second):
		t.Fatal("append failure callback did not start")
	}
	require.ErrorIs(t, <-failStop, errExpected)
	select {
	case err := <-firstWait:
		require.ErrorIs(t, err, errExpected)
	case <-time.After(time.Second):
		t.Fatal("first waiter was not notified before fail-stop release")
	}
	select {
	case err := <-secondWait:
		require.ErrorIs(t, err, errExpected)
	case <-time.After(time.Second):
		t.Fatal("second waiter was not notified before fail-stop release")
	}
	release()
	require.Equal(t, int32(2), callbacks.Load())
	require.Zero(t, client.appendCalls.Load())
	require.Eventually(t, func() bool { return d.pendingWait.Load() == 0 }, time.Second, time.Millisecond)
	require.Zero(t, d.getCommittedDSNWatermark())

	// The wait loop may notify the same committer again during close; the
	// writer's cleared ownership list must keep both terminal results stable.
	require.NoError(t, d.Close())
	require.ErrorIs(t, first.WaitDone(), errExpected)
	require.ErrorIs(t, second.WaitDone(), errExpected)
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
