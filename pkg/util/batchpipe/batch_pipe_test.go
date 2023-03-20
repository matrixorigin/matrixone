// Copyright 2022 Matrix Origin
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

package batchpipe

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func waitChTimeout[T any](
	ch <-chan T,
	onRecvCheck func(element T, closed bool) (goOn bool, err error),
	after time.Duration,
) error {
	timeout := time.After(after)
	for {
		select {
		case <-timeout:
			return moerr.NewInternalError(context.TODO(), "timeout")
		case item, ok := <-ch:
			goOn, err := onRecvCheck(item, !ok)
			if err != nil {
				return err
			}
			if !ok || !goOn {
				return nil
			}
		}
	}
}

func waitUtil(timeout, check_interval time.Duration, check func() bool) error {
	timeoutTimer := time.After(timeout)
	for {
		select {
		case <-timeoutTimer:
			return moerr.NewInternalError(context.TODO(), "timeout")
		default:
			if check() {
				return nil
			}
			time.Sleep(check_interval)
		}
	}
}

type Pos struct {
	line    int
	linepos int
	docpos  int
}

const (
	T_INT = "tst__int"
	T_POS = "tst__pos"
)

type TestItem struct {
	name   string
	intval int
	posval *Pos
}

func (item *TestItem) GetName() string { return item.name }

var _ ItemBuffer[*TestItem, string] = (*intBuf)(nil)

type intBuf struct {
	Reminder
	sum int
}

func (b *intBuf) Add(x *TestItem) { b.sum += x.intval }

func (b *intBuf) Reset() { b.sum = 0; b.RemindReset() }

func (b *intBuf) IsEmpty() bool { return b.sum == 0 }

func (b *intBuf) ShouldFlush() bool { return b.sum > 100 }

func (b *intBuf) GetBatch(_ context.Context, _ *bytes.Buffer) string {
	return fmt.Sprintf("Batch int %d", b.sum)
}

var _ ItemBuffer[*TestItem, string] = (*posBuf)(nil)

type posBuf struct {
	Reminder
	posList  []*Pos
	wakeupCh chan<- time.Time
}

func (b *posBuf) Add(item *TestItem) {
	b.posList = append(b.posList, item.posval)
}

func (b *posBuf) Reset() { b.posList = b.posList[:0]; b.RemindReset() }

func (b *posBuf) IsEmpty() bool {
	select {
	case b.wakeupCh <- time.Now(): // when the reminder fires, it will check IsEmpty
	default:
	}
	return len(b.posList) == 0
}

func (b *posBuf) ShouldFlush() bool { return len(b.posList) > 3 }

// bytes.Buffer to mitigate mem allocaction and the return bytes should own its data
func (b *posBuf) GetBatch(ctx context.Context, buf *bytes.Buffer) string {
	buf.Reset()
	for _, pos := range b.posList {
		buf.WriteString(fmt.Sprintf("Ln %d, Col %d, Doc %d\n", pos.line, pos.linepos, pos.docpos))
	}
	return buf.String()
}

var _ PipeImpl[*TestItem, string] = &testCollector{}

type testCollector struct {
	sync.Mutex
	*BaseBatchPipe[*TestItem, string]
	receivedString []string
	posBufWakeupCh chan time.Time
	notify4Batch   func()
}

// create a new buffer for one kind of Item
func (c *testCollector) NewItemBuffer(name string) ItemBuffer[*TestItem, string] {
	switch name {
	case T_INT:
		return &intBuf{
			Reminder: NewConstantClock(0),
		}
	case T_POS:
		return &posBuf{
			Reminder: NewExpBackOffClock(30*time.Millisecond, 300*time.Millisecond, 2),
			wakeupCh: c.posBufWakeupCh,
		}
	}
	panic("unrecognized name")
}

// BatchHandler handle the StoreBatch from a ItemBuffer, for example, execute inster sql
// this handle may be running on multiple gorutine
func (c *testCollector) NewItemBatchHandler(ctx context.Context) func(batch string) {
	return func(batch string) {
		c.Lock()
		defer c.Unlock()
		c.receivedString = append(c.receivedString, batch)
		if c.notify4Batch != nil {
			c.notify4Batch()
		}
	}
}

func (c *testCollector) Received() []string {
	c.Lock()
	defer c.Unlock()
	return c.receivedString[:]
}

func (c *testCollector) ReceivedContains(item string) bool {
	c.Lock()
	defer c.Unlock()
	for _, s := range c.receivedString[:] {
		if s == item {
			return true
		}
	}
	return false
}

func newTestCollector(opts ...BaseBatchPipeOpt) *testCollector {
	collector := &testCollector{
		receivedString: make([]string, 0),
		posBufWakeupCh: make(chan time.Time, 10),
	}
	base := NewBaseBatchPipe[*TestItem, string](collector, opts...)
	collector.BaseBatchPipe = base
	return collector
}

func TestBaseCollector(t *testing.T) {
	ctx := context.TODO()
	collector := newTestCollector(PipeWithBatchWorkerNum(1))
	require.True(t, collector.Start(context.TODO()))
	require.False(t, collector.Start(context.TODO()))
	err := collector.SendItem(ctx,
		&TestItem{name: T_INT, intval: 32},
		&TestItem{name: T_POS, posval: &Pos{line: 1, linepos: 12, docpos: 12}},
		&TestItem{name: T_INT, intval: 33},
	)
	require.NoError(t, err)
	// after 30ms, flush pos type test item and we can find it
	batchString := fmt.Sprintf("Ln %d, Col %d, Doc %d\n", 1, 12, 12)
	err = waitUtil(60*time.Second, 30*time.Millisecond, func() bool {
		return collector.ReceivedContains(batchString)
	})
	require.NoError(t, err)

	// int sum is 100+, flush
	_ = collector.SendItem(ctx, &TestItem{name: T_INT, intval: 40})

	err = waitUtil(60*time.Second, 30*time.Millisecond, func() bool {
		return collector.ReceivedContains("Batch int 105")
	})
	require.NoError(t, err)

	_ = collector.SendItem(ctx, &TestItem{name: T_INT, intval: 40})
	handle, succ := collector.Stop(false)
	require.True(t, succ)
	// no items are received after stopping
	require.NoError(t, waitChTimeout(handle, func(element struct{}, closed bool) (goOn bool, err error) {
		assert.True(t, closed)
		return
	}, time.Second))
	require.Equal(t, 2, len(collector.Received()))
}

func TestBaseCollectorReminderBackOff(t *testing.T) {
	ctx := context.TODO()
	collector := newTestCollector(PipeWithBatchWorkerNum(1))
	require.True(t, collector.Start(context.TODO()))
	err := collector.SendItem(ctx, &TestItem{name: T_POS, posval: &Pos{line: 1, linepos: 12, docpos: 12}})
	require.NoError(t, err)

	// have to find 2 gaps bigger than 100ms to prove that backoff works well
	bigBackOffCnt := 0
	var prev time.Time
	waitTimeCheck := func(element time.Time, closed bool) (goOn bool, err error) {
		t.Log(element, bigBackOffCnt)
		if bigBackOffCnt >= 2 {
			return
		}
		if !prev.IsZero() && element.Sub(prev).Milliseconds() > 100 {
			bigBackOffCnt += 1
		}
		goOn = true
		prev = element
		return
	}

	// wait on posBufWakeCh, the wakeup freq will be bigger and bigger until 300ms
	require.NoError(t, waitChTimeout(collector.posBufWakeupCh, waitTimeCheck, 10*time.Second))

	batchString := fmt.Sprintf("Ln %d, Col %d, Doc %d\n", 1, 12, 12)
	err = waitUtil(60*time.Second, 10*time.Millisecond, func() bool {
		return collector.ReceivedContains(batchString)
	})
	require.NoError(t, err)

	_ = collector.SendItem(ctx, &TestItem{name: T_POS, posval: &Pos{line: 1, linepos: 12, docpos: 12}})

	// new write will reset timer to 30ms, so it should be received quickly, but…… what do we know about the machine?
	err = waitUtil(60*time.Second, 10*time.Millisecond, func() bool {
		return collector.ReceivedContains(batchString)
	})
	require.NoError(t, err)

	handle, succ := collector.Stop(false)
	require.True(t, succ)
	require.NoError(t, waitChTimeout(handle, func(element struct{}, closed bool) (goOn bool, err error) {
		assert.True(t, closed)
		return
	}, time.Second))
}

func TestBaseCollectorGracefulStop(t *testing.T) {
	ctx := context.TODO()
	collector := newTestCollector(PipeWithBatchWorkerNum(2), PipeWithBufferWorkerNum(1))
	collector.Start(context.TODO())

	err := collector.SendItem(ctx,
		&TestItem{name: T_INT, intval: 32},
		&TestItem{name: T_INT, intval: 40},
		&TestItem{name: T_INT, intval: 33},
	)
	require.NoError(t, err)
	err = waitUtil(60*time.Second, 10*time.Millisecond, func() bool {
		return collector.ReceivedContains("Batch int 105")
	})
	require.NoError(t, err)

	_ = collector.SendItem(ctx, &TestItem{name: T_INT, intval: 40})
	// graceful stopping will wait completing sending
	handle, succ := collector.Stop(true)
	require.True(t, succ)
	handle2, succ2 := collector.Stop(true)
	require.False(t, succ2)
	require.Nil(t, handle2)

	// send after stopping
	require.Error(t, collector.SendItem(ctx, &TestItem{name: T_INT, intval: 77}))
	// no new value
	require.NoError(t, waitChTimeout(handle, func(element struct{}, closed bool) (goOn bool, err error) {
		assert.True(t, closed)
		return
	}, time.Second))
	err = waitUtil(60*time.Second, 10*time.Millisecond, func() bool {
		return collector.ReceivedContains("Batch int 40")
	})
	require.NoError(t, err)
	t.Log(collector.Received())
}

func TestBaseReminder(t *testing.T) {
	ms := time.Millisecond
	registry := newReminderRegistry()
	registry.Register("1", 1*ms)
	registry.Register("2", 0)
	require.NotPanics(t, func() { registry.Register("1", 0*ms) })
	require.Panics(t, func() { registry.Register("1", 1*ms) })
	checkOneRecevied := func(_ string, closed bool) (goOn bool, err error) {
		if closed {
			err = moerr.NewInternalError(context.TODO(), "unexpected close")
		}
		return
	}
	// only one event 1 will be triggered
	require.NoError(t, waitChTimeout(registry.C, checkOneRecevied, 5000*ms))
	require.Error(t, waitChTimeout(registry.C, checkOneRecevied, 500*ms)) // timeout

	// nothing happens after these two lines
	registry.Reset("2", 2*ms)                                            // 2 is not in the registry
	registry.Reset("1", 0*ms)                                            // 0 is ignored
	require.Error(t, waitChTimeout(registry.C, checkOneRecevied, 50*ms)) // timeout
	registry.Reset("1", 5*ms)
	require.NoError(t, waitChTimeout(registry.C, checkOneRecevied, 5000*ms))
	registry.CleanAll()
}

func TestBaseRemind2(t *testing.T) {
	ms := time.Millisecond
	r := newReminderRegistry()
	cases := []*struct {
		id     string
		d      []time.Duration
		offset int
	}{
		{"0", []time.Duration{11 * ms, 8 * ms, 25 * ms}, 0},
		{"1", []time.Duration{7 * ms, 15 * ms, 16 * ms}, 0},
		{"2", []time.Duration{3 * ms, 12 * ms, 11 * ms}, 0},
	}

	type item struct {
		d  time.Duration
		id string
	}

	seq := []item{}

	for _, c := range cases {
		r.Register(c.id, c.d[c.offset])
		c.offset += 1
		t := 0 * ms
		for _, delta := range c.d {
			t += delta
			seq = append(seq, item{t, c.id})
		}
	}

	sort.Slice(seq, func(i, j int) bool {
		return int64(seq[i].d) < int64(seq[j].d)
	})

	gotids := make([]string, 0, 9)
	for i := 0; i < 9; i++ {
		id := <-r.C
		gotids = append(gotids, id)
		idx, _ := strconv.ParseInt(id, 10, 32)
		c := cases[idx]
		if c.offset < 3 {
			r.Reset(id, c.d[c.offset])
			c.offset++
		}
	}

	diff := 0
	for i := range gotids {
		if gotids[i] != seq[i].id {
			diff++
		}
	}

	// We have seen 9 events unitil now, it is ok.
	// Appending to reminder.C happens in a goroutine, considering its scheduling latency,
	// it is not reliable to check the order, so just print it
	if diff > 6 {
		t.Logf("bad order of the events, want %v, got %s", seq, gotids)
	}
}

func TestBaseBackOff(t *testing.T) {
	ms := time.Millisecond
	exp := NewExpBackOffClock(ms, 20*ms, 2)
	for expect := ms; expect <= 20*ms; expect *= time.Duration(2) {
		require.Equal(t, exp.RemindNextAfter(), expect)
		exp.RemindBackOff()
	}
	exp.RemindBackOff()
	require.Equal(t, 20*ms, exp.RemindNextAfter())
}
