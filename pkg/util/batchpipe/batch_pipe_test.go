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
	time.Sleep(50 * time.Millisecond)
	require.Contains(t, collector.Received(), fmt.Sprintf("Ln %d, Col %d, Doc %d\n", 1, 12, 12))

	_ = collector.SendItem(ctx, &TestItem{name: T_INT, intval: 40})
	time.Sleep(20 * time.Millisecond)
	require.Contains(t, collector.Received(), "Batch int 105")

	_ = collector.SendItem(ctx, &TestItem{name: T_INT, intval: 40})
	handle, succ := collector.Stop(false)
	require.True(t, succ)
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

	var prev time.Time
	gap := 30 // time.Millisecond
	waitTimeCheck := func(element time.Time, closed bool) (goOn bool, err error) {
		if gap >= 300 {
			return
		}
		if prev.IsZero() {
			goOn = true
			prev = element
		} else {
			goOn = assert.InDelta(t, gap, element.Sub(prev).Milliseconds(), 30)
			prev = element
			gap *= 2
		}
		return
	}
	require.NoError(t, waitChTimeout(collector.posBufWakeupCh, waitTimeCheck, time.Second))

	require.Contains(t, collector.Received(), fmt.Sprintf("Ln %d, Col %d, Doc %d\n", 1, 12, 12))

	_ = collector.SendItem(ctx, &TestItem{name: T_POS, posval: &Pos{line: 1, linepos: 12, docpos: 12}})

	// new write will reset timer to 30ms
	time.Sleep(50 * time.Millisecond)
	require.Contains(t, collector.Received(), fmt.Sprintf("Ln %d, Col %d, Doc %d\n", 1, 12, 12))

	handle, succ := collector.Stop(false)
	require.True(t, succ)
	require.NoError(t, waitChTimeout(handle, func(element struct{}, closed bool) (goOn bool, err error) {
		assert.True(t, closed)
		return
	}, time.Second))
}

func TestBaseCollectorGracefulStop(t *testing.T) {
	var notify sync.WaitGroup
	var notifyFun = func() {
		notify.Done()
	}
	ctx := context.TODO()
	collector := newTestCollector(PipeWithBatchWorkerNum(2), PipeWithBufferWorkerNum(1))
	collector.notify4Batch = notifyFun
	collector.Start(context.TODO())

	notify.Add(1)
	err := collector.SendItem(ctx,
		&TestItem{name: T_INT, intval: 32},
		&TestItem{name: T_INT, intval: 40},
		&TestItem{name: T_INT, intval: 33},
	)
	notify.Wait()
	require.NoError(t, err)
	time.Sleep(10 * time.Millisecond)
	require.Contains(t, collector.Received(), "Batch int 105")

	notify.Add(1)
	_ = collector.SendItem(ctx, &TestItem{name: T_INT, intval: 40})
	handle, succ := collector.Stop(true)
	require.True(t, succ)
	handle2, succ2 := collector.Stop(true)
	require.False(t, succ2)
	require.Nil(t, handle2)
	notify.Add(1)
	require.Error(t, collector.SendItem(ctx, &TestItem{name: T_INT, intval: 40}))
	require.NoError(t, waitChTimeout(handle, func(element struct{}, closed bool) (goOn bool, err error) {
		assert.True(t, closed)
		return
	}, time.Second))
	require.Contains(t, collector.Received(), "Batch int 40")
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
	require.NoError(t, waitChTimeout(registry.C, checkOneRecevied, 500*ms))
	require.Error(t, waitChTimeout(registry.C, checkOneRecevied, 500*ms))

	// nothing happens after these two lines
	registry.Reset("2", 2*ms)                                            // 2 is not in the registry
	registry.Reset("1", 0*ms)                                            // 0 is ignored
	require.Error(t, waitChTimeout(registry.C, checkOneRecevied, 50*ms)) // timeout
	registry.Reset("1", 5*ms)
	require.NoError(t, waitChTimeout(registry.C, checkOneRecevied, 500*ms))
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

	// Appending to reminder.C happens in a goroutine, considering its scheduling latency,
	// here we tolerate the disorder for 2 pairs
	if diff > 4 {
		t.Errorf("bad order of the events, want %v, got %s", seq, gotids)
	}
}

func TestBaseBackOff(t *testing.T) {
	ms := time.Millisecond
	exp := NewExpBackOffClock(ms, 20*ms, 2)
	for expect := ms; expect <= 20*ms; expect *= time.Duration(2) {
		assert.Equal(t, exp.RemindNextAfter(), expect)
		exp.RemindBackOff()
	}
	exp.RemindBackOff()
	assert.Equal(t, 20*ms, exp.RemindNextAfter())
}
