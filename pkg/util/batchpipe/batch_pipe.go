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
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

const chanCapConst = 10000

// HasName decides which table the owner will go to
type HasName interface {
	GetName() string
}

// Reminder to force flush ItemBuffer
type Reminder interface {
	RemindNextAfter() time.Duration
	RemindBackOff()
	RemindBackOffCnt() int
	RemindReset()
}

// ItemBuffer Stash items and construct a batch can be stored. for instance, an sql inserting all items into a table
type ItemBuffer[T any, B any] interface {
	Reminder
	Add(item T)
	Reset()
	IsEmpty() bool
	ShouldFlush() bool
	// GetBatch use bytes.Buffer to mitigate mem allocation and the returned bytes should own its data
	GetBatch(ctx context.Context, buf *bytes.Buffer) B
}

type PipeImpl[T any, B any] interface {
	// NewItemBuffer create a new buffer for one kind of Item
	NewItemBuffer(name string) ItemBuffer[T, B]
	// NewItemBatchHandler handle the StoreBatch from an ItemBuffer, for example, execute an insert sql.
	// this handle may be running on multiple goroutine
	NewItemBatchHandler(ctx context.Context) func(batch B)
}

type backOffClock struct {
	base       time.Duration
	current    time.Duration
	backOffCnt int
	next       func(int, time.Duration) time.Duration
}

func NewConstantClock(cycle time.Duration) Reminder {
	return NewExpBackOffClock(cycle, cycle, 1)
}

func NewExpBackOffClock(init, max time.Duration, base int) Reminder {
	next := func(_ int, current time.Duration) time.Duration {
		if current < max {
			current = current * time.Duration(base)
			if current >= max {
				current = max
			}
		}
		return current
	}
	return NewBackOffClock(init, next)
}

func NewBackOffClock(base time.Duration, f func(int, time.Duration) time.Duration) Reminder {
	return &backOffClock{
		base:       base,
		current:    base,
		backOffCnt: 0,
		next:       f,
	}
}
func (c *backOffClock) RemindNextAfter() time.Duration { return c.current }
func (c *backOffClock) RemindReset()                   { c.current = c.base; c.backOffCnt = 0 }
func (c *backOffClock) RemindBackOffCnt() int          { return c.backOffCnt }
func (c *backOffClock) RemindBackOff() {
	c.backOffCnt += 1
	if c.next != nil {
		c.current = c.next(c.backOffCnt, c.current)
	}
}

type reminderRegistry struct {
	registry map[string]*time.Timer
	C        chan string
}

func newReminderRegistry() *reminderRegistry {
	return &reminderRegistry{
		registry: make(map[string]*time.Timer),
		C:        make(chan string, chanCapConst),
	}
}

func (r *reminderRegistry) Register(name string, after time.Duration) {
	if after <= 0 {
		return
	}
	if r.registry[name] != nil {
		panic(fmt.Sprintf("%s already registered", name))
	}
	r.registry[name] = time.AfterFunc(after, func() { r.C <- name })
}

func (r *reminderRegistry) Reset(name string, after time.Duration) {
	if after <= 0 {
		return
	}
	if t := r.registry[name]; t != nil {
		t.Reset(after)
	}
}

func (r *reminderRegistry) CleanAll() {
	for _, timer := range r.registry {
		timer.Stop()
	}
}

type baseBatchPipeOpts struct {
	// the number of goroutines to handle buffer batch, default is runtime.NumCPU()
	BatchWorkerNum int
	// the number of goroutines to merge items into a buffer, default is 1
	BufferWorkerNum int
	// ItemNameFormatter
	ItemNameFormatter func(HasName) string
}

type BaseBatchPipeOpt interface {
	ApplyTo(*baseBatchPipeOpts)
}

type PipeWithBatchWorkerNum int

func (x PipeWithBatchWorkerNum) ApplyTo(o *baseBatchPipeOpts) {
	o.BatchWorkerNum = int(x)
}

type PipeWithBufferWorkerNum int

func (x PipeWithBufferWorkerNum) ApplyTo(o *baseBatchPipeOpts) {
	o.BufferWorkerNum = int(x)
}

type PipeWithItemNameFormatter func(HasName) string

func (x PipeWithItemNameFormatter) ApplyTo(o *baseBatchPipeOpts) {
	o.ItemNameFormatter = x
}

func defaultBaseBatchPipeOpts() baseBatchPipeOpts {
	return baseBatchPipeOpts{
		BatchWorkerNum:  runtime.NumCPU(),
		BufferWorkerNum: 1,
		// default use T.GetName()
		ItemNameFormatter: func(T HasName) string { return T.GetName() },
	}
}

type BaseBatchPipe[T HasName, B any] struct {
	impl              PipeImpl[T, B]
	isRunning         int32
	opts              baseBatchPipeOpts
	dropped           int64
	itemCh            chan T
	batchCh           chan B
	sendingLock       sync.RWMutex
	mergeStopWg       sync.WaitGroup
	batchStopWg       sync.WaitGroup
	batchWorkerCancel context.CancelFunc
	mergeWorkerCancel context.CancelFunc
}

func NewBaseBatchPipe[T HasName, B any](impl PipeImpl[T, B], opts ...BaseBatchPipeOpt) *BaseBatchPipe[T, B] {
	initOpts := defaultBaseBatchPipeOpts()
	for _, o := range opts {
		o.ApplyTo(&initOpts)
	}
	return &BaseBatchPipe[T, B]{
		impl:    impl,
		opts:    initOpts,
		itemCh:  make(chan T, chanCapConst),
		batchCh: make(chan B, chanCapConst),
	}
}

// SendItem returns error when pipe is closed
func (bc *BaseBatchPipe[T, B]) SendItem(ctx context.Context, items ...T) error {
	if atomic.LoadInt32(&bc.isRunning) == 0 {
		return moerr.NewWarn(ctx, "Collector has been stopped")
	}
	// avoid data race on itemCh between concurrent sending and closing
	bc.sendingLock.RLock()
	defer bc.sendingLock.RUnlock()
	for _, item := range items {
		select {
		case bc.itemCh <- item:
		default:
			atomic.AddInt64(&bc.dropped, 1)
		}
	}
	return nil
}

// Start kicks off the merge workers and batch workers, return false if the pipe is workinghas been called
func (bc *BaseBatchPipe[T, B]) Start(ctx context.Context) bool {
	if atomic.SwapInt32(&bc.isRunning, 1) == 1 {
		return false
	}
	bc.startBatchWorker(ctx)
	bc.startMergeWorker(ctx)
	return true
}

// Stop terminates all workers. If graceful asked, wait until all items are processed,
// otherwise, quit immediately. Caller can use the returned channel to wait Stop finish
func (bc *BaseBatchPipe[T, B]) Stop(graceful bool) (<-chan struct{}, bool) {
	if atomic.SwapInt32(&bc.isRunning, 0) == 0 {
		return nil, false
	}

	logutil.Infof("BaseBatchPipe accept graceful(%v) Stop", graceful)
	stopCh := make(chan struct{})
	if graceful {
		go func() {
			bc.sendingLock.Lock()
			close(bc.itemCh)
			bc.sendingLock.Unlock()
			bc.mergeStopWg.Wait()
			close(bc.batchCh)
			bc.batchStopWg.Wait()
			close(stopCh)
		}()
	} else {
		bc.batchWorkerCancel()
		bc.mergeWorkerCancel()
		go func() {
			bc.mergeStopWg.Wait()
			bc.batchStopWg.Wait()
			close(stopCh)
		}()
	}
	return stopCh, true
}

func (bc *BaseBatchPipe[T, B]) startBatchWorker(inputCtx context.Context) {
	ctx, cancel := context.WithCancel(inputCtx)
	bc.batchWorkerCancel = cancel
	for i := 0; i < bc.opts.BatchWorkerNum; i++ {
		bc.batchStopWg.Add(1)
		go bc.batchWorker(ctx)
	}
}

func (bc *BaseBatchPipe[T, B]) startMergeWorker(inputCtx context.Context) {
	ctx, cancel := context.WithCancel(inputCtx)
	bc.mergeWorkerCancel = cancel
	for i := 0; i < bc.opts.BufferWorkerNum; i++ {
		bc.mergeStopWg.Add(1)
		go bc.mergeWorker(ctx)
	}
}

func (bc *BaseBatchPipe[T, B]) batchWorker(ctx context.Context) {
	defer bc.batchStopWg.Done()
	quitMsg := quitUnknown
	defer logutil.Infof("batchWorker quit: %s", &quitMsg)
	f := bc.impl.NewItemBatchHandler(ctx)
	for {
		select {
		case <-ctx.Done():
			quitMsg = quitCancel
			return
		case batch, ok := <-bc.batchCh:
			if !ok {
				quitMsg = quitChannelClose
				return
			}
			f(batch)
		}
	}
}

type quitReason string

var (
	quitUnknown      quitReason = "unknown"
	quitCancel       quitReason = "force cancel"
	quitChannelClose quitReason = "closed channel"
)

func (s *quitReason) String() string { return string(*s) }

func (bc *BaseBatchPipe[T, B]) mergeWorker(ctx context.Context) {
	defer bc.mergeStopWg.Done()
	bufByNames := make(map[string]ItemBuffer[T, B])
	batchbuf := new(bytes.Buffer)
	registry := newReminderRegistry()
	quitMsg := quitUnknown
	defer registry.CleanAll()
	defer logutil.Infof("mergeWorker quit: %v", &quitMsg)

	doFlush := func(name string, itembuf ItemBuffer[T, B]) {
		batch := itembuf.GetBatch(ctx, batchbuf)
		bc.batchCh <- batch
		itembuf.Reset()
		itembuf.RemindReset()
		registry.Reset(name, itembuf.RemindNextAfter())
	}

	for {
		select {
		case <-ctx.Done():
			quitMsg = quitCancel
			return
		case item, ok := <-bc.itemCh:
			if !ok {
				for name, buf := range bufByNames {
					if !buf.IsEmpty() {
						doFlush(name, buf)
					}
				}
				quitMsg = quitChannelClose
				return
			}
			name := bc.opts.ItemNameFormatter(item)
			itembuf := bufByNames[name]
			if itembuf == nil {
				itembuf = bc.impl.NewItemBuffer(name)
				bufByNames[name] = itembuf
				registry.Register(name, itembuf.RemindNextAfter())
			}

			itembuf.Add(item)
			if itembuf.ShouldFlush() {
				doFlush(name, itembuf)
			} else if itembuf.RemindBackOffCnt() > 0 {
				itembuf.RemindReset()
				registry.Reset(name, itembuf.RemindNextAfter())
			}
		case name := <-registry.C:
			if itembuf := bufByNames[name]; itembuf != nil {
				if itembuf.IsEmpty() {
					itembuf.RemindBackOff()
					registry.Reset(name, itembuf.RemindNextAfter())
				} else {
					doFlush(name, itembuf)
				}
			}
		}
	}
}
