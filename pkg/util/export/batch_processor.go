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

package export

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	morun "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"

	"go.uber.org/zap"
)

const defaultQueueSize = 1310720 // queue mem cost = 10MB

const LoggerNameMOCollector = "MOCollector"

// bufferHolder hold ItemBuffer content, handle buffer's new/flush/reset/reminder(base on timer) operations.
// work like:
// ---> Add ---> ShouldFlush or trigger.signal -----> StopAndGetBatch ---> FlushAndReset ---> Add ---> ...
// #     ^                   |No                |Yes, go next call
// #     |<------------------/Accept next Add
type bufferHolder struct {
	c   *MOCollector
	ctx context.Context
	// name like a type
	name string
	// buffer is instance of batchpipe.ItemBuffer with its own elimination algorithm(like LRU, LFU)
	buffer batchpipe.ItemBuffer[batchpipe.HasName, any]
	// bufferPool
	bufferPool *sync.Pool
	bufferCnt  atomic.Int32
	discardCnt atomic.Int32
	// reminder
	reminder batchpipe.Reminder
	// signal send signal to Collector
	signal bufferSignalFunc // see awakeBufferFactory
	// impl NewItemBatchHandler
	impl motrace.PipeImpl
	// trigger handle Reminder strategy
	trigger *time.Timer

	mux sync.Mutex
	// stopped mark
	stopped bool
}

type bufferSignalFunc func(*bufferHolder)

func newBufferHolder(ctx context.Context, name batchpipe.HasName, impl motrace.PipeImpl, signal bufferSignalFunc, c *MOCollector) *bufferHolder {
	b := &bufferHolder{
		c:      c,
		ctx:    ctx,
		name:   name.GetName(),
		signal: signal,
		impl:   impl,
	}
	b.bufferPool = &sync.Pool{}
	b.bufferCnt.Swap(0)
	b.bufferPool.New = func() interface{} {
		return b.impl.NewItemBuffer(b.name)
	}
	b.buffer = b.getBuffer()
	b.reminder = b.buffer.(batchpipe.Reminder)
	b.mux.Lock()
	defer b.mux.Unlock()
	b.trigger = time.AfterFunc(time.Hour, func() {})
	return b
}

// Start separated from newBufferHolder, should call only once, fix trigger started before first Add
func (b *bufferHolder) Start() {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.stopped = false
	b.trigger.Stop()
	b.trigger = time.AfterFunc(b.reminder.RemindNextAfter(), func() {
		if b.mux.TryLock() {
			b.mux.Unlock()
		}
		b.signal(b)
	})
}

func (b *bufferHolder) getBuffer() batchpipe.ItemBuffer[batchpipe.HasName, any] {
	b.c.allocBuffer()
	b.bufferCnt.Add(1)
	buffer := b.bufferPool.Get().(batchpipe.ItemBuffer[batchpipe.HasName, any])
	logutil.Debugf("new buffer for %s, cnt: %d, using: %d", b.name, b.bufferCnt.Load(), b.c.bufferTotal.Load())
	return buffer
}

func (b *bufferHolder) putBuffer(buffer batchpipe.ItemBuffer[batchpipe.HasName, any]) {
	buffer.Reset()
	b.bufferPool.Put(buffer)
	b.bufferCnt.Add(-1)
	b.c.releaseBuffer()
	logutil.Debugf("release buffer for %s, cnt: %d, using: %d", b.name, b.bufferCnt.Load(), b.c.bufferTotal.Load())
}

func (b *bufferHolder) discardBuffer(buffer batchpipe.ItemBuffer[batchpipe.HasName, any]) {
	b.discardCnt.Add(1)
	b.putBuffer(buffer)
}

// Add call buffer.Add(), while bufferHolder is NOT readonly
func (b *bufferHolder) Add(item batchpipe.HasName) {
	b.mux.Lock()
	if b.stopped {
		b.mux.Unlock()
		return
	}
	if b.buffer == nil {
		b.buffer = b.getBuffer()
	}
	buf := b.buffer
	buf.Add(item)
	b.mux.Unlock()
	if buf.ShouldFlush() {
		b.signal(b)
	} else if checker, is := item.(table.NeedSyncWrite); is && checker.NeedSyncWrite() {
		b.signal(b)
	}
}

var _ generateReq = (*bufferGenerateReq)(nil)

type bufferGenerateReq struct {
	buffer batchpipe.ItemBuffer[batchpipe.HasName, any]
	// impl NewItemBatchHandler
	b *bufferHolder
}

func (r *bufferGenerateReq) handle(buf *bytes.Buffer) (exportReq, error) {
	batch := r.buffer.GetBatch(r.b.ctx, buf)
	r.b.putBuffer(r.buffer)
	return &bufferExportReq{
		batch: batch,
		b:     r.b,
	}, nil
}

func (r *bufferGenerateReq) callback(err error) {}

var _ exportReq = (*bufferExportReq)(nil)

type bufferExportReq struct {
	batch any
	b     *bufferHolder
}

func (r *bufferExportReq) handle() error {
	if r.batch != nil {
		var flush = r.b.impl.NewItemBatchHandler(context.Background())
		flush(r.batch)
	} else {
		logutil.Debugf("batch is nil, item: %s", r.b.name)
	}
	return nil
}

func (r *bufferExportReq) callback(err error) {}

// getGenerateReq get req to do generate logic
// return nil, if b.buffer is nil
func (b *bufferHolder) getGenerateReq() generateReq {
	b.mux.Lock()
	defer b.mux.Unlock()
	defer b.resetTrigger()
	if b.buffer == nil || b.buffer.IsEmpty() {
		return nil
	}
	req := &bufferGenerateReq{
		buffer: b.buffer,
		b:      b,
	}
	b.buffer = nil
	return req
}

func (b *bufferHolder) resetTrigger() {
	if b.stopped {
		return
	}
	b.trigger.Reset(b.reminder.RemindNextAfter())
}

func (b *bufferHolder) Stop() {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.stopped = true
	b.trigger.Stop()
}

var _ motrace.BatchProcessor = (*MOCollector)(nil)

// MOCollector handle all bufferPipe
type MOCollector struct {
	motrace.BatchProcessor
	ctx    context.Context
	logger *log.MOLogger

	// mux control all changes on buffers
	mux sync.RWMutex
	// buffers maintain working buffer for each type
	buffers map[string]*bufferHolder
	// awakeCollect handle collect signal
	awakeCollect chan batchpipe.HasName
	// awakeGenerate handle generate signal
	awakeGenerate chan generateReq
	// awakeBatch handle export signal
	awakeBatch chan exportReq

	collectorCnt int // WithCollectorCnt
	generatorCnt int // WithGeneratorCnt
	exporterCnt  int // WithExporterCnt
	// pipeImplHolder hold implement
	pipeImplHolder *PipeImplHolder

	statsInterval time.Duration // WithStatsInterval

	maxBufferCnt int32 // cooperate with bufferCond
	bufferTotal  atomic.Int32
	bufferMux    sync.Mutex
	bufferCond   *sync.Cond

	// flow control
	started  uint32
	stopOnce sync.Once
	stopWait sync.WaitGroup
	stopCh   chan struct{}
}

type MOCollectorOption func(*MOCollector)

func NewMOCollector(ctx context.Context, opts ...MOCollectorOption) *MOCollector {
	c := &MOCollector{
		ctx:            ctx,
		logger:         morun.ProcessLevelRuntime().Logger().Named(LoggerNameMOCollector),
		buffers:        make(map[string]*bufferHolder),
		awakeCollect:   make(chan batchpipe.HasName, defaultQueueSize),
		awakeGenerate:  make(chan generateReq, 16),
		awakeBatch:     make(chan exportReq),
		stopCh:         make(chan struct{}),
		collectorCnt:   runtime.NumCPU(),
		generatorCnt:   runtime.NumCPU(),
		exporterCnt:    runtime.NumCPU(),
		pipeImplHolder: newPipeImplHolder(),
		statsInterval:  time.Minute,
		maxBufferCnt:   int32(runtime.NumCPU()),
	}
	c.bufferCond = sync.NewCond(&c.bufferMux)
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func WithCollectorCnt(cnt int) MOCollectorOption {
	return MOCollectorOption(func(c *MOCollector) { c.collectorCnt = cnt })
}
func WithGeneratorCnt(cnt int) MOCollectorOption {
	return MOCollectorOption(func(c *MOCollector) { c.generatorCnt = cnt })
}
func WithExporterCnt(cnt int) MOCollectorOption {
	return MOCollectorOption(func(c *MOCollector) { c.exporterCnt = cnt })
}

func WithOBCollectorConfig(cfg *config.OBCollectorConfig) MOCollectorOption {
	return MOCollectorOption(func(c *MOCollector) {
		c.statsInterval = cfg.ShowStatsInterval.Duration
		c.maxBufferCnt = cfg.BufferCnt
		if c.maxBufferCnt == -1 {
			c.maxBufferCnt = math.MaxInt32
		} else if c.maxBufferCnt == 0 {
			c.maxBufferCnt = int32(runtime.NumCPU())
		}
	})
}

func (c *MOCollector) initCnt() {
	if c.collectorCnt <= 0 {
		c.collectorCnt = c.pipeImplHolder.Size() * 2
	}
	if c.generatorCnt <= 0 {
		c.generatorCnt = c.pipeImplHolder.Size()
	}
	if c.exporterCnt <= 0 {
		c.exporterCnt = c.pipeImplHolder.Size()
	}
}

func (c *MOCollector) Register(name batchpipe.HasName, impl motrace.PipeImpl) {
	_ = c.pipeImplHolder.Put(name.GetName(), impl)
}

// Collect item in chan, if collector is stopped then return error
func (c *MOCollector) Collect(ctx context.Context, item batchpipe.HasName) error {
	select {
	case <-c.stopCh:
		ctx = errutil.ContextWithNoReport(ctx, true)
		return moerr.NewInternalError(ctx, "MOCollector stopped")
	case c.awakeCollect <- item:
		return nil
	}
}

func (c *MOCollector) DiscardableCollect(ctx context.Context, item batchpipe.HasName) error {
	select {
	case <-c.stopCh:
		ctx = errutil.ContextWithNoReport(ctx, true)
		return moerr.NewInternalError(ctx, "MOCollector stopped")
	case c.awakeCollect <- item:
		return nil
	case <-time.After(time.Millisecond):
		return nil
	}
}

// Start all goroutine worker, including collector, generator, and exporter
func (c *MOCollector) Start() bool {
	if atomic.LoadUint32(&c.started) != 0 {
		return false
	}
	c.mux.Lock()
	defer c.mux.Unlock()
	if c.started != 0 {
		return false
	}
	defer atomic.StoreUint32(&c.started, 1)

	c.initCnt()

	logutil.Info("MOCollector Start")
	for i := 0; i < c.collectorCnt; i++ {
		c.stopWait.Add(1)
		go c.doCollect(i)
	}
	for i := 0; i < c.generatorCnt; i++ {
		c.stopWait.Add(1)
		go c.doGenerate(i)
	}
	for i := 0; i < c.exporterCnt; i++ {
		c.stopWait.Add(1)
		go c.doExport(i)
	}
	c.stopWait.Add(1)
	go c.showStats()
	return true
}

func (c *MOCollector) allocBuffer() {
	c.bufferCond.L.Lock()
	for c.bufferTotal.Load() == c.maxBufferCnt {
		c.bufferCond.Wait()
	}
	c.bufferTotal.Add(1)
	c.bufferCond.L.Unlock()
}

func (c *MOCollector) releaseBuffer() {
	c.bufferCond.L.Lock()
	c.bufferTotal.Add(-1)
	c.bufferCond.Signal()
	c.bufferCond.L.Unlock()
}

// doCollect handle all item accept work, send it to the corresponding buffer
// goroutine worker
func (c *MOCollector) doCollect(idx int) {
	defer c.stopWait.Done()
	ctx, span := trace.Start(c.ctx, "MOCollector.doCollect")
	defer span.End()
	logutil.Debugf("doCollect %dth: start", idx)
loop:
	for {
		select {
		case i := <-c.awakeCollect:
			c.mux.RLock()
			if buf, has := c.buffers[i.GetName()]; !has {
				logutil.Debugf("doCollect %dth: init buffer for %s", idx, i.GetName())
				c.mux.RUnlock()
				c.mux.Lock()
				if _, has := c.buffers[i.GetName()]; !has {
					logutil.Debugf("doCollect %dth: init buffer done.", idx)
					if impl, has := c.pipeImplHolder.Get(i.GetName()); !has {
						panic(moerr.NewInternalError(ctx, "unknown item type: %s", i.GetName()))
					} else {
						buf = newBufferHolder(ctx, i, impl, awakeBufferFactory(c), c)
						c.buffers[i.GetName()] = buf
						buf.Add(i)
						buf.Start()
					}
				}
				c.mux.Unlock()
			} else {
				buf.Add(i)
				c.mux.RUnlock()
			}
		case <-c.stopCh:
			break loop
		}
	}
	logutil.Debugf("doCollect %dth: Done.", idx)
}

type generateReq interface {
	handle(*bytes.Buffer) (exportReq, error)
	callback(error)
}

type exportReq interface {
	handle() error
	callback(error)
}

// awakeBufferFactory frozen buffer, send GenRequest to awake
var awakeBufferFactory = func(c *MOCollector) func(holder *bufferHolder) {
	return func(holder *bufferHolder) {
		req := holder.getGenerateReq()
		if req == nil {
			return
		}
		if holder.name != motrace.RawLogTbl {
			select {
			case c.awakeGenerate <- req:
			case <-time.After(time.Second * 3):
				logutil.Warn("awakeBufferFactory: timeout after 3 seconds")
				goto discardL
			}
		} else {
			select {
			case c.awakeGenerate <- req:
			default:
				logutil.Warn("awakeBufferFactory: awakeGenerate chan is full")
				goto discardL
			}
		}
		return
	discardL:
		if r, ok := req.(*bufferGenerateReq); ok {
			r.b.discardBuffer(r.buffer)
		}
	}
}

// doGenerate handle buffer gen BatchRequest, which could be anything
// goroutine worker
func (c *MOCollector) doGenerate(idx int) {
	defer c.stopWait.Done()
	var buf = new(bytes.Buffer)
	logutil.Debugf("doGenerate %dth: start", idx)
loop:
	for {
		select {
		case req := <-c.awakeGenerate:
			if req == nil {
				logutil.Warn("generate req is nil")
			} else if exportReq, err := req.handle(buf); err != nil {
				req.callback(err)
			} else {
				select {
				case c.awakeBatch <- exportReq:
				case <-c.stopCh:
				case <-time.After(time.Second * 10):
					logutil.Info("awakeBatch: timeout after 10 seconds")
				}
			}
		case <-c.stopCh:
			break loop
		}
	}
	logutil.Debugf("doGenerate %dth: Done.", idx)
}

// doExport handle BatchRequest
func (c *MOCollector) doExport(idx int) {
	defer c.stopWait.Done()
	logutil.Debugf("doExport %dth: start", idx)
loop:
	for {
		select {
		case req := <-c.awakeBatch:
			if req == nil {
				logutil.Warn("export req is nil")
			} else if err := req.handle(); err != nil {
				req.callback(err)
			}
		case <-c.stopCh:
			c.mux.Lock()
			for len(c.awakeBatch) > 0 {
				<-c.awakeBatch
			}
			c.mux.Unlock()
			break loop
		}
	}
	logutil.Debugf("doExport %dth: Done.", idx)
}

func (c *MOCollector) showStats() {
	defer c.stopWait.Done()
	c.logger.Debug("start showStats")

loop:
	for {
		select {
		case <-time.After(c.statsInterval):
			fields := make([]zap.Field, 0, 16)
			fields = append(fields, zap.Int32("MaxBufferCnt", c.maxBufferCnt))
			fields = append(fields, zap.Int32("TotalBufferCnt", c.bufferTotal.Load()))
			fields = append(fields, zap.Int("QueueLength", len(c.awakeCollect)))
			for _, b := range c.buffers {
				fields = append(fields, zap.Int32(fmt.Sprintf("%sBufferCnt", b.name), b.bufferCnt.Load()))
				fields = append(fields, zap.Int32(fmt.Sprintf("%sDiscardCnt", b.name), b.discardCnt.Load()))
			}
			c.logger.Info("stats", fields...)
		case <-c.stopCh:
			break loop
		}
	}
	c.logger.Debug("showStats Done.")
}

func (c *MOCollector) Stop(graceful bool) error {
	var err error
	var buf = new(bytes.Buffer)
	c.stopOnce.Do(func() {
		for len(c.awakeCollect) > 0 && graceful {
			c.logger.Debug(fmt.Sprintf("doCollect left %d job", len(c.awakeCollect)))
			time.Sleep(250 * time.Millisecond)
		}
		c.mux.Lock()
		for _, buffer := range c.buffers {
			buffer.Stop()
		}
		c.mux.Unlock()
		close(c.stopCh)
		c.stopWait.Wait()
		close(c.awakeGenerate)
		close(c.awakeBatch)
		if !graceful {
			// shutdown directly
			return
		}
		// handle remain data
		handleExport := func(req exportReq) {
			if err = req.handle(); err != nil {
				req.callback(err)
			}
		}
		handleGen := func(req generateReq) {
			if export, err := req.handle(buf); err != nil {
				req.callback(err)
			} else {
				handleExport(export)
			}
		}
		for req := range c.awakeBatch {
			handleExport(req)
		}
		for req := range c.awakeGenerate {
			handleGen(req)
		}
		for _, buffer := range c.buffers {
			if generate := buffer.getGenerateReq(); generate != nil {
				handleGen(generate)
			}
		}
	})
	return err
}

type PipeImplHolder struct {
	mux   sync.RWMutex
	impls map[string]motrace.PipeImpl
}

func newPipeImplHolder() *PipeImplHolder {
	return &PipeImplHolder{
		impls: make(map[string]motrace.PipeImpl),
	}
}

func (h *PipeImplHolder) Get(name string) (motrace.PipeImpl, bool) {
	h.mux.RLock()
	defer h.mux.RUnlock()
	impl, has := h.impls[name]
	return impl, has
}

func (h *PipeImplHolder) Put(name string, impl motrace.PipeImpl) bool {
	h.mux.Lock()
	defer h.mux.Unlock()
	_, has := h.impls[name]
	h.impls[name] = impl
	return has
}

func (h *PipeImplHolder) Size() int {
	h.mux.Lock()
	defer h.mux.Unlock()
	return len(h.impls)
}
