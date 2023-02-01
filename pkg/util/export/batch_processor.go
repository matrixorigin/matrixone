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
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
)

const defaultQueueSize = 1310720 // queue mem cost = 10MB

// bufferHolder hold ItemBuffer content, handle buffer's new/flush/reset/reminder(base on timer) operations.
// work like:
// ---> Add ---> ShouldFlush or trigger.signal -----> StopAndGetBatch ---> FlushAndReset ---> Add ---> ...
// #     ^                   |No                |Yes, go next call
// #     |<------------------/Accept next Add
type bufferHolder struct {
	ctx context.Context
	// name like a type
	name string
	// buffer is instance of batchpipe.ItemBuffer with its own elimination algorithm(like LRU, LFU)
	buffer batchpipe.ItemBuffer[batchpipe.HasName, any]
	// signal send signal to Collector
	signal bufferSignalFunc // see awakeBufferFactory
	// impl NewItemBatchHandler
	impl motrace.PipeImpl
	// trigger handle Reminder strategy
	trigger *time.Timer

	mux sync.Mutex
}

type bufferSignalFunc func(*bufferHolder)

func newBufferHolder(ctx context.Context, name batchpipe.HasName, impl motrace.PipeImpl, signal bufferSignalFunc) *bufferHolder {
	buffer := impl.NewItemBuffer(name.GetName())
	b := &bufferHolder{
		ctx:    ctx,
		name:   name.GetName(),
		buffer: buffer,
		signal: signal,
		impl:   impl,
	}
	b.mux.Lock()
	defer b.mux.Unlock()
	b.trigger = time.AfterFunc(time.Hour, func() {})
	return b
}

// Start separated from newBufferHolder, should call only once, fix trigger started before first Add
func (b *bufferHolder) Start() {
	b.mux.Lock()
	defer b.mux.Unlock()
	reminder := b.buffer.(batchpipe.Reminder)
	b.trigger.Stop()
	b.trigger = time.AfterFunc(reminder.RemindNextAfter(), func() {
		if b.mux.TryLock() {
			b.mux.Unlock()
		}
		b.signal(b)
	})
}

// Add call buffer.Add(), while bufferHolder is NOT readonly
func (b *bufferHolder) Add(item batchpipe.HasName) {
	b.mux.Lock()
	buf := b.buffer
	buf.Add(item)
	b.mux.Unlock()
	if buf.ShouldFlush() {
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

func (b *bufferHolder) getGenerateReq() generateReq {
	b.mux.Lock()
	defer b.mux.Unlock()
	req := &bufferGenerateReq{
		buffer: b.buffer,
		b:      b,
	}
	b.buffer = b.impl.NewItemBuffer(b.name)
	b.resetTrigger()
	return req
}

// StopTrigger stop buffer's trigger(Reminder)
func (b *bufferHolder) StopTrigger() bool {
	b.mux.Lock()
	defer b.mux.Unlock()
	return b.trigger.Stop()
}

func (b *bufferHolder) resetTrigger() {
	b.trigger.Reset(b.buffer.(batchpipe.Reminder).RemindNextAfter())
}

var _ motrace.BatchProcessor = (*MOCollector)(nil)

// MOCollector handle all bufferPipe
type MOCollector struct {
	motrace.BatchProcessor
	ctx context.Context

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
		buffers:        make(map[string]*bufferHolder),
		awakeCollect:   make(chan batchpipe.HasName, defaultQueueSize),
		awakeGenerate:  make(chan generateReq, 16),
		awakeBatch:     make(chan exportReq),
		stopCh:         make(chan struct{}),
		collectorCnt:   runtime.NumCPU(),
		generatorCnt:   runtime.NumCPU(),
		exporterCnt:    runtime.NumCPU(),
		pipeImplHolder: newPipeImplHolder(),
	}
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

func (c *MOCollector) Collect(ctx context.Context, i batchpipe.HasName) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		c.awakeCollect <- i
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

	logutil.Infof("MOCollector Start")
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
	return true
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
						buf = newBufferHolder(ctx, i, impl, awakeBufferFactory(c))
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
		c.awakeGenerate <- req
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
			if exportReq, err := req.handle(buf); err != nil {
				req.callback(err)
			} else {
				select {
				case c.awakeBatch <- exportReq:
				case <-c.stopCh:
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
			if err := req.handle(); err != nil {
				req.callback(err)
			}
			//c.handleBatch(holder)
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

func (c *MOCollector) Stop(graceful bool) error {
	var err error
	var buf = new(bytes.Buffer)
	c.stopOnce.Do(func() {
		for len(c.awakeCollect) > 0 {
			logutil.Debug(fmt.Sprintf("doCollect left %d job", len(c.awakeCollect)), logutil.NoReportFiled())
			time.Sleep(250 * time.Second)
		}
		c.mux.Lock()
		for _, buffer := range c.buffers {
			_ = buffer.StopTrigger()
		}
		c.mux.Unlock()
		close(c.stopCh)
		c.stopWait.Wait()
		for _, buffer := range c.buffers {
			generate := buffer.getGenerateReq()
			if export, err := generate.handle(buf); err != nil {
				generate.callback(err)
			} else if err = export.handle(); err != nil {
				export.callback(err)
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
