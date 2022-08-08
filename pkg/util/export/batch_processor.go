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
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/logutil/logutil2"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/errors"
)

// bufferHolder
type bufferHolder struct {
	// name like a type
	name string
	// buffer is instance of batchpipe.ItemBuffer with its own elimination algorithm(like LRU, LFU)
	buffer batchpipe.ItemBuffer[batchpipe.HasName, any]
	// signal send signal to Collector
	signal bufferSignalFunc
	// impl NewItemBatchHandler
	impl batchpipe.PipeImpl[batchpipe.HasName, any]
	// trigger handle Reminder strategy
	trigger *time.Timer

	mux      sync.RWMutex
	readonly int32

	batch *any
}

const READWRITE = 0
const READONLY = 1

type bufferSignalFunc func(*bufferHolder)

func newBufferHolder(name batchpipe.HasName, impl batchpipe.PipeImpl[batchpipe.HasName, any], signal bufferSignalFunc) *bufferHolder {
	buffer := impl.NewItemBuffer(name.GetName())
	b := &bufferHolder{
		name:   name.GetName(),
		buffer: buffer,
		signal: signal,
		impl:   impl,
	}
	reminder := buffer.(batchpipe.Reminder)
	b.trigger = time.AfterFunc(reminder.RemindNextAfter(), func() {
		if atomic.LoadInt32(&b.readonly) == READONLY {
			logutil.Debugf("buffer %s trigger time, pass.", name.GetName())
			return
		}
		b.signal(b)
	})
	return b
}

// Add directly call buffer.Add(), while bufferHolder is NOT readonly
func (r *bufferHolder) Add(item batchpipe.HasName) {
	r.mux.RLock()
	for atomic.LoadInt32(&r.readonly) == READONLY {
		r.mux.RUnlock()
		time.Sleep(time.Millisecond)
		r.mux.RLock()
	}
	defer r.mux.RUnlock()
	r.buffer.Add(item)
	if r.buffer.ShouldFlush() {
		r.signal(r)
	}
}

// Stop set bufferHolder readonly, and hold Add request
func (r *bufferHolder) Stop() bool {
	r.mux.RLock()
	defer r.mux.RUnlock()
	if !atomic.CompareAndSwapInt32(&r.readonly, READWRITE, READONLY) {
		return false
	}
	return r.trigger.Stop()
}

func (r *bufferHolder) GetBatch(buf *bytes.Buffer) (any, bool) {
	r.mux.Lock()
	defer r.mux.Unlock()
	if atomic.LoadInt32(&r.readonly) != READONLY {
		return nil, false
	}
	return r.buffer.GetBatch(buf), true
}

func (r *bufferHolder) Flush() bool {
	r.mux.Lock()
	defer r.mux.Unlock()
	if atomic.LoadInt32(&r.readonly) != READONLY {
		return false
	}
	var flush = r.impl.NewItemBatchHandler()
	flush(*r.batch)
	return true
}

func (r *bufferHolder) Reset() {
	r.mux.Lock()
	defer r.mux.Unlock()
	atomic.CompareAndSwapInt32(&r.readonly, READONLY, READWRITE)
	r.trigger.Reset(r.buffer.(batchpipe.Reminder).RemindNextAfter())
	r.buffer.Reset()
	r.batch = nil
}

var _ BatchProcessor = &MOCollector{}

// MOCollector handle all bufferPipe
type MOCollector struct {
	// mux control all changes on buffers
	mux sync.RWMutex
	// buffers maintain working buffer for each type
	buffers map[string]*bufferHolder
	// itemAwake handle collect signal
	itemAwake chan batchpipe.HasName
	// bufferAwake handle generate signal
	bufferAwake chan *bufferHolder
	// batchAwake handle export signal
	batchAwake chan *bufferHolder

	collectorCnt int // see WithCollectorCnt
	generatorCnt int // see WithGeneratorCnt
	exporterCnt  int // see WithExporterCnt

	// flow control
	startOnce sync.Once
	stopOnce  sync.Once
	stopWait  sync.WaitGroup
	stopCh    chan struct{}
}

func NewMOCollector() *MOCollector {
	c := &MOCollector{
		buffers:      make(map[string]*bufferHolder),
		itemAwake:    make(chan batchpipe.HasName, 12800),
		bufferAwake:  make(chan *bufferHolder, 16),
		batchAwake:   make(chan *bufferHolder),
		stopCh:       make(chan struct{}),
		collectorCnt: 2 * gPipeImplHolder.Size(),
		generatorCnt: gPipeImplHolder.Size(),
		exporterCnt:  gPipeImplHolder.Size(),
	}
	return c

}

func (c *MOCollector) Collect(ctx context.Context, i batchpipe.HasName) error {
	c.itemAwake <- i
	return nil
}

func (c *MOCollector) Start() bool {
	c.startOnce.Do(func() {
		logutil2.Infof(DefaultContext(), "MOCollector Start")
		for i := 0; i < c.collectorCnt; i++ {
			go c.doCollect(i)
		}
		for i := 0; i < c.generatorCnt; i++ {
			go c.doGenerate(i)
		}
		for i := 0; i < c.exporterCnt; i++ {
			go c.doExport(i)
		}
	})
	return true
}

// doCollect handle all item accept work, send it to the corresponding buffer
// goroutine worker
func (c *MOCollector) doCollect(idx int) {
	c.stopWait.Add(1)
	defer errors.Recover(DefaultContext())
	defer c.stopWait.Done()
	logutil.Debugf("doCollect %dth: start", idx)
loop:
	for {
		select {
		case i := <-c.itemAwake:
			if _, has := c.buffers[i.GetName()]; !has {
				logutil.Debugf("doCollect %dth: init buffer for %s", idx, i.GetName())
				c.mux.Lock()
				if _, has := c.buffers[i.GetName()]; !has {
					logutil.Debugf("doCollect %dth: init buffer done.", idx)
					if impl, has := gPipeImplHolder.Get(i.GetName()); !has {
						// TODO: PanicError
						panic("unknown item type")
					} else {
						c.buffers[i.GetName()] = newBufferHolder(i, impl, awakeBuffer(c))
					}
				}
				c.mux.Unlock()
			}
			c.mux.RLock()
			c.buffers[i.GetName()].buffer.Add(i)
			c.mux.RUnlock()
		case <-c.stopCh:
			break loop
		}
	}
	logutil.Debugf("doCollect %dth: Done.", idx)
}

func awakeBuffer(c *MOCollector) func(holder *bufferHolder) {
	return func(holder *bufferHolder) {
		c.bufferAwake <- holder
	}
}

// doGenerate handle buffer gen BatchRequest, which could be anything
// goroutine worker
func (c *MOCollector) doGenerate(idx int) {
	var buf = new(bytes.Buffer)
	c.stopWait.Add(1)
	defer errors.Recover(DefaultContext())
	defer c.stopWait.Done()
	logutil.Debugf("doGenerate %dth: start", idx)
loop:
	for {
		select {
		case holder := <-c.bufferAwake:
			c.genBatch(holder, buf)
			c.batchAwake <- holder
		case <-c.stopCh:
			break loop
		}
	}
	logutil.Debugf("doGenerate %dth: Done.", idx)
}

// doExport handle BatchRequest
func (c *MOCollector) doExport(idx int) {
	c.stopWait.Add(1)
	defer c.stopWait.Done()
	defer errors.Recover(DefaultContext())
	logutil.Debugf("doExport %dth: start", idx)
loop:
	for {
		select {
		case holder := <-c.batchAwake:
			c.handleBatch(holder)
		case <-c.stopCh:
			break loop
		}
	}
	logutil.Debugf("doExport %dth: Done.", idx)
}

func (c *MOCollector) genBatch(holder *bufferHolder, buf *bytes.Buffer) {
	holder.Stop()
	if batch, ok := holder.GetBatch(buf); ok {
		holder.batch = &batch
	}
}

func (c *MOCollector) handleBatch(holder *bufferHolder) {
	holder.Flush()
	holder.Reset()
}

func (c *MOCollector) Stop(graceful bool) error {
	var err error
	var buf = new(bytes.Buffer)
	c.stopOnce.Do(func() {
		for len(c.itemAwake) > 0 {
			logutil.Debugf("doCollect left %d job", len(c.itemAwake))
			time.Sleep(250 * time.Second)
		}
		close(c.stopCh)
		c.stopWait.Wait()
		for _, buffer := range c.buffers {
			c.genBatch(buffer, buf)
			c.handleBatch(buffer)
		}
	})
	return err
}

/*
// ForceFlush exports all ended spans that have not yet been exported.
func (bsp *batchSpanProcessor) ForceFlush(ctx context.Context) error {
	var err error
	if bsp.e != nil {
		flushCh := make(chan struct{})
		if bsp.enqueueBlockOnQueueFull(ctx, forceFlushSpan{flushed: flushCh}) {
			select {
			case <-flushCh:
				// Processed any items in queue prior to ForceFlush being called
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		wait := make(chan error)
		go func() {
			wait <- bsp.exportSpans(ctx)
			close(wait)
		}()
		// Wait until the export is finished or the context is cancelled/timed out
		select {
		case err = <-wait:
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	return err
}

*/

var _ BatchProcessor = &noopBatchProcessor{}

type noopBatchProcessor struct {
}

func (n noopBatchProcessor) Collect(context.Context, batchpipe.HasName) error { return nil }
func (n noopBatchProcessor) Start() bool                                      { return true }
func (n noopBatchProcessor) Stop(bool) error                                  { return nil }

var gPipeImplHolder *pipeImplHolder = newPipeImplHolder()

// pipeImplHolder
type pipeImplHolder struct {
	mux   sync.RWMutex
	impls map[string]batchpipe.PipeImpl[batchpipe.HasName, any]
}

func newPipeImplHolder() *pipeImplHolder {
	return &pipeImplHolder{
		impls: make(map[string]batchpipe.PipeImpl[batchpipe.HasName, any]),
	}
}

func (h *pipeImplHolder) Get(name string) (batchpipe.PipeImpl[batchpipe.HasName, any], bool) {
	h.mux.RLock()
	defer h.mux.RUnlock()
	impl, has := h.impls[name]
	return impl, has
}

func (h *pipeImplHolder) Put(name string, impl batchpipe.PipeImpl[batchpipe.HasName, any]) bool {
	h.mux.Lock()
	defer h.mux.Unlock()
	_, has := h.impls[name]
	h.impls[name] = impl
	return has
}

func (h *pipeImplHolder) Size() int {
	return len(h.impls)
}
