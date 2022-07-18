package export

import (
	"bytes"
	"context"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"sync"
	"sync/atomic"
	"time"
)

var gPipeHolder *pipeImplHolder

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
	if _, has := h.impls[name]; has {
		return false
	} else {
		h.impls[name] = impl
		return true
	}
}

// bufferHolder
type bufferHolder struct {
	// buffer is instance of batchpipe.ItemBuffer with its own elimination algorithm(like LRU, LFU)
	buffer batchpipe.ItemBuffer[batchpipe.HasName, any]
	// signal send signal to Collector
	signal bufferSignalFunc
	// flush do real action for flush
	flush func(any)
	// trigger handle Reminder strategy
	trigger *time.Timer

	mux      sync.RWMutex
	readonly int32

	batch *any
}

type bufferSignalFunc func(*bufferHolder)

func newBufferHolder(name batchpipe.HasName, impl batchpipe.PipeImpl[batchpipe.HasName, any], signal bufferSignalFunc) *bufferHolder {
	buffer := impl.NewItemBuffer(name.GetName())
	b := &bufferHolder{
		buffer: buffer,
		signal: signal,
		flush:  impl.NewItemBatchHandler(),
	}
	reminder := buffer.(batchpipe.Reminder)
	b.trigger = time.AfterFunc(reminder.RemindNextAfter(), func() { signal(b) })
	return b
}

// Add directly call buffer.Add(), while bufferHolder is NOT readonly
func (r *bufferHolder) Add(item batchpipe.HasName) {
	r.mux.RLock()
	defer r.mux.RUnlock()
	for atomic.LoadInt32(&r.readonly) != 0 {
		time.Sleep(time.Millisecond)
	}
	r.buffer.Add(item)
}

// Stop set bufferHolder readonly, and hold Add request
func (r *bufferHolder) Stop() bool {
	if atomic.CompareAndSwapInt32(&r.readonly, 0, 1) {
		return false
	}
	return r.trigger.Stop()
}

func (r *bufferHolder) Reset() {
	atomic.CompareAndSwapInt32(&r.readonly, 1, 0)
	r.trigger.Reset(r.buffer.(batchpipe.Reminder).RemindNextAfter())
	r.batch = nil
}

var _ BatchProcessor = &MOCollector{}

// MOCollector handle all bufferPipe
type MOCollector struct {
	mux         sync.RWMutex
	buffers     map[string]*bufferHolder
	itemAwake   chan batchpipe.HasName
	bufferAwake chan *bufferHolder
	batchAwake  chan *bufferHolder
	stopping    int32
}

func newMOCollector() *MOCollector {
	c := &MOCollector{
		buffers:     make(map[string]*bufferHolder),
		itemAwake:   make(chan batchpipe.HasName, 1024),
		bufferAwake: make(chan *bufferHolder, 1024),
	}
	return c

}

func (c *MOCollector) Collect(ctx context.Context, i batchpipe.HasName) error {
	c.itemAwake <- i
	return nil
}

func (c *MOCollector) Start() bool {
	go c.loop()
	return true
}

func (c *MOCollector) Stop(graceful bool) (<-chan struct{}, bool) {
	if atomic.CompareAndSwapInt32(&c.stopping, 0, 1) {
		return nil, false
	}
	stopCh := make(chan struct{})
	close(c.bufferAwake)
	go func() {
		for _, buffer := range c.buffers {
			if graceful {
				c.flushBuffer(buffer)
			} else {
				c.flushBuffer2Disk(buffer)
			}
		}
		close(stopCh)
	}()
	return stopCh, true
}

func (c *MOCollector) loop() {

	for {
		select {
		case item := <-c.itemAwake:
			c.doCollect(item)

		case holder := <-c.bufferAwake:
			c.flushBuffer(holder)

		case batch := <-c.batchAwake:
			c.handleBatch(batch)
		}
	}
}

func (c *MOCollector) doCollect(i batchpipe.HasName) error {

	c.mux.RLock()
	defer c.mux.RUnlock()
	if _, has := c.buffers[i.GetName()]; !has {
		c.mux.Lock()
		defer c.mux.Unlock()
		if impl, has := gPipeHolder.Get(i.GetName()); !has {
			// TODO: PanicError
			panic("unknown item type")
		} else {
			c.buffers[i.GetName()] = newBufferHolder(i, impl, func(holder *bufferHolder) { c.bufferAwake <- holder })
		}
	}
	c.buffers[i.GetName()].buffer.Add(i)
	return nil
}

func (c *MOCollector) genBatch(holder *bufferHolder, buf *bytes.Buffer) {
	holder.Stop()
	batch := holder.buffer.GetBatch(buf)
	holder.batch = &batch
}

func (c *MOCollector) handleBatch(holder *bufferHolder) {
	holder.flush(*holder.batch)
	holder.Reset()
}

func (c *MOCollector) flushBuffer(holder *bufferHolder) {
	buf := new(bytes.Buffer)
	c.genBatch(holder, buf)
	c.handleBatch(holder)
}

func (c *MOCollector) flushBuffer2Disk(holder *bufferHolder) {
	// TODO:
	//holder.buffer.FlushDisk(Writer)

}

func Register(name batchpipe.HasName, impl batchpipe.PipeImpl[batchpipe.HasName, any]) {
	if ok := gPipeHolder.Put(name.GetName(), impl); !ok {
		// record double Register
	}
}

func GetGlobalBatchProcessor() BatchProcessor {
	return gBatchProcessor
}

var gBatchProcessor BatchProcessor
var ini int32

func Init() {
	if !atomic.CompareAndSwapInt32(&ini, 0, 1) {
		return
	}
	gBatchProcessor = newMOCollector()
	gPipeHolder = newPipeImplHolder()

}
