package export

import (
	"bytes"
	"context"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/logutil/logutil2"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"sync"
	"sync/atomic"
	"time"
)

// bufferHolder
type bufferHolder struct {
	// name like a type
	name string
	// buffer is instance of batchpipe.ItemBuffer with its own elimination algorithm(like LRU, LFU)
	buffer batchpipe.ItemBuffer[batchpipe.HasName, any]
	// signal send signal to Collector
	signal bufferSignalFunc
	// flush do real action for flush
	flush func(any)
	impl  batchpipe.PipeImpl[batchpipe.HasName, any]
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
		flush:  impl.NewItemBatchHandler(),
		impl:   impl,
	}
	reminder := buffer.(batchpipe.Reminder)
	b.trigger = time.AfterFunc(reminder.RemindNextAfter(), func() {
		logutil.Debugf("buffer %s trigger time.", name.GetName())
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
	defer r.mux.RUnlock()
	for atomic.LoadInt32(&r.readonly) == READONLY {
		time.Sleep(time.Millisecond)
	}
	r.buffer.Add(item)
	if r.buffer.ShouldFlush() {
		r.signal(r)
	}
}

// Stop set bufferHolder readonly, and hold Add request
func (r *bufferHolder) Stop() bool {
	if !atomic.CompareAndSwapInt32(&r.readonly, READWRITE, READONLY) {
		return false
	}
	return r.trigger.Stop()
}

func (r *bufferHolder) Reset() {
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
	logutil.Debugf("Collect %s", i.GetName())
	c.itemAwake <- i
	return nil
}

func (c *MOCollector) Start() bool {
	c.startOnce.Do(func() {
		logutil2.Infof(nil, "MOCollector Start")
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

func (c *MOCollector) Stop(graceful bool) error {
	var err error
	c.stopOnce.Do(func() {
		doneC := make(chan struct{})
		close(c.stopCh)
		for _, buffer := range c.buffers {
			if graceful {
				c.flushBuffer(buffer)
			} else {
				c.flushBuffer2Disk(buffer)
			}
		}
		close(doneC)
	})
	return err
}

// doCollect handle all item accept work, send it to the corresponding buffer
// goroutine worker
func (c *MOCollector) doCollect(idx int) {
	logutil.Debugf("doCollect %dth: start", idx)
	defer func() {
		if err := recover(); err != nil {
			logutil.Errorf("doCollect %dth: panic %v", err)
		}
	}()
	for {
		select {
		case i := <-c.itemAwake:
			logutil.Debugf("doCollect %dth: accept item %s", idx, i.GetName())
			if _, has := c.buffers[i.GetName()]; !has {
				logutil.Debugf("doCollect %dth: init", idx)
				c.mux.Lock()
				if _, has := c.buffers[i.GetName()]; !has {
					logutil.Debugf("doCollect %dth: init done.", idx)
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
			break
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
	logutil.Debugf("doGenerate %dth: start", idx)
	buf := new(bytes.Buffer)
	for {
		select {
		case holder := <-c.bufferAwake:
			logutil.Debugf("doGenerate %dth: before genBatch", idx)
			c.genBatch(holder, buf)
			c.batchAwake <- holder
			logutil.Debugf("doGenerate %dth: after send batchAwake", idx)
		case <-c.stopCh:
			break
		}
	}
	logutil.Debugf("doGenerate %dth: Done.", idx)
}

// doExport handle BatchRequest
func (c *MOCollector) doExport(idx int) {
	logutil.Debugf("doExport %dth: start", idx)
	for {
		select {
		case holder := <-c.batchAwake:
			logutil.Debugf("doExport %dth: handleBatch", idx)
			c.handleBatch(holder)
		case <-c.stopCh:
			break
		}
	}
	logutil.Debugf("doExport %dth: Done.", idx)
}

func (c *MOCollector) genBatch(holder *bufferHolder, buf *bytes.Buffer) {
	holder.Stop()
	batch := holder.buffer.GetBatch(buf)
	holder.batch = &batch
}

func (c *MOCollector) handleBatch(holder *bufferHolder) {
	var flush = holder.impl.NewItemBatchHandler()
	flush(*holder.batch)
	holder.Reset()
}

func (c *MOCollector) flushBuffer(holder *bufferHolder) error {
	buf := new(bytes.Buffer)
	c.genBatch(holder, buf)
	c.handleBatch(holder)
	return nil
}

func (c *MOCollector) flushBuffer2Disk(holder *bufferHolder) error {
	// TODO:
	//holder.buffer.FlushDisk(Writer)

	return nil
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

type BatchRequest struct {
	content any
}
