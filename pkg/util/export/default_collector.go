package export

import (
	"bytes"
	"context"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"sync"
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

// reminderHolder
type reminderHolder struct {
	reminder batchpipe.Reminder
	buffer   *bufferHolder
	trigger  *time.Timer
	C        chan *reminderHolder
}

func newReminderHolder(reminder batchpipe.Reminder) *reminderHolder {
	r := &reminderHolder{
		reminder: reminder,
		C:        make(chan *reminderHolder),
	}
	r.trigger = time.AfterFunc(reminder.RemindNextAfter(), func() { r.C <- r })
	return r
}

func (r *reminderHolder) Stop() bool {
	return r.trigger.Stop()
}

func (r *reminderHolder) Reset() {
	r.trigger.Reset(r.reminder.RemindNextAfter())
}

// bufferHolder
type bufferHolder struct {
	buf      batchpipe.ItemBuffer[batchpipe.HasName, any]
	flush    func(batch any)
	reminder *reminderHolder
}

func newBufferHolder(buf batchpipe.ItemBuffer[batchpipe.HasName, any], f func(batch any)) *bufferHolder {
	buffer := &bufferHolder{
		buf:      buf,
		flush:    f,
		reminder: newReminderHolder(buf.(batchpipe.Reminder)),
	}
	buffer.reminder.buffer = buffer
	return buffer
}

var _ BatchProcessor = &MOCollector{}

// MOCollector handle all bufferPipe
type MOCollector struct {
	mux           sync.RWMutex
	buffers       map[string]*bufferHolder
	itemAwake     chan batchpipe.HasName
	reminderAwake chan *reminderHolder
	bufferAwake   chan *bufferHolder
}

func newMOCollector() *MOCollector {
	c := &MOCollector{
		buffers:       make(map[string]*bufferHolder),
		itemAwake:     make(chan batchpipe.HasName, 1024),
		reminderAwake: make(chan *reminderHolder, 1024),
		bufferAwake:   make(chan *bufferHolder, 1024),
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
	//TODO implement me
	close(c.reminderAwake)
	close(c.bufferAwake)
	for _, buffer := range c.buffers {
		if graceful {
			c.flushBuffer(buffer)
		} else {
			c.flushBuffer2Disk(buffer)
		}
	}
	panic("implement me")
}

func (c *MOCollector) loop() {

	for {
		select {
		case item := <-c.itemAwake:
			c.doCollect(item)
		case reminder := <-c.reminderAwake:
			c.flushBuffer(reminder.buffer)
		case holder := <-c.bufferAwake:
			c.flushBuffer(holder)
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
			c.buffers[i.GetName()] = newBufferHolder(impl.NewItemBuffer(i.GetName()), impl.NewItemBatchHandler())
		}
	}
	c.buffers[i.GetName()].buf.Add(i)
	return nil
}

func (c *MOCollector) flushBuffer(holder *bufferHolder) {
	buf := new(bytes.Buffer)
	holder.reminder.Stop()
	_, span := trace.Start(trace.DefaultContext(), "GenBatch")
	batch := holder.buf.GetBatch(buf)
	span.End()

	_, span = trace.Start(trace.DefaultContext(), "BatchHandle")
	holder.flush(batch)
	span.End()
	holder.reminder.Reset()
}

func (c *MOCollector) flushBuffer2Disk(holder *bufferHolder) {
	// TODO:
	//holder.buf.FlushDisk(Writer)

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

func Init() {
	gBatchProcessor = newMOCollector()
	gPipeHolder = newPipeImplHolder()

}
