package trace

import (
	"bytes"
	"sync"
	"time"

	bp "github.com/matrixorigin/matrixone/pkg/util/batchpipe"
)

type HasItemSize interface {
	bp.HasName
	Size() int64
}

var _ bp.PipeImpl[HasItemSize, string] = &traceBufferPipeWorker{}

type traceBufferPipeWorker struct {
}

func NewTraceBufferPipeWorker(opt ...buffer2SqlOption) bp.PipeImpl[HasItemSize, string] {
	return &traceBufferPipeWorker{}
}

// NewItemBuffer implement batchpipe.PipeImpl
func (t traceBufferPipeWorker) NewItemBuffer(name string) bp.ItemBuffer[HasItemSize, string] {
	return newBuffer2Sql(withSizeThreshold(1 << 20) /*1MB*/)
}

// NewItemBatchHandler implement batchpipe.PipeImpl
func (t traceBufferPipeWorker) NewItemBatchHandler() func(batch string) {
	/*exec := c.ieFactory()
	exec.ApplySessionOverride(ie.NewOptsBuilder().Database(metricDBConst).Internal(true).Finish())
	return func(batch string) {
		if err := exec.Exec(batch, ie.NewOptsBuilder().Finish()); err != nil {
			logutil.Errorf("[Metric] insert error. sql: %s; err: %v", batch, err)
		}
	}
	*/
	// TODO
	return func(batch string) {
	}
}

var _ bp.ItemBuffer[HasItemSize, string] = &buffer2Sql{}

type buffer2Sql struct {
	bp.Reminder
	buf           []HasItemSize
	mux           sync.Mutex
	sizeThreshold int64
}

func newBuffer2Sql(opts ...buffer2SqlOption) *buffer2Sql {
	b := &buffer2Sql{
		Reminder: bp.NewConstantClock(5 * time.Second),
	}
	for _, opt := range opts {
		opt.apply(b)
	}
	return b
}

func (b *buffer2Sql) Add(item HasItemSize) {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.buf = append(b.buf, item)
	panic("implement me")
}

func (b *buffer2Sql) Reset() {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.buf = b.buf[0:0]
}

func (b *buffer2Sql) IsEmpty() bool {
	b.mux.Lock()
	defer b.mux.Unlock()
	return len(b.buf) == 0
}

func (b *buffer2Sql) ShouldFlush() bool {
	size := int64(0)
	for _, i := range b.buf {
		size += i.Size()
	}
	return size > b.sizeThreshold
}

func (b *buffer2Sql) GetBatch(buf *bytes.Buffer) string {
	b.mux.Lock()
	defer b.mux.Unlock()

	if b.IsEmpty() {
		return ""
	}
	for {

	}

	panic("implement me")
	return buf.String()
}

type buffer2SqlOption interface {
	apply(*buffer2Sql)
}

type buffer2SqlOptionFunc func(*buffer2Sql)

func (f buffer2SqlOptionFunc) apply(b *buffer2Sql) {
	f(b)
}

func withSizeThreshold(size int64) buffer2SqlOption {
	return buffer2SqlOptionFunc(func(b *buffer2Sql) {
		b.sizeThreshold = size
	})
}
