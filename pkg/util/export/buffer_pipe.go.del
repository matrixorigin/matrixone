package export

import (
	"bytes"
	"time"
)

// ItemType return Name decides which table the owner will go to
type ItemType interface {
	GetItemType() string
}

// Reminder to force flush ItemBuffer
type Reminder interface {
	RemindNextAfter() time.Duration
	RemindBackOff()
	RemindBackOffCnt() int
	RemindReset()
}

// ItemBuffer Stash items and construct a batch can be stored. for instance, a sql inserting all items into a table
// 要提供两种基本的buffer类型: 1) for metric/stats; 2) for statement/trace/log
type ItemBuffer[T any, B any] interface {
	Reminder
	Add(item T)
	Reset()
	IsEmpty() bool
	ShouldFlush() bool
	// GetBatch bytes.Buffer is used to mitigate mem allocation and the returned bytes should own its data
	GetBatch(buf *bytes.Buffer) B
}

type PipeImpl[T any, B any] interface {
	// NewItemBuffer create a new buffer for one kind of Item
	NewItemBuffer(name string) ItemBuffer[T, B]
	// NewItemBatchHandler do
	// BatchHandler handle the StoreBatch from an ItemBuffer, for example, execute an insert sql.
	// this handle may be running on multiple goroutine
	NewItemBatchHandler() func(batch B)
}

type BaseBatchPipe[T any] interface {
	// SendItem returns error when pipe is closed
	SendItem(items ...T) error
	// Start kick off the merge workers and batch workers, return false if the pipe is working has been called
	Start() bool
	// Stop terminate all workers. If graceful asked, wait until all items are processed,
	// otherwise, quit immediately. Caller can use the returned channel to wait Stop finish
	Stop(graceful bool) (<-chan struct{}, bool)
}

type baseBatchPipeOpts struct {
	BatchWorkerNum  int
	BufferWorkerNum int
}

type BaseBatchPipeOpt interface {
	ApplyTo(*baseBatchPipeOpts)
}

type WithBatchWorkerNum int

func (x WithBatchWorkerNum) ApplyTo(o *baseBatchPipeOpts) {
	o.BatchWorkerNum = int(x)
}

type WithBufferWorkerNum int

func (x WithBufferWorkerNum) ApplyTo(o *baseBatchPipeOpts) {
	o.BufferWorkerNum = int(x)
}

/*func NewBaseBatchPipe(impl PipeImpl[T, B], opts ...BaseBatchPipeOpt) BaseBatchPipe[T] {

}*/
