package trace

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/export"
)

var _ SpanProcessor = &batchSpanProcessor{}

// batchSpanProcessor is a SpanProcessor that batches asynchronously-received
// spans and sends them to a trace.Exporter when complete.
type batchSpanProcessor struct {
	e export.BatchProcessor

	batchMutex sync.Mutex
	timer      *time.Timer
	stopWait   sync.WaitGroup
	stopOnce   sync.Once
	stopCh     chan struct{}
}

func NewBatchSpanProcessor(exporter export.BatchProcessor) SpanProcessor {
	bsp := &batchSpanProcessor{
		e: exporter,
	}

	return bsp
}

// OnStart method does nothing.
func (bsp *batchSpanProcessor) OnStart(parent context.Context, s Span) {}

// OnEnd method enqueues a ReadOnlySpan for later processing.
func (bsp *batchSpanProcessor) OnEnd(s Span) {
	// Do not enqueue spans if we are just going to drop them.
	if bsp.e == nil {
		return
	}
	if i, ok := s.(batchpipe.HasName); !ok {
		panic("No implement batchpipe.HasName")
	} else {
		bsp.e.Collect(DefaultContext(), i)
	}
}

// Shutdown flushes the queue and waits until all spans are processed.
// It only executes once. Subsequent call does nothing.
func (bsp *batchSpanProcessor) Shutdown(ctx context.Context) error {
	var err error
	bsp.stopOnce.Do(func() {
		wait := make(chan struct{})
		go func() {
			close(bsp.stopCh)
			bsp.stopWait.Wait()
			if bsp.e != nil {
				// bsp.e.Shutdown(ctx)
				if err := bsp.e.Stop(true); err != nil {
					// TODO: otel.Handle(err)
					panic(err)
				}
			}
			close(wait)
		}()
		// Wait until the wait group is done or the context is cancelled
		select {
		case <-wait:
		case <-ctx.Done():
			err = ctx.Err()
		}
	})
	return err
}

func (bsp *batchSpanProcessor) FLush() {
	panic("implement me")
	bsp.e.Stop(false)
}
