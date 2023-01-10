// Copyright The OpenTelemetry Authors
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

// Portions of this file are additionally subject to the following
// copyright.
//
// Copyright (C) 2022 Matrix Origin.
//
// Modified the behavior and the interface of the step.

package motrace

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
)

var _ trace.SpanProcessor = &batchSpanProcessor{}

// batchSpanProcessor is a SpanProcessor that batches asynchronously-received
// spans and sends them to a trace.Exporter when complete.
type batchSpanProcessor struct {
	e BatchProcessor

	stopWait sync.WaitGroup
	stopOnce sync.Once
	stopCh   chan struct{}
}

func NewBatchSpanProcessor(exporter BatchProcessor) trace.SpanProcessor {
	bsp := &batchSpanProcessor{
		e:      exporter,
		stopCh: make(chan struct{}),
	}

	return bsp
}

// OnStart method does nothing.
func (bsp *batchSpanProcessor) OnStart(parent context.Context, s trace.Span) {}

// OnEnd method enqueues a ReadOnlySpan for later processing.
func (bsp *batchSpanProcessor) OnEnd(s trace.Span) {
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
