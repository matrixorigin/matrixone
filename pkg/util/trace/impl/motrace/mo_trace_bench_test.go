package motrace

import (
	"context"
	"sync"
	"testing"
	"time"

	queue "github.com/yireyun/go-queue"
)

// BenchmarkMOSpan_Free
// BenchmarkMOSpan_Free/empty
// BenchmarkMOSpan_Free/empty-10         	  111921	     10699 ns/op
// BenchmarkMOSpan_Free/just_apply
// BenchmarkMOSpan_Free/just_apply-10    	    6447	    173771 ns/op
// BenchmarkMOSpan_Free/goroutine
// BenchmarkMOSpan_Free/goroutine-10     	    2029	    606984 ns/op
// BenchmarkMOSpan_Free/sync
// BenchmarkMOSpan_Free/sync-10          	   10000	    115171 ns/op
// BenchmarkMOSpan_Free/channel
// BenchmarkMOSpan_Free/channel-10       	    4363	    278533 ns/op
// BenchmarkMOSpan_Free/lock_free_queue
// BenchmarkMOSpan_Free/lock_free_queue-10      1045	   1155037 ns/op
func BenchmarkMOSpan_Free(b *testing.B) {

	p := newMOTracerProvider(WithFSWriterFactory(&dummyFileWriterFactory{}), EnableTracer(true))
	tracer := p.Tracer("test").(*MOTracer)
	ctx := context.TODO()

	type mospanChan struct {
		ch chan *MOSpan
	}

	benchmarks := []struct {
		name    string
		prepare func(wg *sync.WaitGroup, eventCnt int) any
		op      func(wg *sync.WaitGroup, eventCnt int, param any)
	}{
		{
			name: "empty",
			op: func(wg *sync.WaitGroup, eventCnt int, param any) {
				for i := 0; i < eventCnt; i++ {
					wg.Done()
				}
			},
		},
		{
			name: "just_apply",
			op: func(wg *sync.WaitGroup, eventCnt int, param any) {
				for i := 0; i < eventCnt; i++ {
					tracer.Start(ctx, "span")
					wg.Done()
				}
			},
		},
		{
			name: "goroutine",
			op: func(wg *sync.WaitGroup, eventCnt int, param any) {
				for i := 0; i < eventCnt; i++ {
					_, span := tracer.Start(ctx, "span")
					go func() {
						span.(*MOSpan).Free()
						wg.Done()
					}()
				}
			},
		},
		{
			name: "sync",
			op: func(wg *sync.WaitGroup, eventCnt int, param any) {
				for i := 0; i < eventCnt; i++ {
					_, span := tracer.Start(ctx, "span")
					span.(*MOSpan).Free()
					wg.Done()
				}
			},
		},
		{
			name: "channel",
			prepare: func(wg *sync.WaitGroup, eventCnt int) any {
				c := &mospanChan{make(chan *MOSpan, eventCnt)}
				go func() {
					for {
						select {
						case s := <-c.ch:
							s.Free()
							wg.Done()
						}
					}
				}()
				return c
			},
			op: func(wg *sync.WaitGroup, eventCnt int, param any) {
				eventC, _ := param.(*mospanChan)

				for i := 0; i < eventCnt; i++ {
					_, span := tracer.Start(ctx, "span")
					eventC.ch <- span.(*MOSpan)
				}
			},
		},
		{
			name: "lock_free_queue",
			prepare: func(wg *sync.WaitGroup, eventCnt int) any {
				q := queue.NewQueue(uint32(eventCnt))
				elems := make([]interface{}, 100)
				var gets, remain uint32
				go func() {
					for {
						if gets, remain = q.Gets(elems); gets > 0 {
							for i := uint32(0); i < gets; i++ {
								elems[i].(*MOSpan).Free()
								wg.Done()
							}
						}
						if remain > 0 {
							continue
						} else {
							time.Sleep(time.Millisecond)
						}
					}
				}()
				return q
			},
			op: func(wg *sync.WaitGroup, eventCnt int, param any) {
				q, _ := param.(*queue.EsQueue)

				for i := 0; i < eventCnt; i++ {
					_, span := tracer.Start(ctx, "span")
					q.Put(span)
				}
			},
		},
	}

	var wg sync.WaitGroup

	eventCnt := 1_000

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			prepare := func(wg *sync.WaitGroup, eventCnt int) any {
				return nil
			}
			if bm.prepare != nil {
				prepare = bm.prepare
			}
			param := prepare(&wg, eventCnt)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				wg.Add(eventCnt)
				go bm.op(&wg, eventCnt, param)
				wg.Wait()
			}
		})
	}
}
