// Copyright 2023 Matrix Origin
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

package motrace

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"math/rand"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

// BenchmarkMOSpan_1kFree
// BenchmarkMOSpan_1kFree/empty
// BenchmarkMOSpan_1kFree/empty-10         	  111921	     10699 ns/op
// BenchmarkMOSpan_1kFree/just_apply
// BenchmarkMOSpan_1kFree/just_apply-10    	    6447	    173771 ns/op
// BenchmarkMOSpan_1kFree/goroutine
// BenchmarkMOSpan_1kFree/goroutine-10     	    2029	    606984 ns/op
// BenchmarkMOSpan_1kFree/sync
// BenchmarkMOSpan_1kFree/sync-10          	   10000	    115171 ns/op
// BenchmarkMOSpan_1kFree/channel
// BenchmarkMOSpan_1kFree/channel-10       	    4363	    278533 ns/op
// BenchmarkMOSpan_1kFree/lock_free_queue
// BenchmarkMOSpan_1kFree/lock_free_queue-10      1045	   1155037 ns/op
//
// with 100 goroutine
// BenchmarkMOSpan_1kFree/empty_multi
// BenchmarkMOSpan_1kFree/empty_multi-10   	     224	   5092005 ns/op
// BenchmarkMOSpan_1kFree/just_apply_multi
// BenchmarkMOSpan_1kFree/just_apply_multi-10         	      69	  16713981 ns/op
// BenchmarkMOSpan_1kFree/goroutine_multi
// BenchmarkMOSpan_1kFree/goroutine_multi-10          	      33	  35532263 ns/op
// BenchmarkMOSpan_1kFree/sync_multi
// BenchmarkMOSpan_1kFree/sync_multi-10               	      74	  15893255 ns/op
// BenchmarkMOSpan_1kFree/channel_multi
// BenchmarkMOSpan_1kFree/channel_multi-10            	      39	  40667865 ns/op
func BenchmarkMOSpan_1kFree(b *testing.B) {

	p := newMOTracerProvider(WithFSWriterFactory(&dummyFileWriterFactory{}), EnableTracer(true))
	tracer := p.Tracer("test").(*MOTracer)
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

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
				loop:
					for {
						select {
						case <-ctx.Done():
							break loop
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
		/*{
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
		},*/
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
	for _, bm := range benchmarks {

		worker := 100
		totalEvent := eventCnt * worker
		b.Run(bm.name+"_multi", func(b *testing.B) {
			prepare := func(wg *sync.WaitGroup, eventCnt int) any {
				return nil
			}
			if bm.prepare != nil {
				prepare = bm.prepare
			}
			param := prepare(&wg, totalEvent)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				wg.Add(totalEvent)
				for j := 0; j < worker; j++ {
					go bm.op(&wg, eventCnt, param)
				}
				wg.Wait()
			}
		})
	}
}

// BenchmarkMOSpan_ApplyOneAndFree/empty
// BenchmarkMOSpan_ApplyOneAndFree/empty-10         	 4520266	       290.6 ns/op
// BenchmarkMOSpan_ApplyOneAndFree/apply
// BenchmarkMOSpan_ApplyOneAndFree/apply-10         	 3541989	       346.5 ns/op
// BenchmarkMOSpan_ApplyOneAndFree/applyNewAndFree
// BenchmarkMOSpan_ApplyOneAndFree/applyNewAndFree-10         	 3931137	       354.3 ns/op
// BenchmarkMOSpan_ApplyOneAndFree/prepare1000ApplyAndFree
// BenchmarkMOSpan_ApplyOneAndFree/prepare1000ApplyAndFree-10 	 3546651	       339.8 ns/op
// BenchmarkMOSpan_ApplyOneAndFree/apply_after
// BenchmarkMOSpan_ApplyOneAndFree/apply_after-10             	 3559951	       370.3 ns/op
func BenchmarkMOSpan_ApplyOneAndFree(b *testing.B) {

	p := newMOTracerProvider(WithFSWriterFactory(&dummyFileWriterFactory{}), EnableTracer(true))
	tracer := p.Tracer("test").(*MOTracer)
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	benchmarks := []struct {
		name    string
		prepare func(wg *sync.WaitGroup, eventCnt int) any
		op      func(wg *sync.WaitGroup, eventCnt int, param any)
	}{
		{
			name: "empty",
			op: func(wg *sync.WaitGroup, eventCnt int, param any) {
			},
		},
		{
			name: "apply",
			op: func(wg *sync.WaitGroup, eventCnt int, param any) {
				tracer.Start(ctx, "span")
			},
		},
		{
			name: "applyNewAndFree",
			op: func(wg *sync.WaitGroup, eventCnt int, param any) {
				for i := 0; i < eventCnt; i++ {
					_, span := tracer.Start(ctx, "span")
					span.(*MOSpan).Free()
				}
			},
		},
		{
			name: "prepare1000ApplyAndFree",
			prepare: func(wg *sync.WaitGroup, _ int) any {
				eventCnt := 1_000 // reset cnt
				for i := 0; i < eventCnt; i++ {
					_, span := tracer.Start(ctx, "span")
					span.(*MOSpan).Free()
				}
				return nil
			},
			op: func(wg *sync.WaitGroup, eventCnt int, param any) {
				_, span := tracer.Start(ctx, "span")
				defer span.(*MOSpan).Free()
			},
		},
		{
			name: "apply_after",
			op: func(wg *sync.WaitGroup, eventCnt int, param any) {
				tracer.Start(ctx, "span")
			},
		},
	}

	var wg sync.WaitGroup

	eventCnt := 1

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
				go bm.op(&wg, eventCnt, param)
			}
		})
	}
}

func dummyReturnNilEndFuncNoCtxValue(
	ctx context.Context,
) (
	_ context.Context,
	end func(),
) {
	return ctx, nil
}

func dummyReturnNilEndFunc(
	ctx context.Context,
) (
	_ context.Context,
	end func(),
) {
	v := ctx.Value(fileservice.CtxKeyStatementProfiler)
	if v == nil {
		return ctx, nil
	}
	return ctx, nil
}

func dummyReturnEmptyEndFunc(
	ctx context.Context,
) (
	_ context.Context,
	end func(),
) {
	v := ctx.Value(fileservice.CtxKeyStatementProfiler)
	if v == nil {
		return ctx, func() {}
	}
	return ctx, func() {}
}

func BenchmarkMOSpan_if_vs_for(b *testing.B) {

	ctx := context.TODO()

	benchmarks := []struct {
		name    string
		prepare func(arrF []func()) []func()
	}{
		{
			name: "if_check_nil_no_ctx_value",
			prepare: func(arrF []func()) []func() {
				_, end := dummyReturnNilEndFuncNoCtxValue(ctx)
				if end != nil {
					return append(arrF, end)
				}
				return arrF
			},
		},
		{
			name: "if_check_nil",
			prepare: func(arrF []func()) []func() {
				_, end := dummyReturnNilEndFunc(ctx)
				if end != nil {
					return append(arrF, end)
				}
				return arrF
			},
		},
		{
			name: "for_empty_end",
			prepare: func(arrF []func()) []func() {
				_, end := dummyReturnEmptyEndFunc(ctx)
				return append(arrF, end)
			},
		},
	}

	eventCnt := rand.Intn(10) + 1
	var arrF = make([]func(), 0, eventCnt)
	b.Logf("eventCnt: %d", eventCnt)

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				arrF = arrF[:0]
				for j := 0; j < eventCnt; j++ {
					arrF = bm.prepare(arrF)
				}
				for _, f := range arrF {
					f()
				}
			}
		})
	}
}

func BenchmarkMOTracer_WithOpts_vs_WithoutOpts(b *testing.B) {
	tracer := &MOTracer{
		TracerConfig: trace.TracerConfig{Name: "motrace_test"},
		provider:     defaultMOTracerProvider(),
	}
	tracer.provider.enable = true

	trace.InitMOCtledSpan()
	trace.SetMoCtledSpanState("local", true, 0)

	b.Run("enable with opts", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tracer.IsEnable(trace.WithKind(trace.SpanKindLocalFSVis))
		}
	})

	b.Run("enable without opts", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tracer.IsEnable()
		}
	})

	b.Run("total with opts", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, span := tracer.Start(context.Background(), "test", trace.WithKind(
				trace.SpanKindLocalFSVis))
			span.End(trace.WithFSReadWriteExtra("xxx", nil, 0))
		}
	})

	b.Run("total without opts", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, span := tracer.Start(context.Background(), "test")
			span.End()
		}
	})
}
