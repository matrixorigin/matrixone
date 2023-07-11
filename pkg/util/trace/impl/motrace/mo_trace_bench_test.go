package motrace

import (
	"context"
	"sync"
	"testing"
)

func BenchmarkMOSpan_Free(b *testing.B) {

	p := newMOTracerProvider(WithFSWriterFactory(&dummyFileWriterFactory{}), EnableTracer(true))
	tracer := p.Tracer("test").(*MOTracer)
	ctx := context.TODO()

	benchmarks := []struct {
		name string
		op   func(wg *sync.WaitGroup, eventCnt int)
	}{
		{
			name: "empty",
			op: func(wg *sync.WaitGroup, eventCnt int) {
				for i := 0; i < eventCnt; i++ {
					wg.Done()
				}
			},
		},
		{
			name: "channel",
			op: func(wg *sync.WaitGroup, eventCnt int) {
				for i := 0; i < eventCnt; i++ {
					_, span := tracer.Start(ctx, "span")
					go span.(*MOSpan).Free()
					wg.Done()
				}
			},
		},
	}

	var wg sync.WaitGroup

	eventCnt := 500_00

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				wg.Add(eventCnt)
				go bm.op(&wg, eventCnt)
				wg.Wait()
			}
		})
	}
}
