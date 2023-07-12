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

package test

import (
	"context"
	"sync"
	"testing"
	"time"
)

// BenchmarkMOHungSpan_TriggerOrChannel result run on MacBook Pro M1:
//
// BenchmarkMOHungSpan_TriggerOrChannel/empty
// BenchmarkMOHungSpan_TriggerOrChannel/empty-10         	     330	   3681184 ns/op
// BenchmarkMOHungSpan_TriggerOrChannel/trigger
// BenchmarkMOHungSpan_TriggerOrChannel/trigger-10       	      26	  46561218 ns/op
// BenchmarkMOHungSpan_TriggerOrChannel/channel
// BenchmarkMOHungSpan_TriggerOrChannel/channel-10       	       4	 350303490 ns/op
// BenchmarkMOHungSpan_TriggerOrChannel/trigger_100worker
// BenchmarkMOHungSpan_TriggerOrChannel/trigger_100worker-10         	      30	  56178135 ns/op
// BenchmarkMOHungSpan_TriggerOrChannel/channel_100worker
// BenchmarkMOHungSpan_TriggerOrChannel/channel_100worker-10         	       5	 216839308 ns/op
func BenchmarkMOHungSpan_TriggerOrChannel(b *testing.B) {

	ctx := context.TODO()

	benchmarks := []struct {
		name string
		// op handle 2 steps:
		// 1. create event num: eventCnt
		// 2. wait all event finish
		op func(wg *sync.WaitGroup, eventCnt int)
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
			name: "trigger",
			op: func(wg *sync.WaitGroup, eventCnt int) {
				for i := 0; i < eventCnt; i++ {
					t := time.AfterFunc(time.Hour, func() {})
					t.Stop()
					wg.Done()
				}
			},
		},
		{
			name: "channel",
			op: func(wg *sync.WaitGroup, eventCnt int) {
				for i := 0; i < eventCnt; i++ {
					ctx, cancel := context.WithTimeout(ctx, time.Hour)
					go func() {
						select {
						case <-ctx.Done():
							wg.Done()
						}
					}()
					cancel()
				}
			},
		},
		{
			name: "trigger_100worker",
			op: func(wg *sync.WaitGroup, eventCnt int) {
				worker := 100
				subCnt := eventCnt / worker
				workFunc := func(cnt int) {
					for i := 0; i < cnt; i++ {
						t := time.AfterFunc(time.Hour, func() {})
						t.Stop()
						wg.Done()
					}
				}
				for i := 0; i < worker; i++ {
					go workFunc(subCnt)
				}
			},
		},

		{
			name: "channel_100worker",
			op: func(wg *sync.WaitGroup, eventCnt int) {
				worker := 100
				subCnt := eventCnt / worker
				workFunc := func(cnt int) {
					for i := 0; i < cnt; i++ {
						ctx, cancel := context.WithTimeout(ctx, time.Hour)
						go func() {
							select {
							case <-ctx.Done():
								wg.Done()
							}
						}()
						cancel()
					}
				}
				for i := 0; i < worker; i++ {
					go workFunc(subCnt)
				}
			},
		},
	}

	var wg sync.WaitGroup

	eventCnt := 500_000

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
