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

package fileservice

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type waitStartedContext struct {
	context.Context
	once    sync.Once
	started chan struct{}
}

func (c *waitStartedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.started) })
	return c.Context.Done()
}

func TestIOMerger(t *testing.T) {
	merger := NewIOMerger()
	n := 1024
	key := IOMergeKey{
		Path: "foo",
	}

	wg := new(sync.WaitGroup)
	wg.Add(n)
	var c int
	var cs []int
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			for {
				done, wait := merger.Merge(key, time.Second)
				if done != nil {
					cs = append(cs, c)
					c++
					done()
					return
				} else {
					wait()
				}
			}
		}()
	}
	wg.Wait()

	if c != 1024 {
		t.Fatal()
	}

	// should be no race and sequential
	for i, c := range cs {
		if c != i {
			t.Fatalf("got %v", cs)
		}
	}
}

func BenchmarkIOMergerNoContention(b *testing.B) {
	merger := NewIOMerger()
	key := IOMergeKey{
		Path: "foo",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		done, wait := merger.Merge(key, time.Second)
		if done != nil {
			done()
		} else {
			wait()
		}
	}
}

func BenchmarkIOMergerParallel(b *testing.B) {
	merger := NewIOMerger()
	key := IOMergeKey{
		Path: "foo",
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			done, wait := merger.Merge(key, time.Second)
			if done != nil {
				done()
			} else {
				wait()
			}
		}
	})
}

func TestIOMergerMaxWait(t *testing.T) {
	merger := NewIOMerger()
	key := IOMergeKey{
		Path: "foo",
	}
	// initiate
	_, _ = merger.Merge(key, time.Second)
	// wait
	_, wait := merger.Merge(key, time.Second)
	// will return
	wait()
}

func TestIOMergerShortMaxWait(t *testing.T) {
	merger := NewIOMerger()
	key := IOMergeKey{
		Path: "foo",
	}

	done, wait := merger.Merge(key, time.Second)
	if done == nil || wait != nil {
		t.Fatal("expected first merge to initiate")
	}
	defer done()

	_, wait = merger.Merge(key, time.Millisecond*20)
	if wait == nil {
		t.Fatal("expected second merge to wait")
	}

	t0 := time.Now()
	wait()
	if elapsed := time.Since(t0); elapsed > time.Second {
		t.Fatalf("short max wait was delayed by slow wait duration: %v", elapsed)
	}
}

func TestIOMergerIsMerging(t *testing.T) {
	merger := NewIOMerger()
	key := IOMergeKey{
		Path: "foo",
	}

	done, wait := merger.Merge(key, time.Second)

	if done == nil || wait != nil {
		t.Fatal("expected first merge to initiate")
	}
	if !merger.IsMerging(key) {
		t.Fatal("expected key to be merging")
	}
	done()
	if merger.IsMerging(key) {
		t.Fatal("expected key to stop merging")
	}
}

func TestIOMergerWaitContext(t *testing.T) {
	merger := NewIOMerger()
	key := IOMergeKey{Path: "foo"}
	done, wait := merger.Merge(key, time.Second)
	if done == nil || wait != nil {
		t.Fatal("expected first merge to initiate")
	}
	defer func() {
		if merger.IsMerging(key) {
			done()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := merger.waitContext(ctx, key); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled wait, got %v", err)
	}

	waitCtx := &waitStartedContext{
		Context: context.Background(),
		started: make(chan struct{}),
	}
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- merger.waitContext(waitCtx, key)
	}()
	<-waitCtx.started
	done()
	if err := <-waitDone; err != nil {
		t.Fatalf("merge completion wait failed: %v", err)
	}
	if err := merger.waitContext(context.Background(), key); err != nil {
		t.Fatalf("completed merge wait failed: %v", err)
	}
}
