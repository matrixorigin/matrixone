// Copyright 2024 Matrix Origin
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

package spool

import (
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
)

type testElement struct {
	N      int
	onFree func()
}

func (i testElement) SizeInSpool() int64 {
	return 1
}

func (i testElement) SpoolFree() {
	if i.onFree != nil {
		i.onFree()
	}
}

func TestSpool(t *testing.T) {
	numConsumers := 128
	spool, cursors := New[testElement](2, numConsumers)

	// consumers
	wg := new(sync.WaitGroup)
	wg.Add(numConsumers)
	var count atomic.Int64
	for i := range numConsumers {
		i := i
		cursor := cursors[i]
		go func() {
			defer wg.Done()
			var expect testElement
			for {
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(10))) // random delay
				v, ok := cursor.Next()
				if !ok {
					break
				}
				if v.N != expect.N {
					panic(fmt.Sprintf("got %v, expecting %v", v, expect))
				}
				expect.N++
				if v.N == 42 {
					break
				}
				count.Add(1)
			}
			cursor.Close()
		}()
	}

	// producer
	var numFree atomic.Int64
	numValues := 128
	for i := range numValues {
		spool.Send(testElement{
			N: i,
			onFree: func() {
				numFree.Add(1)
			},
		})
	}
	spool.Close()

	wg.Wait()

	if c := int(count.Load()); c != numConsumers*42 {
		t.Fatalf("got %v", c)
	}
	if n := numFree.Load(); n != int64(numValues) {
		t.Fatalf("got %v", n)
	}
}

func BenchmarkSingleConsumer(b *testing.B) {
	spool, cursors := New[testElement](128, 1)
	closed := new(atomic.Bool)
	go func() {
		for {
			if closed.Load() {
				return
			}
			spool.Send(testElement{
				N: 42,
			})
		}
	}()
	cursor := cursors[0]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cursor.Next()
	}
	closed.Store(true)
}

func TestSendAny(t *testing.T) {
	numConsumers := 128
	spool, cursors := New[testElement](2, numConsumers)

	// consumers
	wg := new(sync.WaitGroup)
	wg.Add(numConsumers)
	var count atomic.Int64
	for i := range numConsumers {
		i := i
		cursor := cursors[i]
		go func() {
			defer wg.Done()
			for {
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(10))) // random delay
				v, ok := cursor.Next()
				if !ok {
					break
				}
				_ = v
				count.Add(1)
			}
			cursor.Close()
		}()
	}

	// producer
	var numFree atomic.Int64
	numValues := 128
	for i := range numValues {
		spool.SendAny(testElement{
			N: i,
			onFree: func() {
				numFree.Add(1)
			},
		})
	}
	spool.Close()

	wg.Wait()

	if c := int(count.Load()); c != numValues {
		t.Fatalf("got %v", c)
	}
	if n := numFree.Load(); n != int64(numValues) {
		t.Fatalf("got %v", n)
	}
}

func TestSendTo(t *testing.T) {
	spool, cursors := New[testElement](2, 2)
	c1 := cursors[0]
	c2 := cursors[1]

	spool.SendTo(c1, testElement{
		N: 1,
	})
	v, ok := c1.Next()
	if !ok {
		t.Fatal()
	}
	if v.N != 1 {
		t.Fatal()
	}

	spool.SendTo(c2, testElement{
		N: 2,
	})
	v, ok = c2.Next()
	if !ok {
		t.Fatal()
	}
	if v.N != 2 {
		t.Fatal()
	}
}

func TestPeek(t *testing.T) {
	spool, cursors := New[testElement](1024, 1)
	c := cursors[0]

	_, ok := c.Peek()
	if ok {
		t.Fatal()
	}

	for i := range 1024 {
		spool.Send(testElement{
			N: i,
		})
	}

	for i := range 1024 {
		v, ok := c.Peek()
		if !ok {
			t.Fatal()
		}
		if v.N != i {
			t.Fatal()
		}
		v, ok = c.Next()
		if !ok {
			t.Fatal()
		}
		if v.N != i {
			t.Fatal()
		}
	}
}

func TestPeekNotTarget(t *testing.T) {
	spool, cursors := New[testElement](1, 2)
	spool.SendTo(cursors[0], testElement{})
	_, ok := cursors[1].Peek()
	if ok {
		t.Fatal()
	}
}

func TestPeekStoppedSpool(t *testing.T) {
	spool, cursors := New[testElement](1, 1)
	spool.Close()
	_, ok := cursors[0].Peek()
	if ok {
		t.Fatal()
	}
}

func TestClose(t *testing.T) {
	t.Run("Next after close", func(t *testing.T) {
		_, cursors := New[testElement](1, 1)
		cursors[0].Close()
		_, ok := cursors[0].Next()
		if ok {
			t.Fatal()
		}
	})

	t.Run("Peek after close", func(t *testing.T) {
		_, cursors := New[testElement](1, 1)
		cursors[0].Close()
		_, ok := cursors[0].Peek()
		if ok {
			t.Fatal()
		}
	})

	t.Run("send close spool", func(t *testing.T) {
		defer func() {
			p := recover()
			if p == nil {
				t.Fatal("should panic")
			}
			if msg := fmt.Sprintf("%v", p); msg != "send to closed spool" {
				t.Fatalf("got %v", msg)
			}
		}()
		spool, _ := New[testElement](1, 1)
		spool.Close()
		spool.Send(testElement{})
	})

	t.Run("cursor close twice", func(t *testing.T) {
		_, cursors := New[testElement](1, 1)
		cursors[0].Close()
		cursors[0].Close()
	})
}

func BenchmarkParallelConsumer(b *testing.B) {
	numConsumers := runtime.GOMAXPROCS(0)
	b.SetParallelism(1)
	spool, cursors := New[testElement](int64(numConsumers*10), numConsumers)
	closed := new(atomic.Bool)
	go func() {
		for {
			if closed.Load() {
				return
			}
			spool.Send(testElement{
				N: 42,
			})
		}
	}()
	var i atomic.Int32
	i.Store(-1)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		cursor := cursors[i.Add(1)]
		defer cursor.Close()
		var ok bool
		for pb.Next() {
			_, ok = cursor.Next()
			if !ok {
				break
			}
		}
	})
	closed.Store(true)
}

func Test1Capacity(t *testing.T) {
	spool, cursors := New[testElement](1, 1)

	spool.Send(testElement{
		N: 1,
	})
	v, ok := cursors[0].Next()
	if !ok {
		t.Fatal()
	}
	if v.N != 1 {
		t.Fatal()
	}

	go func() {
		spool.Send(testElement{
			N: 2,
		})
	}()
	v, ok = cursors[0].Next()
	if !ok {
		t.Fatal()
	}
	if v.N != 2 {
		t.Fatal()
	}
}

type testBytes struct {
	Bytes       []byte
	deallocator malloc.Deallocator
}

func (t testBytes) SizeInSpool() int64 {
	return int64(len(t.Bytes))
}

func (t testBytes) SpoolFree() {
	t.deallocator.Deallocate(malloc.NoHints)
}

func TestBytes(t *testing.T) {
	numCursors := 128
	spool, cursors := New[testBytes](512, numCursors)

	var nRead atomic.Int64
	wg := new(sync.WaitGroup)
	wg.Add(numCursors)
	for _, cursor := range cursors {
		cursor := cursor
		go func() {
			defer wg.Done()
			defer cursor.Close()
			for i := 0; true; i++ {
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)))
				v, ok := cursor.Next()
				if !ok {
					break
				}
				expected := []byte(fmt.Sprintf("%d", i))
				if !bytes.Equal(v.Bytes, expected) {
					panic("not expected")
				}
				nRead.Add(1)
			}
		}()
	}

	allocator := malloc.GetDefault(nil)
	for i := 0; i < 512; i++ {
		value := []byte(fmt.Sprintf("%d", i))
		bs, dec, err := allocator.Allocate(uint64(len(value)), malloc.NoHints)
		if err != nil {
			t.Fatal(err)
		}
		copy(bs, value)
		spool.Send(testBytes{
			Bytes:       bs,
			deallocator: dec,
		})
	}
	spool.Close()

	wg.Wait()

	if nRead.Load() != int64(numCursors)*512 {
		t.Fatal()
	}
}
