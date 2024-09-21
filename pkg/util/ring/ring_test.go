// Copyright 2014 Workiva, LLC
// Modifications copyright (C) 2023 MatrixOrigin.
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

package ring

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRingInsert(t *testing.T) {
	rb := NewRingBuffer[int](5)
	assert.Equal(t, uint64(8), rb.Cap())

	err := rb.Put(5)
	if !assert.Nil(t, err) {
		return
	}

	result, _, err := rb.Get()
	if !assert.Nil(t, err) {
		return
	}

	assert.Equal(t, 5, result)
}

func TestRingMultipleInserts(t *testing.T) {
	rb := NewRingBuffer[int](5)

	err := rb.Put(1)
	if !assert.Nil(t, err) {
		return
	}

	err = rb.Put(2)
	if !assert.Nil(t, err) {
		return
	}

	result, _, err := rb.Get()
	if !assert.Nil(t, err) {
		return
	}

	assert.Equal(t, 1, result)

	result, _, err = rb.Get()
	if assert.Nil(t, err) {
		return
	}

	assert.Equal(t, 2, result)
}

func TestIntertwinedGetAndPut(t *testing.T) {
	rb := NewRingBuffer[int](5)
	err := rb.Put(1)
	if !assert.Nil(t, err) {
		return
	}

	result, _, err := rb.Get()
	if !assert.Nil(t, err) {
		return
	}

	assert.Equal(t, 1, result)

	err = rb.Put(2)
	if !assert.Nil(t, err) {
		return
	}

	result, _, err = rb.Get()
	if !assert.Nil(t, err) {
		return
	}

	assert.Equal(t, 2, result)
}

func TestPutToFull(t *testing.T) {
	rb := NewRingBuffer[int](3)

	for i := 0; i < 4; i++ {
		err := rb.Put(i)
		if !assert.Nil(t, err) {
			return
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		err := rb.Put(4)
		assert.Nil(t, err)
		wg.Done()
	}()

	go func() {
		defer wg.Done()
		result, _, err := rb.Get()
		if !assert.Nil(t, err) {
			return
		}

		assert.Equal(t, 0, result)
	}()

	wg.Wait()
}

func TestOffer(t *testing.T) {
	rb := NewRingBuffer[string](2)

	ok, err := rb.Offer("foo")
	assert.True(t, ok)
	assert.Nil(t, err)
	ok, err = rb.Offer("bar")
	assert.True(t, ok)
	assert.Nil(t, err)
	ok, err = rb.Offer("baz")
	assert.False(t, ok)
	assert.Nil(t, err)

	item, _, err := rb.Get()
	assert.Nil(t, err)
	assert.Equal(t, "foo", item)
	item, _, err = rb.Get()
	assert.Nil(t, err)
	assert.Equal(t, "bar", item)
}

func TestRingGetEmpty(t *testing.T) {
	rb := NewRingBuffer[int](3)

	var wg sync.WaitGroup
	wg.Add(1)

	// want to kick off this consumer to ensure it blocks
	go func() {
		wg.Done()
		result, _, err := rb.Get()
		assert.Nil(t, err)
		assert.Equal(t, 0, result)
		wg.Done()
	}()

	wg.Wait()
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := rb.Put(0)
		assert.Nil(t, err)
	}()

	wg.Wait()
}

func TestRingPollEmpty(t *testing.T) {
	rb := NewRingBuffer[int](3)

	_, _, err := rb.Poll(1)
	assert.Equal(t, ErrTimeout, err)
}

func TestRingPoll(t *testing.T) {
	rb := NewRingBuffer[string](10)

	// should be able to Poll() before anything is present, without breaking future Puts
	rb.Poll(time.Millisecond)

	rb.Put(`test`)
	result, _, err := rb.Poll(0)
	if !assert.Nil(t, err) {
		return
	}

	assert.Equal(t, `test`, result)
	assert.Equal(t, uint64(0), rb.Len())

	rb.Put(`1`)
	rb.Put(`2`)

	result, _, err = rb.Poll(time.Millisecond)
	if !assert.Nil(t, err) {
		return
	}

	assert.Equal(t, `1`, result)
	assert.Equal(t, uint64(1), rb.Len())

	result, _, err = rb.Poll(time.Millisecond)
	if !assert.Nil(t, err) {
		return
	}

	assert.Equal(t, `2`, result)

	_, _, err = rb.Poll(5 * time.Millisecond)
	assert.Equal(t, ErrTimeout, err)
}

func TestRingLen(t *testing.T) {
	rb := NewRingBuffer[int](4)
	assert.Equal(t, uint64(0), rb.Len())

	rb.Put(1)
	assert.Equal(t, uint64(1), rb.Len())

	rb.Get()
	assert.Equal(t, uint64(0), rb.Len())

	for i := 0; i < 4; i++ {
		rb.Put(1)
	}
	assert.Equal(t, uint64(4), rb.Len())

	rb.Get()
	assert.Equal(t, uint64(3), rb.Len())
}

func TestDisposeOnGet(t *testing.T) {
	numThreads := 8
	var wg sync.WaitGroup
	wg.Add(numThreads)
	rb := NewRingBuffer[int](4)
	var spunUp sync.WaitGroup
	spunUp.Add(numThreads)

	for i := 0; i < numThreads; i++ {
		go func() {
			spunUp.Done()
			defer wg.Done()
			_, _, err := rb.Get()
			assert.NotNil(t, err)
		}()
	}

	spunUp.Wait()
	rb.Dispose()

	wg.Wait()
	assert.True(t, rb.IsDisposed())
}

func TestDisposeOnPut(t *testing.T) {
	numThreads := 8
	var wg sync.WaitGroup
	wg.Add(numThreads)
	rb := NewRingBuffer[int](4)
	var spunUp sync.WaitGroup
	spunUp.Add(numThreads)

	// fill up the queue
	for i := 0; i < 4; i++ {
		rb.Put(i)
	}

	// it's now full
	for i := 0; i < numThreads; i++ {
		go func(i int) {
			spunUp.Done()
			defer wg.Done()
			err := rb.Put(i)
			assert.NotNil(t, err)
		}(i)
	}

	spunUp.Wait()

	rb.Dispose()

	wg.Wait()

	assert.True(t, rb.IsDisposed())
}

func BenchmarkRBLifeCycle(b *testing.B) {
	rb := NewRingBuffer[int](64)

	counter := uint64(0)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			_, _, err := rb.Get()
			assert.Nil(b, err)

			if atomic.AddUint64(&counter, 1) == uint64(b.N) {
				return
			}
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rb.Put(i)
	}

	wg.Wait()
}

func BenchmarkRBLifeCycleContention(b *testing.B) {
	rb := NewRingBuffer[int](64)

	var wwg sync.WaitGroup
	var rwg sync.WaitGroup
	wwg.Add(10)
	rwg.Add(10)

	for i := 0; i < 10; i++ {
		go func() {
			for {
				_, _, err := rb.Get()
				if err == ErrDisposed {
					rwg.Done()
					return
				} else {
					assert.Nil(b, err)
				}
			}
		}()
	}

	b.ResetTimer()

	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < b.N; j++ {
				rb.Put(j)
			}
			wwg.Done()
		}()
	}

	wwg.Wait()
	rb.Dispose()
	rwg.Wait()
}

func BenchmarkRBPut(b *testing.B) {
	rb := NewRingBuffer[int](uint64(b.N))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ok, err := rb.Offer(i)
		if !ok {
			b.Fail()
		}
		if err != nil {
			b.Log(err)
			b.Fail()
		}
	}
}

func BenchmarkRBGet(b *testing.B) {
	rb := NewRingBuffer[int](uint64(b.N))

	for i := 0; i < b.N; i++ {
		rb.Offer(i)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rb.Get()
	}
}

func BenchmarkRBAllocation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		NewRingBuffer[int](1024)
	}
}

func TestRingReset(t *testing.T) {
	rb := NewRingBuffer[int](1)
	assert.Equal(t, uint64(0), rb.queue)
	assert.Equal(t, uint64(0), rb.dequeue)

	assert.NoError(t, rb.Put(1))
	assert.Equal(t, uint64(1), rb.queue)
	assert.Equal(t, uint64(0), rb.dequeue)

	rb.Reset()
	assert.Equal(t, uint64(0), rb.queue)
	assert.Equal(t, uint64(0), rb.dequeue)
	v, ok, err := rb.Poll(time.Millisecond * 10)
	assert.Error(t, err)
	assert.False(t, ok)
	assert.Equal(t, 0, v)

	assert.NoError(t, rb.Put(1))
}

// BenchmarkFullOffer base benchmark for WithGoScheduleThreshold
// goos: darwin
// goarch: amd64
// pkg: github.com/matrixorigin/matrixone/pkg/util/ring
// cpu: Intel(R) Core(TM) i7-8750H CPU @ 2.20GHz
// BenchmarkFullOffer/normal
// BenchmarkFullOffer/normal-12         	78426302	        13.78 ns/op
func BenchmarkFullOffer(b *testing.B) {
	b.Run("normal", func(b *testing.B) {
		rb := NewRingBuffer[int](1)
		rb.Offer(1)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rb.Offer(2)
		}
	})
}
