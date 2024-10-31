// Copyright 2021 - 2024 Matrix Origin
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

package datasync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestQueueEnqueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("empty data", func(t *testing.T) {
		ctx := context.Background()
		q := newDataQueue(10, nil)
		q.enqueue(ctx, nil)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		q := newDataQueue(100, nil)
		for i := 0; i < 10; i++ {
			w := newWrappedData(nil, 0, nil)
			q.enqueue(ctx, w)
		}
		assert.Equal(t, 10, len(q.(*dataQueue).queue))
	})

	t.Run("context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var wg sync.WaitGroup
		wg.Add(1)
		go func(ctx context.Context) {
			q := newDataQueue(10, nil)
			for i := 0; i < 20; i++ {
				w := newWrappedData(nil, 0, nil)
				q.enqueue(ctx, w)
			}
			wg.Done()
		}(ctx)
		time.Sleep(time.Millisecond * 200)
		cancel()
		wg.Wait()
	})

	t.Run("ignore", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var wg sync.WaitGroup
		wg.Add(1)
		go func(ctx context.Context) {
			q := newDataQueue(10, func(data *wrappedData) bool {
				return true
			})
			for i := 0; i < 20; i++ {
				w := newWrappedData(nil, 0, nil)
				q.enqueue(ctx, w)
			}
			wg.Done()
		}(ctx)
		wg.Wait()
	})
}

func TestQueueDequeue(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		q := newDataQueue(20, func(data *wrappedData) bool {
			return true
		})
		for i := 0; i < 10; i++ {
			w := newWrappedData(nil, 0, nil)
			q.enqueue(ctx, w)
		}
		for i := 0; i < 10; i++ {
			v, err := q.dequeue(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, v)
		}
	})

	t.Run("parallel case", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		q := newDataQueue(100, func(data *wrappedData) bool {
			return false
		})
		var wg sync.WaitGroup
		wg.Add(2)
		go func(ctx context.Context) {
			for i := 0; i < 100; i++ {
				w := newWrappedData(nil, 0, nil)
				q.enqueue(ctx, w)
			}
			wg.Done()
		}(ctx)
		go func(ctx context.Context) {
			for i := 0; i < 100; i++ {
				w := newWrappedData(nil, 0, nil)
				q.enqueue(ctx, w)
			}
			wg.Done()
		}(ctx)
		for i := 0; i < 200; i++ {
			v, err := q.dequeue(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, v)
		}
		wg.Wait()
	})

	t.Run("context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var wg sync.WaitGroup
		wg.Add(1)
		go func(ctx context.Context) {
			q := newDataQueue(10, func(data *wrappedData) bool {
				return false
			})
			_, _ = q.dequeue(ctx)
			wg.Done()
		}(ctx)
		cancel()
		wg.Wait()
	})
}

func TestQueueClose(t *testing.T) {
	q := newDataQueue(10, nil)
	q.close()
	v, err := q.dequeue(context.Background())
	assert.NoError(t, err)
	assert.Nil(t, v)
}
