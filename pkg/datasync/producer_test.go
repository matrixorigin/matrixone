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
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/stretchr/testify/assert"
)

func TestProducer_Enqueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	writeLsn := &atomic.Uint64{}
	p := newProducer(common{}, 10, writeLsn)
	ctx := context.Background()
	w := newWrappedData(nil, 0, nil)
	p.Enqueue(ctx, w)
	q := p.(*producer).dataQ
	dw, err := q.dequeue(ctx)
	assert.NoError(t, err)
	assert.Equal(t, w, dw)
}

func TestProducer_EnqueueFull1(t *testing.T) {
	defer leaktest.AfterTest(t)()
	writeLsn := &atomic.Uint64{}
	p := newProducer(common{}, 10, writeLsn)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		w := newWrappedData(nil, 0, nil)
		p.Enqueue(ctx, w)
	}
	q := p.(*producer).dataQ
	assert.Equal(t, 10, len(q.(*dataQueue).queue))
}

func TestProducer_EnqueueFull2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	writeLsn := &atomic.Uint64{}
	p := newProducer(common{}, 10, writeLsn)
	ctx := context.Background()
	q := p.(*producer).dataQ
	var wg1, wg2 sync.WaitGroup

	wg1.Add(1)
	wg2.Add(20)
	go func() {
		for i := 0; i < 10; i++ {
			payload, err := payloadData()
			assert.NoError(t, err)
			assert.NotNil(t, payload)
			w := newWrappedData(genRecord(payload, 0).Data, 0, nil)
			p.Enqueue(ctx, w)
		}
		wg1.Done()

		// the queue is already full
		for i := 0; i < 10; i++ {
			payload, err := payloadData()
			assert.NoError(t, err)
			assert.NotNil(t, payload)
			w := newWrappedData(genRecord(payload, 0).Data, 0, nil)
			p.Enqueue(ctx, w)
		}
		wg2.Wait()
		assert.Equal(t, 0, len(q.(*dataQueue).queue))
	}()
	wg1.Wait()
	time.Sleep(time.Millisecond * 100)
	for i := 0; i < 20; i++ {
		w, err := q.dequeue(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, w)
		wg2.Done()
	}
}

func withProducerStarted(
	t *testing.T,
	rt runtime.Runtime,
	c logservice.Client,
	ignoreWhenError bool,
	expectPanic bool,
	f func(ctx context.Context, p Producer),
) {
	writeLsn := &atomic.Uint64{}
	p := newProducer(
		*newCommon().
			withLog(rt.Logger()).
			withPool(newDataPool(100)),
		10,
		writeLsn,
		withRetryTimes(10),
		withIgnoreWhenError(ignoreWhenError),
	)
	defer p.Close()
	p.(*producer).client.client = c.(logservice.StandbyClient)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if expectPanic {
			assert.Panics(t, func() {
				p.Start(ctx)
			})
		} else {
			p.Start(ctx)
		}
	}()
	f(ctx, p)
}

func TestProducer_Start(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rt := runtime.DefaultRuntime()

	t.Run("context canceled 1", func(t *testing.T) {
		fn := func(t *testing.T, s *logservice.Service, cfg logservice.ClientConfig, c logservice.Client) {
			writeLsn := &atomic.Uint64{}
			p := newProducer(common{}, 10, writeLsn)
			defer p.Close()

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			go p.Start(ctx)
		}
		logservice.RunClientTest(t, false, nil, fn)
	})

	t.Run("context canceled 2", func(t *testing.T) {
		fn := func(t *testing.T, s *logservice.Service, cfg logservice.ClientConfig, c logservice.Client) {
			writeLsn := &atomic.Uint64{}
			p := newProducer(
				*newCommon().
					withLog(rt.Logger()).
					withPool(newDataPool(100)),
				10,
				writeLsn,
			)
			defer p.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go p.Start(ctx)
			time.Sleep(time.Millisecond * 100)
			cancel()
		}
		logservice.RunClientTest(t, false, nil, fn)
	})

	t.Run("invalid upstream lsn", func(t *testing.T) {
		fn := func(t *testing.T, s *logservice.Service, cfg logservice.ClientConfig, c logservice.Client) {
			withProducerStarted(t, rt, c, true, true, func(ctx context.Context, p Producer) {
				var wg sync.WaitGroup
				rec := c.GetLogRecord(16)
				w := newWrappedData(rec.Data, 0, &wg)
				wg.Add(1)
				p.Enqueue(ctx, w)
				wg.Wait()
				assert.Equal(t, uint64(0), p.(*producer).writeLsn.Load())
			})
		}
		logservice.RunClientTest(t, false, nil, fn)
	})

	t.Run("invalid data length", func(t *testing.T) {
		fn := func(t *testing.T, s *logservice.Service, cfg logservice.ClientConfig, c logservice.Client) {
			withProducerStarted(t, rt, c, true, true, func(ctx context.Context, p Producer) {
				var wg sync.WaitGroup
				w := newWrappedData(make([]byte, 3), 0, &wg)
				wg.Add(1)
				p.Enqueue(ctx, w)
				wg.Wait()
				assert.Equal(t, uint64(0), p.(*producer).writeLsn.Load())
			})
		}
		logservice.RunClientTest(t, false, nil, fn)
	})

	t.Run("no locations", func(t *testing.T) {
		fn := func(t *testing.T, s *logservice.Service, cfg logservice.ClientConfig, c logservice.Client) {
			withProducerStarted(t, rt, c, true, false, func(ctx context.Context, p Producer) {
				var wg sync.WaitGroup
				rec := c.GetLogRecord(16)
				w := newWrappedData(rec.Data, 2, &wg)
				wg.Add(1)
				p.Enqueue(ctx, w)
				wg.Wait()
				assert.Equal(t, uint64(0), p.(*producer).writeLsn.Load())
			})
		}
		logservice.RunClientTest(t, false, nil, fn)
	})

	t.Run("invalid lease holder", func(t *testing.T) {
		fn := func(t *testing.T, s *logservice.Service, cfg logservice.ClientConfig, c logservice.Client) {
			withProducerStarted(t, rt, c, true, false, func(ctx context.Context, p Producer) {
				uid := uuid.New()
				var num uint16 = 8
				var alg uint8 = 1
				var offset uint32 = 2
				var length uint32 = 3
				var originSize uint32 = 4

				payload, err := generateCmdPayload(
					*newParameter(),
					genLocation(uid, num, alg, offset, length, originSize),
				)
				assert.NoError(t, err)
				assert.NotNil(t, payload)
				var wg sync.WaitGroup
				w := newWrappedData(genRecord(payload, 0).Data, 20, &wg)
				wg.Add(1)
				p.Enqueue(ctx, w)
				wg.Wait()
			})
		}
		logservice.RunClientTest(t, false, nil, fn)
	})

	t.Run("ok to produce", func(t *testing.T) {
		fn := func(t *testing.T, s *logservice.Service, cfg logservice.ClientConfig, c logservice.Client) {
			withProducerStarted(t, rt, c, true, false, func(ctx context.Context, p Producer) {
				payload, err := payloadData()
				assert.NoError(t, err)
				assert.NotNil(t, payload)
				var wg sync.WaitGroup
				w := newWrappedData(genRecord(payload, 0).Data, 2, &wg)
				wg.Add(1)
				p.Enqueue(ctx, w)
				wg.Wait()
				assert.Equal(t, uint64(4), p.(*producer).writeLsn.Load())
			})
		}
		logservice.RunClientTest(t, false, nil, fn)
	})
}

func payloadData() ([]byte, error) {
	uid := uuid.New()
	var num uint16 = 8
	var alg uint8 = 1
	var offset uint32 = 2
	var length uint32 = 3
	var originSize uint32 = 4
	return generateCmdPayload(
		*newParameter(),
		genLocation(uid, num, alg, offset, length, originSize),
	)
}
