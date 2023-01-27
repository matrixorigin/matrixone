// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/tests"
)

func TestLogtailStream(t *testing.T) {
	tableA := mockTable(1, 1, 1)
	tableB := mockTable(2, 2, 2)
	tableC := mockTable(3, 3, 3)

	addrs, err := tests.GetAddressBatch("127.0.0.1", 1)
	require.NoError(t, err)

	address := addrs[0]
	rt := mockRuntime()

	/* ---- construct logtail server ---- */
	stop := startLogtailServer(t, address, rt, tableA, tableB, tableC)
	defer stop()

	/* ---- construct logtail stream ---- */
	stream, err := NewDefaultLogtailStream(
		address, 100, NewLogtailResponsePool(),
		1*time.Second, 100, 16*mpool.MB, 16*mpool.MB,
		"logtail-stream-client", rt.Logger().RawLogger(),
	)
	require.NoError(t, err)
	require.NotNil(t, stream)

	/* ---- consume logtail response ---- */
	consumer := newResponseConsumer(t, stream)
	consumer.start()
	defer consumer.stop()

	/* ---- send subscription request then wait response ---- */
	subFunc := func(table api.TableID) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		t.Log("===> send subscription request via logtail stream")
		err := stream.Subscribe(ctx, table)
		require.NoError(t, err)

		t.Log("===> wait subscription response via logtail stream")
		select {
		case <-ctx.Done():
			return
		case resp := <-consumer.subResponseCh:
			require.Equal(t, table.String(), resp.GetLogtail().Table.String())
		}
	}
	subFunc(tableA)
	subFunc(tableB)

	/* ---- send unsubscription request then wait response ---- */
	unsubFunc := func(table api.TableID) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		t.Log("===> send unsubscription request via logtail stream")
		err := stream.Unsubscribe(ctx, table)
		require.NoError(t, err)

		t.Log("===> wait unsubscription response via logtail stream")
		select {
		case <-ctx.Done():
			return
		case resp := <-consumer.unsubResponseCh:
			require.Equal(t, table.String(), resp.GetTable().String())
		}

	}
	unsubFunc(tableA)
	unsubFunc(tableB)

	require.Equal(t, 2, consumer.subResponseCount)
	require.Equal(t, 2, consumer.unsubResponseCount)
	require.Equal(t, 0, consumer.errorResponseCount)
	require.True(t, consumer.updateResponseCount > 0)
}

type receiver interface {
	Receive() (*LogtailResponse, error)
}

type responseConsumer struct {
	t          *testing.T
	ctx        context.Context
	cancelFunc context.CancelFunc

	receiver receiver

	subResponseCh   chan *logtail.SubscribeResponse
	unsubResponseCh chan *logtail.UnSubscribeResponse
	errorResponseCh chan *logtail.ErrorResponse

	expectFrom timestamp.Timestamp

	subResponseCount    int
	unsubResponseCount  int
	errorResponseCount  int
	updateResponseCount int
}

func newResponseConsumer(t *testing.T, receiver receiver) *responseConsumer {
	ctx, cancel := context.WithCancel(context.Background())

	return &responseConsumer{
		t:               t,
		ctx:             ctx,
		cancelFunc:      cancel,
		receiver:        receiver,
		subResponseCh:   make(chan *logtail.SubscribeResponse, 1),
		unsubResponseCh: make(chan *logtail.UnSubscribeResponse, 1),
		errorResponseCh: make(chan *logtail.ErrorResponse, 1),
	}
}

func (c *responseConsumer) stop() {
	c.cancelFunc()
}

func (c *responseConsumer) start() {
	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return

			default:
				resp, err := c.receiver.Receive()
				require.Condition(c.t, func() bool {
					return err == nil ||
						moerr.IsMoErrCode(err, moerr.ErrStreamClosed)
				})
				if err != nil {
					return
				}

				if r := resp.GetSubscribeResponse(); r != nil {
					c.subResponseCount += 1
					c.expectFrom = *r.Logtail.Ts
					select {
					case <-c.ctx.Done():
						return
					case c.subResponseCh <- r:
					}
				}

				if r := resp.GetUnsubscribeResponse(); r != nil {
					c.unsubResponseCount += 1
					select {
					case <-c.ctx.Done():
						return
					case c.unsubResponseCh <- r:
					}
				}

				if r := resp.GetError(); r != nil {
					c.errorResponseCount += 1
					select {
					case <-c.ctx.Done():
						return
					case c.errorResponseCh <- r:
					}
				}

				if r := resp.GetUpdateResponse(); r != nil {
					c.updateResponseCount += 1
					require.Equal(c.t, c.expectFrom, *r.From)
					c.expectFrom = *r.To
				}
			}
		}
	}()
}
