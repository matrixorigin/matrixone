// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
	"github.com/stretchr/testify/require"
)

type firstConnectTxnClient struct {
	client.TxnClient
	resumes int
}

func (c *firstConnectTxnClient) Resume() { c.resumes++ }

func TestFirstConnectDisconnectHandoff(t *testing.T) {
	for _, phase := range []string{"before-completion", "delayed-pause", "after-completion"} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				tc := &firstConnectTxnClient{}
				e := &Engine{cli: tc}
				c := &e.pClient
				ctx := c.start(context.Background())
				t.Cleanup(func() {
					c.subscriber.swapClient(nil) // The fixture uses a client without a transport.
					require.NoError(t, c.Close())
				})
				c.connector = newConnector(c, e)
				c.subscriber = newLogTailSubscriber()
				c.subscriber.swapClient(&service.LogtailClient{})
				c.pauseC = make(chan *logtailPauseRequest, 1)

				if phase == "after-completion" {
					c.completeFirstConnect(e)
					require.Equal(t, 1, tc.resumes)
					require.True(t, c.subscriber.ready())
				}
				// Also cover an error that reads first=true, then loses the CPU
				// until the successful first-connect handoff has completed.
				reconnect := !c.connector.first.Load()
				if phase == "delayed-pause" {
					c.completeFirstConnect(e)
				}
				req := c.pause(reconnect)
				resumed := make(chan bool, 1)
				c.runOwned(func() { resumed <- c.waitForResume(ctx, <-c.pauseC) })
				synctest.Wait()
				if phase == "before-completion" {
					require.Empty(t, c.connector.signal)
					c.completeFirstConnect(e)
					synctest.Wait()
					require.Zero(t, tc.resumes, "failed first connection must not admit transactions")
					require.False(t, c.receivedLogTailTime.ready.Load())
					require.False(t, c.subscriber.ready())
				}
				require.False(t, c.connector.first.Load())
				require.Len(t, c.connector.signal, 1)
				<-c.connector.signal
				synctest.Wait()
				require.Empty(t, c.connector.signal, "closed replay notification must not repeatedly reconnect")
				require.True(t, c.resume(req))
				require.True(t, <-resumed)
				require.False(t, c.mu.paused)
			})
		})
	}
}

func TestFirstConnectPauseRetryAndCancel(t *testing.T) {
	for _, cancelWait := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry-before-replay", true: "owner-cancel"}[cancelWait], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := &PushClient{pauseC: make(chan *logtailPauseRequest, 1)}
				ctx := c.start(context.Background())
				t.Cleanup(func() { require.NoError(t, c.Close()) })
				c.connector = newConnector(c, nil)
				req := c.pause(false)
				resumed := make(chan bool, 1)
				c.runOwned(func() { resumed <- c.waitForResume(ctx, <-c.pauseC) })
				synctest.Wait()
				require.Empty(t, c.connector.signal)
				if cancelWait {
					require.NoError(t, c.Close())
					require.False(t, <-resumed)
				} else {
					require.True(t, c.resume(req))
					require.True(t, <-resumed)
					require.True(t, c.connector.first.Load(), "retry must not complete first replay")
				}
				require.Empty(t, c.connector.signal)
			})
		})
	}
}

func TestPauseAcknowledgementAndStaleResume(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := &PushClient{pauseC: make(chan *logtailPauseRequest, 1)}
		ctx := c.start(context.Background())
		t.Cleanup(func() { require.NoError(t, c.Close()) })
		c.connector = newConnector(c, nil)
		first := c.pause(false)
		require.Same(t, first, c.pause(false))
		require.False(t, c.resume(first), "receiver must acknowledge before resume")
		firstDone := make(chan bool, 1)
		c.runOwned(func() { firstDone <- c.waitForResume(ctx, <-c.pauseC) })
		synctest.Wait()
		require.True(t, c.resume(first))
		require.True(t, <-firstDone)

		second := c.pause(false)
		secondDone := make(chan bool, 1)
		c.runOwned(func() { secondDone <- c.waitForResume(ctx, <-c.pauseC) })
		synctest.Wait()
		require.False(t, c.resume(first), "an old resume cannot release a later pause")
		require.True(t, c.resume(second))
		require.True(t, <-secondDone)
	})
}

func TestPauseRejectsRetiredConsumerError(t *testing.T) {
	c := &PushClient{pauseC: make(chan *logtailPauseRequest, 1), subscriber: newLogTailSubscriber()}
	old, current := &service.LogtailClient{}, &service.LogtailClient{}
	c.subscriber.swapClient(old)
	_, oldGeneration := c.subscriber.clientAndGeneration()
	c.subscriber.swapClient(current)
	_, currentGeneration := c.subscriber.clientAndGeneration()
	require.Nil(t, c.pauseIfCurrent(old, oldGeneration, true))
	require.Empty(t, c.pauseC)
	req := c.pauseIfCurrent(current, currentGeneration, true)
	require.NotNil(t, req)
	require.Same(t, req, <-c.pauseC)
	c.subscriber.swapClient(nil)
	require.NoError(t, c.Close())
}

func TestPausedReceiverStillWaitsForConsumerJoin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := &PushClient{pauseC: make(chan *logtailPauseRequest, 1), consumeErrC: make(chan logtailConsumerError, 1)}
		ctx := c.start(context.Background())
		t.Cleanup(func() { require.NoError(t, c.Close()) })
		c.connector = newConnector(c, nil)
		rc := c.createRoutineToConsumeLogTails(ctx, 0, 1, nil)
		c.receiver = []*routineController{rc}
		started, release := make(chan struct{}), make(chan struct{})
		require.True(t, rc.send(failingConsumerCommand(func() error {
			close(started)
			<-release
			return nil
		})))
		<-started
		req := c.pause(false)
		resumed := make(chan bool, 1)
		c.runOwned(func() { resumed <- c.waitForResume(ctx, <-c.pauseC) })
		synctest.Wait()
		require.True(t, waitLogtailPause(ctx, req))
		joined := make(chan struct{})
		go func() { c.stopConsumers(); close(joined) }()
		synctest.Wait()
		select {
		case <-joined:
			t.Fatal("receiver acknowledgement did not join the blocked consumer")
		default:
		}
		close(release)
		<-joined
		require.True(t, c.resume(req))
		require.True(t, <-resumed)
	})
}

func TestPauseWaitsForDirectCatalogDispatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		part := logtailreplay.NewPartition("", nil, 0, catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID, nil)
		require.NoError(t, part.Lock(context.Background()))
		e := &Engine{partitions: map[[2]uint64]*logtailreplay.Partition{{catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID}: part}}
		c := &e.pClient
		ctx := c.start(context.Background())
		e.globalStats = &GlobalStats{ctx: ctx, tailC: make(chan *logtail.TableLogtail, 1)}
		c.subscribed.m = make(map[uint64]*subEntry)
		c.receivedLogTailTime.initLogTailTimestamp(nil)
		c.connector = newConnector(c, e)
		c.subscriber = newLogTailSubscriber()
		c.pauseC = make(chan *logtailPauseRequest, 1)
		stream := newCloseRaceStream()
		lc, err := service.NewLogtailClient(ctx, stream)
		require.NoError(t, err)
		c.subscriber.swapClient(lc)
		t.Cleanup(func() { require.NoError(t, c.Close()) })
		c.runOwned(func() { c.receiveLogtails(ctx, e) })
		ts := timestamp.Timestamp{PhysicalTime: 1}
		response := &service.LogtailResponse{LogtailResponse: logtail.LogtailResponse{
			Response: &logtail.LogtailResponse_SubscribeResponse{SubscribeResponse: &logtail.SubscribeResponse{
				Logtail: logtail.TableLogtail{Ts: &ts, Table: &api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_DATABASE_ID, DbName: catalog.MO_CATALOG, TbName: catalog.MO_DATABASE}},
			}},
		}}
		payload, err := response.Marshal()
		require.NoError(t, err)
		stream.recv <- &service.LogtailResponseSegment{MessageSegment: logtail.MessageSegment{
			MessageSize: int32(len(payload)), Sequence: 1, MaxSequence: 1, Payload: payload,
		}}
		synctest.Wait() // Real direct catalog dispatch waits for the partition lock.
		req := c.pause(false)
		synctest.Wait()
		select {
		case <-req.ack:
			part.Unlock()
			t.Fatal("receiver acknowledged while catalog dispatch was blocked")
		default:
		}
		part.Unlock()
		synctest.Wait()
		select {
		case <-req.ack:
		default:
			t.Fatal("receiver did not acknowledge after catalog dispatch finished")
		}
	})
}

func TestFirstConnectReceiverStreamFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tc := &firstConnectTxnClient{}
		e := &Engine{cli: tc}
		c := &e.pClient
		ctx := c.start(context.Background())
		t.Cleanup(func() { require.NoError(t, c.Close()) })
		c.connector = newConnector(c, e)
		c.subscriber = newLogTailSubscriber()
		c.pauseC = make(chan *logtailPauseRequest, 1)
		stream := newCloseRaceStream()
		lc, err := service.NewLogtailClient(ctx, stream)
		require.NoError(t, err)
		c.subscriber.swapClient(lc)
		c.runOwned(func() { c.receiveLogtails(ctx, e) })
		require.NoError(t, stream.Close(true))
		synctest.Wait()
		require.True(t, c.mu.paused)
		require.Empty(t, c.connector.signal)
		c.completeFirstConnect(e)
		synctest.Wait()
		require.Len(t, c.connector.signal, 1, "real receiver must request recovery after first replay")
		require.Zero(t, tc.resumes)
		require.False(t, c.subscriber.ready())
	})
}

func TestDisconnectPausesBeforeDetachingClient(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tc := &firstConnectTxnClient{}
		e := &Engine{cli: tc}
		c := &e.pClient
		ctx := c.start(context.Background())
		t.Cleanup(func() { require.NoError(t, c.Close()) })
		c.connector = newConnector(c, e)
		c.subscriber = newLogTailSubscriber()
		c.pauseC = make(chan *logtailPauseRequest, 1)
		stream := newCloseRaceStream()
		lc, err := service.NewLogtailClient(ctx, stream)
		require.NoError(t, err)
		c.subscriber.swapClient(lc)
		c.runOwned(func() { c.receiveLogtails(ctx, e) })
		require.NoError(t, c.Disconnect())
		synctest.Wait()
		c.mu.Lock()
		req := c.mu.pause
		c.mu.Unlock()
		require.NotNil(t, req)
		select {
		case <-req.ack:
		default:
			t.Fatal("receiver did not acknowledge the disconnect pause")
		}
		require.Nil(t, c.subscriber.client())
		c.completeFirstConnect(e)
		synctest.Wait()
		require.Len(t, c.connector.signal, 1)
	})
}

func TestFirstConnectCannotPublishReadyAfterClose(t *testing.T) {
	tc := &firstConnectTxnClient{}
	e := &Engine{cli: tc}
	c := &e.pClient
	c.connector = newConnector(c, e)
	c.subscriber = newLogTailSubscriber()
	c.subscriber.setReady()
	c.receivedLogTailTime.ready.Store(true)
	require.NoError(t, c.Close())
	c.completeFirstConnect(e)
	require.Zero(t, tc.resumes)
	require.False(t, c.subscriber.ready())
	require.False(t, c.receivedLogTailTime.ready.Load())
}

func TestCloseCancelsInitBeforeWaitingForDisconnect(t *testing.T) {
	c := &PushClient{subscriber: newLogTailSubscriber()}
	ctx := c.start(context.Background())
	started := make(chan struct{})
	c.LogtailRPCClientFactory = func(ctx context.Context, _, _ string, _ morpc.RPCClient) (morpc.RPCClient, morpc.Stream, error) {
		close(started)
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	initDone := make(chan error, 1)
	go func() { initDone <- c.init(ctx, "addr", nil, &Engine{}) }()
	<-started
	disconnected := make(chan error, 1)
	go func() { disconnected <- c.Disconnect() }()
	deadline := time.After(5 * time.Second)
	for c.mu.TryLock() {
		c.mu.Unlock()
		select {
		case <-deadline:
			c.cancel()
			t.Fatal("disconnect did not acquire its control lock")
		default:
			runtime.Gosched()
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	for _, result := range []struct {
		name string
		ch   <-chan error
	}{
		{"init", initDone}, {"disconnect", disconnected}, {"close", closed},
	} {
		select {
		case err := <-result.ch:
			if result.name == "init" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
		case <-time.After(5 * time.Second):
			c.cancel()
			t.Fatalf("%s did not finish after owner cancellation", result.name)
		}
	}
}
