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
	"testing"
	"testing/synctest"

	"github.com/matrixorigin/matrixone/pkg/txn/client"
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
				t.Cleanup(func() { require.NoError(t, c.Close()) })
				c.connector = newConnector(c, e)
				c.subscriber = newLogTailSubscriber()
				c.pauseC = make(chan bool, 1)
				c.resumeC = make(chan struct{})

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
				c.pause(reconnect)
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
				c.resume()
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
				c := &PushClient{pauseC: make(chan bool, 1), resumeC: make(chan struct{})}
				ctx := c.start(context.Background())
				t.Cleanup(func() { require.NoError(t, c.Close()) })
				c.connector = newConnector(c, nil)
				c.pause(false)
				resumed := make(chan bool, 1)
				c.runOwned(func() { resumed <- c.waitForResume(ctx, <-c.pauseC) })
				synctest.Wait()
				require.Empty(t, c.connector.signal)
				if cancelWait {
					require.NoError(t, c.Close())
					require.False(t, <-resumed)
				} else {
					c.resume()
					require.True(t, <-resumed)
					require.True(t, c.connector.first.Load(), "retry must not complete first replay")
				}
				require.Empty(t, c.connector.signal)
			})
		})
	}
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
		c.pauseC = make(chan bool, 1)
		c.resumeC = make(chan struct{})
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
