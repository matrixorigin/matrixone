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

package morpc

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestControlStream(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
		defer cancel()

		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		n := 10
		rs.RegisterRequestHandler(
			func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
				go func() {
					ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond*100)
					defer cancel()

					for i := 0; i < n; i++ {
						assert.NoError(t, cs.Write(ctx, request.Message, WriteOptions{}))
					}
				}()
				return nil
			})

		st, err := c.NewStream(testAddr, DefaultStreamOptions().WithEnableControl())
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, st.Close())
		}()

		req := newTestMessage(st.ID())
		assert.NoError(t, st.Send(ctx, req, WriteOptions{}))

		ch, err := st.Receive()
		require.NoError(t, err)

		for i := 0; i < n; i++ {
			require.NoError(t, st.Resume(ctx))
			<-ch
		}
	})
}

func TestControlStreamWithWriteTwice(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
		defer cancel()

		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		var wg sync.WaitGroup
		wg.Add(1)
		rs.RegisterRequestHandler(
			func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
				go func() {
					defer wg.Done()

					ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond*100)
					defer cancel()
					require.NoError(t, cs.Write(ctx, request.Message, WriteOptions{}))

					require.Error(t, cs.Write(ctx, request.Message, WriteOptions{}))
				}()
				return nil
			})

		st, err := c.NewStream(testAddr, DefaultStreamOptions().WithEnableControl())
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, st.Close())
		}()

		req := newTestMessage(st.ID())
		assert.NoError(t, st.Send(ctx, req, WriteOptions{}))
		require.NoError(t, st.Resume(ctx))

		ch, err := st.Receive()
		require.NoError(t, err)
		<-ch

		wg.Wait()
	})
}

func TestControlStreamWithChunkWrites(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
		defer cancel()

		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		var wg sync.WaitGroup
		wg.Add(1)
		n := 10
		rs.RegisterRequestHandler(
			func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
				go func() {
					defer wg.Done()

					ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond*100)
					defer cancel()

					for i := 0; i < n; i++ {
						assert.NoError(t, cs.Write(ctx, request.Message, WriteOptions{}.Chunk(i == n-1)))
					}

					assert.Error(t, cs.Write(ctx, request.Message, WriteOptions{}))
				}()
				return nil
			})

		st, err := c.NewStream(testAddr, DefaultStreamOptions().WithEnableControl())
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, st.Close())
		}()

		req := newTestMessage(st.ID())
		assert.NoError(t, st.Send(ctx, req, WriteOptions{}))

		ch, err := st.Receive()
		require.NoError(t, err)

		require.NoError(t, st.Resume(ctx))
		for i := 0; i < n; i++ {
			<-ch
		}
		wg.Wait()
	})
}

func TestControlStreamWithCannotWriteInPause(t *testing.T) {
	testRPCServer(t, func(rs *server) {
		var wg sync.WaitGroup
		wg.Add(1)

		ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
		defer cancel()

		c := newTestClient(t)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		rs.RegisterRequestHandler(
			func(_ context.Context, request RPCMessage, _ uint64, cs ClientSession) error {
				go func() {
					defer wg.Done()

					ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond*100)
					defer cancel()
					require.Error(t, cs.Write(ctx, request.Message, WriteOptions{}))
				}()
				return nil
			})

		st, err := c.NewStream(testAddr, DefaultStreamOptions().WithEnableControl())
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, st.Close())
		}()

		req := newTestMessage(st.ID())
		assert.NoError(t, st.Send(ctx, req, WriteOptions{}))

		wg.Wait()
	})
}
