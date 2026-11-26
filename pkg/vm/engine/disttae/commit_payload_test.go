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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/stretchr/testify/require"
)

// Released commit payload buffers are reused by later commits, up to a
// bound; beyond it, and past the retained size, they are freed.
func TestCommitPayloadBuffersAreRecycled(t *testing.T) {
	mp := mpool.MustNewZero()
	var p payloadBuffers
	newBuf := func(size int) *mpool.MessageBuffer {
		buf := mpool.NewMessageBuffer(mp, maxRetainedCommitPayload)
		_, err := buf.Resize(size)
		require.NoError(t, err)
		return &buf
	}

	small := newBuf(100)
	p.put(small)
	require.Same(t, small, p.get(), "a released buffer is reused")
	require.Zero(t, small.Len(), "and starts a new payload")

	big := newBuf(maxRetainedCommitPayload + 1)
	p.put(big)
	require.Zero(t, p.get().Len())

	for i := 0; i < maxFreeCommitPayloads+3; i++ {
		p.put(newBuf(10))
	}
	require.Len(t, p.free, maxFreeCommitPayloads)
	for _, buf := range p.free {
		buf.Free()
	}
	small.Free()
	require.Zero(t, mp.CurrNB(), "dropped buffers were freed")
}

// A transaction holds one payload buffer until a terminal hook releases it;
// releasing twice is harmless.
func TestTransactionCommitPayloadLifetime(t *testing.T) {
	var txn Transaction
	first := commitPayloads.get()
	txn.holdCommitPayload(first)
	second := commitPayloads.get()
	txn.holdCommitPayload(second)
	require.Same(t, second, txn.commitPayload.Load())

	txn.releaseCommitPayload()
	require.Nil(t, txn.commitPayload.Load())
	txn.releaseCommitPayload()
	got := []*mpool.MessageBuffer{commitPayloads.get(), commitPayloads.get()}
	require.ElementsMatch(t, []*mpool.MessageBuffer{first, second}, got)
	for _, buf := range got {
		commitPayloads.put(buf)
	}
}

// A table's logtail queued to a consumer routine is detached from the
// receive buffer, which the client reuses once dispatch returns.
func TestDetachTableLogtailOwnsBatches(t *testing.T) {
	entry := api.Entry{TableName: "t", Bat: api.NewEncodedBatch([]byte("rows"))}
	wire, err := (&logtail.TableLogtail{Commands: []api.Entry{entry, {TableName: "empty"}}}).Marshal()
	require.NoError(t, err)
	var tail logtail.TableLogtail
	require.NoError(t, tail.Unmarshal(wire))
	require.True(t, tail.Commands[0].Bat.IsView())

	detachTableLogtail(&tail)
	clear(wire)
	require.False(t, tail.Commands[0].Bat.IsView())
	require.Equal(t, []byte("rows"), tail.Commands[0].Bat.Bytes())
	require.Zero(t, tail.Commands[1].Bat.Len())
}

// Commits finishing concurrently share the recycle list without races,
// losses or leaks.
func TestCommitPayloadBuffersConcurrent(t *testing.T) {
	mp := mpool.MustNewZero()
	old := commitPayloadPool
	commitPayloadPool = mp
	defer func() { commitPayloadPool = old }()
	var p payloadBuffers
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				buf := p.get()
				require.Same(t, mp, buf.Pool())
				data, err := buf.Resize(64 + (g*i)%4096)
				require.NoError(t, err)
				data[0] = byte(g)
				p.put(buf)
			}
		}(g)
	}
	wg.Wait()
	require.NotZero(t, mp.CurrNB())
	require.LessOrEqual(t, len(p.free), maxFreeCommitPayloads)
	for _, buf := range p.free {
		buf.Free()
	}
	require.Zero(t, mp.CurrNB())
}

// genWriteReqs marshals the payload once, into the transaction's buffer:
// the payload is a copy of the workspace, which may change afterwards.
func TestGenWriteReqsPayloadIsInTransactionBuffer(t *testing.T) {
	txn := newCatalogCreateTxnForTest(t)
	addActiveDatabaseCreateForTest(txn, 7, "db", 42, 1)
	appendCreateDatabaseTupleForTest(t, txn, 7, "db", 42, 7, "db", false)
	reqs, err := genWriteReqs(context.Background(), txn)
	require.NoError(t, err)
	buf := txn.commitPayload.Load()
	require.NotNil(t, buf)
	payload := reqs[0].CNRequest.Payload
	require.True(t, &payload[0] == &buf.Bytes()[0], "the payload is the buffer's message")

	cleanTxnWritesForTest(txn) // frees the workspace batches and the payload
	require.Nil(t, txn.commitPayload.Load())
}
