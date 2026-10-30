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

package iscp

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

const testFulltext2CancelAlgo = "test-fulltext2-cancel"

type fulltext2CancelHooks struct{}

func (fulltext2CancelHooks) NewSqlWriter(JobID, *ConsumerInfo, *plan.TableDef, []*plan.IndexDef) (IndexSqlWriter, error) {
	return newFT2Writer(fulltext2.ParserNgram), nil
}

func (fulltext2CancelHooks) Run(c *IndexConsumer, ctx context.Context, errch chan error, r DataRetriever) {
	RunFulltext2(c, ctx, errch, r)
}

func init() {
	Register(testFulltext2CancelAlgo, fulltext2CancelHooks{})
}

// The retriever's context can outlive the consumer context. It must receive
// the worker's cancellation error to release a producer blocked in Next.
type fulltext2BlockedRetriever struct {
	nextEntered chan struct{}
	errCh       chan error
	nextOnce    sync.Once
	errOnce     sync.Once
}

func (r *fulltext2BlockedRetriever) Next() *ISCPData {
	r.nextOnce.Do(func() { close(r.nextEntered) })
	return &ISCPData{noMoreData: true, err: <-r.errCh}
}

func (r *fulltext2BlockedRetriever) SetError(err error) {
	r.errOnce.Do(func() { r.errCh <- err })
}

func (*fulltext2BlockedRetriever) GetToTS() types.TS { return types.TS{} }
func (*fulltext2BlockedRetriever) UpdateWatermark(context.Context, string, client.TxnOperator) error {
	return nil
}
func (*fulltext2BlockedRetriever) GetDataType() int8    { return ISCPDataType_Snapshot }
func (*fulltext2BlockedRetriever) GetAccountID() uint32 { return 0 }
func (*fulltext2BlockedRetriever) GetTableID() uint64   { return 0 }

func TestFulltext2ConsumerContextCancelUnblocksRetrieverNext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	writer := newFT2Writer(fulltext2.ParserNgram)
	writer.capacity, writer.postingCap = 64, 1024
	consumer := &IndexConsumer{sqlWriter: writer, algo: testFulltext2CancelAlgo}
	retriever := &fulltext2BlockedRetriever{
		nextEntered: make(chan struct{}),
		errCh:       make(chan error, 1),
	}
	done := make(chan error, 1)
	joined := false
	t.Cleanup(func() {
		cancel()
		retriever.SetError(context.Canceled)
		if !joined {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("Fulltext2 Consume goroutine did not stop during cleanup")
			}
		}
	})
	go func() { done <- consumer.Consume(ctx, retriever) }()
	select {
	case <-retriever.nextEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("producer did not enter DataRetriever.Next")
	}
	cancel()
	select {
	case err := <-done:
		joined = true
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Fulltext2 Consume stayed blocked in DataRetriever.Next after cancellation")
	}
}
