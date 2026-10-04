// Copyright 2021-2026 Matrix Origin
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

package disttae

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

type countingShardingReader struct {
	closes atomic.Int32
}

func (r *countingShardingReader) Close() error {
	r.closes.Add(1)
	return nil
}

func (r *countingShardingReader) Read(
	context.Context,
	[]string,
	*plan.Expr,
	*mpool.MPool,
	*batch.Batch,
) (bool, error) {
	return false, nil
}

func (r *countingShardingReader) SetOrderBy([]*plan.OrderBySpec)       {}
func (r *countingShardingReader) GetOrderBy() []*plan.OrderBySpec      { return nil }
func (r *countingShardingReader) SetIndexParam(*plan.IndexReaderParam) {}
func (r *countingShardingReader) SetFilterZM(objectio.ZoneMap)         {}

func TestShardingRemoteReaderCloseIsIdempotent(t *testing.T) {
	reader := &countingShardingReader{}
	var releases atomic.Int32
	sr := &shardingRemoteReader{
		rd: reader,
		release: func() {
			releases.Add(1)
		},
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sr.close()
		}()
	}
	wg.Wait()

	require.Equal(t, int32(1), reader.closes.Load())
	require.Equal(t, int32(1), releases.Load())
}

func TestCloseExpiredStreamReadersReleasesOwnedProcess(t *testing.T) {
	reader := &countingShardingReader{}
	var releases atomic.Int32
	id := types.Uuid{1}
	sr := &shardingRemoteReader{
		streamID: id,
		rd:       reader,
		deadline: time.Now().Add(-time.Second),
		release: func() {
			releases.Add(1)
		},
	}

	streamHandler.Lock()
	streamHandler.streamReaders[id] = sr
	streamHandler.Unlock()
	t.Cleanup(func() {
		streamHandler.Lock()
		delete(streamHandler.streamReaders, id)
		streamHandler.Unlock()
	})

	closeExpiredStreamReaders(time.Now())
	sr.close()

	streamHandler.Lock()
	_, ok := streamHandler.streamReaders[id]
	streamHandler.Unlock()
	require.False(t, ok)
	require.Equal(t, int32(1), reader.closes.Load())
	require.Equal(t, int32(1), releases.Load())
}
