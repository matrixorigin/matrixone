// Copyright 2024 Matrix Origin
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

package pipelineSpool

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/spool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// TestPipelineBatchSenderToLocal_SendBatch test the basic function for PipelineBatchSenderToLocal.
func TestPipelineBatchSenderToLocal_SendBatch(t *testing.T) {
	proc := testutil.NewProcess()
	maxBatchCanSentInSameTime := 1

	s, cs := spool.New[BatchMessage](int64(1), 1)
	sender := &PipelineBatchSenderToLocal{
		sp:    s,
		cs:    cs,
		cache: initCachedBatch(proc.Mp(), maxBatchCanSentInSameTime),
	}

	// 1. SendBatch should do copy when the batch is not nil and not Empty.
	originBat := batch.NewWithSize(1)
	originBat.Vecs[0] = testutil.NewInt64Vector(5, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3, 4, 5})
	originBat.SetRowCount(5)

	queryDone, err := sender.SendBatch(proc.Ctx, SendToAllLocal, originBat, nil)
	require.False(t, queryDone)
	require.NoError(t, err)

	message, ok := sender.cs[0].Next()
	require.True(t, ok)
	require.NotEqual(t, originBat, message.content)

	originBat.Clean(proc.Mp())
	receivedBat := message.content
	require.Equal(t, 5, receivedBat.RowCount())
	require.Equal(t, 1, len(receivedBat.Vecs))
	vs := vector.MustFixedCol[int64](receivedBat.Vecs[0])
	require.Equal(t, 5, len(vs))
	require.Equal(t, int64(1), vs[0])
	require.Equal(t, int64(2), vs[1])
	require.Equal(t, int64(3), vs[2])
	require.Equal(t, int64(4), vs[3])
	require.Equal(t, int64(5), vs[4])

	// 2. There is no free space in the cache until the receiver consumes the batch.
	require.Equal(t, maxBatchCanSentInSameTime-1, len(sender.cache.freeBatchPointer))

	originBat2 := batch.NewWithSize(1)
	originBat2.Vecs[0] = testutil.NewInt64Vector(5, types.T_int64.ToType(), proc.Mp(), false, []int64{6, 7, 8, 9, 10})
	originBat2.SetRowCount(5)

	c, cancel := context.WithTimeout(context.TODO(), 100*time.Millisecond)
	queryDone, err = sender.SendBatch(c, SendToAllLocal, originBat2, nil)
	require.NoError(t, err)
	require.True(t, queryDone)
	cancel()
	originBat2.Clean(proc.Mp())

	cs[0].Close()
	require.Equal(t, maxBatchCanSentInSameTime, len(sender.cache.freeBatchPointer))

	sender.sp.Close()
	sender.cache.Free()
	// 3. after all, the mp should be empty.
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestPipelineSpoolBehavior1 tests the behavior for one sender and one receiver.
func TestPipelineSpoolBehavior1(t *testing.T) {
	proc := testutil.NewProcess()
	sender, cursor := NewSenderToLocalPipeline(proc.Mp(), 1, 1, 2)

	originBat := batch.NewWithSize(1)
	originBat.Vecs[0] = testutil.NewInt64Vector(5, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3, 4, 5})
	originBat.SetRowCount(5)

	done, err := sender.SendBatch(context.TODO(), SendToAllLocal, originBat, nil)
	require.NoError(t, err)
	require.False(t, done)

	receivedBat0, err := cursor[0].ReceiveBatch()
	require.NoError(t, err)
	{
		require.True(t, originBat != receivedBat0)
		require.Equal(t, 5, receivedBat0.RowCount())
		require.Equal(t, 1, len(receivedBat0.Vecs))
		vs := vector.MustFixedCol[int64](receivedBat0.Vecs[0])
		require.Equal(t, 5, len(vs))
		require.Equal(t, int64(1), vs[0])
		require.Equal(t, int64(2), vs[1])
		require.Equal(t, int64(3), vs[2])
		require.Equal(t, int64(4), vs[3])
		require.Equal(t, int64(5), vs[4])
	}

	timeout, cancel := context.WithTimeout(context.TODO(), 500*time.Millisecond)
	done, err = sender.SendBatch(timeout, SendToAnyLocal, originBat, nil)
	require.NoError(t, err)
	require.False(t, done)

	var b *batch.Batch
	{
		b, err = cursor[0].ReceiveBatch()
		require.NoError(t, err)
		require.True(t, b != nil && b != originBat)
	}
	cancel()

	timeout, cancel = context.WithTimeout(context.TODO(), 100*time.Millisecond)
	done, err = sender.SendBatch(timeout, 0, originBat, nil)
	require.NoError(t, err)
	require.False(t, done)

	{
		b, err = cursor[0].ReceiveBatch()
		require.NoError(t, err)
		require.True(t, b != nil && b != originBat)
	}
	cancel()

	originBat.Clean(proc.Mp())
	go cursor[0].Close()
	sender.Close()

	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestPipelineSpoolBehavior2 tests the behavior for one sender and multiple receiver.
func TestPipelineSpoolBehavior2(t *testing.T) {
	proc := testutil.NewProcess()
	sender, css := NewSenderToLocalPipeline(proc.Mp(), 1, 2, 4)

	// test send to any.
	{
		originBat := batch.NewWithSize(1)
		originBat.Vecs[0] = testutil.NewInt64Vector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{1})
		originBat.SetRowCount(1)

		d, err := sender.SendBatch(context.TODO(), SendToAnyLocal, originBat, nil)
		require.NoError(t, err)
		require.False(t, d)

		var b *batch.Batch
		// css[0] can get the input.
		b, err = css[0].ReceiveBatch()
		require.NoError(t, err)
		require.True(t, b != nil && b != originBat)
		require.Equal(t, int64(1), vector.MustFixedCol[int64](originBat.Vecs[0])[0])

		vs := vector.MustFixedCol[int64](originBat.Vecs[0])
		vs[0] = 2
		d, err = sender.SendBatch(context.TODO(), SendToAnyLocal, originBat, nil)
		require.NoError(t, err)
		require.False(t, d)
		// css[1] can get the input
		b, err = css[0].ReceiveBatch()
		require.NoError(t, err)
		require.True(t, b != nil && b != originBat)
		require.Equal(t, int64(2), vector.MustFixedCol[int64](originBat.Vecs[0])[0])

		originBat.Clean(proc.Mp())
	}

	// test send to all.
	{
		originBat := batch.NewWithSize(1)
		originBat.Vecs[0] = testutil.NewInt64Vector(1, types.T_int64.ToType(), proc.Mp(), false, []int64{1})
		originBat.SetRowCount(1)

		d, err := sender.SendBatch(context.TODO(), SendToAllLocal, originBat, nil)
		require.NoError(t, err)
		require.False(t, d)

		var b *batch.Batch
		// css[0] and css[1] can get the input.
		b, err = css[0].ReceiveBatch()
		require.NoError(t, err)
		require.True(t, b != nil && b != originBat)
		require.Equal(t, int64(1), vector.MustFixedCol[int64](originBat.Vecs[0])[0])

		b, err = css[1].ReceiveBatch()
		require.NoError(t, err)
		require.True(t, b != nil && b != originBat)
		require.Equal(t, int64(1), vector.MustFixedCol[int64](originBat.Vecs[0])[0])

		originBat.Clean(proc.Mp())
	}

	go func() {
		css[0].Close()
		css[1].Close()
	}()
	sender.Close()

	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestPipelineSpoolBehavior3 test the behavior for multi-sender and one receiver.
func TestPipelineSpoolBehavior3(t *testing.T) {
	proc := testutil.NewProcess()
	s, cursor := NewSenderToLocalPipeline(proc.Mp(), 2, 1, 5)

	{
		// test s close.
		// s should close twice.
		tStart := time.Now()

		go func() {
			s.Close()
			time.Sleep(1 * time.Second)
			s.Close()
		}()

		cursor[0].Close()
		duration := time.Since(tStart)
		require.Greater(t, duration, 100*time.Millisecond)
	}
}
