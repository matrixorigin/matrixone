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

package pSpool

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestPublicationFailureBelongsToSpoolGenerationAndPreservesBorrowers(t *testing.T) {
	sourceMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(sourceMP) })
	source := spoolOwnerInput(t, sourceMP, 1)
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	sp := InitMyPipelineSpool(mp, 2)
	t.Cleanup(sp.CloseAfterReadersStopped)
	done, err := sp.SendBatch(context.Background(), SendToAllLocal, source, nil)
	require.NoError(t, err)
	require.False(t, done)
	borrowed, err := sp.ReceiveBatch(0)
	require.NoError(t, err)
	sp.DiscardUnsent(1)
	sp.ForceCleanupAfterTerminalSignal()
	require.Positive(t, mp.AllocatedBytes())
	require.Equal(t, []int64{11, 12}, vector.MustFixedColNoTypeCheck[int64](borrowed.Vecs[0]))
	sp.ReleaseCurrent(0)
	require.Zero(t, mp.AllocatedBytes(), "the unnotified recipient was rolled back; final borrower release retires its data")
	// Both readers have now stopped. Final retirement does not synthesize a
	// spool End or restore the rejected recipient's reference.
	sp.CloseAfterReadersStopped()
	require.Zero(t, mp.AllocatedBytes())
	sp.CloseAfterReadersStopped()
	require.Zero(t, mp.AllocatedBytes())

	// Reusing the same memory pool creates an independent generation. The
	// rejected notification cannot consume data or terminal in that generation.
	next := InitMyPipelineSpool(mp, 1)
	t.Cleanup(next.CloseAfterReadersStopped)
	done, err = next.SendBatch(context.Background(), 0, source, nil)
	require.NoError(t, err)
	require.False(t, done)
	got, err := next.ReceiveBatch(0)
	require.NoError(t, err)
	require.Equal(t, []int64{11, 12}, vector.MustFixedColNoTypeCheck[int64](got.Vecs[0]))
	next.ReleaseCurrent(0)
	done, err = next.SendBatch(context.Background(), 0, nil, nil)
	require.NoError(t, err)
	require.False(t, done)
	got, err = next.ReceiveBatch(0)
	require.NoError(t, err)
	require.Nil(t, got)
	next.ForceCleanupAfterTerminalSignal()
	require.Zero(t, mp.AllocatedBytes())
	next.CloseAfterReadersStopped()
	require.Zero(t, mp.AllocatedBytes())
}
