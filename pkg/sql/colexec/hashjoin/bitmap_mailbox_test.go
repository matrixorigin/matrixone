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

package hashjoin

import (
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

func newMailboxAccountedBitmap(
	t *testing.T,
	mp *mpool.MPool,
	account *mpool.AllocationAccount,
) *bitmap.Bitmap {
	t.Helper()
	value, err := colexec.NewAccountedBitmap(
		1024,
		mp,
		account,
		mpool.AllocationOwnerHashBuild,
		hashJoinAllocationSiteMatchedRows,
	)
	require.NoError(t, err)
	return value
}

func TestBitmapMailboxSealOwnsQueuedAndRejectsLateTransfer(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	mailbox := NewBitmapMailbox(2)

	queued := newMailboxAccountedBitmap(t, mp, account)
	require.True(t, mailbox.Send(queued))
	mailbox.SealAndDrain(mp)
	require.Zero(t, account.Snapshot().Used)
	require.Empty(t, mailbox.ch)

	late := newMailboxAccountedBitmap(t, mp, account)
	require.False(t, mailbox.Send(late))
	require.NotZero(t, account.Snapshot().Used)
	colexec.FreeAccountedBitmap(late, mp)
	require.Zero(t, account.Snapshot().Used)

	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
	require.Zero(t, mp.CurrNB())
}

func TestBitmapMailboxConcurrentSealPreservesSingleOwner(t *testing.T) {
	const workers = 16
	registry, err := mpool.NewAllocationAccountRegistry(1, workers+1)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	mailbox := NewBitmapMailbox(workers)
	values := make([]*bitmap.Bitmap, workers)
	for i := range values {
		values[i] = newMailboxAccountedBitmap(t, mp, account)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(workers)
	for _, value := range values {
		go func(value *bitmap.Bitmap) {
			defer wg.Done()
			<-start
			if !mailbox.Send(value) {
				colexec.FreeAccountedBitmap(value, mp)
			}
		}(value)
	}
	close(start)
	mailbox.SealAndDrain(mp)
	wg.Wait()
	mailbox.SealAndDrain(mp)

	require.Zero(t, account.Snapshot().Used)
	require.Empty(t, mailbox.ch)
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
	require.Zero(t, mp.CurrNB())
}
