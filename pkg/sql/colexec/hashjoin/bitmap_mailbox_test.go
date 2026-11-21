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

func newMailboxBitmap(t *testing.T) *bitmap.Bitmap {
	t.Helper()
	value, err := colexec.NewRowBitmap(1024)
	require.NoError(t, err)
	return value
}

func TestBitmapMailboxSealOwnsQueuedAndRejectsLateTransfer(t *testing.T) {
	mp := mpool.MustNewZero()
	mailbox := NewBitmapMailbox(2)

	require.True(t, mailbox.Send(newMailboxBitmap(t)))
	mailbox.SealAndDrain(mp)
	require.Empty(t, mailbox.ch)

	require.False(t, mailbox.Send(newMailboxBitmap(t)), "a sealed mailbox rejects late transfers")
	require.Empty(t, mailbox.ch)
	require.Zero(t, mp.CurrNB())
}

func TestBitmapMailboxConcurrentSealPreservesSingleOwner(t *testing.T) {
	const workers = 16
	mp := mpool.MustNewZero()
	mailbox := NewBitmapMailbox(workers)
	values := make([]*bitmap.Bitmap, workers)
	for i := range values {
		values[i] = newMailboxBitmap(t)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(workers)
	for _, value := range values {
		go func(value *bitmap.Bitmap) {
			defer wg.Done()
			<-start
			_ = mailbox.Send(value)
		}(value)
	}
	close(start)
	mailbox.SealAndDrain(mp)
	wg.Wait()
	mailbox.SealAndDrain(mp)

	require.Empty(t, mailbox.ch)
	require.Zero(t, mp.CurrNB())
}
