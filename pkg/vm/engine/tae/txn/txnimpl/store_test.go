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

package txnimpl

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type trackingEntry struct {
	entry.Entry
	freed atomic.Bool
}

func (e *trackingEntry) Free() {
	e.freed.Store(true)
}

func TestWaitWalAndTailFreesAllEntriesAfterError(t *testing.T) {
	failed := entry.GetBase()
	t.Cleanup(failed.Free)
	failed.DoneWithErr(assert.AnError)
	succeeded := entry.GetBase()
	t.Cleanup(succeeded.Free)
	succeeded.DoneWithErr(nil)
	failedTracked := &trackingEntry{Entry: failed}
	succeededTracked := &trackingEntry{Entry: succeeded}
	store := &txnStore{logs: []entry.Entry{failedTracked, succeededTracked}}

	require.ErrorIs(t, store.WaitWalAndTail(context.Background()), assert.AnError)
	require.True(t, failedTracked.freed.Load())
	require.True(t, succeededTracked.freed.Load(),
		"a prior WAL error must not retain ownership of later entries")
}
