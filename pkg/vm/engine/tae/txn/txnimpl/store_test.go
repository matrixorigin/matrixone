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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWaitWalAndTailFreesAllEntriesAfterError(t *testing.T) {
	failed := entry.GetBase()
	failed.DoneWithErr(assert.AnError)
	succeeded := entry.GetBase()
	succeeded.DoneWithErr(nil)
	store := &txnStore{logs: []entry.Entry{failed, succeeded}}

	require.ErrorIs(t, store.WaitWalAndTail(context.Background()), assert.AnError)
	// Free resets an entry before returning it to the pool. Both entries must
	// be reset even though the first WaitDone returned an error.
	require.NoError(t, failed.GetError())
	require.NoError(t, succeeded.GetError())
}
