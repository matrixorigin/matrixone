// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// TestPkMoveRewriteReclaimsPersistedOutputUnderCancellation pins r28
// review F3 (D122), cancellation half: a rewrite that persisted its
// replacement object and then observes a commit-ambiguous cancellation
// must reclaim the unpublished file on a context decoupled from the
// request — production object stores reject a cancelled context, so the
// pre-D122 reclaim (run on the request context) left the file behind
// and discarded the inventory.
//
// The injection arms the store write of the object payload: the write
// completes (the object file is on disk), the request context is
// cancelled, and the write caller observes context.Canceled — exactly
// the commit-ambiguous Sync the r28 review describes. The store wrapper
// rejects deletes whose context is cancelled, so on the pre-fix defer
// the file would survive with no cleanup owner.
func TestPkMoveRewriteReclaimsPersistedOutputUnderCancellation(t *testing.T) {
	ctx := context.Background()
	proc, inj, base := newR28SharedProc(t)
	fx := newR28Fixture(t, proc)

	_, tbl := fx.newTxnTbl(t)
	origStats := fx.writeObj(t, tbl, []int64{1, 2, 3})
	require.Len(t, origStats, 1)
	origBlk := objectio.NewBlockidWithObjectID(origStats[0].ObjectName().ObjectId(), 0)
	deletions := map[objectio.Blockid][]int64{origBlk: {0}}
	before := fsInventory(t, base)

	reqCtx, cancel := context.WithCancel(ctx)
	inj.writeCancelOnce = cancel

	_, _, orphans, _, err := tbl.rewriteObjectByDeletion(reqCtx, origStats[0], deletions)
	require.Error(t, err)
	require.ErrorContains(t, err, "context canceled",
		"the commit-ambiguous Sync must surface the cancellation")
	require.Nil(t, orphans,
		"D122: a successful cancellation-decoupled reclaim hands nothing to the caller")
	require.Equal(t, before, fsInventory(t, base),
		"the persisted replacement is reclaimed despite the cancelled request ctx")

	records := inj.deleteAttempts()
	require.NotEmpty(t, records,
		"the writer reclaim must issue a delete for the persisted replacement")
	for _, rec := range records {
		require.NoError(t, rec.ctxErr,
			"D122: the reclaim delete must run on a context free of the request cancellation")
	}
}
