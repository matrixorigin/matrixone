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

package client

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCatalogReadStampRejectsClosedAndDetectsABA(t *testing.T) {
	runOperatorTests(t, func(ctx context.Context, op *txnOperator, _ *testTxnSender) {
		before, err := op.CatalogReadStamp()
		require.NoError(t, err)
		snapshot := before.Snapshot
		snapshot.PhysicalTime++
		op.SetSnapshotTS(snapshot)
		op.SetSnapshotTS(before.Snapshot)
		after, err := op.CatalogReadStamp()
		require.NoError(t, err)
		require.Equal(t, before.Snapshot, after.Snapshot)
		require.Greater(t, after.Revision, before.Revision)
		op.AddWorkspace(nil)
		changed, err := op.CatalogReadStamp()
		require.NoError(t, err)
		require.Greater(t, changed.Revision, after.Revision)
		op.mu.Lock()
		op.mu.closed = true
		op.mu.Unlock()
		_, err = op.CatalogReadStamp()
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnClosed))
	})
}
