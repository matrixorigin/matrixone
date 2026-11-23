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
	"testing"

	"github.com/stretchr/testify/require"
)

// appendWorkspaceEntryForTest appends e the way production does: workspace
// batches live in the workspace pool. A fixture batch built in another pool
// is copied into the workspace and its original released; the copy keeps the
// fixture's *batch.Batch identity, so later references to it stay valid.
func appendWorkspaceEntryForTest(t testing.TB, txn *Transaction, e Entry) {
	t.Helper()
	if e.bat != nil && !txn.isWorkspaceCopy(e.bat) {
		copied, err := txn.copyWorkspaceBatch(e.bat)
		require.NoError(t, err)
		txn.takeWorkspaceCopy(copied)
		e.bat.CleanOwned()
		*e.bat = *copied
	}
	txn.appendWorkspaceEntryLocked(e)
}
