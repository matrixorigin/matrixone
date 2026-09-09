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

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// failClosedTxnTable builds a txnTable whose workspace holds an uncommitted write
// to its own table, so SourceCommitTS fails closed before touching partition state.
func failClosedTxnTable(t *testing.T) *txnTable {
	t.Helper()
	proc := testutil.NewProc(t)
	txn := &Transaction{engine: &Engine{fs: proc.GetFileService()}}
	bat := batch.NewWithSize(1)
	bat.SetRowCount(1)
	txn.writes = []Entry{{tableId: 42, bat: bat}}
	op := newTxnOperatorForTestWithWorkspace(t, txn)
	tbl := &txnTable{tableId: 42, db: &txnDatabase{op: op}}
	tbl.proc.Store(proc)
	return tbl
}

// A transaction-local (uncommitted) write to the source table cannot have reached
// the async index, so SourceCommitTS must fail closed rather than report a bound.
func TestSourceCommitTSFailsClosedOnLocalWrite(t *testing.T) {
	_, err := failClosedTxnTable(t).SourceCommitTS(context.Background())
	require.Error(t, err)
}

// combinedTxnTable delegates to its primary.
func TestCombinedTxnTableSourceCommitTS(t *testing.T) {
	ct := &combinedTxnTable{primary: failClosedTxnTable(t)}
	_, err := ct.SourceCommitTS(context.Background())
	require.Error(t, err)
}

// txnTableDelegate routes to origin, or to the combined table when combined.is.
func TestTxnTableDelegateSourceCommitTS(t *testing.T) {
	origin := &txnTableDelegate{origin: failClosedTxnTable(t)}
	_, err := origin.SourceCommitTS(context.Background())
	require.Error(t, err)

	combined := &txnTableDelegate{}
	combined.combined.is = true
	combined.combined.tbl = &combinedTxnTable{primary: failClosedTxnTable(t)}
	_, err = combined.SourceCommitTS(context.Background())
	require.Error(t, err)
}
