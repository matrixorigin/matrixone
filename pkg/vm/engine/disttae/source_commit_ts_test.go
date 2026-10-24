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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type sourceCommitTSStub struct {
	engine.Relation
	ts    types.TS
	err   error
	calls *int
}

type sourceCommitTSAfterCopyStub struct {
	engine.Relation
	ts    types.TS
	err   error
	calls *int
}

func (rel *sourceCommitTSStub) SourceCommitTS(
	context.Context, types.TS,
) (types.TS, error) {
	*rel.calls++
	return rel.ts, rel.err
}

func (rel *sourceCommitTSAfterCopyStub) SourceCommitTSAfterCopy(
	context.Context, types.TS,
) (types.TS, error) {
	*rel.calls++
	return rel.ts, rel.err
}

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
	_, err := failClosedTxnTable(t).SourceCommitTS(context.Background(), types.TS{})
	require.Error(t, err)
}

func TestSourceCommitTSAfterCopyIncludesCommittedStateDespiteLocalWrites(t *testing.T) {
	tbl, proc := newViewPathTxnTable(t)
	tbl.proc.Store(proc)
	bat := batch.NewWithSize(1)
	bat.SetRowCount(1)
	tbl.getTxn().writes = []Entry{{tableId: 42, bat: bat}}

	_, err := tbl.SourceCommitTS(context.Background(), types.BuildTS(20, 0))
	require.ErrorContains(t, err, "transaction-local writes")

	sourceTS, err := tbl.SourceCommitTSAfterCopy(
		context.Background(), types.BuildTS(20, 0),
	)
	require.NoError(t, err)
	mustExceed := types.BuildTS(20, 0)
	require.True(t, sourceTS.GT(&mustExceed))
}

// newViewPathTxnTable builds a txnTable whose getPartitionState takes the
// view/created-in-txn branch, returning an empty partition state without
// subscribing. The caller decides whether to attach a process.
func newViewPathTxnTable(t *testing.T) (*txnTable, *process.Process) {
	proc := testutil.NewProc(t)
	eng := &Engine{
		fs:         proc.GetFileService(),
		partitions: make(map[[2]uint64]*logtailreplay.Partition),
	}
	txn := &Transaction{engine: eng}
	op := newTxnOperatorForTestWithWorkspace(t, txn)
	op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: 100}).AnyTimes()
	txn.op = op
	tbl := &txnTable{
		tableId: 42, relKind: "V", fake: true, eng: eng,
		db: &txnDatabase{op: op, databaseId: 10},
	}
	return tbl, proc
}

// With an empty partition state SourceCommitTS runs end to end and reports a
// bound without error for a table with no visible source data.
func TestSourceCommitTSEmptyPartitionState(t *testing.T) {
	tbl, proc := newViewPathTxnTable(t)
	tbl.proc.Store(proc)
	_, err := tbl.SourceCommitTS(context.Background(), types.TS{})
	require.NoError(t, err)
}

// SourceCommitTSAt needs a process for its file service and mpool; with none it
// fails closed rather than proceeding.
func TestSourceCommitTSNoProcess(t *testing.T) {
	tbl, _ := newViewPathTxnTable(t)
	_, err := tbl.SourceCommitTS(context.Background(), types.TS{})
	require.Error(t, err)
}

// A combined table without member discovery cannot prove coverage, and must not
// dereference a nil primary.
func TestCombinedTxnTableSourceCommitTSRequiresMembers(t *testing.T) {
	_, err := (&combinedTxnTable{}).SourceCommitTS(context.Background(), types.TS{})
	require.Error(t, err)
}

func TestCombinedTxnTableSourceCommitTSReturnsMax(t *testing.T) {
	calls := 0
	table := &combinedTxnTable{tablesFunc: func() ([]engine.Relation, error) {
		return []engine.Relation{
			&sourceCommitTSStub{ts: types.BuildTS(10, 0), calls: &calls},
			&sourceCommitTSStub{ts: types.BuildTS(30, 0), calls: &calls},
			&sourceCommitTSStub{ts: types.BuildTS(20, 0), calls: &calls},
		}, nil
	}}

	sourceTS, err := table.SourceCommitTS(context.Background(), types.TS{})
	require.NoError(t, err)
	require.Equal(t, types.BuildTS(30, 0), sourceTS)
	require.Equal(t, 3, calls)
}

func TestCombinedTxnTableSourceCommitTSAfterCopyChecksEveryMember(t *testing.T) {
	calls := 0
	table := &combinedTxnTable{tablesFunc: func() ([]engine.Relation, error) {
		return []engine.Relation{
			&sourceCommitTSAfterCopyStub{ts: types.BuildTS(10, 0), calls: &calls},
			&sourceCommitTSAfterCopyStub{ts: types.BuildTS(40, 0), calls: &calls},
		}, nil
	}}

	sourceTS, err := table.SourceCommitTSAfterCopy(
		context.Background(), types.BuildTS(20, 0),
	)
	require.NoError(t, err)
	require.Equal(t, types.BuildTS(40, 0), sourceTS)
	require.Equal(t, 2, calls)
}

func TestCombinedTxnTableSourceCommitTSShortCircuits(t *testing.T) {
	calls := 0
	table := &combinedTxnTable{tablesFunc: func() ([]engine.Relation, error) {
		return []engine.Relation{
			&sourceCommitTSStub{ts: types.BuildTS(30, 0), calls: &calls},
			&sourceCommitTSStub{ts: types.BuildTS(40, 0), calls: &calls},
		}, nil
	}}

	sourceTS, err := table.SourceCommitTS(
		context.Background(), types.BuildTS(20, 0),
	)
	require.NoError(t, err)
	require.Equal(t, types.BuildTS(30, 0), sourceTS)
	require.Equal(t, 1, calls)
}

func TestCombinedTxnTableSourceCommitTSFailsClosedForMissingProvider(t *testing.T) {
	table := &combinedTxnTable{tablesFunc: func() ([]engine.Relation, error) {
		return []engine.Relation{struct{ engine.Relation }{}}, nil
	}}
	_, err := table.SourceCommitTS(context.Background(), types.TS{})
	require.ErrorContains(t, err, "partition member")
}

func TestCombinedTxnTableSourceCommitTSPropagatesMemberError(t *testing.T) {
	calls := 0
	memberErr := errors.New("member unavailable")
	table := &combinedTxnTable{tablesFunc: func() ([]engine.Relation, error) {
		return []engine.Relation{
			&sourceCommitTSStub{err: memberErr, calls: &calls},
			&sourceCommitTSStub{ts: types.BuildTS(40, 0), calls: &calls},
		}, nil
	}}
	_, err := table.SourceCommitTS(context.Background(), types.TS{})
	require.ErrorIs(t, err, memberErr)
	require.Equal(t, 1, calls)
}

// txnTableDelegate routes to origin, or to the combined table when combined.is.
func TestTxnTableDelegateSourceCommitTS(t *testing.T) {
	origin := &txnTableDelegate{origin: failClosedTxnTable(t)}
	_, err := origin.SourceCommitTS(context.Background(), types.TS{})
	require.Error(t, err)

	combined := &txnTableDelegate{}
	combined.combined.is = true
	combined.combined.tbl = &combinedTxnTable{}
	_, err = combined.SourceCommitTS(context.Background(), types.TS{})
	require.Error(t, err)
}
