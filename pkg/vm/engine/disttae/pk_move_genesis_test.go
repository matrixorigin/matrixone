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
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// pkMoveGenesisProbeCall records one invocation of the fake first-touch
// probe: exactly the charge set the design disciplines to one per address
// per registration epoch.
type pkMoveGenesisProbeCall struct {
	accountID uint32
	tableID   uint64
	ownHex    string
	resolve   bool
}

// withPkMoveGenesisProbe installs fn as the epoch first-touch probe for the
// test's duration and restores the previous value. fn answers
// (registered, committedLinks, logicalID, error).
func withPkMoveGenesisProbe(
	t *testing.T,
	fn func(
		ctx context.Context,
		proc *process.Process,
		eng engine.Engine,
		accountID uint32,
		tableID uint64,
		ownTxnIDHex string,
		resolveLogical bool,
	) (bool, bool, uint64, error),
) {
	t.Helper()
	prev := PkMoveGenesisProbe
	PkMoveGenesisProbe = fn
	t.Cleanup(func() { PkMoveGenesisProbe = prev })
}

// newPkMoveGenesisFabricTxn builds a Transaction carrying the fields the
// genesis path touches, with a 16-byte recording-txn identity so the own-txn
// exclusion key is assertable.
func newPkMoveGenesisFabricTxn(t *testing.T, proc *process.Process) *Transaction {
	t.Helper()
	ctrl := gomock.NewController(t)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().Txn().Return(txnpb.TxnMeta{ID: testDisttaePkMoveTxnID(0x11)}).AnyTimes()
	op.EXPECT().Status().Return(txnpb.TxnStatus_Active).AnyTimes()
	return &Transaction{proc: proc, op: op}
}

// pkMoveRetentionEntry stages one retention-ledger insert batch in the exact
// wide seven-column layout the producer composes (row_kind, table_id,
// database_name, table_name, obj_id, created_ts, logical_id) plus the
// materialized composite primary key.
func pkMoveRetentionEntry(
	t *testing.T,
	mp *mpool.MPool,
	accountID uint32,
	tableIDs []uint64,
	logicals []uint64,
) Entry {
	t.Helper()
	n := len(tableIDs)
	if logicals == nil {
		logicals = make([]uint64, n)
	}
	require.Len(t, logicals, n)

	bat := batch.NewWithSize(8)
	bat.SetAttributes([]string{
		catalog.DataBranchLinkRetentionColRowKind,
		catalog.DataBranchLinkRetentionColTableID,
		catalog.DataBranchLinkRetentionColDatabaseName,
		catalog.DataBranchLinkRetentionColTableName,
		catalog.DataBranchLinkRetentionColObjID,
		catalog.DataBranchLinkRetentionColCreatedTS,
		catalog.DataBranchLinkRetentionColLogicalID,
		catalog.CPrimaryKeyColName,
	})
	bat.Vecs[0] = vector.NewVec(types.T_int8.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_uint64.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[4] = vector.NewVec(types.T_uint64.ToType())
	bat.Vecs[5] = vector.NewVec(types.T_uint64.ToType())
	bat.Vecs[6] = vector.NewVec(types.T_uint64.ToType())
	bat.Vecs[7] = vector.NewVec(types.T_varchar.ToType())
	for i := 0; i < n; i++ {
		require.NoError(t, vector.AppendFixed(
			bat.Vecs[0], int8(catalog.DataBranchLinkRetentionRowKindReal), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], tableIDs[i], false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[2], []byte("db"), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[3], []byte("t"), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[4], tableIDs[i], false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[5], uint64(12345), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[6], logicals[i], false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[7], []byte{byte(i)}, false, mp))
	}
	bat.SetRowCount(n)
	return Entry{
		typ:          INSERT,
		accountId:    accountID,
		databaseName: catalog.MO_CATALOG,
		tableName:    catalog.MO_DATA_BRANCH_LINK_RETENTION,
		bat:          bat,
	}
}

// pkMoveRetentionKindEntry is pkMoveRetentionEntry with the row_kind column
// overridden row by row (scratch-row refusal cells).
func pkMoveRetentionKindEntry(
	t *testing.T,
	mp *mpool.MPool,
	accountID uint32,
	tableID uint64,
	rowKind int8,
) Entry {
	t.Helper()
	e := pkMoveRetentionEntry(t, mp, accountID, []uint64{tableID}, nil)
	require.NoError(t, vector.SetFixedAtNoTypeCheck[int8](e.bat.Vecs[0], 0, rowKind))
	return e
}

func pkMoveRetentionLogicals(t *testing.T, e *Entry) []uint64 {
	t.Helper()
	return vector.MustFixedColNoTypeCheck[uint64](e.bat.Vecs[6])
}

// TestPkMoveGenesisProbeAndAdmission drives the first-touch probe and the
// locked admission over every decision branch: fresh miss stages the OWNED
// row with its resolved durable identity, registered hit degrades to the
// read-only EXTERNAL memo, repeat touches pay zero probes and zero inserts,
// and malformed inputs fail loudly instead of guessing.
func TestPkMoveGenesisProbeAndAdmission(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	ctx := context.Background()

	t.Run("fresh miss stages OWNED, backfills resolved logical, journals", func(t *testing.T) {
		txn := newPkMoveGenesisFabricTxn(t, proc)
		var calls []pkMoveGenesisProbeCall
		withPkMoveGenesisProbe(t, func(
			_ context.Context, _ *process.Process, _ engine.Engine,
			accountID uint32, tableID uint64, ownHex string, resolve bool,
		) (bool, bool, uint64, error) {
			calls = append(calls, pkMoveGenesisProbeCall{accountID, tableID, ownHex, resolve})
			return false, true, 7001, nil
		})

		e := pkMoveRetentionEntry(t, mp, 7, []uint64{42}, nil)
		verdicts, err := txn.probePkMoveGenesis(ctx, 7, e.bat)
		require.NoError(t, err)
		require.Len(t, verdicts, 1)
		require.Len(t, calls, 1)
		require.Equal(t, pkMoveGenesisProbeCall{7, 42, calls[0].ownHex, true}, calls[0])
		require.Len(t, calls[0].ownHex, 32)
		require.True(t, strings.HasPrefix(calls[0].ownHex, "11a5"),
			"the own-txn exclusion key is the normalized 16-byte identity in hex")
		require.True(t, verdicts[42].committedLinks)

		require.NoError(t, txn.admitPkMoveGenesisBatchLocked(7, e.bat, verdicts))
		require.Equal(t, 1, e.bat.RowCount())
		require.Equal(t, uint64(7001), pkMoveRetentionLogicals(t, &e)[0],
			"the probe-resolved durable identity rides the staged row")
		epoch := txn.pkMoveGenesisStates[pkMoveAcctTbl{account: 7, table: 42}]
		require.NotNil(t, epoch)
		require.Equal(t, pkMoveGenesisOwned, epoch.state)
		require.True(t, epoch.committedLinks)
		require.Len(t, txn.pkMoveGenesisJournal, 1)
		require.Equal(t, txn.statementID, txn.pkMoveGenesisJournal[0].seq)

		// Repeat touch inside the live epoch: zero probes, zero inserts,
		// zero new journal entries.
		e2 := pkMoveRetentionEntry(t, mp, 7, []uint64{42}, nil)
		verdicts2, err := txn.probePkMoveGenesis(ctx, 7, e2.bat)
		require.NoError(t, err)
		require.Empty(t, verdicts2)
		require.Len(t, calls, 1, "the dedup map serves repeat touches at zero probe cost")
		require.NoError(t, txn.admitPkMoveGenesisBatchLocked(7, e2.bat, verdicts2))
		require.Equal(t, 0, e2.bat.RowCount())
		require.Len(t, txn.pkMoveGenesisJournal, 1)
	})

	t.Run("registered hit records the EXTERNAL memo and swallows the row", func(t *testing.T) {
		txn := newPkMoveGenesisFabricTxn(t, proc)
		withPkMoveGenesisProbe(t, func(
			_ context.Context, _ *process.Process, _ engine.Engine,
			_ uint32, _ uint64, _ string, _ bool,
		) (bool, bool, uint64, error) {
			return true, false, 0, nil
		})
		e := pkMoveRetentionEntry(t, mp, 7, []uint64{42}, nil)
		verdicts, err := txn.probePkMoveGenesis(ctx, 7, e.bat)
		require.NoError(t, err)
		require.NoError(t, txn.admitPkMoveGenesisBatchLocked(7, e.bat, verdicts))
		require.Equal(t, 0, e.bat.RowCount(),
			"a probe hit never stages a second identity row")
		epoch := txn.pkMoveGenesisStates[pkMoveAcctTbl{account: 7, table: 42}]
		require.NotNil(t, epoch)
		require.Equal(t, pkMoveGenesisExternal, epoch.state)
		require.Len(t, txn.pkMoveGenesisJournal, 1,
			"statement rollback still invalidates the memo")
	})

	t.Run("mixed batch keeps only the fresh address in row order", func(t *testing.T) {
		txn := newPkMoveGenesisFabricTxn(t, proc)
		withPkMoveGenesisProbe(t, func(
			_ context.Context, _ *process.Process, _ engine.Engine,
			_ uint32, tableID uint64, _ string, _ bool,
		) (bool, bool, uint64, error) {
			if tableID == 41 {
				return true, false, 0, nil
			}
			return false, false, 9042, nil
		})
		e := pkMoveRetentionEntry(t, mp, 7, []uint64{41, 42}, nil)
		verdicts, err := txn.probePkMoveGenesis(ctx, 7, e.bat)
		require.NoError(t, err)
		require.Len(t, verdicts, 2)
		require.NoError(t, txn.admitPkMoveGenesisBatchLocked(7, e.bat, verdicts))
		require.Equal(t, 1, e.bat.RowCount())
		require.Equal(t, uint64(42),
			vector.MustFixedColNoTypeCheck[uint64](e.bat.Vecs[1])[0])
		require.Equal(t, uint64(9042), pkMoveRetentionLogicals(t, &e)[0])
		require.Len(t, txn.pkMoveGenesisStates, 2)
		require.Len(t, txn.pkMoveGenesisJournal, 2)
	})

	t.Run("descriptor logical id suppresses resolution and wins over the probe", func(t *testing.T) {
		txn := newPkMoveGenesisFabricTxn(t, proc)
		var resolves []bool
		withPkMoveGenesisProbe(t, func(
			_ context.Context, _ *process.Process, _ engine.Engine,
			_ uint32, _ uint64, _ string, resolve bool,
		) (bool, bool, uint64, error) {
			resolves = append(resolves, resolve)
			return false, false, 7777, nil
		})
		e := pkMoveRetentionEntry(t, mp, 7, []uint64{42}, []uint64{555})
		verdicts, err := txn.probePkMoveGenesis(ctx, 7, e.bat)
		require.NoError(t, err)
		require.Equal(t, []bool{false}, resolves,
			"a descriptor-carrying batch never pays the mo_tables round trip")
		require.NoError(t, txn.admitPkMoveGenesisBatchLocked(7, e.bat, verdicts))
		require.Equal(t, uint64(555), pkMoveRetentionLogicals(t, &e)[0])
	})

	t.Run("unavailable probe fails the statement instead of guessing", func(t *testing.T) {
		txn := newPkMoveGenesisFabricTxn(t, proc)
		withPkMoveGenesisProbe(t, nil)
		e := pkMoveRetentionEntry(t, mp, 7, []uint64{42}, nil)
		_, err := txn.probePkMoveGenesis(ctx, 7, e.bat)
		require.Error(t, err)
		require.Contains(t, err.Error(), "probe unavailable")
	})

	t.Run("missing retention catalog degrades to the EXTERNAL swallow", func(t *testing.T) {
		txn := newPkMoveGenesisFabricTxn(t, proc)
		withPkMoveGenesisProbe(t, func(
			_ context.Context, _ *process.Process, _ engine.Engine,
			_ uint32, _ uint64, _ string, _ bool,
		) (bool, bool, uint64, error) {
			return false, false, 0, moerr.NewNoSuchTableNoCtx(
				catalog.MO_CATALOG, catalog.MO_DATA_BRANCH_LINK_RETENTION)
		})
		e := pkMoveRetentionEntry(t, mp, 7, []uint64{42}, nil)
		verdicts, err := txn.probePkMoveGenesis(ctx, 7, e.bat)
		require.NoError(t, err)
		require.True(t, verdicts[42].registered,
			"the pre-upgrade shape degrades as a probe hit, never as a staged row")
		require.NoError(t, txn.admitPkMoveGenesisBatchLocked(7, e.bat, verdicts))
		require.Equal(t, 0, e.bat.RowCount())
		require.Equal(t, pkMoveGenesisExternal,
			txn.pkMoveGenesisStates[pkMoveAcctTbl{account: 7, table: 42}].state)
	})

	t.Run("generic probe error aborts the statement and journals nothing", func(t *testing.T) {
		txn := newPkMoveGenesisFabricTxn(t, proc)
		injected := moerr.NewInternalErrorNoCtx("catalog read blew up")
		withPkMoveGenesisProbe(t, func(
			_ context.Context, _ *process.Process, _ engine.Engine,
			_ uint32, _ uint64, _ string, _ bool,
		) (bool, bool, uint64, error) {
			return false, false, 0, injected
		})
		e := pkMoveRetentionEntry(t, mp, 7, []uint64{42}, nil)
		_, err := txn.probePkMoveGenesis(ctx, 7, e.bat)
		require.ErrorIs(t, err, injected)
		require.Empty(t, txn.pkMoveGenesisStates)
		require.Empty(t, txn.pkMoveGenesisJournal)
	})

	t.Run("fencing scratch rows in a marked batch are refused loudly", func(t *testing.T) {
		txn := newPkMoveGenesisFabricTxn(t, proc)
		probed := false
		withPkMoveGenesisProbe(t, func(
			_ context.Context, _ *process.Process, _ engine.Engine,
			_ uint32, _ uint64, _ string, _ bool,
		) (bool, bool, uint64, error) {
			probed = true
			return false, false, 0, nil
		})
		e := pkMoveRetentionKindEntry(t, mp, 7, 42,
			int8(catalog.DataBranchLinkRetentionRowKindFencingScratch))
		_, err := txn.probePkMoveGenesis(ctx, 7, e.bat)
		require.Error(t, err)
		require.Contains(t, err.Error(), "refuses non-real retention row")
		require.False(t, probed, "the refusal precedes any charged probe")
	})
}

// TestPkMoveGenesisJournalRollback proves statement-atomicity of the epoch
// map: registrations the rolled-back statement journaled return to ABSENT,
// earlier statements' entries survive untouched, and a re-touch after the
// rollback pays a fresh first-touch probe (a NEW epoch).
func TestPkMoveGenesisJournalRollback(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	ctx := context.Background()
	txn := newPkMoveGenesisFabricTxn(t, proc)

	var calls []uint64
	withPkMoveGenesisProbe(t, func(
		_ context.Context, _ *process.Process, _ engine.Engine,
		_ uint32, tableID uint64, _ string, _ bool,
	) (bool, bool, uint64, error) {
		calls = append(calls, tableID)
		return tableID == 42, false, 0, nil
	})
	stage := func(tableID uint64) {
		e := pkMoveRetentionEntry(t, mp, 7, []uint64{tableID}, nil)
		verdicts, err := txn.probePkMoveGenesis(ctx, 7, e.bat)
		require.NoError(t, err)
		require.NoError(t, txn.admitPkMoveGenesisBatchLocked(7, e.bat, verdicts))
	}

	// Statement 0 registers address 40 (OWNED: probe says unregistered for
	// anything but 42).
	txn.statementID = 0
	stage(40)
	require.Len(t, txn.pkMoveGenesisStates, 1)
	require.Len(t, txn.pkMoveGenesisJournal, 1)

	// Statement 1 EXTERNAL-memos 42 and stages a fresh 43.
	txn.statementID = 1
	stage(42)
	stage(43)
	require.Len(t, txn.pkMoveGenesisStates, 3)
	require.Len(t, txn.pkMoveGenesisJournal, 3)

	// Roll statement 1 back exactly like RollbackLastStatement does:
	// decrement first, then the provenance undo.
	txn.statementID = 0
	require.NoError(t, txn.rollbackPkMoveStmtStateLocked())
	require.Len(t, txn.pkMoveGenesisStates, 1,
		"the rolled-back statement's OWNED row and EXTERNAL memo both return to ABSENT")
	require.Contains(t, txn.pkMoveGenesisStates, pkMoveAcctTbl{account: 7, table: 40})
	require.Len(t, txn.pkMoveGenesisJournal, 1)
	require.Equal(t, 0, txn.pkMoveGenesisJournal[0].seq)

	// A re-touch is a NEW epoch: the address is probed again, and the
	// verdict is re-derived rather than resurrecting the dead memo.
	before := len(calls)
	txn.statementID = 1
	stage(42)
	require.Equal(t, before+1, len(calls), "rollback invalidates; the next touch probes anew")
	require.Equal(t, pkMoveGenesisExternal,
		txn.pkMoveGenesisStates[pkMoveAcctTbl{account: 7, table: 42}].state)
}

// TestPkMoveGenesisPruneCancel drives the D161(A.1)/D163 destruction step
// through the production prune entry point: an OWNED registration whose last
// staged link just died and whose first-touch probe found no committed links
// is canceled in one step (staged row shrunk, map entry deleted), while
// committed-links, live-link, and EXTERNAL addresses survive untouched.
func TestPkMoveGenesisPruneCancel(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	const userTable = uint64(42)
	addr := pkMoveAcctTbl{account: 7, table: userTable}

	deadChain := func(t *testing.T) (link Entry, ins Entry, del Entry) {
		rid := pkMoveWorkspaceRowid(51)
		link = pkMovePruneLinkEntry(t, mp, 7,
			[]uint64{userTable}, []*types.Rowid{nil}, []*types.Rowid{&rid},
			[]uint64{10})
		ins = pkMoveStagedInsertEntry(t, mp, 7, userTable,
			[]int64{3}, nil, []types.Rowid{rid})
		del = pkMoveDeleteEntry(t, mp, 7, userTable, []types.Rowid{rid})
		return link, ins, del
	}
	account := func(txn *Transaction, writes []Entry) {
		for i := range writes {
			txn.accountWorkspaceEntryLocked(&writes[i])
		}
		txn.writes = writes
	}

	t.Run("dead last link cancels row and epoch in one step", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		link, ins, del := deadChain(t)
		ret := pkMoveRetentionEntry(t, mp, 7, []uint64{userTable}, nil)
		txn.pkMoveGenesisStates = map[pkMoveAcctTbl]*pkMoveGenesisEpoch{
			addr: {state: pkMoveGenesisOwned, committedLinks: false},
		}
		account(txn, []Entry{link, ins, del, ret})
		txn.pruneDeadPkMoveLinkRowsLocked()
		require.Equal(t, 0, txn.writes[0].bat.RowCount(), "the dead link died in the prune")
		require.Equal(t, 0, txn.writes[3].bat.RowCount(),
			"the staged identity row dies with its last link")
		_, ok := txn.pkMoveGenesisStates[addr]
		require.False(t, ok, "the epoch is destroyed, not left resting")

		// Commit-retry idempotence: a second prune over the shrunken
		// workspace is a strict no-op.
		txn.pruneDeadPkMoveLinkRowsLocked()
		require.Equal(t, 0, txn.writes[3].bat.RowCount())
	})

	t.Run("committed links at first touch keep the registration", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		link, ins, del := deadChain(t)
		ret := pkMoveRetentionEntry(t, mp, 7, []uint64{userTable}, nil)
		txn.pkMoveGenesisStates = map[pkMoveAcctTbl]*pkMoveGenesisEpoch{
			addr: {state: pkMoveGenesisOwned, committedLinks: true},
		}
		account(txn, []Entry{link, ins, del, ret})
		txn.pruneDeadPkMoveLinkRowsLocked()
		require.Equal(t, 0, txn.writes[0].bat.RowCount())
		require.Equal(t, 1, txn.writes[3].bat.RowCount(),
			"a committed link fan-out disciplines the cancel away")
		require.Contains(t, txn.pkMoveGenesisStates, addr)
	})

	t.Run("a live staged link keeps the registration", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		rid := pkMoveWorkspaceRowid(52)
		link := pkMovePruneLinkEntry(t, mp, 7,
			[]uint64{userTable}, []*types.Rowid{nil}, []*types.Rowid{&rid},
			[]uint64{10})
		ins := pkMoveStagedInsertEntry(t, mp, 7, userTable,
			[]int64{3}, nil, []types.Rowid{rid})
		ret := pkMoveRetentionEntry(t, mp, 7, []uint64{userTable}, nil)
		txn.pkMoveGenesisStates = map[pkMoveAcctTbl]*pkMoveGenesisEpoch{
			addr: {state: pkMoveGenesisOwned, committedLinks: false},
		}
		account(txn, []Entry{link, ins, ret})
		txn.pruneDeadPkMoveLinkRowsLocked()
		require.Equal(t, 1, txn.writes[0].bat.RowCount())
		require.Equal(t, 1, txn.writes[2].bat.RowCount())
		require.Contains(t, txn.pkMoveGenesisStates, addr)
	})

	t.Run("EXTERNAL memos are never canceled", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		txn.pkMoveGenesisStates = map[pkMoveAcctTbl]*pkMoveGenesisEpoch{
			addr: {state: pkMoveGenesisExternal, committedLinks: false},
		}
		account(txn, []Entry{})
		txn.pruneDeadPkMoveLinkRowsLocked()
		require.Contains(t, txn.pkMoveGenesisStates, addr,
			"the memo holds no row and no cancel authority")
	})

	t.Run("only the dead-address row leaves a shared batch", func(t *testing.T) {
		txn := &Transaction{proc: proc}
		link, _, del := deadChain(t)
		other := pkMoveAcctTbl{account: 7, table: 43}
		ret := pkMoveRetentionEntry(t, mp, 7, []uint64{userTable, 43}, nil)
		txn.pkMoveGenesisStates = map[pkMoveAcctTbl]*pkMoveGenesisEpoch{
			addr:  {state: pkMoveGenesisOwned, committedLinks: false},
			other: {state: pkMoveGenesisOwned, committedLinks: false},
		}
		account(txn, []Entry{link, del, ret})
		// Address 43 still owns a live staged link, so the prune cancels
		// only 42's row inside the shared retention batch.
		ridLive := pkMoveWorkspaceRowid(53)
		liveLink := pkMovePruneLinkEntry(t, mp, 7,
			[]uint64{43}, []*types.Rowid{nil}, []*types.Rowid{&ridLive},
			[]uint64{11})
		liveIns := pkMoveStagedInsertEntry(t, mp, 7, 43,
			[]int64{4}, nil, []types.Rowid{ridLive})
		txn.writes = append(txn.writes, liveLink, liveIns)
		txn.accountWorkspaceEntryLocked(&txn.writes[3])
		txn.accountWorkspaceEntryLocked(&txn.writes[4])

		txn.pruneDeadPkMoveLinkRowsLocked()
		require.Equal(t, 1, txn.writes[2].bat.RowCount(), "43's identity row survives")
		require.Equal(t, uint64(43),
			vector.MustFixedColNoTypeCheck[uint64](txn.writes[2].bat.Vecs[1])[0])
		_, ok := txn.pkMoveGenesisStates[addr]
		require.False(t, ok)
		require.Contains(t, txn.pkMoveGenesisStates, other)
	})
}

// TestPkMoveGenesisKeepLaneMembership pins the D166(A) forced-dump
// conjunction: name-match alone retains nothing — a retention batch rides
// the keep lane only while this transaction OWNS at least one of its rows'
// addresses. Unowned funnel upserts staged by plain internal SQL spill
// freely.
func TestPkMoveGenesisKeepLaneMembership(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	txn := &Transaction{proc: proc}
	txn.pkMoveGenesisStates = map[pkMoveAcctTbl]*pkMoveGenesisEpoch{
		{account: 7, table: 41}: {state: pkMoveGenesisExternal},
		{account: 7, table: 42}: {state: pkMoveGenesisOwned},
	}

	owned := pkMoveRetentionEntry(t, mp, 7, []uint64{42}, nil)
	require.True(t, txn.pkMoveGenesisAnyOwnedRowLocked(&owned))
	external := pkMoveRetentionEntry(t, mp, 7, []uint64{41}, nil)
	require.False(t, txn.pkMoveGenesisAnyOwnedRowLocked(&external))
	mixed := pkMoveRetentionEntry(t, mp, 7, []uint64{41, 42}, nil)
	require.True(t, txn.pkMoveGenesisAnyOwnedRowLocked(&mixed))
	unknown := pkMoveRetentionEntry(t, mp, 7, []uint64{43}, nil)
	require.False(t, txn.pkMoveGenesisAnyOwnedRowLocked(&unknown),
		"a funnel upsert never becomes lane-owned by name-match alone")
	otherAccount := pkMoveRetentionEntry(t, mp, 8, []uint64{42}, nil)
	require.False(t, txn.pkMoveGenesisAnyOwnedRowLocked(&otherAccount))
	empty := pkMoveRetentionEntry(t, mp, 7, []uint64{42}, nil)
	empty.bat.Shrink([]int64{}, false)
	empty.bat.SetRowCount(0)
	require.False(t, txn.pkMoveGenesisAnyOwnedRowLocked(&empty))

	bare := &Transaction{proc: proc}
	require.False(t, bare.pkMoveGenesisAnyOwnedRowLocked(&owned),
		"a transaction with no epoch map admits nothing into the lane")
}

// TestPkMoveGenesisProbeSkippedForKnownAddress pins the zero-cost repeat
// path: even with the probe seam torn out, a repeat touch inside a live
// epoch dedups silently.
func TestPkMoveGenesisProbeSkippedForKnownAddress(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	ctx := context.Background()
	txn := newPkMoveGenesisFabricTxn(t, proc)
	withPkMoveGenesisProbe(t, nil)
	txn.pkMoveGenesisStates = map[pkMoveAcctTbl]*pkMoveGenesisEpoch{
		{account: 7, table: 42}: {state: pkMoveGenesisOwned},
	}
	e := pkMoveRetentionEntry(t, mp, 7, []uint64{42}, nil)
	verdicts, err := txn.probePkMoveGenesis(ctx, 7, e.bat)
	require.NoError(t, err)
	require.Empty(t, verdicts)
	require.NoError(t, txn.admitPkMoveGenesisBatchLocked(7, e.bat, verdicts))
	require.Equal(t, 0, e.bat.RowCount())
	require.Empty(t, txn.pkMoveGenesisJournal,
		"repeat touches never grow the journal")
}
