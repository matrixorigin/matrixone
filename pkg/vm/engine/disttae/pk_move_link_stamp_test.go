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
	"encoding/binary"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/stretchr/testify/require"
)

// pkMovePairRow describes one staged link row for the D228 pairing and
// D225 stamping tests: every credential column is under per-row control.
type pkMovePairRow struct {
	tableID  uint64
	oldPK    []byte // nil stages a NULL departure key
	newPK    []byte // nil stages a NULL arrival key
	linkID   []byte // nil stages a NULL link id; short ids allowed
	stmtSeq  uint64
	succ     []byte // pre-staged succ_link_id image; nil stages NULL
	commitTS []byte // pre-staged commit_ts image; nil stages NULL
}

// pkMovePairingLinkEntry builds one staged INSERT for the link catalog
// table carrying the full v4 column set (commit_ts, succ_link_id,
// stmt_seq) so the commit-time pairing fixup and the stamp-set collector
// run against their real schema.
func pkMovePairingLinkEntry(
	t *testing.T,
	mp *mpool.MPool,
	accountID uint32,
	rows []pkMovePairRow,
) Entry {
	t.Helper()
	n := len(rows)
	bat := batch.NewWithSize(10)
	bat.SetAttributes([]string{
		catalog.Row_ID,
		catalog.DataBranchUpdateLinkColLinkID,
		catalog.DataBranchUpdateLinkColTableID,
		catalog.DataBranchUpdateLinkColOldPK,
		catalog.DataBranchUpdateLinkColNewPK,
		catalog.DataBranchUpdateLinkColWriteTS,
		catalog.DataBranchUpdateLinkColTxnID,
		catalog.DataBranchUpdateLinkColCommitTS,
		catalog.DataBranchUpdateLinkColSuccLinkID,
		catalog.DataBranchUpdateLinkColStmtSeq,
	})
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_uint64.ToType())
	bat.Vecs[3] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[4] = vector.NewVec(types.T_varbinary.ToType())
	bat.Vecs[5] = vector.NewVec(types.T_uint64.ToType())
	bat.Vecs[6] = vector.NewVec(types.New(types.T_binary, catalog.DataBranchUpdateLinkTxnIDBytes, 0))
	bat.Vecs[7] = vector.NewVec(types.New(types.T_binary, catalog.DataBranchUpdateLinkTSBytes, 0))
	bat.Vecs[8] = vector.NewVec(types.New(types.T_binary, catalog.DataBranchUpdateLinkIDBytes, 0))
	bat.Vecs[9] = vector.NewVec(types.T_uint64.ToType())
	for i, r := range rows {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.BuildTestRowid(9, int64(i+1)), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], r.linkID, r.linkID == nil, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], r.tableID, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[3], r.oldPK, r.oldPK == nil, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[4], r.newPK, r.newPK == nil, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[5], uint64(100+i), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[6], testDisttaePkMoveTxnID(byte(i+1)), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[7], r.commitTS, r.commitTS == nil, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[8], r.succ, r.succ == nil, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[9], r.stmtSeq, false, mp))
	}
	bat.SetRowCount(n)
	return Entry{
		typ:          INSERT,
		accountId:    accountID,
		databaseName: catalog.MO_CATALOG,
		tableName:    catalog.MO_DATA_BRANCH_UPDATE_LINK,
		bat:          bat,
	}
}

// pkMovePairColVec returns one staged column vector of a link entry by
// catalog attribute name.
func pkMovePairColVec(t *testing.T, e *Entry, attr string) *vector.Vector {
	t.Helper()
	for j, a := range e.bat.Attrs {
		if a == attr {
			return e.bat.Vecs[j]
		}
	}
	t.Fatalf("link entry lacks column %s", attr)
	return nil
}

// pkMoveSuccOf reads one row's succ_link_id cell: (image, true) when
// non-NULL, (nil, false) when the cell is NULL.
func pkMoveSuccOf(t *testing.T, e *Entry, row int) ([]byte, bool) {
	t.Helper()
	vec := pkMovePairColVec(t, e, catalog.DataBranchUpdateLinkColSuccLinkID)
	if vec.IsNull(uint64(row)) {
		return nil, false
	}
	return vec.GetBytesAt(row), true
}

// pkMoveStampTestLinkID returns a fabricated full-width link id carrying
// the tag at both ends so ids stay distinguishable in SQL text.
func pkMoveStampTestLinkID(tag byte) []byte {
	id := make([]byte, catalog.DataBranchUpdateLinkIDBytes)
	id[0] = tag
	id[len(id)-1] = tag ^ 0xff
	return id
}

// TestPairPkMoveLinkContinuations pins the D228 commit-time pairing
// fixup: over the converged post-prune link set of one committing
// transaction, every retained link arriving at a key pairs with the same
// transaction's retained link DEPARTING that key at the smallest strictly
// later statement ordinal, the pairing persists on the link row's
// succ_link_id cell, and a link with no such departure keeps NULL —
// NULL means the generation is terminal, so the consumer never falls
// back to inferring continuity from commit-timestamp ties.
func TestPairPkMoveLinkContinuations(t *testing.T) {
	t.Run("each arrival pairs the smallest later same-scope departure", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		id1 := pkMoveStampTestLinkID(1)
		id2 := pkMoveStampTestLinkID(2)
		id3 := pkMoveStampTestLinkID(3)
		// UPDATE 2->3 at statement 1, UPDATE 3->4 at statement 2, and a
		// second, later departure of key 3 at statement 9: the arrival at
		// 3 must pair the SMALLEST later departure (statement 2), never
		// the statement-9 one.
		entry := pkMovePairingLinkEntry(t, mp, 7, []pkMovePairRow{
			{tableID: 42, oldPK: []byte{2}, newPK: []byte{3}, linkID: id1, stmtSeq: 1},
			{tableID: 42, oldPK: []byte{3}, newPK: []byte{4}, linkID: id2, stmtSeq: 2},
			{tableID: 42, oldPK: []byte{3}, newPK: []byte{7}, linkID: id3, stmtSeq: 9},
		})
		txn := &Transaction{proc: proc, writes: []Entry{entry}}
		require.NoError(t, txn.pairPkMoveLinkContinuationsLocked())
		succ, ok := pkMoveSuccOf(t, &txn.writes[0], 0)
		require.True(t, ok, "the arrival at 3 pairs its smallest later departure")
		require.Equal(t, id2, succ)
		_, ok = pkMoveSuccOf(t, &txn.writes[0], 1)
		require.False(t, ok, "no departure of key 4: terminal NULL")
		_, ok = pkMoveSuccOf(t, &txn.writes[0], 2)
		require.False(t, ok, "no departure of key 7: terminal NULL")
	})

	t.Run("a multi-hop chain pairs hop by hop and the terminal stays NULL", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		id1 := pkMoveStampTestLinkID(1)
		id2 := pkMoveStampTestLinkID(2)
		id3 := pkMoveStampTestLinkID(3)
		// UPDATE 2->3; UPDATE 3->4; UPDATE 4->5 in one transaction (the
		// D228 2->3->4 chain): the fixup pairs the chain's links
		// hop-by-hop and records the last terminal as NULL.
		entry := pkMovePairingLinkEntry(t, mp, 7, []pkMovePairRow{
			{tableID: 42, oldPK: []byte{2}, newPK: []byte{3}, linkID: id1, stmtSeq: 1},
			{tableID: 42, oldPK: []byte{3}, newPK: []byte{4}, linkID: id2, stmtSeq: 2},
			{tableID: 42, oldPK: []byte{4}, newPK: []byte{5}, linkID: id3, stmtSeq: 5},
		})
		txn := &Transaction{proc: proc, writes: []Entry{entry}}
		require.NoError(t, txn.pairPkMoveLinkContinuationsLocked())
		succ, ok := pkMoveSuccOf(t, &txn.writes[0], 0)
		require.True(t, ok)
		require.Equal(t, id2, succ)
		succ, ok = pkMoveSuccOf(t, &txn.writes[0], 1)
		require.True(t, ok)
		require.Equal(t, id3, succ)
		_, ok = pkMoveSuccOf(t, &txn.writes[0], 2)
		require.False(t, ok, "the chain's terminal generation keeps NULL")
	})

	t.Run("pairings span staged entry boundaries", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		id1 := pkMoveStampTestLinkID(1)
		id2 := pkMoveStampTestLinkID(2)
		// An intra-transaction S3 flush splits the chain's staging into
		// two workspace entries; the fixup pairs across them because it
		// runs over the whole converged set.
		entry1 := pkMovePairingLinkEntry(t, mp, 7, []pkMovePairRow{
			{tableID: 42, oldPK: []byte{2}, newPK: []byte{3}, linkID: id1, stmtSeq: 1},
		})
		entry2 := pkMovePairingLinkEntry(t, mp, 7, []pkMovePairRow{
			{tableID: 42, oldPK: []byte{3}, newPK: []byte{4}, linkID: id2, stmtSeq: 2},
		})
		txn := &Transaction{proc: proc, writes: []Entry{entry1, entry2}}
		require.NoError(t, txn.pairPkMoveLinkContinuationsLocked())
		succ, ok := pkMoveSuccOf(t, &txn.writes[0], 0)
		require.True(t, ok, "the first entry's link pairs the second entry's departure")
		require.Equal(t, id2, succ)
		_, ok = pkMoveSuccOf(t, &txn.writes[1], 0)
		require.False(t, ok)
	})

	t.Run("an earlier departure never pairs with a later arrival", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		id1 := pkMoveStampTestLinkID(1)
		id2 := pkMoveStampTestLinkID(2)
		// The unrelated row's departure of key 3 precedes the lineage's
		// arrival at 3 (the vacate-before-arrival schedule): statement
		// ordinals strictly increase along the lineage, so the earlier
		// departure can never be its continuation.
		entry := pkMovePairingLinkEntry(t, mp, 7, []pkMovePairRow{
			{tableID: 42, oldPK: []byte{3}, newPK: []byte{4}, linkID: id1, stmtSeq: 1},
			{tableID: 42, oldPK: []byte{2}, newPK: []byte{3}, linkID: id2, stmtSeq: 5},
		})
		txn := &Transaction{proc: proc, writes: []Entry{entry}}
		require.NoError(t, txn.pairPkMoveLinkContinuationsLocked())
		_, ok := pkMoveSuccOf(t, &txn.writes[0], 1)
		require.False(t, ok, "the arrival at 3 keeps NULL: the only departure precedes it")
		_, ok = pkMoveSuccOf(t, &txn.writes[0], 0)
		require.False(t, ok, "no departure of key 4: terminal NULL")
	})

	t.Run("a same-ordinal tie between distinct departures fails loudly", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		// Two distinct departures of key 3 sharing one statement ordinal
		// violate primary-key uniqueness; persisting either pairing would
		// record an ambiguous credential, so the commit fails instead.
		entry := pkMovePairingLinkEntry(t, mp, 7, []pkMovePairRow{
			{tableID: 42, oldPK: []byte{2}, newPK: []byte{3}, linkID: pkMoveStampTestLinkID(1), stmtSeq: 1},
			{tableID: 42, oldPK: []byte{3}, newPK: []byte{4}, linkID: pkMoveStampTestLinkID(2), stmtSeq: 2},
			{tableID: 42, oldPK: []byte{3}, newPK: []byte{5}, linkID: pkMoveStampTestLinkID(3), stmtSeq: 2},
		})
		txn := &Transaction{proc: proc, writes: []Entry{entry}}
		err := txn.pairPkMoveLinkContinuationsLocked()
		require.ErrorContains(t, err, "share statement ordinal 2")
	})

	t.Run("an unstamped statement ordinal is neither consumed nor paired", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		id1 := pkMoveStampTestLinkID(1)
		id2 := pkMoveStampTestLinkID(2)
		// The fill hook stamps every surviving row's ordinal; a zero
		// ordinal marks evidence the pairing must not consume — neither
		// as an arrival nor as a departure.
		entry := pkMovePairingLinkEntry(t, mp, 7, []pkMovePairRow{
			{tableID: 42, oldPK: []byte{2}, newPK: []byte{3}, linkID: id1, stmtSeq: 1},
			{tableID: 42, oldPK: []byte{3}, newPK: []byte{9}, linkID: id2, stmtSeq: 0},
		})
		txn := &Transaction{proc: proc, writes: []Entry{entry}}
		require.NoError(t, txn.pairPkMoveLinkContinuationsLocked())
		_, ok := pkMoveSuccOf(t, &txn.writes[0], 0)
		require.False(t, ok, "the unstamped departure of key 3 is never consumed")
		_, ok = pkMoveSuccOf(t, &txn.writes[0], 1)
		require.False(t, ok, "the unstamped row is never paired")
	})

	t.Run("pairing is scoped per account and table", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		id1 := pkMoveStampTestLinkID(1)
		// The arrival at key 3 in (account 7, table 42) must never pair
		// a departure staged for another table or another account:
		// packer-encoded keys are not comparable across tables.
		entryMain := pkMovePairingLinkEntry(t, mp, 7, []pkMovePairRow{
			{tableID: 42, oldPK: []byte{2}, newPK: []byte{3}, linkID: id1, stmtSeq: 1},
		})
		entryOtherTable := pkMovePairingLinkEntry(t, mp, 7, []pkMovePairRow{
			{tableID: 43, oldPK: []byte{3}, newPK: []byte{4}, linkID: pkMoveStampTestLinkID(2), stmtSeq: 2},
		})
		entryOtherAccount := pkMovePairingLinkEntry(t, mp, 8, []pkMovePairRow{
			{tableID: 42, oldPK: []byte{3}, newPK: []byte{4}, linkID: pkMoveStampTestLinkID(3), stmtSeq: 2},
		})
		txn := &Transaction{proc: proc, writes: []Entry{entryMain, entryOtherTable, entryOtherAccount}}
		require.NoError(t, txn.pairPkMoveLinkContinuationsLocked())
		_, ok := pkMoveSuccOf(t, &txn.writes[0], 0)
		require.False(t, ok, "no cross-table or cross-account pairing")
		_, ok = pkMoveSuccOf(t, &txn.writes[1], 0)
		require.False(t, ok)
		_, ok = pkMoveSuccOf(t, &txn.writes[2], 0)
		require.False(t, ok)
	})

	t.Run("schema drift leaves the batch untouched", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		// A staged batch predating the credential columns (no stmt_seq,
		// no succ_link_id): the pass leaves the cells alone and netted
		// continuations stay unprovable — the mixed-version posture.
		bat := pkMoveLinkStagedBatch(t, mp,
			[]types.Rowid{types.BuildTestRowid(9, 1)},
			[]uint64{42},
			[][]byte{{3}},
		)
		defer bat.Clean(mp)
		entry := Entry{
			typ:          INSERT,
			accountId:    7,
			databaseName: catalog.MO_CATALOG,
			tableName:    catalog.MO_DATA_BRANCH_UPDATE_LINK,
			bat:          bat,
		}
		txn := &Transaction{proc: proc, writes: []Entry{entry}}
		require.NoError(t, txn.pairPkMoveLinkContinuationsLocked())
		require.Equal(t, 11, bat.VectorCount(), "no column is added or rewritten")
	})

	t.Run("the pairing is stamp-independent", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		id1 := pkMoveStampTestLinkID(1)
		id2 := pkMoveStampTestLinkID(2)
		tsCell := make([]byte, catalog.DataBranchUpdateLinkTSBytes)
		tsCell[0] = 0xAB
		// Pairing derives from the RECORDING transaction's own statement
		// ledger, where commit_ts is always still NULL (stamping runs
		// post-commit): the pass pairs the unstamped chain and never
		// touches commit_ts cells. A later stamping UPDATE re-inserts the
		// same rows with commit_ts and the recorded succ filled; the
		// stamping transaction's commit must leave both cells verbatim —
		// the stamp neither alters nor recomputes the pairing credential.
		unstamped := pkMovePairingLinkEntry(t, mp, 7, []pkMovePairRow{
			{tableID: 42, oldPK: []byte{2}, newPK: []byte{3}, linkID: id1, stmtSeq: 1},
			{tableID: 42, oldPK: []byte{3}, newPK: []byte{4}, linkID: id2, stmtSeq: 2},
		})
		restaged := pkMovePairingLinkEntry(t, mp, 7, []pkMovePairRow{
			{tableID: 42, oldPK: []byte{2}, newPK: []byte{3}, linkID: id1, stmtSeq: 1, succ: id2, commitTS: tsCell},
			{tableID: 42, oldPK: []byte{3}, newPK: []byte{4}, linkID: id2, stmtSeq: 2, commitTS: tsCell},
		})
		txnUnstamped := &Transaction{proc: proc, writes: []Entry{unstamped}}
		txnRestaged := &Transaction{proc: proc, writes: []Entry{restaged}}
		require.NoError(t, txnUnstamped.pairPkMoveLinkContinuationsLocked())
		require.NoError(t, txnRestaged.pairPkMoveLinkContinuationsLocked())
		succA, okA := pkMoveSuccOf(t, &txnUnstamped.writes[0], 0)
		require.True(t, okA)
		require.Equal(t, id2, succA, "the recording transaction pairs its own chain")
		succB, okB := pkMoveSuccOf(t, &txnRestaged.writes[0], 0)
		require.True(t, okB, "the re-inserted credential survives the stamping commit")
		require.Equal(t, succA, succB, "identical pairing with and without stamps")
		_, okC := pkMoveSuccOf(t, &txnRestaged.writes[0], 1)
		require.False(t, okC, "the re-inserted terminal stays NULL")
		commitVecA := pkMovePairColVec(t, &txnUnstamped.writes[0], catalog.DataBranchUpdateLinkColCommitTS)
		require.True(t, commitVecA.IsNull(0), "the pass never stamps")
		commitVecB := pkMovePairColVec(t, &txnRestaged.writes[0], catalog.DataBranchUpdateLinkColCommitTS)
		require.Equal(t, tsCell, commitVecB.GetBytesAt(0), "a staged stamp survives the pass verbatim")
	})

	t.Run("re-inserted historical rows never pair across original transactions", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		idA := pkMoveStampTestLinkID(1)
		idB := pkMoveStampTestLinkID(2)
		tsCell := make([]byte, catalog.DataBranchUpdateLinkTSBytes)
		tsCell[0] = 0xCD
		// One stamping UPDATE's chunk carries links recorded by TWO
		// different original transactions: L_A moved 2->3 and was
		// terminal (succ NULL), L_B moved 3->4. Original transaction
		// scope is gone, so ordinal pairing would fabricate L_B as L_A's
		// successor; both rows must keep their staged cells.
		entry := pkMovePairingLinkEntry(t, mp, 7, []pkMovePairRow{
			{tableID: 42, oldPK: []byte{2}, newPK: []byte{3}, linkID: idA, stmtSeq: 1, commitTS: tsCell},
			{tableID: 42, oldPK: []byte{3}, newPK: []byte{4}, linkID: idB, stmtSeq: 2, commitTS: tsCell},
		})
		txn := &Transaction{proc: proc, writes: []Entry{entry}}
		require.NoError(t, txn.pairPkMoveLinkContinuationsLocked())
		_, ok := pkMoveSuccOf(t, &txn.writes[0], 0)
		require.False(t, ok, "a historical terminal never gains another transaction's successor")
		_, ok = pkMoveSuccOf(t, &txn.writes[0], 1)
		require.False(t, ok, "the historical rows keep their staged NULL cells")
	})

	t.Run("a partial-chain re-insert preserves an existing successor verbatim", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		idA1 := pkMoveStampTestLinkID(1)
		idA2 := pkMoveStampTestLinkID(2)
		idX := pkMoveStampTestLinkID(3)
		idY := pkMoveStampTestLinkID(4)
		tsCell := make([]byte, catalog.DataBranchUpdateLinkTSBytes)
		tsCell[0] = 0xEF
		// A stamping UPDATE re-inserts L_A1 ALONE (its recorded successor
		// L_A2 is outside this chunk) alongside this transaction's own
		// fresh links L_X->L_Y, which pair and force the succ column's
		// one-shot rewrite. The rewrite must copy L_A1's staged succ
		// verbatim: rebuilding it as NULL would erase a valid credential
		// whose departure this transaction never staged.
		entry := pkMovePairingLinkEntry(t, mp, 7, []pkMovePairRow{
			{tableID: 42, oldPK: []byte{2}, newPK: []byte{3}, linkID: idA1, stmtSeq: 1, succ: idA2, commitTS: tsCell},
			{tableID: 42, oldPK: []byte{5}, newPK: []byte{6}, linkID: idX, stmtSeq: 1},
			{tableID: 42, oldPK: []byte{6}, newPK: []byte{7}, linkID: idY, stmtSeq: 2},
		})
		txn := &Transaction{proc: proc, writes: []Entry{entry}}
		require.NoError(t, txn.pairPkMoveLinkContinuationsLocked())
		succ, ok := pkMoveSuccOf(t, &txn.writes[0], 0)
		require.True(t, ok, "the out-of-chunk successor credential is preserved")
		require.Equal(t, idA2, succ)
		succ, ok = pkMoveSuccOf(t, &txn.writes[0], 1)
		require.True(t, ok, "the fresh arrival still pairs its own departure")
		require.Equal(t, idY, succ)
		_, ok = pkMoveSuccOf(t, &txn.writes[0], 2)
		require.False(t, ok, "the fresh chain's terminal stays NULL")
	})
}

// pkMoveStampRecord captures one internal-executor Exec call: the SQL
// text, the routed account, and the account attached to the call ctx
// (the frontend's session-txn creation reads the account from the
// request context, so the stamping channel must attach it there too).
type pkMoveStampRecord struct {
	sql           string
	account       uint32
	ctxHasAccount bool
	ctxAccount    uint32
}

// pkMoveStampRecordingExecutor is an ie.InternalExecutor stub that
// records every Exec call and fails the first failN of them.
type pkMoveStampRecordingExecutor struct {
	ie.InternalExecutor
	recs  *[]pkMoveStampRecord
	failN *int
}

func (e pkMoveStampRecordingExecutor) Exec(
	ctx context.Context,
	sql string,
	opts ie.SessionOverrideOptions,
) error {
	var acct uint32
	if opts.AccountId != nil {
		acct = *opts.AccountId
	}
	rec := pkMoveStampRecord{sql: sql, account: acct}
	if ctxAcct, err := defines.GetAccountId(ctx); err == nil {
		rec.ctxHasAccount = true
		rec.ctxAccount = ctxAcct
	}
	*e.recs = append(*e.recs, rec)
	if *e.failN > 0 {
		*e.failN--
		return moerr.NewInternalErrorNoCtx("injected stamping failure")
	}
	return nil
}

// pkMoveStampFakeOperator is a client.TxnOperator stub capturing
// registered event callbacks; every other method panics through the nil
// embedded interface, which these tests never reach.
type pkMoveStampFakeOperator struct {
	client.TxnOperator
	events    []client.EventType
	callbacks []client.TxnEventCallback
}

func (op *pkMoveStampFakeOperator) AppendEventCallback(
	event client.EventType,
	callbacks ...client.TxnEventCallback,
) {
	op.events = append(op.events, event)
	op.callbacks = append(op.callbacks, callbacks...)
}

// armPkMoveStampFault arms the D225 stamping kill switch (sarg scopes it
// to one account; empty arms every account) and schedules its disarm.
func armPkMoveStampFault(t *testing.T, sarg string) {
	t.Helper()
	fault.Enable()
	require.NoError(t, fault.AddFaultPoint(
		context.Background(),
		objectio.FJ_CNDataBranchPkMoveLinkStampFailed,
		":::", "echo", 0, sarg, false))
	t.Cleanup(func() {
		_, _ = fault.RemoveFaultPoint(
			context.Background(), objectio.FJ_CNDataBranchPkMoveLinkStampFailed)
		fault.Disable()
	})
}

// TestCollectPkMoveLinkStampSet pins the stamp-set collector: per account,
// exactly the staged full-width link ids, owned as copies.
func TestCollectPkMoveLinkStampSet(t *testing.T) {
	t.Run("collects the exact per-account link id set", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		idA := pkMoveStampTestLinkID(0xA1)
		idC := pkMoveStampTestLinkID(0xC1)
		idD := pkMoveStampTestLinkID(0xD1)
		entry7 := pkMovePairingLinkEntry(t, mp, 7, []pkMovePairRow{
			{tableID: 42, linkID: idA, stmtSeq: 1},
			{tableID: 42, linkID: nil, stmtSeq: 1},                // NULL id: skipped
			{tableID: 42, linkID: []byte{0x01, 0x02}, stmtSeq: 1}, // short id: skipped
		})
		entry8 := pkMovePairingLinkEntry(t, mp, 8, []pkMovePairRow{
			{tableID: 42, linkID: idC, stmtSeq: 1},
		})
		nonLink := pkMovePairingLinkEntry(t, mp, 7, []pkMovePairRow{
			{tableID: 42, linkID: idD, stmtSeq: 1},
		})
		nonLink.databaseName = "user_db"
		nonLink.tableName = "user_t"
		delLink := pkMoveDeleteEntry(t, mp, 7, 42, []types.Rowid{types.BuildTestRowid(1, 1)})
		delLink.databaseName = catalog.MO_CATALOG
		delLink.tableName = catalog.MO_DATA_BRANCH_UPDATE_LINK
		txn := &Transaction{proc: proc, writes: []Entry{entry7, entry8, nonLink, delLink}}
		set := txn.collectPkMoveLinkStampSetLocked()
		require.Len(t, set, 2)
		require.Equal(t, [][]byte{idA}, set[7], "NULL and short ids never enter the stamp set")
		require.Equal(t, [][]byte{idC}, set[8])
	})

	t.Run("the collected ids are copies the workspace cannot mutate", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		idA := pkMoveStampTestLinkID(0xA1)
		entry := pkMovePairingLinkEntry(t, mp, 7, []pkMovePairRow{
			{tableID: 42, linkID: idA, stmtSeq: 1},
		})
		txn := &Transaction{proc: proc, writes: []Entry{entry}}
		set := txn.collectPkMoveLinkStampSetLocked()
		require.Equal(t, [][]byte{idA}, set[7])
		// The workspace batches may be freed or reused once the commit
		// finishes: corrupting the staged cell must not reach the copy
		// the completion callback owns.
		staged := pkMovePairColVec(t, &txn.writes[0], catalog.DataBranchUpdateLinkColLinkID).GetBytesAt(0)
		for i := range staged {
			staged[i] = 0
		}
		require.Equal(t, idA, set[7][0], "the callback owns its key bytes")
	})

	t.Run("a transaction staging no link rows collects nothing", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		txn := &Transaction{proc: proc}
		require.Nil(t, txn.collectPkMoveLinkStampSetLocked())
	})

	t.Run("rows already carrying a commit_ts never re-arm the stamp", func(t *testing.T) {
		// The stamping UPDATE stages its addressed rows back as an insert
		// image with commit_ts filled; collecting that image would arm the
		// completion callback on the STAMPING transaction's own commit and
		// chain an unbounded sequence of stamping transactions. Only
		// commit_ts-NULL rows are this transaction's staged links (D225).
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		stamped := make([]byte, catalog.DataBranchUpdateLinkTSBytes)
		for i := range stamped {
			stamped[i] = 0x5A
		}
		idFresh := pkMoveStampTestLinkID(0xA2)
		idStamped := pkMoveStampTestLinkID(0xB2)
		entry := pkMovePairingLinkEntry(t, mp, 7, []pkMovePairRow{
			{tableID: 42, linkID: idFresh, stmtSeq: 1},                      // fresh link: collected
			{tableID: 42, linkID: idStamped, stmtSeq: 1, commitTS: stamped}, // stamp image: skipped
		})
		txn := &Transaction{proc: proc, writes: []Entry{entry}}
		set := txn.collectPkMoveLinkStampSetLocked()
		require.Equal(t, [][]byte{idFresh}, set[7],
			"a row already carrying commit_ts is the stamping transaction's own image, never re-armed")

		allStamped := &Transaction{proc: proc, writes: []Entry{
			pkMovePairingLinkEntry(t, mp, 7, []pkMovePairRow{
				{tableID: 42, linkID: idStamped, stmtSeq: 1, commitTS: stamped},
			}),
		}}
		require.Nil(t, allStamped.collectPkMoveLinkStampSetLocked(),
			"a stamping transaction arms no callback: the chain terminates")
	})
}

// TestStampPkMoveLinkCommitTS pins the D225 best-effort stamping channel:
// exact instant image, per-account chunking, failure swallowing, and the
// test-only kill switch.
func TestStampPkMoveLinkCommitTS(t *testing.T) {
	t.Run("stamps one update per account with the exact commit instant", func(t *testing.T) {
		var recs []pkMoveStampRecord
		failN := 0
		exec := pkMoveStampRecordingExecutor{recs: &recs, failN: &failN}
		idA := pkMoveStampTestLinkID(0xA1)
		idC := pkMoveStampTestLinkID(0xC1)
		stampPkMoveLinkCommitTS(
			func() ie.InternalExecutor { return exec },
			1234, 5,
			map[uint32][][]byte{7: {idA}, 8: {idC}},
		)
		require.Len(t, recs, 2, "one UPDATE per account shard")
		for _, r := range recs {
			require.True(t, r.ctxHasAccount,
				"the Exec ctx must carry the account: frontend session-txn creation reads it from the request context")
			require.Equal(t, r.account, r.ctxAccount,
				"the ctx account matches the routed account shard")
		}
		ts := types.BuildTS(1234, 5)
		tsHex := hex.EncodeToString(ts[:])
		byAccount := make(map[uint32]string)
		for _, r := range recs {
			byAccount[r.account] = r.sql
		}
		require.Contains(t, byAccount[7],
			"update mo_catalog.mo_data_branch_update_link set commit_ts = x'"+tsHex+"' where link_id in (")
		require.Contains(t, byAccount[7], "x'"+hex.EncodeToString(idA)+"'")
		require.Contains(t, byAccount[8], "x'"+hex.EncodeToString(idC)+"'")
	})

	t.Run("chunks large id sets and a failing chunk never stops the rest", func(t *testing.T) {
		var recs []pkMoveStampRecord
		failN := 1
		exec := pkMoveStampRecordingExecutor{recs: &recs, failN: &failN}
		ids := make([][]byte, 0, pkMoveLinkStampChunk+1)
		for i := 0; i < pkMoveLinkStampChunk+1; i++ {
			id := make([]byte, catalog.DataBranchUpdateLinkIDBytes)
			binary.BigEndian.PutUint64(id, uint64(i))
			ids = append(ids, id)
		}
		stampPkMoveLinkCommitTS(
			func() ie.InternalExecutor { return exec },
			7, 9,
			map[uint32][][]byte{7: ids},
		)
		require.Len(t, recs, 2,
			"the first chunk's failure is swallowed and the second chunk still runs")
		require.Equal(t, pkMoveLinkStampChunk, strings.Count(recs[0].sql, "x'")-1,
			"the first chunk carries a full 1024-id IN list")
		require.Equal(t, 1, strings.Count(recs[1].sql, "x'")-1,
			"the second chunk carries the remainder")
		last := hex.EncodeToString(ids[pkMoveLinkStampChunk])
		require.NotContains(t, recs[0].sql, last)
		require.Contains(t, recs[1].sql, last)
	})

	t.Run("a missing executor channel leaves rows stamp-pending without panic", func(t *testing.T) {
		require.NotPanics(t, func() {
			stampPkMoveLinkCommitTS(nil, 1, 1, map[uint32][][]byte{7: {pkMoveStampTestLinkID(0xA1)}})
		})
	})

	t.Run("the armed kill switch skips only its account", func(t *testing.T) {
		armPkMoveStampFault(t, "7")
		var recs []pkMoveStampRecord
		failN := 0
		exec := pkMoveStampRecordingExecutor{recs: &recs, failN: &failN}
		stampPkMoveLinkCommitTS(
			func() ie.InternalExecutor { return exec },
			1, 1,
			map[uint32][][]byte{7: {pkMoveStampTestLinkID(0xA1)}, 8: {pkMoveStampTestLinkID(0xC1)}},
		)
		require.Len(t, recs, 1, "account 7 stays stamp-pending by injection")
		require.Equal(t, uint32(8), recs[0].account)
	})

	t.Run("an unscoped kill switch skips every account", func(t *testing.T) {
		armPkMoveStampFault(t, "")
		var recs []pkMoveStampRecord
		failN := 0
		exec := pkMoveStampRecordingExecutor{recs: &recs, failN: &failN}
		stampPkMoveLinkCommitTS(
			func() ie.InternalExecutor { return exec },
			1, 1,
			map[uint32][][]byte{7: {pkMoveStampTestLinkID(0xA1)}, 8: {pkMoveStampTestLinkID(0xC1)}},
		)
		require.Empty(t, recs)
	})
}

// TestRegisterPkMoveLinkCommitStamp pins the commit-completion arming:
// the callback registers exactly the collected stamp set and stamps only
// a committed cost event carrying a non-zero commit instant.
func TestRegisterPkMoveLinkCommitStamp(t *testing.T) {
	newArmedTxn := func(t *testing.T, recs *[]pkMoveStampRecord) (*Transaction, *pkMoveStampFakeOperator, []byte) {
		t.Helper()
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		failN := 0
		eng := &Engine{}
		WithSQLExecFunc(func() ie.InternalExecutor {
			return pkMoveStampRecordingExecutor{recs: recs, failN: &failN}
		})(eng)
		op := &pkMoveStampFakeOperator{}
		idA := pkMoveStampTestLinkID(0xA1)
		txn := &Transaction{
			proc:   proc,
			engine: eng,
			op:     op,
			writes: []Entry{pkMovePairingLinkEntry(t, mp, 7, []pkMovePairRow{
				{tableID: 42, linkID: idA, stmtSeq: 1},
			})},
		}
		return txn, op, idA
	}

	t.Run("arms the completion callback with the collected stamp set", func(t *testing.T) {
		var recs []pkMoveStampRecord
		txn, op, idA := newArmedTxn(t, &recs)
		txn.registerPkMoveLinkCommitStampLocked()
		require.Len(t, op.callbacks, 1)
		require.Equal(t, client.CommitEvent, op.events[0])
		cb := op.callbacks[0]
		require.Equal(t, map[uint32][][]byte{7: {idA}}, cb.Value)

		committed := client.TxnEvent{
			Event:     client.CommitEvent,
			CostEvent: true,
			Txn: txnpb.TxnMeta{
				Status:   txnpb.TxnStatus_Committed,
				CommitTS: timestamp.Timestamp{PhysicalTime: 4321, LogicalTime: 7},
			},
		}
		require.NoError(t, cb.Func(context.Background(), op, committed, cb.Value))
		require.Len(t, recs, 1)
		ts := types.BuildTS(4321, 7)
		require.Contains(t, recs[0].sql, "commit_ts = x'"+hex.EncodeToString(ts[:])+"'")
		require.Equal(t, uint32(7), recs[0].account)
	})

	t.Run("the callback stamps only a committed cost event", func(t *testing.T) {
		var recs []pkMoveStampRecord
		txn, op, _ := newArmedTxn(t, &recs)
		txn.registerPkMoveLinkCommitStampLocked()
		require.Len(t, op.callbacks, 1)
		cb := op.callbacks[0]
		base := client.TxnEvent{
			Event:     client.CommitEvent,
			CostEvent: true,
			Txn: txnpb.TxnMeta{
				Status:   txnpb.TxnStatus_Committed,
				CommitTS: timestamp.Timestamp{PhysicalTime: 1, LogicalTime: 1},
			},
		}
		cases := []struct {
			name   string
			mutate func(*client.TxnEvent)
		}{
			{"not the completion phase", func(e *client.TxnEvent) { e.CostEvent = false }},
			{"a failed commit", func(e *client.TxnEvent) {
				e.Err = moerr.NewInternalErrorNoCtx("boom")
			}},
			{"an aborted transaction", func(e *client.TxnEvent) {
				e.Txn.Status = txnpb.TxnStatus_Aborted
			}},
			{"a zero commit instant", func(e *client.TxnEvent) {
				e.Txn.CommitTS = timestamp.Timestamp{}
			}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				event := base
				tc.mutate(&event)
				require.NoError(t, cb.Func(context.Background(), op, event, cb.Value))
				require.Empty(t, recs, "no stamping SQL for a non-committed or unusable event")
			})
		}
	})

	t.Run("no operator or no staged links arms nothing", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		require.NotPanics(t, func() {
			(&Transaction{proc: proc}).registerPkMoveLinkCommitStampLocked()
		})
		op := &pkMoveStampFakeOperator{}
		txn := &Transaction{proc: proc, engine: &Engine{}, op: op}
		txn.registerPkMoveLinkCommitStampLocked()
		require.Empty(t, op.callbacks)
	})
}

// TestEncodePkMoveLinkStampTS pins the stored commit_ts byte image: the
// verbatim types.TS layout (logical in the low 4 bytes, physical in the
// high 8 bytes, little-endian) the apply side decodes.
func TestEncodePkMoveLinkStampTS(t *testing.T) {
	const phys = int64(0x0102030405060708)
	const logical = uint32(0x0A0B0C0D)
	ts := encodePkMoveLinkStampTS(phys, logical)
	built := types.BuildTS(phys, logical)
	require.Equal(t, built[:], ts[:], "the stored image is the verbatim types.TS layout")
	require.Equal(t, byte(0x0D), ts[0])
	require.Equal(t, byte(0x0C), ts[1])
	require.Equal(t, byte(0x0B), ts[2])
	require.Equal(t, byte(0x0A), ts[3])
	require.Equal(t, byte(0x08), ts[4])
	require.Equal(t, byte(0x01), ts[11])
}
