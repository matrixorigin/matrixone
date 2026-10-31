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

// Carry-journal unit batteries for the r44-approved D141-D151 contracts:
//
//   D143/D144/D150: ONE transaction-level, statement-tagged,
//   chronological carry journal journals every rollback-reachable carry
//   round's effective re-key pairs as its OWN immutable entry stamped
//   with the current statement's seq (r43 review F2, D150: no composition
//   — chained rounds reference the intermediate identity exactly),
//   budget-admitted BEFORE any provenance mutation with the projected
//   charge equal to the real growth; its walker unwinds the rolled
//   statement's entries in strict reverse chronological order through the
//   exact inverse re-key, so a chained sequence restores stepwise through
//   each byte-exact intermediate state (r44: the same-statement chain
//   ends at the pre-chain state; only the cross-statement case preserves
//   the prior statement's completed state). D151 (r43 review F1, D144
//   clause (v)): the carry admission applies the shared participation
//   rule — marker-only transactions are never refused. D145 settles the
//   journal together with the retirement ledger at adoption. D148
//   replaces retained 24-byte row-id cells with the bounded byte-neutral
//   in-place overwrite, so forward plus inverse returns the measured
//   footprint to baseline (D146) and the inverse never re-admits.
//
// The boundary-window and AdvanceSnapshot integration batteries —
// post-carry write-failure rounds, adoption-then-close terminals — extend
// the D139/D140 fixtures in pk_move_boundary_finalization_test.go.

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// pkMoveJournalRidSpace tags the battery's synthetic identities to a block
// space of their own so seeds cannot alias another helper's row ids.
const pkMoveJournalRidSpace = int64(800)

func pkMoveJournalRid(row int64) types.Rowid {
	return types.BuildTestRowid(pkMoveJournalRidSpace, row)
}

func pinPkMoveCoordinatorBudget(t *testing.T, budget int64) {
	t.Helper()
	prev := pkMoveLinkCoordinatorBudget
	pkMoveLinkCoordinatorBudget = budget
	t.Cleanup(func() { pkMoveLinkCoordinatorBudget = prev })
}

// pkMoveJournalStagedLink stages one link row naming oldCell in old_row_id
// (new_row_id NULL); a nil oldCell stages a NULL old_row_id.
func pkMoveJournalStagedLink(
	t *testing.T, mp *mpool.MPool, oldCell, newCell *types.Rowid,
) Entry {
	t.Helper()
	return pkMovePruneLinkEntry(t, mp, 7,
		[]uint64{42},
		[]*types.Rowid{oldCell},
		[]*types.Rowid{newCell},
		[]uint64{700})
}

// pkMoveJournalCarriers reads the four provenance carriers the round-trip
// assertions observe: the staged old_row_id cell, the origin ledger value
// at probeKey, the heal target, and the lineage-binding origin.
func pkMoveJournalCarriers(
	t *testing.T, txn *Transaction, probeKey types.Rowid,
) (cell, originVal, heal, lineage types.Rowid) {
	t.Helper()
	require.NotEmpty(t, txn.writes)
	bat := txn.writes[0].bat
	raw := bat.Vecs[4].GetBytesAt(0)
	require.Len(t, raw, types.RowidSize)
	copy(cell[:], raw)
	originVal = txn.pkMoveRowidOrigins[probeKey]
	heal = txn.pkMoveNullHeals["heal-1"].rid
	lineage = txn.pkMoveKeyLineage[pkMoveAcctTbl{account: 7, table: 42}][string([]byte{0xab})][0].origin
	return cell, originVal, heal, lineage
}

// pkMoveJournalSeedAll builds a transaction whose staged old cell, origin
// ledger value, heal target, and lineage origin all name carrier.
func pkMoveJournalSeedAll(
	t *testing.T, carrier, probeKey types.Rowid,
) (*Transaction, *Entry) {
	t.Helper()
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	link := pkMoveJournalStagedLink(t, mp, &carrier, nil)
	txn := &Transaction{proc: proc}
	txn.writes = []Entry{link}
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{probeKey: carrier}
	txn.pkMoveNullHeals = map[string]pkMoveNullHeal{
		"heal-1": {rid: carrier, stmt: 1},
	}
	txn.pkMoveKeyLineage = map[pkMoveAcctTbl]map[string][]pkMoveLineageBind{
		{account: 7, table: 42}: {
			string([]byte{0xab}): {{origin: carrier, wm: 0, stmt: 1}},
		},
	}
	return txn, &txn.writes[0]
}

func TestPkMoveCarryJournalAdmission(t *testing.T) {
	t.Run("unequal lengths fail loudly before anything mutates", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		txn := &Transaction{proc: proc}
		r1, r2 := pkMoveJournalRid(1), pkMoveJournalRid(2)
		effOld, effNew, err := txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{r1, r2}, []types.Rowid{r1})
		require.ErrorContains(t, err, "length mismatch")
		require.Nil(t, effOld)
		require.Nil(t, effNew)
		require.Empty(t, txn.pkMoveCarryJournal)
	})

	t.Run("a non-injective old side fails loudly", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		txn := &Transaction{proc: proc}
		r1 := pkMoveJournalRid(1)
		_, _, err := txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{r1, r1},
			[]types.Rowid{pkMoveJournalRid(2), pkMoveJournalRid(3)})
		require.ErrorContains(t, err, "not injective")
		require.Empty(t, txn.pkMoveCarryJournal)
	})

	t.Run("empty pairs admit vacuously", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		txn := &Transaction{proc: proc}
		effOld, effNew, err := txn.pkMoveCarryJournalAdmitLocked(nil, nil)
		require.NoError(t, err)
		require.Empty(t, effOld)
		require.Empty(t, effNew)
	})

	t.Run("the effective-only filter drops un-referenced pairs", func(t *testing.T) {
		// D144: a pair whose old identity no provenance carrier names is a
		// definitional no-op for the inverse, so it never reaches the
		// journal (the call site still feeds the mutation the FULL pair
		// list — a no-op pair costs one map probe there).
		proc := testutil.NewProcess(t)
		referenced := pkMoveJournalRid(10)
		other := pkMoveJournalRid(11)
		orphan := pkMoveJournalRid(12)
		txn := &Transaction{proc: proc}
		txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{
			other: referenced,
		}
		effOld, effNew, err := txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{orphan, referenced, other},
			[]types.Rowid{pkMoveJournalRid(20), pkMoveJournalRid(21), pkMoveJournalRid(22)})
		require.NoError(t, err)
		require.Equal(t, []types.Rowid{referenced, other}, effOld)
		require.Equal(t, []types.Rowid{pkMoveJournalRid(21), pkMoveJournalRid(22)}, effNew)
	})

	t.Run("the reference set covers every provenance carrier", func(t *testing.T) {
		// D144's admission filter and the remap engine must agree on what
		// is referenceable: staged old/new link cells, origin keys AND
		// values, null-heal targets, and lineage-binding origins.
		setup := map[string]func(txn *Transaction, mp *mpool.MPool, carrier types.Rowid){
			"origin key": func(txn *Transaction, _ *mpool.MPool, carrier types.Rowid) {
				txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{
					carrier: pkMoveJournalRid(90)}
			},
			"origin value": func(txn *Transaction, _ *mpool.MPool, carrier types.Rowid) {
				txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{
					pkMoveJournalRid(90): carrier}
			},
			"null-heal target": func(txn *Transaction, _ *mpool.MPool, carrier types.Rowid) {
				txn.pkMoveNullHeals = map[string]pkMoveNullHeal{
					"h": {rid: carrier, stmt: 1}}
			},
			"lineage origin": func(txn *Transaction, _ *mpool.MPool, carrier types.Rowid) {
				txn.pkMoveKeyLineage = map[pkMoveAcctTbl]map[string][]pkMoveLineageBind{
					{account: 7, table: 42}: {
						"k": {{origin: carrier, stmt: 1}},
					},
				}
			},
			"staged old cell": func(txn *Transaction, mp *mpool.MPool, carrier types.Rowid) {
				txn.writes = []Entry{pkMoveJournalStagedLink(t, mp, &carrier, nil)}
			},
			"staged new cell": func(txn *Transaction, mp *mpool.MPool, carrier types.Rowid) {
				txn.writes = []Entry{pkMoveJournalStagedLink(t, mp, nil, &carrier)}
			},
		}
		for name, seed := range setup {
			t.Run(name, func(t *testing.T) {
				proc := testutil.NewProcess(t)
				carrier := pkMoveJournalRid(30)
				orphan := pkMoveJournalRid(31)
				txn := &Transaction{proc: proc}
				seed(txn, proc.Mp(), carrier)
				effOld, effNew, err := txn.pkMoveCarryJournalAdmitLocked(
					[]types.Rowid{orphan, carrier},
					[]types.Rowid{pkMoveJournalRid(40), pkMoveJournalRid(41)})
				require.NoError(t, err)
				require.Equal(t, []types.Rowid{carrier}, effOld)
				require.Equal(t, []types.Rowid{pkMoveJournalRid(41)}, effNew)
			})
		}
	})

	t.Run("zero budget disables admission", func(t *testing.T) {
		pinPkMoveCoordinatorBudget(t, 0)
		carrier, probe := pkMoveJournalRid(50), pkMoveJournalRid(90)
		txn, _ := pkMoveJournalSeedAll(t, carrier, probe)
		txn.pkMoveLinkStagedBytes = int64(1) << 40
		_, _, err := txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{carrier}, []types.Rowid{pkMoveJournalRid(51)})
		require.NoError(t, err)
	})

	t.Run("a saturated budget refuses before any provenance state mutates", func(t *testing.T) {
		pinPkMoveCoordinatorBudget(t, 1)
		carrier, probe := pkMoveJournalRid(50), pkMoveJournalRid(90)
		txn, link := pkMoveJournalSeedAll(t, carrier, probe)
		before := link.bat.Vecs[4].GetBytesAt(0)
		effOld, effNew, err := txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{carrier}, []types.Rowid{pkMoveJournalRid(51)})
		require.ErrorContains(t, err, "exceeds the coordinator retention budget")
		require.Nil(t, effOld)
		require.Nil(t, effNew)
		// Refusal happened BEFORE any mutation: maps and staged cells are
		// byte-identical, and nothing reached the journal.
		require.Equal(t, map[types.Rowid]types.Rowid{probe: carrier},
			txn.pkMoveRowidOrigins)
		require.Equal(t, carrier, txn.pkMoveNullHeals["heal-1"].rid)
		require.Equal(t,
			carrier,
			txn.pkMoveKeyLineage[pkMoveAcctTbl{account: 7, table: 42}][string([]byte{0xab})][0].origin)
		require.Equal(t, before, link.bat.Vecs[4].GetBytesAt(0))
		require.Empty(t, txn.pkMoveCarryJournal)
	})

	t.Run("every effective pair pays its own fare: a chained round charges full growth and refuses before mutation", func(t *testing.T) {
		// D150(d) (r43 review F2): no composition discount — a chained
		// round rides its OWN chronological entry, so the projected
		// charge is exactly the real growth: one pair slice per
		// effective pair plus one entry overhead per round, and a
		// refusal still lands before any mutation.
		carrier, probe := pkMoveJournalRid(50), pkMoveJournalRid(90)
		txn, _ := pkMoveJournalSeedAll(t, carrier, probe)
		base := txn.pkMoveBookkeepBytesLocked()
		roundCharge := int64(2*types.RowidSize+16) + pkMoveBookkeepEntryOverhead
		pinPkMoveCoordinatorBudget(t, base+roundCharge)
		n2 := pkMoveJournalRid(51)
		m2 := pkMoveJournalRid(52)
		effOld, effNew, err := txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{carrier}, []types.Rowid{n2})
		require.NoError(t, err)
		txn.pkMoveCarryJournalAppendLocked(pkMoveCarryObject, effOld, effNew)
		require.Equal(t, base+roundCharge, txn.pkMoveBookkeepBytesLocked())
		// Carry the identity forward so the chained round references it.
		require.NoError(t, txn.carryPkMoveProvenanceThroughRekeyLocked(
			[]types.Rowid{carrier}, []types.Rowid{n2}))
		// The chained round pays full fare for its own entry: the
		// exact-fit budget refuses it BEFORE anything mutates — journal,
		// measured charge, and every carried carrier stay untouched.
		effOld2, effNew2, err := txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{n2}, []types.Rowid{m2})
		require.ErrorContains(t, err, "exceeds the coordinator retention budget")
		require.Nil(t, effOld2)
		require.Nil(t, effNew2)
		require.Len(t, txn.pkMoveCarryJournal, 1)
		require.Equal(t, base+roundCharge, txn.pkMoveBookkeepBytesLocked())
		cell, originVal, heal, lineage := pkMoveJournalCarriers(t, txn, probe)
		require.Equal(t, n2, cell)
		require.Equal(t, n2, originVal)
		require.Equal(t, n2, heal)
		require.Equal(t, n2, lineage)
		// Headroom for the chained round's own entry admits it; the
		// measured charge equals the real two-entry growth exactly.
		pinPkMoveCoordinatorBudget(t, base+2*roundCharge)
		effOld2, effNew2, err = txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{n2}, []types.Rowid{m2})
		require.NoError(t, err)
		require.Equal(t, []types.Rowid{n2}, effOld2)
		txn.pkMoveCarryJournalAppendLocked(pkMoveCarryWave, effOld2, effNew2)
		require.Len(t, txn.pkMoveCarryJournal, 2)
		require.Equal(t, []types.Rowid{n2}, txn.pkMoveCarryJournal[1].oldIds,
			"the chained entry references the intermediate identity exactly")
		require.Equal(t, []types.Rowid{m2}, txn.pkMoveCarryJournal[1].newIds)
		require.Equal(t, base+2*roundCharge, txn.pkMoveBookkeepBytesLocked(),
			"a chain of N rounds charges N effective pairs: charge == growth")
	})
}

func TestPkMoveCarryJournalAppendPerRound(t *testing.T) {
	t.Run("chained rounds journal independent immutable entries stamped with the current seq", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		txn := &Transaction{proc: proc}
		txn.statementID = 1
		a, n, m := pkMoveJournalRid(1), pkMoveJournalRid(2), pkMoveJournalRid(3)
		txn.pkMoveCarryJournalAppendLocked(
			pkMoveCarryObject, []types.Rowid{a}, []types.Rowid{n})
		require.Len(t, txn.pkMoveCarryJournal, 1)
		txn.pkMoveCarryJournalAppendLocked(
			pkMoveCarryDump, []types.Rowid{n}, []types.Rowid{m})
		// D150 (r43 review F2): the chained round rides its OWN
		// chronological entry referencing the intermediate identity — no
		// composition — and the earlier entry is immutable once written.
		require.Len(t, txn.pkMoveCarryJournal, 2)
		e0 := txn.pkMoveCarryJournal[0]
		require.Equal(t, 1, e0.seq)
		require.Equal(t, pkMoveCarryObject, e0.kind)
		require.Equal(t, []types.Rowid{a}, e0.oldIds)
		require.Equal(t, []types.Rowid{n}, e0.newIds,
			"the chaining append must not rewrite the earlier entry")
		e1 := txn.pkMoveCarryJournal[1]
		require.Equal(t, 1, e1.seq,
			"the chained round carries the CURRENT statement's seq")
		require.Equal(t, pkMoveCarryDump, e1.kind,
			"each round keeps its own kind")
		require.Equal(t, []types.Rowid{n}, e1.oldIds)
		require.Equal(t, []types.Rowid{m}, e1.newIds)
	})

	t.Run("independent rounds append chronologically with their kinds", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		txn := &Transaction{proc: proc}
		txn.statementID = 1
		a, b := pkMoveJournalRid(1), pkMoveJournalRid(11)
		c, d := pkMoveJournalRid(2), pkMoveJournalRid(12)
		txn.pkMoveCarryJournalAppendLocked(
			pkMoveCarryWave, []types.Rowid{a}, []types.Rowid{b})
		txn.pkMoveCarryJournalAppendLocked(
			pkMoveCarryObject, []types.Rowid{c}, []types.Rowid{d})
		require.Len(t, txn.pkMoveCarryJournal, 2)
		require.Equal(t, pkMoveCarryWave, txn.pkMoveCarryJournal[0].kind)
		require.Equal(t, pkMoveCarryObject, txn.pkMoveCarryJournal[1].kind)
		require.Equal(t, []types.Rowid{a}, txn.pkMoveCarryJournal[0].oldIds)
		require.Equal(t, []types.Rowid{d}, txn.pkMoveCarryJournal[1].newIds)
	})

	t.Run("a chained pair inside a later round rides its own entry while the earlier entry stays immutable", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		txn := &Transaction{proc: proc}
		txn.statementID = 1
		a, n, m := pkMoveJournalRid(1), pkMoveJournalRid(2), pkMoveJournalRid(3)
		d, e := pkMoveJournalRid(4), pkMoveJournalRid(5)
		txn.pkMoveCarryJournalAppendLocked(
			pkMoveCarryObject, []types.Rowid{a, d}, []types.Rowid{n, e})
		txn.pkMoveCarryJournalAppendLocked(
			pkMoveCarryObject, []types.Rowid{n}, []types.Rowid{m})
		require.Len(t, txn.pkMoveCarryJournal, 2)
		entry0 := txn.pkMoveCarryJournal[0]
		require.Equal(t, []types.Rowid{a, d}, entry0.oldIds)
		require.Equal(t, []types.Rowid{n, e}, entry0.newIds,
			"the first round's entry is never rewritten by a later chain")
		entry1 := txn.pkMoveCarryJournal[1]
		require.Equal(t, []types.Rowid{n}, entry1.oldIds)
		require.Equal(t, []types.Rowid{m}, entry1.newIds)
	})
}

func TestUnwindPkMoveCarryJournal(t *testing.T) {
	probe := pkMoveJournalRid(90)

	t.Run("the exact inverse restores cells, origins, heals, and lineage origins", func(t *testing.T) {
		carrier := pkMoveJournalRid(50)
		n2 := pkMoveJournalRid(51)
		txn, _ := pkMoveJournalSeedAll(t, carrier, probe)
		txn.statementID = 1
		effOld, effNew, err := txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{carrier}, []types.Rowid{n2})
		require.NoError(t, err)
		txn.pkMoveCarryJournalAppendLocked(pkMoveCarryObject, effOld, effNew)
		require.NoError(t, txn.carryPkMoveProvenanceThroughRekeyLocked(
			[]types.Rowid{carrier}, []types.Rowid{n2}))
		cell, originVal, heal, lineage := pkMoveJournalCarriers(t, txn, probe)
		require.Equal(t, n2, cell)
		require.Equal(t, n2, originVal)
		require.Equal(t, n2, heal)
		require.Equal(t, n2, lineage)

		require.NoError(t, txn.unwindPkMoveCarryJournalLocked(1))
		cell, originVal, heal, lineage = pkMoveJournalCarriers(t, txn, probe)
		require.Equal(t, carrier, cell)
		require.Equal(t, carrier, originVal)
		require.Equal(t, carrier, heal)
		require.Equal(t, carrier, lineage)
		require.Empty(t, txn.pkMoveCarryJournal)
	})

	t.Run("a two-round chain unwinds stepwise through the byte-exact intermediate checkpoint", func(t *testing.T) {
		// D150 (r43 review F2, r44 oracle): the chain keeps TWO entries;
		// rolling back their shared statement unwinds the failed-or-last
		// round's inverse first — landing the carriers on the byte-exact
		// intermediate checkpoint — then the first round's inverse,
		// ending at the pre-chain state.
		carrier := pkMoveJournalRid(50)
		n2, m2 := pkMoveJournalRid(51), pkMoveJournalRid(52)
		txn, _ := pkMoveJournalSeedAll(t, carrier, probe)
		txn.statementID = 1
		// Round 1: journal and carry O -> N.
		effOld, effNew, err := txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{carrier}, []types.Rowid{n2})
		require.NoError(t, err)
		txn.pkMoveCarryJournalAppendLocked(pkMoveCarryObject, effOld, effNew)
		require.NoError(t, txn.carryPkMoveProvenanceThroughRekeyLocked(
			[]types.Rowid{carrier}, []types.Rowid{n2}))
		// Round 2: the chained mint rides its own entry.
		effOld2, effNew2, err := txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{n2}, []types.Rowid{m2})
		require.NoError(t, err)
		txn.pkMoveCarryJournalAppendLocked(pkMoveCarryWave, effOld2, effNew2)
		require.NoError(t, txn.carryPkMoveProvenanceThroughRekeyLocked(
			[]types.Rowid{n2}, []types.Rowid{m2}))
		require.Len(t, txn.pkMoveCarryJournal, 2,
			"no composition: each round of the chain keeps its own entry")
		require.Equal(t, []types.Rowid{carrier}, txn.pkMoveCarryJournal[0].oldIds)
		require.Equal(t, []types.Rowid{n2}, txn.pkMoveCarryJournal[0].newIds)
		require.Equal(t, []types.Rowid{n2}, txn.pkMoveCarryJournal[1].oldIds)
		require.Equal(t, []types.Rowid{m2}, txn.pkMoveCarryJournal[1].newIds)
		cell, _, _, lineage := pkMoveJournalCarriers(t, txn, probe)
		require.Equal(t, m2, cell)
		require.Equal(t, m2, lineage)

		// Record the retained-cell inverse stream: the unwind must pass
		// through the byte-exact N checkpoint before reaching O.
		origOverwrite := pkMoveVecOverwriteRowidAt
		t.Cleanup(func() { pkMoveVecOverwriteRowidAt = origOverwrite })
		var recs []pkMoveOverwriteRecord
		pkMoveVecOverwriteRowidAt = func(v *vector.Vector, idx int, bs []byte, m *mpool.MPool) error {
			var rid types.Rowid
			copy(rid[:], bs)
			recs = append(recs, pkMoveOverwriteRecord{row: idx, payload: rid})
			return origOverwrite(v, idx, bs, m)
		}

		require.NoError(t, txn.unwindPkMoveCarryJournalLocked(1))
		require.Equal(t, []pkMoveOverwriteRecord{
			{row: 0, payload: n2},
			{row: 0, payload: carrier},
		}, recs,
			"stepwise unwind: the byte-exact intermediate N checkpoint, then the pre-chain O state")
		cell2, originVal2, heal2, lineage2 := pkMoveJournalCarriers(t, txn, probe)
		require.Equal(t, carrier, cell2)
		require.Equal(t, carrier, originVal2)
		require.Equal(t, carrier, heal2)
		require.Equal(t, carrier, lineage2)
		require.Empty(t, txn.pkMoveCarryJournal)
	})

	t.Run("kind-interleaved entries unwind every pair", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		a, b, c := pkMoveJournalRid(60), pkMoveJournalRid(61), pkMoveJournalRid(62)
		a2, b2, c2 := pkMoveJournalRid(63), pkMoveJournalRid(64), pkMoveJournalRid(65)
		txn := &Transaction{proc: proc}
		txn.writes = []Entry{pkMoveJournalStagedLink(t, mp, &a, nil)}
		txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{probe: a}
		txn.pkMoveNullHeals = map[string]pkMoveNullHeal{"heal-1": {rid: b, stmt: 1}}
		txn.pkMoveKeyLineage = map[pkMoveAcctTbl]map[string][]pkMoveLineageBind{
			{account: 7, table: 42}: {string([]byte{0xab}): {{origin: c, stmt: 1}}},
		}
		txn.statementID = 1
		for _, round := range []struct {
			kind         pkMoveCarryKind
			oldID, newID types.Rowid
		}{
			{pkMoveCarryDump, a, a2},
			{pkMoveCarryWave, b, b2},
			{pkMoveCarryObject, c, c2},
		} {
			effOld, effNew, err := txn.pkMoveCarryJournalAdmitLocked(
				[]types.Rowid{round.oldID}, []types.Rowid{round.newID})
			require.NoError(t, err)
			txn.pkMoveCarryJournalAppendLocked(round.kind, effOld, effNew)
			require.NoError(t, txn.carryPkMoveProvenanceThroughRekeyLocked(
				[]types.Rowid{round.oldID}, []types.Rowid{round.newID}))
		}
		require.Len(t, txn.pkMoveCarryJournal, 3)
		require.Equal(t, pkMoveCarryDump, txn.pkMoveCarryJournal[0].kind)
		require.Equal(t, pkMoveCarryWave, txn.pkMoveCarryJournal[1].kind)
		require.Equal(t, pkMoveCarryObject, txn.pkMoveCarryJournal[2].kind)
		cell, _, heal, lineage := pkMoveJournalCarriers(t, txn, probe)
		require.Equal(t, a2, cell)
		require.Equal(t, b2, heal)
		require.Equal(t, c2, lineage)

		require.NoError(t, txn.unwindPkMoveCarryJournalLocked(1))
		cell, originVal, heal, lineage := pkMoveJournalCarriers(t, txn, probe)
		require.Equal(t, a, cell)
		require.Equal(t, a, originVal)
		require.Equal(t, b, heal)
		require.Equal(t, c, lineage)
		require.Empty(t, txn.pkMoveCarryJournal)
	})

	t.Run("strict reverse order restores inter-entry chains", func(t *testing.T) {
		// D150 keeps every round of an intra-statement chain in its own
		// entry; this crafted two-entry chain pins the walker's documented
		// strict-reverse discipline: walking forward would leave the state
		// parked on the middle identity B.
		carrier := pkMoveJournalRid(50)
		b2, c2 := pkMoveJournalRid(51), pkMoveJournalRid(52)
		txn, _ := pkMoveJournalSeedAll(t, carrier, probe)
		require.NoError(t, txn.carryPkMoveProvenanceThroughRekeyLocked(
			[]types.Rowid{carrier}, []types.Rowid{b2}))
		require.NoError(t, txn.carryPkMoveProvenanceThroughRekeyLocked(
			[]types.Rowid{b2}, []types.Rowid{c2}))
		txn.pkMoveCarryJournal = []pkMoveCarryJournalEntry{
			{seq: 1, kind: pkMoveCarryObject,
				oldIds: []types.Rowid{carrier}, newIds: []types.Rowid{b2}},
			{seq: 1, kind: pkMoveCarryWave,
				oldIds: []types.Rowid{b2}, newIds: []types.Rowid{c2}},
		}
		require.NoError(t, txn.unwindPkMoveCarryJournalLocked(1))
		cell, originVal, heal, lineage := pkMoveJournalCarriers(t, txn, probe)
		require.Equal(t, carrier, cell)
		require.Equal(t, carrier, originVal)
		require.Equal(t, carrier, heal)
		require.Equal(t, carrier, lineage)
		require.Empty(t, txn.pkMoveCarryJournal)
	})

	t.Run("entries older than the rolled statement stay journaled", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		p1, q1 := pkMoveJournalRid(70), pkMoveJournalRid(71)
		p2, q2 := pkMoveJournalRid(72), pkMoveJournalRid(73)
		keyP, keyQ := pkMoveJournalRid(90), pkMoveJournalRid(91)
		txn := &Transaction{proc: proc}
		txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{keyP: p1, keyQ: q1}
		txn.statementID = 1
		effOld, effNew, err := txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{p1}, []types.Rowid{p2})
		require.NoError(t, err)
		txn.pkMoveCarryJournalAppendLocked(pkMoveCarryWave, effOld, effNew)
		require.NoError(t, txn.carryPkMoveProvenanceThroughRekeyLocked(
			[]types.Rowid{p1}, []types.Rowid{p2}))
		txn.statementID = 2
		effOld2, effNew2, err := txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{q1}, []types.Rowid{q2})
		require.NoError(t, err)
		txn.pkMoveCarryJournalAppendLocked(pkMoveCarryObject, effOld2, effNew2)
		require.NoError(t, txn.carryPkMoveProvenanceThroughRekeyLocked(
			[]types.Rowid{q1}, []types.Rowid{q2}))
		require.Len(t, txn.pkMoveCarryJournal, 2)

		// Rolling back statement 2 unwinds only its own carry; statement
		// 1's entry stays journaled and its re-key survives intact.
		require.NoError(t, txn.unwindPkMoveCarryJournalLocked(2))
		require.Equal(t, q1, txn.pkMoveRowidOrigins[keyQ])
		require.Equal(t, p2, txn.pkMoveRowidOrigins[keyP])
		require.Len(t, txn.pkMoveCarryJournal, 1)
		require.Equal(t, 1, txn.pkMoveCarryJournal[0].seq)
		require.Equal(t, []types.Rowid{p2}, txn.pkMoveCarryJournal[0].newIds)
	})

	t.Run("an out-of-order stamp surfaces loudly and leaves the journal untouched", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		txn := &Transaction{proc: proc}
		a, b := pkMoveJournalRid(1), pkMoveJournalRid(2)
		c, d := pkMoveJournalRid(3), pkMoveJournalRid(4)
		txn.pkMoveCarryJournal = []pkMoveCarryJournalEntry{
			{seq: 5, kind: pkMoveCarryObject,
				oldIds: []types.Rowid{a}, newIds: []types.Rowid{b}},
			{seq: 3, kind: pkMoveCarryObject,
				oldIds: []types.Rowid{c}, newIds: []types.Rowid{d}},
		}
		err := txn.unwindPkMoveCarryJournalLocked(4)
		require.ErrorContains(t, err, "ordering violation")
		require.Len(t, txn.pkMoveCarryJournal, 2)
		require.Equal(t, []types.Rowid{a}, txn.pkMoveCarryJournal[0].oldIds)
		require.Equal(t, []types.Rowid{c}, txn.pkMoveCarryJournal[1].oldIds)
	})
}

func TestSettlePkMoveAdoptedLedgers(t *testing.T) {
	t.Run("adoption settles the retirement prefix and drops adopted journal seqs", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		txn := &Transaction{proc: proc}
		txn.statementID = 2
		txn.retiredTransfers = []retiredTransferRecord{
			{statementSeq: 1},
			{statementSeq: 2},
			{statementSeq: 3},
		}
		txn.pkMoveCarryJournal = []pkMoveCarryJournalEntry{
			{seq: 1, kind: pkMoveCarryDump,
				oldIds: []types.Rowid{pkMoveJournalRid(1)},
				newIds: []types.Rowid{pkMoveJournalRid(2)}},
			{seq: 2, kind: pkMoveCarryObject,
				oldIds: []types.Rowid{pkMoveJournalRid(3)},
				newIds: []types.Rowid{pkMoveJournalRid(4)}},
			{seq: 3, kind: pkMoveCarryWave,
				oldIds: []types.Rowid{pkMoveJournalRid(5)},
				newIds: []types.Rowid{pkMoveJournalRid(6)}},
		}
		require.NoError(t, txn.settlePkMoveAdoptedLedgersLocked())
		require.Equal(t, 2, txn.retiredTransfersSettled)
		require.Len(t, txn.retiredTransfers, 3, "settlement re-labels; it never removes records")
		require.Len(t, txn.pkMoveCarryJournal, 1)
		require.Equal(t, 3, txn.pkMoveCarryJournal[0].seq)
	})

	t.Run("an empty ledger settles vacuously", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		txn := &Transaction{proc: proc}
		txn.statementID = 7
		require.NoError(t, txn.settlePkMoveAdoptedLedgersLocked())
		require.Zero(t, txn.retiredTransfersSettled)
		require.Empty(t, txn.pkMoveCarryJournal)
	})

	t.Run("an ordering inversion beyond the boundary fails loudly without state change", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		txn := &Transaction{proc: proc}
		txn.statementID = 2
		txn.retiredTransfers = []retiredTransferRecord{
			{statementSeq: 1},
			{statementSeq: 3},
			{statementSeq: 2},
		}
		txn.pkMoveCarryJournal = []pkMoveCarryJournalEntry{
			{seq: 1, kind: pkMoveCarryObject,
				oldIds: []types.Rowid{pkMoveJournalRid(1)},
				newIds: []types.Rowid{pkMoveJournalRid(2)}},
		}
		err := txn.settlePkMoveAdoptedLedgersLocked()
		require.ErrorContains(t, err, "settlement ordering violation")
		require.Zero(t, txn.retiredTransfersSettled)
		require.Len(t, txn.pkMoveCarryJournal, 1)
	})

	t.Run("resurrection rejects a settled prefix longer than the ledger", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		txn := &Transaction{proc: proc}
		txn.retiredTransfers = []retiredTransferRecord{{statementSeq: 1}}
		txn.retiredTransfersSettled = 3
		err := txn.resurrectRetiredTransferEntriesLocked(1)
		require.ErrorContains(t, err, "settled prefix")
	})

	t.Run("fresh same-seq work stays active while the settled record stays put", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		txn := &Transaction{proc: proc}
		// D145 core: the ensuing statement re-issues the adopted numeric
		// seq for its FRESH work, so eligibility is ledger MEMBERSHIP,
		// never the numeric value. The settled record (with its entry,
		// never entered) and the fresh record bear the same seq 2; only
		// the fresh one is resurrection-reachable.
		freshSeq := 2
		txn.retiredTransfers = []retiredTransferRecord{
			{statementSeq: freshSeq, entries: make([]retiredTransferEntry, 1)},
			{statementSeq: freshSeq},
		}
		txn.retiredTransfersSettled = 1
		require.NoError(t, txn.resurrectRetiredTransferEntriesLocked(freshSeq))
		require.Len(t, txn.retiredTransfers, 1)
		require.Len(t, txn.retiredTransfers[0].entries, 1,
			"the settled prefix record survives untouched with its entry")
	})
}

func TestPkMoveVecOverwriteRowidAt(t *testing.T) {
	newVec := func(t *testing.T, mp *mpool.MPool, payloads ...[]byte) *vector.Vector {
		t.Helper()
		vec := vector.NewVec(types.T_binary.ToType())
		for _, p := range payloads {
			if p == nil {
				require.NoError(t, pkMoveVecAppendBytes(vec, nil, true, mp))
				continue
			}
			require.NoError(t, pkMoveVecAppendBytes(vec, p, false, mp))
		}
		return vec
	}

	t.Run("a forward-plus-inverse overwrite is byte-neutral and storage-neutral", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		o0, o1, o2 := pkMoveJournalRid(1), pkMoveJournalRid(2), pkMoveJournalRid(3)
		n1 := pkMoveJournalRid(4)
		vec := newVec(t, mp, o0[:], o1[:], o2[:])
		defer vec.Free(mp)
		require.Equal(t, 3, vec.Length())
		base := vec.Size()

		require.NoError(t, pkMoveVecOverwriteRowidAtBase(vec, 1, n1[:], mp))
		require.Equal(t, n1[:], vec.GetBytesAt(1))
		require.Equal(t, o0[:], vec.GetBytesAt(0))
		require.Equal(t, o2[:], vec.GetBytesAt(2))
		require.Equal(t, base, vec.Size())

		// The exact inverse rides the same bounded path home.
		require.NoError(t, pkMoveVecOverwriteRowidAtBase(vec, 1, o1[:], mp))
		require.Equal(t, o1[:], vec.GetBytesAt(1))
		require.Equal(t, base, vec.Size())
	})

	t.Run("a wrong payload length refuses without mutation", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		o0 := pkMoveJournalRid(1)
		vec := newVec(t, mp, o0[:])
		defer vec.Free(mp)
		for _, bad := range [][]byte{make([]byte, 23), make([]byte, 25)} {
			require.ErrorContains(t,
				pkMoveVecOverwriteRowidAtBase(vec, 0, bad, mp),
				"replacement payload")
			require.Equal(t, o0[:], vec.GetBytesAt(0))
		}
	})

	t.Run("a const vector refuses loudly", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		o0, n1 := pkMoveJournalRid(1), pkMoveJournalRid(2)
		vec, err := vector.NewConstBytes(
			types.T_binary.ToType(), o0[:], 2, mp)
		require.NoError(t, err)
		defer vec.Free(mp)
		require.ErrorContains(t,
			pkMoveVecOverwriteRowidAtBase(vec, 0, n1[:], mp),
			"const vector")
	})

	t.Run("an out-of-range row refuses loudly", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		o0, n1 := pkMoveJournalRid(1), pkMoveJournalRid(2)
		vec := newVec(t, mp, o0[:])
		defer vec.Free(mp)
		for _, row := range []int{-1, 1, 100} {
			require.ErrorContains(t,
				pkMoveVecOverwriteRowidAtBase(vec, row, n1[:], mp),
				"outside vector length")
		}
	})

	t.Run("a null cell refuses loudly", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		o0, n1 := pkMoveJournalRid(1), pkMoveJournalRid(2)
		vec := newVec(t, mp, o0[:], nil)
		defer vec.Free(mp)
		require.ErrorContains(t,
			pkMoveVecOverwriteRowidAtBase(vec, 1, n1[:], mp),
			"null cell")
	})

	t.Run("a short displaced cell refuses loudly", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		// A 23-byte varlena inlines: appending a 24-byte payload over it is
		// NOT the bounded area rewrite, so the overwrite has no appending
		// fallback — it fails the statement instead of minting uncharged
		// retained growth.
		n1 := pkMoveJournalRid(2)
		vec := newVec(t, mp, make([]byte, 23))
		defer vec.Free(mp)
		require.ErrorContains(t,
			pkMoveVecOverwriteRowidAtBase(vec, 0, n1[:], mp),
			"displaced payload")
		require.Len(t, vec.GetBytesAt(0), 23)
	})
}

func TestPkMoveCarryJournalFootprint(t *testing.T) {
	probe := pkMoveJournalRid(90)

	t.Run("forward plus inverse returns the measured footprint to baseline", func(t *testing.T) {
		p1, q1 := pkMoveJournalRid(70), pkMoveJournalRid(71)
		p2, q2 := pkMoveJournalRid(72), pkMoveJournalRid(73)
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		txn := &Transaction{proc: proc}
		txn.writes = []Entry{
			pkMovePruneLinkEntry(t, mp, 7,
				[]uint64{42, 42},
				[]*types.Rowid{&p1, &q1},
				[]*types.Rowid{nil, nil},
				[]uint64{700, 701}),
		}
		txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{probe: p1}
		txn.statementID = 1
		txn.accountWorkspaceEntryLocked(&txn.writes[0])
		bat := txn.writes[0].bat
		baseBat := bat.Size()
		baseWs := txn.workspaceSize
		require.NotZero(t, baseWs)

		effOld, effNew, err := txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{p1, q1}, []types.Rowid{p2, q2})
		require.NoError(t, err)
		txn.pkMoveCarryJournalAppendLocked(pkMoveCarryObject, effOld, effNew)
		require.NoError(t, txn.carryPkMoveProvenanceThroughRekeyLocked(
			[]types.Rowid{p1, q1}, []types.Rowid{p2, q2}))
		require.Equal(t, p2[:], bat.Vecs[4].GetBytesAt(0))
		require.Equal(t, q2[:], bat.Vecs[4].GetBytesAt(1))
		// D148/D146: the in-place overwrite leaves the batch's measured
		// size, and with it the workspace accounting, untouched.
		require.Equal(t, baseBat, bat.Size())
		require.Equal(t, baseWs, txn.workspaceSize)
		require.Equal(t, uint64(baseBat), txn.writes[0].accountedSize)

		require.NoError(t, txn.unwindPkMoveCarryJournalLocked(1))
		require.Equal(t, p1[:], bat.Vecs[4].GetBytesAt(0))
		require.Equal(t, q1[:], bat.Vecs[4].GetBytesAt(1))
		require.Equal(t, baseBat, bat.Size())
		require.Equal(t, baseWs, txn.workspaceSize)
		require.NoError(t, txn.checkWorkspaceAccountingLocked())
		txn.unaccountWorkspaceEntryLocked(&txn.writes[0])
		require.Zero(t, txn.workspaceSize)
	})

	t.Run("repeated forced mid-carry failures leave no footprint and the retry succeeds", func(t *testing.T) {
		// D148's footprint regression: k forced post-carry failures must
		// leave the retained cells, the measured batch size, and the
		// bookkeep charge exactly at baseline, and the un-armed retry then
		// carries cleanly under the unchanged budget.
		p1, q1 := pkMoveJournalRid(70), pkMoveJournalRid(71)
		p2, q2 := pkMoveJournalRid(72), pkMoveJournalRid(73)
		proc := testutil.NewProcess(t)
		mp := proc.Mp()
		txn := &Transaction{proc: proc}
		txn.writes = []Entry{
			pkMovePruneLinkEntry(t, mp, 7,
				[]uint64{42, 42},
				[]*types.Rowid{&p1, &q1},
				[]*types.Rowid{nil, nil},
				[]uint64{700, 701}),
		}
		txn.statementID = 1
		bat := txn.writes[0].bat
		baseBat := bat.Size()
		baseBook := txn.pkMoveBookkeepBytesLocked()

		origOverwrite := pkMoveVecOverwriteRowidAt
		t.Cleanup(func() { pkMoveVecOverwriteRowidAt = origOverwrite })
		armed := false
		seamCalls := 0
		pkMoveVecOverwriteRowidAt = func(v *vector.Vector, idx int, bs []byte, m *mpool.MPool) error {
			if armed {
				seamCalls++
				if seamCalls == 2 {
					return errPkMoveInjected
				}
			}
			return origOverwrite(v, idx, bs, m)
		}

		for round := 0; round < 3; round++ {
			seamCalls = 0
			armed = true
			effOld, effNew, err := txn.pkMoveCarryJournalAdmitLocked(
				[]types.Rowid{p1, q1}, []types.Rowid{p2, q2})
			require.NoError(t, err)
			txn.pkMoveCarryJournalAppendLocked(pkMoveCarryObject, effOld, effNew)
			err = txn.carryPkMoveProvenanceThroughRekeyLocked(
				[]types.Rowid{p1, q1}, []types.Rowid{p2, q2})
			require.ErrorIs(t, err, errPkMoveInjected)
			require.Equal(t, 2, seamCalls)
			armed = false
			// The rollback unwinds the journaled exact inverse; the inverse
			// never re-admits and never trips the budget.
			require.NoError(t, txn.unwindPkMoveCarryJournalLocked(1))
			require.Equal(t, p1[:], bat.Vecs[4].GetBytesAt(0))
			require.Equal(t, q1[:], bat.Vecs[4].GetBytesAt(1))
			require.Equal(t, baseBat, bat.Size())
			require.Equal(t, baseBook, txn.pkMoveBookkeepBytesLocked())
		}

		// Healthy retry under the unchanged budget.
		effOld, effNew, err := txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{p1, q1}, []types.Rowid{p2, q2})
		require.NoError(t, err)
		txn.pkMoveCarryJournalAppendLocked(pkMoveCarryWave, effOld, effNew)
		require.NoError(t, txn.carryPkMoveProvenanceThroughRekeyLocked(
			[]types.Rowid{p1, q1}, []types.Rowid{p2, q2}))
		require.Equal(t, p2[:], bat.Vecs[4].GetBytesAt(0))
		require.Equal(t, q2[:], bat.Vecs[4].GetBytesAt(1))
		require.Equal(t, baseBat, bat.Size())
	})
}

func TestPkMoveCarryJournalBudgetRelease(t *testing.T) {
	probe := pkMoveJournalRid(90)

	t.Run("statement rollback releases the journal charge", func(t *testing.T) {
		carrier := pkMoveJournalRid(50)
		n2 := pkMoveJournalRid(51)
		txn, _ := pkMoveJournalSeedAll(t, carrier, probe)
		txn.statementID = 1
		base := txn.pkMoveBookkeepBytesLocked()
		effOld, effNew, err := txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{carrier}, []types.Rowid{n2})
		require.NoError(t, err)
		txn.pkMoveCarryJournalAppendLocked(pkMoveCarryObject, effOld, effNew)
		require.Greater(t, txn.pkMoveBookkeepBytesLocked(), base)
		require.NoError(t, txn.unwindPkMoveCarryJournalLocked(1))
		require.Equal(t, base, txn.pkMoveBookkeepBytesLocked())
	})

	t.Run("boundary adoption releases the journal charge", func(t *testing.T) {
		carrier := pkMoveJournalRid(50)
		n2 := pkMoveJournalRid(51)
		txn, _ := pkMoveJournalSeedAll(t, carrier, probe)
		txn.statementID = 1
		base := txn.pkMoveBookkeepBytesLocked()
		effOld, effNew, err := txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{carrier}, []types.Rowid{n2})
		require.NoError(t, err)
		txn.pkMoveCarryJournalAppendLocked(pkMoveCarryObject, effOld, effNew)
		require.Greater(t, txn.pkMoveBookkeepBytesLocked(), base)
		// The boundary's adoption walk drops the just-completed
		// statement's entries: their identities are beyond every later
		// rollback's reach.
		require.NoError(t, txn.settlePkMoveAdoptedLedgersLocked())
		require.Empty(t, txn.pkMoveCarryJournal)
		require.Equal(t, base, txn.pkMoveBookkeepBytesLocked())
	})
}

// pkMoveOverwriteRecord captures one retained-cell overwrite the seam
// performed, so a stepwise unwind's intermediate checkpoints are
// observable byte-exactly.
type pkMoveOverwriteRecord struct {
	row     int
	payload types.Rowid
}

// armPkMoveOverwriteRecorder swaps the retained-cell overwrite seam for
// a pass-through recorder and returns the recording slice and the
// original seam for restoration.
func armPkMoveOverwriteRecorder(
	t *testing.T,
) (recs *[]pkMoveOverwriteRecord, restore func()) {
	t.Helper()
	orig := pkMoveVecOverwriteRowidAt
	t.Cleanup(func() { pkMoveVecOverwriteRowidAt = orig })
	records := []pkMoveOverwriteRecord{}
	pkMoveVecOverwriteRowidAt = func(v *vector.Vector, idx int, bs []byte, m *mpool.MPool) error {
		var rid types.Rowid
		copy(rid[:], bs)
		records = append(records, pkMoveOverwriteRecord{row: idx, payload: rid})
		return orig(v, idx, bs, m)
	}
	return &records, func() { pkMoveVecOverwriteRowidAt = orig }
}

// armPkMoveOverwriteFailAfter returns an arm func; once armed, the
// seam's first skip calls pass through and the (skip+1)-th returns the
// injected error once, producing a deterministic PARTIAL round whose
// earlier staged-cell rewrites already landed.
func armPkMoveOverwriteFailAfter(
	t *testing.T, skip int,
) (arm func(), restore func()) {
	t.Helper()
	orig := pkMoveVecOverwriteRowidAt
	t.Cleanup(func() { pkMoveVecOverwriteRowidAt = orig })
	armed := false
	calls := 0
	pkMoveVecOverwriteRowidAt = func(v *vector.Vector, idx int, bs []byte, m *mpool.MPool) error {
		if armed {
			calls++
			if calls > skip {
				armed = false
				return errPkMoveInjected
			}
		}
		return orig(v, idx, bs, m)
	}
	return func() { armed = true }, func() { pkMoveVecOverwriteRowidAt = orig }
}

// pkMoveChainFixture seeds one 2-row staged link batch plus a probe
// origin ledger entry, so a chain's carries and inverses are observable
// on two independent staged cells and one map value.
func pkMoveChainFixture(
	t *testing.T, p1, q1, probe types.Rowid,
) *Transaction {
	t.Helper()
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	txn := &Transaction{proc: proc}
	txn.writes = []Entry{
		pkMovePruneLinkEntry(t, mp, 7,
			[]uint64{42, 42},
			[]*types.Rowid{&p1, &q1},
			[]*types.Rowid{nil, nil},
			[]uint64{700, 701}),
	}
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{probe: p1}
	txn.statementID = 1
	return txn
}

// pkMoveChainCarryRound runs one admitted+journaled+carried chain round.
func pkMoveChainCarryRound(
	t *testing.T, txn *Transaction, kind pkMoveCarryKind,
	oldIds, newIds []types.Rowid,
) {
	t.Helper()
	effOld, effNew, err := txn.pkMoveCarryJournalAdmitLocked(oldIds, newIds)
	require.NoError(t, err)
	txn.pkMoveCarryJournalAppendLocked(kind, effOld, effNew)
	require.NoError(t,
		txn.carryPkMoveProvenanceThroughRekeyLocked(oldIds, newIds))
}

// TestPkMoveCarryChainedPartialFailure is the r43 review F2 (D150)
// battery: a completed round followed by a PARTIALLY failed chained
// round must unwind stepwise — the failed round's own entry first
// (landing every partial write on the byte-exact intermediate
// checkpoint), then the completed round's entry — with the acceptance
// oracle split by statement membership (r44): the SAME statement's
// rollback restores the PRE-CHAIN state and consumes both entries; the
// cross-statement rollback restores only the failed statement's partial
// writes and preserves the prior statement's completed state.
func TestPkMoveCarryChainedPartialFailure(t *testing.T) {
	probe := pkMoveJournalRid(90)
	p1, q1 := pkMoveJournalRid(70), pkMoveJournalRid(71)
	p2, q2 := pkMoveJournalRid(72), pkMoveJournalRid(73)
	p3, q3 := pkMoveJournalRid(74), pkMoveJournalRid(75)

	seedChain := func(t *testing.T) *Transaction {
		txn := pkMoveChainFixture(t, p1, q1, probe)
		pkMoveChainCarryRound(t, txn, pkMoveCarryObject,
			[]types.Rowid{p1, q1}, []types.Rowid{p2, q2})
		require.Equal(t, p2[:], txn.writes[0].bat.Vecs[4].GetBytesAt(0))
		require.Equal(t, q2[:], txn.writes[0].bat.Vecs[4].GetBytesAt(1))
		require.Equal(t, p2, txn.pkMoveRowidOrigins[probe])
		return txn
	}

	// failPartialChain journals the chained round and then fails its
	// carry with row 0 already rewritten: the staged cells split
	// across the N and P identities.
	failPartialChain := func(t *testing.T, txn *Transaction) {
		effOld, effNew, err := txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{p2, q2}, []types.Rowid{p3, q3})
		require.NoError(t, err)
		txn.pkMoveCarryJournalAppendLocked(pkMoveCarryWave, effOld, effNew)
		arm, restore := armPkMoveOverwriteFailAfter(t, 1)
		defer restore()
		arm()
		err = txn.carryPkMoveProvenanceThroughRekeyLocked(
			[]types.Rowid{p2, q2}, []types.Rowid{p3, q3})
		require.ErrorIs(t, err, errPkMoveInjected)
		require.Equal(t, p3[:], txn.writes[0].bat.Vecs[4].GetBytesAt(0),
			"the partial failure rewrote row 0 onto the P identity")
		require.Equal(t, q2[:], txn.writes[0].bat.Vecs[4].GetBytesAt(1),
			"row 1 never reached P: the staged cells split across identities")
		require.Len(t, txn.pkMoveCarryJournal, 2,
			"the failed round journaled its own entry BEFORE mutating")
	}

	retryFailedRound := func(t *testing.T, txn *Transaction, journalLen int) {
		pkMoveChainCarryRound(t, txn, pkMoveCarryWave,
			[]types.Rowid{p2, q2}, []types.Rowid{p3, q3})
		require.Equal(t, p3[:], txn.writes[0].bat.Vecs[4].GetBytesAt(0))
		require.Equal(t, q3[:], txn.writes[0].bat.Vecs[4].GetBytesAt(1))
		require.Equal(t, p3, txn.pkMoveRowidOrigins[probe])
		require.Len(t, txn.pkMoveCarryJournal, journalLen,
			"the healthy retry journaled every round it carried")
	}

	retryWholeChain := func(t *testing.T, txn *Transaction, journalLen int) {
		pkMoveChainCarryRound(t, txn, pkMoveCarryObject,
			[]types.Rowid{p1, q1}, []types.Rowid{p2, q2})
		retryFailedRound(t, txn, journalLen)
	}

	t.Run("same statement: the unwind passes the byte-exact N checkpoint, ends at the pre-chain state, consumes both entries, and the whole-chain retry succeeds", func(t *testing.T) {
		// D150(a) with the r44 oracle: both rounds carry the rolled
		// statement's seq, so the stepwise unwind applies BOTH inverses
		// — the failed round's entry first, then the completed round's.
		// Accepting the intermediate N state as final would preserve
		// identities of the rolled-back statement and is a failure.
		txn := seedChain(t)
		failPartialChain(t, txn)

		recs, _ := armPkMoveOverwriteRecorder(t)
		require.NoError(t, txn.unwindPkMoveCarryJournalLocked(1))
		require.Equal(t, []pkMoveOverwriteRecord{
			{row: 0, payload: p2},
			{row: 0, payload: p1},
			{row: 1, payload: q1},
		}, *recs,
			"stepwise inverse stream: the partial P write lands back on the byte-exact N checkpoint, then both cells reach the pre-chain O state")
		require.Equal(t, p1[:], txn.writes[0].bat.Vecs[4].GetBytesAt(0))
		require.Equal(t, q1[:], txn.writes[0].bat.Vecs[4].GetBytesAt(1))
		require.Equal(t, p1, txn.pkMoveRowidOrigins[probe],
			"every provenance map is byte-identical to the PRE-CHAIN state")
		require.Empty(t, txn.pkMoveCarryJournal,
			"both entries of the rolled statement are consumed")

		retryWholeChain(t, txn, 2)
	})

	t.Run("cross statement: the failed statement's partial writes restore to N while the prior statement's completed round stays, and the retry succeeds", func(t *testing.T) {
		// D150(b): statement 1's completed O->N round is not rolled;
		// statement 2's chained N->P round fails mid-carry, its OWN
		// stamped entry is inverted, and the state returns byte-exactly
		// to statement 1's completed N state.
		txn := seedChain(t)
		txn.statementID = 2
		failPartialChain(t, txn)
		require.Equal(t, 2, txn.pkMoveCarryJournal[1].seq,
			"the failed round's entry carries the failing statement's seq")

		require.NoError(t, txn.unwindPkMoveCarryJournalLocked(2))
		require.Equal(t, p2[:], txn.writes[0].bat.Vecs[4].GetBytesAt(0),
			"statement 2's partial P write restored to N")
		require.Equal(t, q2[:], txn.writes[0].bat.Vecs[4].GetBytesAt(1))
		require.Equal(t, p2, txn.pkMoveRowidOrigins[probe])
		require.Len(t, txn.pkMoveCarryJournal, 1,
			"statement 1's completed entry is not rolled")
		require.Equal(t, 1, txn.pkMoveCarryJournal[0].seq)
		require.Equal(t, []types.Rowid{p1, q1}, txn.pkMoveCarryJournal[0].oldIds)
		require.Equal(t, []types.Rowid{p2, q2}, txn.pkMoveCarryJournal[0].newIds)

		retryFailedRound(t, txn, 2)
	})

	t.Run("N completed chained rounds unwind stepwise byte-exactly", func(t *testing.T) {
		// D150(c): a 3-round chain restores every intermediate identity
		// in strict reverse order before reaching the pre-chain state.
		p4, q4 := pkMoveJournalRid(76), pkMoveJournalRid(77)
		txn := seedChain(t)
		pkMoveChainCarryRound(t, txn, pkMoveCarryWave,
			[]types.Rowid{p2, q2}, []types.Rowid{p3, q3})
		pkMoveChainCarryRound(t, txn, pkMoveCarryDump,
			[]types.Rowid{p3, q3}, []types.Rowid{p4, q4})
		require.Len(t, txn.pkMoveCarryJournal, 3)
		require.Equal(t, p4[:], txn.writes[0].bat.Vecs[4].GetBytesAt(0))
		require.Equal(t, q4[:], txn.writes[0].bat.Vecs[4].GetBytesAt(1))

		recs, _ := armPkMoveOverwriteRecorder(t)
		require.NoError(t, txn.unwindPkMoveCarryJournalLocked(1))
		require.Equal(t, []pkMoveOverwriteRecord{
			{row: 0, payload: p3}, {row: 1, payload: q3},
			{row: 0, payload: p2}, {row: 1, payload: q2},
			{row: 0, payload: p1}, {row: 1, payload: q1},
		}, *recs,
			"strict reverse order walks every intermediate identity byte-exactly")
		require.Equal(t, p1[:], txn.writes[0].bat.Vecs[4].GetBytesAt(0))
		require.Equal(t, q1[:], txn.writes[0].bat.Vecs[4].GetBytesAt(1))
		require.Equal(t, p1, txn.pkMoveRowidOrigins[probe])
		require.Empty(t, txn.pkMoveCarryJournal)
	})
}

// pkMoveMarkerOnlySeed builds a transaction holding ONLY continuity
// markers (origin ledger value, null-heal target, lineage origin) and
// no staged link rows: the r22/r23 non-participant shape whose
// bookkeeping dies with the transaction.
func pkMoveMarkerOnlySeed(t *testing.T, carrier, probe types.Rowid) *Transaction {
	t.Helper()
	proc := testutil.NewProcess(t)
	txn := &Transaction{proc: proc}
	txn.pkMoveRowidOrigins = map[types.Rowid]types.Rowid{probe: carrier}
	txn.pkMoveNullHeals = map[string]pkMoveNullHeal{
		"heal-1": {rid: carrier, stmt: 1},
	}
	txn.pkMoveKeyLineage = map[pkMoveAcctTbl]map[string][]pkMoveLineageBind{
		{account: 7, table: 42}: {
			string([]byte{0xab}): {{origin: carrier, wm: 0, stmt: 1}},
		},
	}
	return txn
}

// TestPkMoveCarryAdmissionParticipation is the r43 review F1 (D151)
// battery: the carry admission applies the shared participation rule —
// with no live staged link rows the integrity checks, the effective-pair
// computation, and the journal append still run; only the budget refusal
// is skipped. With staged links the admission enforces unchanged.
func TestPkMoveCarryAdmissionParticipation(t *testing.T) {
	carrier, probe := pkMoveJournalRid(50), pkMoveJournalRid(90)
	n2 := pkMoveJournalRid(51)
	m2 := pkMoveJournalRid(52)
	roundCharge := int64(2*types.RowidSize+16) + pkMoveBookkeepEntryOverhead

	lineageVal := func(txn *Transaction) types.Rowid {
		return txn.pkMoveKeyLineage[pkMoveAcctTbl{account: 7, table: 42}][string([]byte{0xab})][0].origin
	}

	markerOnlyCarry := func(
		t *testing.T, kind pkMoveCarryKind,
	) (txn *Transaction) {
		txn = pkMoveMarkerOnlySeed(t, carrier, probe)
		require.False(t, txn.pkMoveHasLiveStagedLinksLocked())
		require.True(t, txn.holdsPkMoveProvenanceLocked(),
			"the transfer route's gate fires on marker presence alone")
		base := txn.pkMoveBookkeepBytesLocked()
		// Lowered so an enforcing admission would refuse this round.
		pinPkMoveCoordinatorBudget(t, base+roundCharge-1)
		txn.statementID = 1
		effOld, effNew, err := txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{carrier}, []types.Rowid{n2})
		require.NoError(t, err,
			"a marker-only transaction must never be refused by the provenance budget")
		require.Equal(t, []types.Rowid{carrier}, effOld)
		require.Equal(t, []types.Rowid{n2}, effNew)
		txn.pkMoveCarryJournalAppendLocked(kind, effOld, effNew)
		require.NoError(t, txn.carryPkMoveProvenanceThroughRekeyLocked(
			[]types.Rowid{carrier}, []types.Rowid{n2}))
		require.Equal(t, n2, txn.pkMoveRowidOrigins[probe])
		require.Equal(t, n2, txn.pkMoveNullHeals["heal-1"].rid)
		require.Equal(t, n2, lineageVal(txn))
		require.Len(t, txn.pkMoveCarryJournal, 1,
			"rollback journaling was preserved — only the refusal was skipped")
		return txn
	}

	t.Run("dump route: a marker-only transaction crosses the lowered budget without refusal", func(t *testing.T) {
		// D151(a): integrity, the effective-pair computation, and the
		// journal append still run; only the refusal is skipped — the
		// transaction commits no provenance and its marker bookkeeping
		// dies with it.
		markerOnlyCarry(t, pkMoveCarryDump)
	})

	t.Run("transfer route: marker presence alone reaches admission and also commits", func(t *testing.T) {
		// D151(b): holdsPkMoveProvenanceLocked is true on marker
		// presence alone, so the tombstone object-transfer round reaches
		// the same admission — it must commit under the same lowered
		// budget.
		markerOnlyCarry(t, pkMoveCarryObject)
	})

	t.Run("rollback of a later statement restores the carried maps byte-identically and the retry succeeds", func(t *testing.T) {
		// D151(c): journaling is intact under the skipped refusal, so
		// statement rollback restores the carried provenance exactly.
		txn := markerOnlyCarry(t, pkMoveCarryObject)
		txn.statementID = 2
		effOld, effNew, err := txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{n2}, []types.Rowid{m2})
		require.NoError(t, err)
		txn.pkMoveCarryJournalAppendLocked(pkMoveCarryWave, effOld, effNew)
		require.NoError(t, txn.carryPkMoveProvenanceThroughRekeyLocked(
			[]types.Rowid{n2}, []types.Rowid{m2}))
		require.Equal(t, m2, txn.pkMoveRowidOrigins[probe])
		require.Equal(t, m2, lineageVal(txn))

		require.NoError(t, txn.unwindPkMoveCarryJournalLocked(2))
		require.Equal(t, n2, txn.pkMoveRowidOrigins[probe],
			"statement 2's carry restored byte-identically to statement 1's state")
		require.Equal(t, n2, txn.pkMoveNullHeals["heal-1"].rid)
		require.Equal(t, n2, lineageVal(txn))
		require.Len(t, txn.pkMoveCarryJournal, 1,
			"statement 1's entry survives the later statement's rollback")

		// The retry round succeeds under the unchanged lowered budget.
		effOld, effNew, err = txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{n2}, []types.Rowid{m2})
		require.NoError(t, err)
		txn.pkMoveCarryJournalAppendLocked(pkMoveCarryWave, effOld, effNew)
		require.NoError(t, txn.carryPkMoveProvenanceThroughRekeyLocked(
			[]types.Rowid{n2}, []types.Rowid{m2}))
		require.Equal(t, m2, txn.pkMoveRowidOrigins[probe])
		require.Len(t, txn.pkMoveCarryJournal, 2)
	})

	t.Run("one staged link row re-enables the full-measure refusal before any mutation", func(t *testing.T) {
		// D151(d): with live staged link rows the admission enforces
		// unchanged — a statement-atomic refusal ahead of any mutation.
		txn := pkMoveMarkerOnlySeed(t, carrier, probe)
		txn.writes = []Entry{
			pkMoveJournalStagedLink(t, txn.proc.Mp(), &carrier, nil)}
		require.True(t, txn.pkMoveHasLiveStagedLinksLocked())
		base := txn.pkMoveBookkeepBytesLocked()
		pinPkMoveCoordinatorBudget(t, base+roundCharge-1)
		beforeCell := txn.writes[0].bat.Vecs[4].GetBytesAt(0)
		effOld, effNew, err := txn.pkMoveCarryJournalAdmitLocked(
			[]types.Rowid{carrier}, []types.Rowid{n2})
		require.ErrorContains(t, err, "exceeds the coordinator retention budget")
		require.Nil(t, effOld)
		require.Nil(t, effNew)
		require.Empty(t, txn.pkMoveCarryJournal)
		require.Equal(t, carrier, txn.pkMoveRowidOrigins[probe])
		require.Equal(t, beforeCell, txn.writes[0].bat.Vecs[4].GetBytesAt(0),
			"the refusal preceded every mutation")
	})
}
