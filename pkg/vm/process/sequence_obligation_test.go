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

package process

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The pass-issued range and the preserve-or-restore stamp decision are the
// load-bearing primitives of the rollback issue-once protocol (#30896).
// These tables lock the exact boundaries in both directions and domains.

// negBits stores a negative signed sequence value in its uint64 bit slot; a
// variable conversion keeps negative constants representable.
func negBits(v int64) uint64 { return uint64(v) }

func TestSequenceObligationIssuedContains(t *testing.T) {
	type row struct {
		name      string
		ob        SequenceObligation
		candidate uint64
		want      bool
	}
	rows := []row{
		// Ascending signed, pass [2..5].
		{name: "asc signed below", ob: SequenceObligation{PassStart: 2, Floor: 5}, candidate: 1, want: false},
		{name: "asc signed floor edge", ob: SequenceObligation{PassStart: 2, Floor: 5}, candidate: 5, want: true},
		{name: "asc signed pass start edge", ob: SequenceObligation{PassStart: 2, Floor: 5}, candidate: 2, want: true},
		{name: "asc signed inside", ob: SequenceObligation{PassStart: 2, Floor: 5}, candidate: 3, want: true},
		{name: "asc signed above", ob: SequenceObligation{PassStart: 2, Floor: 5}, candidate: 6, want: false},
		// Descending signed, pass [9..4] (floor is the allocation extreme).
		{name: "desc signed inside", ob: SequenceObligation{PassStart: 9, Floor: 4, Minus: true}, candidate: 6, want: true},
		{name: "desc signed floor edge", ob: SequenceObligation{PassStart: 9, Floor: 4, Minus: true}, candidate: 4, want: true},
		{name: "desc signed above floor", ob: SequenceObligation{PassStart: 9, Floor: 4, Minus: true}, candidate: 5, want: true},
		{name: "desc signed beyond floor", ob: SequenceObligation{PassStart: 9, Floor: 4, Minus: true}, candidate: 3, want: false},
		{name: "desc signed above pass", ob: SequenceObligation{PassStart: 9, Floor: 4, Minus: true}, candidate: 10, want: false},
		// Ascending unsigned.
		{name: "asc unsigned inside", ob: SequenceObligation{PassStart: 2, Floor: 5, Unsigned: true}, candidate: 4, want: true},
		{name: "asc unsigned outside", ob: SequenceObligation{PassStart: 2, Floor: 5, Unsigned: true}, candidate: 1, want: false},
		// Descending unsigned.
		{name: "desc unsigned inside", ob: SequenceObligation{PassStart: 9, Floor: 4, Minus: true, Unsigned: true}, candidate: 7, want: true},
		{name: "desc unsigned beyond floor", ob: SequenceObligation{PassStart: 9, Floor: 4, Minus: true, Unsigned: true}, candidate: 3, want: false},
		// Negative signed values round-trip through the bit slot.
		{name: "signed negatives inside", ob: SequenceObligation{PassStart: negBits(-2), Floor: negBits(-8), Minus: true}, candidate: negBits(-5), want: true},
		{name: "signed negatives beyond floor", ob: SequenceObligation{PassStart: negBits(-2), Floor: negBits(-8), Minus: true}, candidate: negBits(-9), want: false},
		{name: "signed negative vs positive", ob: SequenceObligation{PassStart: negBits(-8), Floor: negBits(-3)}, candidate: 1, want: false},
	}
	for _, r := range rows {
		require.Equal(t, r.want, r.ob.IssuedContains(r.candidate), r.name)
	}
}

// Extend is the allocation-time forward-floor invariant (K1; #30359 pre-PR
// review): an emission picked up after a statement rewind may sit BEHIND the
// recorded floor (the rewind restored an earlier workspace position while a
// pre-rewind emission was already observed). Widening must never lower the
// frontier: a candidate ahead of the floor advances it, a candidate at or
// behind the pass start pulls the start back, an interior candidate changes
// nothing, so the envelope can never collapse into protecting nothing.
func TestSequenceObligationExtend(t *testing.T) {
	type row struct {
		name      string
		ob        SequenceObligation
		bits      uint64
		wantStart uint64
		wantFloor uint64
	}
	rows := []row{
		// Ascending signed.
		{name: "asc forward advances floor", ob: SequenceObligation{PassStart: 10, Floor: 12}, bits: 15, wantStart: 10, wantFloor: 15},
		{name: "asc interior unchanged", ob: SequenceObligation{PassStart: 10, Floor: 60}, bits: 40, wantStart: 10, wantFloor: 60},
		{name: "asc floor edge unchanged", ob: SequenceObligation{PassStart: 10, Floor: 60}, bits: 60, wantStart: 10, wantFloor: 60},
		{name: "asc start edge unchanged", ob: SequenceObligation{PassStart: 10, Floor: 60}, bits: 10, wantStart: 10, wantFloor: 60},
		{name: "asc rewound candidate pulls start, floor kept", ob: SequenceObligation{PassStart: 60, Floor: 60}, bits: 10, wantStart: 10, wantFloor: 60},
		// Descending signed mirror: the frontier is the smallest value.
		{name: "desc forward advances floor", ob: SequenceObligation{PassStart: 10, Floor: 5, Minus: true}, bits: 3, wantStart: 10, wantFloor: 3},
		{name: "desc interior unchanged", ob: SequenceObligation{PassStart: 10, Floor: 2, Minus: true}, bits: 6, wantStart: 10, wantFloor: 2},
		{name: "desc rewound candidate pulls start, floor kept", ob: SequenceObligation{PassStart: 2, Floor: 2, Minus: true}, bits: 10, wantStart: 10, wantFloor: 2},
		// Unsigned and signed-negative domains order in their own domain.
		{name: "unsigned rewound candidate", ob: SequenceObligation{PassStart: 60, Floor: 60, Unsigned: true}, bits: 10, wantStart: 10, wantFloor: 60},
		{name: "signed negative rewound candidate", ob: SequenceObligation{PassStart: negBits(-5), Floor: negBits(-5)}, bits: negBits(-20), wantStart: negBits(-20), wantFloor: negBits(-5)},
		{name: "signed negative forward", ob: SequenceObligation{PassStart: negBits(-20), Floor: negBits(-8)}, bits: negBits(-3), wantStart: negBits(-20), wantFloor: negBits(-3)},
		{name: "unsigned desc rewound candidate", ob: SequenceObligation{PassStart: 2, Floor: 2, Minus: true, Unsigned: true}, bits: 10, wantStart: 10, wantFloor: 2},
	}
	for _, r := range rows {
		ob := r.ob
		ob.Extend(r.bits)
		require.Equal(t, r.wantStart, ob.PassStart, r.name)
		require.Equal(t, r.wantFloor, ob.Floor, r.name)
	}

	// The finding's end-to-end invariant at the primitive level: pending 10,
	// SETVAL+allocation observed 60, the statement failed and the workspace
	// rewound to 10. Extending with the rewound emission keeps BOTH sides
	// issued, so a later allocation hitting the envelope repairs forward
	// instead of re-issuing, and the commit stamp cannot preserve the
	// regressed position.
	ob := SequenceObligation{PassStart: 60, Floor: 60}
	ob.Extend(10)
	require.True(t, ob.IssuedContains(60), "the pre-rewind emission stays issued")
	require.True(t, ob.IssuedContains(10), "the rewound emission joins the envelope")
	require.True(t, ob.IssuedContains(35), "the envelope's interior stays protected")
	require.True(t, ob.UnsafeToPreserve(10, true),
		"COMMIT cannot persist the regressed position: the stamp restores past the floor")
	require.False(t, ob.UnsafeToPreserve(60, true), "the true frontier position is preserved")

	desc := SequenceObligation{PassStart: 2, Floor: 2, Minus: true}
	desc.Extend(10)
	require.True(t, desc.IssuedContains(2))
	require.True(t, desc.IssuedContains(10))
	require.True(t, desc.UnsafeToPreserve(10, true),
		"descending mirror: the rewound position is unsafe for the committed stamp")
}

func TestSequenceObligationUnsafeToPreserve(t *testing.T) {
	type row struct {
		name   string
		ob     SequenceObligation
		wBits  uint64
		called bool
		want   bool
	}
	rows := []row{
		// Ascending floor 5: restore behind the floor or equal-uncalled.
		{name: "asc at floor called preserves", ob: SequenceObligation{Floor: 5}, wBits: 5, called: true, want: false},
		{name: "asc equal uncalled restores", ob: SequenceObligation{Floor: 5}, wBits: 5, called: false, want: true},
		{name: "asc behind called restores", ob: SequenceObligation{Floor: 5}, wBits: 3, called: true, want: true},
		{name: "asc behind uncalled restores", ob: SequenceObligation{Floor: 5}, wBits: 4, called: false, want: true},
		{name: "asc beyond survives reconcile", ob: SequenceObligation{Floor: 5}, wBits: 8, called: true, want: false},
		{name: "asc older-pass above floor preserves", ob: SequenceObligation{Floor: 5}, wBits: 9, called: false, want: false},
		// Descending floor 4: restore above the floor or equal-uncalled.
		{name: "desc at floor called preserves", ob: SequenceObligation{Floor: 4, Minus: true}, wBits: 4, called: true, want: false},
		{name: "desc equal uncalled restores", ob: SequenceObligation{Floor: 4, Minus: true}, wBits: 4, called: false, want: true},
		{name: "desc above restores", ob: SequenceObligation{Floor: 4, Minus: true}, wBits: 9, called: true, want: true},
		{name: "desc below preserves reconcile", ob: SequenceObligation{Floor: 4, Minus: true}, wBits: 2, called: true, want: false},
		{name: "desc below uncalled preserves", ob: SequenceObligation{Floor: 4, Minus: true}, wBits: 1, called: false, want: false},
		// Unsigned domains.
		{name: "unsigned asc behind restores", ob: SequenceObligation{Floor: 5, Unsigned: true}, wBits: 4, called: true, want: true},
		{name: "unsigned desc above restores", ob: SequenceObligation{Floor: 4, Minus: true, Unsigned: true}, wBits: 9, called: true, want: true},
		{name: "signed negative at floor preserves", ob: SequenceObligation{Floor: negBits(-8)}, wBits: negBits(-8), called: true, want: false},
		{name: "signed negative behind restores", ob: SequenceObligation{Floor: negBits(-8)}, wBits: negBits(-10), called: true, want: true},
		{name: "signed negative beyond preserves", ob: SequenceObligation{Floor: negBits(-8)}, wBits: negBits(-5), called: true, want: false},
	}
	for _, r := range rows {
		require.Equal(t, r.want, r.ob.UnsafeToPreserve(r.wBits, r.called), r.name)
	}
}

func TestSequenceObligationSetOps(t *testing.T) {
	var nilSet *SequenceObligationSet
	_, ok := nilSet.Get(7)
	require.False(t, ok)
	nilSet.Remove(7) // no-op, no panic
	nilSet.Clear()   // no-op, no panic
	require.Nil(t, nilSet.Snapshot())
	// A nil store cannot record an obligation: failing loudly keeps
	// issue-once intact on processes without a session-owned store.
	require.Error(t, nilSet.Raise(SequenceObligation{RelID: 7}))

	set := NewSequenceObligationSet()
	require.NoError(t, set.Raise(SequenceObligation{RelID: 9, Floor: 3}))
	require.NoError(t, set.Raise(SequenceObligation{RelID: 4, Floor: 8}))
	// Upsert replaces the entry.
	require.NoError(t, set.Raise(SequenceObligation{RelID: 9, Floor: 5}))
	ob, ok := set.Get(9)
	require.True(t, ok)
	require.Equal(t, uint64(5), ob.Floor)

	// Snapshot is ordered by relation id for deterministic stamping.
	snap := set.Snapshot()
	require.Len(t, snap, 2)
	require.Equal(t, uint64(4), snap[0].RelID)
	require.Equal(t, uint64(9), snap[1].RelID)

	set.Remove(9)
	_, ok = set.Get(9)
	require.False(t, ok)
	require.Len(t, set.Snapshot(), 1)

	set.Clear()
	require.Empty(t, set.Snapshot())
	// The store stays usable after a terminal cleanup.
	require.NoError(t, set.Raise(SequenceObligation{RelID: 1, Floor: 1}))
	require.Len(t, set.Snapshot(), 1)
}

// A SETVAL boundary executes mid-statement: the removal must participate in
// the statement's success/rollback. Staged entries leave enforcement and the
// stamp exactly like a removal; success applies them; failure restores them,
// envelope-merging any same-statement post-SETVAL obligation so neither
// side's issued values are re-issuable.
func TestSequenceObligationStagedRemovals(t *testing.T) {
	var nilSet *SequenceObligationSet
	nilSet.StageRemoval(7) // no-op, no panic
	nilSet.ApplyStaged()   // no-op, no panic
	nilSet.RestoreStaged() // no-op, no panic

	// Success: the staged boundary applies and never comes back.
	set := NewSequenceObligationSet()
	require.NoError(t, set.Raise(SequenceObligation{RelID: 5, PassStart: 10, Floor: 12}))
	set.StageRemoval(5)
	_, ok := set.Get(5)
	require.False(t, ok, "staged entries leave enforcement")
	require.Empty(t, set.Snapshot(), "staged entries must not reach the pre-commit stamp")
	set.ApplyStaged()
	_, ok = set.Get(5)
	require.False(t, ok)

	// Failure: the staged boundary is restored when the statement rolls back.
	require.NoError(t, set.Raise(SequenceObligation{RelID: 6, PassStart: 10, Floor: 12}))
	set.StageRemoval(6)
	set.RestoreStaged()
	ob, ok := set.Get(6)
	require.True(t, ok)
	require.Equal(t, uint64(10), ob.PassStart)
	require.Equal(t, uint64(12), ob.Floor)

	// Failure after a same-statement post-SETVAL allocation: the restored
	// boundary and the raised obligation envelope-merge (ascending), because
	// both sides' values were emitted while the statement ran.
	require.NoError(t, set.Raise(SequenceObligation{RelID: 7, PassStart: 10, Floor: 12}))
	set.StageRemoval(7)
	require.NoError(t, set.Raise(SequenceObligation{RelID: 7, PassStart: 60, Floor: 63}))
	set.RestoreStaged()
	ob, ok = set.Get(7)
	require.True(t, ok)
	require.Equal(t, uint64(10), ob.PassStart)
	require.Equal(t, uint64(63), ob.Floor)
	require.True(t, ob.IssuedContains(11), "pre-SETVAL side stays protected")
	require.True(t, ob.IssuedContains(62), "post-SETVAL side stays protected")
	require.True(t, ob.IssuedContains(40), "envelope gaps are deliberate, never re-issued")

	// A second staged boundary for the same identity envelope-merges with
	// the first instead of losing the earlier range (descending mirror).
	require.NoError(t, set.Raise(SequenceObligation{RelID: 8, PassStart: 90, Floor: 80, Minus: true}))
	set.StageRemoval(8)
	require.NoError(t, set.Raise(SequenceObligation{RelID: 8, PassStart: 50, Floor: 40, Minus: true}))
	set.StageRemoval(8)
	require.NoError(t, set.Raise(SequenceObligation{RelID: 8, PassStart: 30, Floor: 25, Minus: true}))
	set.RestoreStaged()
	ob, ok = set.Get(8)
	require.True(t, ok)
	require.Equal(t, uint64(90), ob.PassStart)
	require.Equal(t, uint64(25), ob.Floor)
	require.True(t, ob.IssuedContains(85))
	require.True(t, ob.IssuedContains(45))
	require.True(t, ob.IssuedContains(28))

	// Terminal cleanup drops staged entries with live ones.
	set.StageRemoval(8)
	set.Clear()
	require.Empty(t, set.Snapshot())
	ob, ok = set.Get(8)
	require.False(t, ok)
}

// Signed-domain envelope oracles (#30359 review): an envelope merge must
// compare in the identity's numeric domain, exactly like IssuedContains.
// With raw uint64 bit comparison, restoring a failed boundary between
// signed issued ranges [-10,-10] and [60,60] collapsed to PassStart=60,
// Floor=-10 — an envelope protecting neither side, so a workspace rewind to
// (-10,false) let the next allocation re-emit the already-issued -10.
func TestSequenceObligationEnvelopeSignedDomain(t *testing.T) {
	// Ascending across zero: staged pre-SETVAL range [-10,-10] merges with
	// the same-statement post-SETVAL range [60,60] on restore.
	set := NewSequenceObligationSet()
	require.NoError(t, set.Raise(SequenceObligation{RelID: 11, PassStart: negBits(-10), Floor: negBits(-10)}))
	set.StageRemoval(11) // SETVAL starts its boundary mid-statement
	require.NoError(t, set.Raise(SequenceObligation{RelID: 11, PassStart: 60, Floor: 60}))
	set.RestoreStaged() // the SETVAL statement failed; the boundary never persisted
	ob, ok := set.Get(11)
	require.True(t, ok)
	require.Equal(t, negBits(-10), ob.PassStart)
	require.Equal(t, uint64(60), ob.Floor)
	require.False(t, ob.Unsigned)
	require.False(t, ob.Minus)
	require.True(t, ob.IssuedContains(negBits(-10)), "pre-SETVAL side stays protected")
	require.True(t, ob.IssuedContains(60), "post-SETVAL side stays protected")
	require.True(t, ob.IssuedContains(0), "the envelope spans zero")
	require.True(t, ob.IssuedContains(30), "interior gaps are deliberate, never re-issued")
	require.False(t, ob.IssuedContains(negBits(-11)), "just before the envelope")
	require.False(t, ob.IssuedContains(61), "just past the envelope")
	// The rewound workspace candidate is caught: after a rewind to
	// (-10,false) the next allocation sees -10 as issued and repairs forward.
	require.True(t, ob.IssuedContains(negBits(-10)), "rewound -10 must read as issued")
	require.True(t, ob.UnsafeToPreserve(negBits(-10), false), "the rewound state sits behind the floor and is unsafe to preserve")

	// Descending mirror: staged [60,60] merges with same-statement [-10,-10].
	set = NewSequenceObligationSet()
	require.NoError(t, set.Raise(SequenceObligation{RelID: 12, PassStart: 60, Floor: 60, Minus: true}))
	set.StageRemoval(12)
	require.NoError(t, set.Raise(SequenceObligation{RelID: 12, PassStart: negBits(-10), Floor: negBits(-10), Minus: true}))
	set.RestoreStaged()
	ob, ok = set.Get(12)
	require.True(t, ok)
	require.Equal(t, uint64(60), ob.PassStart, "descending pass-start is the highest issued value")
	require.Equal(t, negBits(-10), ob.Floor, "descending floor is the lowest issued value")
	require.True(t, ob.Minus)
	require.True(t, ob.IssuedContains(60))
	require.True(t, ob.IssuedContains(negBits(-10)))
	require.True(t, ob.IssuedContains(0), "the envelope spans zero")
	require.True(t, ob.IssuedContains(negBits(-3)))
	require.False(t, ob.IssuedContains(61))
	require.False(t, ob.IssuedContains(negBits(-11)))

	// Fully negative ascending range across a second negative range.
	set = NewSequenceObligationSet()
	require.NoError(t, set.Raise(SequenceObligation{RelID: 13, PassStart: negBits(-30), Floor: negBits(-25)}))
	set.StageRemoval(13)
	require.NoError(t, set.Raise(SequenceObligation{RelID: 13, PassStart: negBits(-12), Floor: negBits(-8)}))
	set.RestoreStaged()
	ob, ok = set.Get(13)
	require.True(t, ok)
	require.Equal(t, negBits(-30), ob.PassStart)
	require.Equal(t, negBits(-8), ob.Floor)
	require.True(t, ob.IssuedContains(negBits(-27)))
	require.True(t, ob.IssuedContains(negBits(-10)))
	require.False(t, ob.IssuedContains(negBits(-31)))
	require.False(t, ob.IssuedContains(negBits(-7)))
	require.False(t, ob.IssuedContains(0), "zero lies outside the fully negative envelope")

	// Descending fully negative mirror.
	set = NewSequenceObligationSet()
	require.NoError(t, set.Raise(SequenceObligation{RelID: 14, PassStart: negBits(-8), Floor: negBits(-12), Minus: true}))
	set.StageRemoval(14)
	require.NoError(t, set.Raise(SequenceObligation{RelID: 14, PassStart: negBits(-25), Floor: negBits(-30), Minus: true}))
	set.RestoreStaged()
	ob, ok = set.Get(14)
	require.True(t, ok)
	require.Equal(t, negBits(-8), ob.PassStart)
	require.Equal(t, negBits(-30), ob.Floor)
	require.True(t, ob.IssuedContains(negBits(-10)))
	require.True(t, ob.IssuedContains(negBits(-27)))
	require.False(t, ob.IssuedContains(negBits(-7)))
	require.False(t, ob.IssuedContains(negBits(-31)))

	// Unsigned domain keeps plain uint64 order (a second staged boundary of
	// the same identity envelope-merges with the first).
	set = NewSequenceObligationSet()
	require.NoError(t, set.Raise(SequenceObligation{RelID: 15, PassStart: 40, Floor: 45, Unsigned: true}))
	set.StageRemoval(15)
	require.NoError(t, set.Raise(SequenceObligation{RelID: 15, PassStart: 5, Floor: 9, Unsigned: true}))
	set.StageRemoval(15)
	set.RestoreStaged()
	ob, ok = set.Get(15)
	require.True(t, ok)
	require.Equal(t, uint64(5), ob.PassStart)
	require.Equal(t, uint64(45), ob.Floor)
	require.True(t, ob.IssuedContains(7))
	require.True(t, ob.IssuedContains(42))
	require.False(t, ob.IssuedContains(4))
	require.False(t, ob.IssuedContains(46))
}
