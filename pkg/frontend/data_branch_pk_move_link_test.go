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

package frontend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	ants "github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"

	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
)

// pkMoveWalkTestLink fabricates one link row with a recorded identity:
// keys are single raw bytes (matching the encoded-key form the maps
// use), the row ids are byte-sequence row ids, and writeTS is the moving
// transaction's snapshot timestamp.
func pkMoveWalkTestLink(oldKey byte, oldRidSeq byte, newKey byte, newRidSeq byte, writeTS uint64) pkMoveLink {
	var oldRid, newRid types.Rowid
	oldRid[0] = oldRidSeq
	newRid[0] = newRidSeq
	return pkMoveLink{
		oldRowID:    oldRid,
		newRowID:    newRid,
		hasNewRowID: true,
		oldPK:       []byte{oldKey},
		newPK:       []byte{newKey},
		writeTS:     types.BuildTS(int64(writeTS), 0),
	}
}

// pkMoveWalkTestScope rebuilds the transaction scope the D156 capture
// index binds for a fabricated link family (no durable txn id): the
// create timestamp is the whole identity.
func pkMoveWalkTestScope(writeTS uint64) pkMoveTxnScope {
	return pkMoveTxnScope{writeTS: types.BuildTS(int64(writeTS), 0)}
}

// pkMoveWalkFeedCommits attaches a unit-test capture feed to a walk
// provider: one fabricated window (marked fully scanned so the arrival
// completeness gate stays open) whose streamLinkCommits records the
// given scope->commit pairs through the same admission fold the engine
// path uses (r50 (B); the admission filter still restricts to scopes
// the provider's own link cache carries).
func pkMoveWalkFeedCommits(p *pkMoveLinkProvider, feeds ...any) {
	p.windows = []pkMoveLinkTableWindow{{tableID: 1}}
	p.fullWindows = map[uint64][2]uint64{1: {}}
	p.streamLinkCommits = func(
		ctx context.Context, w pkMoveLinkTableWindow, from, end types.TS,
		accept func(scope pkMoveTxnScope, cts types.TS) error,
	) error {
		for i := 0; i+1 < len(feeds); i += 2 {
			scope, ok1 := feeds[i].(pkMoveTxnScope)
			cts, ok2 := feeds[i+1].(types.TS)
			if !ok1 || !ok2 {
				return moerr.NewInternalErrorNoCtx("malformed capture feed")
			}
			if err := accept(scope, cts); err != nil {
				return err
			}
		}
		return nil
	}
}

// pkMoveWalkTestProvider loads fabricated links into a provider without any
// SQL access: the provider carries no table windows, so every key counts as
// fully probed and successorOf never issues SQL.
func pkMoveWalkTestProvider(links ...pkMoveLink) *pkMoveLinkProvider {
	p := &pkMoveLinkProvider{
		byRow:    make(map[string]pkMoveLink),
		byPK:     make(map[string][]pkMoveLink),
		probed:   make(map[string]struct{}),
		poisoned: make(map[string]struct{}),
	}
	for _, l := range links {
		p.byRow[pkMoveLinkRowKey(l.oldRowID, l.oldPK)] = l
		p.byPK[string(l.oldPK)] = append(p.byPK[string(l.oldPK)], l)
	}
	return p
}

// pkMoveWalkTestView stubs pkMoveStreamView over fabricated rows. Identity
// evidence (row id + commit ts) rides the parallel maps keyed by the row
// bytes, so tests control exactly what the committed stream claims about
// each row; tombs holds the decoded tombstone references the production
// index would carry, and departed maps popped row ids to captured births.
type pkMoveWalkTestView struct {
	live     map[string][][]byte
	rowIDs   map[string]types.Rowid
	births   map[string]types.TS
	tombs    map[string][]pkMoveTombRef
	departed map[pkMoveDepartKey]types.TS
	// tombCache indexes one key's tombstone history once per view (r55
	// review F2); builds and scans feed the chain-walk scaling
	// regression's operation-count assertions.
	tombCache pkMoveTombIndexCache
	// pairCache computes one key's pairing ambiguity once per evidence
	// generation and serves each walk's pairing from the departed-birth
	// index (r56 review F2); builds and scans feed the scaling
	// regression's operation-count assertions.
	pairCache pkMovePairingIndexCache
}

func (v *pkMoveWalkTestView) liveRowsAt(key []byte) ([][]byte, error) {
	return v.live[string(key)], nil
}

func (v *pkMoveWalkTestView) rowIDAndBirth(row []byte) (types.Rowid, types.TS, bool) {
	rid, ok1 := v.rowIDs[string(row)]
	birth, ok2 := v.births[string(row)]
	return rid, birth, ok1 && ok2
}

func (v *pkMoveWalkTestView) tombstonesAt(key []byte) []pkMoveTombRef {
	return v.tombs[string(key)]
}

func (v *pkMoveWalkTestView) departedBirth(rowID types.Rowid, cts types.TS) (types.TS, bool) {
	birth, ok := v.departed[pkMoveDepartKey{rowID: rowID, cts: cts}]
	return birth, ok
}

func (v *pkMoveWalkTestView) deathRefsOf(key []byte, rowID types.Rowid) []pkMoveTombRef {
	idx := v.tombCache.indexFor(key, v.tombstonesAt(key), v.departedBirth)
	return v.tombCache.deathRefsOf(idx, rowID)
}

func (v *pkMoveWalkTestView) prevDeath(
	key []byte, excludeRowID types.Rowid, before types.TS,
) (pkMoveTombRef, bool, bool) {
	idx := v.tombCache.indexFor(key, v.tombstonesAt(key), v.departedBirth)
	return v.tombCache.prevDeath(idx, excludeRowID, before)
}

func (v *pkMoveWalkTestView) bornDiedRefs(key []byte, birthCTS types.TS) []pkMoveTombRef {
	idx := v.tombCache.indexFor(key, v.tombstonesAt(key), v.departedBirth)
	return v.tombCache.bornDiedRefs(idx, birthCTS)
}

func (v *pkMoveWalkTestView) pairingAt(
	key []byte, chainTS types.TS,
	succOf func(types.Rowid) (pkMoveLink, int, bool, error),
	linksLen func() int,
) pkMovePairing {
	return v.pairCache.pairingFor(
		key, v.tombstonesAt(key), v.departedBirth, succOf, linksLen, chainTS)
}

// tombEvidenceStats exposes the per-view tombstone-index instrumentation
// (builds, scanned references) for the chain-walk scaling regression.
func (v *pkMoveWalkTestView) tombEvidenceStats() (builds, scans uint64) {
	return v.tombCache.builds, v.tombCache.scans
}

// pairingEvidenceStats exposes the per-view pairing-index instrumentation
// (builds, scanned references) for the chain-walk scaling regression.
func (v *pkMoveWalkTestView) pairingEvidenceStats() (builds, scans uint64) {
	return v.pairCache.pairingEvidenceStats()
}

func pkMoveWalkTestStubView(entries ...pkMoveWalkTestRow) *pkMoveWalkTestView {
	v := &pkMoveWalkTestView{
		live:     make(map[string][][]byte),
		rowIDs:   make(map[string]types.Rowid),
		births:   make(map[string]types.TS),
		tombs:    make(map[string][]pkMoveTombRef),
		departed: make(map[pkMoveDepartKey]types.TS),
	}
	for _, e := range entries {
		switch {
		case e.departedRow:
			v.departed[pkMoveDepartKey{rowID: e.rowID, cts: e.death}] = e.birth
		case e.tombRow:
			v.tombs[string(e.key)] = append(v.tombs[string(e.key)],
				pkMoveTombRef{rowID: e.rowID, cts: e.birth})
		default:
			v.live[string(e.key)] = append(v.live[string(e.key)], e.row)
			v.rowIDs[string(e.row)] = e.rowID
			v.births[string(e.row)] = e.birth
		}
	}
	return v
}

// pkMoveWalkTestRow describes one fabricated stream entry: a live data row,
// a tombstone row, or a departed-version birth record. For a departed row
// birth is the version's birth and death the tombstone commit instant that
// deleted it, pairing exactly as capturePkMoveStreamEvidence keys it.
type pkMoveWalkTestRow struct {
	key         []byte
	row         []byte
	rowID       types.Rowid
	birth       types.TS
	death       types.TS
	tombRow     bool
	departedRow bool
}

func pkMoveLiveRow(key byte, rowByte byte, ridSeq byte, birth uint64) pkMoveWalkTestRow {
	var rid types.Rowid
	rid[0] = ridSeq
	return pkMoveWalkTestRow{
		key:   []byte{key},
		row:   []byte{rowByte},
		rowID: rid,
		birth: types.BuildTS(int64(birth), 0),
	}
}

// pkMoveTombRow fabricates an in-window tombstone under key: rowByte tags
// the row bytes, ridSeq the deleted row id, cts its commit timestamp.
func pkMoveTombRow(key byte, rowByte byte, ridSeq byte, cts uint64) pkMoveWalkTestRow {
	var rid types.Rowid
	rid[0] = ridSeq
	return pkMoveWalkTestRow{
		key:     []byte{key},
		row:     []byte{rowByte},
		rowID:   rid,
		birth:   types.BuildTS(int64(cts), 0),
		tombRow: true,
	}
}

// pkMoveDepartRow records the birth a born-died popped version carried,
// deleted by the tombstone committing at cts: the exact pairing
// capturePkMoveStreamEvidence preserves for the walk.
func pkMoveDepartRow(ridSeq byte, cts uint64, birth uint64) pkMoveWalkTestRow {
	var rid types.Rowid
	rid[0] = ridSeq
	return pkMoveWalkTestRow{
		rowID:       rid,
		birth:       types.BuildTS(int64(birth), 0),
		death:       types.BuildTS(int64(cts), 0),
		departedRow: true,
	}
}

// pkMoveWorkspaceRid fabricates the pre-commit identity a workspace-staged
// row carries: TxnWorkspaceSegment with the given block/row offsets.
func pkMoveWorkspaceRid(blkSeq byte, rowOff uint32) types.Rowid {
	var rid types.Rowid
	rid.SetSegment(colexec.TxnWorkspaceSegment)
	rid.SetBlkOffset(uint16(blkSeq))
	rid.SetRowOffset(rowOff)
	return rid
}

func mustRowID(t *testing.T, first byte) types.Rowid {
	t.Helper()
	var rid types.Rowid
	rid[0] = first
	return rid
}

// mustWorkspaceRowID fabricates a transaction-local identity: its segment
// is the fixed workspace segment every transaction allocates from, so raw
// byte equality with another test rowid made the same way reproduces the
// cross-transaction byte collision the successor lookup must namespace.
func mustWorkspaceRowID(t *testing.T, seq byte) types.Rowid {
	t.Helper()
	var rid types.Rowid
	copy(rid[0:16], colexec.TxnWorkspaceSegment[:])
	rid[types.RowidSize-1] = seq
	return rid
}

// pkMoveTestKey encodes one int64 pk exactly the way the link writer and
// the branch hashmap encode keys (types.Packer via vector.EncodeRowToPacker).
func pkMoveTestKey(t *testing.T, v int64) []byte {
	t.Helper()
	packer := types.NewPacker()
	defer packer.Close()
	packer.EncodeInt64(v)
	return append([]byte(nil), packer.Bytes()...)
}

func TestPkMoveLinkRowKeySplit(t *testing.T) {
	var ridA, ridB types.Rowid
	ridA[0] = 1
	ridB[types.RowidSize-1] = 1

	// The fixed-width rowid prefix keeps (rowid, oldPK) pairs whose
	// concatenations would otherwise collide distinct.
	require.NotEqual(t,
		pkMoveLinkRowKey(ridA, []byte{1, 2}),
		pkMoveLinkRowKey(ridB, []byte{1}),
	)
	require.Equal(t,
		pkMoveLinkRowKey(ridA, []byte{7}),
		pkMoveLinkRowKey(ridA, []byte{7}),
	)
}

func TestPkMoveClaimedKeys(t *testing.T) {
	var nilSet *pkMoveClaimedKeys
	nilSet.add([]byte{1})
	require.False(t, nilSet.has([]byte{1}))

	s := newPkMoveClaimedKeys()
	require.False(t, s.has([]byte{3}))

	key := []byte{3, 4}
	s.add(key)
	// The set copies: mutating the caller's slice must not disturb it.
	key[0] = 9
	require.True(t, s.has([]byte{3, 4}))
	require.False(t, s.has([]byte{9, 4}))
}

func TestResolvePkMoveChain(t *testing.T) {
	t.Run("single hop claims the stream row born with the anchor transaction", func(t *testing.T) {
		// The link's recorded replacement identity (rid 9) is the
		// workspace-stage attribution; the committed row carries rid 42.
		// The commit-timestamp tie (both at 15) is the binding evidence.
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 9, 10))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveLiveRow(3, 0xaa, 42, 15),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{3}, finalPK)
		require.Equal(t, []byte{0xaa}, row)
	})

	t.Run("workspace-harvested identity folds by the timestamp tie", func(t *testing.T) {
		// The live regression this suite guards: the harvest recorded the
		// replacement's pre-commit (TxnWorkspaceSegment) row id, so no
		// committed stream row ever carries it.
		link := pkMoveWalkTestLink(2, 1, 3, 9, 10)
		link.newRowID = pkMoveWorkspaceRid(1, 0)
		p := pkMoveWalkTestProvider(link)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveLiveRow(3, 0xaa, 42, 15),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{3}, finalPK)
		require.Equal(t, []byte{0xaa}, row)
	})

	t.Run("mixed-version link without recorded identity still folds through the timestamp tie", func(t *testing.T) {
		// Both durable write routes (direct workspace writes and the S3
		// flush replay) stage the owning insert in the workspace and get
		// the back-filled identity. A NULL new_row_id survives only from
		// a writer that could not attribute the replacement: an old
		// binary's link, a drifted schema, or a disabled link ctx. The
		// walk still proves the move, and the single landed generation is
		// identified by the commit tie.
		link := pkMoveWalkTestLink(2, 1, 3, 9, 10)
		link.hasNewRowID = false
		p := pkMoveWalkTestProvider(link)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveLiveRow(3, 0xaa, 42, 15),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{3}, finalPK)
		require.Equal(t, []byte{0xaa}, row)
	})

	t.Run("multi hop follows cross-transaction continuations", func(t *testing.T) {
		// 2->3 commits at 15, 3->4 commits at 25. The landed version at 3
		// was born at 15, departed at 25, and its successor names the
		// departed row id exactly.
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 1, 3, 7, 10),
			pkMoveWalkTestLink(3, 7, 4, 5, 20),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 25),
			pkMoveDepartRow(7, 25, 15),
			pkMoveLiveRow(4, 0xbb, 5, 25),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{4}, finalPK)
		require.Equal(t, []byte{0xbb}, row)
	})

	t.Run("occupant at a vacated key plus real continuation resolves past it", func(t *testing.T) {
		// Canonical review regression: 2->3->4 happens, then an
		// independent insert reuses key 3. The impostor's birth (41) ties
		// to no chain timestamp, so the walk follows the continuation and
		// claims the row at 4; the impostor keeps its plain insert
		// meaning.
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 1, 3, 7, 10),
			pkMoveWalkTestLink(3, 7, 4, 5, 20),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 25),
			pkMoveDepartRow(7, 25, 15),
			pkMoveLiveRow(3, 0xcc, 8, 41),
			pkMoveLiveRow(4, 0xbb, 5, 25),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{4}, finalPK)
		require.Equal(t, []byte{0xbb}, row)
	})

	t.Run("occupant at the new key without continuation never folds", func(t *testing.T) {
		// UPDATE 2->3; DELETE the moved row; INSERT (3,'unrelated'). The
		// lineage's own departure has no link, and the surviving row at 3
		// was born elsewhere: folding here was the bug that re-parented
		// foreign-key children to an unrelated row.
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 10))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 20),
			pkMoveDepartRow(7, 20, 15),
			pkMoveLiveRow(3, 0xcc, 8, 41),
		)
		_, _, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("same-instant tombstone at the claim key fails closed without recorded identity", func(t *testing.T) {
		// S3-route counterpart of the in-transaction prune: one
		// transaction moves 2->3, deletes the replacement, and inserts an
		// unrelated row at 3. The stream shows a live row at 3 born at the
		// chain's own commit timestamp next to a same-key tombstone whose
		// departed version was born-and-died at the SAME instant. The S3
		// route records NULL replacement identity, so the commit-time
		// prune cannot see the in-transaction death, and the bare
		// timestamp tie cannot tell the lineage's landed version from the
		// reincarnation: fail closed.
		link := pkMoveWalkTestLink(2, 1, 3, 9, 10)
		link.hasNewRowID = false
		p := pkMoveWalkTestProvider(link)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 15),
			pkMoveDepartRow(7, 15, 15),
			pkMoveLiveRow(3, 0xcc, 8, 15),
		)
		_, _, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("same-instant tombstone at the claim key fails closed with recorded identity", func(t *testing.T) {
		// The ambiguity is in the stream, not in the link: a link written
		// before the commit-time prune existed carries a usable
		// identity, yet the reincarnation shares the lineage's commit
		// timestamp. The guard must not trust identity in this shape.
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 9, 10))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 15),
			pkMoveDepartRow(7, 15, 15),
			pkMoveLiveRow(3, 0xcc, 8, 15),
		)
		_, _, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("same-instant tombstone deleting an earlier-born version still folds", func(t *testing.T) {
		// The merge apply's own footprint: one apply transaction deletes
		// the contested key's old occupant (born at 12) and lands the
		// moved row at 3 (born at 15). The same-instant victim tombstone
		// is an accounted-for removal of an older generation, so a later
		// diff walking this side must still fold (merge-twice idempotency
		// rides this path).
		link := pkMoveWalkTestLink(2, 1, 3, 9, 10)
		link.hasNewRowID = false
		p := pkMoveWalkTestProvider(link)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 15),
			pkMoveDepartRow(7, 15, 12),
			pkMoveLiveRow(3, 0xaa, 42, 15),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{3}, finalPK)
		require.Equal(t, []byte{0xaa}, row)
	})

	t.Run("an earlier tombstone at the claim key still folds", func(t *testing.T) {
		// The source deleted key 3's previous occupant in an EARLIER
		// transaction before moving 2->3: the victim's tombstone predates
		// the chain's birth instant and must not trip the same-instant
		// ambiguity guard (the E3/E4 victim matrices ride this path).
		link := pkMoveWalkTestLink(2, 1, 3, 9, 10)
		link.hasNewRowID = false
		p := pkMoveWalkTestProvider(link)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 12),
			pkMoveDepartRow(7, 12, 12),
			pkMoveLiveRow(3, 0xaa, 42, 15),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{3}, finalPK)
		require.Equal(t, []byte{0xaa}, row)
	})

	t.Run("successor links of other rows are never followed", func(t *testing.T) {
		// The occupant of 3 itself moved 3->4, but its departed version
		// was born at 18, not at the chain's 15: the occupant's link must
		// not pull the walk onto the foreign chain, which is exactly the
		// re-parenting bug the departed-birth check exists to block.
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 1, 3, 7, 10),
			pkMoveWalkTestLink(3, 8, 4, 5, 20),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 20),
			pkMoveDepartRow(7, 20, 15),
			pkMoveTombRow(3, 0x03, 8, 25),
			pkMoveDepartRow(8, 25, 18),
			pkMoveLiveRow(4, 0xbb, 5, 25),
		)
		_, _, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("multi-hop lineage terminated by a plain delete keeps delete semantics", func(t *testing.T) {
		// 2->3->4 happened and an impostor reuses 3, and the row at 4
		// was itself deleted without provenance: the complete stream
		// proves the lineage's TERMINAL deletion at key 4, so the
		// anchor's own DELETE stands (the impostor born at 41 replays as
		// an independent insert).
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 1, 3, 7, 10),
			pkMoveWalkTestLink(3, 7, 4, 5, 20),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 25),
			pkMoveDepartRow(7, 25, 15),
			pkMoveTombRow(4, 0x04, 5, 30),
			pkMoveDepartRow(5, 30, 25),
			pkMoveLiveRow(3, 0xcc, 8, 41),
		)
		_, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 1))
		require.Equal(t, pkMoveProvenDeleted, verdict)
	})

	t.Run("same-transaction chain resolves through workspace identities", func(t *testing.T) {
		// UPDATE 2->3; UPDATE 3->4 inside one transaction: the
		// intermediate version leaves no stream row, and the continuation
		// chains off the previous link's workspace-stage new_row_id under
		// one shared snapshot. The final version is born at the anchor's
		// commit timestamp.
		w1 := pkMoveWorkspaceRid(1, 0)
		w2 := pkMoveWorkspaceRid(2, 0)
		l1 := pkMoveWalkTestLink(2, 1, 3, 7, 10)
		l1.newRowID = w1
		l2 := pkMoveWalkTestLink(3, 7, 4, 5, 10)
		l2.oldRowID = w1
		l2.newRowID = w2
		p := pkMoveWalkTestProvider(l1, l2)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveLiveRow(4, 0xbb, 5, 15),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{4}, finalPK)
		require.Equal(t, []byte{0xbb}, row)
	})

	t.Run("same-transaction intruder at a vacated intermediate key does not end the chain", func(t *testing.T) {
		// UPDATE 2->3; UPDATE 3->4; INSERT (3,'unrelated') inside one
		// transaction: the intruder and the lineage's final version share
		// the anchor's commit timestamp, and the vacated intermediate
		// generation leaves no committed tombstone. A bare timestamp
		// claim at key 3 would claim the intruder as the lineage end;
		// the recorded workspace successor of the incoming link names the
		// true continuation and must be followed first.
		w1 := pkMoveWorkspaceRid(1, 0)
		w2 := pkMoveWorkspaceRid(2, 0)
		l1 := pkMoveWalkTestLink(2, 1, 3, 7, 10)
		l1.newRowID = w1
		l2 := pkMoveWalkTestLink(3, 7, 4, 5, 10)
		l2.oldRowID = w1
		l2.newRowID = w2
		p := pkMoveWalkTestProvider(l1, l2)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveLiveRow(3, 0xdd, 8, 15), // the intruder, born at the same instant
			pkMoveLiveRow(4, 0xbb, 5, 15),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{4}, finalPK)
		require.Equal(t, []byte{0xbb}, row)
	})

	t.Run("same-transaction chain requires the shared snapshot", func(t *testing.T) {
		// A continuation carrying a different snapshot cannot share the
		// netted row identity with the anchor's transaction.
		w1 := pkMoveWorkspaceRid(1, 0)
		w2 := pkMoveWorkspaceRid(2, 0)
		l1 := pkMoveWalkTestLink(2, 1, 3, 7, 10)
		l1.newRowID = w1
		l2 := pkMoveWalkTestLink(3, 7, 4, 5, 11)
		l2.oldRowID = w1
		l2.newRowID = w2
		p := pkMoveWalkTestProvider(l1, l2)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveLiveRow(4, 0xbb, 5, 15),
		)
		_, _, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("null-identity chain departs the impostor key by recorded provenance", func(t *testing.T) {
		// S3-route counterpart of the workspace intruder test: UPDATE
		// 2->3; UPDATE 3->4; INSERT (3,'impostor') in one transaction,
		// both moves flushing straight to uncommitted objects so neither
		// link carries a replacement identity. A bare timestamp claim at
		// key 3 would end the lineage on the intruder; the recorded
		// same-transaction departure from key 3 is the commit-prune's own
		// chain continuation and must be followed instead.
		l1 := pkMoveWalkTestLink(2, 1, 3, 7, 10)
		l1.hasNewRowID = false
		l2 := pkMoveWalkTestLink(3, 7, 4, 5, 10)
		l2.hasNewRowID = false
		p := pkMoveWalkTestProvider(l1, l2)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveLiveRow(3, 0xdd, 8, 15), // the intruder, born at the same instant
			pkMoveLiveRow(4, 0xbb, 5, 15),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{4}, finalPK)
		require.Equal(t, []byte{0xbb}, row)
	})

	t.Run("same-key departure of another transaction does not redirect the claim", func(t *testing.T) {
		// Control: the lineage truly ends at key 3 (claim born with the
		// anchor transaction) while an UNRELATED later transaction moved
		// its own version under the same key. That foreign link must not
		// be followed; the timestamp claim still wins.
		l1 := pkMoveWalkTestLink(2, 1, 3, 7, 10)
		l1.hasNewRowID = false
		l2 := pkMoveWalkTestLink(3, 9, 4, 5, 20) // a different transaction, namespaced by write_ts
		l2.hasNewRowID = false
		p := pkMoveWalkTestProvider(l1, l2)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveLiveRow(3, 0xdd, 8, 15),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{3}, finalPK)
		require.Equal(t, []byte{0xdd}, row)
	})

	t.Run("two same-transaction departures from the claim key fail closed", func(t *testing.T) {
		// Two versions of key 3 moved inside one transaction: the walk
		// cannot pick a continuation by key alone and must refuse.
		l1 := pkMoveWalkTestLink(2, 1, 3, 7, 10)
		l1.hasNewRowID = false
		l2 := pkMoveWalkTestLink(3, 7, 4, 5, 10)
		l2.hasNewRowID = false
		l3 := pkMoveWalkTestLink(3, 9, 6, 6, 10)
		l3.hasNewRowID = false
		p := pkMoveWalkTestProvider(l1, l2, l3)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveLiveRow(3, 0xdd, 8, 15),
			pkMoveLiveRow(4, 0xbb, 5, 15),
		)
		_, _, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("same-transaction netted hop resolves through the recorded departure", func(t *testing.T) {
		// 2->3 (S3 persist path: NULL new_row_id), then 3->4, in one
		// transaction committed at 15. The intermediate at 3 was born
		// and deleted inside the transaction, so the committed stream
		// carries neither its row nor its tombstone. Era-5 refused the
		// hop because a NULL identity could not prove the departure
		// continued THIS lineage; the commit-time prune already owns
		// exactly that same-transaction grouping (its continuer rescue
		// keeps the NULL link alive only while an alive departure at the
		// new key exists), so the consumer now mirrors it and follows
		// the recorded departure key-wise. Netted-fiction shapes
		// ("UPDATE 2->3; UPDATE 3->4" vs "UPDATE 2->3; silent lineage
		// death; UPDATE impostor 3->4") produce identical committed
		// streams, and folding applies the committed final image, which
		// reproduces the source final state in either reading.
		l1 := pkMoveWalkTestLink(2, 1, 3, 7, 10)
		l1.hasNewRowID = false
		w1 := pkMoveWorkspaceRid(1, 0)
		l2 := pkMoveWalkTestLink(3, 7, 4, 5, 10)
		l2.oldRowID = w1
		p := pkMoveWalkTestProvider(l1, l2)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveLiveRow(4, 0xbb, 5, 15),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{4}, finalPK)
		require.Equal(t, []byte{0xbb}, row)
	})

	t.Run("same-transaction continuation leaves a committed mid generation", func(t *testing.T) {
		// Mixed chain: 2->3->4 in one transaction (committed at 15, both
		// links workspace-identities of snapshot 10), then 4->5 in a
		// later transaction committed at 25.
		w1 := pkMoveWorkspaceRid(1, 0)
		w2 := pkMoveWorkspaceRid(2, 0)
		l1 := pkMoveWalkTestLink(2, 1, 3, 7, 10)
		l1.newRowID = w1
		l2 := pkMoveWalkTestLink(3, 7, 4, 5, 10)
		l2.oldRowID = w1
		l2.newRowID = w2
		l3 := pkMoveWalkTestLink(4, 9, 5, 6, 20)
		p := pkMoveWalkTestProvider(l1, l2, l3)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(4, 0x02, 9, 25),
			pkMoveDepartRow(9, 25, 15),
			pkMoveLiveRow(5, 0xbb, 6, 25),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{5}, finalPK)
		require.Equal(t, []byte{0xbb}, row)
	})

	t.Run("two successors for one identity fail closed", func(t *testing.T) {
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 1, 3, 7, 10),
			pkMoveWalkTestLink(3, 7, 4, 5, 20),
			pkMoveWalkTestLink(3, 7, 5, 6, 20),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 25),
			pkMoveDepartRow(7, 25, 15),
			pkMoveLiveRow(4, 0xbb, 5, 25),
		)
		_, _, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("post-window departure of the claimed row ends the lineage at the window", func(t *testing.T) {
		// Same stream shape as the refusal above, but the departure of
		// the claimed row has NO binding tombstone inside this window:
		// the recording transaction was created before the window
		// endpoint and committed after it, so this window never saw the
		// departure. A complete window stream shows every committed
		// in-window departure as its tombstone, so the absence proves
		// the recorded departure is future history: the lineage ends at
		// the claimed version for this window and must not fail closed.
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 1, 3, 7, 10),
			pkMoveWalkTestLink(3, 42, 4, 5, 20),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveLiveRow(3, 0xdd, 42, 15),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{3}, finalPK)
		require.Equal(t, []byte{0xdd}, row)
	})

	t.Run("departure of a committed pre-image is not the landed lineage", func(t *testing.T) {
		// Vacate-before-arrival inside one S3-route transaction:
		// committed occupants (2,'A') and (3,'B') exist; the transaction
		// moves B 3->4 (vacating a COMMITTED row) and then moves A 2->3
		// (landing at the vacated key). Both links carry NULL
		// replacement identities and the same create timestamp. A's
		// arrival at 3 must claim the row its own update inserted (the
		// lineage's continuation), NOT follow B's same-transaction 3->4
		// departure, whose old row id matches the committed stream
		// tombstone at 3 — evidence that link consumed B's pre-image, a
		// different generation than the landed one.
		lA := pkMoveWalkTestLink(2, 10, 3, 0, 10)
		lA.hasNewRowID = false
		lB := pkMoveWalkTestLink(3, 20, 4, 0, 10)
		lB.hasNewRowID = false
		p := pkMoveWalkTestProvider(lA, lB)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 10, 15),
			pkMoveTombRow(3, 0x02, 20, 15),
			pkMoveLiveRow(3, 0xaa, 30, 15),
			pkMoveLiveRow(4, 0xbb, 40, 15),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 10))
		require.True(t, ok)
		require.Equal(t, []byte{3}, finalPK)
		require.Equal(t, []byte{0xaa}, row,
			"A's lineage ends on A's own landed row at 3")
		// B's own lineage still folds 3->4 with B's payload.
		finalPK, row, ok = p.resolvePkMoveChain(view, []byte{3}, mustRowID(t, 20))
		require.True(t, ok)
		require.Equal(t, []byte{4}, finalPK)
		require.Equal(t, []byte{0xbb}, row)
	})

	t.Run("unpopped claim candidate with in-window departure defers and folds downstream", func(t *testing.T) {
		// The born-died merge pops data rows in parallel shard order, so
		// the walk can meet the intermediate version (born at the anchor
		// commit timestamp) still live under the landed key even though
		// its departure is recorded and in-window. The walk must not
		// claim it and must not fail: it defers to the departure hop and
		// folds the final version, exactly as if the intermediate data
		// row had already been popped.
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 1, 3, 7, 10),
			pkMoveWalkTestLink(3, 7, 5, 9, 20),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveLiveRow(3, 0xcc, 7, 15),
			pkMoveTombRow(3, 0x02, 7, 20),
			pkMoveDepartRow(7, 20, 15),
			pkMoveLiveRow(5, 0xbb, 9, 20),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{5}, finalPK)
		require.Equal(t, []byte{0xbb}, row)
	})

	t.Run("stale live row fails the timestamp tie", func(t *testing.T) {
		// The live row predates the anchor transaction: not the
		// replacement version, and no recorded continuation exists.
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 10))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveLiveRow(3, 0xdd, 42, 5),
		)
		_, _, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("row without identity evidence fails closed", func(t *testing.T) {
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 10))
		view := pkMoveWalkTestStubView(pkMoveTombRow(2, 0x01, 1, 15))
		view.live[string([]byte{3})] = [][]byte{{0xdd}}
		_, _, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("occupant rows at other timestamps do not block a claim", func(t *testing.T) {
		// Several stream rows may appear under one key across versions:
		// the one born at the chain timestamp is claimed; the rest keep
		// their plain meaning.
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 10))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveLiveRow(3, 0xdd, 42, 15),
			pkMoveLiveRow(3, 0xde, 8, 16),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{3}, finalPK)
		require.Equal(t, []byte{0xdd}, row)
	})

	t.Run("two stream rows born at the chain timestamp fail closed", func(t *testing.T) {
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 10))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveLiveRow(3, 0xdd, 42, 15),
			pkMoveLiveRow(3, 0xde, 8, 15),
		)
		_, _, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("older-version churn at the landed key is skipped", func(t *testing.T) {
		// A tombstone at or before the chain timestamp names an older
		// version of the key; its successor link must not interrupt the
		// claim of the version born at the chain timestamp.
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 1, 3, 7, 10),
			pkMoveWalkTestLink(3, 50, 9, 60, 20),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x03, 50, 5),
			pkMoveLiveRow(3, 0xdd, 42, 15),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{3}, finalPK)
		require.Equal(t, []byte{0xdd}, row)
	})

	t.Run("departure of a version born elsewhere is skipped", func(t *testing.T) {
		// The landed key holds only a departed version born before the
		// chain reached it; its linked departure is not this lineage's.
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 1, 3, 7, 10),
			pkMoveWalkTestLink(3, 7, 4, 5, 20),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 25),
			pkMoveDepartRow(7, 25, 5),
			pkMoveLiveRow(4, 0xbb, 5, 25),
		)
		_, _, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("cycle fails closed", func(t *testing.T) {
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 1, 3, 7, 10),
			pkMoveWalkTestLink(3, 7, 2, 9, 20),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 25),
			pkMoveDepartRow(7, 25, 15),
		)
		_, _, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("same-key update tail folds the re-born image", func(t *testing.T) {
		// 2->3 commits at 15; a stable-key update at 3 commits at 25:
		// its delete half carries no link, so the tombstone with the
		// lineage birth (15) advances the certificate to 25 and the
		// re-born image is claimed at the same key.
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 10))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 25),
			pkMoveDepartRow(7, 25, 15),
			pkMoveLiveRow(3, 0xdd, 42, 25),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{3}, finalPK)
		require.Equal(t, []byte{0xdd}, row)
	})

	t.Run("same-key tail follows a cross-transaction hop and passes an occupant", func(t *testing.T) {
		// The recorded BVT shape: 2->3->4 across transactions, a fresh
		// insert reusing vacated key 3, and a stable-key edit at 4 as the
		// newest generation. The walk hops to 4, tails past the edit's
		// delete half, and claims the edited image; the occupant keeps
		// its plain insert meaning.
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 1, 3, 7, 10),
			pkMoveWalkTestLink(3, 7, 4, 5, 20),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 25),
			pkMoveDepartRow(7, 25, 15),
			pkMoveTombRow(4, 0x03, 5, 35),
			pkMoveDepartRow(5, 35, 25),
			pkMoveLiveRow(3, 0xcc, 8, 41),
			pkMoveLiveRow(4, 0xbb, 77, 35),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{4}, finalPK)
		require.Equal(t, []byte{0xbb}, row)
	})

	t.Run("same-key tail then a linked continuation hops on", func(t *testing.T) {
		// 2->3 at 15, a stable-key edit at 3 at 25, then 3->4 at 35:
		// the tail re-enters key 3 with certificate 25 and the linked
		// departure of the edited version continues the walk.
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 1, 3, 7, 10),
			pkMoveWalkTestLink(3, 42, 4, 5, 30),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 25),
			pkMoveDepartRow(7, 25, 15),
			pkMoveTombRow(3, 0x03, 42, 35),
			pkMoveDepartRow(42, 35, 25),
			pkMoveLiveRow(4, 0xbb, 5, 35),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{4}, finalPK)
		require.Equal(t, []byte{0xbb}, row)
	})

	t.Run("same-key tail claims through several stable-key updates", func(t *testing.T) {
		// Two consecutive stable-key updates at the landed key chain
		// through both netted generations before the live row is claimed.
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 10))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 25),
			pkMoveDepartRow(7, 25, 15),
			pkMoveTombRow(3, 0x03, 42, 35),
			pkMoveDepartRow(42, 35, 25),
			pkMoveLiveRow(3, 0xdd, 9, 35),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{3}, finalPK)
		require.Equal(t, []byte{0xdd}, row)
	})

	t.Run("lineage deleted and re-inserted in separate transactions keeps delete semantics", func(t *testing.T) {
		// The landed version died at 25 with no recorded move: complete
		// evidence proves the lineage ended in a plain deletion, so the
		// anchor's own DELETE stands. The row born at 41 is an
		// independent re-insert, not the deleting transaction's image:
		// no fold may claim it, and the stream replays it as an
		// independent insert.
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 10))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 25),
			pkMoveDepartRow(7, 25, 15),
			pkMoveLiveRow(3, 0xcc, 8, 41),
		)
		_, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 1))
		require.Equal(t, pkMoveProvenDeleted, verdict)
	})

	t.Run("plain delete ending the lineage is a proven terminal deletion", func(t *testing.T) {
		// The tail takes the delete timestamp as certificate, the claim
		// finds no row born at it, and the cleared inhabitant blocks any
		// same-transaction continuation off the consumed link: complete
		// evidence proves the move's lineage was terminated by a plain
		// deletion — categorically distinct from incomplete provenance.
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 10))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 25),
			pkMoveDepartRow(7, 25, 15),
		)
		_, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 1))
		require.Equal(t, pkMoveProvenDeleted, verdict)
	})

	t.Run("two lineage departures from one version fail closed", func(t *testing.T) {
		// Committed one-image-per-key permits one death of the version
		// born at the chain timestamp; a second matching tombstone is
		// stream corruption.
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 10))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 25),
			pkMoveTombRow(3, 0x03, 8, 25),
			pkMoveDepartRow(7, 25, 15),
			pkMoveDepartRow(8, 25, 15),
			pkMoveLiveRow(3, 0xdd, 42, 25),
		)
		_, _, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("same-key tail never follows a foreign departure's successor", func(t *testing.T) {
		// A linked departure of a version born elsewhere sits at the
		// landed key next to the lineage's own link-less departure; the
		// walk tails on its own certificate and never hops along the
		// foreign chain.
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 1, 3, 7, 10),
			pkMoveWalkTestLink(3, 50, 9, 60, 20),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 25),
			pkMoveDepartRow(7, 25, 15),
			pkMoveTombRow(3, 0x03, 50, 26),
			pkMoveDepartRow(50, 26, 6),
			pkMoveLiveRow(3, 0xdd, 42, 25),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{3}, finalPK)
		require.Equal(t, []byte{0xdd}, row)
	})

	t.Run("anchor row id mismatch fails closed", func(t *testing.T) {
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 10))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 2, 15),
			pkMoveLiveRow(3, 0xdd, 42, 15),
		)
		// The tombstone names row id 2 while the link names row id 1:
		// independent delete+insert reusing the key stays delete+insert.
		_, _, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 2))
		require.False(t, ok)
	})

	t.Run("anchor without a stream tombstone timestamp fails closed", func(t *testing.T) {
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 10))
		view := pkMoveWalkTestStubView(pkMoveLiveRow(3, 0xdd, 42, 15))
		_, _, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("anchor reincarnated by a later transaction fails closed", func(t *testing.T) {
		// F2 review scenario: DELETE 2 commits at 12 (rid 0 is the LCA
		// version the destination holds), an unrelated INSERT (2, ...) is
		// born at 20 as rid 1, and only THAT row moves 2->3 at 30. The
		// anchor row descends from the 20-commit reincarnation, not from
		// the LCA lineage, so folding would rewrite the destination's
		// original lineage and cascade children onto an unrelated row.
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 25))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 0, 12),
			pkMoveTombRow(2, 0x02, 1, 30),
			pkMoveDepartRow(1, 30, 20),
			pkMoveLiveRow(3, 0xdd, 42, 30),
		)
		_, _, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("anchor re-born by the same transaction that deleted the previous version folds", func(t *testing.T) {
		// One transaction DELETEs the LCA version (rid 0) and inserts the
		// replacement row (rid 1) at key 2: both commit at 12. A later
		// transaction moves the re-born lineage 2->3 at 30. The anchor's
		// birth ties to the previous version's death, so continuity
		// across the in-transaction reincarnation holds and the fold
		// proceeds.
		//
		// This acceptance is deliberate, not a gap (design doc D87,
		// re-affirmed D94 for the r22 review): the committed change
		// stream makes a same-transaction same-key DELETE+INSERT
		// byte-identical to multi_update's own stable-key UPDATE shape,
		// and #27996 defines that shape as a single update replay.
		// Refusing the tie would also refuse every genuine
		// payload-update-then-pk-move chain (the anchor's only
		// continuity evidence for a stable-key rewrite is this same
		// tie, since such rewrites record nothing durable) and would
		// diverge from how a NATIVE replay of the same window behaves,
		// so the walk accepts the same-instant re-birth as the
		// lineage's own reincarnation. The era-16 live batteries prove
		// the indistinguishability executably: payload-update-then-move
		// (A) and explicit-txn-DELETE+INSERT-then-move (B) fold to
		// identical cascades, the plain path replays B-without-move as
		// an UPDATE keeping children (D), and pinned BVT sections
		// etj2a_/etj2b_/etj2d_ (merge) and etj2pa_/etj2pb_/etj2pd_
		// (pick) record the contract statement-by-statement against
		// native DELETE-CASCADE (children removed) and RESTRICT
		// (rejected) controls. The independent DELETE-then-INSERT
		// *across* transactions keeps delete semantics — see the
		// reincarnated/fail-closed companions above and the tx_p
		// counterexample in pick_foreign_key_pk_move.
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 25))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 0, 12),
			pkMoveTombRow(2, 0x02, 1, 30),
			pkMoveDepartRow(1, 30, 12),
			pkMoveLiveRow(3, 0xdd, 42, 30),
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{3}, finalPK)
		require.Equal(t, []byte{0xdd}, row)
	})

	t.Run("anchor with no birth evidence facing a previous death fails closed", func(t *testing.T) {
		// Continuity cannot be established without the anchor row's
		// birth: with an earlier tombstone under the anchor key and no
		// departed-birth record for the anchor row, the walk refuses.
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 25))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 0, 12),
			pkMoveTombRow(2, 0x02, 1, 30),
			pkMoveLiveRow(3, 0xdd, 42, 30),
		)
		_, _, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("second tombstone at the anchor commit instant fails closed", func(t *testing.T) {
		// Two tombstones under the anchor key committed at the anchor's
		// own commit timestamp cannot both name the transaction's image
		// of that key: the stream contradicts itself and the walk
		// refuses.
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 25))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 9, 30),
			pkMoveTombRow(2, 0x02, 1, 30),
			pkMoveLiveRow(3, 0xdd, 42, 30),
		)
		_, _, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("nil provider fails closed", func(t *testing.T) {
		var p *pkMoveLinkProvider
		_, _, ok := p.resolvePkMoveChain(pkMoveWalkTestStubView(), []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("chain longer than the hop bound fails closed", func(t *testing.T) {
		var (
			links   []pkMoveLink
			entries []pkMoveWalkTestRow
		)
		// Hop i moves key 10+i to 11+i, committed at 100+i; every landed
		// version is born at 100+i and departs at 100+i+1, so the
		// cross-transaction checks pass until the hop cap trips.
		entries = append(entries, pkMoveTombRow(10, 0x01, 1, 100))
		for i := 0; i < pkMoveLinkMaxChain+2; i++ {
			links = append(links, pkMoveWalkTestLink(
				byte(10+i), byte(i%250+1), byte(11+i), byte(i%250+2), uint64(10+i),
			))
			entries = append(entries,
				pkMoveTombRow(byte(11+i), byte(i+2), byte(i%250+2), uint64(101+i)),
				pkMoveDepartRow(byte(i%250+2), uint64(101+i), uint64(100+i)),
			)
		}
		p := pkMoveWalkTestProvider(links...)
		view := pkMoveWalkTestStubView(entries...)
		_, _, ok := p.resolvePkMoveChain(view, []byte{10}, mustRowID(t, 1))
		require.False(t, ok)
		// F3: cap exhaustion is UNRESOLVED provenance, not absent intent —
		// the apply must abort loudly instead of replaying a destructive
		// delete for a genuine move chain.
		_, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{10}, mustRowID(t, 1))
		require.Equal(t, pkMoveUnresolved, verdict)
	})

	t.Run("cross-transaction chain returning to a vacated key folds", func(t *testing.T) {
		// The live #28026 xn shape: transaction one (create ts 10, commit
		// 100) moves 2->3->4, transaction two (create ts 20, commit 200)
		// moves 4->3->5. Both transactions mint their workspace identities
		// from the same segment, so raw ids wsA and wsB each belong to two
		// different generations; only the recorded identities plus the
		// (row id, death, birth) stream pairing disambiguate the lineage.
		wsA := mustWorkspaceRowID(t, 1)
		wsB := mustWorkspaceRowID(t, 2)
		link := func(oldKey byte, oldRid types.Rowid, newKey byte, newRid types.Rowid, writeTS uint64) pkMoveLink {
			return pkMoveLink{
				oldRowID:    oldRid,
				newRowID:    newRid,
				hasNewRowID: true,
				oldPK:       []byte{oldKey},
				newPK:       []byte{newKey},
				writeTS:     types.BuildTS(int64(writeTS), 0),
			}
		}
		p := pkMoveWalkTestProvider(
			link(2, mustRowID(t, 1), 3, wsA, 10),
			link(3, wsA, 4, wsB, 10),
			link(4, wsB, 3, wsA, 20),
			link(3, wsA, 5, wsB, 20),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 100),
			// Transaction one's netted intermediate at 3: born and
			// deleted at its own commit instant.
			pkMoveWalkTestRow{key: []byte{3}, rowID: wsA, birth: types.BuildTS(100, 0), tombRow: true},
			pkMoveWalkTestRow{rowID: wsA, birth: types.BuildTS(100, 0), death: types.BuildTS(100, 0), departedRow: true},
			// The committed row at 4 departs at transaction two's commit.
			pkMoveWalkTestRow{key: []byte{4}, rowID: wsB, birth: types.BuildTS(200, 0), tombRow: true},
			pkMoveWalkTestRow{rowID: wsB, birth: types.BuildTS(100, 0), death: types.BuildTS(200, 0), departedRow: true},
			// Transaction two's netted intermediate at 3.
			pkMoveWalkTestRow{key: []byte{3}, rowID: wsA, birth: types.BuildTS(200, 0), tombRow: true},
			pkMoveWalkTestRow{rowID: wsA, birth: types.BuildTS(200, 0), death: types.BuildTS(200, 0), departedRow: true},
			// The final row at 5 carries transaction two's second mint:
			// byte-identical to transaction one's row at 4.
			pkMoveWalkTestRow{key: []byte{5}, row: []byte{0xee}, rowID: wsB, birth: types.BuildTS(200, 0)},
		)
		finalPK, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{5}, finalPK)
		require.Equal(t, []byte{0xee}, row)
	})

	t.Run("same-transaction key return still fails closed", func(t *testing.T) {
		// One transaction rotating 2->3->4->3 never advances the birth
		// certificate, so revisiting key 3 inside the same commit instant
		// cannot be distinguished from a rotation cycle and the walk must
		// refuse to interpret it.
		wsA := mustWorkspaceRowID(t, 1)
		wsB := mustWorkspaceRowID(t, 2)
		wsC := mustWorkspaceRowID(t, 3)
		link := func(oldKey byte, oldRid types.Rowid, newKey byte, newRid types.Rowid) pkMoveLink {
			return pkMoveLink{
				oldRowID:    oldRid,
				newRowID:    newRid,
				hasNewRowID: true,
				oldPK:       []byte{oldKey},
				newPK:       []byte{newKey},
				writeTS:     types.BuildTS(10, 0),
			}
		}
		p := pkMoveWalkTestProvider(
			link(2, mustRowID(t, 1), 3, wsA),
			link(3, wsA, 4, wsB),
			link(4, wsB, 3, wsC),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 100),
		)
		_, _, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("committed namesakes of a raw workspace id keep the departure ambiguous", func(t *testing.T) {
		// Two committed links departing (4, wsA) from different
		// transactions: a committed (key, row id) pair names one committed
		// image, so namesakes this spare cannot exist for one image — the
		// pair can only arise when two reincarnations of the raw id sat at
		// the same key, which the walk treats as ambiguous evidence.
		wsA := mustWorkspaceRowID(t, 1)
		anchor := pkMoveLink{
			oldRowID:    mustRowID(t, 1),
			hasNewRowID: false, // pre-fix or S3-unattributed link: no identity
			oldPK:       []byte{2},
			newPK:       []byte{4},
			writeTS:     types.BuildTS(10, 0),
		}
		fork1 := pkMoveLink{
			oldRowID:    wsA,
			newRowID:    mustRowID(t, 7),
			hasNewRowID: true,
			oldPK:       []byte{4},
			newPK:       []byte{7},
			writeTS:     types.BuildTS(10, 0),
		}
		fork2 := fork1
		fork2.newRowID = mustRowID(t, 9)
		fork2.newPK = []byte{9}
		fork2.writeTS = types.BuildTS(20, 0)
		p := pkMoveWalkTestProvider(anchor, fork1, fork2)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 100),
			pkMoveWalkTestRow{key: []byte{4}, rowID: wsA, birth: types.BuildTS(200, 0), tombRow: true},
			pkMoveWalkTestRow{rowID: wsA, birth: types.BuildTS(100, 0), death: types.BuildTS(200, 0), departedRow: true},
		)
		_, _, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("cross-transaction return to an earlier key folds once departures are attributable", func(t *testing.T) {
		// 2->3 (txn 1), 3->4 (txn 2), 4->3 again (txn 3): all three
		// transactions mint the same raw workspace id wsA for their first
		// row, so the returned lineage's final row carries the very id an
		// EARLIER link already records a departure for. Pre-txn-id builds
		// kept legacy semantics on this shape: the claim's departure
		// probe could not attribute that link to its incarnation and
		// refused. The durable recording-transaction identity (the create
		// timestamp fallback for identity-less links, as fabricated here)
		// attributes the 3->4 departure to the chain's own second hop, so
		// the return-to-start shape folds one continuation per recorded
		// native update and lands back on key 3's live row.
		wsA := mustWorkspaceRowID(t, 1)
		link := func(oldKey byte, oldRid types.Rowid, newKey byte, writeTS uint64) pkMoveLink {
			return pkMoveLink{
				oldRowID:    oldRid,
				newRowID:    wsA,
				hasNewRowID: true,
				oldPK:       []byte{oldKey},
				newPK:       []byte{newKey},
				writeTS:     types.BuildTS(int64(writeTS), 0),
			}
		}
		p := pkMoveWalkTestProvider(
			link(2, mustRowID(t, 1), 3, 10),
			link(3, wsA, 4, 20),
			link(4, wsA, 3, 30),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 100),
			pkMoveWalkTestRow{key: []byte{3}, rowID: wsA, birth: types.BuildTS(200, 0), tombRow: true},
			pkMoveWalkTestRow{rowID: wsA, birth: types.BuildTS(100, 0), death: types.BuildTS(200, 0), departedRow: true},
			pkMoveWalkTestRow{key: []byte{4}, rowID: wsA, birth: types.BuildTS(300, 0), tombRow: true},
			pkMoveWalkTestRow{rowID: wsA, birth: types.BuildTS(200, 0), death: types.BuildTS(300, 0), departedRow: true},
			pkMoveWalkTestRow{key: []byte{3}, row: []byte{0xcc}, rowID: wsA, birth: types.BuildTS(300, 0)},
		)
		k, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{3}, k)
		require.Equal(t, []byte{0xcc}, row)
	})

	t.Run("same-transaction homecoming resolves through recorded identity hops", func(t *testing.T) {
		// 2->3 then 3->2 with a changed payload inside ONE transaction:
		// explicit workspace identities join both moves, so the lineage
		// provably returns to its original key (the re-birthed committed
		// row is born at the anchor's commit instant). The intermediate
		// generation at 3 never reaches the stream. The fold planner's
		// oldKey==finalPK path then decides: changed payload folds an
		// in-place update, identical payload folds a no-op.
		w1 := pkMoveWorkspaceRid(1, 0)
		w2 := pkMoveWorkspaceRid(2, 0)
		l1 := pkMoveWalkTestLink(2, 1, 3, 7, 10)
		l1.newRowID = w1
		l2 := pkMoveWalkTestLink(3, 7, 2, 5, 10)
		l2.oldRowID = w1
		l2.newRowID = w2
		p := pkMoveWalkTestProvider(l1, l2)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveLiveRow(2, 0x02, 9, 15),
		)
		finalPK, row, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 1))
		require.Equal(t, pkMoveResolved, verdict)
		require.Equal(t, []byte{2}, finalPK)
		require.Equal(t, []byte{0x02}, row)
	})

	t.Run("same-transaction homecoming through a longer chain", func(t *testing.T) {
		// 2->3->4->2 in one transaction: the revisit of the anchor key
		// at the shared instant is admitted only because every arrival
		// was a recorded identity hop, and the re-birthed row resolves.
		w1 := pkMoveWorkspaceRid(1, 0)
		w2 := pkMoveWorkspaceRid(2, 0)
		w3 := pkMoveWorkspaceRid(3, 0)
		l1 := pkMoveWalkTestLink(2, 1, 3, 7, 10)
		l1.newRowID = w1
		l2 := pkMoveWalkTestLink(3, 7, 4, 5, 10)
		l2.oldRowID = w1
		l2.newRowID = w2
		l3 := pkMoveWalkTestLink(4, 5, 2, 9, 10)
		l3.oldRowID = w2
		l3.newRowID = w3
		p := pkMoveWalkTestProvider(l1, l2, l3)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveLiveRow(2, 0x03, 9, 15),
		)
		finalPK, row, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 1))
		require.Equal(t, pkMoveResolved, verdict)
		require.Equal(t, []byte{2}, finalPK)
		require.Equal(t, []byte{0x03}, row)
	})

	t.Run("same-transaction key rotation of two rows still resolves each walk", func(t *testing.T) {
		// A moves 2->3 while B moves 3->2 in ONE transaction: each row's
		// walk must resolve (their recorded identities never cross),
		// while the FOLD PLANNER keeps rejecting the key exchange as a
		// rotation cycle — the cycle defense does not live in this guard.
		wA := pkMoveWorkspaceRid(1, 0)
		wB := pkMoveWorkspaceRid(2, 0)
		lA := pkMoveWalkTestLink(2, 1, 3, 7, 10)
		lA.newRowID = wA
		lB := pkMoveWalkTestLink(3, 8, 2, 5, 10)
		lB.oldRowID = mustRowID(t, 8)
		lB.newRowID = wB
		p := pkMoveWalkTestProvider(lA, lB)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 8, 15),
			pkMoveLiveRow(3, 0xaa, 42, 15), // A's landed row
			pkMoveLiveRow(2, 0xbb, 43, 15), // B's landed row
		)
		finalPK, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 1))
		require.Equal(t, pkMoveResolved, verdict)
		require.Equal(t, []byte{3}, finalPK)
		finalPK, _, verdict = p.resolvePkMoveChainVerdict(view, []byte{3}, mustRowID(t, 8))
		require.Equal(t, pkMoveResolved, verdict)
		require.Equal(t, []byte{2}, finalPK)
	})

	t.Run("recorded arrival at a just-vacated key proves the anchor is not the LCA lineage", func(t *testing.T) {
		// The competing-lineage counterexample: key 1 holds B (rid 2),
		// key 2 holds A (rid 1); one transaction deletes key 1 and moves
		// 2->1, the next moves 1->3. The key-1 anchor's birth tie to the
		// previous key-1 death (both at 10) is EXACTLY the chain's own
		// reincarnation shape the continuity check folds — but the
		// recorded 2->1 link names the anchor row's own durable identity
		// arriving from key 2, so the version at 1 descends from the
		// key-2 lineage, not from the image the destination holds at 1.
		// Letting this anchor fold makes it compete with the genuine
		// key-2 fold for one replacement row: one lineage gets rewritten
		// as the other. Complete evidence instead proves the anchor has
		// no LCA continuity: keep the tombstone's own delete semantics.
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 1, 1, 7, 8),  // recorded arrival 2->1
			pkMoveWalkTestLink(1, 1, 3, 5, 15), // the anchor's own move 1->3
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 10),  // A departs key 2 at T1
			pkMoveTombRow(1, 0x11, 2, 10),  // B (previous occupant) dies at key 1 at T1
			pkMoveTombRow(1, 0x12, 1, 20),  // anchor: A departs key 1 at T2
			pkMoveDepartRow(1, 20, 10),     // A's version at 1 was born at T1
			pkMoveLiveRow(3, 0xbb, 42, 20), // final generation at 3 born at T2
		)
		_, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{1}, mustRowID(t, 1))
		require.Equal(t, pkMoveProvenNone, verdict)

		// The genuine anchor at the arrival's origin resolves the whole
		// lineage 2->1->3 into one native move 2->3, in the same view —
		// the verdict above did not come from broken fabrication.
		finalPK, row, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 1))
		require.Equal(t, pkMoveResolved, verdict)
		require.Equal(t, []byte{3}, finalPK)
		require.Equal(t, []byte{0xbb}, row)
	})

	t.Run("same-instant birth without a recorded arrival still folds", func(t *testing.T) {
		// D87/D94 continuity control: rid 2 reincarnates key 2 inside
		// the transaction that deleted rid 1 (same-instant re-birth at
		// 10), then moves 2->3 at 20. No recorded link ever lands rid 2
		// AT key 2, so the birth-equals-deletion tie keeps its approved
		// meaning and the anchor folds.
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 2, 3, 5, 15),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 10), // rid 1 dies at key 2 at T1
			pkMoveTombRow(2, 0x02, 2, 20), // anchor: rid 2 departs key 2 at T2
			pkMoveDepartRow(2, 20, 10),    // rid 2 was born at T1
			pkMoveLiveRow(3, 0xbb, 42, 20),
		)
		finalPK, row, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 2))
		require.Equal(t, pkMoveResolved, verdict)
		require.Equal(t, []byte{3}, finalPK)
		require.Equal(t, []byte{0xbb}, row)
	})

	t.Run("a recorded arrival of another row does not disqualify the anchor", func(t *testing.T) {
		// Same reincarnation shape, but a recorded link lands a
		// DIFFERENT row at key 2 one instant EARLIER (its own deleting
		// tombstone at 9 commits at 6): the arrival's (key, instant)
		// pair never matches the anchor version's birth, so the tie is
		// the anchor's own lineage evidence and the fold proceeds.
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 2, 3, 5, 15),
			pkMoveWalkTestLink(9, 7, 2, 8, 6),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 10),
			pkMoveTombRow(2, 0x02, 2, 20),
			pkMoveTombRow(9, 0x09, 7, 6), // the foreign arrival's own departure, at 6
			pkMoveDepartRow(2, 20, 10),
			pkMoveLiveRow(3, 0xbb, 42, 20),
		)
		finalPK, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 2))
		require.Equal(t, pkMoveResolved, verdict)
		require.Equal(t, []byte{3}, finalPK)
	})

	t.Run("recorded arrival chain through a netted intermediate key disqualifies the impostor anchor", func(t *testing.T) {
		// The r53 F1 counterexample: key 1 holds B (rid 2), key 2 holds
		// A (rid 1); ONE transaction deletes key 1 and moves 2->4->1,
		// netting the intermediate generation at 4 out of the committed
		// stream, the next transaction moves 1->3. The key-1 anchor's
		// birth tie is exact (born 10 == previous death 10) but the
		// standing version at 1 descends from the KEY-2 lineage: the
		// recorded chain grounds its arrival on the committed departure
		// at key 2 even though key 4 carries no tombstone. Accepting
		// the tie here made PICK KEYS(1) cascade B's child onto A's
		// lineage, and made MERGE / KEYS(1,2) trip the unique-claim
		// abort on an otherwise provable chain.
		w1 := pkMoveWorkspaceRid(1, 0)
		w2 := pkMoveWorkspaceRid(2, 0)
		l1 := pkMoveWalkTestLink(2, 1, 4, 7, 10) // 2->4 in txn T1
		l1.newRowID = w1
		l2 := pkMoveWalkTestLink(4, 7, 1, 8, 10) // 4->1 in txn T1
		l2.oldRowID = w1
		l2.newRowID = w2
		p := pkMoveWalkTestProvider(
			l1,
			l2,
			pkMoveWalkTestLink(1, 3, 3, 5, 20), // the anchor's own move 1->3 at T2
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 10),  // A departs key 2 at T1 (the chain's ground)
			pkMoveTombRow(1, 0x11, 2, 10),  // B (previous occupant) dies at key 1 at T1
			pkMoveTombRow(1, 0x12, 3, 20),  // anchor: A's landed version departs key 1 at T2
			pkMoveDepartRow(3, 20, 10),     // that landed version was born at T1
			pkMoveLiveRow(3, 0xbb, 42, 20), // final generation at 3 born at T2
		)
		_, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{1}, mustRowID(t, 3))
		require.Equal(t, pkMoveProvenNone, verdict)

		// The genuine anchor at the chain's origin resolves the whole
		// lineage 2->4->1->3 into one native move 2->3, in the same
		// view: the disqualification above did not come from broken
		// fabrication.
		finalPK, row, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 1))
		require.Equal(t, pkMoveResolved, verdict)
		require.Equal(t, []byte{3}, finalPK)
		require.Equal(t, []byte{0xbb}, row)
	})

	t.Run("a recorded arrival chain landing at another key does not disqualify the anchor", func(t *testing.T) {
		// The foreign chain grounds its arrival at key 9 (its origin
		// departs key 7 at 12): nothing lands at the anchor's key 2 at
		// the tie instant, so the D87/D94 tie keeps its meaning.
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 2, 3, 5, 20), // anchor's own move 2->3 at 20
			func() pkMoveLink {
				w := pkMoveWorkspaceRid(1, 0)
				l := pkMoveWalkTestLink(7, 7, 8, 9, 12)
				l.newRowID = w
				return l
			}(),
			func() pkMoveLink {
				w := pkMoveWorkspaceRid(1, 0)
				l := pkMoveWalkTestLink(8, 9, 9, 10, 12)
				l.oldRowID = w
				return l
			}(),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 10),
			pkMoveTombRow(2, 0x02, 2, 20),
			pkMoveTombRow(7, 0x07, 7, 12), // the foreign chain's ground
			pkMoveDepartRow(2, 20, 10),
			pkMoveLiveRow(3, 0xbb, 42, 20),
		)
		finalPK, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 2))
		require.Equal(t, pkMoveResolved, verdict)
		require.Equal(t, []byte{3}, finalPK)
	})

	t.Run("intermediate generations sharing a temporary key attribute by exact identity", func(t *testing.T) {
		// The r54 review's F2: two live lineages may legitimately reuse
		// one netted intermediate key inside a single transaction. Both
		// moves land at key 4 — A's continuation (2->4 feeding 4->1) and
		// C's independent first hop (5->4 feeding 4->6) — sharing the
		// transaction scope, but only A's carries the replacement
		// identity the 4->1 link consumed. Attribution must bind that
		// exact identity; the other generation's existence at the same
		// temporary key is not a contradiction.
		wA := pkMoveWorkspaceRid(11, 0)
		wC := pkMoveWorkspaceRid(12, 0)
		lA1 := pkMoveWalkTestLink(2, 1, 4, 21, 10) // A: 2->4 (netted intermediate)
		lA1.newRowID = wA
		lA2 := pkMoveWalkTestLink(4, 22, 1, 3, 10) // A: 4->1
		lA2.oldRowID = wA
		lA3 := pkMoveWalkTestLink(1, 3, 3, 5, 20)   // A: later 1->3
		lC1 := pkMoveWalkTestLink(5, 28, 4, 23, 10) // C: 5->4 (same temporary key)
		lC1.newRowID = wC
		lC2 := pkMoveWalkTestLink(4, 24, 6, 30, 10) // C: 4->6
		lC2.oldRowID = wC
		p := pkMoveWalkTestProvider(lA3, lA1, lA2, lC1, lC2)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(1, 0x01, 2, 10),  // key 1's standing version dies at 10
			pkMoveTombRow(2, 0x02, 1, 10),  // A's origin ground
			pkMoveTombRow(5, 0x05, 28, 10), // C's origin ground
			pkMoveTombRow(1, 0x11, 3, 20),  // A's landed generation departs key 1 at 20
			pkMoveDepartRow(1, 10, 5),      // A's origin version at key 2: born 5, died 10
			pkMoveDepartRow(28, 10, 5),     // C's origin version at key 5: born 5, died 10
			pkMoveDepartRow(3, 20, 10),     // A's landed version at key 1: born 10, died 20
			pkMoveLiveRow(3, 0xbb, 5, 20),  // A's final row
			pkMoveLiveRow(6, 0xcc, 30, 10), // C's final row
		)
		// The continuity tie at key 1 (A's landed generation was born at
		// the instant key 1's standing version died) must resolve as a
		// PROVEN arrival: A's chain 2->4->1 grounds at key 2's committed
		// tombstone even though C's link names the same temporary key
		// with a different generation.
		_, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{1}, mustRowID(t, 3))
		require.Equal(t, pkMoveProvenNone, verdict)

		// C's own lineage still folds end to end ...
		finalPK, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{5}, mustRowID(t, 28))
		require.Equal(t, pkMoveResolved, verdict)
		require.Equal(t, []byte{6}, finalPK)

		// ... and so does A's, reached from its origin key.
		finalPK, _, verdict = p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 1))
		require.Equal(t, pkMoveResolved, verdict)
		require.Equal(t, []byte{3}, finalPK)
	})

	t.Run("competing exact-identity predecessors still fail closed", func(t *testing.T) {
		// Two same-scope predecessors landing the SAME generation at the
		// chain's intermediate key: attribution cannot choose between
		// them, so the evidence stays incomplete rather than picking
		// one — the retained fail-closed lane of the r54 review's F2.
		// The chain's links ride its transaction's creation instant 5
		// while every birth/death instant is commit-shaped (r59 review
		// F1); the death at a recorded departure key committing at 10 is
		// what keeps the tie instant among the chain's feasible commit
		// instants.
		wA := pkMoveWorkspaceRid(13, 0)
		landed := pkMoveWalkTestLink(6, 9, 2, 10, 5)
		landed.oldRowID = wA
		rival1 := pkMoveWalkTestLink(4, 7, 6, 11, 5)
		rival1.newRowID = wA
		rival2 := pkMoveWalkTestLink(5, 8, 6, 12, 5)
		rival2.newRowID = wA
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 2, 3, 5, 20),
			landed,
			rival1,
			rival2,
		)
		// D156: the chain's scope is captured committed in-window at 10
		// (the death at a recorded departure key committing past
		// creation), so the tie instant it names fails the failed
		// attribution closed.
		pkMoveWalkFeedCommits(p, pkMoveWalkTestScope(5), types.BuildTS(10, 0))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 10),
			pkMoveTombRow(2, 0x02, 2, 20),
			pkMoveTombRow(6, 0x05, 30, 10), // a death at a recorded departure key, committing past creation
			pkMoveDepartRow(2, 20, 10),
			pkMoveLiveRow(3, 0xbb, 42, 20),
		)
		_, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 2))
		require.Equal(t, pkMoveUnresolved, verdict)
	})

	t.Run("an ungrounded insert-then-move chain keeps the D87 approximation lane", func(t *testing.T) {
		// A chain landing at the tie key whose origin staged its old
		// version inside the transaction (insert 5, move 5->6->2 in one
		// commit) has no committed departure anywhere: the stream
		// cannot attribute it, so the tie keeps the approved
		// approximation (fail same as the unrecorded reincarnation).
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 2, 3, 5, 20), // anchor's own move 2->3 at 20
			func() pkMoveLink {
				w := pkMoveWorkspaceRid(1, 0)
				l := pkMoveWalkTestLink(5, 7, 6, 9, 10)
				l.newRowID = w
				return l
			}(),
			func() pkMoveLink {
				w := pkMoveWorkspaceRid(1, 0)
				l := pkMoveWalkTestLink(6, 9, 2, 10, 10)
				l.oldRowID = w
				return l
			}(),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 10),
			pkMoveTombRow(2, 0x02, 2, 20),
			pkMoveDepartRow(2, 20, 10),
			pkMoveLiveRow(3, 0xbb, 42, 20),
		)
		finalPK, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 2))
		require.Equal(t, pkMoveResolved, verdict)
		require.Equal(t, []byte{3}, finalPK)
	})

	t.Run("unattributable arrival evidence fails closed instead of accepting the tie", func(t *testing.T) {
		// Ambiguous predecessors: two same-scope links land at the
		// chain's intermediate key, so the arrival at key 2 at the tie
		// instant cannot be attributed. Neither accepting nor
		// disproving the tie is sound: the verdict stays unresolved.
		// The links ride the transaction's creation instant 5 while
		// births and deaths carry commit instants (r59 review F1), and
		// the death at key 6 — a recorded departure key of the chain —
		// keeps the tie instant among its feasible commit instants.
		mkAmbiguous := func() *pkMoveLinkProvider {
			rival1 := pkMoveWalkTestLink(4, 7, 6, 11, 5)
			rival1.hasNewRowID = false // S3 route: no staged replacement identity
			rival2 := pkMoveWalkTestLink(5, 8, 6, 12, 5)
			rival2.hasNewRowID = false
			p := pkMoveWalkTestProvider(
				pkMoveWalkTestLink(2, 2, 3, 5, 20),
				pkMoveWalkTestLink(6, 9, 2, 10, 5), // landed at the tie key in txn T1
				rival1,                             // rival predecessor 1 at key 6
				rival2,                             // rival predecessor 2 at key 6
			)
			// D156: T1's scope is captured committed in-window at the
			// tie instant 10, so its failed attribution fails closed.
			pkMoveWalkFeedCommits(p, pkMoveWalkTestScope(5), types.BuildTS(10, 0))
			return p
		}
		mkView := func() *pkMoveWalkTestView {
			return pkMoveWalkTestStubView(
				pkMoveTombRow(2, 0x01, 1, 10),
				pkMoveTombRow(2, 0x02, 2, 20),
				pkMoveTombRow(6, 0x05, 30, 10), // a foreign death: not the landed link's old side
				pkMoveDepartRow(2, 20, 10),
				pkMoveLiveRow(3, 0xbb, 42, 20),
			)
		}
		_, _, verdict := mkAmbiguous().resolvePkMoveChainVerdict(mkView(), []byte{2}, mustRowID(t, 2))
		require.Equal(t, pkMoveUnresolved, verdict)

		// Contradictory generation: one same-scope predecessor lands a
		// DIFFERENT generation at the intermediate key than the
		// continuation moved — one committed image per key per commit
		// makes that corrupt, not ambiguous-but-harmless.
		w1 := pkMoveWorkspaceRid(1, 0)
		w2 := pkMoveWorkspaceRid(2, 0)
		landed := pkMoveWalkTestLink(6, 9, 2, 10, 5)
		landed.oldRowID = w1
		contra := pkMoveWalkTestLink(4, 7, 6, 11, 5)
		contra.newRowID = w2
		p2 := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 2, 3, 5, 20),
			landed,
			contra,
		)
		pkMoveWalkFeedCommits(p2, pkMoveWalkTestScope(5), types.BuildTS(10, 0))
		_, _, verdict = p2.resolvePkMoveChainVerdict(mkView(), []byte{2}, mustRowID(t, 2))
		require.Equal(t, pkMoveUnresolved, verdict)

		// Cyclic attribution: the landed link's backward walk returns to
		// a generation it already visited (a 2<->6 rotation staged in one
		// transaction). Unbounded by construction, so fail closed.
		wA := pkMoveWorkspaceRid(3, 0)
		wB := pkMoveWorkspaceRid(4, 0)
		lA := pkMoveWalkTestLink(2, 13, 6, 14, 5)
		lA.oldRowID, lA.newRowID = wA, wB
		lB := pkMoveWalkTestLink(6, 14, 2, 15, 5)
		lB.oldRowID, lB.newRowID = wB, wA
		p3 := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 2, 3, 5, 20),
			lA,
			lB,
		)
		pkMoveWalkFeedCommits(p3, pkMoveWalkTestScope(5), types.BuildTS(10, 0))
		_, _, verdict = p3.resolvePkMoveChainVerdict(mkView(), []byte{2}, mustRowID(t, 2))
		require.Equal(t, pkMoveUnresolved, verdict)
	})
}

// TestAnchorArrivalVerdictEvidenceCompleteness carries the r54 review's
// F1: an arrival-index miss proves "no recorded grounded arrival" only
// when the provider saw every link of every registered window — a whole
// scan enumerates both the departures AND the arrivals one key's tie can
// ask about. After a whole-window overflow the cache holds only per-key
// OLD-key probes, which cannot enumerate the moves LANDING at a probed
// key (link_id's prefix is reachable in the old-key direction only), so
// both a miss and a recorded hit in the partial index must fail closed
// instead of ruling the continuity tie.
func TestAnchorArrivalVerdictEvidenceCompleteness(t *testing.T) {
	window := pkMoveLinkTableWindow{tableID: 7, startPhys: 100, endPhys: 200}
	mkProvider := func() *pkMoveLinkProvider {
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(6, 1, 5, 2, 10), // grounded arrival landing at key 5 at 10
			pkMoveWalkTestLink(9, 3, 8, 4, 30), // unrelated history
		)
		p.windows = []pkMoveLinkTableWindow{window}
		return p
	}
	view := pkMoveWalkTestStubView(
		pkMoveTombRow(6, 0x06, 1, 10), // grounds 6->5 at commit instant 10
	)

	t.Run("a whole-window miss is a proven absence", func(t *testing.T) {
		p := mkProvider()
		p.fullWindows = map[uint64][2]uint64{
			window.tableID: {window.startPhys, window.endPhys},
		}
		// No recorded chain lands at key 4: the D87/D94 tie keeps its
		// meaning; the genuine grounded arrival at key 5 still rules.
		require.Equal(t, pkMoveArrivalNone,
			p.anchorArrivalVerdict(view, []byte{4}, types.BuildTS(10, 0)))
		require.Equal(t, pkMoveArrivalRecorded,
			p.anchorArrivalVerdict(view, []byte{5}, types.BuildTS(10, 0)))
	})

	t.Run("an overflow-narrowed miss cannot prove absence", func(t *testing.T) {
		p := mkProvider()
		p.overflowWindows = map[uint64][2]uint64{
			window.tableID: {window.startPhys, window.endPhys},
		}
		// Per-key OLD-key probes cannot enumerate what landed at key 4:
		// an incoming 2->...->4 chain the narrowing missed would be
		// folded onto the standing version — the silent lineage
		// corruption of the review's counterexample — so the miss must
		// fail closed instead.
		require.Equal(t, pkMoveArrivalIncomplete,
			p.anchorArrivalVerdict(view, []byte{4}, types.BuildTS(10, 0)))
	})

	t.Run("a never-completed window cannot prove absence", func(t *testing.T) {
		p := mkProvider()
		// An exhausted cache leaves keys unprobed without rescanning the
		// window; neither coverage flag is set, and absence is unproven.
		require.Equal(t, pkMoveArrivalIncomplete,
			p.anchorArrivalVerdict(view, []byte{4}, types.BuildTS(10, 0)))
	})

	t.Run("a narrowed window trusts no positive entry either", func(t *testing.T) {
		p := mkProvider()
		p.overflowWindows = map[uint64][2]uint64{
			window.tableID: {window.startPhys, window.endPhys},
		}
		// Narrowed evidence cannot show this chain's every hop was
		// probed, so even a recorded arrival stays unresolved — the tie
		// is neither accepted nor positively disproved on partial
		// evidence.
		require.Equal(t, pkMoveArrivalIncomplete,
			p.anchorArrivalVerdict(view, []byte{5}, types.BuildTS(10, 0)))
	})
}

// TestAnchorArrivalEvidenceScalesLinearly proves the continuity tie's
// arrival evidence is built ONCE per diff and reused across anchors
// (r53 F2): with N recorded links whose landed keys are all disjoint,
// evaluating independent anchors must not rescan the complete cache
// (the pre-existing per-anchor full scan cost N comparisons per tie).
func TestAnchorArrivalEvidenceScalesLinearly(t *testing.T) {
	const N = 2000 // recorded links laying disjoint grounded arrivals
	const M = 64   // independent anchors evaluated through the tie
	encKey := func(i int) []byte {
		return []byte{byte(i >> 16), byte(i >> 8), byte(i)}
	}
	ridOf := func(tag, i int) types.Rowid {
		var rid types.Rowid
		rid[0] = byte(i)
		rid[1] = byte(i >> 8)
		rid[2] = byte(tag)
		return rid
	}
	var links []pkMoveLink
	var entries []pkMoveWalkTestRow
	for i := 0; i < N; i++ {
		oldK, landK, farK := encKey(3*i), encKey(3*i+1), encKey(3*i+2)
		// A's direct grounded arrival at landK committed at 10.
		l := pkMoveWalkTestLink(oldK[2], byte(i), landK[2], byte(i+1), 10)
		l.oldPK, l.newPK = oldK, landK
		l.oldRowID = ridOf(1, i)
		l.newRowID = ridOf(2, i)
		links = append(links, l)
		entries = append(entries, pkMoveWalkTestRow{
			key: oldK, rowID: ridOf(1, i), birth: types.BuildTS(10, 0), tombRow: true,
		})
		// The landed generation's own later move to farK at 20, making
		// it a walkable anchor; its previous occupant died at the
		// arrival's instant, producing the continuity tie.
		d := pkMoveWalkTestLink(landK[2], byte(i+2), farK[2], byte(i+3), 20)
		d.oldPK, d.newPK = landK, farK
		d.oldRowID = ridOf(3, i)
		d.newRowID = ridOf(4, i)
		links = append(links, d)
		entries = append(entries,
			pkMoveWalkTestRow{
				key: landK, rowID: ridOf(5, i), birth: types.BuildTS(10, 0), tombRow: true,
			},
			pkMoveWalkTestRow{
				key: landK, rowID: ridOf(3, i), birth: types.BuildTS(20, 0), tombRow: true,
			},
			pkMoveWalkTestRow{rowID: ridOf(3, i), birth: types.BuildTS(10, 0),
				death: types.BuildTS(20, 0), departedRow: true},
		)
	}
	p := pkMoveWalkTestProvider(links...)
	view := pkMoveWalkTestStubView(entries...)
	runAnchors := func() {
		for j := 0; j < M; j++ {
			_, _, verdict := p.resolvePkMoveChainVerdict(view, encKey(3*j+1), ridOf(3, j))
			require.Equal(t, pkMoveProvenNone, verdict)
		}
	}
	runAnchors()
	builds, scans := p.arrivalEvidenceStats()
	require.Equal(t, uint64(1), builds, "arrival evidence must be built once and reused")
	// A design that rescans the whole cache per anchor pays at least
	// 2N link comparisons per anchor (>= 2N*M here); the shared index
	// pays one linear build plus O(1) candidate work per anchor.
	require.LessOrEqual(t, scans, uint64(8*N),
		"arrival evidence must stay linear in the link count (naive rescan needs >= %d)", 2*N*M)
	scansAfterFirstPass := scans
	runAnchors()
	builds, scans = p.arrivalEvidenceStats()
	require.Equal(t, uint64(1), builds, "a second pass over the same diff reuses the index")
	require.Equal(t, scansAfterFirstPass, scans, "a reused index performs no further link scans")
}

// TestAnchorArrivalEvidenceBucketsScaleLinearly bounds the work inside
// ONE arrival-index build (r54 review F3): an attribution must consult
// only the predecessor candidates of its OWN transaction scope and the
// tombstone references of the exact (key, row) pair it grounds, so a
// shared temporary key or a high-churn origin key cannot make one build
// quadratic in the bucket sizes.
func TestAnchorArrivalEvidenceBucketsScaleLinearly(t *testing.T) {
	encKey := func(i int) []byte {
		return []byte{byte(i >> 16), byte(i >> 8), byte(i)}
	}
	ridOf := func(tag, i int) types.Rowid {
		var rid types.Rowid
		rid[0] = byte(i)
		rid[1] = byte(i >> 8)
		rid[2] = byte(tag)
		return rid
	}

	t.Run("many transactions sharing one temporary key", func(t *testing.T) {
		// KB transactions i each move origin_i -> scratch -> final_i in
		// one commit; scratch never commits, so each scratch->final_i
		// link must chain back through ITS OWN transaction's link at the
		// shared key. The pre-fix build scanned the whole scratch bucket
		// (KB links) for every chain, costing ~KB*KB comparisons here.
		const KB = 256
		scratch := encKey(500000)
		var links []pkMoveLink
		var entries []pkMoveWalkTestRow
		for i := 0; i < KB; i++ {
			ts := uint64(100 + i)
			a := pkMoveWalkTestLink(0, 0, 0, 0, ts) // origin_i -> scratch
			a.oldPK, a.newPK = encKey(i), scratch
			a.oldRowID, a.newRowID = ridOf(11, i), ridOf(12, i)
			links = append(links, a)
			b := pkMoveWalkTestLink(0, 0, 0, 0, ts) // scratch -> final_i
			b.oldPK, b.newPK = scratch, encKey(100000+i)
			b.oldRowID, b.newRowID = ridOf(12, i), ridOf(13, i)
			links = append(links, b)
			entries = append(entries, pkMoveWalkTestRow{
				key: encKey(i), rowID: ridOf(11, i),
				birth: types.BuildTS(int64(ts), 0), tombRow: true, // grounds a_i
			})
		}
		p := pkMoveWalkTestProvider(links...)
		view := pkMoveWalkTestStubView(entries...)
		runVerdicts := func() {
			for _, i := range []int{0, 1, KB / 2, KB - 1} {
				require.Equal(t, pkMoveArrivalRecorded,
					p.anchorArrivalVerdict(view, encKey(100000+i), types.BuildTS(int64(100+i), 0)),
					"chain %d grounds at its origin's committed tombstone", i)
			}
		}
		runVerdicts()
		builds, scans := p.arrivalEvidenceStats()
		require.Equal(t, uint64(1), builds, "arrival evidence is built once")
		require.LessOrEqual(t, scans, uint64(12*KB),
			"one build must not rescan the shared-key bucket per chain (bucket scans would cost ~%d)",
			KB*KB)
		scansBefore := scans
		runVerdicts()
		builds, scans = p.arrivalEvidenceStats()
		require.Equal(t, uint64(1), builds)
		require.Equal(t, scansBefore, scans, "a reused index performs no further scans")
	})

	t.Run("repeated departures from one high-churn key", func(t *testing.T) {
		// KC transactions each move a distinct row out of the same hot
		// key H into their own goal; every grounding must consult ONLY
		// its own (key, row) tombstone reference, while the pre-fix
		// build rescanned H's whole tombstone history per departure
		// (~KC*KC comparisons).
		const KC = 256
		hotKey := encKey(600000)
		var links []pkMoveLink
		var entries []pkMoveWalkTestRow
		for i := 0; i < KC; i++ {
			ts := uint64(200 + i)
			l := pkMoveWalkTestLink(0, 0, 0, 0, ts)
			l.oldPK, l.newPK = hotKey, encKey(700000+i)
			l.oldRowID, l.newRowID = ridOf(21, i), ridOf(22, i)
			links = append(links, l)
			entries = append(entries, pkMoveWalkTestRow{
				key: hotKey, rowID: ridOf(21, i),
				birth: types.BuildTS(int64(ts), 0), tombRow: true,
			})
		}
		p := pkMoveWalkTestProvider(links...)
		view := pkMoveWalkTestStubView(entries...)
		runVerdicts := func() {
			for _, i := range []int{0, 1, KC / 2, KC - 1} {
				require.Equal(t, pkMoveArrivalRecorded,
					p.anchorArrivalVerdict(view, encKey(700000+i), types.BuildTS(int64(200+i), 0)),
					"departure %d grounds at its own tombstone reference", i)
			}
		}
		runVerdicts()
		builds, scans := p.arrivalEvidenceStats()
		require.Equal(t, uint64(1), builds, "arrival evidence is built once")
		require.LessOrEqual(t, scans, uint64(6*KC),
			"grounding must not rescan the hot key's tombstone history per departure (~%d)",
			KC*KC)
		scansBefore := scans
		runVerdicts()
		builds, scans = p.arrivalEvidenceStats()
		require.Equal(t, uint64(1), builds)
		require.Equal(t, scansBefore, scans, "a reused index performs no further scans")
	})

	t.Run("many lineages sharing one temporary key in one transaction", func(t *testing.T) {
		// KS lineages each move origin_i -> scratch -> final_i inside ONE
		// transaction (one durable txn id, one commit instant), and two
		// further lineages do the same WITHOUT a recorded transaction id
		// at the same instant. Every predecessor lookup at the shared
		// scratch key must consult only its own scope's exact replacement
		// identity: scanning the whole transaction bucket — plus the whole
		// timestamp bucket to re-filter the identity-less lane — per
		// chain costs ~2*KS*(KS+2) comparisons here (r55 review F1).
		const KS = 160
		scratch := encKey(500001)
		var txnID [catalog.DataBranchUpdateLinkTxnIDBytes]byte
		txnID[0] = 0x5A
		const T0 = 900
		var links []pkMoveLink
		var entries []pkMoveWalkTestRow
		mkLineage := func(i int, withTxn bool) {
			a := pkMoveWalkTestLink(0, 0, 0, 0, T0) // origin_i -> scratch
			a.oldPK, a.newPK = encKey(200000+i), scratch
			a.oldRowID, a.newRowID = ridOf(31, i), ridOf(32, i)
			b := pkMoveWalkTestLink(0, 0, 0, 0, T0) // scratch -> final_i
			b.oldPK, b.newPK = scratch, encKey(300000+i)
			b.oldRowID, b.newRowID = ridOf(32, i), ridOf(33, i)
			if withTxn {
				a.hasTxnID, a.txnID = true, txnID
				b.hasTxnID, b.txnID = true, txnID
			}
			links = append(links, a, b)
			entries = append(entries, pkMoveWalkTestRow{
				key: encKey(200000 + i), rowID: ridOf(31, i),
				birth: types.BuildTS(T0, 0), tombRow: true, // grounds a_i
			})
		}
		for i := 0; i < KS; i++ {
			mkLineage(i, true)
		}
		for i := KS; i < KS+2; i++ {
			mkLineage(i, false) // unknown-transaction lanes at the same instant
		}
		p := pkMoveWalkTestProvider(links...)
		view := pkMoveWalkTestStubView(entries...)
		birth := types.BuildTS(T0, 0)
		runVerdicts := func() {
			for _, i := range []int{0, 1, KS / 2, KS - 1, KS, KS + 1} {
				require.Equal(t, pkMoveArrivalRecorded,
					p.anchorArrivalVerdict(view, encKey(300000+i), birth),
					"lineage %d grounds at its origin's committed tombstone", i)
			}
		}
		runVerdicts()
		builds, scans := p.arrivalEvidenceStats()
		require.Equal(t, uint64(1), builds, "arrival evidence is built once")
		require.LessOrEqual(t, scans, uint64(16*(KS+2)),
			"predecessor lookups must bind by exact identity within a scope (pre-fix ~%d scans)",
			2*KS*(KS+2))
		scansBefore := scans
		runVerdicts()
		builds, scans = p.arrivalEvidenceStats()
		require.Equal(t, uint64(1), builds)
		require.Equal(t, scansBefore, scans, "a reused index performs no further scans")
	})
}

// TestResolvePkMoveChainScalesLinearly pins the COMPLETE forward chain
// walk's operation counts through resolvePkMoveChainVerdict — exactly the
// per-candidate fan-out resolveAndBuildPkMoveFolds performs for every
// destination tombstone (r55 review F2). Departure successor lookups must
// serve one scratch key's many chains without rescanning its whole
// departure bucket per chain, and anchor-death / previous-death evidence
// must serve many anchors standing on one hot key without rescanning its
// tombstone history per anchor. Pre-fix each shape cost ~N^2 link or
// tombstone examinations.
func TestResolvePkMoveChainScalesLinearly(t *testing.T) {
	encKey := func(i int) []byte {
		return []byte{byte(i >> 16), byte(i >> 8), byte(i)}
	}
	ridOf := func(tag, i int) types.Rowid {
		var rid types.Rowid
		rid[0] = byte(i)
		rid[1] = byte(i >> 8)
		rid[2] = byte(tag)
		return rid
	}

	t.Run("many chains through one shared scratch key", func(t *testing.T) {
		// N two-hop lineages origin_i -> scratch -> final_i in N
		// transactions; the workspace identity staged at scratch pins
		// each chain's continuation. Walking chain i must examine chain
		// i's OWN departure at scratch, not all N departures (pre-fix
		// successorOf scanned byPK[scratch] per hop: N*N total).
		const N = 128
		scratch := encKey(500002)
		var links []pkMoveLink
		var entries []pkMoveWalkTestRow
		for i := 0; i < N; i++ {
			ts := uint64(1000 + i)  // moving transaction's write timestamp
			cts := uint64(1500 + i) // anchor's deleting commit instant
			ws := pkMoveWorkspaceRid(1, uint32(i+1))
			a := pkMoveWalkTestLink(0, 0, 0, 0, ts) // origin_i -> scratch
			a.oldPK, a.newPK = encKey(400000+i), scratch
			a.oldRowID, a.newRowID = ridOf(11, i), ws
			b := pkMoveWalkTestLink(0, 0, 0, 0, ts) // scratch -> final_i
			b.oldPK, b.newPK = scratch, encKey(410000+i)
			b.oldRowID, b.newRowID = ws, ridOf(13, i)
			links = append(links, a, b)
			entries = append(entries,
				pkMoveWalkTestRow{ // the anchor's deleting tombstone
					key: encKey(400000 + i), rowID: ridOf(11, i),
					birth: types.BuildTS(int64(cts), 0), tombRow: true,
				},
				pkMoveWalkTestRow{ // the lineage's landed claim row
					key:   encKey(410000 + i),
					row:   []byte{byte(i >> 8), byte(i), 0x13},
					rowID: ridOf(13, i),
					birth: types.BuildTS(int64(cts), 0),
				},
			)
		}
		p := pkMoveWalkTestProvider(links...)
		view := pkMoveWalkTestStubView(entries...)
		runWalks := func() {
			for i := 0; i < N; i++ {
				finalPK, finalRow, verdict := p.resolvePkMoveChainVerdict(
					view, encKey(400000+i), ridOf(11, i))
				require.Equal(t, pkMoveResolved, verdict, "chain %d resolves", i)
				require.Equal(t, encKey(410000+i), finalPK, "chain %d lands at its own goal", i)
				require.Equal(t, []byte{byte(i >> 8), byte(i), 0x13}, finalRow,
					"chain %d claims its own landed image", i)
			}
		}
		runWalks()
		depBuilds, depScans := p.departureEvidenceStats()
		tombBuilds, tombScans := view.tombEvidenceStats()
		require.LessOrEqual(t, depBuilds, uint64(N+2),
			"departure evidence is indexed once per consulted key")
		require.LessOrEqual(t, depScans, uint64(8*N),
			"each chain must consult only its own departure at the shared key (pre-fix ~%d)", N*N)
		require.LessOrEqual(t, tombBuilds, uint64(2*N+2),
			"tombstone evidence is indexed once per consulted key")
		require.LessOrEqual(t, tombScans, uint64(5*N),
			"anchor evidence stays per-key linear across %d chains", N)
		runWalks()
		depBuilds2, depScans2 := p.departureEvidenceStats()
		tombBuilds2, tombScans2 := view.tombEvidenceStats()
		require.Equal(t, depBuilds, depBuilds2, "cached departure indexes are reused")
		require.Equal(t, tombBuilds, tombBuilds2, "cached tombstone indexes are reused")
		require.LessOrEqual(t, depScans2-depScans, uint64(8*N),
			"a second pass serves indexed answers, not bucket rescans")
		require.LessOrEqual(t, tombScans2-tombScans, uint64(5*N),
			"a second pass serves indexed answers, not history rescans")
	})

	t.Run("many anchors on one hot key", func(t *testing.T) {
		// K committed generations of one hot key X each depart to their
		// own goal: anchor i's walk must consult ONLY version i's death
		// record out of X's K-tombstone history. Pre-fix the walk
		// rescanned the whole bucket for the anchor death AND again for
		// the previous-death continuity check (2K per anchor, ~2*K*K
		// total) — the repeated-committed-departures shape
		// resolveAndBuildPkMoveFolds hits for every candidate.
		const K = 128
		hot := encKey(500003)
		var links []pkMoveLink
		var entries []pkMoveWalkTestRow
		for i := 0; i < K; i++ {
			ts := uint64(2000 + i) // version v_i's deleting commit instant t_i
			goal := encKey(420000 + i)
			a := pkMoveWalkTestLink(0, 0, 0, 0, ts) // X -> goal_i
			a.oldPK, a.newPK = hot, goal
			a.oldRowID, a.newRowID = ridOf(21, i), ridOf(23, i)
			links = append(links, a)
			entries = append(entries,
				pkMoveWalkTestRow{ // tombstone deleting version v_i at t_i
					key: hot, rowID: ridOf(21, i),
					birth: types.BuildTS(int64(ts), 0), tombRow: true,
				},
				pkMoveWalkTestRow{ // version v_i was born at t_{i-1}
					rowID:       ridOf(21, i),
					birth:       types.BuildTS(int64(ts-1), 0),
					death:       types.BuildTS(int64(ts), 0),
					departedRow: true,
				},
				pkMoveWalkTestRow{ // landed claim at goal_i, born at t_i
					key:   goal,
					row:   []byte{byte(i >> 8), byte(i), 0x23},
					rowID: ridOf(23, i),
					birth: types.BuildTS(int64(ts), 0),
				},
			)
		}
		p := pkMoveWalkTestProvider(links...)
		view := pkMoveWalkTestStubView(entries...)
		runWalks := func() {
			for i := 0; i < K; i++ {
				finalPK, _, verdict := p.resolvePkMoveChainVerdict(view, hot, ridOf(21, i))
				require.Equal(t, pkMoveResolved, verdict,
					"anchor %d descends from the prior generation and resolves", i)
				require.Equal(t, encKey(420000+i), finalPK, "anchor %d lands at its own goal", i)
			}
		}
		runWalks()
		tombBuilds, tombScans := view.tombEvidenceStats()
		_, depScans := p.departureEvidenceStats()
		require.LessOrEqual(t, tombBuilds, uint64(K+1),
			"tombstone evidence is indexed once per consulted key")
		require.LessOrEqual(t, tombScans, uint64(6*K),
			"anchor evidence must not rescan the hot key per anchor (pre-fix ~%d)", 2*K*K)
		require.LessOrEqual(t, depScans, uint64(4*K), "goal-key departure lookups stay linear")
		runWalks()
		tombBuilds2, tombScans2 := view.tombEvidenceStats()
		require.Equal(t, tombBuilds, tombBuilds2, "cached tombstone indexes are reused")
		require.LessOrEqual(t, tombScans2-tombScans, uint64(6*K),
			"a second pass serves indexed answers, not history rescans")
	})

	t.Run("one workspace identity shared across distinct durable transactions", func(t *testing.T) {
		// N two-hop lineages origin_i -> scratch -> final_i in N
		// DISTINCT transactions carrying durable transaction identities,
		// plus 2 unknown-transaction lineages, all staging the SAME
		// workspace identity at scratch: workspace row ids restart per
		// transaction (NewTxnWorkSpace pins the workspace segment and
		// batchAllocNewRowIds assigns offsets from zero), so this is the
		// normal allocation shape for same-shaped moves. Walking chain i
		// must bind continuation i by (scope, row identity) and examine
		// ONLY its own namesake (pre-fix successorOf grouped departures
		// by bare row id and scanned all KS namesakes per hop: KS^2
		// total — r56 review F1). Existing transaction-identity collision
		// tests establish these namesakes are valid inputs.
		const N = 128
		const KS = N + 2
		scratch := encKey(500005)
		ws := pkMoveWorkspaceRid(1, 1)
		var links []pkMoveLink
		var entries []pkMoveWalkTestRow
		for i := 0; i < N; i++ {
			ts := uint64(1000 + i)
			cts := uint64(1500 + i)
			a := pkMoveTxnScopedLink(0, ridOf(31, i), 0, mustWorkspaceRowID(t, 0), ts, byte(i+1), true)
			a.oldPK, a.newPK = encKey(400000+i), scratch
			a.newRowID = ws
			b := pkMoveTxnScopedLink(0, mustWorkspaceRowID(t, 0), 0, ridOf(33, i), ts, byte(i+1), true)
			b.oldPK, b.newPK = scratch, encKey(410000+i)
			b.oldRowID = ws
			links = append(links, a, b)
			entries = append(entries,
				pkMoveWalkTestRow{ // the anchor's deleting tombstone
					key: encKey(400000 + i), rowID: ridOf(31, i),
					birth: types.BuildTS(int64(cts), 0), tombRow: true,
				},
				pkMoveWalkTestRow{ // the lineage's landed claim row
					key:   encKey(410000 + i),
					row:   []byte{byte(i >> 8), byte(i), 0x21},
					rowID: ridOf(33, i),
					birth: types.BuildTS(int64(cts), 0),
				},
			)
		}
		// Two UNKNOWN-transaction lineages share the same staged
		// identity; the unknown lane binds them by their own commit
		// timestamp without scanning the namesakes either.
		for u := 0; u < 2; u++ {
			ts := uint64(7000 + u)
			cts := uint64(7500 + u)
			a := pkMoveTxnScopedLink(0, ridOf(34, u), 0, mustWorkspaceRowID(t, 0), ts, 0, false)
			a.oldPK, a.newPK = encKey(440000+u), scratch
			a.newRowID = ws
			b := pkMoveTxnScopedLink(0, mustWorkspaceRowID(t, 0), 0, ridOf(35, u), ts, 0, false)
			b.oldPK, b.newPK = scratch, encKey(450000+u)
			b.oldRowID = ws
			links = append(links, a, b)
			entries = append(entries,
				pkMoveWalkTestRow{
					key: encKey(440000 + u), rowID: ridOf(34, u),
					birth: types.BuildTS(int64(cts), 0), tombRow: true,
				},
				pkMoveWalkTestRow{
					key:   encKey(450000 + u),
					row:   []byte{0x77, byte(u), 0x23},
					rowID: ridOf(35, u),
					birth: types.BuildTS(int64(cts), 0),
				},
			)
		}
		p := pkMoveWalkTestProvider(links...)
		view := pkMoveWalkTestStubView(entries...)
		runWalks := func() {
			for i := 0; i < N; i++ {
				finalPK, finalRow, verdict := p.resolvePkMoveChainVerdict(
					view, encKey(400000+i), ridOf(31, i))
				require.Equal(t, pkMoveResolved, verdict, "chain %d resolves", i)
				require.Equal(t, encKey(410000+i), finalPK, "chain %d lands at its own goal", i)
				require.Equal(t, []byte{byte(i >> 8), byte(i), 0x21}, finalRow,
					"chain %d claims its own landed image", i)
			}
			for u := 0; u < 2; u++ {
				finalPK, finalRow, verdict := p.resolvePkMoveChainVerdict(
					view, encKey(440000+u), ridOf(34, u))
				require.Equal(t, pkMoveResolved, verdict, "unknown-txn chain %d resolves", u)
				require.Equal(t, encKey(450000+u), finalPK, "unknown-txn chain %d lands at its own goal", u)
				require.Equal(t, []byte{0x77, byte(u), 0x23}, finalRow,
					"unknown-txn chain %d claims its own landed image", u)
			}
		}
		runWalks()
		depBuilds, depScans := p.departureEvidenceStats()
		tombBuilds, tombScans := view.tombEvidenceStats()
		require.LessOrEqual(t, depBuilds, uint64(KS+2),
			"departure evidence is indexed once per consulted key")
		require.LessOrEqual(t, depScans, uint64(8*KS),
			"each chain examines only its own scoped namesake (pre-fix ~%d)", KS*KS)
		require.LessOrEqual(t, tombBuilds, uint64(2*KS+2),
			"tombstone evidence is indexed once per consulted key")
		require.LessOrEqual(t, tombScans, uint64(5*KS),
			"anchor evidence stays per-key linear across %d chains", KS)
		runWalks()
		depBuilds2, depScans2 := p.departureEvidenceStats()
		tombBuilds2, tombScans2 := view.tombEvidenceStats()
		require.Equal(t, depBuilds, depBuilds2, "cached departure indexes are reused")
		require.Equal(t, tombBuilds, tombBuilds2, "cached tombstone indexes are reused")
		require.LessOrEqual(t, depScans2-depScans, uint64(8*KS),
			"a second pass serves indexed answers, not namesake rescans")
		require.LessOrEqual(t, tombScans2-tombScans, uint64(5*KS),
			"a second pass serves indexed answers, not history rescans")
	})

	t.Run("many committed chains through a shared intermediate key", func(t *testing.T) {
		// N lineages origin_i -> scratch -> final_i whose intermediate
		// versions COMMITTED at scratch (txn A_i moved origin_i into
		// scratch, later txn B_i moved the committed intermediate on to
		// final_i): scratch's tombstone bucket holds one death record per
		// lineage. Every origin walk must pair ONLY its own intermediate
		// version's death — the pre-index loop scanned the bucket's N
		// references AND issued N successor lookups per walk, KS^2
		// reference examinations for N candidates, exactly the fan-out
		// resolveAndBuildPkMoveFolds performs per destination tombstone
		// (r56 review F2). Key-wide ambiguity must be computed once per
		// evidence generation; valid departures and tails must be indexed
		// by departed birth and commit instant.
		const N = 128
		scratch := encKey(500007)
		var links []pkMoveLink
		var entries []pkMoveWalkTestRow
		for i := 0; i < N; i++ {
			ctsA := uint64(3000 + i) // intermediate's birth == anchor's deleting instant
			ctsB := uint64(4000 + i) // intermediate's death == final's birth
			a := pkMoveTxnScopedLink(0, ridOf(41, i), 0, ridOf(43, i), ctsA, byte(2*i+1), true)
			a.oldPK, a.newPK = encKey(400000+i), scratch
			b := pkMoveTxnScopedLink(0, ridOf(43, i), 0, ridOf(45, i), ctsB, byte(2*i+2), true)
			b.oldPK, b.newPK = scratch, encKey(430000+i)
			links = append(links, a, b)
			entries = append(entries,
				pkMoveWalkTestRow{ // the anchor's deleting tombstone
					key: encKey(400000 + i), rowID: ridOf(41, i),
					birth: types.BuildTS(int64(ctsA), 0), tombRow: true,
				},
				pkMoveWalkTestRow{ // scratch's N-strong tombstone bucket
					key: scratch, rowID: ridOf(43, i),
					birth: types.BuildTS(int64(ctsB), 0), tombRow: true,
				},
				pkMoveWalkTestRow{ // the intermediate was born at ctsA
					rowID:       ridOf(43, i),
					birth:       types.BuildTS(int64(ctsA), 0),
					death:       types.BuildTS(int64(ctsB), 0),
					departedRow: true,
				},
				pkMoveWalkTestRow{ // the lineage's landed claim row
					key:   encKey(430000 + i),
					row:   []byte{byte(i >> 8), byte(i), 0x2d},
					rowID: ridOf(45, i),
					birth: types.BuildTS(int64(ctsB), 0),
				},
			)
		}
		p := pkMoveWalkTestProvider(links...)
		view := pkMoveWalkTestStubView(entries...)
		runWalks := func() {
			for i := 0; i < N; i++ {
				finalPK, finalRow, verdict := p.resolvePkMoveChainVerdict(
					view, encKey(400000+i), ridOf(41, i))
				require.Equal(t, pkMoveResolved, verdict, "chain %d resolves", i)
				require.Equal(t, encKey(430000+i), finalPK,
					"chain %d's pairing binds its OWN intermediate's departure", i)
				require.Equal(t, []byte{byte(i >> 8), byte(i), 0x2d}, finalRow,
					"chain %d claims its own landed image", i)
			}
		}
		runWalks()
		depBuilds, depScans := p.departureEvidenceStats()
		tombBuilds, tombScans := view.tombEvidenceStats()
		pairBuilds, pairScans := view.pairingEvidenceStats()
		require.LessOrEqual(t, pairBuilds, uint64(2),
			"key-wide pairing ambiguity is computed once per evidence generation (pre-fix %d)", N)
		require.LessOrEqual(t, pairScans, uint64(4*N),
			"each walk consults only its own departed-birth group (pre-fix ~%d)", N*N)
		require.LessOrEqual(t, depBuilds, uint64(N+2),
			"departure evidence is indexed once per consulted key")
		require.LessOrEqual(t, depScans, uint64(8*N),
			"successor counts are evaluated once per bucket reference (pre-fix ~%d)", N*N)
		require.LessOrEqual(t, tombBuilds, uint64(2*N+2),
			"tombstone evidence is indexed once per consulted key")
		require.LessOrEqual(t, tombScans, uint64(5*N),
			"anchor evidence stays per-key linear across %d chains", N)
		runWalks()
		depBuilds2, depScans2 := p.departureEvidenceStats()
		tombBuilds2, tombScans2 := view.tombEvidenceStats()
		pairBuilds2, pairScans2 := view.pairingEvidenceStats()
		require.Equal(t, pairBuilds, pairBuilds2, "cached pairing indexes are reused")
		require.Equal(t, depBuilds, depBuilds2, "cached departure indexes are reused")
		require.Equal(t, tombBuilds, tombBuilds2, "cached tombstone indexes are reused")
		require.LessOrEqual(t, pairScans2-pairScans, uint64(2*N),
			"a second pass consults the departed-birth group, not the bucket")
		require.Equal(t, uint64(0), depScans2-depScans,
			"a second pass reuses cached pairing without provider examinations")
		require.LessOrEqual(t, tombScans2-tombScans, uint64(5*N),
			"a second pass serves indexed answers, not history rescans")
	})
}

func TestPkMoveSuccessorOf(t *testing.T) {
	t.Run("inhabitant identity filters other rows' departures", func(t *testing.T) {
		mine := pkMoveWalkTestLink(3, 7, 4, 5, 20)
		theirs := pkMoveWalkTestLink(3, 8, 5, 6, 20)
		p := pkMoveWalkTestProvider(mine, theirs)
		succ, count, complete, err := p.successorOf([]byte{3}, mustRowID(t, 7), pkMoveTxnScope{})
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, 1, count)
		require.Equal(t, []byte{4}, succ.newPK)

		_, count, complete, err = p.successorOf([]byte{3}, mustRowID(t, 9), pkMoveTxnScope{})
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, 0, count)
	})

	t.Run("departure snapshots never gate the identity match", func(t *testing.T) {
		// Ordering belongs to the stream timestamps the walk checks
		// itself; the successor lookup is pure identity.
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(3, 7, 4, 5, 6))
		succ, count, complete, err := p.successorOf([]byte{3}, mustRowID(t, 7), pkMoveTxnScope{})
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, 1, count)
		require.Equal(t, []byte{4}, succ.newPK)
	})

	t.Run("two matching departures are ambiguous", func(t *testing.T) {
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(3, 7, 4, 5, 20),
			pkMoveWalkTestLink(3, 7, 5, 6, 20),
		)
		_, count, complete, err := p.successorOf([]byte{3}, mustRowID(t, 7), pkMoveTxnScope{})
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, 2, count)
	})

	t.Run("cross-transaction namesakes of a workspace identity are not successors", func(t *testing.T) {
		// Workspace row ids restart in every transaction: a second
		// transaction whose chain crosses the same key mints a link
		// naming the very same (key, raw old row id) pair. That namesake
		// must be excluded BEFORE counting, or every such collision
		// reports a phantom ambiguity and the walk fails closed.
		inhabitant := mustWorkspaceRowID(t, 7)
		mine := pkMoveLink{
			oldRowID:    inhabitant,
			newRowID:    mustWorkspaceRowID(t, 8),
			hasNewRowID: true,
			oldPK:       []byte{3},
			newPK:       []byte{4},
			writeTS:     types.BuildTS(100, 0),
		}
		namesake := mine
		namesake.writeTS = types.BuildTS(200, 0)
		namesake.newPK = []byte{9}
		p := pkMoveWalkTestProvider(mine, namesake)
		succ, count, complete, err := p.successorOf([]byte{3}, inhabitant, pkMoveTxnScope{writeTS: types.BuildTS(100, 0)})
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, 1, count)
		require.Equal(t, []byte{4}, succ.newPK)

		// Same-transaction duplicates still report ambiguity: the
		// namespace scopes counting, it does not blind it.
		alsoMine := mine
		alsoMine.newPK = []byte{5}
		p = pkMoveWalkTestProvider(mine, alsoMine)
		_, count, complete, err = p.successorOf([]byte{3}, inhabitant, pkMoveTxnScope{writeTS: types.BuildTS(100, 0)})
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, 2, count)
	})

	t.Run("durable inhabitants ignore the transaction namespace", func(t *testing.T) {
		// A committed row id is globally unique: any recorded departure
		// of it is evidence no matter which transaction wrote it.
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(3, 7, 4, 5, 20))
		succ, count, complete, err := p.successorOf([]byte{3}, mustRowID(t, 7), pkMoveTxnScope{writeTS: types.BuildTS(999, 0)})
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, 1, count)
		require.Equal(t, []byte{4}, succ.newPK)
	})

	t.Run("disabled provider reports incomplete evidence", func(t *testing.T) {
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(3, 7, 4, 5, 20))
		p.disable("test")
		_, count, complete, err := p.successorOf([]byte{3}, mustRowID(t, 7), pkMoveTxnScope{})
		require.NoError(t, err)
		require.False(t, complete)
		require.Equal(t, 0, count)

		// A disabled provider must not turn any evidence into a claim.
		p = pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 10))
		p.disable("test")
		_, _, ok := p.resolvePkMoveChain(pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveLiveRow(3, 0xdd, 7, 15),
		), []byte{2}, mustRowID(t, 1))
		require.False(t, ok)
	})

	t.Run("nil provider answers no successor with complete evidence", func(t *testing.T) {
		var p *pkMoveLinkProvider
		_, count, complete, err := p.successorOf([]byte{3}, mustRowID(t, 7), pkMoveTxnScope{})
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, 0, count)
	})

	t.Run("committed lookup crosses the transaction namespace", func(t *testing.T) {
		// A committed stream identity carries no write-timestamp
		// namespace: the continuation of a committed departure belongs to
		// a LATER transaction by construction and the raw workspace id of
		// the departed row may byte-collide with transaction-local
		// identities of that later transaction.
		wsA := mustWorkspaceRowID(t, 1)
		cross := pkMoveWalkTestLink(4, 0, 7, 0, 30)
		cross.oldRowID = wsA
		p := pkMoveWalkTestProvider(cross)

		// The transaction-scoped lookup must not see it...
		_, count, complete, err := p.successorOf([]byte{4}, wsA, pkMoveTxnScope{writeTS: types.BuildTS(10, 0)})
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, 0, count)

		// ...while the committed lookup does, exactly once.
		succ, count, complete, err := p.successorOfCommitted([]byte{4}, wsA)
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, 1, count)
		require.Equal(t, []byte{7}, succ.newPK)

		// Two committed links naming the same (key, row id) pair are
		// namesakes of one raw workspace id and stay ambiguous.
		fork2 := cross
		fork2.newPK = []byte{9}
		fork2.writeTS = types.BuildTS(40, 0)
		p = pkMoveWalkTestProvider(cross, fork2)
		_, count, complete, err = p.successorOfCommitted([]byte{4}, wsA)
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, 2, count)

		var nilP *pkMoveLinkProvider
		_, count, complete, err = nilP.successorOfCommitted([]byte{4}, wsA)
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, 0, count)
	})
}

// pkMoveTestTxnID builds one deterministic recording-transaction identity
// from a single seed byte.
func pkMoveTestTxnID(seed byte) (id [catalog.DataBranchUpdateLinkTxnIDBytes]byte) {
	for i := range id {
		id[i] = seed
	}
	return id
}

// pkMoveTxnScopedLink fabricates one link row whose recording-transaction
// identity is known (hasTxn) or left at the all-zero "identity unknown"
// form. Keys are single raw bytes; row ids come from the caller so both
// durable and workspace identities can collide deliberately.
func pkMoveTxnScopedLink(
	oldKey byte, oldRid types.Rowid, newKey byte, newRid types.Rowid,
	writeTS uint64, txnSeed byte, hasTxn bool,
) pkMoveLink {
	l := pkMoveLink{
		oldRowID:    oldRid,
		newRowID:    newRid,
		hasNewRowID: true,
		oldPK:       []byte{oldKey},
		newPK:       []byte{newKey},
		writeTS:     types.BuildTS(int64(writeTS), 0),
	}
	if hasTxn {
		l.txnID = pkMoveTestTxnID(txnSeed)
		l.hasTxnID = true
	}
	return l
}

func TestPkMoveConsumerTxnIdentityCollision(t *testing.T) {
	txnA := pkMoveTestTxnID(0xa1)
	txnB := pkMoveTestTxnID(0xb2)

	t.Run("distinct transaction identities tie-break a one-instant collision", func(t *testing.T) {
		// Two transactions created inside one physical instant share the
		// create timestamp (the HLC advances only its logical component),
		// and their workspaces mint the very same raw row ids. The
		// consumer must separate their departures by the durable
		// recording-transaction identity: drop that comparison and both
		// lookups report a phantom two-way ambiguity.
		ws7 := mustWorkspaceRowID(t, 7)
		mine := pkMoveTxnScopedLink(3, ws7, 4, mustWorkspaceRowID(t, 8), 100, 0xa1, true)
		namesake := pkMoveTxnScopedLink(3, ws7, 9, mustWorkspaceRowID(t, 9), 100, 0xb2, true)
		p := pkMoveWalkTestProvider(mine, namesake)

		scopeA := pkMoveTxnScope{writeTS: types.BuildTS(100, 0), txnID: txnA, hasTxn: true}
		succ, count, complete, err := p.successorOf([]byte{3}, ws7, scopeA)
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, 1, count)
		require.Equal(t, []byte{4}, succ.newPK)

		scopeB := pkMoveTxnScope{writeTS: types.BuildTS(100, 0), txnID: txnB, hasTxn: true}
		succ, count, complete, err = p.successorOf([]byte{3}, ws7, scopeB)
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, 1, count)
		require.Equal(t, []byte{9}, succ.newPK)

		// An identity-less scope cannot tie-break: two same-instant
		// departures of one workspace identity stay genuinely ambiguous.
		_, count, complete, err = p.successorOf([]byte{3}, ws7,
			pkMoveTxnScope{writeTS: types.BuildTS(100, 0)})
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, 2, count)
	})

	t.Run("identity-less links fall back to the create timestamp", func(t *testing.T) {
		// A link with the all-zero stored identity is "identity unknown":
		// matching uses the create timestamp regardless of the identity
		// the scope itself carries, so pre-txn-id rows and defensive
		// foreign-write fallbacks keep their old deterministic behavior.
		ws7 := mustWorkspaceRowID(t, 7)
		plain := pkMoveTxnScopedLink(3, ws7, 4, mustWorkspaceRowID(t, 8), 100, 0, false)
		other := pkMoveTxnScopedLink(3, ws7, 9, mustWorkspaceRowID(t, 9), 200, 0, false)
		p := pkMoveWalkTestProvider(plain, other)

		succ, count, complete, err := p.successorOf([]byte{3}, ws7,
			pkMoveTxnScope{writeTS: types.BuildTS(100, 0), txnID: txnA, hasTxn: true})
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, 1, count)
		require.Equal(t, []byte{4}, succ.newPK)

		// A DIFFERENT transaction's known identity excludes its link
		// affirmatively (identity evidence wins over the timestamp),
		// while the identity-less same-instant departure still matches
		// by timestamp: the scope sees exactly one candidate.
		known := pkMoveTxnScopedLink(3, ws7, 6, mustWorkspaceRowID(t, 6), 100, 0xc3, true)
		p = pkMoveWalkTestProvider(plain, known)
		succ, count, complete, err = p.successorOf([]byte{3}, ws7,
			pkMoveTxnScope{writeTS: types.BuildTS(100, 0), txnID: txnA, hasTxn: true})
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, 1, count)
		require.Equal(t, []byte{4}, succ.newPK)
	})

	t.Run("competing successors at one instant resolve by recording identity along the walk", func(t *testing.T) {
		// The chain's own second hop and a foreign same-instant namesake
		// depart the same key on the same raw workspace identity. The
		// walk must follow only the recording transaction's continuation;
		// timestamp-only matching turns the hop into a phantom ambiguity
		// and fails the whole chain closed.
		ws7 := mustWorkspaceRowID(t, 7)
		anchor := pkMoveTxnScopedLink(2, mustRowID(t, 1), 3, ws7, 15, 0xa1, true)
		hopMine := pkMoveTxnScopedLink(3, ws7, 5, mustRowID(t, 0), 15, 0xa1, true)
		hopMine.hasNewRowID = false
		hopForeign := pkMoveTxnScopedLink(3, ws7, 9, mustRowID(t, 0), 15, 0xb2, true)
		hopForeign.hasNewRowID = false
		p := pkMoveWalkTestProvider(anchor, hopMine, hopForeign)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveLiveRow(5, 0xee, 0x51, 15),
			pkMoveLiveRow(9, 0x99, 0x52, 15),
		)
		k, row, ok := p.resolvePkMoveChain(view, []byte{2}, mustRowID(t, 1))
		require.True(t, ok)
		require.Equal(t, []byte{5}, k)
		require.Equal(t, []byte{0xee}, row)
	})
}

// TestPkMoveResolveVerdictTaxonomy pins the F3 distinction the apply relies
// on: complete evidence with no update intent (or with an affirmatively
// foreign lineage) keeps the legacy DELETE+INSERT replay, while recorded
// or unprovable provenance resolves UNRESOLVED so the apply aborts instead
// of cascading a destructive delete onto dependents of a row that moved.
func TestPkMoveResolveVerdictTaxonomy(t *testing.T) {
	t.Run("complete evidence without an anchor link is proven absence", func(t *testing.T) {
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 9, 3, 7, 10))
		view := pkMoveWalkTestStubView(pkMoveTombRow(2, 0x01, 1, 15))
		// The tombstone names a row no link references: an independent
		// delete, proven by a complete probe space.
		_, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 1))
		require.Equal(t, pkMoveProvenNone, verdict)
		// The complete-empty provider proves absence the same way.
		p2 := pkMoveWalkTestProvider()
		_, _, verdict = p2.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 1))
		require.Equal(t, pkMoveProvenNone, verdict)
	})

	t.Run("affirmatively foreign reincarnation keeps delete semantics", func(t *testing.T) {
		// Same fixture as "anchor reincarnated by a later transaction
		// fails closed": the walk PROVES the moved lineage is not the
		// destination's lineage, so the delete's own semantics stand —
		// proven absence of a FOLD, not unresolvable provenance.
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 25))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 0, 12),
			pkMoveTombRow(2, 0x02, 1, 30),
			pkMoveDepartRow(1, 30, 20),
			pkMoveLiveRow(3, 0xdd, 42, 30),
		)
		_, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 1))
		require.Equal(t, pkMoveProvenNone, verdict)
	})

	t.Run("poisoned anchor evidence is unresolved", func(t *testing.T) {
		rid := mustRowID(t, 1)
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 10))
		p.poisoned[pkMoveLinkRowKey(rid, []byte{2})] = struct{}{}
		view := pkMoveWalkTestStubView(pkMoveTombRow(2, 0x01, 1, 15))
		_, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, rid)
		require.Equal(t, pkMoveUnresolved, verdict)
	})

	t.Run("disabled provider is unresolved, not absent", func(t *testing.T) {
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 10))
		p.disabled = true
		view := pkMoveWalkTestStubView(pkMoveTombRow(2, 0x01, 1, 15))
		_, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 1))
		require.Equal(t, pkMoveUnresolved, verdict)
	})

	t.Run("chain exactly at the hop bound still folds", func(t *testing.T) {
		// The boundary twin of "chain longer than the hop bound fails
		// closed": exactly pkMoveLinkMaxChain hops must resolve, so the
		// limit only ever aborts genuinely longer chains.
		var (
			links   []pkMoveLink
			entries []pkMoveWalkTestRow
		)
		entries = append(entries, pkMoveTombRow(10, 0x01, 1, 100))
		for i := 0; i < pkMoveLinkMaxChain; i++ {
			links = append(links, pkMoveWalkTestLink(
				byte(10+i), byte(i+1), byte(11+i), byte(i+2), uint64(10+i),
			))
			if i == pkMoveLinkMaxChain-1 {
				continue
			}
			entries = append(entries,
				pkMoveTombRow(byte(11+i), byte(i+2), byte(i+2), uint64(101+i)),
				pkMoveDepartRow(byte(i+2), uint64(101+i), uint64(100+i)),
			)
		}
		// The 32nd move lands at the last key with no further departure:
		// a live row born at its commit instant is the lineage end.
		entries = append(entries, pkMoveLiveRow(byte(10+pkMoveLinkMaxChain), 0xee,
			byte(pkMoveLinkMaxChain+1), uint64(100+pkMoveLinkMaxChain-1)))
		p := pkMoveWalkTestProvider(links...)
		view := pkMoveWalkTestStubView(entries...)
		finalPK, row, verdict := p.resolvePkMoveChainVerdict(view, []byte{10}, mustRowID(t, 1))
		require.Equal(t, pkMoveResolved, verdict)
		require.Equal(t, []byte{byte(10 + pkMoveLinkMaxChain)}, finalPK)
		require.Equal(t, []byte{0xee}, row)
	})

	t.Run("corrupt duplicate same-instant birth is unresolved", func(t *testing.T) {
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 10))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveLiveRow(3, 0xdd, 42, 15),
			pkMoveLiveRow(3, 0xde, 43, 15),
		)
		_, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 1))
		require.Equal(t, pkMoveUnresolved, verdict)
	})

	t.Run("recorded move terminated by a plain deletion keeps delete semantics", func(t *testing.T) {
		// Category contract of pkMoveProvenDeleted: recorded provenance
		// proves the lineage moved; complete stream evidence proves the
		// landed row then died with no successor and no rebirth — a
		// terminal deletion, categorically distinct from both an
		// independent delete (pkMoveProvenNone) and incomplete
		// provenance (pkMoveUnresolved).
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 10))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 25),
			pkMoveDepartRow(7, 25, 15),
		)
		_, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 1))
		require.Equal(t, pkMoveProvenDeleted, verdict)
	})

	t.Run("tail before a continuation does not leak into an unproven end", func(t *testing.T) {
		// 2->3, a stable-key rebirth at 3 (consumed as a tail), then a
		// further move 3->4 whose landed row has NO terminal evidence at
		// all. The earlier tail must not downgrade this to a proven
		// deletion: the lineage continued past it, and its final fate is
		// simply unknown.
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(2, 1, 3, 7, 10),
			pkMoveWalkTestLink(3, 42, 4, 5, 30),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
			pkMoveTombRow(3, 0x02, 7, 25),
			pkMoveDepartRow(7, 25, 15),
			pkMoveTombRow(3, 0x03, 42, 35),
			pkMoveDepartRow(42, 35, 25),
		)
		_, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 1))
		require.Equal(t, pkMoveUnresolved, verdict)
	})

	t.Run("landed row out of the evidence window is not a proven deletion", func(t *testing.T) {
		// The lineage moved 2->3 and the window shows nothing further
		// about the landed row — no death, no rebirth, nothing. Without
		// a consumed terminal death there is no deletion PROOF, so the
		// verdict stays unresolved (the apply aborts rather than guess).
		link := pkMoveWalkTestLink(2, 1, 3, 7, 10)
		link.hasNewRowID = false
		p := pkMoveWalkTestProvider(link)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(2, 0x01, 1, 15),
		)
		_, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{2}, mustRowID(t, 1))
		require.Equal(t, pkMoveUnresolved, verdict)
	})
}

// TestResolveAndBuildPkMoveFoldsAbortsOnUnresolved pins the apply-level
// contract of the F3 taxonomy behind its F4 relevance gate: a candidate
// whose recorded provenance cannot be proven aborts the whole fold build
// loudly (statement-atomic — no partial apply) only while the
// destination lineage actually holds its old key at the LCA instant.
// Proven absence still degrades to the legacy DELETE+INSERT replay, and a
// lineage the destination never held (or one admitted only as a PICK
// dependency key) never reaches the verdict walk at all: its stream rows
// keep legacy meaning and its provenance, provable or not, cannot abort
// an otherwise empty merge.
func TestResolveAndBuildPkMoveFoldsAbortsOnUnresolved(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	mp := ses.proc.Mp()

	anchorRid := buildHashDiffRowID(t, 71)
	key2 := pkMoveTestKey(t, 2)
	key3 := pkMoveTestKey(t, 3)
	branchTS := types.BuildTS(10, 0).ToTimestamp()

	// swapLCAProbe replaces the anchor-vs-LCA relevance probe with a
	// fixture: held lists the old keys the destination lineage still
	// holds at the LCA instant. The batch comes from tblStuff's ret pool
	// so the caller's release path type-checks the fixture exactly the
	// way the production probe's result does.
	swapLCAProbe := func(t *testing.T, tblStuff tableStuff, held ...int64) {
		t.Helper()
		prev := pkMoveLCAAnchorProbe
		pkMoveLCAAnchorProbe = func(
			_ context.Context, probeSes *Session, _ BackgroundExec,
			_ *batch.Batch, probeStuff tableStuff, _ timestamp.Timestamp,
		) (*batch.Batch, error) {
			dBat := probeStuff.retPool.acquireRetBatch(probeStuff, false)
			for _, pk := range held {
				require.NoError(t, vector.AppendFixed(dBat.Vecs[0], pk, false, probeSes.proc.Mp()))
				require.NoError(t, vector.AppendBytes(dBat.Vecs[1], []byte("lca"), false, probeSes.proc.Mp()))
				require.NoError(t, vector.AppendBytes(dBat.Vecs[2], []byte("h"), false, probeSes.proc.Mp()))
			}
			dBat.SetRowCount(len(held))
			return dBat, nil
		}
		t.Cleanup(func() { pkMoveLCAAnchorProbe = prev })
	}

	// newInput builds the stream side of one anchor tombstone at key2:
	// the tombstone index the production capture would return, plus the
	// tombstone hashmap whose DecodeRow feeds the anchor-probe batch.
	newInput := func(t *testing.T) (*pkMoveHashmapView, databranchutils.BranchHashmap) {
		tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, []pkMoveStreamRow{
			{rowID: anchorRid, id: 2, birth: types.BuildTS(15, 0)},
		})
		view := &pkMoveHashmapView{
			tombs: map[string][]pkMoveTombRef{
				string(key2): {{rowID: anchorRid, cts: types.BuildTS(15, 0)}},
			},
		}
		t.Cleanup(func() { require.NoError(t, tombstoneHashmap.Close()) })
		return view, tombstoneHashmap
	}

	t.Run("unresolved provenance aborts the apply", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		// Any apply-side SQL would fail this mock: the abort precedes
		// every fold emission.
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		tblStuff := newTestBranchTableStuff(ctrl)
		swapLCAProbe(t, tblStuff, 2)

		p := pkMoveWalkTestProvider(pkMoveLink{
			oldRowID: anchorRid, newRowID: buildHashDiffRowID(t, 72), hasNewRowID: true,
			oldPK: key2, newPK: key3, writeTS: types.BuildTS(10, 0),
		})
		// Poisoned evidence makes the recorded intent unprovable.
		p.poisoned[pkMoveLinkRowKey(anchorRid, key2)] = struct{}{}

		view, tombstoneHashmap := newInput(t)
		_, err := resolveAndBuildPkMoveFolds(
			ctx, ses, bh, tblStuff, diffSideBase,
			p, view, tombstoneHashmap, newPkMoveClaimedKeys(), branchTS,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "could not be resolved completely")
	})

	t.Run("proven absence degrades to legacy replay", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		tblStuff := newTestBranchTableStuff(ctrl)
		swapLCAProbe(t, tblStuff, 2)

		// The recorded link names another row entirely: complete evidence
		// with no move intent for this tombstone row.
		p := pkMoveWalkTestProvider(pkMoveLink{
			oldRowID: buildHashDiffRowID(t, 79), newRowID: buildHashDiffRowID(t, 80), hasNewRowID: true,
			oldPK: key2, newPK: key3, writeTS: types.BuildTS(10, 0),
		})
		view, tombstoneHashmap := newInput(t)
		plan, err := resolveAndBuildPkMoveFolds(
			ctx, ses, bh, tblStuff, diffSideBase,
			p, view, tombstoneHashmap, newPkMoveClaimedKeys(), branchTS,
		)
		require.NoError(t, err)
		require.Nil(t, plan)
	})

	t.Run("source-only lineage never reaches the verdict walk", func(t *testing.T) {
		// F4: a lineage born, moved, and deleted entirely on the source
		// branch leaves a pending tombstone whose lineage the destination
		// does not hold at the LCA instant. Its net diff is empty and its
		// replay needs no cascade, so relevance (LCA membership) gates
		// the verdict walk: even unprovable recorded provenance must not
		// abort an otherwise empty merge.
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		tblStuff := newTestBranchTableStuff(ctrl)
		// The LCA snapshot holds no row at key 2.
		swapLCAProbe(t, tblStuff)

		p := pkMoveWalkTestProvider(pkMoveLink{
			oldRowID: anchorRid, newRowID: buildHashDiffRowID(t, 72), hasNewRowID: true,
			oldPK: key2, newPK: key3, writeTS: types.BuildTS(10, 0),
		})
		// Would be unresolved if the walk ever ran.
		p.poisoned[pkMoveLinkRowKey(anchorRid, key2)] = struct{}{}

		view, tombstoneHashmap := newInput(t)
		plan, err := resolveAndBuildPkMoveFolds(
			ctx, ses, bh, tblStuff, diffSideBase,
			p, view, tombstoneHashmap, newPkMoveClaimedKeys(), branchTS,
		)
		require.NoError(t, err)
		require.Nil(t, plan)
	})

	t.Run("PICK dependency keys never reach the verdict walk", func(t *testing.T) {
		// F4's twin gate: dependency keys are admitted only to reassemble
		// a picked lineage; an unrelated anchor at such a key must not be
		// folded, and must not count as unresolved either.
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		tblStuff := newTestBranchTableStuff(ctrl)
		tblStuff.pickDepKeys = map[string]struct{}{string(key2): {}}
		swapLCAProbe(t, tblStuff, 2)

		p := pkMoveWalkTestProvider(pkMoveLink{
			oldRowID: anchorRid, newRowID: buildHashDiffRowID(t, 72), hasNewRowID: true,
			oldPK: key2, newPK: key3, writeTS: types.BuildTS(10, 0),
		})
		p.poisoned[pkMoveLinkRowKey(anchorRid, key2)] = struct{}{}

		view, tombstoneHashmap := newInput(t)
		plan, err := resolveAndBuildPkMoveFolds(
			ctx, ses, bh, tblStuff, diffSideBase,
			p, view, tombstoneHashmap, newPkMoveClaimedKeys(), branchTS,
		)
		require.NoError(t, err)
		require.Nil(t, plan)
	})
}

// TestResolveAndBuildPkMoveFoldsCappedEvidence pins the F1 fail-closed
// contract of a budget-capped tombstone capture: an empty index after a
// cap is dropped evidence, not a quiet window. The apply may keep the
// legacy replay only while the recorded provenance space within the
// captured windows is PROVEN empty (a fully scanned window cache entry or
// a quiet catalog probe). Any recorded link or any provider that can no
// longer disprove existence aborts the apply statement-atomically:
// replaying plain DELETE+INSERT could cascade a declared delete action
// onto dependents of a row that actually MOVED.
func TestResolveAndBuildPkMoveFoldsCappedEvidence(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	branchTS := types.BuildTS(42, 0).ToTimestamp()

	newView := func() *pkMoveHashmapView {
		return &pkMoveHashmapView{
			capped: true,
			tombs:  map[string][]pkMoveTombRef{},
		}
	}

	t.Run("cached links in window abort the apply", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		// Cached evidence answers existence directly: no SQL fires.
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		tblStuff := newTestBranchTableStuff(ctrl)
		p := pkMoveWalkTestProvider(pkMoveWalkTestLink(2, 1, 3, 7, 10))
		_, err := resolveAndBuildPkMoveFolds(
			ctx, ses, bh, tblStuff, diffSideBase, p, newView(), nil, nil, branchTS,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "tombstone capture exceeded its entry budget")
	})

	t.Run("fully scanned quiet window keeps legacy replay", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		// Any Exec would fail this mock: the full-window mark must answer
		// existence without a catalog round trip.
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		tblStuff := newTestBranchTableStuff(ctrl)
		window := pkMoveLinkTableWindow{tableID: 88, startPhys: 500, endPhys: 777}
		p := newPkMoveLinkProvider(ctx, ses, bh, []pkMoveLinkTableWindow{window})
		require.NotNil(t, p)
		p.fullWindows[window.tableID] = [2]uint64{window.startPhys, window.endPhys}
		plan, err := resolveAndBuildPkMoveFolds(
			ctx, ses, bh, tblStuff, diffSideBase, p, newView(), nil, nil, branchTS,
		)
		require.NoError(t, err)
		require.Nil(t, plan)
	})

	t.Run("uncovered window probes the catalog and aborts on links", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		tblStuff := newTestBranchTableStuff(ctrl)
		window := pkMoveLinkTableWindow{tableID: 88, startPhys: 500, endPhys: 777}

		var queries []string
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, sql string) error {
				queries = append(queries, sql)
				return nil
			},
		).Times(1)
		oneRow := &MysqlResultSet{}
		col := &MysqlColumn{}
		col.SetName(catalog.DataBranchUpdateLinkColTableID)
		col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
		oneRow.AddColumn(col)
		oneRow.AddRow([]interface{}{int64(window.tableID)})
		bh.EXPECT().GetExecResultSet().Return([]interface{}{oneRow}).Times(1)
		bh.EXPECT().ClearExecResultSet().Times(1)

		p := newPkMoveLinkProvider(ctx, ses, bh, []pkMoveLinkTableWindow{window})
		require.NotNil(t, p)
		_, err := resolveAndBuildPkMoveFolds(
			ctx, ses, bh, tblStuff, diffSideBase, p, newView(), nil, nil, branchTS,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "tombstone capture exceeded its entry budget")
		require.Len(t, queries, 1)
		require.Contains(t, queries[0], catalog.MO_DATA_BRANCH_UPDATE_LINK)
		require.Contains(t, queries[0], "limit 1")
	})

	t.Run("uncovered quiet window keeps legacy replay", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		tblStuff := newTestBranchTableStuff(ctrl)
		window := pkMoveLinkTableWindow{tableID: 88, startPhys: 500, endPhys: 777}

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).Times(1)
		empty := &MysqlResultSet{}
		col := &MysqlColumn{}
		col.SetName(catalog.DataBranchUpdateLinkColTableID)
		col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
		empty.AddColumn(col)
		bh.EXPECT().GetExecResultSet().Return([]interface{}{empty}).Times(1)
		bh.EXPECT().ClearExecResultSet().Times(1)

		p := newPkMoveLinkProvider(ctx, ses, bh, []pkMoveLinkTableWindow{window})
		require.NotNil(t, p)
		plan, err := resolveAndBuildPkMoveFolds(
			ctx, ses, bh, tblStuff, diffSideBase, p, newView(), nil, nil, branchTS,
		)
		require.NoError(t, err)
		require.Nil(t, plan)
	})

	t.Run("disabled provider fails closed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		tblStuff := newTestBranchTableStuff(ctrl)
		p := pkMoveWalkTestProvider()
		p.disable("simulated probe failure")
		_, err := resolveAndBuildPkMoveFolds(
			ctx, ses, bh, tblStuff, diffSideBase, p, newView(), nil, nil, branchTS,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "tombstone capture exceeded its entry budget")
	})
}

func TestPkMoveLinkDecodeRow(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()

	buildCols := func(t *testing.T, oldRid, newRid, oldPK, newPK, txnID []byte, nullNewRid bool, writeTS, tableID uint64) []*vector.Vector {
		t.Helper()
		oldRidVec := vector.NewVec(types.T_varbinary.ToType())
		newRidVec := vector.NewVec(types.T_varbinary.ToType())
		oldPKVec := vector.NewVec(types.T_varbinary.ToType())
		newPKVec := vector.NewVec(types.T_varbinary.ToType())
		writeTSVec := vector.NewVec(types.T_uint64.ToType())
		txnIDVec := vector.NewVec(types.T_binary.ToType())
		tableIDVec := vector.NewVec(types.T_uint64.ToType())
		require.NoError(t, vector.AppendBytes(oldRidVec, oldRid, false, mp))
		require.NoError(t, vector.AppendBytes(newRidVec, newRid, nullNewRid, mp))
		require.NoError(t, vector.AppendBytes(oldPKVec, oldPK, false, mp))
		require.NoError(t, vector.AppendBytes(newPKVec, newPK, false, mp))
		require.NoError(t, vector.AppendFixed(writeTSVec, writeTS, false, mp))
		require.NoError(t, vector.AppendBytes(txnIDVec, txnID, false, mp))
		require.NoError(t, vector.AppendFixed(tableIDVec, tableID, false, mp))
		return []*vector.Vector{oldRidVec, newRidVec, oldPKVec, newPKVec, writeTSVec, txnIDVec, tableIDVec}
	}

	rid := buildHashDiffRowID(t, 3)
	p := pkMoveWalkTestProvider()
	pkMoveTestTxnID := bytes.Repeat([]byte{0xab}, catalog.DataBranchUpdateLinkTxnIDBytes)

	t.Run("full identity row decodes", func(t *testing.T) {
		newRid := buildHashDiffRowID(t, 4)
		cols := buildCols(t, rid[:], newRid[:], []byte{9}, []byte{10}, pkMoveTestTxnID, false, 77, 88)
		defer func() {
			for _, c := range cols {
				c.Free(mp)
			}
		}()
		var loadErr error
		link, ok := p.decodeRow(cols, 0, &loadErr)
		require.True(t, ok)
		require.NoError(t, loadErr)
		require.Equal(t, rid, link.oldRowID)
		require.True(t, link.hasNewRowID)
		require.Equal(t, newRid, link.newRowID)
		require.Equal(t, []byte{9}, link.oldPK)
		require.Equal(t, []byte{10}, link.newPK)
		wantTS := types.BuildTS(77, 0)
		require.True(t, link.writeTS.EQ(&wantTS))
		require.True(t, link.hasTxnID)
		require.Equal(t, pkMoveTestTxnID, link.txnID[:])
	})

	t.Run("NULL new row id decodes as unrecorded identity", func(t *testing.T) {
		cols := buildCols(t, rid[:], nil, []byte{9}, []byte{10}, pkMoveTestTxnID, true, 77, 88)
		defer func() {
			for _, c := range cols {
				c.Free(mp)
			}
		}()
		var loadErr error
		link, ok := p.decodeRow(cols, 0, &loadErr)
		require.True(t, ok)
		require.NoError(t, loadErr)
		require.False(t, link.hasNewRowID)
	})

	t.Run("bad row id widths load as errors", func(t *testing.T) {
		cols := buildCols(t, []byte{1, 2, 3}, nil, []byte{9}, []byte{10}, pkMoveTestTxnID, true, 77, 88)
		defer func() {
			for _, c := range cols {
				c.Free(mp)
			}
		}()
		var loadErr error
		_, ok := p.decodeRow(cols, 0, &loadErr)
		require.False(t, ok)
		require.Error(t, loadErr)

		newRid := buildHashDiffRowID(t, 4)
		cols2 := buildCols(t, rid[:], newRid[:9], []byte{9}, []byte{10}, pkMoveTestTxnID, false, 77, 88)
		defer func() {
			for _, c := range cols2 {
				c.Free(mp)
			}
		}()
		loadErr = nil
		_, ok = p.decodeRow(cols2, 0, &loadErr)
		require.False(t, ok)
		require.Error(t, loadErr)
	})

	t.Run("all-zero txn id decodes as identity unknown", func(t *testing.T) {
		zeroTxn := make([]byte, catalog.DataBranchUpdateLinkTxnIDBytes)
		cols := buildCols(t, rid[:], nil, []byte{9}, []byte{10}, zeroTxn, true, 77, 88)
		defer func() {
			for _, c := range cols {
				c.Free(mp)
			}
		}()
		var loadErr error
		link, ok := p.decodeRow(cols, 0, &loadErr)
		require.True(t, ok)
		require.NoError(t, loadErr)
		require.False(t, link.hasTxnID)
	})

	t.Run("bad txn id width loads as error", func(t *testing.T) {
		cols := buildCols(t, rid[:], nil, []byte{9}, []byte{10}, []byte{1, 2}, true, 77, 88)
		defer func() {
			for _, c := range cols {
				c.Free(mp)
			}
		}()
		var loadErr error
		_, ok := p.decodeRow(cols, 0, &loadErr)
		require.False(t, ok)
		require.Error(t, loadErr)
	})
}

// buildPkMoveLinkProbeResultSet renders fabricated link rows in the probe
// projection (old_row_id, new_row_id, old_pk, new_pk, write_ts, txn_id,
// table_id) for the mock-BackgroundExec SQL path.
func buildPkMoveLinkProbeResultSet(rows ...[]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	colDefs := []struct {
		name string
		typ  defines.MysqlType
	}{
		{name: "old_row_id", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "new_row_id", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "old_pk", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "new_pk", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "write_ts", typ: defines.MYSQL_TYPE_LONGLONG},
		{name: "txn_id", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "table_id", typ: defines.MYSQL_TYPE_LONGLONG},
	}
	for _, def := range colDefs {
		mysqlCol := &MysqlColumn{}
		mysqlCol.SetName(def.name)
		mysqlCol.SetColumnType(def.typ)
		mrs.AddColumn(mysqlCol)
	}
	for _, row := range rows {
		mrs.AddRow(row)
	}
	return mrs
}

// pkMoveProbeRow renders one link row for buildPkMoveLinkProbeResultSet.
func pkMoveProbeRow(oldRid types.Rowid, newRid *types.Rowid, oldPK, newPK []byte, writeTS uint64) []interface{} {
	var newRidCell interface{}
	if newRid != nil {
		newRidCell = string(newRid[:])
	}
	return []interface{}{
		string(oldRid[:]), newRidCell, string(oldPK), string(newPK),
		int64(writeTS),
		string(bytes.Repeat([]byte{0xcd}, catalog.DataBranchUpdateLinkTxnIDBytes)),
		int64(88),
	}
}

func TestPkMoveLinkProbeBudgetAndBounds(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)

	oldCap := pkMoveLinkMaxCachedRows
	pkMoveLinkMaxCachedRows = 5
	defer func() { pkMoveLinkMaxCachedRows = oldCap }()

	window := pkMoveLinkTableWindow{tableID: 88, endPhys: 777}

	t.Run("probe SQL carries the write_ts window bound and the budget limit", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		rid1, rid2 := buildHashDiffRowID(t, 1), buildHashDiffRowID(t, 2)
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, sql string) error {
				require.NotContains(t, sql, "write_ts >=")
				require.Contains(t, sql, "write_ts <= 777")
				require.Contains(t, sql, "limit 6")
				require.Contains(t, sql, "order by link_id")
				return nil
			},
		).Times(1)
		bh.EXPECT().GetExecResultSet().Return([]interface{}{
			buildPkMoveLinkProbeResultSet(
				pkMoveProbeRow(rid1, nil, pkMoveTestKey(t, 2), pkMoveTestKey(t, 3), 100),
				pkMoveProbeRow(rid2, &rid1, pkMoveTestKey(t, 2), pkMoveTestKey(t, 4), 200),
			),
		}).Times(1)
		bh.EXPECT().ClearExecResultSet().Times(1)

		p := newPkMoveLinkProvider(ctx, ses, bh, []pkMoveLinkTableWindow{window})
		require.NotNil(t, p)
		require.NoError(t, p.probeOldPKs(window, [][]byte{pkMoveTestKey(t, 2)}))

		// Both rows merged; the key counts as fully probed.
		link, ok := p.byRowProbe(pkMoveTestKey(t, 2), rid1)
		require.True(t, ok)
		require.False(t, link.hasNewRowID)
		link, ok = p.byRowProbe(pkMoveTestKey(t, 2), rid2)
		require.True(t, ok)
		require.True(t, link.hasNewRowID)
		require.Equal(t, rid1, link.newRowID)
		succ, count, complete, err := p.successorOf(pkMoveTestKey(t, 2), rid2, pkMoveTxnScope{})
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, 1, count)
		require.Equal(t, pkMoveTestKey(t, 4), succ.newPK)

		// A repeat probe of the same key has nothing left to fetch.
		require.NoError(t, p.probeOldPKs(window, [][]byte{pkMoveTestKey(t, 2)}))
	})

	t.Run("probe SQL spans full history below the endpoint (straddlers included)", func(t *testing.T) {
		// write_ts is the recording transaction's CREATE timestamp: a
		// transaction created before the window start can commit relevant
		// moves inside it, so a lower-bound predicate would make omitted
		// departures look like complete absence. Window membership is
		// decided by stream commit instants during the walk; startPhys now
		// only participates in the full-window cache signature.
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		narrowed := pkMoveLinkTableWindow{tableID: 88, startPhys: 500, endPhys: 777}
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, sql string) error {
				require.NotContains(t, sql, "write_ts >=")
				require.Contains(t, sql, "write_ts <= 777")
				return nil
			},
		).Times(1)
		bh.EXPECT().GetExecResultSet().Return([]interface{}{
			buildPkMoveLinkProbeResultSet(),
		}).Times(1)
		bh.EXPECT().ClearExecResultSet().Times(1)

		p := newPkMoveLinkProvider(ctx, ses, bh, []pkMoveLinkTableWindow{narrowed})
		require.NotNil(t, p)
		require.NoError(t, p.probeOldPKs(narrowed, [][]byte{pkMoveTestKey(t, 9)}))
	})

	t.Run("conflicting window signature disables the provider", func(t *testing.T) {
		// One full-window scan caches a table generation under its exact
		// time bounds; a later probe of the same table under different
		// bounds cannot trust that interval and must fail closed instead
		// of answering from a window that does not cover it.
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		rid1, rid2 := buildHashDiffRowID(t, 41), buildHashDiffRowID(t, 42)
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).Times(1)
		bh.EXPECT().GetExecResultSet().Return([]interface{}{
			buildPkMoveLinkProbeResultSet(
				pkMoveProbeRow(rid1, &rid2, pkMoveTestKey(t, 2), pkMoveTestKey(t, 3), 100),
			),
		}).Times(1)
		bh.EXPECT().ClearExecResultSet().Times(1)

		p := newPkMoveLinkProvider(ctx, ses, bh, []pkMoveLinkTableWindow{window})
		require.NotNil(t, p)
		require.NoError(t, p.prefetchContinuations([][]byte{pkMoveTestKey(t, 2)}))

		require.NoError(t, p.probeOldPKs(
			pkMoveLinkTableWindow{tableID: 88, startPhys: 600, endPhys: 777},
			[][]byte{pkMoveTestKey(t, 2)}))
		require.True(t, p.disabled)
	})

	t.Run("window overflow narrows to per-key probing instead of disabling", func(t *testing.T) {
		// The r23 review's F4: a whole-window scan crossing the row
		// budget used to disable the provider wholesale, so even a PICK
		// of keys with no move history failed on UNRELATED keys'
		// oversized history. The truncated page is discarded wholesale,
		// the window switches to per-key (sha, sha+1) probes of exactly
		// the requested keys, and every already-answered key keeps its
		// answer.
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		key2 := pkMoveTestKey(t, 2)
		rid1, rid2 := buildHashDiffRowID(t, 1), buildHashDiffRowID(t, 2)
		rid7 := buildHashDiffRowID(t, 9)
		// Six rows (one over the cap) spread over UNRELATED keys: the
		// whole-window page overflows and every row of it must be
		// discarded, never cached.
		truncated := make([][]interface{}, 0, 6)
		for i := 0; i < 6; i++ {
			rid := buildHashDiffRowID(t, 20+i)
			truncated = append(truncated, pkMoveProbeRow(
				rid, nil, pkMoveTestKey(t, 10+int64(i)), pkMoveTestKey(t, 30+int64(i)), 100))
		}
		var queries []string
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, sql string) error {
				queries = append(queries, sql)
				return nil
			},
		).Times(3)
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			switch len(queries) {
			case 1:
				return []interface{}{buildPkMoveLinkProbeResultSet(truncated...)}
			case 2:
				// The per-key page answers exactly the requested key.
				return []interface{}{buildPkMoveLinkProbeResultSet(
					pkMoveProbeRow(rid1, &rid2, key2, pkMoveTestKey(t, 3), 100),
				)}
			default:
				return []interface{}{buildPkMoveLinkProbeResultSet(
					pkMoveProbeRow(rid7, nil, pkMoveTestKey(t, 7), pkMoveTestKey(t, 8), 100),
				)}
			}
		}).Times(3)
		bh.EXPECT().ClearExecResultSet().Times(3)

		p := newPkMoveLinkProvider(ctx, ses, bh, []pkMoveLinkTableWindow{window})
		require.NotNil(t, p)
		require.NoError(t, p.probeOldPKs(window, [][]byte{key2}))

		require.False(t, p.disabled, "unrelated overflow must not disable the provider")
		_, full := p.fullWindows[window.tableID]
		require.False(t, full, "a truncated scan never completes the window")

		// A later key of the same window is served per key as well: one
		// more SQL over exactly that key's sha range, no table-wide retry.
		require.NoError(t, p.probeOldPKs(window, [][]byte{pkMoveTestKey(t, 7)}))
		require.Len(t, queries, 3)
		sha7 := sha256.Sum256(pkMoveTestKey(t, 7))
		require.Contains(t, queries[2], fmt.Sprintf("%x", sha7[:8]),
			"the overflowed window serves later keys through their own prefix")
		_, hit, complete := p.byRowProbeVerdict(pkMoveTestKey(t, 7), rid7)
		require.True(t, complete)
		require.True(t, hit)

		// The discarded page cached nothing: an unrelated key from it
		// reports neither a hit nor complete evidence (it stays unprobed,
		// not silently answered from the truncated prefix).
		for i := 0; i < 6; i++ {
			_, hit, complete := p.byRowProbeVerdict(
				pkMoveTestKey(t, 10+int64(i)), buildHashDiffRowID(t, 20+i))
			require.False(t, hit)
			require.False(t, complete)
		}

		// The requested key probed cleanly per key: marked probed,
		// rows cached, anchors and successors answer completely.
		_, done := p.probed[pkMoveLinkProbedKey(window.tableID, key2)]
		require.True(t, done)
		link, hit, complete := p.byRowProbeVerdict(key2, rid1)
		require.True(t, complete)
		require.True(t, hit)
		require.Equal(t, pkMoveTestKey(t, 3), link.newPK)
		succ, count, complete, err := p.successorOf(key2, rid1, pkMoveTxnScope{})
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, 1, count)
		require.Equal(t, pkMoveTestKey(t, 3), succ.newPK)

		// Three probes, all bounded: the whole-window attempt and the
		// per-key retries over each requested key's sha range.
		require.Len(t, queries, 3)
		require.Contains(t, queries[0], "limit 6")
		require.Contains(t, queries[1], "limit 6")
		sha := sha256.Sum256(key2)
		require.Contains(t, queries[1], fmt.Sprintf("%x", sha[:8]),
			"the retry scans exactly the requested key's link_id prefix")
	})

	t.Run("cache saturation leaves only the unprobed keys incomplete", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// No Exec expectation: any SQL the provider issued would fail the
		// mock.
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		p := newPkMoveLinkProvider(ctx, ses, bh, []pkMoveLinkTableWindow{window})
		require.NotNil(t, p)
		p.rows = pkMoveLinkMaxCachedRows + 1
		p.byRow[pkMoveLinkRowKey(buildHashDiffRowID(t, 30), pkMoveTestKey(t, 2))] =
			pkMoveWalkTestLink(2, 30, 3, 31, 10)

		require.NoError(t, p.probeOldPKs(window, [][]byte{pkMoveTestKey(t, 2)}))
		require.False(t, p.disabled,
			"a full cache narrows to the unprobed keys, not the whole provider")
		// The requested key could not be probed: its verdict must report
		// INCOMPLETE evidence, never read as "no provenance".
		_, _, complete := p.byRowProbeVerdict(pkMoveTestKey(t, 2), buildHashDiffRowID(t, 30))
		require.False(t, complete)
		// Already-cached rows stay held for the keys they answered.
		require.NotEmpty(t, p.byRow)
	})

	t.Run("unrelated overflow history cannot abort a small link-free pick", func(t *testing.T) {
		// The r23 review's F4 regression: PICK of a key with NO links
		// must still apply (plain legacy replay) even when the source
		// table accumulates more link history for UNRELATED keys than the
		// cache budget holds — links accumulate across transactions past
		// any writer-side budget. The whole-window scan overflows, the
		// per-key probe proves absence for the selected key, and the
		// chain verdict is proven-none — never unresolved.
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		key2 := pkMoveTestKey(t, 2)
		anchorRid := buildHashDiffRowID(t, 40)
		truncated := make([][]interface{}, 0, 6)
		for i := 0; i < 6; i++ {
			rid := buildHashDiffRowID(t, 20+i)
			truncated = append(truncated, pkMoveProbeRow(
				rid, nil, pkMoveTestKey(t, 10+int64(i)), pkMoveTestKey(t, 30+int64(i)), 100))
		}
		var queries []string
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, sql string) error {
				queries = append(queries, sql)
				return nil
			},
		).Times(2)
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			if len(queries) == 1 {
				return []interface{}{buildPkMoveLinkProbeResultSet(truncated...)}
			}
			// The per-key probe of the selected key: NO history anywhere.
			return []interface{}{buildPkMoveLinkProbeResultSet()}
		}).Times(2)
		bh.EXPECT().ClearExecResultSet().Times(2)

		p := newPkMoveLinkProvider(ctx, ses, bh, []pkMoveLinkTableWindow{window})
		require.NotNil(t, p)
		require.NoError(t, p.prefetch([][]byte{key2}))
		require.False(t, p.disabled,
			"unrelated history must not disable a small pick's provider")
		require.Len(t, queries, 2,
			"one whole-window attempt, then exactly one per-key probe")

		view := pkMoveWalkTestStubView()
		_, _, verdict := p.resolvePkMoveChainVerdict(view, key2, anchorRid)
		require.Equal(t, pkMoveProvenNone, verdict,
			"complete proven absence keeps the key's own delete semantics")
	})

	t.Run("walk crossing the budget mid-chain fails that key closed", func(t *testing.T) {
		// Injected cap-boundary regression: the anchor resolves from
		// prefetched evidence and its tombstone seeds the birth
		// certificate; the impostor at the landed key was born at another
		// timestamp, so the walk must probe provenance for the lineage's
		// recorded departure there — and that probe overflows the budget,
		// in whole-window mode first and again per key. The walk must
		// treat THIS key's evidence as incomplete and never claim the
		// impostor, even though a real continuation arrived in the same
		// truncated reply — while the provider and the anchor stay alive
		// (r23 review F4: the failure narrows to the crossed key).
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		oldCap2 := pkMoveLinkMaxCachedRows
		pkMoveLinkMaxCachedRows = 1
		defer func() { pkMoveLinkMaxCachedRows = oldCap2 }()

		key2, key3, key4 := pkMoveTestKey(t, 2), pkMoveTestKey(t, 3), pkMoveTestKey(t, 4)
		anchorRid := buildHashDiffRowID(t, 40)
		lineageRid := buildHashDiffRowID(t, 41)
		impostorRid := buildHashDiffRowID(t, 42)
		finalRid := buildHashDiffRowID(t, 43)

		// Both probes — the whole-window attempt and the per-key retry —
		// return the same two rows (one over the budget of 1): the real
		// continuation (3->4) and the impostor's own departure.
		page := []interface{}{
			buildPkMoveLinkProbeResultSet(
				pkMoveProbeRow(lineageRid, &finalRid, key3, key4, 20),
				pkMoveProbeRow(impostorRid, nil, key3, pkMoveTestKey(t, 9), 21),
			),
		}
		var queries []string
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, sql string) error {
				require.Contains(t, sql, "limit 2")
				queries = append(queries, sql)
				return nil
			},
		).Times(2)
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return page
		}).Times(2)
		bh.EXPECT().ClearExecResultSet().Times(2)

		p := newPkMoveLinkProvider(ctx, ses, bh, []pkMoveLinkTableWindow{window})
		require.NotNil(t, p)
		anchor := pkMoveLink{
			oldRowID:    anchorRid,
			newRowID:    lineageRid,
			hasNewRowID: true,
			oldPK:       key2,
			newPK:       key3,
			writeTS:     types.BuildTS(10, 0),
		}
		p.byRow[pkMoveLinkRowKey(anchorRid, key2)] = anchor
		p.byPK[string(key2)] = append(p.byPK[string(key2)], anchor)
		p.probed[pkMoveLinkProbedKey(window.tableID, key2)] = struct{}{}

		// Anchor tombstone at key2: the deleting transaction committed at
		// 15, so only a version born at exactly 15 may be claimed at key3.
		// The impostor there was born at 25 and stays unclaimed; the
		// tombstone naming the lineage row at key3 (committed at 20)
		// forces the successorOf probe that crosses the budget.
		impostorRow := []byte{0xcc}
		view := pkMoveWalkTestStubView()
		view.tombs[string(key2)] = []pkMoveTombRef{{rowID: anchorRid, cts: types.BuildTS(15, 0)}}
		view.live[string(key3)] = [][]byte{impostorRow}
		view.rowIDs[string(impostorRow)] = impostorRid
		view.births[string(impostorRow)] = types.BuildTS(25, 0)
		view.tombs[string(key3)] = []pkMoveTombRef{{rowID: lineageRid, cts: types.BuildTS(20, 0)}}

		_, _, ok := p.resolvePkMoveChain(view, key2, anchorRid)
		require.False(t, ok)
		require.False(t, p.disabled,
			"the failure narrows to the budget-crossed key, not the provider")
		// The key whose own history crossed the budget answers
		// terminal-incomplete WITHOUT re-probing (the mock allows no
		// third page), while the anchor key keeps its clean verdict.
		_, _, hopComplete, err := p.successorOf(key3, lineageRid, pkMoveTxnScope{})
		require.NoError(t, err)
		require.False(t, hopComplete, "the landed key's own history crossed the budget")
		_, hit, anchorComplete := p.byRowProbeVerdict(key2, anchorRid)
		require.True(t, anchorComplete)
		require.True(t, hit, "the anchor's prefetched evidence survives the hop's overflow")
		// F3: a budget-crossed probe is unresolved provenance — loud apply
		// abort, never a silent legacy delete replay.
		_, _, verdict := p.resolvePkMoveChainVerdict(view, key2, anchorRid)
		require.Equal(t, pkMoveUnresolved, verdict)
	})
}

// Continuation prefetching covers the keys a chain walk lands on: one
// window scan loads every link of the table generation, so hop successor
// lookups resolve from cache instead of issuing one SQL round trip per
// anchor (the historical single-key probe path) or one chunked range
// query per anchor shard (the intermediate path, whose range-OR
// predicate planning grows superlinearly with the range count).
func TestPkMoveLinkPrefetchContinuations(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)

	window := pkMoveLinkTableWindow{tableID: 88, endPhys: 777}
	key2, key3, key4 := pkMoveTestKey(t, 2), pkMoveTestKey(t, 3), pkMoveTestKey(t, 4)
	rid1, rid2 := buildHashDiffRowID(t, 50), buildHashDiffRowID(t, 51)
	rid3 := buildHashDiffRowID(t, 52)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var queries []string
	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, sql string) error {
			queries = append(queries, sql)
			return nil
		},
	).Times(1)
	bh.EXPECT().GetExecResultSet().Return([]interface{}{
		buildPkMoveLinkProbeResultSet(
			pkMoveProbeRow(rid1, &rid2, key2, key3, 100),
			pkMoveProbeRow(rid2, &rid3, key3, key4, 110),
		),
	}).Times(1)
	bh.EXPECT().ClearExecResultSet().Times(1)

	p := newPkMoveLinkProvider(ctx, ses, bh, []pkMoveLinkTableWindow{window})
	require.NotNil(t, p)
	require.NoError(t, p.prefetchContinuations([][]byte{key2}))

	// One scanned window answers every later key: anchors, hops, and the
	// terminal key with no recorded departure. Only requested keys are
	// marked individually; the rest ride the full-window mark.
	require.Equal(t,
		[2]uint64{window.startPhys, window.endPhys},
		p.fullWindows[window.tableID])
	_, done := p.probed[pkMoveLinkProbedKey(window.tableID, key2)]
	require.True(t, done)
	require.Len(t, queries, 1)
	require.NotContains(t, queries[0], "write_ts >=")
	require.Contains(t, queries[0], "write_ts <= 777")
	require.Contains(t, queries[0], "order by link_id")

	// The resolved walk consumes only cache: both hops come out of the
	// prefetched evidence with no further SQL (the mock allows exactly
	// one Exec).
	link, ok := p.byRowProbe(key2, rid1)
	require.True(t, ok)
	require.Equal(t, key3, link.newPK)
	succ, count, complete, err := p.successorOf(key3, rid2, pkMoveTxnScope{})
	require.NoError(t, err)
	require.True(t, complete)
	require.Equal(t, 1, count)
	require.Equal(t, key4, succ.newPK)
	_, count, complete, err = p.successorOf(key4, rid3, pkMoveTxnScope{})
	require.NoError(t, err)
	require.True(t, complete)
	require.Equal(t, 0, count)
}

type pkMoveStreamRow struct {
	rowID  types.Rowid
	id     int64
	name   string
	hidden string
	birth  types.TS
}

func TestDecodePkMoveDataRowOverHashmap(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()

	hm := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{{
		rowID: buildHashDiffRowID(t, 7), id: 3, name: "three", hidden: "h",
		birth: types.BuildTS(50, 0),
	}})
	defer func() { require.NoError(t, hm.Close()) }()

	ret, err := hm.GetByEncodedKey(pkMoveTestKey(t, 3))
	require.NoError(t, err)
	require.True(t, ret.Exists)
	require.Len(t, ret.Rows, 1)

	rowID, birth, ok := decodePkMoveStreamRowIDAndBirth(hm, ret.Rows[0])
	require.True(t, ok)
	require.Equal(t, buildHashDiffRowID(t, 7), rowID)
	wantBirth := types.BuildTS(50, 0)
	require.True(t, birth.EQ(&wantBirth))
}

// buildPkMoveStreamHashmap builds a data hashmap in the production layout:
// [rowid, pk, name, hidden, __commit_ts], keyed by the pk column.
func buildPkMoveStreamHashmap(
	t testing.TB,
	mp *mpool.MPool,
	rows []pkMoveStreamRow,
) databranchutils.BranchHashmap {
	t.Helper()

	vecs := []*vector.Vector{
		vector.NewVec(types.T_Rowid.ToType()),
		vector.NewVec(types.T_int64.ToType()),
		vector.NewVec(types.T_varchar.ToType()),
		vector.NewVec(types.T_varchar.ToType()),
		vector.NewVec(types.T_TS.ToType()),
	}
	defer func() {
		for _, v := range vecs {
			v.Free(mp)
		}
	}()
	for _, row := range rows {
		require.NoError(t, appendTestVectorValue(vecs[0], row.rowID, mp))
		require.NoError(t, appendTestVectorValue(vecs[1], row.id, mp))
		require.NoError(t, appendTestVectorValue(vecs[2], row.name, mp))
		require.NoError(t, appendTestVectorValue(vecs[3], row.hidden, mp))
		require.NoError(t, appendTestVectorValue(vecs[4], row.birth, mp))
	}

	hm, err := databranchutils.NewBranchHashmap(databranchutils.WithBranchHashmapShardCount(1))
	require.NoError(t, err)
	require.NoError(t, hm.PutByVectors(vecs, []int{1}))
	return hm
}

// buildPkMoveTombstoneHashmap builds a tombstone hashmap in the production
// layout: [rowid, pk, __commit_ts], keyed by the pk column; the row's
// birth field plays the deletion commit timestamp.
func buildPkMoveTombstoneHashmap(
	t testing.TB,
	mp *mpool.MPool,
	rows []pkMoveStreamRow,
) databranchutils.BranchHashmap {
	t.Helper()

	vecs := []*vector.Vector{
		vector.NewVec(types.T_Rowid.ToType()),
		vector.NewVec(types.T_int64.ToType()),
		vector.NewVec(types.T_TS.ToType()),
	}
	defer func() {
		for _, v := range vecs {
			v.Free(mp)
		}
	}()
	for _, row := range rows {
		require.NoError(t, appendTestVectorValue(vecs[0], row.rowID, mp))
		require.NoError(t, appendTestVectorValue(vecs[1], row.id, mp))
		require.NoError(t, appendTestVectorValue(vecs[2], row.birth, mp))
	}

	hm, err := databranchutils.NewBranchHashmap(databranchutils.WithBranchHashmapShardCount(1))
	require.NoError(t, err)
	require.NoError(t, hm.PutByVectors(vecs, []int{1}))
	return hm
}

// TestPairPkMovingTombstones exercises the real pairing consumer: an
// The stream-evidence capture decodes every tombstone into the walk's
// key-indexed ref set and records departed-version births only for
// tombstones whose data row is still visible.
func TestCapturePkMoveStreamEvidence(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()

	depRid := buildHashDiffRowID(t, 61)
	anchorRid := buildHashDiffRowID(t, 62)
	liveRid := buildHashDiffRowID(t, 63)

	dataHashmap := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{
		{rowID: depRid, id: 3, name: "three", hidden: "h", birth: types.BuildTS(20, 0)},
		{rowID: liveRid, id: 4, name: "four", hidden: "h", birth: types.BuildTS(30, 0)},
	})
	tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, []pkMoveStreamRow{
		{rowID: depRid, id: 3, birth: types.BuildTS(25, 0)},
		{rowID: anchorRid, id: 2, birth: types.BuildTS(15, 0)},
	})
	defer func() {
		require.NoError(t, dataHashmap.Close())
		require.NoError(t, tombstoneHashmap.Close())
	}()

	departed, tombs, capped, err := capturePkMoveStreamEvidence(context.Background(), dataHashmap, tombstoneHashmap, 3)
	require.NoError(t, err)
	require.False(t, capped)

	// Every tombstone is indexed under its encoded key with row id and
	// commit timestamp decoded.
	require.Equal(t, []pkMoveTombRef{{rowID: depRid, cts: types.BuildTS(25, 0)}},
		tombs[string(pkMoveTestKey(t, 3))])
	require.Equal(t, []pkMoveTombRef{{rowID: anchorRid, cts: types.BuildTS(15, 0)}},
		tombs[string(pkMoveTestKey(t, 2))])

	// Only the born-died pair records a departed birth; the LCA-era
	// anchor tombstone has no paired data row and records nothing.
	require.Equal(t, map[pkMoveDepartKey]types.TS{{rowID: depRid, cts: types.BuildTS(25, 0)}: types.BuildTS(20, 0)}, departed)

	// The captured key survives the source iteration: mutate nothing here,
	// but the index must remain readable after the maps close below.
	require.Len(t, tombs, 2)
}

// Exact row-id pairing (tier 1) covers the version the key-level heuristic
// cannot see: one born and deleted inside a single transaction, where the
// image's birth equals the deletion's own commit instant. The exact match
// must win over an older same-key generation when both images are visible,
// must refuse an image born after the deletion, and the corruption
// fail-closed (two generations sharing the greatest birth below) still
// records nothing.
func TestCapturePkMoveStreamEvidenceExactRowIDPairing(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()

	run := func(t *testing.T, dataRows, tombRows []pkMoveStreamRow) map[pkMoveDepartKey]types.TS {
		dataHashmap := buildPkMoveStreamHashmap(t, mp, dataRows)
		tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, tombRows)
		defer func() {
			require.NoError(t, dataHashmap.Close())
			require.NoError(t, tombstoneHashmap.Close())
		}()
		departed, _, _, err := capturePkMoveStreamEvidence(context.Background(), dataHashmap, tombstoneHashmap, 3)
		require.NoError(t, err)
		return departed
	}

	t.Run("born-and-died version pairs on the shared row id", func(t *testing.T) {
		rid := buildHashDiffRowID(t, 81)
		departed := run(t,
			[]pkMoveStreamRow{{rowID: rid, id: 3, name: "v", hidden: "h", birth: types.BuildTS(15, 0)}},
			[]pkMoveStreamRow{{rowID: rid, id: 3, birth: types.BuildTS(15, 0)}},
		)
		require.Equal(t, map[pkMoveDepartKey]types.TS{{rowID: rid, cts: types.BuildTS(15, 0)}: types.BuildTS(15, 0)}, departed)
	})

	t.Run("exact row id wins over an older same-key generation", func(t *testing.T) {
		rid := buildHashDiffRowID(t, 82)
		olderRid := buildHashDiffRowID(t, 83)
		departed := run(t,
			[]pkMoveStreamRow{
				{rowID: rid, id: 3, name: "new", hidden: "h", birth: types.BuildTS(15, 0)},
				{rowID: olderRid, id: 3, name: "old", hidden: "h", birth: types.BuildTS(10, 0)},
			},
			[]pkMoveStreamRow{{rowID: rid, id: 3, birth: types.BuildTS(15, 0)}},
		)
		require.Equal(t, map[pkMoveDepartKey]types.TS{{rowID: rid, cts: types.BuildTS(15, 0)}: types.BuildTS(15, 0)}, departed)
	})

	t.Run("an image newer than the deletion cannot pair", func(t *testing.T) {
		rid := buildHashDiffRowID(t, 84)
		departed := run(t,
			[]pkMoveStreamRow{{rowID: rid, id: 3, name: "v", hidden: "h", birth: types.BuildTS(25, 0)}},
			[]pkMoveStreamRow{{rowID: rid, id: 3, birth: types.BuildTS(20, 0)}},
		)
		require.Empty(t, departed)
	})

	t.Run("a reincarnated row id pairs each deletion with its newest earlier birth", func(t *testing.T) {
		// Workspace allocation restarts per transaction, so one raw row
		// id can be born at 15 and again at 25 under the same key. The
		// tombstone at 26 deleted the 25-born incarnation, the tombstone
		// at 20 the 15-born one; keying by (row id, death instant) keeps
		// both pairings instead of letting one overwrite the other.
		rid := buildHashDiffRowID(t, 88)
		departed := run(t,
			[]pkMoveStreamRow{
				{rowID: rid, id: 3, name: "old", hidden: "h", birth: types.BuildTS(15, 0)},
				{rowID: rid, id: 3, name: "new", hidden: "h", birth: types.BuildTS(25, 0)},
			},
			[]pkMoveStreamRow{
				{rowID: rid, id: 3, birth: types.BuildTS(20, 0)},
				{rowID: rid, id: 3, birth: types.BuildTS(26, 0)},
			},
		)
		require.Equal(t, map[pkMoveDepartKey]types.TS{
			{rowID: rid, cts: types.BuildTS(20, 0)}: types.BuildTS(15, 0),
			{rowID: rid, cts: types.BuildTS(26, 0)}: types.BuildTS(25, 0),
		}, departed)
	})

	t.Run("reincarnation at one instant pairs the generation born there", func(t *testing.T) {
		// Born-and-died netting inside transaction 26: the newest birth
		// not after the deletion IS the deletion's own instant.
		rid := buildHashDiffRowID(t, 89)
		departed := run(t,
			[]pkMoveStreamRow{
				{rowID: rid, id: 3, name: "old", hidden: "h", birth: types.BuildTS(15, 0)},
				{rowID: rid, id: 3, name: "new", hidden: "h", birth: types.BuildTS(26, 0)},
			},
			[]pkMoveStreamRow{{rowID: rid, id: 3, birth: types.BuildTS(26, 0)}},
		)
		require.Equal(t, map[pkMoveDepartKey]types.TS{
			{rowID: rid, cts: types.BuildTS(26, 0)}: types.BuildTS(26, 0),
		}, departed)
	})

	t.Run("two generations sharing the greatest birth record nothing", func(t *testing.T) {
		rid := buildHashDiffRowID(t, 85)
		departed := run(t,
			[]pkMoveStreamRow{
				{rowID: buildHashDiffRowID(t, 86), id: 3, name: "a", hidden: "h", birth: types.BuildTS(12, 0)},
				{rowID: buildHashDiffRowID(t, 87), id: 3, name: "b", hidden: "h", birth: types.BuildTS(12, 0)},
			},
			[]pkMoveStreamRow{{rowID: rid, id: 3, birth: types.BuildTS(20, 0)}},
		)
		require.Empty(t, departed)
	})
}

// The evidence indexes live outside the hashmap allocator's spill budget,
// so a bound protects them: past the bound the capture clears BOTH indexes
// (partial evidence could pass an anchor-vs-LCA continuity check that full
// evidence rejects), cancellation aborts the walk input build, and every
// key's refs come out in a deterministic (commit ts, row id) order
// regardless of shard iteration order.
func TestCapturePkMoveStreamEvidenceLimits(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()

	ridA := buildHashDiffRowID(t, 71)
	ridB := buildHashDiffRowID(t, 72)
	ridC := buildHashDiffRowID(t, 73)

	t.Run("entries past the bound clear both indexes", func(t *testing.T) {
		// The capture decodes tombstones against a live (possibly empty)
		// data hashmap; a nil data hashmap means "no data side" and the
		// capture declines before scanning.
		emptyData := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{
			{rowID: buildHashDiffRowID(t, 999), id: 999, birth: types.BuildTS(1, 0)},
		})
		defer func() { require.NoError(t, emptyData.Close()) }()

		var tombRows []pkMoveStreamRow
		for i := 0; i < 6; i++ {
			tombRows = append(tombRows, pkMoveStreamRow{
				rowID: buildHashDiffRowID(t, 100+i),
				id:    int64(2 + i),
				birth: types.BuildTS(int64(10+i), 0),
			})
		}
		tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, tombRows)
		defer func() { require.NoError(t, tombstoneHashmap.Close()) }()

		// Bound of three over a six-tombstone window trips the cap; the
		// same window under a bound of six passes untouched.
		departed, tombs, capped, err := capturePkMoveStreamEvidenceBounded(
			context.Background(), emptyData, tombstoneHashmap, 1, 3)
		require.NoError(t, err)
		require.True(t, capped, "entries past the bound must mark the capture capped")
		require.Empty(t, departed)
		require.Empty(t, tombs)

		departed2, tombs2, capped2, err := capturePkMoveStreamEvidenceBounded(
			context.Background(), emptyData, tombstoneHashmap, 1, 6)
		require.NoError(t, err)
		require.False(t, capped2)
		require.Empty(t, departed2)
		require.Len(t, tombs2, 6)
	})

	t.Run("parallel shards never oversubscribe the shared budget", func(t *testing.T) {
		// Budget portability across shards: slots are reserved atomically
		// with the cap check, so N qualifying rows under a bound of N pass
		// complete while a bound of N-1 caps the WHOLE capture — a late
		// per-shard publish could let two shards admit against the same
		// remaining slot and return an oversized, un-capped index.
		emptyData := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{
			{rowID: buildHashDiffRowID(t, 998), id: 998, birth: types.BuildTS(1, 0)},
		})
		defer func() { require.NoError(t, emptyData.Close()) }()

		const rowCnt = 32
		tombVecs := []*vector.Vector{
			vector.NewVec(types.T_Rowid.ToType()),
			vector.NewVec(types.T_int64.ToType()),
			vector.NewVec(types.T_TS.ToType()),
		}
		defer func() {
			for _, v := range tombVecs {
				v.Free(mp)
			}
		}()
		for i := 0; i < rowCnt; i++ {
			require.NoError(t, appendTestVectorValue(tombVecs[0], buildHashDiffRowID(t, 300+i), mp))
			require.NoError(t, appendTestVectorValue(tombVecs[1], int64(2000+i), mp))
			require.NoError(t, appendTestVectorValue(tombVecs[2], types.BuildTS(40, 0), mp))
		}
		// Four shards over thirty-two keys with parallelism three: the
		// exact shard split varies with the hash, the budget arithmetic
		// must not.
		tombstoneHashmap, err := databranchutils.NewBranchHashmap(
			databranchutils.WithBranchHashmapShardCount(4))
		require.NoError(t, err)
		require.NoError(t, tombstoneHashmap.PutByVectors(tombVecs, []int{1}))
		defer func() { require.NoError(t, tombstoneHashmap.Close()) }()

		departed, tombs, capped, err := capturePkMoveStreamEvidenceBounded(
			context.Background(), emptyData, tombstoneHashmap, 3, rowCnt)
		require.NoError(t, err)
		require.False(t, capped)
		require.Len(t, tombs, rowCnt)
		require.Empty(t, departed)

		departed, tombs, capped, err = capturePkMoveStreamEvidenceBounded(
			context.Background(), emptyData, tombstoneHashmap, 3, rowCnt-1)
		require.NoError(t, err)
		require.True(t, capped, "one row over the shared budget must mark the capture capped")
		require.Empty(t, tombs)
		require.Empty(t, departed)
	})

	t.Run("cancellation aborts the capture", func(t *testing.T) {
		emptyData := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{
			{rowID: buildHashDiffRowID(t, 999), id: 999, birth: types.BuildTS(1, 0)},
		})
		defer func() { require.NoError(t, emptyData.Close()) }()
		tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, []pkMoveStreamRow{
			{rowID: ridA, id: 2, birth: types.BuildTS(10, 0)},
		})
		defer func() { require.NoError(t, tombstoneHashmap.Close()) }()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		departed, tombs, capped, err := capturePkMoveStreamEvidenceBounded(
			ctx, emptyData, tombstoneHashmap, 1, pkMoveEvidenceMaxEntries)
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, capped)
		require.Nil(t, departed)
		require.Nil(t, tombs)
	})

	t.Run("per-key refs sort by commit timestamp then row id", func(t *testing.T) {
		// Two tombstones of one key share a commit instant; a third is
		// newer. Insertion order is deliberately scrambled.
		emptyData := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{
			{rowID: buildHashDiffRowID(t, 999), id: 999, birth: types.BuildTS(1, 0)},
		})
		defer func() { require.NoError(t, emptyData.Close()) }()
		tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, []pkMoveStreamRow{
			{rowID: ridC, id: 2, birth: types.BuildTS(30, 0)},
			{rowID: ridB, id: 2, birth: types.BuildTS(10, 0)},
			{rowID: ridA, id: 2, birth: types.BuildTS(10, 0)},
		})
		defer func() { require.NoError(t, tombstoneHashmap.Close()) }()

		_, tombs, _, err := capturePkMoveStreamEvidence(
			context.Background(), emptyData, tombstoneHashmap, 1)
		require.NoError(t, err)
		got := tombs[string(pkMoveTestKey(t, 2))]
		require.Equal(t, []pkMoveTombRef{
			{rowID: ridA, cts: types.BuildTS(10, 0)},
			{rowID: ridB, cts: types.BuildTS(10, 0)},
			{rowID: ridC, cts: types.BuildTS(30, 0)},
		}, got)
	})
}

// getByEncodedKeyCountingHashmap counts data-side fetches. The capture
// consults the data map once per key per shard; the helper maps are
// single-shard and all runs below pass parallelism one, so the counter
// stays single-threaded.
type getByEncodedKeyCountingHashmap struct {
	databranchutils.BranchHashmap
	calls int64
}

func (h *getByEncodedKeyCountingHashmap) GetByEncodedKey(key []byte) (databranchutils.GetResult, error) {
	h.calls++
	return h.BranchHashmap.GetByEncodedKey(key)
}

// A high-churn key pairs every one of its tombstones against ONE
// materialized version index: the data map is fetched once per key, an
// absent key is fetched once and pairs nothing, and the pairings
// themselves are exactly the ones the per-tombstone scan produced —
// exact row id first (with recency), then the newest birth strictly
// below the deletion across every row id of the key.
func TestCapturePkMoveStreamEvidenceKeyIndexReuse(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()

	ridOld := buildHashDiffRowID(t, 301)
	ridMid := buildHashDiffRowID(t, 302)
	ridNew := buildHashDiffRowID(t, 303)
	ridKey4 := buildHashDiffRowID(t, 304)
	ridStray := buildHashDiffRowID(t, 305)
	ridAbsentA := buildHashDiffRowID(t, 306)
	ridAbsentB := buildHashDiffRowID(t, 307)

	baseData := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{
		{rowID: ridOld, id: 3, name: "o", hidden: "h", birth: types.BuildTS(10, 0)},
		{rowID: ridMid, id: 3, name: "m", hidden: "h", birth: types.BuildTS(20, 0)},
		{rowID: ridNew, id: 3, name: "n", hidden: "h", birth: types.BuildTS(30, 0)},
		{rowID: ridKey4, id: 4, name: "k4", hidden: "h", birth: types.BuildTS(5, 0)},
	})
	dataHashmap := &getByEncodedKeyCountingHashmap{BranchHashmap: baseData}
	tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, []pkMoveStreamRow{
		{rowID: ridNew, id: 3, birth: types.BuildTS(30, 0)},
		{rowID: ridMid, id: 3, birth: types.BuildTS(20, 0)},
		// A tombstone whose row id is not visible pairs by key-level
		// recency only: the newest birth below 25 is 20's incarnation.
		{rowID: ridStray, id: 3, birth: types.BuildTS(25, 0)},
		{rowID: ridKey4, id: 4, birth: types.BuildTS(8, 0)},
		// Two tombstones of a key the data map does not hold: fetched
		// once, pair none.
		{rowID: ridAbsentA, id: 9, birth: types.BuildTS(3, 0)},
		{rowID: ridAbsentB, id: 9, birth: types.BuildTS(4, 0)},
	})
	defer func() {
		require.NoError(t, dataHashmap.Close())
		require.NoError(t, tombstoneHashmap.Close())
	}()

	departed, tombs, capped, err := capturePkMoveStreamEvidence(
		context.Background(), dataHashmap, tombstoneHashmap, 1)
	require.NoError(t, err)
	require.False(t, capped)
	require.Equal(t, map[pkMoveDepartKey]types.TS{
		{rowID: ridNew, cts: types.BuildTS(30, 0)}:   types.BuildTS(30, 0),
		{rowID: ridMid, cts: types.BuildTS(20, 0)}:   types.BuildTS(20, 0),
		{rowID: ridStray, cts: types.BuildTS(25, 0)}: types.BuildTS(20, 0),
		{rowID: ridKey4, cts: types.BuildTS(8, 0)}:   types.BuildTS(5, 0),
	}, departed)
	// Every tombstone is indexed under its encoded key, including the
	// absent key's two.
	require.Len(t, tombs[string(pkMoveTestKey(t, 3))], 3)
	require.Len(t, tombs[string(pkMoveTestKey(t, 4))], 1)
	require.Len(t, tombs[string(pkMoveTestKey(t, 9))], 2)
	// The data side is fetched once per touched key: three keys over six
	// tombstones, where the per-tombstone scan fetched six times.
	require.EqualValues(t, 3, dataHashmap.calls,
		"every tombstone of a key reuses the key's single version index")
}

// Two exact matches sharing the greatest birth not after the deletion are
// corruption: nothing pairs, and tier 2 must NOT step in — the scan this
// replaced skipped it on any tier-1 hit, tie included.
func TestCapturePkMoveStreamEvidenceTierOneTieStaysClosed(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()

	rid := buildHashDiffRowID(t, 310)
	other := buildHashDiffRowID(t, 311)

	dataHashmap := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{
		{rowID: rid, id: 3, name: "a", hidden: "h", birth: types.BuildTS(15, 0)},
		{rowID: rid, id: 3, name: "b", hidden: "h", birth: types.BuildTS(15, 0)},
		{rowID: other, id: 3, name: "c", hidden: "h", birth: types.BuildTS(10, 0)},
	})
	tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, []pkMoveStreamRow{
		{rowID: rid, id: 3, birth: types.BuildTS(20, 0)},
	})
	defer func() {
		require.NoError(t, dataHashmap.Close())
		require.NoError(t, tombstoneHashmap.Close())
	}()

	departed, _, capped, err := capturePkMoveStreamEvidence(
		context.Background(), dataHashmap, tombstoneHashmap, 1)
	require.NoError(t, err)
	require.False(t, capped)
	require.Empty(t, departed,
		"a tier-1 tie fails closed without consulting tier 2")
}

// High-churn same-key histories — the shape the evidence capture used to
// handle quadratically. Every tombstone here is a plain overwrite delete
// of the key's previous generation: nothing about the history records a
// pk move, which is exactly the common case an apply-capable diff pays
// for. The wide-keyspace group is the constant-work guard: one tombstone
// per key over many keys must stay cheap. Before the version index the
// same-key group re-fetched and re-decoded every version per tombstone,
// doubling size quadrupled cost; with it cost stays near-linear.
func BenchmarkCapturePkMoveStreamEvidenceKeyChurn(b *testing.B) {
	for _, n := range []int{64, 256, 1024} {
		b.Run(fmt.Sprintf("same_key_tombstones_%d", n), func(b *testing.B) {
			mp := mpool.MustNewZero()
			dataRows := make([]pkMoveStreamRow, 0, n)
			tombRows := make([]pkMoveStreamRow, 0, n)
			for i := 0; i < n; i++ {
				rid := types.BuildTestRowid(1, int64(i+1))
				dataRows = append(dataRows, pkMoveStreamRow{
					rowID: rid, id: 3, name: "v", hidden: "h",
					birth: types.BuildTS(int64(i+10), 0),
				})
				// The version born at i+10 is deleted at i+20: every
				// tombstone pairs exactly by its own row id.
				tombRows = append(tombRows, pkMoveStreamRow{
					rowID: rid, id: 3,
					birth: types.BuildTS(int64(i+20), 0),
				})
			}
			dataHashmap := buildPkMoveStreamHashmap(b, mp, dataRows)
			tombstoneHashmap := buildPkMoveTombstoneHashmap(b, mp, tombRows)
			defer func() {
				_ = dataHashmap.Close()
				_ = tombstoneHashmap.Close()
			}()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, _, err := capturePkMoveStreamEvidence(
					context.Background(), dataHashmap, tombstoneHashmap, 1); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	for _, n := range []int{64, 256, 1024} {
		b.Run(fmt.Sprintf("wide_keyspace_tombstones_%d", n), func(b *testing.B) {
			mp := mpool.MustNewZero()
			dataRows := make([]pkMoveStreamRow, 0, n)
			tombRows := make([]pkMoveStreamRow, 0, n)
			for i := 0; i < n; i++ {
				rid := types.BuildTestRowid(2, int64(i+1))
				key := int64(1000 + i)
				dataRows = append(dataRows, pkMoveStreamRow{
					rowID: rid, id: key, name: "v", hidden: "h",
					birth: types.BuildTS(10, 0),
				})
				tombRows = append(tombRows, pkMoveStreamRow{
					rowID: rid, id: key,
					birth: types.BuildTS(20, 0),
				})
			}
			dataHashmap := buildPkMoveStreamHashmap(b, mp, dataRows)
			tombstoneHashmap := buildPkMoveTombstoneHashmap(b, mp, tombRows)
			defer func() {
				_ = dataHashmap.Close()
				_ = tombstoneHashmap.Close()
			}()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, _, err := capturePkMoveStreamEvidence(
					context.Background(), dataHashmap, tombstoneHashmap, 1); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// The SQL-free fold assembly: a resolved anchor plus its LCA before-image
// must fold into a key-moving delete+insert pair whose replacement row
// leaves the data hashmap; PICK dependency-only anchors, second anchors of
// one old key, and vanished replacements keep legacy meaning, and
// interacting folds emit in topological apply order.
func TestAssemblePkMoveFoldPlan(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	mp := ses.proc.Mp()

	const (
		anchorRidSeq  = 51
		lineageRidSeq = 52
	)

	type foldFixture struct {
		tblStuff   tableStuff
		provider   *pkMoveLinkProvider
		claimed    *pkMoveClaimedKeys
		view       *pkMoveHashmapView
		anchorRid  types.Rowid
		lineageRid types.Rowid
		oldKeyEnc  []byte
		newKeyEnc  []byte
	}

	newTableStuff := func(t *testing.T) tableStuff {
		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)
		return newTestBranchTableStuff(ctrl)
	}

	setup := func(t *testing.T, hasIdentity bool) foldFixture {
		fix := foldFixture{tblStuff: newTableStuff(t), claimed: newPkMoveClaimedKeys()}

		fix.anchorRid = buildHashDiffRowID(t, anchorRidSeq)
		fix.lineageRid = buildHashDiffRowID(t, lineageRidSeq)
		fix.oldKeyEnc = pkMoveTestKey(t, 2)
		fix.newKeyEnc = pkMoveTestKey(t, 3)

		fix.provider = pkMoveWalkTestProvider(pkMoveLink{
			oldRowID:    fix.anchorRid,
			newRowID:    fix.lineageRid,
			hasNewRowID: hasIdentity,
			oldPK:       fix.oldKeyEnc,
			newPK:       fix.newKeyEnc,
			writeTS:     types.BuildTS(10, 0),
		})

		dataHashmap := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{{
			rowID: fix.lineageRid, id: 3, name: "three", hidden: "h",
			birth: types.BuildTS(50, 0),
		}})
		// The anchor tombstone carries the moving transaction's commit
		// timestamp 50, matching the replacement row's birth. The view's
		// tombstone index is built by the same capture the diff runs.
		tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, []pkMoveStreamRow{{
			rowID: fix.anchorRid, id: 2, birth: types.BuildTS(50, 0),
		}})
		departed, tombs, _, err := capturePkMoveStreamEvidence(ctx, dataHashmap, tombstoneHashmap, 1)
		require.NoError(t, err)
		fix.view = &pkMoveHashmapView{data: dataHashmap, tombs: tombs, departed: departed}
		t.Cleanup(func() {
			require.NoError(t, dataHashmap.Close())
			require.NoError(t, tombstoneHashmap.Close())
		})
		return fix
	}

	// lcaBat holds the LCA before-images the anchor probe would return:
	// data columns only, keyed by the OLD pk.
	lcaBat := func(t *testing.T, rows ...[]any) *batch.Batch {
		dBat := batch.NewWithSize(3)
		dBat.SetAttributes([]string{"id", "name", "hidden"})
		dBat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		dBat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		dBat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
		for _, row := range rows {
			require.NoError(t, vector.AppendFixed(dBat.Vecs[0], row[0].(int64), false, mp))
			require.NoError(t, vector.AppendBytes(dBat.Vecs[1], []byte(row[1].(string)), false, mp))
			require.NoError(t, vector.AppendBytes(dBat.Vecs[2], []byte(row[2].(string)), false, mp))
		}
		dBat.SetRowCount(len(rows))
		t.Cleanup(func() { dBat.Clean(mp) })
		return dBat
	}

	resolveOne := func(t *testing.T, fix foldFixture, key []byte, rid types.Rowid) pkMoveFoldCandidate {
		c := pkMoveFoldCandidate{key: key, rid: rid}
		c.finalPK, c.finalRow, c.resolved = fix.provider.resolvePkMoveChain(fix.view, key, rid)
		require.True(t, c.resolved)
		return c
	}

	releasePlan := func(t *testing.T, tblStuff tableStuff, plan *pkMoveFoldPlan) {
		t.Helper()
		if plan == nil {
			return
		}
		if plan.deleteBat != nil {
			tblStuff.retPool.releaseRetBatch(plan.deleteBat, false)
		}
		if plan.insertBat != nil {
			tblStuff.retPool.releaseRetBatch(plan.insertBat, false)
		}
	}

	t.Run("proven anchor folds and claims the replacement row", func(t *testing.T) {
		fix := setup(t, true)
		dBat := lcaBat(t, []any{int64(2), "two", "h"})
		c := resolveOne(t, fix, fix.oldKeyEnc, fix.anchorRid)

		plan, err := assemblePkMoveFoldPlan(
			ctx, ses, fix.tblStuff, fix.view, fix.claimed, dBat,
			[]pkMoveFoldCandidate{c}, nil,
		)
		require.NoError(t, err)
		require.NotNil(t, plan)
		defer releasePlan(t, fix.tblStuff, plan)

		_, anchor := plan.anchorKeys[string(fix.oldKeyEnc)]
		require.True(t, anchor)
		require.Equal(t, [][]any{{int64(2), nil, nil}}, plan.oldKeys)
		require.True(t, fix.claimed.has(fix.newKeyEnc))
		require.Equal(t, []bool{true}, plan.victimAuth)
		require.Equal(t, []bool{false}, plan.unchangedOcc)

		deleted := decodeCapturedRows(t, plan.deleteBat, fix.tblStuff.def.colTypes)
		require.Equal(t, [][]any{{int64(2), "two", "h"}}, deleted)
		inserted := decodeCapturedRows(t, plan.insertBat, fix.tblStuff.def.colTypes)
		require.Equal(t, [][]any{{int64(3), "three", "h"}}, inserted)

		// The replacement row left the stream: a later same-key pop or
		// insert walk can no longer reuse it.
		ret, err := fix.view.data.GetByEncodedKey(fix.newKeyEnc)
		require.NoError(t, err)
		require.False(t, ret.Exists)
	})

	t.Run("NULL recorded identity still folds through the timestamp tie", func(t *testing.T) {
		// A replacement whose identity the writer could not attribute is
		// still bound by the anchor commit timestamp, so the fold is sound.
		fix := setup(t, false)
		dBat := lcaBat(t, []any{int64(2), "two", "h"})
		c := resolveOne(t, fix, fix.oldKeyEnc, fix.anchorRid)

		plan, err := assemblePkMoveFoldPlan(
			ctx, ses, fix.tblStuff, fix.view, fix.claimed, dBat,
			[]pkMoveFoldCandidate{c}, nil,
		)
		require.NoError(t, err)
		require.NotNil(t, plan)
		defer releasePlan(t, fix.tblStuff, plan)

		require.True(t, fix.claimed.has(fix.newKeyEnc))
		require.NotNil(t, plan.oldKeys)
		ret, err := fix.view.data.GetByEncodedKey(fix.newKeyEnc)
		require.NoError(t, err)
		require.False(t, ret.Exists)
	})

	t.Run("PICK dependency-only anchor never folds", func(t *testing.T) {
		// NF4: keys admitted by the dependency closure reassemble the
		// SELECTED lineage; an unrelated lineage whose old key collides
		// with a dependency key must not become an applied move.
		fix := setup(t, true)
		fix.tblStuff.pickDepKeys = map[string]struct{}{string(fix.oldKeyEnc): {}}
		dBat := lcaBat(t, []any{int64(2), "two", "h"})
		c := resolveOne(t, fix, fix.oldKeyEnc, fix.anchorRid)

		plan, err := assemblePkMoveFoldPlan(
			ctx, ses, fix.tblStuff, fix.view, fix.claimed, dBat,
			[]pkMoveFoldCandidate{c}, nil,
		)
		require.NoError(t, err)
		require.Nil(t, plan)
		require.False(t, fix.claimed.has(fix.newKeyEnc))

		ret, err := fix.view.data.GetByEncodedKey(fix.newKeyEnc)
		require.NoError(t, err)
		require.True(t, ret.Exists)
	})

	t.Run("anchor missing from the LCA keeps legacy meaning", func(t *testing.T) {
		// A source-only lineage (born after the branch point) needs no
		// destination cascade: no fold emits and its replacement stays in
		// the stream for born-died netting.
		fix := setup(t, true)
		dBat := lcaBat(t, []any{int64(9), "nine", "h"})
		c := resolveOne(t, fix, fix.oldKeyEnc, fix.anchorRid)

		plan, err := assemblePkMoveFoldPlan(
			ctx, ses, fix.tblStuff, fix.view, fix.claimed, dBat,
			[]pkMoveFoldCandidate{c}, nil,
		)
		require.NoError(t, err)
		require.Nil(t, plan)
		require.False(t, fix.claimed.has(fix.newKeyEnc))

		ret, err := fix.view.data.GetByEncodedKey(fix.newKeyEnc)
		require.NoError(t, err)
		require.True(t, ret.Exists)
	})

	t.Run("interacting folds emit in topological order", func(t *testing.T) {
		// F3: destination rows 2 and 3; the source moved 3->4 (at 50) and
		// then 2->3 (at 60). Replaying 2->3 first would kill the row the
		// 3->4 move still needs as its source; the plan must emit 3->4
		// first.
		tblStuff := newTableStuff(t)
		rid2 := buildHashDiffRowID(t, 61)
		rid3 := buildHashDiffRowID(t, 62)
		lin3 := buildHashDiffRowID(t, 63)
		lin4 := buildHashDiffRowID(t, 64)
		key2 := pkMoveTestKey(t, 2)
		key3 := pkMoveTestKey(t, 3)
		key4 := pkMoveTestKey(t, 4)

		provider := pkMoveWalkTestProvider(
			pkMoveLink{oldRowID: rid2, newRowID: lin3, hasNewRowID: true,
				oldPK: key2, newPK: key3, writeTS: types.BuildTS(10, 0)},
			pkMoveLink{oldRowID: rid3, newRowID: lin4, hasNewRowID: true,
				oldPK: key3, newPK: key4, writeTS: types.BuildTS(11, 0)},
		)
		claimed := newPkMoveClaimedKeys()
		dataHashmap := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{
			{rowID: lin3, id: 3, name: "two-was", hidden: "h", birth: types.BuildTS(60, 0)},
			{rowID: lin4, id: 4, name: "three-was", hidden: "h", birth: types.BuildTS(50, 0)},
		})
		tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, []pkMoveStreamRow{
			{rowID: rid3, id: 3, birth: types.BuildTS(50, 0)},
			{rowID: rid2, id: 2, birth: types.BuildTS(60, 0)},
		})
		departed, tombs, _, err := capturePkMoveStreamEvidence(ctx, dataHashmap, tombstoneHashmap, 1)
		require.NoError(t, err)
		view := &pkMoveHashmapView{data: dataHashmap, tombs: tombs, departed: departed}
		t.Cleanup(func() {
			require.NoError(t, dataHashmap.Close())
			require.NoError(t, tombstoneHashmap.Close())
		})

		dBat := lcaBat(t, []any{int64(2), "two", "h"}, []any{int64(3), "three", "h"})
		candA := pkMoveFoldCandidate{key: key2, rid: rid2}
		candA.finalPK, candA.finalRow, candA.resolved = provider.resolvePkMoveChain(view, key2, rid2)
		require.True(t, candA.resolved)
		candB := pkMoveFoldCandidate{key: key3, rid: rid3}
		candB.finalPK, candB.finalRow, candB.resolved = provider.resolvePkMoveChain(view, key3, rid3)
		require.True(t, candB.resolved)

		plan, err := assemblePkMoveFoldPlan(
			ctx, ses, tblStuff, view, claimed, dBat,
			[]pkMoveFoldCandidate{candA, candB}, nil,
		)
		require.NoError(t, err)
		require.NotNil(t, plan)
		defer releasePlan(t, tblStuff, plan)

		// The 3->4 move leads, so its source row is already at 4 when the
		// 2->3 move lands at the vacated key.
		inserted := decodeCapturedRows(t, plan.insertBat, tblStuff.def.colTypes)
		require.Equal(t, [][]any{
			{int64(4), "three-was", "h"},
			{int64(3), "two-was", "h"},
		}, inserted)
		deleted := decodeCapturedRows(t, plan.deleteBat, tblStuff.def.colTypes)
		require.Equal(t, [][]any{
			{int64(3), "three", "h"},
			{int64(2), "two", "h"},
		}, deleted)
		require.Equal(t, [][]any{{int64(3), nil, nil}, {int64(2), nil, nil}}, plan.oldKeys)
		require.Equal(t, []bool{true, true}, plan.victimAuth)
		require.True(t, claimed.has(key3))
		require.True(t, claimed.has(key4))
	})

	t.Run("one fold per old key: the earliest anchor wins", func(t *testing.T) {
		// Two proven anchors under one old key cannot both own the
		// destination lineage; the earliest commit wins and the later
		// replacement keeps insert meaning. (The walk itself fails closed
		// on a same-key predecessor — see the continuity suite — so the
		// second candidate is pre-resolved here to exercise the dedup in
		// isolation.)
		tblStuff := newTableStuff(t)
		ridA := buildHashDiffRowID(t, 71)
		ridB := buildHashDiffRowID(t, 72)
		linA := buildHashDiffRowID(t, 73)
		linB := buildHashDiffRowID(t, 74)
		key2 := pkMoveTestKey(t, 2)
		key3 := pkMoveTestKey(t, 3)
		key4 := pkMoveTestKey(t, 4)

		provider := pkMoveWalkTestProvider(
			pkMoveLink{oldRowID: ridA, newRowID: linA, hasNewRowID: true,
				oldPK: key2, newPK: key3, writeTS: types.BuildTS(10, 0)},
		)
		claimed := newPkMoveClaimedKeys()
		dataHashmap := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{
			{rowID: linA, id: 3, name: "three", hidden: "h", birth: types.BuildTS(50, 0)},
			{rowID: linB, id: 4, name: "four", hidden: "h", birth: types.BuildTS(70, 0)},
		})
		tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, []pkMoveStreamRow{
			{rowID: ridB, id: 2, birth: types.BuildTS(70, 0)},
			{rowID: ridA, id: 2, birth: types.BuildTS(50, 0)},
		})
		departed, tombs, _, err := capturePkMoveStreamEvidence(ctx, dataHashmap, tombstoneHashmap, 1)
		require.NoError(t, err)
		// The capture sorts each key's refs by commit timestamp, so the
		// earliest anchor is the first candidate.
		require.Equal(t, []types.Rowid{ridA, ridB},
			[]types.Rowid{tombs[string(key2)][0].rowID, tombs[string(key2)][1].rowID})
		view := &pkMoveHashmapView{data: dataHashmap, tombs: tombs, departed: departed}
		t.Cleanup(func() {
			require.NoError(t, dataHashmap.Close())
			require.NoError(t, tombstoneHashmap.Close())
		})

		dBat := lcaBat(t, []any{int64(2), "two", "h"})
		candA := pkMoveFoldCandidate{key: key2, rid: ridA}
		candA.finalPK, candA.finalRow, candA.resolved = provider.resolvePkMoveChain(view, key2, ridA)
		require.True(t, candA.resolved)
		// The later anchor fails continuity against the earlier same-key
		// tombstone (the anchor row does not descend from the LCA row).
		candLate := pkMoveFoldCandidate{key: key2, rid: ridB}
		candLate.finalPK, candLate.finalRow, candLate.resolved = provider.resolvePkMoveChain(view, key2, ridB)
		require.False(t, candLate.resolved)

		// Pre-resolve the second candidate by hand to force the dedup
		// branch: both anchors proven, the earliest must own the fold.
		retB, err := view.data.GetByEncodedKey(key4)
		require.NoError(t, err)
		require.True(t, retB.Exists)
		require.Len(t, retB.Rows, 1)
		candB := pkMoveFoldCandidate{
			key: key2, rid: ridB, finalPK: key4, finalRow: retB.Rows[0], resolved: true,
		}

		plan, err := assemblePkMoveFoldPlan(
			ctx, ses, tblStuff, view, claimed, dBat,
			[]pkMoveFoldCandidate{candA, candB}, nil,
		)
		require.NoError(t, err)
		require.NotNil(t, plan)
		defer releasePlan(t, tblStuff, plan)

		require.Len(t, plan.oldKeys, 1)
		inserted := decodeCapturedRows(t, plan.insertBat, tblStuff.def.colTypes)
		require.Equal(t, [][]any{{int64(3), "three", "h"}}, inserted)
		require.True(t, claimed.has(key3))
		require.False(t, claimed.has(key4))
		// The later anchor's replacement stays in the stream.
		ret, err := view.data.GetByEncodedKey(key4)
		require.NoError(t, err)
		require.True(t, ret.Exists)
	})

	t.Run("key-rotation cycle aborts the apply loud", func(t *testing.T) {
		// A rotated key cannot be replayed as an in-place native UPDATE:
		// every landing key of the cycle still holds a member row until
		// that member moves, so any emission order deletes a row another
		// member still needs. Legacy DELETE+INSERT replay is no fallback
		// either: same-key matching would swap payloads while each
		// destination child stays attached to the row id at its own key,
		// the lineage that moved away, and no FK violation stays loud.
		// The pass therefore aborts statement-atomically — nothing
		// popped, claimed, or anchored — instead of silently detaching
		// dependents onto the wrong lineage.
		tblStuff := newTableStuff(t)
		ridA := buildHashDiffRowID(t, 81)
		ridB := buildHashDiffRowID(t, 82)
		linA := buildHashDiffRowID(t, 83)
		linB := buildHashDiffRowID(t, 84)
		key2 := pkMoveTestKey(t, 2)
		key3 := pkMoveTestKey(t, 3)

		provider := pkMoveWalkTestProvider(
			pkMoveLink{oldRowID: ridA, newRowID: linA, hasNewRowID: true,
				oldPK: key2, newPK: key3, writeTS: types.BuildTS(10, 0)},
			pkMoveLink{oldRowID: ridB, newRowID: linB, hasNewRowID: true,
				oldPK: key3, newPK: key2, writeTS: types.BuildTS(12, 0)},
		)
		claimed := newPkMoveClaimedKeys()
		dataHashmap := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{
			{rowID: linA, id: 3, name: "three", hidden: "h", birth: types.BuildTS(50, 0)},
			{rowID: linB, id: 2, name: "two", hidden: "h", birth: types.BuildTS(70, 0)},
		})
		tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, []pkMoveStreamRow{
			{rowID: ridB, id: 3, birth: types.BuildTS(70, 0)},
			{rowID: ridA, id: 2, birth: types.BuildTS(50, 0)},
		})
		departed, tombs, _, err := capturePkMoveStreamEvidence(ctx, dataHashmap, tombstoneHashmap, 1)
		require.NoError(t, err)
		view := &pkMoveHashmapView{data: dataHashmap, tombs: tombs, departed: departed}
		t.Cleanup(func() {
			require.NoError(t, dataHashmap.Close())
			require.NoError(t, tombstoneHashmap.Close())
		})

		dBat := lcaBat(t, []any{int64(2), "lca-two", "h"}, []any{int64(3), "lca-three", "h"})
		candA := pkMoveFoldCandidate{key: key2, rid: ridA}
		candA.finalPK, candA.finalRow, candA.resolved = provider.resolvePkMoveChain(view, key2, ridA)
		require.True(t, candA.resolved)
		candB := pkMoveFoldCandidate{key: key3, rid: ridB}
		candB.finalPK, candB.finalRow, candB.resolved = provider.resolvePkMoveChain(view, key3, ridB)
		require.True(t, candB.resolved)

		plan, err := assemblePkMoveFoldPlan(
			ctx, ses, tblStuff, view, claimed, dBat,
			[]pkMoveFoldCandidate{candA, candB}, nil,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "key rotation cycle")
		// The whole plan is a cycle: it aborts before any emission.
		require.Nil(t, plan)
		require.False(t, claimed.has(key2))
		require.False(t, claimed.has(key3))
		// Nothing popped: both replacement rows stay in the stream and
		// both anchor tombstones stay unabsorbed, so the statement-atomic
		// rollback faces an untouched input.
		ret, err := view.data.GetByEncodedKey(key2)
		require.NoError(t, err)
		require.True(t, ret.Exists)
		ret, err = view.data.GetByEncodedKey(key3)
		require.NoError(t, err)
		require.True(t, ret.Exists)
	})

	t.Run("one cyclic member aborts the whole plan", func(t *testing.T) {
		// A key rotation (2->3, 3->2) and an independent move (7->8) mix:
		// partial folding is not an option because the independent fold's
		// emission would already mutate the stream view while the cycle's
		// rows silently mis-apply through legacy same-key matching. The
		// abort is statement-atomic: no fold materializes at all, and
		// every stream row, claim, and tombstone stays untouched.
		tblStuff := newTableStuff(t)
		ridA := buildHashDiffRowID(t, 81)
		ridB := buildHashDiffRowID(t, 82)
		ridC := buildHashDiffRowID(t, 85)
		linA := buildHashDiffRowID(t, 83)
		linB := buildHashDiffRowID(t, 84)
		linC := buildHashDiffRowID(t, 86)
		key2 := pkMoveTestKey(t, 2)
		key3 := pkMoveTestKey(t, 3)
		key7 := pkMoveTestKey(t, 7)
		key8 := pkMoveTestKey(t, 8)

		provider := pkMoveWalkTestProvider(
			pkMoveLink{oldRowID: ridA, newRowID: linA, hasNewRowID: true,
				oldPK: key2, newPK: key3, writeTS: types.BuildTS(10, 0)},
			pkMoveLink{oldRowID: ridB, newRowID: linB, hasNewRowID: true,
				oldPK: key3, newPK: key2, writeTS: types.BuildTS(12, 0)},
			pkMoveLink{oldRowID: ridC, newRowID: linC, hasNewRowID: true,
				oldPK: key7, newPK: key8, writeTS: types.BuildTS(14, 0)},
		)
		claimed := newPkMoveClaimedKeys()
		dataHashmap := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{
			{rowID: linA, id: 3, name: "three", hidden: "h", birth: types.BuildTS(50, 0)},
			{rowID: linB, id: 2, name: "two", hidden: "h", birth: types.BuildTS(70, 0)},
			{rowID: linC, id: 8, name: "eight", hidden: "h", birth: types.BuildTS(90, 0)},
		})
		tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, []pkMoveStreamRow{
			{rowID: ridB, id: 3, birth: types.BuildTS(70, 0)},
			{rowID: ridA, id: 2, birth: types.BuildTS(50, 0)},
			{rowID: ridC, id: 7, birth: types.BuildTS(90, 0)},
		})
		departed, tombs, _, err := capturePkMoveStreamEvidence(ctx, dataHashmap, tombstoneHashmap, 1)
		require.NoError(t, err)
		view := &pkMoveHashmapView{data: dataHashmap, tombs: tombs, departed: departed}
		t.Cleanup(func() {
			require.NoError(t, dataHashmap.Close())
			require.NoError(t, tombstoneHashmap.Close())
		})

		dBat := lcaBat(t,
			[]any{int64(2), "lca-two", "h"},
			[]any{int64(3), "lca-three", "h"},
			[]any{int64(7), "lca-seven", "h"})
		candA := pkMoveFoldCandidate{key: key2, rid: ridA}
		candA.finalPK, candA.finalRow, candA.resolved = provider.resolvePkMoveChain(view, key2, ridA)
		require.True(t, candA.resolved)
		candB := pkMoveFoldCandidate{key: key3, rid: ridB}
		candB.finalPK, candB.finalRow, candB.resolved = provider.resolvePkMoveChain(view, key3, ridB)
		require.True(t, candB.resolved)
		candC := pkMoveFoldCandidate{key: key7, rid: ridC}
		candC.finalPK, candC.finalRow, candC.resolved = provider.resolvePkMoveChain(view, key7, ridC)
		require.True(t, candC.resolved)

		plan, err := assemblePkMoveFoldPlan(
			ctx, ses, tblStuff, view, claimed, dBat,
			[]pkMoveFoldCandidate{candA, candB, candC}, nil,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "key rotation cycle")
		require.Nil(t, plan)

		// No fold materializes: the independent move's key is claimed by
		// nothing, no anchor map exists, and every replacement row stays
		// in the stream for the rolled-back statement.
		require.False(t, claimed.has(key8))
		require.False(t, claimed.has(key2))
		require.False(t, claimed.has(key3))
		ret, err := view.data.GetByEncodedKey(key2)
		require.NoError(t, err)
		require.True(t, ret.Exists)
		ret, err = view.data.GetByEncodedKey(key3)
		require.NoError(t, err)
		require.True(t, ret.Exists)
		ret, err = view.data.GetByEncodedKey(key8)
		require.NoError(t, err)
		require.True(t, ret.Exists)
	})

	// newReturnFixture builds the "return to the original key" shape:
	// two separately committed source updates moved the lineage 2->3
	// (commit 50) and back 3->2 (commit 70). The committed intermediate
	// generation stays visible in the stream exports; the final arrival
	// lives at the ORIGINAL anchor key with payload finalName.
	newReturnFixture := func(t *testing.T, finalName string) (
		tblStuff tableStuff, provider *pkMoveLinkProvider, claimed *pkMoveClaimedKeys,
		view *pkMoveHashmapView, key2, key3 []byte,
		ridA, linA, linB types.Rowid,
	) {
		tblStuff = newTableStuff(t)
		ridA = buildHashDiffRowID(t, 71)
		linA = buildHashDiffRowID(t, 72)
		linB = buildHashDiffRowID(t, 73)
		key2 = pkMoveTestKey(t, 2)
		key3 = pkMoveTestKey(t, 3)
		provider = pkMoveWalkTestProvider(
			pkMoveLink{oldRowID: ridA, newRowID: linA, hasNewRowID: true,
				oldPK: key2, newPK: key3, writeTS: types.BuildTS(10, 0)},
			pkMoveLink{oldRowID: linA, newRowID: linB, hasNewRowID: true,
				oldPK: key3, newPK: key2, writeTS: types.BuildTS(12, 0)},
		)
		claimed = newPkMoveClaimedKeys()
		dataHashmap := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{
			{rowID: linA, id: 3, name: "three", hidden: "h", birth: types.BuildTS(50, 0)},
			{rowID: linB, id: 2, name: finalName, hidden: "h", birth: types.BuildTS(70, 0)},
		})
		tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, []pkMoveStreamRow{
			{rowID: ridA, id: 2, birth: types.BuildTS(50, 0)},
			{rowID: linA, id: 3, birth: types.BuildTS(70, 0)},
		})
		departed, tombs, _, err := capturePkMoveStreamEvidence(ctx, dataHashmap, tombstoneHashmap, 1)
		require.NoError(t, err)
		view = &pkMoveHashmapView{data: dataHashmap, tombs: tombs, departed: departed}
		t.Cleanup(func() {
			require.NoError(t, dataHashmap.Close())
			require.NoError(t, tombstoneHashmap.Close())
		})
		return
	}

	t.Run("return to the original key folds as an in-place update", func(t *testing.T) {
		// The advancing-timestamp chain resolves the net final key back
		// to the anchor's own old key. That fold depends only on itself:
		// the planner's blanket prerequisite count once registered the
		// fold as its own blocker and aborted the plan as a "key
		// rotation cycle", rejecting a payload change at the original
		// key. The fold must instead apply in place — the native UPDATE
		// at the anchor key carries the latest payload, and any declared
		// ON UPDATE action sees a stable key.
		tblStuff, provider, claimed, view, key2, _, ridA, _, _ := newReturnFixture(t, "two-back")
		dBat := lcaBat(t, []any{int64(2), "two", "h"})
		cand := pkMoveFoldCandidate{key: key2, rid: ridA}
		cand.finalPK, cand.finalRow, cand.resolved = provider.resolvePkMoveChain(view, key2, ridA)
		require.True(t, cand.resolved)
		require.Equal(t, key2, cand.finalPK)

		plan, err := assemblePkMoveFoldPlan(
			ctx, ses, tblStuff, view, claimed, dBat,
			[]pkMoveFoldCandidate{cand}, nil,
		)
		require.NoError(t, err)
		require.NotNil(t, plan)
		defer releasePlan(t, tblStuff, plan)

		require.Equal(t, [][]any{{int64(2), nil, nil}}, plan.oldKeys)
		deleted := decodeCapturedRows(t, plan.deleteBat, tblStuff.def.colTypes)
		require.Equal(t, [][]any{{int64(2), "two", "h"}}, deleted)
		inserted := decodeCapturedRows(t, plan.insertBat, tblStuff.def.colTypes)
		require.Equal(t, [][]any{{int64(2), "two-back", "h"}}, inserted)
		require.Equal(t, []bool{true}, plan.victimAuth)
		require.True(t, claimed.has(key2))
	})

	t.Run("return with unchanged payload still folds in place", func(t *testing.T) {
		// A net no-op return carries the ORIGINAL payload back to the
		// anchor key (the shape a literal-key PICK window exports even
		// when nothing changed). The fold still replays as one in-place
		// UPDATE — never as DELETE+INSERT — so declared foreign-key
		// actions are not triggered and children stay attached.
		tblStuff, provider, claimed, view, key2, _, ridA, _, _ := newReturnFixture(t, "two")
		dBat := lcaBat(t, []any{int64(2), "two", "h"})
		cand := pkMoveFoldCandidate{key: key2, rid: ridA}
		cand.finalPK, cand.finalRow, cand.resolved = provider.resolvePkMoveChain(view, key2, ridA)
		require.True(t, cand.resolved)
		require.Equal(t, key2, cand.finalPK)

		plan, err := assemblePkMoveFoldPlan(
			ctx, ses, tblStuff, view, claimed, dBat,
			[]pkMoveFoldCandidate{cand}, nil,
		)
		require.NoError(t, err)
		require.NotNil(t, plan)
		defer releasePlan(t, tblStuff, plan)

		inserted := decodeCapturedRows(t, plan.insertBat, tblStuff.def.colTypes)
		require.Equal(t, [][]any{{int64(2), "two", "h"}}, inserted)
		require.Equal(t, []bool{true}, plan.victimAuth)
		require.True(t, claimed.has(key2))
	})

	t.Run("a self-landing fold never dissolves a genuine cycle", func(t *testing.T) {
		// The self-edge exclusion exists for the return-to-origin fold
		// only: a genuine multi-row key rotation mixed into the same
		// plan must still abort statement-atomically, and the self-fold
		// must not materialize either (no partial emission).
		tblStuff, _, claimed, _, key2, _, ridA, _, _ := newReturnFixture(t, "two-back")
		ridC := buildHashDiffRowID(t, 74)
		ridD := buildHashDiffRowID(t, 75)
		linC := buildHashDiffRowID(t, 76)
		linD := buildHashDiffRowID(t, 77)
		key5 := pkMoveTestKey(t, 5)
		key6 := pkMoveTestKey(t, 6)
		// Rebuild the shared fixture with the cycle members added: the
		// self-fold keeps its links, the 5<->6 rotation adds its own.
		provider := pkMoveWalkTestProvider(
			pkMoveLink{oldRowID: ridA, newRowID: buildHashDiffRowID(t, 72), hasNewRowID: true,
				oldPK: key2, newPK: pkMoveTestKey(t, 3), writeTS: types.BuildTS(10, 0)},
			pkMoveLink{oldRowID: buildHashDiffRowID(t, 72), newRowID: buildHashDiffRowID(t, 73), hasNewRowID: true,
				oldPK: pkMoveTestKey(t, 3), newPK: key2, writeTS: types.BuildTS(12, 0)},
			pkMoveLink{oldRowID: ridC, newRowID: linC, hasNewRowID: true,
				oldPK: key5, newPK: key6, writeTS: types.BuildTS(14, 0)},
			pkMoveLink{oldRowID: ridD, newRowID: linD, hasNewRowID: true,
				oldPK: key6, newPK: key5, writeTS: types.BuildTS(16, 0)},
		)
		dataHashmap := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{
			{rowID: buildHashDiffRowID(t, 72), id: 3, name: "three", hidden: "h", birth: types.BuildTS(50, 0)},
			{rowID: buildHashDiffRowID(t, 73), id: 2, name: "two-back", hidden: "h", birth: types.BuildTS(70, 0)},
			{rowID: linC, id: 6, name: "six", hidden: "h", birth: types.BuildTS(50, 0)},
			{rowID: linD, id: 5, name: "five", hidden: "h", birth: types.BuildTS(70, 0)},
		})
		tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, []pkMoveStreamRow{
			{rowID: ridA, id: 2, birth: types.BuildTS(50, 0)},
			{rowID: buildHashDiffRowID(t, 72), id: 3, birth: types.BuildTS(70, 0)},
			{rowID: ridC, id: 5, birth: types.BuildTS(50, 0)},
			{rowID: ridD, id: 6, birth: types.BuildTS(70, 0)},
		})
		departed, tombs, _, err := capturePkMoveStreamEvidence(ctx, dataHashmap, tombstoneHashmap, 1)
		require.NoError(t, err)
		view := &pkMoveHashmapView{data: dataHashmap, tombs: tombs, departed: departed}
		t.Cleanup(func() {
			require.NoError(t, dataHashmap.Close())
			require.NoError(t, tombstoneHashmap.Close())
		})

		dBat := lcaBat(t,
			[]any{int64(2), "two", "h"},
			[]any{int64(5), "five", "h"},
			[]any{int64(6), "six", "h"})
		selfCand := pkMoveFoldCandidate{key: key2, rid: ridA}
		selfCand.finalPK, selfCand.finalRow, selfCand.resolved = provider.resolvePkMoveChain(view, key2, ridA)
		require.True(t, selfCand.resolved)
		require.Equal(t, key2, selfCand.finalPK)
		cycA := pkMoveFoldCandidate{key: key5, rid: ridC}
		cycA.finalPK, cycA.finalRow, cycA.resolved = provider.resolvePkMoveChain(view, key5, ridC)
		require.True(t, cycA.resolved)
		cycB := pkMoveFoldCandidate{key: key6, rid: ridD}
		cycB.finalPK, cycB.finalRow, cycB.resolved = provider.resolvePkMoveChain(view, key6, ridD)
		require.True(t, cycB.resolved)

		plan, err := assemblePkMoveFoldPlan(
			ctx, ses, tblStuff, view, claimed, dBat,
			[]pkMoveFoldCandidate{selfCand, cycA, cycB}, nil,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "key rotation cycle")
		require.Nil(t, plan)
		// Statement-atomic: nothing popped, claimed, or anchored — the
		// self-fold's arrival and both cycle replacements stay in the
		// stream for the rolled-back statement.
		require.False(t, claimed.has(key2))
		require.False(t, claimed.has(key5))
		require.False(t, claimed.has(key6))
		ret, err := view.data.GetByEncodedKey(key2)
		require.NoError(t, err)
		require.True(t, ret.Exists)
		ret, err = view.data.GetByEncodedKey(key5)
		require.NoError(t, err)
		require.True(t, ret.Exists)
	})

	t.Run("interacting moves emit in vacate order", func(t *testing.T) {
		// Regression for the topological edge direction: three distinct
		// rows shift 1->2, 2->3, 3->4, interacting through one another's
		// keys. The only safe replay applies the vacating move first —
		// 3->4, then 2->3, then 1->2: applying 1->2 while the 2->3
		// lineage still occupies key 2 deletes that still-needed row as
		// an authorized victim.
		tblStuff := newTableStuff(t)
		ridA := buildHashDiffRowID(t, 91)
		ridB := buildHashDiffRowID(t, 92)
		ridC := buildHashDiffRowID(t, 93)
		linA := buildHashDiffRowID(t, 94)
		linB := buildHashDiffRowID(t, 95)
		linC := buildHashDiffRowID(t, 96)
		key1 := pkMoveTestKey(t, 1)
		key2 := pkMoveTestKey(t, 2)
		key3 := pkMoveTestKey(t, 3)
		key4 := pkMoveTestKey(t, 4)

		provider := pkMoveWalkTestProvider(
			pkMoveLink{oldRowID: ridA, newRowID: linA, hasNewRowID: true,
				oldPK: key1, newPK: key2, writeTS: types.BuildTS(10, 0)},
			pkMoveLink{oldRowID: ridB, newRowID: linB, hasNewRowID: true,
				oldPK: key2, newPK: key3, writeTS: types.BuildTS(12, 0)},
			pkMoveLink{oldRowID: ridC, newRowID: linC, hasNewRowID: true,
				oldPK: key3, newPK: key4, writeTS: types.BuildTS(14, 0)},
		)
		claimed := newPkMoveClaimedKeys()
		dataHashmap := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{
			{rowID: linA, id: 2, name: "two-moved", hidden: "h", birth: types.BuildTS(50, 0)},
			{rowID: linB, id: 3, name: "three-moved", hidden: "h", birth: types.BuildTS(50, 0)},
			{rowID: linC, id: 4, name: "four-moved", hidden: "h", birth: types.BuildTS(50, 0)},
		})
		tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, []pkMoveStreamRow{
			{rowID: ridA, id: 1, birth: types.BuildTS(50, 0)},
			{rowID: ridB, id: 2, birth: types.BuildTS(50, 0)},
			{rowID: ridC, id: 3, birth: types.BuildTS(50, 0)},
		})
		departed, tombs, _, err := capturePkMoveStreamEvidence(ctx, dataHashmap, tombstoneHashmap, 1)
		require.NoError(t, err)
		view := &pkMoveHashmapView{data: dataHashmap, tombs: tombs, departed: departed}
		t.Cleanup(func() {
			require.NoError(t, dataHashmap.Close())
			require.NoError(t, tombstoneHashmap.Close())
		})

		dBat := lcaBat(t,
			[]any{int64(1), "one", "h"},
			[]any{int64(2), "two", "h"},
			[]any{int64(3), "three", "h"})
		candA := pkMoveFoldCandidate{key: key1, rid: ridA}
		candA.finalPK, candA.finalRow, candA.resolved = provider.resolvePkMoveChain(view, key1, ridA)
		require.True(t, candA.resolved)
		candB := pkMoveFoldCandidate{key: key2, rid: ridB}
		candB.finalPK, candB.finalRow, candB.resolved = provider.resolvePkMoveChain(view, key2, ridB)
		require.True(t, candB.resolved)
		candC := pkMoveFoldCandidate{key: key3, rid: ridC}
		candC.finalPK, candC.finalRow, candC.resolved = provider.resolvePkMoveChain(view, key3, ridC)
		require.True(t, candC.resolved)

		plan, err := assemblePkMoveFoldPlan(
			ctx, ses, tblStuff, view, claimed, dBat,
			[]pkMoveFoldCandidate{candA, candB, candC}, nil,
		)
		require.NoError(t, err)
		require.NotNil(t, plan)
		defer releasePlan(t, tblStuff, plan)

		// Apply order is 3->4, 2->3, 1->2: every landing key was vacated
		// before the move needing it applies.
		require.Equal(t, [][]any{
			{int64(3), nil, nil},
			{int64(2), nil, nil},
			{int64(1), nil, nil},
		}, plan.oldKeys)
		inserted := decodeCapturedRows(t, plan.insertBat, tblStuff.def.colTypes)
		require.Equal(t, [][]any{
			{int64(4), "four-moved", "h"},
			{int64(3), "three-moved", "h"},
			{int64(2), "two-moved", "h"},
		}, inserted)
	})

	t.Run("occupancy probe gates victim authorization", func(t *testing.T) {
		// F4: with no source tombstone at the new key, live destination
		// occupancy withholds the inline-delete authorization; an empty
		// destination keeps it.
		probeCalls := 0
		var probeKeys [][]byte
		occupiedProbe := func(keys [][]byte, _ []types.Tuple) (map[string]bool, error) {
			probeCalls++
			probeKeys = keys
			m := make(map[string]bool, len(keys))
			for _, k := range keys {
				m[string(k)] = true
			}
			return m, nil
		}
		freeProbe := func([][]byte, []types.Tuple) (map[string]bool, error) { return nil, nil }

		fix := setup(t, true)
		dBat := lcaBat(t, []any{int64(2), "two", "h"})
		c := resolveOne(t, fix, fix.oldKeyEnc, fix.anchorRid)
		plan, err := assemblePkMoveFoldPlan(
			ctx, ses, fix.tblStuff, fix.view, fix.claimed, dBat,
			[]pkMoveFoldCandidate{c}, occupiedProbe,
		)
		require.NoError(t, err)
		require.NotNil(t, plan)
		require.Equal(t, []bool{false}, plan.victimAuth)
		require.Equal(t, []bool{true}, plan.unchangedOcc)
		releasePlan(t, fix.tblStuff, plan)

		// One batched call carried the fold's landed key: the plan never
		// issues a per-fold probe.
		require.Equal(t, 1, probeCalls)
		require.Equal(t, [][]byte{fix.newKeyEnc}, probeKeys)

		fix2 := setup(t, true)
		dBat2 := lcaBat(t, []any{int64(2), "two", "h"})
		c2 := resolveOne(t, fix2, fix2.oldKeyEnc, fix2.anchorRid)
		plan2, err := assemblePkMoveFoldPlan(
			ctx, ses, fix2.tblStuff, fix2.view, fix2.claimed, dBat2,
			[]pkMoveFoldCandidate{c2}, freeProbe,
		)
		require.NoError(t, err)
		require.NotNil(t, plan2)
		require.Equal(t, []bool{true}, plan2.victimAuth)
		require.Equal(t, []bool{false}, plan2.unchangedOcc)
		releasePlan(t, fix2.tblStuff, plan2)
	})

	t.Run("PICK selection scopes tombstone victim authorization", func(t *testing.T) {
		// F4: an in-window source tombstone at the fold's new key
		// authorizes the inline victim delete only when that deletion is
		// part of THIS replay's selection. MERGE selects every source
		// change; PICK keys(2) leaves key 3's lineage alone, so the
		// destination occupant at 3 must be classified by the occupancy
		// probe instead of silently erased.
		newFix := func(t *testing.T) foldFixture {
			fix := foldFixture{tblStuff: newTableStuff(t), claimed: newPkMoveClaimedKeys()}
			fix.anchorRid = buildHashDiffRowID(t, anchorRidSeq)
			fix.lineageRid = buildHashDiffRowID(t, lineageRidSeq)
			fix.oldKeyEnc = pkMoveTestKey(t, 2)
			fix.newKeyEnc = pkMoveTestKey(t, 3)
			priorVictimRid := buildHashDiffRowID(t, 91)

			fix.provider = pkMoveWalkTestProvider(pkMoveLink{
				oldRowID: fix.anchorRid, newRowID: fix.lineageRid, hasNewRowID: true,
				oldPK: fix.oldKeyEnc, newPK: fix.newKeyEnc, writeTS: types.BuildTS(10, 0),
			})
			dataHashmap := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{{
				rowID: fix.lineageRid, id: 3, name: "three", hidden: "h",
				birth: types.BuildTS(50, 0),
			}})
			tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, []pkMoveStreamRow{
				{rowID: fix.anchorRid, id: 2, birth: types.BuildTS(50, 0)},
				// The source deleted key 3 in an earlier transaction: an
				// in-window tombstone sits on the fold's new key.
				{rowID: priorVictimRid, id: 3, birth: types.BuildTS(45, 0)},
			})
			departed, tombs, _, err := capturePkMoveStreamEvidence(ctx, dataHashmap, tombstoneHashmap, 1)
			require.NoError(t, err)
			fix.view = &pkMoveHashmapView{data: dataHashmap, tombs: tombs, departed: departed}
			t.Cleanup(func() {
				require.NoError(t, dataHashmap.Close())
				require.NoError(t, tombstoneHashmap.Close())
			})
			return fix
		}
		occupiedProbe := func(keys [][]byte, _ []types.Tuple) (map[string]bool, error) {
			m := make(map[string]bool, len(keys))
			for _, k := range keys {
				m[string(k)] = true
			}
			return m, nil
		}

		// MERGE: any in-window tombstone at the new key authorizes; the
		// occupancy probe is not consulted.
		fix := newFix(t)
		dBat := lcaBat(t, []any{int64(2), "two", "h"})
		c := resolveOne(t, fix, fix.oldKeyEnc, fix.anchorRid)
		plan, err := assemblePkMoveFoldPlan(
			ctx, ses, fix.tblStuff, fix.view, fix.claimed, dBat,
			[]pkMoveFoldCandidate{c}, occupiedProbe,
		)
		require.NoError(t, err)
		require.NotNil(t, plan)
		require.Equal(t, []bool{true}, plan.victimAuth)
		require.Equal(t, []bool{false}, plan.unchangedOcc)
		releasePlan(t, fix.tblStuff, plan)

		// PICK with key 3 in the literal selection: the deletion of 3 is
		// part of the replay, so the tombstone still authorizes.
		fix = newFix(t)
		fix.tblStuff.pickLiteralKeys = map[string]struct{}{
			string(fix.oldKeyEnc): {}, string(fix.newKeyEnc): {},
		}
		dBat = lcaBat(t, []any{int64(2), "two", "h"})
		c = resolveOne(t, fix, fix.oldKeyEnc, fix.anchorRid)
		plan, err = assemblePkMoveFoldPlan(
			ctx, ses, fix.tblStuff, fix.view, fix.claimed, dBat,
			[]pkMoveFoldCandidate{c}, occupiedProbe,
		)
		require.NoError(t, err)
		require.NotNil(t, plan)
		require.Equal(t, []bool{true}, plan.victimAuth)
		require.Equal(t, []bool{false}, plan.unchangedOcc)
		releasePlan(t, fix.tblStuff, plan)

		// PICK keys(2): the tombstone at 3 is outside the selection and
		// the probe finds a live destination occupant, so authorization
		// is withheld and the fold is classified unchanged-occupied.
		fix = newFix(t)
		fix.tblStuff.pickLiteralKeys = map[string]struct{}{string(fix.oldKeyEnc): {}}
		dBat = lcaBat(t, []any{int64(2), "two", "h"})
		c = resolveOne(t, fix, fix.oldKeyEnc, fix.anchorRid)
		plan, err = assemblePkMoveFoldPlan(
			ctx, ses, fix.tblStuff, fix.view, fix.claimed, dBat,
			[]pkMoveFoldCandidate{c}, occupiedProbe,
		)
		require.NoError(t, err)
		require.NotNil(t, plan)
		require.Equal(t, []bool{false}, plan.victimAuth)
		require.Equal(t, []bool{true}, plan.unchangedOcc)
		releasePlan(t, fix.tblStuff, plan)

		// PICK keys(2) with a free destination at 3 still authorizes.
		fix = newFix(t)
		fix.tblStuff.pickLiteralKeys = map[string]struct{}{string(fix.oldKeyEnc): {}}
		dBat = lcaBat(t, []any{int64(2), "two", "h"})
		c = resolveOne(t, fix, fix.oldKeyEnc, fix.anchorRid)
		plan, err = assemblePkMoveFoldPlan(
			ctx, ses, fix.tblStuff, fix.view, fix.claimed, dBat,
			[]pkMoveFoldCandidate{c},
			func([][]byte, []types.Tuple) (map[string]bool, error) { return nil, nil },
		)
		require.NoError(t, err)
		require.NotNil(t, plan)
		require.Equal(t, []bool{true}, plan.victimAuth)
		require.Equal(t, []bool{false}, plan.unchangedOcc)
		releasePlan(t, fix.tblStuff, plan)
	})

	t.Run("probe failure aborts the whole pass", func(t *testing.T) {
		fix := setup(t, true)
		dBat := lcaBat(t, []any{int64(2), "two", "h"})
		c := resolveOne(t, fix, fix.oldKeyEnc, fix.anchorRid)
		_, err := assemblePkMoveFoldPlan(
			ctx, ses, fix.tblStuff, fix.view, fix.claimed, dBat,
			[]pkMoveFoldCandidate{c},
			func([][]byte, []types.Tuple) (map[string]bool, error) {
				return nil, moerr.NewInternalErrorNoCtx("probe boom")
			},
		)
		require.Error(t, err)
	})

	t.Run("one batched probe covers every probe-needing fold", func(t *testing.T) {
		// Scale guard: N folds without tombstone-authorized victims must
		// reach the destination with a single occupancy call, not N
		// point queries. Here the 2->3 fold is covered by the in-window
		// tombstone at 3 (the 3->4 move's own anchor), so only the 3->4
		// fold's landed key travels to the probe.
		tblStuff := newTableStuff(t)
		ridA := buildHashDiffRowID(t, 61)
		ridB := buildHashDiffRowID(t, 62)
		linA := buildHashDiffRowID(t, 63)
		linB := buildHashDiffRowID(t, 64)
		key2 := pkMoveTestKey(t, 2)
		key3 := pkMoveTestKey(t, 3)
		key4 := pkMoveTestKey(t, 4)

		provider := pkMoveWalkTestProvider(
			pkMoveLink{oldRowID: ridA, newRowID: linA, hasNewRowID: true,
				oldPK: key2, newPK: key3, writeTS: types.BuildTS(10, 0)},
			pkMoveLink{oldRowID: ridB, newRowID: linB, hasNewRowID: true,
				oldPK: key3, newPK: key4, writeTS: types.BuildTS(12, 0)},
		)
		claimed := newPkMoveClaimedKeys()
		dataHashmap := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{
			{rowID: linA, id: 3, name: "three-was", hidden: "h", birth: types.BuildTS(50, 0)},
			{rowID: linB, id: 4, name: "four", hidden: "h", birth: types.BuildTS(70, 0)},
		})
		tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, []pkMoveStreamRow{
			{rowID: ridA, id: 2, birth: types.BuildTS(50, 0)},
			{rowID: ridB, id: 3, birth: types.BuildTS(70, 0)},
		})
		departed, tombs, _, err := capturePkMoveStreamEvidence(ctx, dataHashmap, tombstoneHashmap, 1)
		require.NoError(t, err)
		view := &pkMoveHashmapView{data: dataHashmap, tombs: tombs, departed: departed}
		t.Cleanup(func() {
			require.NoError(t, dataHashmap.Close())
			require.NoError(t, tombstoneHashmap.Close())
		})

		dBat := lcaBat(t, []any{int64(2), "two", "h"}, []any{int64(3), "three", "h"})
		candA := pkMoveFoldCandidate{key: key2, rid: ridA}
		candA.finalPK, candA.finalRow, candA.resolved = provider.resolvePkMoveChain(view, key2, ridA)
		require.True(t, candA.resolved)
		candB := pkMoveFoldCandidate{key: key3, rid: ridB}
		candB.finalPK, candB.finalRow, candB.resolved = provider.resolvePkMoveChain(view, key3, ridB)
		require.True(t, candB.resolved)

		probeCalls := 0
		var probeKeys [][]byte
		plan, err := assemblePkMoveFoldPlan(
			ctx, ses, tblStuff, view, claimed, dBat,
			[]pkMoveFoldCandidate{candA, candB},
			func(keys [][]byte, _ []types.Tuple) (map[string]bool, error) {
				probeCalls++
				probeKeys = keys
				return nil, nil
			},
		)
		require.NoError(t, err)
		require.NotNil(t, plan)
		defer releasePlan(t, tblStuff, plan)

		require.Equal(t, 1, probeCalls)
		require.Equal(t, [][]byte{key4}, probeKeys)
		require.Equal(t, []bool{true, true}, plan.victimAuth)
		require.Equal(t, []bool{false, false}, plan.unchangedOcc)
	})

	t.Run("nil provider or nil view folds nothing", func(t *testing.T) {
		fix := setup(t, true)
		plan, err := resolveAndBuildPkMoveFolds(
			ctx, ses, nil, fix.tblStuff, diffSideTarget,
			nil, fix.view, nil, fix.claimed, timestamp.Timestamp{},
		)
		require.NoError(t, err)
		require.Nil(t, plan)

		plan, err = resolveAndBuildPkMoveFolds(
			ctx, ses, nil, fix.tblStuff, diffSideTarget,
			fix.provider, nil, nil, fix.claimed, timestamp.Timestamp{},
		)
		require.NoError(t, err)
		require.Nil(t, plan)
	})

	t.Run("two chains claiming the same replacement row abort the apply atomically", func(t *testing.T) {
		// The competing-claim defect posed at the plan boundary: both
		// the genuine lineage's anchor (key 2) and an anchor the walk
		// failed to separate (key 1) resolve to ONE replacement row.
		// Consuming that row for the fold that happens to pop first and
		// replaying the losing anchor as DELETE rewrites one lineage as
		// the other (the review's counterexample). Ownership must be
		// unique BEFORE any pop: the apply aborts statement-atomically.
		tblStuff := newTableStuff(t)
		claimed := newPkMoveClaimedKeys()
		finalKey := pkMoveTestKey(t, 3)
		linRid := buildHashDiffRowID(t, 52)
		dataHashmap := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{
			{rowID: linRid, id: 3, name: "three", hidden: "h", birth: types.BuildTS(50, 0)},
		})
		tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, nil)
		departed, tombs, _, err := capturePkMoveStreamEvidence(ctx, dataHashmap, tombstoneHashmap, 1)
		require.NoError(t, err)
		view := &pkMoveHashmapView{data: dataHashmap, tombs: tombs, departed: departed}
		t.Cleanup(func() {
			require.NoError(t, dataHashmap.Close())
			require.NoError(t, tombstoneHashmap.Close())
		})
		ret, err := dataHashmap.GetByEncodedKey(finalKey)
		require.NoError(t, err)
		require.True(t, ret.Exists)
		finalRow := append([]byte(nil), ret.Rows[0]...)

		dBat := lcaBat(t,
			[]any{int64(1), "lca-bee", "h"},
			[]any{int64(2), "lca-two", "h"})
		cand1 := pkMoveFoldCandidate{key: pkMoveTestKey(t, 1), rid: buildHashDiffRowID(t, 51),
			resolved: true, finalPK: finalKey, finalRow: finalRow}
		cand2 := pkMoveFoldCandidate{key: pkMoveTestKey(t, 2), rid: buildHashDiffRowID(t, 53),
			resolved: true, finalPK: finalKey, finalRow: finalRow}

		plan, err := assemblePkMoveFoldPlan(
			ctx, ses, tblStuff, view, claimed, dBat,
			[]pkMoveFoldCandidate{cand1, cand2}, nil,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "claim the same replacement")
		require.Nil(t, plan)

		// Statement-atomic: no pop consumed the replacement, nothing was
		// claimed, and the stream view is untouched.
		ret, err = view.data.GetByEncodedKey(finalKey)
		require.NoError(t, err)
		require.True(t, ret.Exists)
		require.False(t, claimed.has(finalKey))
	})

	t.Run("two chains claiming conflicting replacements at one key abort", func(t *testing.T) {
		// Two resolved chains ending at the same final key with
		// DIFFERENT rows is contradictory stream evidence (one committed
		// image per key per instant): never pick a winner by pop order.
		tblStuff := newTableStuff(t)
		claimed := newPkMoveClaimedKeys()
		finalKey := pkMoveTestKey(t, 3)
		dataHashmap := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{
			{rowID: buildHashDiffRowID(t, 52), id: 3, name: "three-a", hidden: "h", birth: types.BuildTS(50, 0)},
			{rowID: buildHashDiffRowID(t, 54), id: 3, name: "three-b", hidden: "h", birth: types.BuildTS(60, 0)},
		})
		tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, nil)
		departed, tombs, _, err := capturePkMoveStreamEvidence(ctx, dataHashmap, tombstoneHashmap, 1)
		require.NoError(t, err)
		view := &pkMoveHashmapView{data: dataHashmap, tombs: tombs, departed: departed}
		t.Cleanup(func() {
			require.NoError(t, dataHashmap.Close())
			require.NoError(t, tombstoneHashmap.Close())
		})
		ret, err := dataHashmap.GetByEncodedKey(finalKey)
		require.NoError(t, err)
		require.True(t, ret.Exists)
		require.Len(t, ret.Rows, 2)

		dBat := lcaBat(t,
			[]any{int64(1), "lca-bee", "h"},
			[]any{int64(2), "lca-two", "h"})
		cand1 := pkMoveFoldCandidate{key: pkMoveTestKey(t, 1), rid: buildHashDiffRowID(t, 51),
			resolved: true, finalPK: finalKey, finalRow: append([]byte(nil), ret.Rows[0]...)}
		cand2 := pkMoveFoldCandidate{key: pkMoveTestKey(t, 2), rid: buildHashDiffRowID(t, 53),
			resolved: true, finalPK: finalKey, finalRow: append([]byte(nil), ret.Rows[1]...)}

		plan, err := assemblePkMoveFoldPlan(
			ctx, ses, tblStuff, view, claimed, dBat,
			[]pkMoveFoldCandidate{cand1, cand2}, nil,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "claim conflicting replacement rows")
		require.Nil(t, plan)
	})

	t.Run("a vanished proven replacement aborts instead of degrading to a legacy delete", func(t *testing.T) {
		// With unique ownership enforced, a pop that finds nothing means
		// the claimed final row contradicted the stream it was resolved
		// from. Falling through to legacy replay would DELETE the
		// anchor's key — exactly the silent mis-apply the aborts guard.
		tblStuff := newTableStuff(t)
		claimed := newPkMoveClaimedKeys()
		finalKey := pkMoveTestKey(t, 3)
		linRid := buildHashDiffRowID(t, 52)
		dataHashmap := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{
			{rowID: linRid, id: 3, name: "three", hidden: "h", birth: types.BuildTS(50, 0)},
		})
		tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, nil)
		departed, tombs, _, err := capturePkMoveStreamEvidence(ctx, dataHashmap, tombstoneHashmap, 1)
		require.NoError(t, err)
		view := &pkMoveHashmapView{data: dataHashmap, tombs: tombs, departed: departed}
		t.Cleanup(func() {
			require.NoError(t, dataHashmap.Close())
			require.NoError(t, tombstoneHashmap.Close())
		})
		ret, err := dataHashmap.GetByEncodedKey(finalKey)
		require.NoError(t, err)
		require.True(t, ret.Exists)
		finalRow := append([]byte(nil), ret.Rows[0]...)

		// Simulate the contradiction: the row the walk claimed is gone
		// before the plan materializes.
		removed, err := view.data.PopByEncodedKeyValue(finalKey, finalRow, false)
		require.NoError(t, err)
		require.Equal(t, 1, removed)

		dBat := lcaBat(t, []any{int64(2), "lca-two", "h"})
		cand := pkMoveFoldCandidate{key: pkMoveTestKey(t, 2), rid: buildHashDiffRowID(t, 51),
			resolved: true, finalPK: finalKey, finalRow: finalRow}
		plan, err := assemblePkMoveFoldPlan(
			ctx, ses, tblStuff, view, claimed, dBat,
			[]pkMoveFoldCandidate{cand}, nil,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "proven replacement")
		require.Nil(t, plan)
	})
}

func TestRemoveKeyMovingOldKeys(t *testing.T) {
	oldKeys := [][]any{{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}}
	kept := removeKeyMovingOldKeys(oldKeys, []int64{1, 3})
	require.Equal(t, [][]any{{int64(1)}, {int64(3)}}, kept)

	require.Empty(t, removeKeyMovingOldKeys(oldKeys, []int64{0, 1, 2, 3}))
	require.Equal(t, oldKeys, removeKeyMovingOldKeys(oldKeys, nil))
}

func TestSortKeyMovingUpdateBat(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()

	newIntBatch := func(vals ...int64) *batch.Batch {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		for _, v := range vals {
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], v, false, mp))
		}
		bat.SetRowCount(len(vals))
		return bat
	}

	t.Run("reorders rows and old keys together", func(t *testing.T) {
		bat := newIntBatch(3, 1, 2)
		defer bat.Clean(mp)
		oldKeys := [][]any{{"old3"}, {"old1"}, {"old2"}}
		require.NoError(t, sortKeyMovingUpdateBat(bat, 0, mp, oldKeys))
		require.Equal(t, []int64{1, 2, 3}, vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0]))
		require.Equal(t, [][]any{{"old1"}, {"old2"}, {"old3"}}, oldKeys)
	})

	t.Run("float keys keep bit-level identity order and alignment", func(t *testing.T) {
		bat := batch.NewWithSize(1)
		defer bat.Clean(mp)
		bat.Vecs[0] = vector.NewVec(types.T_float64.ToType())
		// Distinct NaN payloads: Go's math.NaN() uses 0x7ff8000000000001, so
		// the literals below pick three genuinely different bit patterns.
		vals := []float64{
			2.5,
			math.Float64frombits(0x7ff4000000000000),
			1.5,
			math.Float64frombits(0x7ff80000000000ff),
		}
		for _, v := range vals {
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], v, false, mp))
		}
		bat.SetRowCount(len(vals))
		oldKeys := [][]any{{"two"}, {"snan"}, {"onehalf"}, {"nan"}}
		require.NoError(t, sortKeyMovingUpdateBat(bat, 0, mp, oldKeys))
		got := vector.MustFixedColNoTypeCheck[float64](bat.Vecs[0])
		// Whatever identity order the sort picked, every key must still sit
		// next to its own old-key entry.
		pairing := map[uint64]string{
			math.Float64bits(2.5): "two",
			0x7ff4000000000000:    "snan",
			math.Float64bits(1.5): "onehalf",
			0x7ff80000000000ff:    "nan",
		}
		for i, v := range got {
			require.Equal(t, pairing[math.Float64bits(v)], oldKeys[i][0])
		}
	})

	t.Run("cardinality mismatch is an error", func(t *testing.T) {
		bat := newIntBatch(1, 2)
		defer bat.Clean(mp)
		require.Error(t, sortKeyMovingUpdateBat(bat, 0, mp, [][]any{{"only"}}))
	})

	t.Run("single row is a no-op", func(t *testing.T) {
		bat := newIntBatch(9)
		defer bat.Clean(mp)
		oldKeys := [][]any{{"old9"}}
		require.NoError(t, sortKeyMovingUpdateBat(bat, 0, mp, oldKeys))
		require.Equal(t, []int64{9}, vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0]))
		require.Equal(t, [][]any{{"old9"}}, oldKeys)
	})
}

func TestKeyMovingApplyRanks(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()

	newStuff := func() tableStuff {
		tblStuff := tableStuff{}
		tblStuff.def.baseColNames = []string{"id", "note"}
		tblStuff.def.colTypes = []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}
		tblStuff.def.pkColIdxes = []int{0}
		return tblStuff
	}
	newBatch := func(vals ...int64) *batch.Batch {
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		for _, v := range vals {
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], v, false, mp))
			require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("n"), false, mp))
		}
		bat.SetRowCount(len(vals))
		return bat
	}

	t.Run("interacting moves rank in vacate order", func(t *testing.T) {
		// Regression for the topological edge direction: rows shifting
		// 1->2, 2->3, 3->4 must replay 3->4 first — every landing key is
		// vacated before the move needing it applies, no matter the
		// batch's arrival order.
		tblStuff := newStuff()
		bat := newBatch(2, 3, 4)
		defer bat.Clean(mp)
		wrapped := batchWithKind{
			batch: bat,
			keyMovingOldKeys: [][]any{
				{int64(1), nil},
				{int64(2), nil},
				{int64(3), nil},
			},
			keyMovingUpdate: true,
		}
		rank, err := keyMovingApplyRanks(ses, tblStuff, wrapped)
		require.NoError(t, err)
		require.Equal(t, []uint64{2, 1, 0}, rank)
	})

	t.Run("cyclic residue is a loud error", func(t *testing.T) {
		// The fold-plan pass excludes key rotations before emission, so an
		// apply batch still carrying a cycle means a producer bug; it must
		// surface loudly instead of being assigned an unsafe order.
		tblStuff := newStuff()
		bat := newBatch(2, 1)
		defer bat.Clean(mp)
		wrapped := batchWithKind{
			batch: bat,
			keyMovingOldKeys: [][]any{
				{int64(1), nil},
				{int64(2), nil},
			},
			keyMovingUpdate: true,
		}
		_, err := keyMovingApplyRanks(ses, tblStuff, wrapped)
		require.Error(t, err)
	})

	t.Run("retargeted moves rank on their effective where keys", func(t *testing.T) {
		// Pre-PR finding F1: both sides moved the same rows; the conflict
		// resolver installs WHERE overrides pointing at the destination
		// lineage keys. Source-side: 1->3, 2->4 with destination lineage
		// at 5 and 3, so the effective predicates are (5->3) and (3->4),
		// and the row landing on 3 must apply after the row vacating 3.
		// Ranking on the original old keys (1,2) sees no interaction and
		// orders 3 then 4 by new key, making the second UPDATE rewrite the
		// row the first one just installed.
		tblStuff := newStuff()
		bat := newBatch(3, 4)
		defer bat.Clean(mp)
		wrapped := batchWithKind{
			batch: bat,
			keyMovingOldKeys: [][]any{
				{int64(1), nil},
				{int64(2), nil},
			},
			keyMovingWhereKeys: [][]any{
				{int64(5), nil},
				{int64(3), nil},
			},
			keyMovingUpdate: true,
		}
		rank, err := keyMovingApplyRanks(ses, tblStuff, wrapped)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 0}, rank)
	})

	t.Run("retarget-introduced cycle is a loud error", func(t *testing.T) {
		// Old keys {1,2} do not interact, but the effective WHERE keys
		// {3,2} form a cycle with the new keys {2,3}: each move vacates
		// the key the other one predicates on. No in-place order exists,
		// so the ranker must fail closed rather than pick one silently.
		tblStuff := newStuff()
		bat := newBatch(2, 3)
		defer bat.Clean(mp)
		wrapped := batchWithKind{
			batch: bat,
			keyMovingOldKeys: [][]any{
				{int64(1), nil},
				{int64(2), nil},
			},
			keyMovingWhereKeys: [][]any{
				{int64(3), nil},
				{int64(2), nil},
			},
			keyMovingUpdate: true,
		}
		_, err := keyMovingApplyRanks(ses, tblStuff, wrapped)
		require.Error(t, err)
	})

	t.Run("where-key cardinality mismatch is a loud error", func(t *testing.T) {
		tblStuff := newStuff()
		bat := newBatch(2, 3)
		defer bat.Clean(mp)
		wrapped := batchWithKind{
			batch: bat,
			keyMovingOldKeys: [][]any{
				{int64(1), nil},
				{int64(2), nil},
			},
			keyMovingWhereKeys: [][]any{
				{int64(3), nil},
			},
			keyMovingUpdate: true,
		}
		_, err := keyMovingApplyRanks(ses, tblStuff, wrapped)
		require.Error(t, err)
	})

	t.Run("same-final-key rows apply in place", func(t *testing.T) {
		// Pre-PR finding F5: source and destination both moved the same
		// rows to the same final keys, so the conflict resolver installs
		// WHERE overrides equal to the NEW keys. Each row is then an
		// in-place update — it never waits for another row to vacate
		// anything, and counting it as its own prerequisite deadlocked
		// the scheduler into a false cyclic-plan error.
		tblStuff := newStuff()
		bat := newBatch(3, 4)
		defer bat.Clean(mp)
		wrapped := batchWithKind{
			batch: bat,
			keyMovingOldKeys: [][]any{
				{int64(1), nil},
				{int64(2), nil},
			},
			keyMovingWhereKeys: [][]any{
				{int64(3), nil},
				{int64(4), nil},
			},
			keyMovingUpdate: true,
		}
		rank, err := keyMovingApplyRanks(ses, tblStuff, wrapped)
		require.NoError(t, err)
		require.Equal(t, []uint64{0, 1}, rank)
	})

	t.Run("mixed in-place and moving rows still rank the movers", func(t *testing.T) {
		// The in-place exemption must not erode real cross-row ordering:
		// the row vacating key 6 (6->7) still applies before the row
		// landing on 6 (5->6), while the same-final-key row (3->3) waits
		// for nobody and is picked first on the new-key tie-break.
		tblStuff := newStuff()
		bat := newBatch(3, 7, 6)
		defer bat.Clean(mp)
		wrapped := batchWithKind{
			batch: bat,
			keyMovingOldKeys: [][]any{
				{int64(1), nil},
				{int64(2), nil},
				{int64(9), nil},
			},
			keyMovingWhereKeys: [][]any{
				{int64(3), nil},
				{int64(6), nil},
				{int64(5), nil},
			},
			keyMovingUpdate: true,
		}
		rank, err := keyMovingApplyRanks(ses, tblStuff, wrapped)
		require.NoError(t, err)
		require.Equal(t, []uint64{0, 1, 2}, rank)
	})

	t.Run("in-place rows do not dissolve a real cycle", func(t *testing.T) {
		// A same-final-key row rides along in a batch that still carries
		// a genuine two-row rotation (WHERE keys 3,2 against NEW keys
		// 2,3). The self-edge exemption covers only the in-place row; the
		// rotation residue must remain a loud producer-bug error.
		tblStuff := newStuff()
		bat := newBatch(2, 3, 5)
		defer bat.Clean(mp)
		wrapped := batchWithKind{
			batch: bat,
			keyMovingOldKeys: [][]any{
				{int64(1), nil},
				{int64(2), nil},
				{int64(4), nil},
			},
			keyMovingWhereKeys: [][]any{
				{int64(3), nil},
				{int64(2), nil},
				{int64(5), nil},
			},
			keyMovingUpdate: true,
		}
		_, err := keyMovingApplyRanks(ses, tblStuff, wrapped)
		require.Error(t, err)
	})
}

// BenchmarkKeyMovingApplyRanks scales the conflict-side apply ranker the
// way a both-branches-moved-many-rows reconciliation does: the fold batch
// reaches the ranker complete, so the scheduler's per-pick strategy
// dominates. The conflict-shaped (chain) variant additionally forces a
// single topological order, the case where a rescan-per-pick scheduler
// degrades worst. The F6 rewrite replaces that rescan with a push-driven
// min-heap over the same edges; this benchmark records the before/after.
func BenchmarkKeyMovingApplyRanks(b *testing.B) {
	ses := newValidateSession(b)
	mp := ses.proc.Mp()

	tblStuff := tableStuff{}
	tblStuff.def.baseColNames = []string{"id", "note"}
	tblStuff.def.colTypes = []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}
	tblStuff.def.pkColIdxes = []int{0}

	build := func(n int, chained bool) (batchWithKind, func()) {
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		oldKeys := make([][]any, 0, n)
		for i := 0; i < n; i++ {
			newKey := int64(i + n + 1)
			if chained {
				// Effective WHERE chain i+1 -> ... -> i+n: every row but
				// the last needs the next row to vacate first.
				newKey = int64(i + 1)
			}
			if err := vector.AppendFixed(bat.Vecs[0], newKey, false, mp); err != nil {
				b.Fatal(err)
			}
			if err := vector.AppendBytes(bat.Vecs[1], []byte("n"), false, mp); err != nil {
				b.Fatal(err)
			}
			oldKeys = append(oldKeys, []any{int64(i), nil})
		}
		bat.SetRowCount(n)
		wrapped := batchWithKind{
			batch:            bat,
			keyMovingOldKeys: oldKeys,
			keyMovingUpdate:  true,
		}
		return wrapped, func() { bat.Clean(mp) }
	}

	for _, tc := range []struct {
		name    string
		n       int
		chained bool
	}{
		{"independent-4096", 4096, false},
		{"independent-16384", 16384, false},
		{"chain-4096", 4096, true},
		{"chain-16384", 16384, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				wrapped, cleanup := build(tc.n, tc.chained)
				rank, err := keyMovingApplyRanks(ses, tblStuff, wrapped)
				if err != nil {
					b.Fatal(err)
				}
				if len(rank) != tc.n {
					b.Fatalf("ranked %d of %d rows", len(rank), tc.n)
				}
				cleanup()
			}
		})
	}
}

func TestDataBranchKeyMovingUpdateSQL(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	baseRel := mock_frontend.NewMockRelation(ctrl)
	baseRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		DbName: "db1",
		Name:   "t1",
	}).AnyTimes()

	newStuff := func() tableStuff {
		tblStuff := tableStuff{baseRel: baseRel}
		tblStuff.def.baseColNames = []string{"id", "note"}
		tblStuff.def.colTypes = []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}
		tblStuff.def.pkColIdxes = []int{0}
		tblStuff.def.writableIdxes = []int{0, 1}
		return tblStuff
	}

	t.Run("sets the new key and pins the old key", func(t *testing.T) {
		tblStuff := newStuff()
		var buf bytes.Buffer
		statements, err := dataBranchKeyMovingUpdateSQL(
			context.Background(), nil, tblStuff,
			[]any{int64(3), "three"},
			[]any{int64(2), nil},
			&buf,
		)
		require.NoError(t, err)
		require.Equal(t, []string{
			"update `db1`.`t1` set `id` = 3,`note` = 'three' where `id` = 2 limit 1",
		}, statements)
	})

	t.Run("composite old key predicate", func(t *testing.T) {
		tblStuff := newStuff()
		tblStuff.def.baseColNames = []string{"k1", "k2", "note"}
		tblStuff.def.colTypes = []types.Type{
			types.T_int64.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
		}
		tblStuff.def.pkColIdxes = []int{0, 1}
		tblStuff.def.writableIdxes = []int{0, 1, 2}
		var buf bytes.Buffer
		statements, err := dataBranchKeyMovingUpdateSQL(
			context.Background(), nil, tblStuff,
			[]any{int64(4), "d", "nn"},
			[]any{int64(2), "old", nil},
			&buf,
		)
		require.NoError(t, err)
		require.Equal(t, []string{
			"update `db1`.`t1` set `k1` = 4,`k2` = 'd',`note` = 'nn' " +
				"where `k1` = 2 and `k2` = 'old' limit 1",
		}, statements)
	})

	t.Run("no writable column is an error", func(t *testing.T) {
		tblStuff := newStuff()
		tblStuff.def.writableIdxes = nil
		var buf bytes.Buffer
		_, err := dataBranchKeyMovingUpdateSQL(
			context.Background(), nil, tblStuff,
			[]any{int64(3), "three"},
			[]any{int64(2), nil},
			&buf,
		)
		require.Error(t, err)
	})
}

func TestDataBranchDeleteRowByExactKeySQL(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	baseRel := mock_frontend.NewMockRelation(ctrl)
	baseRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		DbName: "db1",
		Name:   "t1",
	}).AnyTimes()

	tblStuff := tableStuff{baseRel: baseRel}
	tblStuff.def.baseColNames = []string{"id", "note"}
	tblStuff.def.colTypes = []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}
	tblStuff.def.pkColIdxes = []int{0}

	var buf bytes.Buffer
	sql, err := dataBranchDeleteRowByExactKeySQL(
		context.Background(), nil, tblStuff, []any{int64(3), "three"}, &buf,
	)
	require.NoError(t, err)
	require.Equal(t, "delete from `db1`.`t1` where `id` = 3 limit 1", sql)
}

func TestFormatPKValuesAsString(t *testing.T) {
	tblStuff := tableStuff{}
	tblStuff.def.colTypes = []types.Type{
		types.T_int64.ToType(), types.T_varchar.ToType(), types.T_int64.ToType(),
	}

	t.Run("single key", func(t *testing.T) {
		tblStuff.def.pkColIdxes = []int{0}
		key, err := formatPKValuesAsString(nil, tblStuff, []any{int64(42), "x", int64(9)})
		require.NoError(t, err)
		require.Equal(t, "42", key)
	})

	t.Run("composite key keeps extractPKAsString shape", func(t *testing.T) {
		tblStuff.def.pkColIdxes = []int{0, 1}
		key, err := formatPKValuesAsString(nil, tblStuff, []any{int64(1), "a", int64(9)})
		require.NoError(t, err)
		require.Equal(t, "1,'a'", key)
	})

	t.Run("null component renders NULL", func(t *testing.T) {
		tblStuff.def.pkColIdxes = []int{0, 1}
		key, err := formatPKValuesAsString(nil, tblStuff, []any{nil, "a", int64(9)})
		require.NoError(t, err)
		require.Equal(t, "NULL,'a'", key)
	})
}

// Ensure the fabricated probe rows and helpers stay in sync with the
// catalog projection; a widening mismatch would turn into confusing
// failures inside convertRowsIntoBatch otherwise.
func TestPkMoveLinkProbeProjectionWidth(t *testing.T) {
	rs := buildPkMoveLinkProbeResultSet()
	require.Len(t, rs.Columns, 7)
	require.True(t, strings.EqualFold("old_row_id", rs.Columns[0].Name()))
	require.True(t, strings.EqualFold("write_ts", rs.Columns[4].Name()))
	require.True(t, strings.EqualFold("txn_id", rs.Columns[5].Name()))
	require.True(t, strings.EqualFold("table_id", rs.Columns[6].Name()))
}

// Era-15 (r21 F5): the fold planner runs its verdict walk BEFORE any LCA
// hydration, probes the surviving anchors in maxTombstoneBatchCnt-row
// chunks, and retains only the before-image rows of resolved folds. A
// link-free window pays zero probe calls, and a window of plain deletes
// with one genuine move probes only that move's anchors instead of
// hydrating the whole diff.
func TestResolveAndBuildPkMoveFoldsBoundsTheLCAProbe(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	mp := ses.proc.Mp()

	// recordingProbe swaps the LCA anchor probe with a fixture that
	// records the encoded anchor keys of every call and returns an LCA
	// hit for each probed anchor.
	recordingProbe := func(t *testing.T) *[][]string {
		got := new([][]string)
		prev := pkMoveLCAAnchorProbe
		pkMoveLCAAnchorProbe = func(
			_ context.Context, probeSes *Session, _ BackgroundExec,
			in *batch.Batch, probeStuff tableStuff, _ timestamp.Timestamp,
		) (*batch.Batch, error) {
			dBat := probeStuff.retPool.acquireRetBatch(probeStuff, false)
			var keys []string
			for i := 0; i < in.RowCount(); i++ {
				pk := vector.MustFixedColNoTypeCheck[int64](in.Vecs[0])[i]
				require.NoError(t, vector.AppendFixed(dBat.Vecs[0], pk, false, probeSes.proc.Mp()))
				require.NoError(t, vector.AppendBytes(dBat.Vecs[1], []byte("lca"), false, probeSes.proc.Mp()))
				require.NoError(t, vector.AppendBytes(dBat.Vecs[2], []byte("h"), false, probeSes.proc.Mp()))
				keys = append(keys, string(in.Vecs[2].GetBytesAt(i)))
			}
			dBat.SetRowCount(in.RowCount())
			*got = append(*got, keys)
			return dBat, nil
		}
		t.Cleanup(func() { pkMoveLCAAnchorProbe = prev })
		return got
	}

	moveFixture := func(t *testing.T, seq int, oldKey, newKey int64, commit int64) (pkMoveLink, pkMoveStreamRow, pkMoveStreamRow) {
		anchorRid := buildHashDiffRowID(t, seq)
		lineageRid := buildHashDiffRowID(t, seq+40)
		link := pkMoveLink{
			oldRowID: anchorRid, newRowID: lineageRid, hasNewRowID: true,
			oldPK: pkMoveTestKey(t, oldKey), newPK: pkMoveTestKey(t, newKey),
			writeTS: types.BuildTS(10, 0),
		}
		tomb := pkMoveStreamRow{rowID: anchorRid, id: oldKey, birth: types.BuildTS(commit, 0)}
		live := pkMoveStreamRow{
			rowID: lineageRid, id: newKey, name: "n", hidden: "h",
			birth: types.BuildTS(commit, 0),
		}
		return link, tomb, live
	}

	t.Run("a link-free window probes nothing", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		tblStuff := newTestBranchTableStuff(ctrl)
		calls := recordingProbe(t)

		tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, []pkMoveStreamRow{
			{rowID: buildHashDiffRowID(t, 1), id: 7, birth: types.BuildTS(50, 0)},
			{rowID: buildHashDiffRowID(t, 2), id: 8, birth: types.BuildTS(50, 0)},
			{rowID: buildHashDiffRowID(t, 3), id: 9, birth: types.BuildTS(50, 0)},
		})
		defer func() { require.NoError(t, tombstoneHashmap.Close()) }()
		dataHashmap := buildPkMoveStreamHashmap(t, mp, nil)
		defer func() { require.NoError(t, dataHashmap.Close()) }()
		departed, tombs, capped, err := capturePkMoveStreamEvidence(ctx, dataHashmap, tombstoneHashmap, 1)
		require.NoError(t, err)
		require.False(t, capped)
		view := &pkMoveHashmapView{data: dataHashmap, tombs: tombs, departed: departed}

		plan, err := resolveAndBuildPkMoveFolds(
			ctx, ses, bh, tblStuff, diffSideBase,
			pkMoveWalkTestProvider(), view, tombstoneHashmap,
			newPkMoveClaimedKeys(), types.BuildTS(10, 0).ToTimestamp(),
		)
		require.NoError(t, err)
		require.Nil(t, plan)
		require.Empty(t, *calls,
			"no recorded update intent in window: the LCA probe must not run")
	})

	t.Run("the probe runs chunked at the tombstone batch bound", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		tblStuff := newTestBranchTableStuff(ctrl)
		tblStuff.maxTombstoneBatchCnt = 1
		calls := recordingProbe(t)

		linkA, tombA, liveA := moveFixture(t, 11, 2, 20, 50)
		linkB, tombB, liveB := moveFixture(t, 12, 3, 30, 60)
		linkC, tombC, liveC := moveFixture(t, 13, 4, 40, 70)
		provider := pkMoveWalkTestProvider(linkA, linkB, linkC)
		dataHashmap := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{liveA, liveB, liveC})
		tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, []pkMoveStreamRow{tombA, tombB, tombC})
		defer func() {
			require.NoError(t, dataHashmap.Close())
			require.NoError(t, tombstoneHashmap.Close())
		}()
		departed, tombs, _, err := capturePkMoveStreamEvidence(ctx, dataHashmap, tombstoneHashmap, 1)
		require.NoError(t, err)
		view := &pkMoveHashmapView{data: dataHashmap, tombs: tombs, departed: departed}

		plan, err := resolveAndBuildPkMoveFolds(
			ctx, ses, bh, tblStuff, diffSideBase,
			provider, view, tombstoneHashmap,
			newPkMoveClaimedKeys(), types.BuildTS(10, 0).ToTimestamp(),
		)
		require.NoError(t, err)
		require.NotNil(t, plan)
		defer releasePkMoveFoldPlan(tblStuff, plan)
		require.Len(t, plan.oldKeys, 3)
		require.Len(t, *calls, 3,
			"three survivors at a 1-row bound probe in three chunks")
		for i, got := range *calls {
			require.Len(t, got, 1, "chunk %d holds the bound's single row", i)
		}
	})

	t.Run("proven absences never reach the probe", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		tblStuff := newTestBranchTableStuff(ctrl)
		calls := recordingProbe(t)

		linkA, tombA, liveA := moveFixture(t, 21, 2, 20, 50)
		provider := pkMoveWalkTestProvider(linkA)
		dataHashmap := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{liveA})
		tombstoneHashmap := buildPkMoveTombstoneHashmap(t, mp, []pkMoveStreamRow{
			tombA,
			{rowID: buildHashDiffRowID(t, 22), id: 8, birth: types.BuildTS(51, 0)},
			{rowID: buildHashDiffRowID(t, 23), id: 9, birth: types.BuildTS(52, 0)},
		})
		defer func() {
			require.NoError(t, dataHashmap.Close())
			require.NoError(t, tombstoneHashmap.Close())
		}()
		departed, tombs, _, err := capturePkMoveStreamEvidence(ctx, dataHashmap, tombstoneHashmap, 1)
		require.NoError(t, err)
		view := &pkMoveHashmapView{data: dataHashmap, tombs: tombs, departed: departed}

		plan, err := resolveAndBuildPkMoveFolds(
			ctx, ses, bh, tblStuff, diffSideBase,
			provider, view, tombstoneHashmap,
			newPkMoveClaimedKeys(), types.BuildTS(10, 0).ToTimestamp(),
		)
		require.NoError(t, err)
		require.NotNil(t, plan)
		defer releasePkMoveFoldPlan(tblStuff, plan)
		require.Len(t, plan.oldKeys, 1)
		require.Len(t, *calls, 1)
		require.Equal(t, []string{string(pkMoveTestKey(t, 2))}, (*calls)[0],
			"only the genuine move's anchor reaches the LCA probe")
	})
}

// Era-15 (r21 F3): a restored key-moving row — the destination deleted the
// very lineage the source moved — replays as the authorized victim delete
// at the landed key plus a direct INSERT, never as an UPDATE at the vacated
// old key. The live delpick RED (accepted conflict missing its source row)
// is banked at /tmp/e15_red_live_f3.log.
func TestExecKeyMovingUpdateRestoreMissing(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tblStuff := newTestBranchTableStuff(ctrl)
	tblStuff.def.baseColNames = []string{"id", "name", "hidden"}

	row := []any{int64(3), "three", "h3"}
	whereRow := []any{int64(2), "two", "h2"}

	collect := func(t *testing.T) (stmts *[]string, writeFile func([]byte) error) {
		stmts = new([]string)
		writeFile = func(b []byte) error {
			*stmts = append(*stmts, string(b))
			return nil
		}
		return
	}

	t.Run("restore inserts at the landed key without a victim delete", func(t *testing.T) {
		stmts, writeFile := collect(t)
		// writeFile mode keeps bh untouched: the restore has no preflight,
		// so a nil BackgroundExec must survive.
		err := execKeyMovingUpdate(ctx, ses, nil, writeFile, tblStuff,
			row, whereRow, false, true, &bytes.Buffer{})
		require.NoError(t, err)
		require.Len(t, *stmts, 1)
		require.True(t, strings.HasPrefix((*stmts)[0], "insert into"),
			(*stmts)[0])
		require.Contains(t, (*stmts)[0], "`base`")
	})

	t.Run("restore keeps the authorized victim delete first", func(t *testing.T) {
		stmts, writeFile := collect(t)
		err := execKeyMovingUpdate(ctx, ses, nil, writeFile, tblStuff,
			row, whereRow, true, true, &bytes.Buffer{})
		require.NoError(t, err)
		require.Len(t, *stmts, 2)
		require.True(t, strings.HasPrefix((*stmts)[0], "delete from"),
			(*stmts)[0])
		require.Contains(t, (*stmts)[0], "= 3",
			"the victim delete pins the landed key")
		require.True(t, strings.HasPrefix((*stmts)[1], "insert into"),
			(*stmts)[1])
	})

	t.Run("non-restore moves keep the native UPDATE shape", func(t *testing.T) {
		stmts, writeFile := collect(t)
		err := execKeyMovingUpdate(ctx, ses, nil, writeFile, tblStuff,
			row, whereRow, false, false, &bytes.Buffer{})
		require.NoError(t, err)
		require.Len(t, *stmts, 1)
		require.True(t, strings.HasPrefix((*stmts)[0], "update "), (*stmts)[0])
		require.Contains(t, (*stmts)[0], " set ")
		require.Contains(t, (*stmts)[0], "= 2",
			"the predicate still pins the old key")
	})
}

// r60 review P1 (I4): the authorized victim delete at the move's NEW key
// fires ON DELETE actions on the destination — a self-referencing ON
// DELETE CASCADE edge from the move target onto the victim erases the
// very row the move is about to rewrite. The zero-match preflight runs
// after the victim delete inside the same transaction, so a cascaded-away
// target aborts the apply instead of letting a zero-row UPDATE commit the
// loss. The scripted destination below returns one row from the guard
// count only while the query precedes the delete (pre-fix order), so the
// cascade subtest is RED against the old ordering.
func TestExecKeyMovingUpdateVictimDeletePrecedesGuard(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tblStuff := newTestBranchTableStuff(ctrl)
	tblStuff.def.baseColNames = []string{"id", "name", "hidden"}

	row := []any{int64(3), "three", "h3"}
	whereRow := []any{int64(2), "two", "h2"}

	countResult := func(n int64) *MysqlResultSet {
		mrs := &MysqlResultSet{}
		col := &MysqlColumn{}
		col.SetName("count(*)")
		col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
		mrs.AddColumn(col)
		mrs.AddRow([]interface{}{n})
		return mrs
	}

	// cascadeErasesTarget scripts the destination's answer to the guard:
	// once the victim delete has executed, the cascade has removed the
	// move target, so the count reads zero.
	newGuardedBh := func(cascadeErasesTarget bool, executed *[]string) *mock_frontend.MockBackgroundExec {
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		deleted := false
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, sql string) error {
				*executed = append(*executed, sql)
				if strings.HasPrefix(sql, "delete from") {
					deleted = true
				}
				return nil
			}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			if len(*executed) == 0 {
				return nil
			}
			last := (*executed)[len(*executed)-1]
			if !strings.HasPrefix(last, "select count(*)") {
				return nil
			}
			if cascadeErasesTarget && deleted {
				return []interface{}{countResult(0)}
			}
			return []interface{}{countResult(1)}
		}).AnyTimes()
		bh.EXPECT().ClearExecResultSet().AnyTimes()
		return bh
	}

	t.Run("cascaded target aborts instead of updating zero rows", func(t *testing.T) {
		var executed []string
		bh := newGuardedBh(true, &executed)
		err := execKeyMovingUpdate(ctx, ses, bh, nil, tblStuff,
			row, whereRow, true, false, &bytes.Buffer{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot apply the accepted row")
		require.Len(t, executed, 2,
			"victim delete then settled guard; the UPDATE must never run")
		require.True(t, strings.HasPrefix(executed[0], "delete from"),
			"the authorized victim delete must execute first: %q", executed[0])
		require.True(t, strings.HasPrefix(executed[1], "select count(*)"),
			"the guard preflight follows the settled delete: %q", executed[1])
	})

	t.Run("survived target still applies the move", func(t *testing.T) {
		var executed []string
		bh := newGuardedBh(false, &executed)
		err := execKeyMovingUpdate(ctx, ses, bh, nil, tblStuff,
			row, whereRow, true, false, &bytes.Buffer{})
		require.NoError(t, err)
		require.Len(t, executed, 3)
		require.True(t, strings.HasPrefix(executed[0], "delete from"), executed[0])
		require.True(t, strings.HasPrefix(executed[1], "select count(*)"), executed[1])
		require.True(t, strings.HasPrefix(executed[2], "update "),
			"the move runs only after the settled guard counts one: %q", executed[2])
	})
}

// buildPkMovePickProbeResultSet renders link rows in the 4-column closure
// probe projection (old_row_id, old_pk, new_pk, write_ts) used by
// probePickWindowLinks and the overflow per-key fallback.
func buildPkMovePickProbeResultSet(rows ...[]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	for _, def := range []struct {
		name string
		typ  defines.MysqlType
	}{
		{"old_row_id", defines.MYSQL_TYPE_VARCHAR},
		{"old_pk", defines.MYSQL_TYPE_VARCHAR},
		{"new_pk", defines.MYSQL_TYPE_VARCHAR},
		{"write_ts", defines.MYSQL_TYPE_LONGLONG},
	} {
		mysqlCol := &MysqlColumn{}
		mysqlCol.SetName(def.name)
		mysqlCol.SetColumnType(def.typ)
		mrs.AddColumn(mysqlCol)
	}
	for _, row := range rows {
		mrs.AddRow(row)
	}
	return mrs
}

func pkMovePickProbeRow(t *testing.T, oldRid types.Rowid, oldPK, newPK []byte, writeTS uint64) []interface{} {
	return []interface{}{string(oldRid[:]), string(oldPK), string(newPK), int64(writeTS)}
}

// TestExpandPickKeysClosureOverflowPerKey covers the r25 pre-PR review
// finding 4: the closure's whole-window scan discards every row it read
// when the table's link history crosses the reader budget, and the
// expansion then stops entirely — so PICK KEYS(2) of a plain 2->3 move
// never admitted replacement key 3 once UNRELATED moves pushed the table
// past the cap, and the fold could not reassemble the moved row. The
// truncated window must fall back to per-frontier-key (sha, sha+1)
// prefix probes, exactly like the r23 provider narrowing: the selected
// move's dependency chain completes while unrelated history stays
// unread.
func TestExpandPickKeysClosureOverflowPerKey(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)

	oldCap := pkMoveLinkMaxCachedRows
	pkMoveLinkMaxCachedRows = 3
	defer func() { pkMoveLinkMaxCachedRows = oldCap }()

	window := pkMoveLinkTableWindow{tableID: 88, endPhys: 777}

	newHashmap := func(t *testing.T) databranchutils.BranchHashmap {
		hm, err := databranchutils.NewBranchHashmap(
			databranchutils.WithBranchHashmapShardCount(1))
		require.NoError(t, err)
		vec := vector.NewVec(types.T_int64.ToType())
		defer vec.Free(ses.proc.Mp())
		require.NoError(t, vector.AppendFixed(vec, int64(2), false, ses.proc.Mp()))
		require.NoError(t, hm.PutByVectors([]*vector.Vector{vec}, []int{0}))
		return hm
	}
	tblStuff := func(ctrl *gomock.Controller) tableStuff {
		ts := newTestBranchTableStuff(ctrl)
		ts.def.colTypes = []types.Type{types.T_int64.ToType()}
		ts.def.pkColIdx = 0
		ts.def.pkColIdxes = []int{0}
		return ts
	}
	mockTableExists := func() *MysqlResultSet {
		mrs := &MysqlResultSet{}
		mysqlCol := &MysqlColumn{}
		mysqlCol.SetName("relname")
		mysqlCol.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		mrs.AddColumn(mysqlCol)
		mrs.AddRow([]interface{}{catalog.MO_DATA_BRANCH_UPDATE_LINK})
		return mrs
	}

	t.Run("truncated window falls back to per-frontier-key probes", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		key2, key3 := pkMoveTestKey(t, 2), pkMoveTestKey(t, 3)
		truncated := make([][]interface{}, 0, 4)
		for i := 0; i < 4; i++ {
			truncated = append(truncated, pkMovePickProbeRow(t,
				buildHashDiffRowID(t, 20+i),
				pkMoveTestKey(t, 10+int64(i)), pkMoveTestKey(t, 30+int64(i)), 100))
		}
		var queries []string
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, sql string) error {
				queries = append(queries, sql)
				return nil
			},
		).Times(4)
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			switch len(queries) {
			case 1:
				// pkMoveLinkTableExists
				return []interface{}{mockTableExists()}
			case 2:
				// whole-window scan crosses the budget: 4 rows at LIMIT 4.
				return []interface{}{buildPkMovePickProbeResultSet(truncated...)}
			case 3:
				// per-key probe of the selected key answers the 2->3 move.
				return []interface{}{buildPkMovePickProbeResultSet(
					pkMovePickProbeRow(t, buildHashDiffRowID(t, 40), key2, key3, 100))}
			default:
				// per-key probe of the admitted key: no further history.
				return []interface{}{buildPkMovePickProbeResultSet()}
			}
		}).Times(4)
		bh.EXPECT().ClearExecResultSet().Times(4)

		hm := newHashmap(t)
		defer func() { require.NoError(t, hm.Close()) }()
		depSet := make(map[string]struct{})
		added, err := expandPickKeysWithPkMoveLinks(
			ctx, ses, bh, []pkMoveLinkTableWindow{window}, tblStuff(ctrl), hm, depSet)
		require.NoError(t, err)
		require.True(t, added,
			"the selected 2->3 move must complete despite unrelated overflow history")
		require.Contains(t, depSet, string(key3))
		require.Len(t, queries, 4,
			"one existence probe, one window attempt, one probe per frontier key")

		sha2 := sha256.Sum256(key2)
		require.Contains(t, queries[2], fmt.Sprintf("%x", sha2[:8]),
			"the fallback probes exactly the selected key's link_id prefix")
		sha3 := sha256.Sum256(key3)
		require.Contains(t, queries[3], fmt.Sprintf("%x", sha3[:8]),
			"round 2 probes the admitted replacement key's own prefix")
		// The truncated page's UNRELATED keys never enter the pick set.
		for i := 0; i < 4; i++ {
			require.NotContains(t, depSet, string(pkMoveTestKey(t, 30+int64(i))))
		}
		// The admitted key is materialized into the pick hashmap itself.
		probe := vector.NewVec(types.T_int64.ToType())
		defer probe.Free(ses.proc.Mp())
		require.NoError(t, vector.AppendFixed(probe, int64(3), false, ses.proc.Mp()))
		res, err := hm.GetByVectors([]*vector.Vector{probe})
		require.NoError(t, err)
		require.Len(t, res, 1)
		require.True(t, res[0].Exists,
			"replacement key 3 rides the pick set so the fold can reassemble the move")
	})

	t.Run("a per-key overflow poisons only that frontier key", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		key2 := pkMoveTestKey(t, 2)
		truncated := make([][]interface{}, 0, 4)
		for i := 0; i < 4; i++ {
			truncated = append(truncated, pkMovePickProbeRow(t,
				buildHashDiffRowID(t, 20+i),
				pkMoveTestKey(t, 10+int64(i)), pkMoveTestKey(t, 30+int64(i)), 100))
		}
		ownHistory := make([][]interface{}, 0, 4)
		for i := 0; i < 4; i++ {
			ownHistory = append(ownHistory, pkMovePickProbeRow(t,
				buildHashDiffRowID(t, 50+i),
				key2, pkMoveTestKey(t, 40+int64(i)), uint64(100+i)))
		}
		var queries []string
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, sql string) error {
				queries = append(queries, sql)
				return nil
			},
		).Times(3)
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			switch len(queries) {
			case 1:
				return []interface{}{mockTableExists()}
			case 2:
				return []interface{}{buildPkMovePickProbeResultSet(truncated...)}
			default:
				// The selected key's OWN history crosses the budget.
				return []interface{}{buildPkMovePickProbeResultSet(ownHistory...)}
			}
		}).Times(3)
		bh.EXPECT().ClearExecResultSet().Times(3)

		hm := newHashmap(t)
		defer func() { require.NoError(t, hm.Close()) }()
		depSet := make(map[string]struct{})
		added, err := expandPickKeysWithPkMoveLinks(
			ctx, ses, bh, []pkMoveLinkTableWindow{window}, tblStuff(ctrl), hm, depSet)
		require.NoError(t, err)
		require.False(t, added,
			"a key whose own history crosses the budget cannot expand: it fails closed")
		require.Empty(t, depSet)
		require.Len(t, queries, 3,
			"the poisoned key stops expansion: no round-2 probes are issued")
	})
}

// r51 review F1: a table shared by the source and destination branch
// paths registers one closure window per side; the surviving window must
// carry the GREATEST required endpoint so destination-side moves
// committed after the source (or BETWEEN-upper) snapshot stay visible to
// the fold, while first registration order is preserved. These windows
// scope closure reads only; each side's collect bounds are unaffected.
func TestAppendPkMoveLinkWindow(t *testing.T) {
	seen := make(map[uint64]int)
	var wins []pkMoveLinkTableWindow

	wins = appendPkMoveLinkWindow(wins, seen, 0, 100, branchIdentity{})
	require.Empty(t, wins, "table id 0 is never windowed")

	wins = appendPkMoveLinkWindow(wins, seen, 800, 100, branchIdentity{})
	wins = appendPkMoveLinkWindow(wins, seen, 900, 200, branchIdentity{})
	require.Equal(t, []pkMoveLinkTableWindow{
		{tableID: 800, endPhys: 100},
		{tableID: 900, endPhys: 200},
	}, wins)

	// Earlier duplicate: keeps the established (greater) endpoint.
	wins = appendPkMoveLinkWindow(wins, seen, 800, 50, branchIdentity{})
	require.Equal(t, uint64(100), wins[0].endPhys)

	// Later duplicate: raises the shared window to the greatest required
	// endpoint — first-wins dedup dropped exactly this destination bound.
	wins = appendPkMoveLinkWindow(wins, seen, 800, 300, branchIdentity{})
	require.Equal(t, uint64(300), wins[0].endPhys)

	// Registration order and the sibling window stay untouched, and the
	// seen index still addresses the raised window for later duplicates.
	require.Equal(t, []pkMoveLinkTableWindow{
		{tableID: 800, endPhys: 300},
		{tableID: 900, endPhys: 200},
	}, wins)
	wins = appendPkMoveLinkWindow(wins, seen, 700, 400, branchIdentity{})
	wins = appendPkMoveLinkWindow(wins, seen, 800, 300, branchIdentity{})
	require.Equal(t, []pkMoveLinkTableWindow{
		{tableID: 800, endPhys: 300},
		{tableID: 900, endPhys: 200},
		{tableID: 700, endPhys: 400},
	}, wins)
	require.Len(t, seen, 3)
}

// --- r58 review F1/F2: same-key tails screen recorded arrivals; arrival
// undecidability is scoped to the instants its evidence touches ---
//
// TestSameKeyTailChecksRecordedArrivals pins r58 review F1: the walk's
// same-key tail re-admitted the deleted key for a bare commit-timestamp
// claim without consulting incoming provenance, so a foreign lineage
// landing at exactly the death instant was adopted as the original
// lineage's re-birth (PICK cascaded children onto the foreign row).
// The shared incoming-provenance check now screens the tail: a RECORDED
// grounded arrival at the death instant preserves the original lineage's
// terminal deletion, INCOMPLETE evidence aborts the fold atomically, and
// only a proven-absence re-admits the key — where the stable-key tail
// controls prove the fold still continues.
func TestSameKeyTailChecksRecordedArrivals(t *testing.T) {
	t.Run("a proven foreign arrival preserves the original lineage's terminal deletion", func(t *testing.T) {
		// The review's counterexample at link/stream level: lineage A
		// moved 1->2 at commit 10; one later transaction at 20 deleted
		// A's landed row linkless AND moved lineage B 3->2 — the tail
		// must not adopt B's row as A's continuation.
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(1, 1, 2, 2, 10), // A: 1->2 at 10
			pkMoveWalkTestLink(3, 3, 2, 4, 20), // B: 3->2 at 20
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(1, 0x11, 1, 10), // A's origin deleted by the move
			pkMoveTombRow(2, 0x12, 2, 20), // A's landed version dies linkless at 20
			pkMoveDepartRow(2, 20, 10),    // ... born at 10
			pkMoveTombRow(3, 0x13, 3, 20), // B's origin ground: B really arrived at 2 at 20
			pkMoveLiveRow(2, 0xbb, 4, 20), // B's landed row at key 2, born 20
		)
		finalPK, finalRow, verdict := p.resolvePkMoveChainVerdict(view, []byte{1}, mustRowID(t, 1))
		require.Equal(t, pkMoveProvenDeleted, verdict,
			"a recorded foreign arrival at (2, 20) leaves A's lineage terminally deleted")
		require.Nil(t, finalPK)
		require.Nil(t, finalRow)
	})

	t.Run("a stable-key re-birth still folds the same-key tail", func(t *testing.T) {
		// Control the fix must not disturb: lineage C moved 4->5 at 30;
		// the deleting transaction at 50 linklessly deleted the landed
		// row and re-created the SAME key (records no move link): the
		// tail re-admits the key and the claim folds it like the legacy
		// stable-key matcher does.
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(4, 5, 5, 6, 30), // C: 4->5 at 30
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(4, 0x14, 5, 30),
			pkMoveTombRow(5, 0x15, 6, 50), // the landed version dies linkless at 50
			pkMoveDepartRow(6, 50, 30),    // ... born at 30
			pkMoveLiveRow(5, 0xcc, 7, 50), // the same-key re-birth at 50
		)
		finalPK, finalRow, verdict := p.resolvePkMoveChainVerdict(view, []byte{4}, mustRowID(t, 5))
		require.Equal(t, pkMoveResolved, verdict,
			"no recorded arrival at (5, 50): the tail claim folds the stable-key re-birth")
		require.Equal(t, []byte{5}, finalPK)
		require.Equal(t, []byte{0xcc}, finalRow)
	})

	t.Run("undecidable arrival evidence at the tail instant aborts the fold", func(t *testing.T) {
		// Same shape as the foreign-arrival case, but B's origin carries
		// TWO committed tombstones deleting its version — corrupt stream
		// evidence whose instant set IS the tail instant, so neither
		// accepting nor waiving the tail is sound.
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(1, 1, 2, 2, 10),
			pkMoveWalkTestLink(3, 3, 2, 4, 20),
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(1, 0x11, 1, 10),
			pkMoveTombRow(2, 0x12, 2, 20),
			pkMoveDepartRow(2, 20, 10),
			pkMoveTombRow(3, 0x13, 3, 20), // corrupt duplicate ...
			pkMoveTombRow(3, 0x14, 3, 20), // ... deleting one version
			pkMoveLiveRow(2, 0xbb, 4, 20),
		)
		_, _, verdict := p.resolvePkMoveChainVerdict(view, []byte{1}, mustRowID(t, 1))
		require.Equal(t, pkMoveUnresolved, verdict,
			"undecidable arrival evidence at (2, 20) fails the fold closed")
	})
}

// TestArrivalUndecidabilityScopesToTouchedInstants pins r58 review F2: a
// failed arrival attribution (ambiguous, contradictory, cyclic, over-cap
// or corrupt) poisoned EVERY birth instant queried at the key — including
// clean in-window moves folded long after the failing chain committed.
// Undecidability now disturbs exactly the instants the failed evidence
// touches: the instants of corrupt in-window tombstones and the failed
// candidate's own record instant; every other instant answers on the
// recorded evidence.
func TestArrivalUndecidabilityScopesToTouchedInstants(t *testing.T) {
	t.Run("an out-of-window over-cap chain cannot poison later instants", func(t *testing.T) {
		// A 40-hop same-transaction chain (write instant 40 — the
		// pre-fork history the review's reproducer reloads) whose
		// grounding tombstones lie outside the stream: backward
		// attribution exhausts the hop cap. Its transaction committed
		// OUTSIDE the window, so no in-window query can name its commit
		// instant: every instant answers clean, and the recorded write
		// instant itself — a creation time, never a commit instant —
		// must not be poisoned (r59 review F1).
		var links []pkMoveLink
		for i := 1; i <= 40; i++ {
			links = append(links,
				pkMoveWalkTestLink(byte(i), byte(i), byte(i+1), byte(i+1), 40))
		}
		p := pkMoveWalkTestProvider(links...)
		view := pkMoveWalkTestStubView() // no in-window committing evidence at all
		require.Equal(t, pkMoveArrivalNone,
			p.anchorArrivalVerdict(view, []byte{41}, types.BuildTS(50, 0)),
			"a later clean instant must not inherit the out-of-window chain's failure")
		require.Equal(t, pkMoveArrivalNone,
			p.anchorArrivalVerdict(view, []byte{41}, types.BuildTS(40, 0)),
			"the write timestamp is a creation time, never a commit instant (r59 review F1)")
	})

	t.Run("a truncated chain with an in-window origin poisons the commit instant, never the creation instant", func(t *testing.T) {
		// r59 review F1, restated under the D156 capture (r50 (B)): the
		// same 40-hop chain, but its origin's deleting tombstone commits
		// INSIDE the window (instant 60), beyond the attribution hop
		// cap, while the links carry the transaction's creation instant
		// 40. The capture feed marks the chain's scope committed at
		// exactly 60, so the failed attribution fails closed at that
		// instant — and ONLY there: the answer at the creation instant
		// stays clean, because creation time is not commit time.
		var links []pkMoveLink
		for i := 1; i <= 40; i++ {
			links = append(links,
				pkMoveWalkTestLink(byte(i), byte(i), byte(i+1), byte(i+1), 40))
		}
		p := pkMoveWalkTestProvider(links...)
		pkMoveWalkFeedCommits(p, pkMoveWalkTestScope(40), types.BuildTS(60, 0))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(1, 0x21, 1, 60), // the origin's deleting tombstone, beyond the hop cap
		)
		require.Equal(t, pkMoveArrivalIncomplete,
			p.anchorArrivalVerdict(view, []byte{41}, types.BuildTS(60, 0)),
			"the chain's feasible commit instant stays fail-closed")
		require.Equal(t, pkMoveArrivalNone,
			p.anchorArrivalVerdict(view, []byte{41}, types.BuildTS(40, 0)),
			"the recorded creation instant is not a commit instant (r59 review F1)")
		require.Equal(t, pkMoveArrivalNone,
			p.anchorArrivalVerdict(view, []byte{41}, types.BuildTS(50, 0)),
			"an instant between creation and commit answers clean: the origin's tombstone pins the only feasible commit")
		require.Equal(t, pkMoveArrivalNone,
			p.anchorArrivalVerdict(view, []byte{41}, types.BuildTS(70, 0)),
			"an unrelated later instant answers clean")
	})

	t.Run("a truncated chain with an arrival-key legacy death poisons exactly the commit instant", func(t *testing.T) {
		// r59 review F1's live state, restated under the D156 capture:
		// hop-cap truncation with the origin's grounding tombstone
		// pruned out of the observed window (a PICK's closure-founded
		// row filter reaches only closure keys, and the depth-capped
		// closure stops before the origin). The capture feed marks the
		// chain's scope committed at exactly 60 — the arrival instant
		// the transaction vacuumed the landed key's previous committed
		// occupant — so the failed attribution fails closed there and
		// only there (r50 (B)).
		var links []pkMoveLink
		for i := 1; i <= 40; i++ {
			links = append(links,
				pkMoveWalkTestLink(byte(i), byte(i), byte(i+1), byte(i+1), 40))
		}
		p := pkMoveWalkTestProvider(links...)
		pkMoveWalkFeedCommits(p, pkMoveWalkTestScope(40), types.BuildTS(60, 0))
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(41, 0x51, 0x51, 60), // the landed key's previous occupant vacuumed at 60
			pkMoveDepartRow(0x51, 60, 30),     // ... born at 30, before the transaction's creation
		)
		require.Equal(t, pkMoveArrivalIncomplete,
			p.anchorArrivalVerdict(view, []byte{41}, types.BuildTS(60, 0)),
			"the captured scope->commit record names the chain's exact commit instant")
		require.Equal(t, pkMoveArrivalNone,
			p.anchorArrivalVerdict(view, []byte{41}, types.BuildTS(40, 0)),
			"the recorded creation instant is not a commit instant (r59 review F1)")
		require.Equal(t, pkMoveArrivalNone,
			p.anchorArrivalVerdict(view, []byte{41}, types.BuildTS(50, 0)),
			"an instant between creation and commit answers clean")
		require.Equal(t, pkMoveArrivalNone,
			p.anchorArrivalVerdict(view, []byte{41}, types.BuildTS(70, 0)),
			"an unrelated later instant answers clean")
	})

	t.Run("the same truncated chain without a capture record answers clean at the legacy death instant", func(t *testing.T) {
		// The D156 counterexample pair's other half (r50 (B)): the
		// identical stream shape with NO capture record for the chain's
		// scope — the scope committed OUTSIDE the apply windows — so
		// the arrival-key legacy death names NOTHING and the fold
		// proceeds. Absence is silence; only a recorded commit
		// undecides the tie.
		var links []pkMoveLink
		for i := 1; i <= 40; i++ {
			links = append(links,
				pkMoveWalkTestLink(byte(i), byte(i), byte(i+1), byte(i+1), 40))
		}
		p := pkMoveWalkTestProvider(links...)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(41, 0x51, 0x51, 60),
			pkMoveDepartRow(0x51, 60, 30),
		)
		require.Equal(t, pkMoveArrivalNone,
			p.anchorArrivalVerdict(view, []byte{41}, types.BuildTS(60, 0)),
			"an outside-window commit names no in-window instant")
		require.Equal(t, pkMoveArrivalNone,
			p.anchorArrivalVerdict(view, []byte{41}, types.BuildTS(40, 0)))
	})

	t.Run("an arrival-key death of the chain's own generation names nothing", func(t *testing.T) {
		// The discrimination control: the same truncated chain, but the
		// landed key's tombstone deletes a version born AT-or-after the
		// transaction's creation (a staged-and-replaced generation or a
		// later replacement of the landed image). It cannot be the
		// previous committed occupant's vacuum, so it never names a
		// feasible commit instant.
		var links []pkMoveLink
		for i := 1; i <= 40; i++ {
			links = append(links,
				pkMoveWalkTestLink(byte(i), byte(i), byte(i+1), byte(i+1), 40))
		}
		p := pkMoveWalkTestProvider(links...)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(41, 0x61, 0x61, 70), // a later replacement vacuums the landed image at 70
			pkMoveDepartRow(0x61, 70, 60),     // ... the landed image, born at 60 (after creation 40)
		)
		require.Equal(t, pkMoveArrivalNone,
			p.anchorArrivalVerdict(view, []byte{41}, types.BuildTS(70, 0)),
			"a replacement death of the landed image must not be attributed to the chain's commit")
		require.Equal(t, pkMoveArrivalNone,
			p.anchorArrivalVerdict(view, []byte{41}, types.BuildTS(60, 0)),
			"the landed image's own birth stays clean when no occupant-vacuum evidence exists")
	})

	t.Run("an unpaired arrival-key death names nothing", func(t *testing.T) {
		// The pre-window landing lane (the hpo merge control): the
		// landed key's tombstone deletes a version whose birth insert
		// lies outside the observed window — a pre-window chain's
		// landed image dying to a later replacement — so the departed-
		// birth index cannot pair it. That shape is indistinguishable
		// from a pre-window occupant's vacuum; both attach to the
		// re-born instant, and poisoning there would falsely abort a
		// fold that must succeed, so an unpaired death names nothing.
		var links []pkMoveLink
		for i := 1; i <= 40; i++ {
			links = append(links,
				pkMoveWalkTestLink(byte(i), byte(i), byte(i+1), byte(i+1), 40))
		}
		p := pkMoveWalkTestProvider(links...)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(41, 0x71, 0x71, 60), // edit death at 60, birth out of window
		)
		require.Equal(t, pkMoveArrivalNone,
			p.anchorArrivalVerdict(view, []byte{41}, types.BuildTS(60, 0)),
			"an unpaired collision death leaves the re-born instant decidable")
		require.Equal(t, pkMoveArrivalNone,
			p.anchorArrivalVerdict(view, []byte{41}, types.BuildTS(40, 0)),
			"creation time stays clean")
	})

	t.Run("corrupt ground evidence poisons exactly its in-window instants", func(t *testing.T) {
		// Two committed tombstones deleting the landed link's vacated
		// version: the arrival's own instant is undecidable at exactly
		// those in-window candidates, and only there.
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(8, 8, 9, 9, 20), // landed at 9, recorded at 20
		)
		view := pkMoveWalkTestStubView(
			pkMoveTombRow(8, 0x18, 8, 21), // corrupt: two committed tombstones ...
			pkMoveTombRow(8, 0x19, 8, 22), // ... deleting one version
		)
		require.Equal(t, pkMoveArrivalIncomplete,
			p.anchorArrivalVerdict(view, []byte{9}, types.BuildTS(21, 0)))
		require.Equal(t, pkMoveArrivalIncomplete,
			p.anchorArrivalVerdict(view, []byte{9}, types.BuildTS(22, 0)))
		require.Equal(t, pkMoveArrivalNone,
			p.anchorArrivalVerdict(view, []byte{9}, types.BuildTS(20, 0)),
			"the link's recorded instant is its creation time, no commit instant (r59 review F1)")
		require.Equal(t, pkMoveArrivalNone,
			p.anchorArrivalVerdict(view, []byte{9}, types.BuildTS(23, 0)),
			"an untouched instant answers clean")
	})
}

// TestSameKeyTailFailsClosedPastTruncatedForeignArrival pins r59 review
// F1 at walk level — the review's counterexample: lineage A moved 1->2
// at commit instant 10; one later transaction (created at instant 15,
// committed at 20) deleted A's landed row at 2 linkless AND moved
// lineage B 3->4->...->42->2 through 40 hops. B's arrival attribution
// truncates at the hop cap even though its origin's grounding tombstone
// commits in-window (beyond the cutoff), so its feasible in-window
// commit instants stay undecidable and A's tail at its own death instant
// must fail CLOSED — an atomic apply rejection — instead of adopting
// B's foreign row as A's continuation and cascading children onto it.
func TestSameKeyTailFailsClosedPastTruncatedForeignArrival(t *testing.T) {
	links := []pkMoveLink{
		pkMoveWalkTestLink(1, 1, 2, 2, 10), // A: 1->2 at 10
	}
	for i := 3; i <= 42; i++ {
		// B: 3->4->...->42->2, forty hops recorded at the transaction's
		// creation instant 15, committing at 20.
		newKey, newRid := byte(i+1), byte(i+1)
		if i == 42 {
			newKey, newRid = 2, 0xc0
		}
		links = append(links, pkMoveWalkTestLink(byte(i), byte(i), newKey, newRid, 15))
	}
	p := pkMoveWalkTestProvider(links...)
	// D156: B's scope is captured committed in-window at 20 — the
	// arrival instant its transaction vacuumed A's landed row at key 2 —
	// so the tail's failed attribution fails closed at exactly (2, 20).
	pkMoveWalkFeedCommits(p, pkMoveWalkTestScope(15), types.BuildTS(20, 0))
	view := pkMoveWalkTestStubView(
		pkMoveTombRow(1, 0x11, 1, 10),    // A's origin deleted by its move
		pkMoveTombRow(2, 0x12, 2, 20),    // A's landed version dies linkless at 20
		pkMoveDepartRow(2, 20, 10),       // ... born at 10
		pkMoveLiveRow(2, 0xbb, 0xc0, 20), // B's landed row at key 2, born 20
	)
	// Live, a PICK's closure-founded row filter prunes the picked
	// change scan to closure keys, and the depth-capped closure never
	// reached B's origin key 3 through 40 hops: no grounding tombstone
	// for B's chain is visible at all (the exact e59 live-RED state).
	// B's transaction still vacuumed A's landed row at key 2 inside its
	// own commit image, and the capture records that scope committed at
	// the vacuum instant exactly.
	finalPK, finalRow, verdict := p.resolvePkMoveChainVerdict(view, []byte{1}, mustRowID(t, 1))
	require.Equal(t, pkMoveUnresolved, verdict,
		"truncated foreign arrival evidence at (2, 20) fails the tail fold closed")
	require.Nil(t, finalPK)
	require.Nil(t, finalRow)
}

// TestAnchorContinuityPastPreWindowArrivalFailure pins r58 review F2's
// walk-level reproducer: a long pre-fork chain lands at key 41 with no
// in-window grounding; AFTER the fork a payload edit re-births 41 at 50
// and the 41->42 move at 60 anchors. The anchor's continuity tie queries
// arrival evidence at the EDIT instant, which the old chain cannot touch:
// the one-hop in-window move resolves natively instead of inheriting the
// historical chain's cap failure.
func TestAnchorContinuityPastPreWindowArrivalFailure(t *testing.T) {
	var links []pkMoveLink
	for i := 1; i <= 40; i++ {
		links = append(links,
			pkMoveWalkTestLink(byte(i), byte(i), byte(i+1), byte(i+1), 40))
	}
	links = append(links,
		pkMoveWalkTestLink(41, 0xA1, 42, 0xA2, 60)) // the post-fork one-hop move
	p := pkMoveWalkTestProvider(links...)
	view := pkMoveWalkTestStubView(
		pkMoveTombRow(41, 0x51, 0xA0, 50), // the payload edit deletes the old 41 at 50 —
		// unpaired: that image was born at the pre-window chain's own
		// commit, outside the observed window (the live merge merge
		// never sees its insert), so collision evidence cannot pair it.
		pkMoveTombRow(41, 0x52, 0xA1, 60), // the move's anchor death at 60
		pkMoveDepartRow(0xA1, 60, 50),     // the edited version: born 50, died 60
		pkMoveLiveRow(42, 0xdd, 0xA2, 60), // the moved row at 42, born 60
	)
	finalPK, finalRow, verdict := p.resolvePkMoveChainVerdict(
		view, []byte{41}, mustRowID(t, 0xA1))
	require.Equal(t, pkMoveResolved, verdict,
		"the in-window one-hop move has complete evidence and must resolve natively")
	require.Equal(t, []byte{42}, finalPK)
	require.Equal(t, []byte{0xdd}, finalRow)
}

// ---------------------------------------------------------------------------
// D156 (r50-r53): the scope->commit capture over the link relation replaces
// the r49 approximation channels; these tests pin the capture contract.

// The capture bounds mirror the collector's full-types.TS from/end, with
// the r52 zero-start normalization sending the reserved empty instant to
// [MinTs().Next(), end] and the unit-fixture fallback reading unbounded.
func TestPkMoveLinkWindowCaptureBounds(t *testing.T) {
	end := types.BuildTS(90, 3)
	zero := types.MinTs()
	from, got := pkMoveLinkTableWindow{tableID: 7, end: end}.captureBounds()
	require.Equal(t, zero.Next(), from,
		"a whole-history window never passes the reserved zero instant")
	require.Equal(t, end, got)

	exact := types.BuildTS(50, 2)
	from, got = pkMoveLinkTableWindow{tableID: 7, from: exact, end: end}.captureBounds()
	require.Equal(t, exact, from, "a per-generation lower bound mirrors verbatim")
	require.Equal(t, end, got)

	from, got = pkMoveLinkTableWindow{tableID: 7}.captureBounds()
	require.Equal(t, zero.Next(), from)
	require.Equal(t, types.MaxTs(), got, "the no-recorded-bounds fallback is unbounded")
}

// A recorded scope names exactly its commit instant at the failed
// attribution; repeats of the same record dedupe across window
// boundaries (adjacent generations share one instant).
func TestArrivalCaptureFeedBindingAndDedupe(t *testing.T) {
	scope := pkMoveWalkTestScope(40)
	commit := types.BuildTS(60, 0)

	p := pkMoveWalkTestProvider(pkMoveWalkTestLink(1, 1, 2, 2, 40))
	p.windows = []pkMoveLinkTableWindow{{tableID: 1}, {tableID: 2}}
	p.fullWindows = map[uint64][2]uint64{1: {}, 2: {}}
	feedCalls := 0
	p.streamLinkCommits = func(
		ctx context.Context, w pkMoveLinkTableWindow, from, end types.TS,
		accept func(scope pkMoveTxnScope, cts types.TS) error,
	) error {
		feedCalls++
		// Both adjacent generations observed the same boundary commit.
		if err := accept(scope, commit); err != nil {
			return err
		}
		return accept(scope, commit)
	}
	truncated := pkMoveWalkTestProvider(
		pkMoveWalkTestLink(1, 1, 2, 2, 40),
		pkMoveWalkTestLink(2, 2, 3, 3, 40),
		pkMoveWalkTestLink(3, 3, 4, 4, 40),
		pkMoveWalkTestLink(4, 4, 5, 5, 40),
		pkMoveWalkTestLink(5, 5, 6, 6, 40),
		pkMoveWalkTestLink(6, 6, 7, 7, 40),
		pkMoveWalkTestLink(7, 7, 8, 8, 40),
		pkMoveWalkTestLink(8, 8, 9, 9, 40),
		pkMoveWalkTestLink(9, 9, 10, 10, 40),
		pkMoveWalkTestLink(10, 10, 11, 11, 40),
		pkMoveWalkTestLink(11, 11, 12, 12, 40),
		pkMoveWalkTestLink(12, 12, 13, 13, 40),
		pkMoveWalkTestLink(13, 13, 14, 14, 40),
		pkMoveWalkTestLink(14, 14, 15, 15, 40),
		pkMoveWalkTestLink(15, 15, 16, 16, 40),
		pkMoveWalkTestLink(16, 16, 17, 17, 40),
		pkMoveWalkTestLink(17, 17, 18, 18, 40),
		pkMoveWalkTestLink(18, 18, 19, 19, 40),
		pkMoveWalkTestLink(19, 19, 20, 20, 40),
		pkMoveWalkTestLink(20, 20, 21, 21, 40),
		pkMoveWalkTestLink(21, 21, 22, 22, 40),
		pkMoveWalkTestLink(22, 22, 23, 23, 40),
		pkMoveWalkTestLink(23, 23, 24, 24, 40),
		pkMoveWalkTestLink(24, 24, 25, 25, 40),
		pkMoveWalkTestLink(25, 25, 26, 26, 40),
		pkMoveWalkTestLink(26, 26, 27, 27, 40),
		pkMoveWalkTestLink(27, 27, 28, 28, 40),
		pkMoveWalkTestLink(28, 28, 29, 29, 40),
		pkMoveWalkTestLink(29, 29, 30, 30, 40),
		pkMoveWalkTestLink(30, 30, 31, 31, 40),
		pkMoveWalkTestLink(31, 31, 32, 32, 40),
		pkMoveWalkTestLink(32, 32, 33, 33, 40),
		pkMoveWalkTestLink(33, 33, 34, 34, 40),
		pkMoveWalkTestLink(34, 34, 35, 35, 40),
		pkMoveWalkTestLink(35, 35, 36, 36, 40),
		pkMoveWalkTestLink(36, 36, 37, 37, 40),
		pkMoveWalkTestLink(37, 37, 38, 38, 40),
		pkMoveWalkTestLink(38, 38, 39, 39, 40),
		pkMoveWalkTestLink(39, 39, 40, 40, 40),
		pkMoveWalkTestLink(40, 40, 41, 41, 40),
	)
	// The truncated 40-hop chain forces the failed-attribution lane,
	// where the capture record decides the tie.
	truncated.windows = p.windows
	truncated.fullWindows = p.fullWindows
	truncated.streamLinkCommits = p.streamLinkCommits

	view := pkMoveWalkTestStubView()
	require.Equal(t, pkMoveArrivalIncomplete,
		truncated.anchorArrivalVerdict(view, []byte{41}, commit),
		"the recorded scope names exactly its commit instant")
	require.Equal(t, pkMoveArrivalNone,
		truncated.anchorArrivalVerdict(view, []byte{41}, types.BuildTS(40, 0)),
		"a scope's create instant is not a commit instant")
	require.Equal(t, pkMoveArrivalNone,
		truncated.anchorArrivalVerdict(view, []byte{41}, types.BuildTS(70, 0)),
		"unrecorded instants stay clean")
	require.False(t, truncated.arrivalCapFailed, "boundary duplicates dedupe, never conflict")
	require.Equal(t, 2, feedCalls,
		"one build streams each registered window exactly once (D157)")
	// Repeated verdicts reuse the build's index without re-capture.
	require.Equal(t, pkMoveArrivalIncomplete,
		truncated.anchorArrivalVerdict(view, []byte{41}, commit))
	require.Equal(t, 2, feedCalls, "a fresh build is not issued per query")
}

// Encoded scopes are not admitted: the capture filters retained state to
// scopes the provider's link cache actually asks about (r51 (G)(ii)).
func TestArrivalCaptureAdmissionFiltersUnrelatedScopes(t *testing.T) {
	p := pkMoveWalkTestProvider(pkMoveWalkTestLink(1, 1, 2, 2, 40))
	pkMoveWalkFeedCommits(p, pkMoveWalkTestScope(41), types.BuildTS(60, 0))
	p.captureArrivalCommitsLocked()
	require.False(t, p.arrivalCapFailed)
	require.NotNil(t, p.arrivalCommitIdx)
	require.Empty(t, p.arrivalCommitIdx.byTSNone,
		"scope 41 is not in the provider's cache, so the record is streamed and dropped")
}

// A context that could yield synthetic stamps is rejected outright:
// visible-state policy or recovery resources both disqualify the
// capture (r51 (E)), poisoning the build wholesale.
func TestArrivalCaptureGuardRejectsDirtyContext(t *testing.T) {
	for name, ctx := range map[string]context.Context{
		"visible-state policy": engine.WithSnapshotReadPolicy(
			context.Background(), engine.SnapshotReadPolicyVisibleState),
		"recovery resources": engine.WithVisibleStateRecoveryResources(
			context.Background(), &dataBranchVisibleStateRecoveryResources{}),
	} {
		t.Run(name, func(t *testing.T) {
			p := pkMoveWalkTestProvider(pkMoveWalkTestLink(1, 1, 2, 2, 40))
			p.ctx = ctx
			p.captureArrivalCommitsLocked()
			require.True(t, p.arrivalCapFailed,
				"a dirty capture context can never certify recorded commits")
			require.Nil(t, p.arrivalCommitIdx)
		})
	}
}

// A scope observed committing at two instants is corruption; the build
// fails closed wholesale — every tie query of that build is Incomplete,
// including queries the conflict never touched (r50 (C)).
func TestArrivalCaptureConflictPoisonsBuild(t *testing.T) {
	scope := pkMoveWalkTestScope(40)
	p := pkMoveWalkTestProvider(pkMoveWalkTestLink(1, 1, 2, 2, 40))
	p.windows = []pkMoveLinkTableWindow{{tableID: 1}}
	p.fullWindows = map[uint64][2]uint64{1: {}}
	p.streamLinkCommits = func(
		ctx context.Context, w pkMoveLinkTableWindow, from, end types.TS,
		accept func(scope pkMoveTxnScope, cts types.TS) error,
	) error {
		if err := accept(scope, types.BuildTS(60, 0)); err != nil {
			return err
		}
		return accept(scope, types.BuildTS(70, 0))
	}
	view := pkMoveWalkTestStubView()
	require.Equal(t, pkMoveArrivalIncomplete,
		p.anchorArrivalVerdict(view, []byte{2}, types.BuildTS(60, 0)))
	require.Equal(t, pkMoveArrivalIncomplete,
		p.anchorArrivalVerdict(view, []byte{99}, types.BuildTS(99, 0)),
		"a poisoned build answers no query, even one the corruption never touched")
	require.True(t, p.arrivalCapFailed)
}

// Capacity exhaustion takes the approved lane: the provider disables
// with the capacity reason instead of adopting anything (r53 (G)(v)).
func TestArrivalCaptureCapacityDisablesProvider(t *testing.T) {
	p := pkMoveWalkTestProvider(pkMoveWalkTestLink(1, 1, 2, 2, 40))
	p.windows = []pkMoveLinkTableWindow{{tableID: 1}}
	p.fullWindows = map[uint64][2]uint64{1: {}}
	p.streamLinkCommits = func(
		ctx context.Context, w pkMoveLinkTableWindow, from, end types.TS,
		accept func(scope pkMoveTxnScope, cts types.TS) error,
	) error {
		return engine.NewChangeRangeCapacityError("fabric overflow")
	}
	p.captureArrivalCommitsLocked()
	require.True(t, p.disabled, "capacity disables the provider")
	require.False(t, p.arrivalCapFailed,
		"the disable lane replaces build poisoning for capacity")
	require.Empty(t, p.byPK, "the disable releases all caches")
}

// Cancellation mid-capture poisons the build (D157); nothing partial is
// adopted and the next build retries cleanly.
func TestArrivalCaptureCancellationPoisonsBuild(t *testing.T) {
	scope := pkMoveWalkTestScope(40)
	ctx, cancel := context.WithCancel(context.Background())
	p := pkMoveWalkTestProvider(pkMoveWalkTestLink(1, 1, 2, 2, 40))
	p.ctx = ctx
	p.windows = []pkMoveLinkTableWindow{{tableID: 1}}
	p.fullWindows = map[uint64][2]uint64{1: {}}
	p.streamLinkCommits = func(
		ctx context.Context, w pkMoveLinkTableWindow, from, end types.TS,
		accept func(scope pkMoveTxnScope, cts types.TS) error,
	) error {
		if err := accept(scope, types.BuildTS(60, 0)); err != nil {
			return err
		}
		cancel()
		return ctx.Err()
	}
	p.captureArrivalCommitsLocked()
	require.True(t, p.arrivalCapFailed,
		"a cancelled capture adopts no partial index")
	require.Nil(t, p.arrivalCommitIdx)
}

// pkMoveArrivalCancelAfterChecksCtx is a deterministic cancellation
// fabric: the first `after` Done() observations report a live context,
// every later one reports cancellation. The arrival build observes ctx
// only at capture-batch and construction iteration boundaries (one
// Done() read per check), so counting those reads selects the exact
// build phase that trips.
type pkMoveArrivalCancelAfterChecksCtx struct {
	context.Context
	after  int
	checks int
	closed chan struct{}
}

func (c *pkMoveArrivalCancelAfterChecksCtx) Done() <-chan struct{} {
	c.checks++
	if c.checks > c.after {
		return c.closed
	}
	return nil
}

func (c *pkMoveArrivalCancelAfterChecksCtx) Err() error {
	if c.checks > c.after {
		return context.Canceled
	}
	return c.Context.Err()
}

// Cancellation AFTER a successful capture — at the grouping walk, mid
// grouping, and inside one attribution's chain hop (D157 (A)) — aborts
// the arrival build before the index publishes: bounded remaining work,
// no partial verdict state adopted, fail-safe answers, and a clean
// wholesale retry once the provider context is live again.
func TestArrivalConstructionCancellationAbortsBuild(t *testing.T) {
	scope := pkMoveWalkTestScope(40)
	newProvider := func() *pkMoveLinkProvider {
		p := pkMoveWalkTestProvider(
			pkMoveWalkTestLink(1, 1, 2, 2, 40),
			pkMoveWalkTestLink(2, 3, 3, 4, 40),
		)
		pkMoveWalkFeedCommits(p, scope, types.BuildTS(60, 0))
		return p
	}
	view := pkMoveWalkTestStubView()
	const totalLinks = uint64(2)
	// The fabric admits exactly one record, so capture reads Done()
	// twice (one per admitted record, one final boundary) before any
	// construction check runs.
	const captureChecks = 2

	// Control: the same fixture with a permanently live context builds
	// and serves wholesale.
	control := newProvider()
	control.ctx = context.Background()
	controlVerdict := control.anchorArrivalVerdict(view, []byte{2}, types.BuildTS(60, 0))
	require.NotEqual(t, pkMoveArrivalIncomplete, controlVerdict,
		"control setup must keep the completeness gate open")
	require.NotNil(t, control.arrivals)
	require.Equal(t, totalLinks, control.arrivalScans)

	cases := []struct {
		name      string
		after     int
		wantScans uint64
	}{
		// Trips at the first grouping boundary: no key grouped at all.
		{"construction entry", captureChecks, 0},
		// Trips at the second key's boundary: exactly one of the two
		// single-link keys was grouped (both keys carry one link, so
		// the count is order-independent), strictly less than the full
		// walk. The build then aborts before attribution.
		{"mid grouping", captureChecks + 1, 1},
		// Grouping completes (both keys grouped, scans == total)
		// followed by the attribution key boundary, then the trip
		// fires inside the first candidate's chain hop: every
		// attribution is discarded before adoption.
		{"in-chain attribution", captureChecks + int(totalLinks) + 1, totalLinks},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newProvider()
			p.ctx = &pkMoveArrivalCancelAfterChecksCtx{
				Context: context.Background(),
				after:   tc.after,
				closed:  make(chan struct{}),
			}
			close(p.ctx.(*pkMoveArrivalCancelAfterChecksCtx).closed)
			verdict := p.anchorArrivalVerdict(view, []byte{2}, types.BuildTS(60, 0))
			require.Equal(t, pkMoveArrivalIncomplete, verdict,
				"a cancelled build answers fail-safe")
			require.Nil(t, p.arrivals,
				"a cancelled build publishes no index")
			require.True(t, p.arrivalCapFailed,
				"a cancelled build poisons fail-safe, never a silent downgrade")
			require.Nil(t, p.arrivalCommitIdx,
				"a cancelled build discards the capture's adopted index")
			require.Equal(t, tc.wantScans, p.arrivalScans,
				"remaining construction work is bounded by the trip point")

			// Clean retry: a live context rebuilds wholesale — the
			// poison clears, the index publishes, and the answer
			// matches the never-cancelled control.
			p.ctx = context.Background()
			retry := p.anchorArrivalVerdict(view, []byte{2}, types.BuildTS(60, 0))
			require.Equal(t, controlVerdict, retry,
				"the retry rebuilds wholesale and rules like the control")
			require.False(t, p.arrivalCapFailed)
			require.Equal(t, control.arrivals, p.arrivals,
				"the retried index is byte-identical to the control build")
			require.Equal(t, tc.wantScans+totalLinks, p.arrivalScans,
				"the cancelled work plus the wholesale retry is fully accounted")
		})
	}
}

// A provider with windows but no engine and no fabric captures a
// complete EMPTY index: absence is silence (r50 (D); the walk
// fixtures' posture).
func TestArrivalCaptureNoEngineNoFabricIsCompleteSilence(t *testing.T) {
	p := pkMoveWalkTestProvider(pkMoveWalkTestLink(1, 1, 2, 2, 40))
	p.windows = []pkMoveLinkTableWindow{{tableID: 1}}
	p.fullWindows = map[uint64][2]uint64{1: {}}
	p.captureArrivalCommitsLocked()
	require.False(t, p.arrivalCapFailed)
	require.NotNil(t, p.arrivalCommitIdx)
	view := pkMoveWalkTestStubView()
	require.Equal(t, pkMoveArrivalNone,
		p.anchorArrivalVerdict(view, []byte{2}, types.BuildTS(60, 0)),
		"no capture source answers every tie from the caches alone")
}

// The production capture fold decodes the link table's change stream
// positionally (DDL order plus the appended commit-ts column), admits
// only this window's relation, and rejects malformed widths.
func TestFoldPkMoveLinkCaptureBatch(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	newVecs := func() []*vector.Vector {
		vecs := make([]*vector.Vector, 11)
		for i, typ := range []types.T{
			types.T_varbinary, // link_id
			types.T_uint64,    // table_id
			types.T_varbinary, // old_pk_sha
			types.T_varbinary, // old_row_id
			types.T_varbinary, // old_pk
			types.T_varbinary, // new_pk_sha
			types.T_varbinary, // new_pk
			types.T_varbinary, // new_row_id
			types.T_uint64,    // write_ts
			types.T_varbinary, // txn_id
			types.T_TS,        // commit ts
		} {
			vecs[i] = vector.NewVec(typ.ToType())
		}
		return vecs
	}
	appendRow := func(vecs []*vector.Vector, tableID, writeTS uint64, commitTS types.TS) {
		require.NoError(t, vector.AppendBytes(vecs[0], make([]byte, 64), false, mp))
		require.NoError(t, vector.AppendFixed(vecs[1], tableID, false, mp))
		require.NoError(t, vector.AppendBytes(vecs[2], make([]byte, 32), false, mp))
		require.NoError(t, vector.AppendBytes(vecs[3], make([]byte, 24), false, mp))
		require.NoError(t, vector.AppendBytes(vecs[4], []byte{1}, false, mp))
		require.NoError(t, vector.AppendBytes(vecs[5], make([]byte, 32), false, mp))
		require.NoError(t, vector.AppendBytes(vecs[6], []byte{2}, false, mp))
		require.NoError(t, vector.AppendBytes(vecs[7], make([]byte, 24), false, mp))
		require.NoError(t, vector.AppendFixed(vecs[8], writeTS, false, mp))
		require.NoError(t, vector.AppendBytes(vecs[9], make([]byte, 16), false, mp))
		require.NoError(t, vector.AppendFixed(vecs[10], commitTS, false, mp))
	}
	vecs := newVecs()
	appendRow(vecs, 7, 40, types.BuildTS(60, 0))
	appendRow(vecs, 8, 42, types.BuildTS(61, 0)) // another relation's history
	appendRow(vecs, 7, 44, types.BuildTS(62, 1))
	data := batch.NewWithSize(11)
	data.Vecs = vecs
	data.SetRowCount(3)

	type record struct {
		scope pkMoveTxnScope
		cts   types.TS
	}
	var got []record
	err := foldPkMoveLinkCaptureBatch(
		pkMoveLinkTableWindow{tableID: 7}, data,
		func(scope pkMoveTxnScope, cts types.TS) error {
			got = append(got, record{scope, cts})
			return nil
		})
	require.NoError(t, err)
	require.Len(t, got, 2, "only this window's relation is admitted")
	require.Equal(t, types.BuildTS(40, 0), got[0].scope.writeTS,
		"write_ts is a physical-nanosecond create instant, logical zero")
	require.False(t, got[0].scope.hasTxn, "an all-zero txn id is identity unknown")
	require.Equal(t, types.BuildTS(60, 0), got[0].cts)
	require.Equal(t, types.BuildTS(44, 0), got[1].scope.writeTS)
	require.Equal(t, types.BuildTS(62, 1), got[1].cts)
	data.Clean(mp)

	// A malformed stream width is corruption, failing the capture.
	bad := batch.NewWithSize(10)
	bad.SetRowCount(0)
	require.Error(t, foldPkMoveLinkCaptureBatch(
		pkMoveLinkTableWindow{tableID: 7}, bad, func(pkMoveTxnScope, types.TS) error { return nil }))
	require.Zero(t, mp.CurrNB())
}

// TestPkMoveSyntheticWindowSeedFailsClosed pins the r53 D156(E) amendment
// at the walk's first binding site: an anchor tombstone whose commit
// instant is a CERTIFIED-SYNTHETIC window end (an end-stamping state
// read, a Snapshot hint, or a recovery substitution — never a recorded
// commit) cannot seed the chain's birth certificate, so the identical
// stream that RESOLVES through a certified-exact reader classifies
// UNRESOLVED through a tainted one.
func TestPkMoveSyntheticWindowSeedFailsClosed(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()

	anchorRid := buildHashDiffRowID(t, 51)
	lineageRid := buildHashDiffRowID(t, 52)
	oldKeyEnc := pkMoveTestKey(t, 2)
	newKeyEnc := pkMoveTestKey(t, 3)
	commitTS := types.BuildTS(50, 0)

	p := pkMoveWalkTestProvider(pkMoveLink{
		oldRowID:    anchorRid,
		newRowID:    lineageRid,
		hasNewRowID: true,
		oldPK:       oldKeyEnc,
		newPK:       newKeyEnc,
		writeTS:     types.BuildTS(10, 0),
	})
	dataHashmap := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{{
		rowID: lineageRid, id: 3, name: "three", hidden: "h",
		birth: commitTS,
	}})
	t.Cleanup(func() { require.NoError(t, dataHashmap.Close()) })
	tombs := map[string][]pkMoveTombRef{
		string(oldKeyEnc): {{rowID: anchorRid, cts: commitTS}},
	}

	// Certified-quiet control: the same instant is a recorded commit, so
	// the anchor's birth certificate binds and the chain resolves.
	quiet := &pkMoveHashmapView{data: dataHashmap, tombs: tombs}
	_, _, verdict := p.resolvePkMoveChainVerdict(quiet, oldKeyEnc, anchorRid)
	require.Equal(t, pkMoveResolved, verdict)

	// The identical stream through a window certified synthetic: its end
	// stamp never seeds a birth certificate.
	synthetic := &pkMoveHashmapView{
		data:    dataHashmap,
		tombs:   tombs,
		tainted: map[types.TS]struct{}{commitTS: {}},
	}
	_, _, verdict = p.resolvePkMoveChainVerdict(synthetic, oldKeyEnc, anchorRid)
	require.Equal(t, pkMoveUnresolved, verdict)
}

// TestPkMoveConstructSubstitutedWindowCannotAuthorizeFold runs the r53
// D156(E) amendment end to end across the CONSTRUCTION-time certification
// path: the visible-state recovery reader can be installed (and notify
// the observer) while NewPartitionChangesHandle splits a window's range,
// before any drain-side substitution baseline exists, so
// constructChangeHandle records the swap in the window metadata. The
// end-stamped extract that window serves must consequently never
// authorize a pk-move fold — provenance treats the instant as
// unverifiable and the apply's statement-atomic UNRESOLVED lane (whose
// rejection leaves parent and child rows unchanged, as the fail-atomic
// fixtures pin) is what a MERGE/PICK sees — while the byte-identical
// stream served by a certified exact reader resolves and replays.
func TestPkMoveConstructSubstitutedWindowCannotAuthorizeFold(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := ses.proc.Mp()

	anchorRid := buildHashDiffRowID(t, 51)
	lineageRid := buildHashDiffRowID(t, 52)
	oldKeyEnc := pkMoveTestKey(t, 2)
	commitTS := types.BuildTS(50, 0)

	p := pkMoveWalkTestProvider(pkMoveLink{
		oldRowID:    buildHashDiffRowID(t, 51),
		newRowID:    lineageRid,
		hasNewRowID: true,
		oldPK:       oldKeyEnc,
		newPK:       pkMoveTestKey(t, 3),
		writeTS:     types.BuildTS(10, 0),
	})
	dataHashmap := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{{
		rowID: lineageRid, id: 3, name: "three", hidden: "h",
		birth: commitTS,
	}})
	t.Cleanup(func() { require.NoError(t, dataHashmap.Close()) })

	// Drain one window through the production certification path: the
	// recovery reader's serving shape is a tail-hinted stream stamped
	// with the window END instant, and the window metadata carries the
	// construction-time swap flag (or not, for the certified-exact
	// control). buildHashmapForTable owns the certification verdict.
	drain := func(constructSubst bool) (map[string][]pkMoveTombRef, map[types.TS]struct{}) {
		tblStuff := newTestBranchTableStuff(ctrl)
		tblStuff.def.pkColIdx = 0
		tblStuff.def.pkColIdxes = []int{0}
		tblStuff.hashmapAllocator = newBranchHashmapAllocator(dataBranchHashmapLimitRate)
		worker, err := ants.NewPool(1)
		require.NoError(t, err)
		defer worker.Release()
		tblStuff.worker = worker
		tblStuff.recoveryResources = &dataBranchVisibleStateRecoveryResources{
			allocator: tblStuff.hashmapAllocator,
		}
		tblStuff.tarWindowEnds = []types.TS{commitTS}
		tblStuff.tarWindowFromEmpty = []bool{false}
		tblStuff.tarWindowSubstConstruct = []bool{constructSubst}

		dataBat := buildIntDataBatch(
			t, mp,
			[]string{catalog.Row_ID, "id", "name", "hidden", "cts"},
			[]types.Type{
				types.T_Rowid.ToType(), types.T_int64.ToType(),
				types.T_varchar.ToType(), types.T_varchar.ToType(),
				types.T_TS.ToType(),
			},
			[][]any{{lineageRid, int64(3), "three", "h", commitTS}},
		)
		tombBat := buildIntDataBatch(
			t, mp,
			[]string{catalog.Row_ID, "id", "cts"},
			[]types.Type{
				types.T_Rowid.ToType(), types.T_int64.ToType(), types.T_TS.ToType(),
			},
			[][]any{{anchorRid, int64(2), commitTS}},
		)
		handles := []engine.ChangesHandle{
			&stubEngineChangesHandle{responses: []stubEngineChangesHandleResponse{
				{data: dataBat, tombstone: tombBat, hint: engine.ChangesHandle_Tail_done},
			}},
		}
		_, _, evidence, err := buildHashmapForTable(
			context.Background(), mp, &tblStuff, handles, "target", nil, true,
		)
		require.NoError(t, err)
		require.NotNil(t, evidence)
		return evidence.tombs, evidence.tainted
	}

	// Certified exact: the recorded anchor death at the window's terminal
	// instant binds the birth certificate and the chain resolves — the
	// same serving bytes the recovery reader would emit.
	exactTombs, exactTainted := drain(false)
	require.Empty(t, exactTainted)
	exact := &pkMoveHashmapView{data: dataHashmap, tombs: exactTombs, tainted: exactTainted}
	_, _, verdict := p.resolvePkMoveChainVerdict(exact, oldKeyEnc, anchorRid)
	require.Equal(t, pkMoveResolved, verdict)

	// Construction-substituted: the end stamp is a serving artifact, not
	// a recorded commit; provenance can no longer bind it, so the fold
	// the exact reader authorized is refused.
	substTombs, substTainted := drain(true)
	require.Contains(t, substTainted, commitTS,
		"the construction-time swap alone certifies the window end synthetic")
	subst := &pkMoveHashmapView{data: dataHashmap, tombs: substTombs, tainted: substTainted}
	_, _, verdict = p.resolvePkMoveChainVerdict(subst, oldKeyEnc, anchorRid)
	require.Equal(t, pkMoveUnresolved, verdict)
}

// TestDrainPkMoveLinkCaptureHandle pins the capture drain protocol of
// r51 (G) against the engine's serving contract: a page is live while
// EITHER side carries rows, so the standalone tombstone pages the
// engine legitimately serves — a size-bounded tombstone page ahead of
// later data, or link reclamation rows from an unrelated dropped
// generation under the capture's account-wide read — must not end the
// stream. Stopping at the first data==nil certifies false ABSENCE in
// the arrival-commit index and re-opens the unsafe fold D156 was
// introduced to reject. Every returned batch is caller-owned and
// released on success and failure alike, and a read failure propagates
// unchanged so the caller's incomplete (fail-closed) classification
// stands.
func TestDrainPkMoveLinkCaptureHandle(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	newLinkVecs := func() []*vector.Vector {
		vecs := make([]*vector.Vector, 11)
		for i, typ := range []types.T{
			types.T_varbinary, // link_id
			types.T_uint64,    // table_id
			types.T_varbinary, // old_pk_sha
			types.T_varbinary, // old_row_id
			types.T_varbinary, // old_pk
			types.T_varbinary, // new_pk_sha
			types.T_varbinary, // new_pk
			types.T_varbinary, // new_row_id
			types.T_uint64,    // write_ts
			types.T_varbinary, // txn_id
			types.T_TS,        // commit ts
		} {
			vecs[i] = vector.NewVec(typ.ToType())
		}
		return vecs
	}
	appendLinkRow := func(vecs []*vector.Vector, tableID, writeTS uint64, txnTag byte, commitTS types.TS) {
		txnID := make([]byte, catalog.DataBranchUpdateLinkTxnIDBytes)
		txnID[0] = txnTag
		require.NoError(t, vector.AppendBytes(vecs[0], make([]byte, 64), false, mp))
		require.NoError(t, vector.AppendFixed(vecs[1], tableID, false, mp))
		require.NoError(t, vector.AppendBytes(vecs[2], make([]byte, 32), false, mp))
		require.NoError(t, vector.AppendBytes(vecs[3], make([]byte, 24), false, mp))
		require.NoError(t, vector.AppendBytes(vecs[4], []byte{1}, false, mp))
		require.NoError(t, vector.AppendBytes(vecs[5], make([]byte, 32), false, mp))
		require.NoError(t, vector.AppendBytes(vecs[6], []byte{2}, false, mp))
		require.NoError(t, vector.AppendBytes(vecs[7], make([]byte, 24), false, mp))
		require.NoError(t, vector.AppendFixed(vecs[8], writeTS, false, mp))
		require.NoError(t, vector.AppendBytes(vecs[9], txnID, false, mp))
		require.NoError(t, vector.AppendFixed(vecs[10], commitTS, false, mp))
	}
	mkTombstonePage := func(n int) *batch.Batch {
		// The capture drain never inspects tombstone rows (skipDeletes
		// semantics); only ownership matters here.
		page := batch.NewWithSize(2)
		page.Vecs[0] = vector.NewVec(types.T_varbinary.ToType())
		page.Vecs[1] = vector.NewVec(types.T_TS.ToType())
		for i := 0; i < n; i++ {
			require.NoError(t, vector.AppendBytes(page.Vecs[0], []byte{byte(i)}, false, mp))
			require.NoError(t, vector.AppendFixed(page.Vecs[1], types.BuildTS(2, uint32(i)), false, mp))
		}
		page.SetRowCount(n)
		return page
	}

	t.Run("a tombstone-only page does not end the capture", func(t *testing.T) {
		linkVecs := newLinkVecs()
		appendLinkRow(linkVecs, 7, 40, 1, types.BuildTS(60, 0))
		appendLinkRow(linkVecs, 8, 42, 2, types.BuildTS(61, 0)) // another relation's history
		appendLinkRow(linkVecs, 7, 44, 3, types.BuildTS(62, 1))
		linkBat := batch.NewWithSize(11)
		linkBat.Vecs = linkVecs
		linkBat.SetRowCount(3)

		stub := &stubEngineChangesHandle{responses: []stubEngineChangesHandleResponse{
			{tombstone: mkTombstonePage(2)}, // standalone tombstone page FIRST
			{data: linkBat},                 // then the relevant link inserts
			{data: nil, tombstone: nil},     // true exhaustion
		}}

		type admission struct {
			scope pkMoveTxnScope
			cts   types.TS
		}
		var got []admission
		err := drainPkMoveLinkCaptureHandle(
			context.Background(), stub, mp,
			func(data *batch.Batch) error {
				return foldPkMoveLinkCaptureBatch(
					pkMoveLinkTableWindow{tableID: 7}, data,
					func(scope pkMoveTxnScope, cts types.TS) error {
						got = append(got, admission{scope, cts})
						return nil
					})
			},
		)
		require.NoError(t, err)
		// The pre-fix protocol returned success at the tombstone page and
		// certified absence for both of these admitted commits.
		require.Len(t, got, 2, "only this window's relation is admitted, beyond the tombstone page")
		require.True(t, got[0].scope.hasTxn)
		require.Equal(t, types.BuildTS(60, 0), got[0].cts)
		require.Equal(t, types.BuildTS(62, 1), got[1].cts)
		require.Zero(t, mp.CurrNB())
		require.Zero(t, mp.OnHeapCurrNB(), "every served page is released")
	})

	t.Run("read failure releases partial batches and propagates", func(t *testing.T) {
		partData := mkTombstonePage(1)
		partTomb := mkTombstonePage(1)
		wantErr := moerr.NewInternalErrorNoCtx("capture read failed")
		stub := &stubEngineChangesHandle{responses: []stubEngineChangesHandleResponse{
			{data: partData, tombstone: partTomb, err: wantErr},
		}}
		foldCalls := 0
		err := drainPkMoveLinkCaptureHandle(
			context.Background(), stub, mp,
			func(data *batch.Batch) error {
				foldCalls++
				return nil
			},
		)
		require.Equal(t, wantErr, err)
		require.Zero(t, foldCalls, "an errored page is a partial page: it is released, never folded")
		require.Zero(t, mp.CurrNB())
		require.Zero(t, mp.OnHeapCurrNB())
	})

	t.Run("failure after a tombstone page stays fail-closed", func(t *testing.T) {
		wantErr := moerr.NewInternalErrorNoCtx("capture read failed mid-stream")
		stub := &stubEngineChangesHandle{responses: []stubEngineChangesHandleResponse{
			{tombstone: mkTombstonePage(2)},
			{err: wantErr},
		}}
		err := drainPkMoveLinkCaptureHandle(
			context.Background(), stub, mp,
			func(data *batch.Batch) error { return nil },
		)
		require.Equal(t, wantErr, err, "the read failure reaches the caller, which poisons the index")
		require.Zero(t, mp.CurrNB())
		require.Zero(t, mp.OnHeapCurrNB())
	})
}

// TestPkMoveSyntheticContinuityFailsClosed pins the r53 D156(E) amendment
// at the anchor-vs-LCA continuity comparison: a tainted previous-death
// instant can never prove a reincarnation (the certified-quiet control
// answers PROVEN-NONE and keeps the delete's semantics), and a tainted
// anchor birth can never hold the same-instant tie the certified-quiet
// control resolves through.
func TestPkMoveSyntheticContinuityFailsClosed(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()

	anchorRid := buildHashDiffRowID(t, 51)
	lineageRid := buildHashDiffRowID(t, 52)
	prevRid := buildHashDiffRowID(t, 53)
	oldKeyEnc := pkMoveTestKey(t, 2)
	newKeyEnc := pkMoveTestKey(t, 3)
	commitTS := types.BuildTS(50, 0)
	prevTS := types.BuildTS(40, 0)

	newProvider := func() *pkMoveLinkProvider {
		return pkMoveWalkTestProvider(pkMoveLink{
			oldRowID:    anchorRid,
			newRowID:    lineageRid,
			hasNewRowID: true,
			oldPK:       oldKeyEnc,
			newPK:       newKeyEnc,
			writeTS:     types.BuildTS(10, 0),
		})
	}
	dataHashmap := buildPkMoveStreamHashmap(t, mp, []pkMoveStreamRow{{
		rowID: lineageRid, id: 3, name: "three", hidden: "h",
		birth: commitTS,
	}})
	t.Cleanup(func() { require.NoError(t, dataHashmap.Close()) })
	tombs := map[string][]pkMoveTombRef{
		string(oldKeyEnc): {
			{rowID: prevRid, cts: prevTS},
			{rowID: anchorRid, cts: commitTS},
		},
	}

	t.Run("previous death instant tainted", func(t *testing.T) {
		// Control: an exact previous death with no anchor birth proves
		// the anchor reincarnated the key in a LATER transaction — the
		// lineage the destination holds ended there, so the delete's own
		// semantics stand.
		quiet := &pkMoveHashmapView{data: dataHashmap, tombs: tombs}
		_, _, verdict := newProvider().resolvePkMoveChainVerdict(quiet, oldKeyEnc, anchorRid)
		require.Equal(t, pkMoveProvenNone, verdict)

		synthetic := &pkMoveHashmapView{
			data:    dataHashmap,
			tombs:   tombs,
			tainted: map[types.TS]struct{}{prevTS: {}},
		}
		_, _, verdict = newProvider().resolvePkMoveChainVerdict(synthetic, oldKeyEnc, anchorRid)
		require.Equal(t, pkMoveUnresolved, verdict)
	})

	t.Run("anchor birth instant tainted", func(t *testing.T) {
		// The anchor version was born at exactly the previous holder's
		// death instant (the update chain's reincarnation shape).
		departed := map[pkMoveDepartKey]types.TS{
			{rowID: anchorRid, cts: commitTS}: prevTS,
		}
		// Control: the bare tie holds (no recorded move lands at the
		// anchor key at the tie instant) and the chain resolves.
		quiet := &pkMoveHashmapView{data: dataHashmap, tombs: tombs, departed: departed}
		_, _, verdict := newProvider().resolvePkMoveChainVerdict(quiet, oldKeyEnc, anchorRid)
		require.Equal(t, pkMoveResolved, verdict)

		synthetic := &pkMoveHashmapView{
			data:     dataHashmap,
			tombs:    tombs,
			departed: departed,
			tainted:  map[types.TS]struct{}{prevTS: {}},
		}
		_, _, verdict = newProvider().resolvePkMoveChainVerdict(synthetic, oldKeyEnc, anchorRid)
		require.Equal(t, pkMoveUnresolved, verdict)
	})
}
