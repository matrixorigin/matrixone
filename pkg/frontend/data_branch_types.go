// Copyright 2025 Matrix Origin
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

package frontend

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/panjf2000/ants/v2"
)

const (
	fakeKind = iota
	normalKind
	compositeKind
)

const (
	diffInsert = "INSERT"
	diffDelete = "DELETE"
	diffUpdate = "UPDATE"
)

const (
	diffSideUnknown = iota
	diffSideTarget
	diffSideBase
)

const (
	dataBranchApplyModeOnlineMerge dataBranchApplyMode = iota
	dataBranchApplyModeOnlinePKOnly
	dataBranchApplyModePortableSQL
)

const dataBranchHashmapLimitRate = 0.8

type dataBranchApplyMode int

type branchHashmapAllocator struct {
	upstream  malloc.Allocator
	throttler rscthrottler.RSCThrottler
}

type branchHashmapDeallocator struct {
	upstream  malloc.Deallocator
	throttler rscthrottler.RSCThrottler
	size      uint64
}

var diffTempTableSeq uint64

const (
	maxSqlBatchCnt   = objectio.BlockMaxRows * 10
	maxSqlBatchSize  = mpool.MB * 32
	defaultRowBytes  = int64(128)
	tombstoneRowMult = int64(3)
	tombstoneRowDiv  = int64(2)
)

type collectRange struct {
	from             []types.TS
	end              []types.TS
	rel              []engine.Relation
	startBoundaryRel []engine.Relation
	// idents carries the physical identity each segment was opened under
	// (design §6): the walking side's endpoint identity for the endpoint
	// segment and the winning candidate identity for reopened lineage
	// nodes. A nil slice means every segment runs under the caller
	// context, the pre-routing behavior local-local diffs rely on.
	idents []branchIdentity
}

func (r collectRange) segmentIdentity(i int, fallback branchIdentity) branchIdentity {
	if i >= 0 && i < len(r.idents) {
		return r.idents[i]
	}
	return fallback
}

func (r collectRange) startBoundary(i int) engine.Relation {
	if i < 0 || i >= len(r.startBoundaryRel) {
		return nil
	}
	return r.startBoundaryRel[i]
}

// branchMetaInfo describes the lineage relationship between the two
// endpoints of a `data branch diff` request, expressed entirely as
// the DAG paths from the LCA (lowest common ancestor) down to each
// endpoint.
//
// All downstream consumers (decideCollectRange, hashDiff,
// findDeleteAndUpdateBat, …) derive whatever they need from these
// paths through the helper methods on this struct, so the path
// fields are the single source of truth for diff lineage.
//
// Path encoding. Index 0 is the LCA itself; the last index is the
// endpoint. pathFromLCAToXxxTS[i] is the CloneTS of node i (i.e. the
// moment node i was forked off node i-1). Index 0 holds the LCA's
// own CloneTS, which is unused for diff purposes — the LCA's own
// mutation window always starts at its creation commit TS instead.
//
// lcaTableId == 0 means "no LCA" (two unrelated tables, or
// snapshot-only diff on a single table without branch lineage).
//
// Running example. Tree (depth 4, 19 nodes, 10 leaves):
//
//	          t0
//	       /   |   \
//	     t1   t2   t3
//	    /  \   |   /  \
//	  t4   t5 t6  t7   t8
//	 / \   /\ /\  /\   /\
//	t9 t10 ... ... ... t18
//
// For `data branch diff t9 against t0`:
//
//	pathFromLCAToTar  = [t0, t1, t4, t9]
//	pathFromLCAToBase = [t0]
//
// For `data branch diff t9 against t11` (t11 sits under t5):
//
//	pathFromLCAToTar  = [t1, t4, t9]
//	pathFromLCAToBase = [t1, t5, t11]
//
// For `data branch diff t9 against t13` (t13 sits under t2→t6):
//
//	pathFromLCAToTar  = [t0, t1, t4, t9]
//	pathFromLCAToBase = [t0, t2, t6, t13]
type branchMetaInfo struct {
	lcaTableId uint64

	pathFromLCAToTar   []uint64
	pathFromLCAToTarTS []types.TS
	// LineageOnly[i] marks an ALTER copy-and-swap edge into path[i]. Such
	// edges split physical collection ranges but do not represent a branch
	// fork for LCA probing/conflict semantics.
	pathFromLCAToTarLineageOnly  []bool
	pathFromLCAToBase            []uint64
	pathFromLCAToBaseTS          []types.TS
	pathFromLCAToBaseLineageOnly []bool
}

// hasLCA reports whether tar and base share a common ancestor in the
// branch DAG.
func (m *branchMetaInfo) hasLCA() bool {
	return m.lcaTableId != 0
}

// tarLCASnapshot is the snapshot of the LCA at which tar's view of
// the LCA's state is anchored. It is one of the two per-side anchors
// that lcaProbeSnapshot composes into the shared divergence point
// both sides resolve against.
//
// When tar forked off the LCA (len(pathFromLCAToTar) > 1), the
// anchor is tar's own first-child CloneTS — the moment tar diverged.
// When tar IS the LCA (length-1 path), tar inherits the meeting
// point from base: base's first-child CloneTS, or baseSP if base is
// also the LCA (case-0 same-table diff).
func (m *branchMetaInfo) tarLCASnapshot(baseSP types.TS) types.TS {
	if ts, ok := firstDataBranchForkTS(
		m.pathFromLCAToTarTS, m.pathFromLCAToTarLineageOnly,
	); ok {
		return ts
	}
	if ts, ok := firstDataBranchForkTS(
		m.pathFromLCAToBaseTS, m.pathFromLCAToBaseLineageOnly,
	); ok {
		return ts
	}
	return baseSP
}

// baseLCASnapshot is the mirror of tarLCASnapshot for the base side.
func (m *branchMetaInfo) baseLCASnapshot(tarSP types.TS) types.TS {
	if ts, ok := firstDataBranchForkTS(
		m.pathFromLCAToBaseTS, m.pathFromLCAToBaseLineageOnly,
	); ok {
		return ts
	}
	if ts, ok := firstDataBranchForkTS(
		m.pathFromLCAToTarTS, m.pathFromLCAToTarLineageOnly,
	); ok {
		return ts
	}
	return tarSP
}

func firstDataBranchForkTS(pathTS []types.TS, lineageOnly []bool) (types.TS, bool) {
	for i := 1; i < len(pathTS); i++ {
		if i < len(lineageOnly) && lineageOnly[i] {
			continue
		}
		return pathTS[i], true
	}
	return types.TS{}, false
}

// lcaProbeSnapshot is the shared divergence point of the two
// lineages: the earlier of the two per-side fork anchors. diffOnBase
// uses it to fetch the LCA relation handle when the LCA is a
// third-party node (neither tar nor base), so the handle is valid on
// both sides' views, and hashDiffIfHasLCA uses it to anchor both
// sides' tombstone resolution so the LCA's writes between the two
// forks surface as the later-forked side's divergence instead of
// being absorbed into that side's fork baseline.
func (m *branchMetaInfo) lcaProbeSnapshot(tarSP, baseSP types.TS) types.TS {
	t := m.tarLCASnapshot(baseSP)
	b := m.baseLCASnapshot(tarSP)
	if t.LT(&b) {
		return t
	}
	return b
}

type tableStuff struct {
	tarRel   engine.Relation
	baseRel  engine.Relation
	tarSnap  *plan.Snapshot
	baseSnap *plan.Snapshot

	// tarOperand/baseOperand are the operand descriptors of design §6.1,
	// nil in unit tests that construct tableStuff directly. Their
	// identities drive every catalog read that targets an operand's
	// physical owner (design §6, I6).
	tarOperand  *branchOperand
	baseOperand *branchOperand

	lcaRel            engine.Relation
	lcaCTS            types.TS
	lcaHasZeroHistory bool
	// lcaIdent is the winning owner identity lcaRel was reopened under
	// (design §6.4). Nil when the LCA is an endpoint of a legacy
	// local-local diff; identity accessors fall back to the session.
	lcaIdent *branchIdentity

	def struct {
		colNames      []string     // all columns
		colTypes      []types.Type // all columns
		visibleIdxes  []int
		writableIdxes []int
		pkColIdx      int
		pkSeqnum      int   // physical column seqnum for PK (for ZoneMap lookup)
		pkColIdxes    []int // expanded pk columns
		pkKind        int

		commonIdxes         []int    // indices of common columns (target data-batch ordering)
		commonVisibleIdxes  []int    // user-visible subset of commonIdxes for DIFF and row identity
		commonWritableIdxes []int    // destination-writable subset of commonVisibleIdxes for apply
		tarOnlyIdxes        []int    // indices of target-only columns (target data-batch ordering)
		baseColToTarIdx     []int    // for base batch Vec[i+1], the target column index, or -1
		baseColNames        []string // target data column index to lineage-resolved base name
		lcaColNames         []string // target data column index to lineage-resolved LCA name
		// indexedSpecialUpdateIdxes are special writable columns that belong to
		// a unique secondary index. MatrixOne rejects assigning those columns in
		// ON DUPLICATE KEY UPDATE, so changed values use a native UPDATE instead.
		indexedSpecialUpdateIdxes []int
	}

	worker               *ants.Pool
	hashmapAllocator     *branchHashmapAllocator
	maxTombstoneBatchCnt int
	// pkMoveTarWindows / pkMoveBaseWindows record the physical table ids
	// and upper collect bounds of every relation whose change stream feeds
	// this side's diff, so pk-move provenance probes cover the whole
	// lineage (multi-generation branches) and never read links written
	// after the side's collect window ends.
	pkMoveTarWindows  []pkMoveLinkTableWindow
	pkMoveBaseWindows []pkMoveLinkTableWindow
	// recoveryResources is the visible-state governance instance the
	// apply attaches to its collection ctx; the hashmap build reads its
	// substitution counter to certify each window's serving mode (r53
	// D156(E) amendment).
	recoveryResources *dataBranchVisibleStateRecoveryResources
	// tarWindowEnds / baseWindowEnds carry each drained handle's window
	// end in handle order, aligning the collector's per-generation
	// windows with the hashmap build's sequential drain (nil-handled
	// windows never append, so the projection skips them identically).
	tarWindowEnds  []types.TS
	baseWindowEnds []types.TS
	// tarWindowFromEmpty / baseWindowFromEmpty mark, in the same handle
	// order, a whole-history window whose empty lower bound dispatched
	// the end-stamping checkpoint state reader (r53 D156(E) amendment:
	// serving-mode certification point (a)).
	tarWindowFromEmpty  []bool
	baseWindowFromEmpty []bool
	// tarWindowSubstConstruct / baseWindowSubstConstruct mark, in the same
	// handle order, a window whose handle CONSTRUCTION already swapped a
	// range to the end-stamping visible-state recovery reader: the
	// partition dispatcher can install that reader (and notify the
	// observer) while NewPartitionChangesHandle splits the range, before
	// any drain-side baseline exists (r53 D156(E) amendment: serving-mode
	// certification point (c) spans construction and drain).
	tarWindowSubstConstruct  []bool
	baseWindowSubstConstruct []bool
	// pickDepKeys marks encoded keys the PICK closure admitted as row
	// reconstruction dependencies rather than literal selection. A folded
	// key-moving update may only be produced for a tombstone whose anchor
	// key was literally picked; dependency keys exist so the selected
	// lineage can be reassembled, never to admit an unrelated lineage.
	pickDepKeys map[string]struct{}
	// pickLiteralKeys holds the literal PICK KEYS selection (encoded-pk
	// strings), captured before any dependency expansion. The fold pass
	// authorizes an inline victim delete at a fold's new key only when the
	// source deleted that key AND the key belongs to this selection; MERGE
	// leaves it nil, in which case any in-window source tombstone at the
	// new key authorizes (every source change is selected).
	pickLiteralKeys map[string]struct{}
	// lcaReaderProbeMode is shared across copies of tableStuff in a single diff
	// request. When enabled, LCA probing skips SQL and directly uses reader
	// fallback.
	lcaReaderProbeMode *atomic.Bool

	retPool *retBatchList

	bufPool *sync.Pool

	// seqWatermarks is the merge-scoped sequence watermark accumulator
	// (design §4 B), created by mergeDiffs before production and reconciled
	// after the final flush. Nil for every other apply path.
	seqWatermarks *sequenceWatermarkTracker
}

// resolvedSnapshots returns the effective per-side snapshot timestamps:
// the snapshot bound to {tar,base}Snap when present, otherwise the
// session's transaction snapshot. Centralized here so every call site
// (decideCollectRange, diffOnBase, hashDiffIfHasLCA) uses the exact
// same derivation.
func (t *tableStuff) resolvedSnapshots(ses *Session) (tarSP, baseSP types.TS) {
	tarSP = types.TimestampToTS(ses.GetTxnHandler().GetTxn().SnapshotTS())
	baseSP = tarSP
	if t.tarSnap != nil && t.tarSnap.TS != nil {
		tarSP = types.TimestampToTS(*t.tarSnap.TS)
	}
	if t.baseSnap != nil && t.baseSnap.TS != nil {
		baseSP = types.TimestampToTS(*t.baseSnap.TS)
	}
	return
}

// endpointIdentities resolves the physical owner identity of each endpoint
// (design §6.1). Descriptors carry the routed identity; a nil descriptor
// (unit tests, pre-routing constructions) falls back to the session
// identity, which keeps local-local behavior unchanged.
func (t *tableStuff) endpointIdentities(ctx context.Context, ses *Session) (tarIdent, baseIdent branchIdentity) {
	if t.tarOperand != nil {
		tarIdent = t.tarOperand.identity()
	} else {
		tarIdent = localOperandIdentity(ctx, ses.GetAccountId())
	}
	if t.baseOperand != nil {
		baseIdent = t.baseOperand.identity()
	} else {
		baseIdent = localOperandIdentity(ctx, ses.GetAccountId())
	}
	return
}

// resolvedLCAIdentity returns the owner identity lcaRel was resolved
// under (design §6.4), falling back to the session identity for legacy
// constructions that never set it.
func (t *tableStuff) resolvedLCAIdentity(ctx context.Context, ses *Session) branchIdentity {
	if t.lcaIdent != nil {
		return *t.lcaIdent
	}
	if t.lcaRel != nil {
		if t.tarOperand != nil && t.tarRel != nil &&
			t.lcaRel.GetTableID(ctx) == t.tarRel.GetTableID(ctx) {
			return t.tarOperand.identity()
		}
		if t.baseOperand != nil && t.baseRel != nil &&
			t.lcaRel.GetTableID(ctx) == t.baseRel.GetTableID(ctx) {
			return t.baseOperand.identity()
		}
	}
	return localOperandIdentity(ctx, ses.GetAccountId())
}

type batchWithKind struct {
	name       string
	kind       string
	side       int
	fromUpdate bool
	// requiresNativeUpdate marks an update whose changed special column is
	// part of a unique secondary index. It must bypass the staged ODKU path.
	requiresNativeUpdate bool
	restoreMissing       bool
	// hasReplacement marks a preserved PICK conflict whose source side emitted
	// a row to replace the destination conflict victim.
	hasReplacement bool
	// keyMovingUpdate marks a native destination UPDATE whose primary key
	// changes: batch rows carry the NEW values (kind diffInsert) or the OLD
	// rows (kind diffDelete, conflict checks only), and keyMovingOldKeys[i]
	// holds the OLD primary-key values (def.pkColIdxes order) for batch row i.
	// Provenance comes from mo_catalog.mo_data_branch_update_link; without a
	// recorded link the same change stream replays as delete+insert.
	// keyMovingWhereKeys, when set, has one entry per batch row; a non-nil
	// entry overrides the WHERE key of that row's native UPDATE. The merge
	// conflict resolver fills it when the destination moved the same row
	// elsewhere: the accepted source version must apply to the destination
	// row's current key, not to the shared old key.
	keyMovingUpdate    bool
	keyMovingOldKeys   [][]any
	keyMovingWhereKeys [][]any
	// keyMovingVictimAuth, when set, has one entry per batch row: whether
	// the apply may delete the destination row holding this move's new key
	// before the native UPDATE lands. Authorization comes from the source
	// stream (the source itself displaced that key-holder, so merge delete
	// semantics cover it) or from the conflict resolver honouring ACCEPT;
	// an occupant unchanged since the branch point with no source tombstone
	// at the new key stays unauthorized, so apply never silently deletes it
	// (the UPDATE then meets the raw duplicate-key guard atomically).
	keyMovingVictimAuth []bool
	// keyMovingUnchangedOcc, when set, has one entry per batch row: the
	// fold-time probe found a destination row holding the new key that the
	// destination never changed since the branch point and that no source
	// tombstone covers. The conflict resolver enforces the statement's
	// conflict option on it (FAIL/bare error, SKIP drops the move, ACCEPT
	// leaves the row for the raw duplicate-key guard).
	keyMovingUnchangedOcc []bool
	batch                 *batch.Batch
}

// logicalOperation returns the SQL-level operation represented by this diff
// batch. Expanded UPDATEs use DELETE/INSERT batches internally, but retain
// their original operation for diagnostics through fromUpdate.
func (b batchWithKind) logicalOperation() string {
	if b.fromUpdate {
		return diffUpdate
	}
	return b.kind
}

type emitFunc func(batchWithKind) (stop bool, err error)

type retBatchList struct {
	mu sync.Mutex
	// 0: data
	// 1: tombstone
	dList []*batch.Batch
	tList []*batch.Batch

	pinned map[*batch.Batch]struct{}

	dataVecCnt    int
	tombVecCnt    int
	dataTypes     []types.Type
	tombstoneType types.Type
	tombRowIDType types.Type
	tombKeyType   types.Type
}

type compositeOption struct {
	// onlineApply orders transaction probes and retry-capable MERGE/PICK writes.
	onlineApply           bool
	conflictOpt           *tree.ConflictOpt
	outputSQL             bool
	expandUpdate          bool
	preservePickConflicts bool
	// pickUserConflictOpt carries the PICK statement's own WHEN CONFLICT
	// clause (nil for MERGE and for a bare PICK). conflictOpt is forced to
	// ACCEPT under PICK so the producer preserves every conflict for the
	// consumer; conflict-resolution code that enforces folded-move
	// semantics must read this field instead.
	pickUserConflictOpt *tree.ConflictOpt
}
