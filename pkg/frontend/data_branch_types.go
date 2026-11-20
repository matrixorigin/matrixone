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
	relSnapshot      []types.TS // The snapshot at which each relation was opened.
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

func (r collectRange) segmentSnapshot(i int, fallback types.TS) types.TS {
	if i >= 0 && i < len(r.relSnapshot) {
		return r.relSnapshot[i]
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

func firstDataBranchForkIndex(lineageOnly []bool, pathLen int) (int, bool) {
	for i := 1; i < pathLen; i++ {
		if i < len(lineageOnly) && lineageOnly[i] {
			continue
		}
		return i, true
	}
	return 0, false
}

func firstDataBranchForkTS(pathTS []types.TS, lineageOnly []bool) (types.TS, bool) {
	if i, ok := firstDataBranchForkIndex(lineageOnly, len(pathTS)); ok {
		return pathTS[i], true
	}
	return types.TS{}, false
}

// lcaProbeSnapshot is the shared baseline used to open the LCA relation.
func (m *branchMetaInfo) lcaProbeSnapshot(tarSP, baseSP types.TS) types.TS {
	probe, _ := m.lcaProbeSnapshots(tarSP, baseSP, false)
	return probe
}

// lcaProbeAnchors anchors each side at its first branch fork, or its
// endpoint snapshot if it observes the LCA directly (possibly through ALTER
// generations).
func (m *branchMetaInfo) lcaProbeAnchors(tarSP, baseSP types.TS) (tarAnchor, baseAnchor types.TS, bothForked bool) {
	tarAnchor, tarForked := firstDataBranchForkTS(m.pathFromLCAToTarTS, m.pathFromLCAToTarLineageOnly)
	baseAnchor, baseForked := firstDataBranchForkTS(m.pathFromLCAToBaseTS, m.pathFromLCAToBaseLineageOnly)
	if !tarForked {
		tarAnchor = tarSP
	}
	if !baseForked {
		baseAnchor = baseSP
	}
	return tarAnchor, baseAnchor, tarForked && baseForked
}

// lcaProbeSnapshots uses the earlier observation as the shared baseline.
func (m *branchMetaInfo) lcaProbeSnapshots(
	tarSP, baseSP types.TS, preservePickConflicts bool,
) (tarProbe, baseProbe types.TS) {
	tarAnchor, baseAnchor, bothForked := m.lcaProbeAnchors(tarSP, baseSP)
	// For sibling PICK, each side's conflict baseline includes the state it
	// inherited at its own fork. With an ancestor operand both probes must
	// honor the shared baseline, including an endpoint older than the fork.
	if preservePickConflicts && bothForked {
		return tarAnchor, baseAnchor
	}
	if tarAnchor.LT(&baseAnchor) {
		return tarAnchor, tarAnchor
	}
	return baseAnchor, baseAnchor
}

// lcaProbeTableIDs pairs the sibling PICK snapshots with their fork parents.
func (m *branchMetaInfo) lcaProbeTableIDs(preservePickConflicts bool) (tarTableID, baseTableID uint64) {
	tarTableID, baseTableID = m.lcaTableId, m.lcaTableId
	if preservePickConflicts {
		tarIdx, tarForked := firstDataBranchForkIndex(m.pathFromLCAToTarLineageOnly, len(m.pathFromLCAToTarTS))
		baseIdx, baseForked := firstDataBranchForkIndex(m.pathFromLCAToBaseLineageOnly, len(m.pathFromLCAToBaseTS))
		if tarForked && baseForked {
			if tarIdx > 0 && tarIdx-1 < len(m.pathFromLCAToTar) {
				tarTableID = m.pathFromLCAToTar[tarIdx-1]
			}
			if baseIdx > 0 && baseIdx-1 < len(m.pathFromLCAToBase) {
				baseTableID = m.pathFromLCAToBase[baseIdx-1]
			}
		}
	}
	return tarTableID, baseTableID
}

type tableStuff struct {
	tarRel   engine.Relation
	baseRel  engine.Relation
	tarSnap  *plan.Snapshot
	baseSnap *plan.Snapshot

	// The endpoint handles can be opened by a background transaction whose
	// snapshot is later than the foreground observation snapshot.
	tarRelSnapshot  types.TS
	baseRelSnapshot types.TS

	// tarOperand/baseOperand are the operand descriptors of design §6.1,
	// nil in unit tests that construct tableStuff directly. Their
	// identities drive every catalog read that targets an operand's
	// physical owner (design §6, I6).
	tarOperand  *branchOperand
	baseOperand *branchOperand

	lcaRel            engine.Relation
	lcaRelSnapshot    types.TS // snapshot at which lcaRel was opened
	lcaProbeTableId   uint64   // nonzero when the probe uses a different generation from lcaRel
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
		tarLCAColNames      []string // LCA names at the target fork, in target batch order
		baseLCAColNames     []string // LCA names at the base fork, in target batch order
		// indexedSpecialUpdateIdxes are special writable columns that belong to
		// a unique secondary index. MatrixOne rejects assigning those columns in
		// ON DUPLICATE KEY UPDATE, so changed values use a native UPDATE instead.
		indexedSpecialUpdateIdxes []int
	}

	worker               *ants.Pool
	hashmapAllocator     *branchHashmapAllocator
	maxTombstoneBatchCnt int
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

func (t tableStuff) lcaProbeTableID(ctx context.Context) uint64 {
	if t.lcaProbeTableId != 0 {
		return t.lcaProbeTableId
	}
	if t.lcaRel != nil {
		return t.lcaRel.GetTableID(ctx)
	}
	return 0
}

func (t tableStuff) lcaFallbackRangeRel(snapshotTS types.TS) engine.Relation {
	if t.lcaProbeTableId != 0 {
		return nil
	}
	if !t.lcaRelSnapshot.IsEmpty() && t.lcaRelSnapshot.EQ(&snapshotTS) {
		return t.lcaRel
	}
	return nil
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

// resolvedRelSnapshots returns the snapshots used to open the endpoint handles.
func (t *tableStuff) resolvedRelSnapshots(ses *Session) (tarRelSP, baseRelSP types.TS) {
	tarRelSP, baseRelSP = t.tarRelSnapshot, t.baseRelSnapshot
	if tarRelSP.IsEmpty() || baseRelSP.IsEmpty() {
		tarSP, baseSP := t.resolvedSnapshots(ses)
		if tarRelSP.IsEmpty() {
			tarRelSP = tarSP
		}
		if baseRelSP.IsEmpty() {
			baseRelSP = baseSP
		}
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
	batch          *batch.Batch
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
}
